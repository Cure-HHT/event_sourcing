// Implements: EVS-PRD-destinations/C
// (FIFO delivery order — drain attempts rows
//   in sequence_in_queue order; a wedged head halts the pass so trail rows are
//   never sent ahead of it)
// Implements: EVS-PRD-destinations/D
// (durable queue — drain reads from the
//   StorageBackend so queued rows survive restarts and are delivered on resume)
// Implements: EVS-PRD-destinations/E
// (pluggable delivery — drain delegates
//   each attempt to Destination.send, the application-supplied transport)
// Implements: EVS-PRD-destinations/G
// (failure isolation — drain runs against
//   one destination's queue, so a wedged head halts that pass and leaves every
//   other destination's queue drainable)
// Implements: EVS-PRD-destinations/H
// (an error raised by the
//   application-supplied transport is caught and categorized as SendTransient
//   rather than propagated to the caller of the pass)
// Implements: EVS-PRD-destinations/I
// (a failed attempt below maxAttempts
//   commits the attempt alone, leaving the row pending at the head of its
//   queue)
// Implements: EVS-PRD-destinations/J
// (every attempt is recorded, in the
//   transaction that commits the outcome it produced, or not at all when
//   that transaction does not commit)
// Implements: EVS-DEV-destination-drain/C
// (the attempt and the status it
//   produces commit in one transaction, or the attempt alone when the wedge
//   transaction does not commit)
// Implements: EVS-DEV-destination-drain/D
// (an attempt is recorded only on the
//   pending head the drain sent, and a wedge event is appended only for the
//   pending head the drain wedges)
// Implements: EVS-PRD-destinations/P+Q
// (a wedging outcome commits the attempt,
//   the wedged status and the wedge event recording its cause in one event
//   store transaction)
// Implements: EVS-DEV-destination-drain/J
// (an attempt that wedges commits with the
//   wedge, or alone when that transaction does not commit; each pass first
//   gives the head the status its recorded attempts call for (an exhausted
//   budget only where a budget is in effect), before any halt honour,
//   backoff check or send; a budget below one is refused)
// Implements: EVS-PRD-destinations/U
// (the drainer honours an operator's halt
//   request by wedging the queue head; a request committed before a send's
//   pre-send fence stops that send)
// Implements: EVS-DEV-destination-drain/N
// (the halt is honoured after the status
//   derivation and before the backoff check, in a transaction that re-reads
//   and verifies the request; immediately before each send a fence
//   transaction that writes finds no open request and the head unchanged
//   since the payload was built, and no send starts otherwise)
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/destinations/wedge_cause.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/transaction_rerun_limit.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// The configuration the drainer declares for a destination, and its
/// fingerprint, recorded in the wedge events the drainer appends for it.
@internal
final class DrainerConfiguration {
  const DrainerConfiguration({
    required this.configuration,
    required this.fingerprint,
  });

  /// The declared configuration map (`declaredConfiguration`).
  final Map<String, Object?> configuration;

  /// Its fingerprint (`configurationFingerprint`).
  final String fingerprint;
}

/// The first step of every queue-changing transaction of the drainer:
/// checks [lock] inside [txn], then runs the `beforeQueueWrites` test seam.
Future<void> _lockCheck(
  DrainLock lock,
  Transaction txn,
  String destinationId,
) async {
  await lock.assertHeldInTxn(txn);
  await DeliveryTestHooks.current?.beforeQueueWrites?.call(destinationId);
}

/// Drain the head of [destination]'s queue, in the backend of [registry]'s
/// event store: derive the head's status from its recorded attempts, check
/// backoff, call [Destination.send], and commit the attempt with the status
/// it produces. Returns when:
///
/// - the queue has no drain candidate (`readFifoHead` returns null after
///   skipping `sent` and `tombstoned` items);
/// - the head is [FinalStatus.wedged] (strict-order halt; operator recovery
///   via `tombstoneAndRefill`);
/// - the head's backoff has not elapsed;
/// - the most recent [Destination.send] returned [SendTransient] below the
///   `maxAttempts` budget (backoff applies on the next pass); or
/// - a transaction that decides a wedge reported failure (logged; the next
///   pass reads the head again and derives its status before any send).
///
/// Each iteration runs these steps in this order:
///
/// 1. Read the head; none, or a wedged head, ends the pass.
/// 2. Derive the status the head's recorded attempts call for: a last
///    attempt that reported a permanent failure wedges it with cause
///    [WedgeCause.permanentRefusal]; an attempt count at or above the
///    budget in effect wedges it with cause
///    [WedgeCause.retryBudgetExhausted] (covering a budget lowered since
///    the attempts were recorded). The wedge commits in its own
///    transaction, without a send, and ends the pass.
/// 3. Honour an open halt request: a read outside any transaction decides
///    whether to try, and the honouring transaction re-reads the request
///    and the head, verifies the request event in the log, and wedges the
///    head with cause [WedgeCause.operatorHalt], which ends the pass. A
///    request cancelled or replaced since the read changes nothing and the
///    iteration starts again; a stored request whose event the log does not
///    hold is removed, logged, and delivery continues.
/// 4. Backoff.
/// 5. Build the payload (every read of the item's events happens here).
/// 6. Pre-send fence: one transaction re-reads the halt request and the
///    head and writes the send fence record. It proceeds only when no
///    request is open and the head is the same item, still pending, with
///    the attempt count the payload was built from; otherwise it writes
///    nothing and the iteration starts again (an open request is then
///    honoured at step 3).
/// 7. Send, then commit the outcome.
///
/// On [SendOk] the head is marked `sent` and the loop advances. On
/// [SendPermanent], or a [SendTransient] whose attempt reaches the budget,
/// one event-store transaction records the attempt, marks the head wedged,
/// consumes any open halt request, appends the wedge event and writes the
/// destination's wedge record (`DestinationRegistry.wedgeHeadInTxn`). When that transaction reports
/// failure, the failure is logged and a second transaction reads the head
/// again: a pending head (the wedge rolled back) gets the attempt alone,
/// and step 2 of a later pass wedges it before any further send; a head
/// the wedge record names as wedged (the wedge committed before the
/// failure was reported) gets nothing more. The pass then ends. Items
/// behind a wedged head are never attempted.
///
/// [policy] is an optional [SyncPolicy] override; when null, the drain
/// falls back to [SyncPolicy.defaults]. Its `maxAttempts` is the budget in
/// effect, recorded in every wedge event. A budget below one is refused
/// with an [ArgumentError] before anything is read or sent.
///
/// Every transaction that changes the queue (a wedge, a halt honour, the
/// pre-send fence, each outcome) checks [lock] first and commits nothing
/// when the drainer no longer holds it: the [DrainLockLostException]
/// propagates to the caller, and no send starts after it. [declared] is the
/// configuration the drainer declares for the destination, recorded with
/// [lock]'s epoch in the wedge events it appends. [stopRequested], when it
/// returns true at the top of an iteration or once the pre-send fence has
/// committed, ends the drain before any further halt honour or send (the
/// delivery cycle is closing, or detected the loss of its lock); an outcome
/// already being recorded still commits.
@internal
Future<void> drain(
  Destination destination, {
  required DestinationRegistry registry,
  required DrainLock lock,
  Clock? clock,
  SyncPolicy? policy,
  DrainerConfiguration? declared,
  bool Function()? stopRequested,
}) async {
  final backend = registry.backend;
  final now = clock ?? () => DateTime.now().toUtc();
  final effective = policy ?? SyncPolicy.defaults;
  checkRetryBudget(effective);
  final destinationId = destination.id;
  while (true) {
    if (stopRequested?.call() ?? false) return;
    // (1) Read the head. A wedged head halts the drain; recovery is
    // tombstoneAndRefill. readFifoHead returns the wedged row (rather than
    // skipping it) so UI surfaces can observe the wedge via that entry
    // point.
    final head = await backend.readFifoHead(destinationId);
    if (head == null) return;
    if (head.finalStatus == FinalStatus.wedged) return;
    // head.finalStatus is null from here on: a drain candidate.

    // (2) Status derivation from the recorded attempts.
    if (_derivedCause(head, effective.maxAttempts) != null) {
      await _wedgeFromAttempts(
        registry,
        lock,
        destinationId: destinationId,
        maxAttempts: effective.maxAttempts,
        declared: declared,
      );
      return;
    }

    // (3) Halt honour. The read outside any transaction only decides
    // whether to try; the honouring transaction decides.
    final requested = await backend.transaction(
      (txn) => backend.readHaltRequestTxn(txn, destinationId),
    );
    await DeliveryTestHooks.current?.afterHaltLoopTopRead?.call(destinationId);
    if (requested != null) {
      final honour = await _honourHalt(
        registry,
        lock,
        destinationId: destinationId,
        requestEventId: requested.requestEventId,
        maxAttempts: effective.maxAttempts,
        declared: declared,
      );
      if (honour == null || honour == HaltHonour.honoured) return;
      continue;
    }

    // (4) Backoff: only the last attempt's timestamp matters; a head never
    // attempted is sent at once.
    if (head.attempts.isNotEmpty) {
      final backoff = effective.backoffFor(head.attempts.length);
      final nextAllowed = head.attempts.last.attemptedAt.add(backoff);
      if (now().isBefore(nextAllowed)) return;
    }

    // (5) Build the payload. Native `esd/batch@2` rows reconstruct bytes
    // from `envelopeMetadata` + `eventIds`-resolved events through
    // `BatchEnvelope.encode`, which JCS-canonicalizes the envelope so the
    // result is byte-identical across retries. Third-party rows (any other
    // wireFormat) carry the bytes as a stored JSON-Map `wirePayload`,
    // re-encoded to bytes verbatim for `Destination.send`.
    final WirePayload payload;
    final envelope = head.envelopeMetadata;
    if (envelope != null) {
      final events = <Map<String, Object?>>[];
      for (final eventId in head.eventIds) {
        final ev = await backend.findEventById(eventId);
        if (ev == null) {
          throw StateError(
            'native FIFO row ${head.entryId} references missing event '
            '$eventId; cannot reconstruct esd/batch@2 wire bytes',
          );
        }
        events.add(Map<String, Object?>.from(ev.toMap()));
      }
      final bytes = envelope.toEnvelope(events).encode();
      payload = WirePayload(
        bytes: bytes,
        contentType: BatchEnvelope.wireFormat,
        transformVersion: head.transformVersion,
      );
    } else {
      payload = WirePayload(
        bytes: Uint8List.fromList(utf8.encode(jsonEncode(head.wirePayload))),
        contentType: head.wireFormat,
        transformVersion: head.transformVersion,
      );
    }

    // (6) Pre-send fence. No await sits between its commit and the send.
    await DeliveryTestHooks.current?.beforeSendFence?.call(destinationId);
    final fenceAt = now();
    final proceed = await backend.transaction((txn) async {
      _observeFenceBodyRun(destinationId);
      await _lockCheck(lock, txn, destinationId);
      if (await backend.readHaltRequestTxn(txn, destinationId) != null) {
        return false;
      }
      final current = await backend.readFifoHeadTxn(txn, destinationId);
      if (current == null ||
          current.entryId != head.entryId ||
          current.finalStatus != null ||
          current.attempts.length != head.attempts.length) {
        return false;
      }
      await backend.writeSendFenceTxn(
        txn,
        destinationId,
        SendFence(
          entryId: head.entryId,
          attemptCount: head.attempts.length,
          at: fenceAt,
        ),
      );
      return true;
    });
    if (!proceed) continue;
    // A loss or close detected while the fence ran starts no send; nothing
    // is awaited between this check and the send.
    if (stopRequested?.call() ?? false) return;

    // (7) Send. A thrown error is categorized as SendTransient: both mean
    // "try again later". Its full diagnostic is recorded in the item's
    // attempts only.
    SendResult result;
    try {
      result = await destination.send(payload);
    } catch (error, stack) {
      result = SendTransient(error: 'uncaught exception: $error\n$stack');
    }
    await DeliveryTestHooks.current?.afterSendBeforeOutcome?.call(
      destinationId,
    );

    final attempt = _attemptFromResult(result, now());
    // head.attempts.length is the count before this attempt.
    final cause = switch (result) {
      SendOk() => null,
      SendPermanent() => WedgeCause.permanentRefusal,
      SendTransient() =>
        head.attempts.length + 1 >= effective.maxAttempts
            ? WedgeCause.retryBudgetExhausted
            : null,
    };

    if (cause != null) {
      // The attempt, the wedged status, the wedge event and the wedge
      // record commit together.
      try {
        final discarded = await registry.eventStore.runTransaction((
          txn,
          collector,
        ) async {
          await _lockCheck(lock, txn, destinationId);
          await backend.appendAttemptTxn(
            txn,
            destinationId,
            head.entryId,
            attempt,
          );
          final wedged = await registry.wedgeHeadInTxn(
            txn,
            collector,
            destinationId: destinationId,
            rowId: head.entryId,
            cause: cause,
            maxAttempts: effective.maxAttempts,
            drainerEpoch: lock.epoch,
            configuration: declared?.configuration,
            configurationFingerprint: declared?.fingerprint,
          );
          _injectOutcomeFailure(destinationId, attempt.outcome);
          return wedged.discardedHaltRequestEventId;
        });
        _logDiscardedHaltRequest(destinationId, discarded);
        _injectAfterWedgeTransaction(destinationId);
      } on DrainLockLostException {
        // The check is the transaction's first step: nothing committed,
        // and a drainer that lost the lock records nothing more.
        rethrow;
      } on TransactionRerunLimitException {
        // The handle cannot commit: nothing committed, and the delivery
        // cycle stops.
        rethrow;
      } on Object catch (e, st) {
        // Logged first, so the failure is on record whatever the fallback
        // does. A transaction that reports failure may still have
        // committed (a connection lost during the commit, or a failure
        // after it), so the fallback reads the head before it writes, and
        // before its lock check.
        libraryLog(
          'drain',
          'the transaction wedging the head of $destinationId reported '
              'failure',
          level: LibraryLogLevel.severe,
          error: e,
          stackTrace: st,
        );
        final attemptRecordedAlone = await backend.transaction((txn) async {
          final current = await backend.readFifoHeadTxn(txn, destinationId);
          if (current != null &&
              current.entryId == head.entryId &&
              current.finalStatus == FinalStatus.wedged) {
            final record = await backend.readWedgeRecordTxn(txn, destinationId);
            if (record?.rowId == head.entryId) return false;
          }
          await _lockCheck(lock, txn, destinationId);
          // Anything but the pending head is refused by the storage layer.
          await backend.appendAttemptTxn(
            txn,
            destinationId,
            head.entryId,
            attempt,
          );
          _injectOutcomeFailure(destinationId, attempt.outcome);
          return true;
        });
        libraryLog(
          'drain',
          attemptRecordedAlone
              ? 'the wedge of $destinationId did not commit; its attempt was '
                    'recorded alone and the pass ends'
              : 'the wedge of $destinationId committed before its failure '
                    'was reported; nothing more is recorded and the pass ends',
          level: LibraryLogLevel.warning,
        );
      }
      return;
    }

    // SendOk marks the head sent; a SendTransient below the budget records
    // the attempt alone and leaves the head pending (backoff applies on the
    // next pass).
    final sent = result is SendOk;
    await backend.transaction((txn) async {
      await _lockCheck(lock, txn, destinationId);
      await backend.appendAttemptTxn(txn, destinationId, head.entryId, attempt);
      if (sent) {
        await backend.setFinalStatusTxn(
          txn,
          destinationId,
          head.entryId,
          FinalStatus.sent,
        );
      }
      _injectOutcomeFailure(destinationId, attempt.outcome);
    });
    if (!sent) return;
  }
}

/// Honour the open halt request of [destinationId] for a destination the
/// draining process does not register, with no send: the status
/// derivation of step 2 of [drain] that needs no retry budget, then step 3.
/// A head whose last attempt reported a permanent failure is wedged with
/// cause [WedgeCause.permanentRefusal], a wedge that consumes the request;
/// otherwise the request is honoured with cause [WedgeCause.operatorHalt].
/// The wedge event takes the wire format and transform version from the
/// queue item and records `max_attempts` as null, since no retry budget is
/// in effect for the destination in this process. Returns without writing
/// when the queue has no pending head or no request is open.
///
/// Its transactions check [lock] first, as the drain's do.
@internal
Future<void> honourHaltById(
  String destinationId, {
  required DestinationRegistry registry,
  required DrainLock lock,
}) async {
  final backend = registry.backend;
  final head = await backend.readFifoHead(destinationId);
  if (head == null || head.finalStatus != null) return;
  final requested = await backend.transaction(
    (txn) => backend.readHaltRequestTxn(txn, destinationId),
  );
  await DeliveryTestHooks.current?.afterHaltLoopTopRead?.call(destinationId);
  if (requested == null) return;
  if (_derivedCause(head, null) != null) {
    await _wedgeFromAttempts(
      registry,
      lock,
      destinationId: destinationId,
      maxAttempts: null,
      declared: null,
    );
    return;
  }
  await _honourHalt(
    registry,
    lock,
    destinationId: destinationId,
    requestEventId: requested.requestEventId,
    maxAttempts: null,
    declared: null,
  );
}

/// Runs the transaction that honours [requestEventId] for [destinationId]
/// and logs what needs logging. Returns what it did, or null when the
/// transaction reported failure (logged; whether it committed is not known,
/// and the next pass reads the head again before any send).
Future<HaltHonour?> _honourHalt(
  DestinationRegistry registry,
  DrainLock lock, {
  required String destinationId,
  required String requestEventId,
  required int? maxAttempts,
  required DrainerConfiguration? declared,
}) async {
  final HaltHonour honour;
  try {
    honour = await registry.eventStore.runTransaction((txn, collector) async {
      await _lockCheck(lock, txn, destinationId);
      return registry.honourHaltInTxn(
        txn,
        collector,
        destinationId: destinationId,
        requestEventId: requestEventId,
        maxAttempts: maxAttempts,
        drainerEpoch: lock.epoch,
        configuration: declared?.configuration,
        configurationFingerprint: declared?.fingerprint,
      );
    });
    if (honour == HaltHonour.honoured) {
      _injectAfterWedgeTransaction(destinationId);
    }
  } on DrainLockLostException {
    rethrow;
  } on TransactionRerunLimitException {
    // The handle cannot commit: the delivery cycle stops.
    rethrow;
  } on Object catch (e, st) {
    libraryLog(
      'drain',
      'honouring the halt request $requestEventId of $destinationId reported '
          'failure; the pass ends',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
    return null;
  }
  if (honour == HaltHonour.unverified) {
    libraryLog(
      'drain',
      'the stored halt request of $destinationId cites $requestEventId, '
          'which is no halt request of this destination and database in the '
          'log; the stored request is removed, nothing is wedged, and '
          'delivery continues',
      level: LibraryLogLevel.severe,
    );
  }
  return honour;
}

/// Logs, after the wedge of [destinationId] committed, the unverifiable
/// stored halt request that wedge removed without recording it, if any.
void _logDiscardedHaltRequest(String destinationId, String? requestEventId) {
  if (requestEventId == null) return;
  libraryLog(
    'drain',
    'the stored halt request of $destinationId cited $requestEventId, which '
        'is no halt request of this destination and database in the log; the '
        'wedge removed it and does not record it',
    level: LibraryLogLevel.severe,
  );
}

/// Reports a run of the pre-send fence body to the `onFenceBodyRun` test
/// seam; an exception the seam throws is logged and does not reach the
/// drainer.
void _observeFenceBodyRun(String destinationId) {
  final seam = DeliveryTestHooks.current?.onFenceBodyRun;
  if (seam == null) return;
  try {
    seam(destinationId);
  } on Object catch (e, st) {
    libraryLog(
      'drain',
      'the onFenceBodyRun test seam threw',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
  }
}

/// Wedges [destinationId]'s pending head for the cause its recorded
/// attempts call for under [maxAttempts] (null: no budget in effect, so
/// only a permanent refusal), in its own transaction, without a send. The
/// transaction reads the head again and decides again before it wedges:
/// with one drainer the head cannot change between the two reads, so the
/// second decision is defence in depth. A failure is logged; whether the
/// transaction committed is not known, and the next pass reads the head
/// again before any send.
Future<void> _wedgeFromAttempts(
  DestinationRegistry registry,
  DrainLock lock, {
  required String destinationId,
  required int? maxAttempts,
  required DrainerConfiguration? declared,
}) async {
  final backend = registry.backend;
  try {
    final discarded = await registry.eventStore.runTransaction((
      txn,
      collector,
    ) async {
      await _lockCheck(lock, txn, destinationId);
      final current = await backend.readFifoHeadTxn(txn, destinationId);
      if (current == null || current.finalStatus != null) return null;
      final cause = _derivedCause(current, maxAttempts);
      if (cause == null) return null;
      final wedged = await registry.wedgeHeadInTxn(
        txn,
        collector,
        destinationId: destinationId,
        rowId: current.entryId,
        cause: cause,
        maxAttempts: maxAttempts,
        drainerEpoch: lock.epoch,
        configuration: declared?.configuration,
        configurationFingerprint: declared?.fingerprint,
      );
      return wedged.discardedHaltRequestEventId;
    });
    _logDiscardedHaltRequest(destinationId, discarded);
    _injectAfterWedgeTransaction(destinationId);
  } on DrainLockLostException {
    rethrow;
  } on TransactionRerunLimitException {
    // The handle cannot commit: the delivery cycle stops.
    rethrow;
  } on Object catch (e, st) {
    libraryLog(
      'drain',
      'wedging the head of $destinationId from its recorded attempts '
          'reported failure; the pass ends',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
  }
}

/// The cause for which [head]'s recorded attempts call for a wedge under
/// [maxAttempts], or null when they leave it pending. With no budget in
/// effect ([maxAttempts] null) only a permanent refusal is derived.
WedgeCause? _derivedCause(FifoEntry head, int? maxAttempts) {
  if (head.attempts.isNotEmpty && head.attempts.last.outcome == 'permanent') {
    return WedgeCause.permanentRefusal;
  }
  if (maxAttempts != null && head.attempts.length >= maxAttempts) {
    return WedgeCause.retryBudgetExhausted;
  }
  return null;
}

/// Refuses a retry budget below one: under it every pending head would
/// wedge before its first send, recorded as an exhausted budget with no
/// attempt behind it.
@internal
void checkRetryBudget(SyncPolicy policy) {
  if (policy.maxAttempts < 1) {
    throw ArgumentError.value(
      policy.maxAttempts,
      'maxAttempts',
      'the retry budget must be at least one attempt',
    );
  }
}

/// Consults the `afterWedgeTransaction` test seam after the wedge
/// transaction committed.
void _injectAfterWedgeTransaction(String destinationId) {
  if (DeliveryTestHooks.current?.afterWedgeTransaction?.call(destinationId) ??
      false) {
    throw InjectedFailure('after the wedge transaction of $destinationId');
  }
}

/// Consults the `failOutcomeTransaction` test seam after an outcome
/// transaction's writes.
void _injectOutcomeFailure(String destinationId, String outcome) {
  if (DeliveryTestHooks.current?.failOutcomeTransaction?.call(
        destinationId,
        outcome,
      ) ??
      false) {
    throw InjectedFailure('drain outcome transaction of $destinationId');
  }
}

AttemptResult _attemptFromResult(SendResult result, DateTime attemptedAt) {
  switch (result) {
    case SendOk():
      return AttemptResult(attemptedAt: attemptedAt, outcome: 'ok');
    case SendTransient(:final error, :final httpStatus):
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'transient',
        errorMessage: error,
        httpStatus: httpStatus,
      );
    case SendPermanent(:final error):
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'permanent',
        errorMessage: error,
      );
  }
}
