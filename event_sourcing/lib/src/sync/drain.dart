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
// (a failed attempt below the retry
//   budget commits the attempt alone, leaving the row pending at the head
//   of its queue)
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
// Implements: EVS-DEV-destination-drain/Z
// (a pending head marked transform-failed is
//   wedged with cause transform_failed, without a send, before any halt
//   honour or backoff check)

part of '../event_store.dart';

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
///   retry budget in effect (backoff applies on the next pass); or
/// - a transaction that decides a wedge reported failure (logged; the next
///   pass reads the head again and derives its status before any send).
///
/// Each iteration runs these steps in this order:
///
/// 1. Read the head; none, or a wedged head, ends the pass.
/// 2. Derive the status the head's recorded attempts call for: a last
///    attempt that reported a permanent failure wedges it with cause
///    [WedgeCause.permanentRefusal]; an attempt count at or above the
///    budget in effect, or recorded attempts whose capped gaps have spent
///    the time bound (`EVS-DEV-destination-retry-budget/A`), wedges it
///    with cause [WedgeCause.retryBudgetExhausted] (covering a budget
///    lowered since the attempts were recorded). The wedge commits in its
///    own transaction, without a send, and ends the pass.
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
/// For a destination that serializes natively, step 5 numbers the delivery
/// one above the sender channel record and links it to that record's hash,
/// the fence of step 6 proceeds only while the record is unchanged and
/// writes the delivery's number and hash in the send fence record, and the
/// receiver's record returned with its answer decides the outcome (see
/// [_readReceiverAnswer]): the head is marked `sent` only on a record
/// naming its delivery, and a [SendOk] (an acceptance carrying no record)
/// wedges it with cause [WedgeCause.acknowledgementInvalid]. A resume or a
/// new generation of the channel ends the pass.
///
/// On [SendOk] the head is marked `sent` and the loop advances. On
/// [SendPermanent], or a [SendTransient] whose attempt reaches the budget,
/// one event-store transaction records the attempt, marks the head wedged,
/// consumes any open halt request, appends the wedge event and writes the
/// destination's wedge record (`DestinationRegistry._wedgeHeadInTxn`). When that transaction reports
/// failure, the failure is logged and a second transaction reads the head
/// again: a pending head (the wedge rolled back) gets the attempt alone,
/// and step 2 of a later pass wedges it before any further send; a head
/// the wedge record names as wedged (the wedge committed before the
/// failure was reported) gets nothing more. The pass then ends. Items
/// behind a wedged head are never attempted.
///
/// [policy] is an optional [SyncPolicy] override; when null, the drain
/// falls back to [SyncPolicy.defaults]. Its `maxAttempts` and
/// `maxRetryTime` are the budget in effect, recorded in every wedge event
/// as `max_attempts` and `max_retry_ms`. A budget whose attempt bound is
/// below one or whose time bound is negative is refused with an
/// [ArgumentError] before anything is read or sent. [cadence] is the
/// delivery cycle's cadence, added to the retry curve's longest allowed
/// delay to cap each gap the time bound counts
/// (`EVS-DEV-destination-retry-budget/A`).
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
  required Duration cadence,
  Clock? clock,
  SyncPolicy? policy,
  DrainerConfiguration? declared,
  bool Function()? stopRequested,
}) async {
  final backend = registry._backend;
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

    // (1b) A transform-failed head wedges immediately, without a send,
    // before any halt honour, status derivation from send attempts, or
    // backoff check: the item was never sent, so no attempt-based status
    // applies to it.
    if (head.transformFailed) {
      await _wedgeTransformFailed(
        registry,
        lock,
        destinationId: destinationId,
        policy: effective,
        declared: declared,
      );
      return;
    }

    // (2) Status derivation from the recorded attempts.
    if (_derivedCause(head, effective, cadence) != null) {
      await _wedgeFromAttempts(
        registry,
        lock,
        destinationId: destinationId,
        policy: effective,
        cadence: cadence,
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
        policy: effective,
        cadence: cadence,
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

    // (5) Build the payload. A destination that serializes natively sends
    // a delivery on its channel, numbered from the sender channel record.
    // Any other destination's row carries the bytes as a stored JSON-Map
    // `wirePayload`, re-encoded to bytes verbatim for `Destination.send`.
    final WirePayload payload;
    _Delivery? delivery;
    if (destination.serializesNatively) {
      delivery = await _buildDelivery(backend, destinationId, head);
      payload = delivery.payload;
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
      // Implements: EVS-DEV-delivery-channel/H
      // a delivery is sent only when the sender channel record the fence
      //   reads equals the one its number, link and hash were built from.
      // Implements: EVS-DEV-delivery-channel/G
      // the delivery's number (the record's plus one) and link (the
      //   record's hash) are those of the record read in the fence.
      if (delivery != null &&
          await backend.readSenderChannelRecordTxn(txn, destinationId) !=
              delivery.builtFrom) {
        return false;
      }
      // Implements: EVS-DEV-delivery-channel/I
      // the send fence record names the number and hash of the delivery in
      //   flight.
      await backend.writeSendFenceTxn(
        txn,
        destinationId,
        SendFence(
          entryId: head.entryId,
          attemptCount: head.attempts.length,
          at: fenceAt,
          deliveryNumber: delivery?.number,
          deliveryHash: delivery?.hash,
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

    // A send outcome stating that delivery was not attempted records
    // nothing: the head stays exactly as it was, and this destination's
    // pass ends. On a delivery channel nothing is written beyond the
    // fence already committed in step 6, so the next fence recomputes the
    // same number and link and the channel's numbering has no gap.
    // Implements: EVS-DEV-destination-retry-budget/C
    // Implements: EVS-DEV-destination-retry-budget/D
    if (result is SendNotAttempted) {
      return;
    }

    final attempt = _attemptFromResult(result, now(), delivery);

    // A receiver's answer on a delivery channel carries its record of the
    // channel, which decides the outcome.
    if (delivery != null && result is SendAnswered) {
      final advance = await _readReceiverAnswer(
        registry,
        lock,
        destinationId: destinationId,
        head: head,
        delivery: delivery,
        response: result.response,
        attempt: attempt,
        policy: effective,
        cadence: cadence,
        declared: declared,
      );
      if (advance) continue;
      return;
    }

    // head.attempts.length is the count before this attempt.
    // A receiver's answer to a destination that is no delivery channel is
    // recorded as a transient attempt.
    final cause = switch (result) {
      // Implements: EVS-DEV-delivery-channel/Q
      // on a delivery channel, an accepting outcome that carries no record
      //   wedges the head with cause acknowledgement_invalid, in the
      //   transaction that records the attempt.
      // Implements: EVS-PRD-destinations/Q
      // the wedge event records an acceptance that carries no receiver
      //   record as its cause.
      SendOk() => delivery != null ? WedgeCause.acknowledgementInvalid : null,
      SendPermanent() => WedgeCause.permanentRefusal,
      // Unreachable: drain() returns above whenever result is
      // SendNotAttempted, before attempt is built.
      SendNotAttempted() => throw StateError(
        'a not-attempted send outcome never reaches the wedge-cause switch',
      ),
      SendTransient() || SendAnswered() =>
        budgetSpent(
              <AttemptResult>[...head.attempts, attempt],
              effective,
              cadence,
            )
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
          final wedged = await registry._wedgeHeadInTxn(
            txn,
            collector,
            destinationId: destinationId,
            rowId: head.entryId,
            cause: cause,
            maxAttempts: effective.maxAttempts,
            maxRetryMs: effective.maxRetryTime.inMilliseconds,
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
/// queue item and records `max_attempts` and `max_retry_ms` as null, since
/// no retry budget is in effect for the destination in this process.
/// Returns without writing when the queue has no pending head or no
/// request is open.
///
/// Its transactions check [lock] first, as the drain's do.
@internal
Future<void> honourHaltById(
  String destinationId, {
  required DestinationRegistry registry,
  required DrainLock lock,
}) async {
  final backend = registry._backend;
  final head = await backend.readFifoHead(destinationId);
  if (head == null || head.finalStatus != null) return;
  final requested = await backend.transaction(
    (txn) => backend.readHaltRequestTxn(txn, destinationId),
  );
  await DeliveryTestHooks.current?.afterHaltLoopTopRead?.call(destinationId);
  if (requested == null) return;
  if (_derivedCause(head, null, Duration.zero) != null) {
    await _wedgeFromAttempts(
      registry,
      lock,
      destinationId: destinationId,
      policy: null,
      cadence: Duration.zero,
      declared: null,
    );
    return;
  }
  await _honourHalt(
    registry,
    lock,
    destinationId: destinationId,
    requestEventId: requested.requestEventId,
    policy: null,
    cadence: Duration.zero,
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
  required SyncPolicy? policy,
  required Duration cadence,
  required DrainerConfiguration? declared,
}) async {
  final HaltHonour honour;
  try {
    honour = await registry.eventStore.runTransaction((txn, collector) async {
      await _lockCheck(lock, txn, destinationId);
      return registry._honourHaltInTxn(
        txn,
        collector,
        destinationId: destinationId,
        requestEventId: requestEventId,
        maxAttempts: policy?.maxAttempts,
        maxRetryMs: policy?.maxRetryTime.inMilliseconds,
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
/// attempts call for under [policy] and [cadence] (a null [policy]: no
/// budget in effect, so only a permanent refusal), in its own transaction,
/// without a send. The transaction reads the head again and decides again
/// before it wedges: with one drainer the head cannot change between the
/// two reads, so the second decision is defence in depth. A failure is
/// logged; whether the transaction committed is not known, and the next
/// pass reads the head again before any send.
Future<void> _wedgeFromAttempts(
  DestinationRegistry registry,
  DrainLock lock, {
  required String destinationId,
  required SyncPolicy? policy,
  required Duration cadence,
  required DrainerConfiguration? declared,
}) async {
  final backend = registry._backend;
  try {
    final discarded = await registry.eventStore.runTransaction((
      txn,
      collector,
    ) async {
      await _lockCheck(lock, txn, destinationId);
      final current = await backend.readFifoHeadTxn(txn, destinationId);
      if (current == null || current.finalStatus != null) return null;
      final cause = _derivedCause(current, policy, cadence);
      if (cause == null) return null;
      final wedged = await registry._wedgeHeadInTxn(
        txn,
        collector,
        destinationId: destinationId,
        rowId: current.entryId,
        cause: cause,
        maxAttempts: policy?.maxAttempts,
        maxRetryMs: policy?.maxRetryTime.inMilliseconds,
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

/// Wedges [destinationId]'s pending, transform-failed head with cause
/// [WedgeCause.transformFailed], in its own transaction, without a send.
/// The transaction reads the head again and decides again before it
/// wedges: with one drainer the head cannot change between the two reads,
/// so the second decision is defence in depth. Consumes an open halt
/// request like any other wedge (`EVS-DEV-destination-drain/O`): the
/// request is not honoured with cause `operator_halt` for a transform-
/// failed head. A failure is logged; whether the transaction committed
/// is not known, and the next pass reads the head again before any send.
// Implements: EVS-DEV-destination-drain/Z
// the wedge runs in its own transaction, without a send, consuming an
//   open halt request the way any other wedge does.
Future<void> _wedgeTransformFailed(
  DestinationRegistry registry,
  DrainLock lock, {
  required String destinationId,
  required SyncPolicy policy,
  required DrainerConfiguration? declared,
}) async {
  final backend = registry._backend;
  try {
    final discarded = await registry.eventStore.runTransaction((
      txn,
      collector,
    ) async {
      await _lockCheck(lock, txn, destinationId);
      final current = await backend.readFifoHeadTxn(txn, destinationId);
      if (current == null ||
          current.finalStatus != null ||
          !current.transformFailed) {
        return null;
      }
      final wedged = await registry._wedgeHeadInTxn(
        txn,
        collector,
        destinationId: destinationId,
        rowId: current.entryId,
        cause: WedgeCause.transformFailed,
        maxAttempts: policy.maxAttempts,
        maxRetryMs: policy.maxRetryTime.inMilliseconds,
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
      'wedging the transform-failed head of $destinationId reported '
          'failure; the pass ends',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
  }
}

/// The cause for which [head]'s recorded attempts call for a wedge under
/// [policy] and [cadence], or null when they leave it pending. With no
/// budget in effect ([policy] null) only a permanent refusal is derived.
WedgeCause? _derivedCause(
  FifoEntry head,
  SyncPolicy? policy,
  Duration cadence,
) {
  if (head.attempts.isNotEmpty && head.attempts.last.outcome == 'permanent') {
    return WedgeCause.permanentRefusal;
  }
  if (policy != null && budgetSpent(head.attempts, policy, cadence)) {
    return WedgeCause.retryBudgetExhausted;
  }
  return null;
}

/// Whether [attempts]' retry budget counts as spent under [policy] and the
/// delivery cycle's [cadence]: the attempt bound is reached, or the sum,
/// over each two consecutive recorded attempts, of the time between them
/// (each gap capped at the retry curve's longest allowed delay after the
/// earlier of the two, plus [cadence]), reaches the time bound.
///
/// The cap means a gap longer than the drainer would itself have waited —
/// a declined pause, a sleeping device, or a stopped drainer — spends at
/// most one capped gap, never the wall-clock time it actually lasted.
///
/// With no recorded attempt the time bound is never reached, whatever its
/// value: a zero time bound is accepted (`checkRetryBudget` refuses only a
/// negative one), and it wedges only once an attempt has actually held the
/// item at the head, never before the first send.
// Implements: EVS-DEV-destination-retry-budget/A
// Implements: EVS-PRD-destinations/W
@internal
bool budgetSpent(
  List<AttemptResult> attempts,
  SyncPolicy policy,
  Duration cadence,
) => retryBudgetSpentAt(
  <DateTime>[for (final a in attempts) a.attemptedAt],
  policy,
  cadence,
);

/// Whether the retry budget counts as spent given [times] (the recorded
/// times of an item's send attempts, or of a transform's failures), oldest
/// first: the count reaches [SyncPolicy.maxAttempts], or the sum, over each
/// two consecutive times, of the gap between them (each capped at the
/// retry curve's longest allowed delay after the earlier one, plus
/// [cadence]), reaches [SyncPolicy.maxRetryTime]. [budgetSpent] applies
/// this to a queue item's recorded attempts; the fill applies it to a
/// transform failure record's recorded failure times.
// Implements: EVS-DEV-destination-retry-budget/A
// Implements: EVS-DEV-destination-retry-budget/B
@internal
bool retryBudgetSpentAt(
  List<DateTime> times,
  SyncPolicy policy,
  Duration cadence,
) {
  if (times.length >= policy.maxAttempts) return true;
  if (times.isEmpty) return false;
  var spent = Duration.zero;
  for (var i = 1; i < times.length; i++) {
    final gap = times[i].difference(times[i - 1]);
    final cap = policy.longestDelayAfter(i) + cadence;
    spent += gap < cap ? gap : cap;
  }
  return spent >= policy.maxRetryTime;
}

/// The reason [policy]'s retry budget is unusable — its attempt bound is
/// below one, or its time bound is negative — or `null` when the budget is
/// usable. Under either fault, sending anything under the budget would be
/// meaningless (every pending head would wedge before its first send,
/// recorded as an exhausted budget with no attempt behind it, or against a
/// time bound that can never be reached). A caller that only needs to
/// decide whether to proceed (a resolved policy, checked once per pass)
/// reads this without paying for stack-trace-carrying control flow; one
/// that treats an unusable budget as a programming error calls
/// [checkRetryBudget] instead.
///
// Implements: EVS-DEV-destination-drain/J
@internal
String? retryBudgetRefusalReason(SyncPolicy policy) {
  if (policy.maxAttempts < 1) {
    return 'the retry budget must be at least one attempt';
  }
  if (policy.maxRetryTime.isNegative) {
    return "the retry budget's time bound must not be negative";
  }
  return null;
}

/// Throws [ArgumentError] for the reason [retryBudgetRefusalReason]
/// reports, or returns normally when [policy]'s budget is usable.
///
// Implements: EVS-DEV-destination-drain/J
@internal
void checkRetryBudget(SyncPolicy policy) {
  final reason = retryBudgetRefusalReason(policy);
  if (reason == null) return;
  if (policy.maxAttempts < 1) {
    throw ArgumentError.value(policy.maxAttempts, 'maxAttempts', reason);
  }
  throw ArgumentError.value(policy.maxRetryTime, 'maxRetryTime', reason);
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

AttemptResult _attemptFromResult(
  SendResult result,
  DateTime attemptedAt,
  _Delivery? delivery,
) {
  // Implements: EVS-DEV-delivery-channel/I
  // the attempt a send of a delivery produces records its number and hash.
  final number = delivery?.number;
  final hash = delivery?.hash;
  switch (result) {
    case SendOk():
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'ok',
        deliveryNumber: number,
        deliveryHash: hash,
      );
    case SendTransient(:final error, :final httpStatus):
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'transient',
        errorMessage: error,
        httpStatus: httpStatus,
        deliveryNumber: number,
        deliveryHash: hash,
      );
    case SendPermanent(:final error):
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'permanent',
        errorMessage: error,
        deliveryNumber: number,
        deliveryHash: hash,
      );
    case SendAnswered(:final response):
      return AttemptResult(
        attemptedAt: attemptedAt,
        outcome: 'transient',
        errorMessage: 'receiver answered: $response',
        deliveryNumber: number,
        deliveryHash: hash,
      );
    case SendNotAttempted():
      // drain() returns before this call whenever result is
      // SendNotAttempted (EVS-DEV-destination-retry-budget/C): no
      // attempt is ever built for it.
      throw StateError(
        'a not-attempted send outcome records no attempt and is never '
        'turned into one',
      );
  }
}

// ---------------------------------------------------------------------------
// Delivery channels
// ---------------------------------------------------------------------------

/// A delivery the drainer built for the head of a delivery channel: its
/// number, link and hash, from the sender channel record [builtFrom].
final class _Delivery {
  const _Delivery({
    required this.builtFrom,
    required this.channel,
    required this.number,
    required this.hash,
    required this.payload,
  });

  /// The sender channel record the delivery was numbered from.
  final SenderChannelRecord builtFrom;

  /// The channel the delivery is sent on.
  final DeliveryChannel channel;

  /// The delivery number: [builtFrom]'s number plus one.
  final int number;

  /// The delivery hash.
  final String hash;

  /// The `esd/batch@3` bytes handed to the destination.
  final WirePayload payload;
}

/// Builds the delivery that carries [head] on its channel: numbered one
/// above the sender channel record read now, linked to that record's hash,
/// with the item's envelope fields, channel and attributes and the events
/// the item names, as stored. Every read of the item's events happens here.
Future<_Delivery> _buildDelivery(
  StorageBackend backend,
  String destinationId,
  FifoEntry head,
) async {
  final metadata = head.envelopeMetadata;
  final channel = metadata?.channel;
  final attributes = metadata?.attributes;
  if (metadata == null || channel == null || attributes == null) {
    throw StateError(
      'queue item ${head.entryId} of $destinationId, a destination that '
      'serializes natively, carries no delivery channel',
    );
  }
  final record = await backend.transaction(
    (txn) => backend.readSenderChannelRecordTxn(txn, destinationId),
  );
  if (record == null) {
    throw StateError(
      '$destinationId serializes natively and has no sender channel record',
    );
  }
  if (record.generation != channel.generation) {
    throw StateError(
      'queue item ${head.entryId} of $destinationId is on generation '
      '${channel.generation}; the sender channel record is on generation '
      '${record.generation}',
    );
  }
  final events = <Map<String, Object?>>[];
  for (final eventId in head.eventIds) {
    final ev = await backend.findEventById(eventId);
    if (ev == null) {
      throw StateError(
        'queue item ${head.entryId} of $destinationId references missing '
        'event $eventId; cannot build its delivery',
      );
    }
    events.add(Map<String, Object?>.from(ev.toMap()));
  }
  final sealed = DeliveryEnvelope.seal(
    batchId: metadata.batchId,
    senderHop: metadata.senderHop,
    senderIdentifier: metadata.senderIdentifier,
    senderSoftwareVersion: metadata.senderSoftwareVersion,
    sentAt: metadata.sentAt,
    channel: channel,
    deliveryNumber: record.receiverRecord.deliveryNumber + 1,
    previousDeliveryHash: record.receiverRecord.deliveryHash,
    events: events,
    attributes: attributes,
  );
  return _Delivery(
    builtFrom: record,
    channel: channel,
    number: sealed.deliveryNumber,
    hash: sealed.deliveryHash,
    payload: WirePayload(
      bytes: sealed.encode(),
      contentType: DeliveryEnvelope.wireFormat,
      transformVersion: head.transformVersion,
    ),
  );
}

/// How the drainer reads a receiver record returned for a delivery.
enum _Reading {
  /// The record names the delivery in flight at the next number: the head
  /// is marked sent under it.
  acknowledged,

  /// The record names, at the next number, another delivery the sender
  /// attempted: the sender adopts it and marks nothing sent.
  adopted,

  /// The record equals the sender channel record.
  inStep,

  /// The receiver is behind and every delivery it lacks is retained.
  receiverBehind,

  /// The record is ahead and names no delivery the sender attempted.
  senderRegressed,

  /// No automatic path explains the record, or another receiver answered.
  unexplained,

  /// The answer names another channel than the delivery's; it says nothing
  /// of this channel and is a transient failure.
  otherChannel,
}

/// Reads [response], the receiver's answer to [delivery], the delivery of
/// [head], and commits what it calls for with [attempt] in one transaction
/// that checks [lock] first. Returns true when the head was marked sent
/// (the drain goes on to the next item), false when the pass ends.
///
/// In this order:
///
/// - An answer naming another channel is a transient failure.
/// - A response from another receiver database than the one the sender
///   channel record holds, when it holds one, starts a new generation with
///   a `channel_unexplained` finding.
/// - A record numbered one above the sender channel record that names a
///   delivery the sender attempted (the one the send fence record names,
///   or one an attempt on a pending, wedged or tombstoned item carries)
///   becomes the sender channel record, with the responding receiver, and
///   marks the head sent under it when it names the delivery in flight.
/// - A record equal to the sender channel record changes nothing.
/// - A record below the sender channel record, every delivery above it
///   retained and the first linking to its hash, resumes the channel.
/// - A record above the sender channel record naming no delivery the
///   sender attempted starts a new generation with a `sender_regressed`
///   finding.
/// - Every other record starts a new generation with a
///   `channel_unexplained` finding.
///
/// The attempt is recorded on the head first; an attempt that leaves the
/// head pending and spends the retry budget wedges it in the same
/// transaction.
// Implements: EVS-DEV-delivery-channel/N
// an accepting outcome carrying another record, and an out_of_sequence
//   refusal, are read here as the receiver's record, never as a permanent
//   failure.
Future<bool> _readReceiverAnswer(
  DestinationRegistry registry,
  DrainLock lock, {
  required String destinationId,
  required FifoEntry head,
  required _Delivery delivery,
  required ReceiverResponse response,
  required AttemptResult attempt,
  required SyncPolicy policy,
  required Duration cadence,
  required DrainerConfiguration? declared,
}) async {
  final backend = registry._backend;
  final discarded = await registry.eventStore.runTransaction((
    txn,
    collector,
  ) async {
    await _lockCheck(lock, txn, destinationId);
    final sender = await backend.readSenderChannelRecordTxn(txn, destinationId);
    if (sender == null) {
      throw StateError(
        '$destinationId serializes natively and has no sender channel record',
      );
    }
    final fence = await backend.readSendFenceTxn(txn, destinationId);
    final items = await backend.listFifoEntriesTxn(txn, destinationId);
    final record = response.record;
    final responding = response.receiverDatabaseId;
    final reading = response.channel != delivery.channel
        ? _Reading.otherChannel
        : await _readRecord(
            backend,
            txn,
            destinationId: destinationId,
            headEntryId: head.entryId,
            sender: sender,
            record: record,
            responding: responding,
            fence: fence,
            items: items,
          );
    final acknowledged = reading == _Reading.acknowledged;
    final recorded = AttemptResult(
      attemptedAt: attempt.attemptedAt,
      outcome: acknowledged ? 'ok' : 'transient',
      errorMessage: acknowledged
          ? null
          : 'receiver ${response.receiverDatabaseId} answered with record '
                '${record.deliveryNumber}: ${reading.name}',
      deliveryNumber: attempt.deliveryNumber,
      deliveryHash: attempt.deliveryHash,
    );
    await backend.appendAttemptTxn(txn, destinationId, head.entryId, recorded);
    switch (reading) {
      case _Reading.acknowledged:
      case _Reading.adopted:
        // Implements: EVS-DEV-delivery-resume/H
        // a record at the next number naming a delivery the sender
        //   attempted becomes the sender channel record, with the responding
        //   receiver, and marks the pending head sent only when it names the
        //   delivery the send fence record names.
        // Implements: EVS-DEV-delivery-channel/M
        // the head is marked sent only on a record whose number and hash are
        //   those of the delivery it sent.
        // Implements: EVS-PRD-delivery-channel/F
        // a queued item is marked delivered only on a receiver record naming
        //   the delivery the sender made of it.
        if (acknowledged) {
          await backend.markSentTxn(
            txn,
            destinationId,
            head.entryId,
            generation: sender.generation,
            deliveryNumber: record.deliveryNumber,
            deliveryHash: record.deliveryHash!,
          );
        }
        await backend.writeSenderChannelRecordTxn(
          txn,
          destinationId,
          SenderChannelRecord(
            generation: sender.generation,
            receiverRecord: record,
            receiverDatabaseId: responding,
          ),
        );
      case _Reading.inStep:
      case _Reading.otherChannel:
        break;
      case _Reading.receiverBehind:
        await _resumeInTxn(
          registry,
          txn,
          collector,
          lock: lock,
          destinationId: destinationId,
          sender: sender,
          record: record,
          responding: responding,
          items: await backend.listFifoEntriesTxn(txn, destinationId),
        );
        return null;
      case _Reading.senderRegressed:
      case _Reading.unexplained:
        await _newGenerationInTxn(
          registry,
          txn,
          collector,
          destinationId: destinationId,
          channel: delivery.channel,
          sender: sender,
          record: record,
          responding: responding,
          kind: reading == _Reading.senderRegressed
              ? FindingKind.senderRegressed
              : FindingKind.channelUnexplained,
          items: await backend.listFifoEntriesTxn(txn, destinationId),
        );
        return null;
    }
    if (acknowledged ||
        !budgetSpent(
          <AttemptResult>[...head.attempts, recorded],
          policy,
          cadence,
        )) {
      return null;
    }
    // The attempt leaves the head pending and spends the budget.
    final wedged = await registry._wedgeHeadInTxn(
      txn,
      collector,
      destinationId: destinationId,
      rowId: head.entryId,
      cause: WedgeCause.retryBudgetExhausted,
      maxAttempts: policy.maxAttempts,
      maxRetryMs: policy.maxRetryTime.inMilliseconds,
      drainerEpoch: lock.epoch,
      configuration: declared?.configuration,
      configurationFingerprint: declared?.fingerprint,
    );
    return wedged.discardedHaltRequestEventId;
  });
  _logDiscardedHaltRequest(destinationId, discarded);
  final after = await backend.readFifoRow(destinationId, head.entryId);
  return after?.finalStatus == FinalStatus.sent;
}

/// Reads [record], returned by [responding] on the channel of [sender],
/// against the sender's own records: the sender channel record, the send
/// fence record [fence] and the attempts the registration's queue [items]
/// carry.
// Implements: EVS-DEV-delivery-resume/Y
// a response from another receiver database than the one the sender channel
//   record holds, and a record that is not the sender's, calls for no
//   resume, and is not above it or is more than one above it naming an
//   attempted delivery, is unexplained.
// Implements: EVS-DEV-delivery-resume/K
// a record above the sender channel record naming no delivery the sender
//   attempted is a sender regression.
// Implements: EVS-PRD-delivery-channel/I
// a record from the channel's receiver database ahead of the sender's that
//   names no delivery the sender attempted is recorded as a sender
//   regression.
Future<_Reading> _readRecord(
  StorageBackend backend,
  Transaction txn, {
  required String destinationId,
  required String headEntryId,
  required SenderChannelRecord sender,
  required DeliveryRecord record,
  required String responding,
  required SendFence? fence,
  required List<FifoEntry> items,
}) async {
  final held = sender.receiverDatabaseId;
  if (held != null && held != responding) return _Reading.unexplained;
  final own = sender.receiverRecord;
  final attempted = _namesAttempted(record, fence, items);
  if (record.deliveryNumber == own.deliveryNumber + 1 && attempted) {
    return fence != null &&
            fence.entryId == headEntryId &&
            fence.deliveryNumber == record.deliveryNumber &&
            fence.deliveryHash == record.deliveryHash
        ? _Reading.acknowledged
        : _Reading.adopted;
  }
  if (record == own) return _Reading.inStep;
  if (record.deliveryNumber < own.deliveryNumber &&
      await _resendable(
        backend,
        txn,
        destinationId: destinationId,
        sender: sender,
        record: record,
      )) {
    return _Reading.receiverBehind;
  }
  if (record.deliveryNumber > own.deliveryNumber && !attempted) {
    return _Reading.senderRegressed;
  }
  return _Reading.unexplained;
}

/// Whether [record] names a delivery the sender attempted: the one the send
/// fence record names, or one an attempt recorded on a pending, wedged or
/// tombstoned item of the queue carries. The delivery hash covers the
/// channel, so a match names a delivery of the same registration and
/// generation.
bool _namesAttempted(
  DeliveryRecord record,
  SendFence? fence,
  List<FifoEntry> items,
) {
  final number = record.deliveryNumber;
  final hash = record.deliveryHash;
  if (number == 0 || hash == null) return false;
  if (fence != null &&
      fence.deliveryNumber == number &&
      fence.deliveryHash == hash) {
    return true;
  }
  for (final item in items) {
    if (item.finalStatus == FinalStatus.sent) continue;
    for (final a in item.attempts) {
      if (a.deliveryNumber == number && a.deliveryHash == hash) return true;
    }
  }
  return false;
}

/// Whether the sender retains a delivery at every number above [record] up
/// to [sender]'s, and the one numbered one above [record] links to its hash:
/// its hash recomputes from its channel, number, events and attributes with
/// [record]'s hash as its link.
// Implements: EVS-DEV-delivery-resume/I
// the channel resumes as a receiver behind when every number above the
//   record up to the sender's is retained and the first links to the
//   record's hash.
// Implements: EVS-DEV-delivery-resume/W
// the retained delivery at a number is the one the queue last marked sent
//   there under the current generation; a number without one is not
//   retained.
Future<bool> _resendable(
  StorageBackend backend,
  Transaction txn, {
  required String destinationId,
  required SenderChannelRecord sender,
  required DeliveryRecord record,
}) async {
  for (
    var n = record.deliveryNumber + 1;
    n <= sender.receiverRecord.deliveryNumber;
    n++
  ) {
    final retained = await backend.readRetainedDeliveryTxn(
      txn,
      destinationId,
      generation: sender.generation,
      deliveryNumber: n,
    );
    if (retained == null) return false;
    if (n != record.deliveryNumber + 1) continue;
    final metadata = retained.envelopeMetadata;
    final channel = metadata?.channel;
    final attributes = metadata?.attributes;
    if (channel == null || attributes == null) return false;
    final hashes = <Object?>[];
    for (final id in retained.eventIds) {
      final event = await backend.findEventByIdInTxn(txn, id);
      if (event == null) return false;
      hashes.add(event.toMap()['event_hash']);
    }
    final relinked = computeDeliveryHash(
      channel: channel,
      deliveryNumber: n,
      previousDeliveryHash: record.deliveryHash,
      eventHashes: hashes,
      attributes: attributes,
    );
    if (relinked != retained.deliveryHash) return false;
  }
  return true;
}

/// Retires [items]' pending ones inside [txn]: deletes each that carries no
/// attempt and tombstones each that carries attempts. Returns the lowest
/// event sequence number they carry, or null when none was pending.
Future<int?> _retirePendingInTxn(
  StorageBackend backend,
  Transaction txn,
  String destinationId,
  List<FifoEntry> items,
) async {
  int? lowest;
  for (final item in items) {
    if (item.finalStatus != null) continue;
    if (item.attempts.isEmpty) {
      await backend.deleteFifoEntryTxn(txn, destinationId, item.entryId);
    } else {
      await backend.setFinalStatusTxn(
        txn,
        destinationId,
        item.entryId,
        FinalStatus.tombstoned,
      );
    }
    final first = item.sequenceRange.firstSeq;
    if (lowest == null || first < lowest) lowest = first;
  }
  return lowest;
}

/// The resume of a receiver behind, inside [txn]: retires the pending
/// items, rewinds the fill position below the lowest event they carry,
/// enqueues one resend item per delivery number above [record] up to
/// [sender]'s, carrying the retained delivery's events, envelope fields
/// and attributes, sets the sender channel record to [record] and appends
/// the resume event.
// Implements: EVS-DEV-delivery-resume/N
// the resume commits in one transaction that verifies the drain lock,
//   retires the pending items (deleting those without attempts,
//   tombstoning those with), rewinds the fill position below their lowest
//   event, enqueues the resend items, sets the sender channel record to the
//   receiver record and appends the resume event.
// Implements: EVS-DEV-delivery-resume/M
// one resend item per delivery number above the receiver record up to the
//   sender's, in ascending order, each carrying the retained delivery's
//   events and attributes in its order.
// Implements: EVS-PRD-delivery-channel/H
// every retained delivery after the receiver's record is sent again with
//   the events, number, link and hash it was first sent with, and the
//   resume is recorded as one event.
// Implements: EVS-PRD-delivery-channel/L
// a receiver that moved back is realigned by resending the retained
//   deliveries it lacks that link to its record.
// Implements: EVS-DEV-destination-drain/E
// the receiver-behind resume enqueues the resend items; it is the drainer.
Future<void> _resumeInTxn(
  DestinationRegistry registry,
  Transaction txn,
  PublishCollector collector, {
  required DrainLock lock,
  required String destinationId,
  required SenderChannelRecord sender,
  required DeliveryRecord record,
  required String responding,
  required List<FifoEntry> items,
}) async {
  final backend = registry._backend;
  final lowest = await _retirePendingInTxn(backend, txn, destinationId, items);
  if (lowest != null) {
    final cursor = await backend.readFillCursorTxn(txn, destinationId);
    if (lowest - 1 < cursor) {
      await backend.writeFillCursorTxn(txn, destinationId, lowest - 1);
    }
  }
  // Implements: EVS-DEV-delivery-resume/N
  // the resume removes the registration's transform failure record with
  //   the rewind.
  await backend.clearTransformFailureRecordTxn(txn, destinationId);
  for (
    var n = record.deliveryNumber + 1;
    n <= sender.receiverRecord.deliveryNumber;
    n++
  ) {
    final retained = (await backend.readRetainedDeliveryTxn(
      txn,
      destinationId,
      generation: sender.generation,
      deliveryNumber: n,
    ))!;
    final events = <StoredEvent>[];
    for (final id in retained.eventIds) {
      events.add((await backend.findEventByIdInTxn(txn, id))!);
    }
    await backend.enqueueFifoTxn(
      txn,
      destinationId,
      events,
      nativeEnvelope: retained.envelopeMetadata,
    );
  }
  await backend.writeSenderChannelRecordTxn(
    txn,
    destinationId,
    SenderChannelRecord(
      generation: sender.generation,
      receiverRecord: record,
      receiverDatabaseId: responding,
    ),
  );
  final schedule = await backend.readScheduleTxn(txn, destinationId);
  // Implements: EVS-DEV-resume-event/A
  // the resume event carries exactly id, database_id, registration_id,
  //   generation, resume_after, previous_record and drainer_epoch.
  await registry._emitDestinationAuditInTxn(
    txn,
    collector,
    entryType: kDestinationChannelResumedEntryType,
    eventType: kDestinationChannelResumedEventType,
    data: <String, Object?>{
      'id': destinationId,
      // `database_id` is added by the audit emitter.
      'registration_id': schedule?.registrationId,
      'generation': sender.generation,
      'resume_after': record.toJson(),
      'previous_record': sender.receiverRecord.toJson(),
      'drainer_epoch': lock.epoch,
    },
    initiator: _drainInitiator,
  );
}

/// A new generation of [destinationId]'s registration, inside [txn]:
/// records the finding [kind] under the role `sender`, retires the pending
/// items, rewinds the fill position to the start of the log and sets the
/// sender channel record to the next generation, number 0, a null hash and
/// the responding receiver.
// Implements: EVS-DEV-delivery-resume/Z
// the new generation commits in one transaction that verifies the drain
//   lock, appends the finding under the detector role sender naming the
//   channel, both records and both receiver identities, retires the pending
//   items, rewinds the fill position to the start and sets the record to
//   the next generation at number 0 with the responding receiver.
// Implements: EVS-PRD-delivery-channel/X
// a record no automatic path explains continues the registration on a new
//   generation, numbered from delivery 1 and filled again from the start of
//   the log.
Future<void> _newGenerationInTxn(
  DestinationRegistry registry,
  Transaction txn,
  PublishCollector collector, {
  required String destinationId,
  required DeliveryChannel channel,
  required SenderChannelRecord sender,
  required DeliveryRecord record,
  required String responding,
  required FindingKind kind,
  required List<FifoEntry> items,
}) async {
  final backend = registry._backend;
  await registry.eventStore._recordFindingInTxn(
    txn,
    collector,
    role: FindingRole.sender,
    kind: kind,
    evidence: <String, Object?>{
      'channel': channel.toJson(),
      'sender_record': sender.receiverRecord.toJson(),
      'receiver_record': record.toJson(),
      'recorded_receiver_database_id': sender.receiverDatabaseId,
      'responding_receiver_database_id': responding,
    },
    aggregates: const <String>[],
  );
  await _retirePendingInTxn(backend, txn, destinationId, items);
  await backend.writeFillCursorTxn(txn, destinationId, -1);
  // Implements: EVS-DEV-delivery-resume/Z
  // the new generation removes the registration's transform failure
  //   record with the rewind.
  await backend.clearTransformFailureRecordTxn(txn, destinationId);
  await backend.writeSenderChannelRecordTxn(
    txn,
    destinationId,
    SenderChannelRecord(
      generation: sender.generation + 1,
      receiverRecord: DeliveryRecord.none,
      receiverDatabaseId: responding,
    ),
  );
}
