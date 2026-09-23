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
//   transaction that commits the outcome it produced)
// Implements: EVS-DEV-destination-drain/C
// (the attempt and the status it
//   produces commit in one transaction)
// Implements: EVS-DEV-destination-drain/D
// (an attempt is recorded only on the
//   pending head the drain sent)
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// Drain the head of [destination]'s FIFO: check backoff, call
/// [Destination.send], record the attempt, and route the result to a
/// terminal `final_status` of `sent` or `wedged` as appropriate.
/// Returns when:
///
/// - the FIFO has no drain-candidate rows (`readFifoHead` returns null
///   after skipping `sent` and `tombstoned` rows); or
/// - the head row's `final_status` is [FinalStatus.wedged] (strict-order
///   halt; operator recovery via `tombstoneAndRefill`);
/// - the head row's backoff has not elapsed; or
/// - the most recent [Destination.send] returned [SendTransient] below
///   the `maxAttempts` cap (backoff applies on the next drain tick).
///
/// On [SendOk] the head is marked `sent` and the loop advances to the
/// next drain candidate. On [SendPermanent] or [SendTransient]-at-
/// `maxAttempts` the head is marked [FinalStatus.wedged]; the next loop
/// iteration reads the newly-wedged row via `readFifoHead` and the
/// top-of-loop halt check returns. Trail rows
/// behind a wedged head are NEVER attempted — strict-order delivery
/// guarantees that once a bundle has been retired to `wedged`, no
/// later bundle is sent ahead of it. Recovery is the operator's
/// `tombstoneAndRefill` primitive.
///
/// Strict FIFO order: within a single drain pass, rows
/// are attempted in `sequence_in_queue` order and a wedged head halts
/// the pass. `sent` and `tombstoned` rows are terminal-passable and are
/// skipped by `readFifoHead` so they do not block a later drain
/// candidate.
///
/// [policy] is an optional [SyncPolicy] override; when null, the drain
/// loop falls back to [SyncPolicy.defaults].
@internal
Future<void> drain(
  Destination destination, {
  required StorageBackend backend,
  Clock? clock,
  SyncPolicy? policy,
}) async {
  final now = clock ?? () => DateTime.now().toUtc();
  final effective = policy ?? SyncPolicy.defaults;
  while (true) {
    final head = await backend.readFifoHead(destination.id);
    if (head == null) return;
    // A wedged head halts the drain; recovery is tombstoneAndRefill.
    // readFifoHead returns the wedged row (rather than skipping it) so UI
    // surfaces can observe the wedge via that single entry point.
    if (head.finalStatus == FinalStatus.wedged) return;
    // head.finalStatus is null from here on — this is a drain candidate.

    // Backoff check: only the N-th attempt's timestamp matters; skip if
    // the entry has never been attempted (fresh head).
    if (head.attempts.isNotEmpty) {
      final backoff = effective.backoffFor(head.attempts.length);
      final nextAllowed = head.attempts.last.attemptedAt.add(backoff);
      if (now().isBefore(nextAllowed)) return;
    }

    // Reconstruct a WirePayload from the FifoEntry's stored fields.
    // Native `esd/batch@2` rows reconstruct bytes from
    // `envelopeMetadata` + `eventIds`-resolved events through
    // `BatchEnvelope.encode`, which JCS-canonicalizes the envelope so
    // the result is byte-identical across retries.
    // 3rd-party rows (any other wireFormat) carry the bytes as a stored
    // JSON-Map `wirePayload`; we re-encode that Map to bytes verbatim
    // for `Destination.send`, preserving the previous storage shape.
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

    // Call the destination. Categorize any thrown error as SendTransient —
    // the drain-loop contract does not distinguish between a thrown
    // exception and an explicit SendTransient return: both mean "try
    // again later".
    SendResult result;
    try {
      result = await destination.send(payload);
    } catch (error, stack) {
      result = SendTransient(error: 'uncaught exception: $error\n$stack');
    }

    final attempt = _attemptFromResult(result, now());

    // Route the outcome. The attempt and the status it produces commit in
    // one transaction: SendOk marks the head sent; SendPermanent and
    // SendTransient at the attempt cap mark it wedged; a SendTransient below
    // the cap records the attempt alone and leaves the head pending (backoff
    // applies on the next pass). The next loop iteration sees a wedged head
    // via readFifoHead and halts at the top-of-loop check, so trail rows are
    // never attempted ahead of a wedged head.
    final FinalStatus? status;
    switch (result) {
      case SendOk():
        status = FinalStatus.sent;
      case SendPermanent():
        status = FinalStatus.wedged;
      case SendTransient():
        // head.attempts.length is the count BEFORE this attempt.
        status = head.attempts.length + 1 >= effective.maxAttempts
            ? FinalStatus.wedged
            : null;
    }
    await backend.transaction((txn) async {
      await backend.appendAttemptTxn(
        txn,
        destination.id,
        head.entryId,
        attempt,
      );
      if (status != null) {
        await backend.setFinalStatusTxn(
          txn,
          destination.id,
          head.entryId,
          status,
        );
      }
      if (DeliveryTestHooks.current?.failOutcomeTransaction?.call(
            destination.id,
            attempt.outcome,
          ) ??
          false) {
        throw InjectedFailure('drain outcome transaction of ${destination.id}');
      }
    });
    if (status == null) return;
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
