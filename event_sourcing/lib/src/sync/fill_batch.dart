// Implements: EVS-PRD-destinations/B
// (per-destination filter — fillBatch
//   evaluates destination.filter.matches on every candidate event and skips
//   non-matching events, advancing the cursor past them)
// Implements: EVS-PRD-destinations/C
// (FIFO order — events are enqueued in
//   sequence_number order; the cursor advance to batch.last.sequenceNumber
//   preserves ordering for subsequent drain calls)
// Implements: EVS-PRD-destinations/D
// (durable queue — enqueue and
//   fill_cursor advance run inside a single StorageBackend transaction so
//   the FIFO state is crash-consistent across restarts)
// Implements: EVS-DEV-destination-drain/E
// (only the drainer's fill enqueues: it
//   performs a pending replay request, under the destination the drainer
//   registers, before it fills, and never advances the fill position while
//   a request is pending)
// Implements: EVS-DEV-destination-drain/G
// (fill compare-and-set: the fill writes
//   queue items, the fill position or a cleared replay request only when,
//   inside its transaction, the persisted schedule, head status, fill
//   position, replay request and refill guard equal those its batch was
//   computed from; the transform and every walk of the log run outside that
//   transaction)
// Implements: EVS-DEV-destination-drain/F
// (a refill guard holds the fill of a
//   drainer that declares the guarded configuration; a fill under any other
//   configuration removes the guard in its compare-and-set transaction)
// Implements: EVS-DEV-destination-drain-lock/B
// (every transaction of the fill checks
//   the drain lock first and commits nothing when the drainer no longer
//   holds it)
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/historical_replay.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// The persisted state a fill computes from, read before its consumer code
/// and its log walk run, and compared again inside the transaction that
/// writes.
///
/// The head is compared by its status only (wedged, pending or no head):
/// the fill appends at the tail, so a head another drainer delivered in the
/// meantime does not change what the fill may write. A deletion changes
/// the schedule and a recovery lowers the fill position, so both are seen
/// without the head's identity.
class _FillState {
  const _FillState({
    required this.schedule,
    required this.headStatus,
    required this.cursor,
    required this.request,
    required this.refillGuard,
  });

  final DestinationSchedule? schedule;
  final FinalStatus? headStatus;
  final int cursor;
  final ReplayRequest? request;
  final RefillGuard? refillGuard;

  bool get headWedged => headStatus == FinalStatus.wedged;

  static Future<_FillState> readTxn(
    StorageBackend backend,
    Transaction txn,
    String destinationId,
  ) async {
    final schedule = await backend.readScheduleTxn(txn, destinationId);
    final head = await backend.readFifoHeadTxn(txn, destinationId);
    final cursor = await backend.readFillCursorTxn(txn, destinationId);
    final request = await backend.readReplayRequestTxn(txn, destinationId);
    final refillGuard = await backend.readRefillGuardTxn(txn, destinationId);
    return _FillState(
      schedule: schedule,
      headStatus: head?.finalStatus,
      cursor: cursor,
      request: request,
      refillGuard: refillGuard,
    );
  }

  static Future<_FillState> read(
    StorageBackend backend,
    String destinationId,
  ) => backend.transaction((txn) => readTxn(backend, txn, destinationId));

  @override
  bool operator ==(Object other) =>
      other is _FillState &&
      other.schedule == schedule &&
      other.headStatus == headStatus &&
      other.cursor == cursor &&
      other.request == request &&
      other.refillGuard == refillGuard;

  @override
  int get hashCode =>
      Object.hash(schedule, headStatus, cursor, request, refillGuard);
}

/// Runs [write] inside one transaction only when the persisted state still
/// equals [computedFrom]; otherwise writes nothing. Returns whether it
/// wrote. The transaction checks [lock] first. When [computedFrom] carries
/// a refill guard, the fill proceeds only because it declares another
/// configuration, and the same transaction removes the guard once the fill
/// position it leaves ([cursorAfter], the position [computedFrom] read when
/// the write leaves it unchanged) reaches the guard's `refillThrough`.
Future<bool> _compareAndSet(
  StorageBackend backend,
  DrainLock lock,
  String destinationId,
  _FillState computedFrom,
  Future<void> Function(Transaction txn) write, {
  int? cursorAfter,
}) async {
  final seam = DeliveryTestHooks.current?.afterFillReads;
  if (seam != null) await seam(destinationId);
  return backend.transaction((txn) async {
    await lock.assertHeldInTxn(txn);
    await DeliveryTestHooks.current?.beforeQueueWrites?.call(destinationId);
    final now = await _FillState.readTxn(backend, txn, destinationId);
    if (now != computedFrom) return false;
    await write(txn);
    final guard = computedFrom.refillGuard;
    if (guard != null &&
        (cursorAfter ?? computedFrom.cursor) >= guard.refillThrough) {
      await backend.clearRefillGuardTxn(txn, destinationId);
    }
    if (DeliveryTestHooks.current?.failFillTransaction?.call(destinationId) ??
        false) {
      throw InjectedFailure('fill transaction of $destinationId');
    }
    return true;
  });
}

/// Promote matching events from the event log into a destination's FIFO
/// as batches, advancing `fill_cursor` accordingly.
///
/// The fill decides from the destination's persisted schedule, queue head,
/// fill position and pending replay request. It reads them, walks the log
/// and runs the destination's `transform` outside any transaction, then
/// commits its items and the new fill position in one transaction that
/// re-reads that state and writes nothing when any of it changed (another
/// process deleted, re-added or rescheduled the destination, or an operator
/// recovered its queue); the next fill recomputes.
///
/// Algorithm:
///
/// 1. No persisted schedule (the destination was deleted): nothing to do.
/// 2. A wedged head: nothing to do. Drain halts at it, so any item promoted
///    now would be speculative work the recovery's trail sweep would undo.
/// 3. A pending replay request (a first activation, or a start date moved
///    earlier) is performed first, and the fill ends when it could not
///    commit it; the fill never advances the position while a request is
///    pending. A first-activation replay enqueues every admitted event past
///    the position, to completion, and advances the position; a gap replay
///    enqueues the admitted events at or below the position whose client
///    timestamp lies in `[startDate, gapUpper)`, and leaves the position.
/// 4. Dormant schedule (`startDate == null`) or a window entirely in the
///    future: nothing to do.
/// 5. Fetch the events past the position and walk them: an event the filter
///    rejects or that precedes `startDate` is decided; an event past
///    `upper = min(endDate, now)` is deferred and stops the walk; every
///    other event is in the window.
/// 6. No event in the window: advance the position past the decided events
///    (so they are not re-evaluated) and return.
/// 7. Otherwise assemble a greedy batch through `canAddToBatch`. A lone
///    event younger than `maxAccumulateTime` is held (nothing written)
///    unless [flushHeld] is set.
/// 8. Build the item (a native destination gets a library-built envelope;
///    any other destination's `transform` runs) and commit it with the
///    position advanced to the batch's last event.
///
/// A refill guard (left by an accepted recovery of a reconfigure halt) that
/// names [declaredFingerprint], the fingerprint of the configuration the
/// drainer declares for the destination, holds the fill: it writes nothing.
/// Under any other fingerprint the fill proceeds, and the guard is removed
/// in the compare-and-set transaction that advances the fill position to
/// the guard's `refillThrough` (the position the recovery rewound from), so
/// a drainer declaring the guarded configuration that takes the lock part
/// way through the refill does not continue it. When nothing is left to
/// fill, a transaction of its own removes the guard.
///
/// With [registrationId] (the registration the draining process holds the
/// destination under), a persisted schedule of another registration (the
/// destination was deleted and registered again elsewhere) holds the fill:
/// it writes nothing. The compare-and-set re-reads the schedule, so a
/// registration that changes while the fill builds its items commits
/// nothing either.
///
/// Every transaction of the fill checks [lock] first and commits nothing
/// when the drainer no longer holds it. A native destination's envelope
/// carries [source], the source identity of the event store whose
/// [backend] the fill writes. [clock] defaults to
/// `() => DateTime.now().toUtc()`.
@internal
Future<void> fillBatch(
  Destination destination, {
  required StorageBackend backend,
  required Source source,
  required DrainLock lock,
  Clock? clock,
  bool flushHeld = false,
  String? declaredFingerprint,
  String? registrationId,
}) async {
  final now = (clock ?? () => DateTime.now().toUtc())();
  final id = destination.id;

  // Perform every pending replay request before filling: a request recorded
  // while one is performed is performed next, so the position never
  // advances while a request is pending.
  _FillState state;
  for (;;) {
    state = await _FillState.read(backend, id);
    if (state.schedule == null || state.headWedged) return;
    if (registrationId != null &&
        state.schedule!.registrationId != registrationId) {
      return;
    }
    final guard = state.refillGuard;
    if (guard != null && guard.fingerprint == declaredFingerprint) return;
    final request = state.request;
    if (request == null) break;
    final performed = await _performReplayRequest(
      destination,
      backend,
      lock,
      state,
      request,
      source: source,
      now: now,
    );
    if (!performed) return;
  }

  final schedule = state.schedule!;
  final startDate = schedule.startDate;
  if (startDate == null) return;
  final endDate = schedule.endDate;
  final upper = endDate == null || endDate.isAfter(now) ? now : endDate;
  if (startDate.isAfter(upper)) return;

  final candidates = await backend.findAllEvents(afterSequence: state.cursor);
  if (candidates.isEmpty) {
    // Nothing is left to refill: the guard goes in a transaction of its
    // own.
    if (state.refillGuard != null) {
      await _compareAndSet(backend, lock, id, state, (_) async {});
    }
    return;
  }

  // Permanent rejections (filter, startDate-lower) are decided and let the
  // position pass them; a deferred event (past the upper bound, which a
  // later end date or the clock may widen) stops the walk. The permanent
  // checks run first, so an event the filter rejects never blocks the walk
  // even when its client timestamp is past the upper bound.
  final inWindow = <StoredEvent>[];
  int? lastDecidedSeq;
  for (final e in candidates) {
    if (!destination.filter.matches(e)) {
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (e.clientTimestamp.isBefore(startDate)) {
      // A later backward start-date move records a gap replay for events
      // behind the position, so the fill need not keep these re-evaluable.
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (e.clientTimestamp.isAfter(upper)) break;
    inWindow.add(e);
    lastDecidedSeq = e.sequenceNumber;
  }

  if (inWindow.isEmpty) {
    if (lastDecidedSeq == null) return;
    final advanceTo = lastDecidedSeq;
    await _compareAndSet(backend, lock, id, state, (txn) async {
      await backend.writeFillCursorTxn(txn, id, advanceTo);
    }, cursorAfter: advanceTo);
    return;
  }

  final batch = <StoredEvent>[inWindow.first];
  for (final c in inWindow.skip(1)) {
    if (destination.canAddToBatch(batch, c)) {
      batch.add(c);
    } else {
      break;
    }
  }

  // maxAccumulateTime hold: a lone event is held (nothing written, the
  // position not advanced) until it is older than maxAccumulateTime, so a
  // later event can join it. A forced cycle (flushHeld) ships it now.
  final oldestAge = now.difference(batch.first.clientTimestamp);
  if (!flushHeld &&
      batch.length == 1 &&
      oldestAge < destination.maxAccumulateTime) {
    return;
  }

  final item = await buildQueueItem(
    destination,
    batch,
    source: source,
    now: now,
  );
  await _compareAndSet(backend, lock, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, <BuiltQueueItem>[item]);
    await backend.writeFillCursorTxn(txn, id, batch.last.sequenceNumber);
  }, cursorAfter: batch.last.sequenceNumber);
}

/// Perform [request] under the compare-and-set: build its items outside any
/// transaction, then commit them, the advanced position (first activation)
/// and the cleared request together. Returns whether it committed.
Future<bool> _performReplayRequest(
  Destination destination,
  StorageBackend backend,
  DrainLock lock,
  _FillState state,
  ReplayRequest request, {
  required Source source,
  required DateTime now,
}) async {
  final id = destination.id;
  final schedule = state.schedule!;
  final items = <BuiltQueueItem>[];
  int? advanceTo;
  final gapUpper = request.gapUpper;
  final startDate = schedule.startDate;
  if (gapUpper != null && startDate != null && startDate.isBefore(gapUpper)) {
    final events = await _eventsAtOrBelow(
      backend,
      state.cursor,
      keep: (e) =>
          !e.clientTimestamp.isBefore(startDate) &&
          e.clientTimestamp.isBefore(gapUpper),
    );
    items.addAll(
      await buildGapReplayRows(
        destination,
        events,
        startDate: startDate,
        gapUpper: gapUpper,
        fillCursor: state.cursor,
        now: now,
        source: source,
      ),
    );
  }
  if (request.firstActivation) {
    final candidates = await backend.findAllEvents(afterSequence: state.cursor);
    final build = await buildHistoricalReplayRows(
      destination,
      candidates,
      startDate: startDate,
      endDate: schedule.endDate,
      now: now,
      source: source,
    );
    items.addAll(build.items);
    advanceTo = build.cursor;
  }
  final cursor = advanceTo;
  return _compareAndSet(backend, lock, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, items);
    if (cursor != null) await backend.writeFillCursorTxn(txn, id, cursor);
    await backend.clearReplayRequestTxn(txn, id);
  }, cursorAfter: cursor);
}

/// The events whose sequence number is at or below [cursor] and that [keep]
/// accepts, in `sequence_number` order. The log is read in pages that stop
/// at the cursor, and only the kept events are held.
Future<List<StoredEvent>> _eventsAtOrBelow(
  StorageBackend backend,
  int cursor, {
  required bool Function(StoredEvent) keep,
}) async {
  const pageSize = 500;
  final events = <StoredEvent>[];
  int? after;
  for (;;) {
    final page = await backend.findAllEvents(
      afterSequence: after,
      limit: pageSize,
    );
    for (final e in page) {
      if (e.sequenceNumber > cursor) return events;
      if (keep(e)) events.add(e);
    }
    if (page.length < pageSize) return events;
    after = page.last.sequenceNumber;
  }
}
