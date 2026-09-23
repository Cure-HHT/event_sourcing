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
//   position and replay request equal those its batch was computed from;
//   the transform and every walk of the log run outside that transaction)
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
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
  });

  final DestinationSchedule? schedule;
  final FinalStatus? headStatus;
  final int cursor;
  final ReplayRequest? request;

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
    return _FillState(
      schedule: schedule,
      headStatus: head?.finalStatus,
      cursor: cursor,
      request: request,
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
      other.request == request;

  @override
  int get hashCode => Object.hash(schedule, headStatus, cursor, request);
}

/// Runs [write] inside one transaction only when the persisted state still
/// equals [computedFrom]; otherwise writes nothing. Returns whether it
/// wrote.
Future<bool> _compareAndSet(
  StorageBackend backend,
  String destinationId,
  _FillState computedFrom,
  Future<void> Function(Transaction txn) write,
) async {
  final seam = DeliveryTestHooks.current?.afterFillReads;
  if (seam != null) await seam(destinationId);
  return backend.transaction((txn) async {
    final now = await _FillState.readTxn(backend, txn, destinationId);
    if (now != computedFrom) return false;
    await write(txn);
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
/// 8. Build the item (a native destination gets a library-built envelope
///    from [source]; any other destination's `transform` runs) and commit
///    it with the position advanced to the batch's last event.
///
/// [source] is required when `destination.serializesNatively` is true.
/// [clock] defaults to `() => DateTime.now().toUtc()`.
@internal
Future<void> fillBatch(
  Destination destination, {
  required StorageBackend backend,
  Source? source,
  Clock? clock,
  bool flushHeld = false,
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
    final request = state.request;
    if (request == null) break;
    final performed = await _performReplayRequest(
      destination,
      backend,
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
  if (candidates.isEmpty) return;

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
    await _compareAndSet(backend, id, state, (txn) async {
      await backend.writeFillCursorTxn(txn, id, advanceTo);
    });
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
  await _compareAndSet(backend, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, <BuiltQueueItem>[item]);
    await backend.writeFillCursorTxn(txn, id, batch.last.sequenceNumber);
  });
}

/// Perform [request] under the compare-and-set: build its items outside any
/// transaction, then commit them, the advanced position (first activation)
/// and the cleared request together. Returns whether it committed.
Future<bool> _performReplayRequest(
  Destination destination,
  StorageBackend backend,
  _FillState state,
  ReplayRequest request, {
  required Source? source,
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
  return _compareAndSet(backend, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, items);
    if (cursor != null) await backend.writeFillCursorTxn(txn, id, cursor);
    await backend.clearReplayRequestTxn(txn, id);
  });
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
