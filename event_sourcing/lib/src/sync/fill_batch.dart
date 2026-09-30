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
// Implements: EVS-DEV-destination-drain/V
// (own events only — the walk skips an
//   event whose originator entry does not name the database's own identity
//   or whose last provenance entry is not that originator entry, advancing
//   the cursor past it like any other decided event)
// Implements: EVS-DEV-destination-drain/X
// (channel-wide events — a security
//   finding or a succession event this database appended bypasses the
//   destination's filter on a natively serializing registration)
// Implements: EVS-DEV-destination-drain-lock/B
// (every transaction of the fill checks
//   the drain lock first and commits nothing when the drainer no longer
//   holds it)
// Implements: EVS-DEV-destination-drain/Y
// (a failing transform is recorded in
//   the destination's transform failure record and retried at later passes
//   under the retry curve; once the recorded failures have spent the
//   retry budget the batch enqueues as one transform-failed item, with no
//   payload, and the record is cleared, in one transaction; a transform
//   that recovers clears the record in the transaction that enqueues its
//   item)
// Implements: EVS-DEV-destination-retry-budget/B
// (the fill records the time of each
//   transform failure in the transform failure record and treats its
//   budget as spent by the same attempt-count-or-capped-time rule a send's
//   recorded attempts are measured by)

part of '../event_store.dart';

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
    required this.senderChannel,
    required this.transformFailureRecord,
  });

  final DestinationSchedule? schedule;
  final FinalStatus? headStatus;
  final int cursor;
  final ReplayRequest? request;
  final RefillGuard? refillGuard;

  /// The sender channel record of a destination that serializes natively
  /// (null for any other): a new generation changes it, so a fill computed
  /// before it commits nothing.
  final SenderChannelRecord? senderChannel;

  /// The destination's transform failure record, or null when its transform
  /// is not currently failing. Read here so a fill's decision to retry the
  /// transform, or to leave the record alone, is re-validated by the same
  /// compare-and-set as every other fill decision
  /// (`EVS-DEV-destination-drain/G`).
  final TransformFailureRecord? transformFailureRecord;

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
    final senderChannel = await backend.readSenderChannelRecordTxn(
      txn,
      destinationId,
    );
    final transformFailureRecord = await backend.readTransformFailureRecordTxn(
      txn,
      destinationId,
    );
    return _FillState(
      schedule: schedule,
      headStatus: head?.finalStatus,
      cursor: cursor,
      request: request,
      refillGuard: refillGuard,
      senderChannel: senderChannel,
      transformFailureRecord: transformFailureRecord,
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
      other.refillGuard == refillGuard &&
      other.senderChannel == senderChannel &&
      other.transformFailureRecord == transformFailureRecord;

  @override
  int get hashCode => Object.hash(
    schedule,
    headStatus,
    cursor,
    request,
    refillGuard,
    senderChannel,
    transformFailureRecord,
  );
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
///    commit it. A first-activation replay enqueues every admitted event
///    past the position, batch by batch, advancing the position as each
///    batch decides; a batch whose transform is retrying under
///    `EVS-DEV-destination-drain/Y` stops the walk there and the request
///    stays pending, so the position advances to the last resolved batch
///    while later, undecided events wait for a later pass. A gap replay
///    enqueues the admitted events at or below the position whose client
///    timestamp lies in `[startDate, gapUpper)`, and leaves the position;
///    a gap replay's transform is not retried (see `_performReplayRequest`).
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
/// 8. Build the item: a native destination gets a library-built envelope
///    and never runs a transform. Any other destination's `transform` runs
///    outside any transaction; on success the item commits with the
///    position advanced to the batch's last event, clearing any earlier
///    transform failure record for the destination. On failure the fill
///    records the failure time in the destination's transform failure
///    record (replacing a record left by an earlier, different batch) and
///    writes nothing else; while the retry curve's backoff after the last
///    recorded failure has not elapsed, a later pass reruns nothing for a
///    matching batch. Once the recorded failures have spent [policy]'s
///    retry budget (`EVS-DEV-destination-retry-budget/B`), the batch
///    enqueues as one transform-failed item — no payload, the failure
///    count, the destination's `wireFormat` — the position advances to the
///    batch's last event and the record is cleared, all in one transaction
///    (`EVS-DEV-destination-drain/Y`).
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
/// when the drainer no longer holds it. [databaseId] (the identity of the
/// database this fill runs for) decides which events the walk admits at
/// all: only an event whose originator entry names it and whose last
/// provenance entry is that originator entry (`EVS-DEV-destination-drain/V`);
/// every other event is decided like a filter rejection, so the cursor
/// passes it. A security finding or a succession event this database
/// appended bypasses the destination's filter on a natively serializing
/// registration (`EVS-DEV-destination-drain/X`). A native destination's
/// envelope carries [source], the source identity of the event store whose
/// [backend] the fill writes, and its delivery channel: [databaseId], the
/// destination, the persisted schedule's registration and the generation of
/// the destination's sender channel record. [clock] defaults to
/// `() => DateTime.now().toUtc()`. [policy] is the retry budget in effect
/// (the fill's caller resolves it the way `drain` does, falling back to
/// [SyncPolicy.defaults]); [cadence] is the delivery cycle's cadence. Both
/// bound how long a failing transform is retried before its batch enqueues
/// as transform-failed (`EVS-DEV-destination-retry-budget/B`).
@internal
Future<void> fillBatch(
  Destination destination, {
  required StorageBackend backend,
  required Source source,
  required DrainLock lock,
  required String databaseId,
  required SyncPolicy policy,
  required Duration cadence,
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
      channel: _channelOf(destination, state, databaseId),
      databaseId: databaseId,
      policy: policy,
      cadence: cadence,
    );
    if (!performed) return;
  }

  final schedule = state.schedule!;
  final startDate = schedule.startDate;
  final endDate = schedule.endDate;
  final upper = endDate == null || endDate.isAfter(now) ? now : endDate;
  // Implements: EVS-DEV-destination-drain/X
  // A dormant schedule or a window entirely in
  // the future is nothing to do for an ordinary event, but a natively
  // serializing destination still walks the log for a channel-wide own
  // event, which bypasses the window as it bypasses the filter.
  final windowDormant = startDate == null || startDate.isAfter(upper);
  final channel = _channelOf(destination, state, databaseId);
  if (windowDormant && !destination.serializesNatively) return;

  final candidates = await backend.findAllEvents(afterSequence: state.cursor);
  if (candidates.isEmpty) {
    // Nothing is left to refill: the guard goes in a transaction of its
    // own.
    if (state.refillGuard != null) {
      await _compareAndSet(backend, lock, id, state, (_) async {});
    }
    return;
  }

  // A channel-wide own event already queued under the channel's current
  // generation is not enqueued again: a resume, a new generation or an
  // operator recovery that retires or removes its item lets it back into
  // this set.
  final alreadyQueuedChannelWide = destination.serializesNatively
      ? queuedChannelWideEventIds(
          await backend.listFifoEntries(id),
          channel?.generation,
        )
      : const <String>{};

  // Permanent rejections (filter, startDate-lower) are decided and let the
  // position pass them; a deferred event (past the upper bound, which a
  // later end date or the clock may widen) stops the ordinary walk there,
  // but the walk continues past it to find every channel-wide own event,
  // which is enqueued on its own without moving the position.
  final walk = walkAdmission(
    destination,
    candidates,
    databaseId: databaseId,
    startDate: startDate,
    upper: upper,
    alreadyQueued: alreadyQueuedChannelWide,
    initiallyDeferred: windowDormant,
  );
  final standalone = await buildStandaloneChannelWideItems(
    destination,
    walk.standaloneChannelWide,
    source: source,
    now: now,
    channel: channel,
  );
  final inWindow = walk.inWindow;

  if (inWindow.isEmpty) {
    if (standalone.isEmpty) {
      if (walk.lastDecidedSeq == null) return;
      final advanceTo = walk.lastDecidedSeq!;
      await _compareAndSet(backend, lock, id, state, (txn) async {
        await backend.writeFillCursorTxn(txn, id, advanceTo);
      }, cursorAfter: advanceTo);
      return;
    }
    final advanceTo = walk.lastDecidedSeq;
    await _compareAndSet(backend, lock, id, state, (txn) async {
      await writeQueueItemsTxn(txn, backend, id, standalone);
      if (advanceTo != null) {
        await backend.writeFillCursorTxn(txn, id, advanceTo);
      }
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

  // maxAccumulateTime hold: a lone ordinary event is held (nothing written,
  // the position not advanced) until it is older than maxAccumulateTime, so
  // a later event can join it. A forced cycle (flushHeld) ships it now. A
  // channel-wide event found beyond the batch is never held: it enqueues in
  // this same pass regardless.
  final oldestAge = now.difference(batch.first.clientTimestamp);
  if (!flushHeld &&
      batch.length == 1 &&
      oldestAge < destination.maxAccumulateTime) {
    if (standalone.isNotEmpty) {
      await _compareAndSet(backend, lock, id, state, (txn) async {
        await writeQueueItemsTxn(txn, backend, id, standalone);
      });
    }
    return;
  }

  final decision = await _decideBatch(
    destination,
    batch,
    existingRecord: state.transformFailureRecord,
    source: source,
    now: now,
    channel: channel,
    policy: policy,
    cadence: cadence,
  );
  if (decision.skip) {
    // The retry curve has not yet allowed another attempt: the fill
    // reruns nothing on the ordinary batch this pass, but a channel-wide
    // event found alongside it still enqueues.
    if (standalone.isNotEmpty) {
      await _compareAndSet(backend, lock, id, state, (txn) async {
        await writeQueueItemsTxn(txn, backend, id, standalone);
      });
    }
    return;
  }
  final pendingRecord = decision.pendingRecord;
  if (pendingRecord != null) {
    await _compareAndSet(backend, lock, id, state, (txn) async {
      if (standalone.isNotEmpty) {
        await writeQueueItemsTxn(txn, backend, id, standalone);
      }
      await backend.writeTransformFailureRecordTxn(txn, id, pendingRecord);
    });
    return;
  }
  final item = decision.item!;
  await _compareAndSet(backend, lock, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, <BuiltQueueItem>[
      item,
      ...standalone,
    ]);
    await backend.writeFillCursorTxn(txn, id, batch.last.sequenceNumber);
    if (state.transformFailureRecord != null) {
      await backend.clearTransformFailureRecordTxn(txn, id);
    }
  }, cursorAfter: batch.last.sequenceNumber);
}

/// Perform [request] under the compare-and-set: build its items outside any
/// transaction, then commit them, the advanced position, a pending
/// transform failure record and (once every part of the request is
/// resolved) the cleared request together. Returns whether it committed.
///
/// A first-activation batch whose transform is still backing off, or has
/// failed again without spending its retry budget, stops the walk: the
/// request stays pending and events past the reached position are
/// retried at a later fill (`EVS-DEV-destination-drain/Y`). The transform
/// failure record this stop reads and writes is always the one a first
/// activation's walk names; a gap replay never reads or writes it, whatever
/// it currently holds (that record, if any, belongs to an unrelated live
/// fill batch — see the guard below).
///
/// [buildGapReplayRows] is not retry-aware: a transform failure while it
/// builds a gap replay's items propagates out of this call uncaught, and
/// the pass writes nothing. Once a first activation's walk has stopped
/// early at least once (a persisted record proves an earlier call already
/// built, and this call already committed, the gap replay's own items),
/// the gap portion is not rebuilt on a later retry, so a stalled
/// activation's retries do not double-enqueue it.
Future<bool> _performReplayRequest(
  Destination destination,
  StorageBackend backend,
  DrainLock lock,
  _FillState state,
  ReplayRequest request, {
  required Source source,
  required DateTime now,
  required DeliveryChannel? channel,
  required String databaseId,
  required SyncPolicy policy,
  required Duration cadence,
}) async {
  final id = destination.id;
  final schedule = state.schedule!;
  final items = <BuiltQueueItem>[];
  int? advanceTo;
  TransformFailureRecord? pendingRecord;
  var activationDone = !request.firstActivation;
  final gapUpper = request.gapUpper;
  final startDate = schedule.startDate;
  final gapActive =
      gapUpper != null && startDate != null && startDate.isBefore(gapUpper);
  // A persisted record proves an earlier call for this same request already
  // built (and this function will have committed) the gap portion's items:
  // see the doc comment above. This guards only a request that also carries
  // a first activation: a pure gap replay has no incremental progress to
  // protect, and the record it sees may belong to an unrelated live-fill
  // batch that has nothing to do with this request.
  final runGapPortion =
      gapActive &&
      (!request.firstActivation || state.transformFailureRecord == null);
  // Implements: EVS-DEV-destination-drain/X
  // A channel-wide own event (a succession
  // event or a security finding) already queued as pending, wedged, or
  // sent under the channel's current generation is not enqueued a second
  // time by either portion of this call.
  final alreadyQueuedChannelWide = destination.serializesNatively
      ? queuedChannelWideEventIds(
          await backend.listFifoEntries(id),
          channel?.generation,
        )
      : const <String>{};
  if (runGapPortion) {
    final events = await _eventsAtOrBelow(
      backend,
      state.cursor,
      keep: (e) =>
          (destination.serializesNatively &&
              isChannelWideEntryType(e.entryType)) ||
          (!e.clientTimestamp.isBefore(startDate) &&
              e.clientTimestamp.isBefore(gapUpper)),
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
        channel: channel,
        databaseId: databaseId,
        alreadyQueuedChannelWide: alreadyQueuedChannelWide,
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
      channel: channel,
      databaseId: databaseId,
      policy: policy,
      cadence: cadence,
      existingFailureRecord: state.transformFailureRecord,
      alreadyQueuedChannelWide: alreadyQueuedChannelWide,
    );
    items.addAll(build.items);
    advanceTo = build.cursor;
    pendingRecord = build.pendingFailureRecord;
    activationDone = !build.stoppedEarly;
  }
  final cursor = advanceTo;
  // The record this function ever writes or clears is the one a first
  // activation's walk consulted; a pure gap replay (request.firstActivation
  // false) never touches it, whatever it currently holds — that record, if
  // any, belongs to an unrelated live fill.
  final clearsRecord =
      request.firstActivation &&
      activationDone &&
      pendingRecord == null &&
      state.transformFailureRecord != null;
  final recordChanges = pendingRecord != null || clearsRecord;
  if (items.isEmpty && cursor == null && !recordChanges && !activationDone) {
    // Nothing at all would change: the same "still backing off, write
    // nothing" outcome the live fill returns without a transaction.
    return false;
  }
  final committed = await _compareAndSet(backend, lock, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, items);
    if (cursor != null) await backend.writeFillCursorTxn(txn, id, cursor);
    if (pendingRecord != null) {
      await backend.writeTransformFailureRecordTxn(txn, id, pendingRecord);
    } else if (clearsRecord) {
      await backend.clearTransformFailureRecordTxn(txn, id);
    }
    if (activationDone) await backend.clearReplayRequestTxn(txn, id);
  }, cursorAfter: cursor);
  // Only a fully resolved request (activationDone: the request cleared, or
  // there was no first-activation portion to resolve) tells the caller's
  // loop to look for further work under the same `now`. A batch that
  // stopped early committed real progress (a failure record, or the items
  // built before it), but retrying immediately would re-run the very
  // backoff check that just stopped it, at the same clock reading, forever:
  // the next fill pass (a later `now`) is what lets the retry curve elapse.
  return committed && activationDone;
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

/// The delivery channel a fill enqueues [destination]'s items on, computed
/// from the persisted [state]: the sending database [databaseId], the
/// destination, the schedule's registration and the sender channel record's
/// generation. Null for a destination that does not serialize natively.
DeliveryChannel? _channelOf(
  Destination destination,
  _FillState state,
  String databaseId,
) {
  if (!destination.serializesNatively) return null;
  final registrationId = state.schedule?.registrationId;
  final record = state.senderChannel;
  if (registrationId == null || record == null) {
    throw StateError(
      'destination "${destination.id}" serializes natively but its persisted '
      'registration holds no registration identifier or no sender channel '
      'record',
    );
  }
  return DeliveryChannel(
    senderDatabaseId: databaseId,
    destinationId: destination.id,
    registrationId: registrationId,
    generation: record.generation,
  );
}
