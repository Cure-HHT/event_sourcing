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
//   position, replay request, refill guard and key lists equal those its
//   batch was computed from; the transform, the block-key evaluation and
//   every walk of the log run outside that transaction)
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
// Implements: EVS-DEV-destination-key-hold/A+B+C+D+E+G+L+M
// (keyed fill — the block key of each
//   event is evaluated outside the transaction and stored on its item; an
//   item carries one key's events; a listed key's events are inserted into
//   its key list, untransformed, on the fill and every replay; a failing key
//   evaluation follows the transform failure path)
// Implements: EVS-DEV-destination-retry-budget/B
// (the fill records the time of each
//   transform failure in the transform failure record and treats its
//   budget as spent by the same attempt-count-or-capped-time rule a send's
//   recorded attempts are measured by)
// Implements: EVS-DEV-destination-key-hold/A+Q+R+T+V+W
// (release — before its walk, the fill
//   releases each releasing key list from its front, only events at or
//   below the fill position and only while no replay request is pending,
//   whatever the window; it transforms and evaluates the block keys outside
//   the transaction, then enqueues the item recording the list's hold id
//   and removes exactly those events from the list in one compare-and-set
//   transaction, removing an emptied list with its clearing event; a
//   failing release is retried under the key's release failure record and,
//   once the budget is spent, holds the key again)
// Implements: EVS-DEV-destination-retry-budget/E+F+G
// (one release failure record per
//   failing key, recording the time of each failure and spent by the same
//   attempt-count-or-capped-time rule)

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
    required this.keyLists,
    required this.releaseFailures,
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

  /// The destination's key lists, by block key: a fill routes a listed
  /// key's events into its list, so a hold or a release that changes them
  /// after the fill read them makes its commit write nothing
  /// (`EVS-DEV-destination-drain/G`).
  final Map<String, KeyList> keyLists;

  /// The destination's release failure records, by block key: a release
  /// step decides from them whether to retry and when to hold the key
  /// again, so a record that changed after the fill read it makes its
  /// commit write nothing.
  final Map<String, ReleaseFailureRecord> releaseFailures;

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
    final keyLists = await backend.readKeyListsTxn(txn, destinationId);
    final releaseFailures = await backend.readReleaseFailuresTxn(
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
      keyLists: keyLists,
      releaseFailures: releaseFailures,
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
      other.transformFailureRecord == transformFailureRecord &&
      const MapEquality<String, KeyList>().equals(other.keyLists, keyLists) &&
      const MapEquality<String, ReleaseFailureRecord>().equals(
        other.releaseFailures,
        releaseFailures,
      );

  @override
  int get hashCode => Object.hash(
    schedule,
    headStatus,
    cursor,
    request,
    refillGuard,
    senderChannel,
    transformFailureRecord,
    const MapEquality<String, KeyList>().hash(keyLists),
    const MapEquality<String, ReleaseFailureRecord>().hash(releaseFailures),
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
  return backend.transaction(
    (txn) => _compareAndSetInTxn(
      backend,
      txn,
      lock,
      destinationId,
      computedFrom,
      () => write(txn),
      cursorAfter: cursorAfter,
    ),
  );
}

/// [_compareAndSet] in a transaction of [registry]'s event store, for a
/// write that appends an event through [registry] (a release step's
/// clearing or hold event). It never moves the fill position.
Future<bool> _compareAndSetPublishing(
  DestinationRegistry registry,
  DrainLock lock,
  String destinationId,
  _FillState computedFrom,
  Future<void> Function(Transaction txn, PublishCollector collector) write,
) async {
  final seam = DeliveryTestHooks.current?.afterFillReads;
  if (seam != null) await seam(destinationId);
  return registry.eventStore.runTransaction(
    (txn, collector) => _compareAndSetInTxn(
      registry._backend,
      txn,
      lock,
      destinationId,
      computedFrom,
      () => write(txn, collector),
    ),
  );
}

/// The body of the fill's compare-and-set inside [txn]: see
/// [_compareAndSet].
Future<bool> _compareAndSetInTxn(
  StorageBackend backend,
  Transaction txn,
  DrainLock lock,
  String destinationId,
  _FillState computedFrom,
  Future<void> Function() write, {
  int? cursorAfter,
}) async {
  await lock.assertHeldInTxn(txn);
  await DeliveryTestHooks.current?.beforeQueueWrites?.call(destinationId);
  final now = await _FillState.readTxn(backend, txn, destinationId);
  if (now != computedFrom) return false;
  await write();
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
///    a gap batch whose transform is retrying commits only its failure
///    record and keeps the request pending (see `_performReplayRequest`).
/// 4. With [registry] (the registry whose event store is over [backend]),
///    every key list in state releasing is released by one step, in key
///    order (see [_releaseStep]), whatever the window.
/// 5. Dormant schedule (`startDate == null`) or a window entirely in the
///    future: nothing further to do.
/// 6. Fetch the events past the position and walk them: an event the filter
///    rejects or that precedes `startDate` is decided; an event past
///    `upper = min(endDate, now)` is deferred and stops the walk; every
///    other event is in the window.
/// 7. No event in the window: advance the position past the decided events
///    (so they are not re-evaluated) and return.
/// 8. Otherwise assemble a greedy batch through `canAddToBatch`, of events
///    whose block key (evaluated outside any transaction on a keyed
///    destination) is the first event's. A batch of a key that has a key
///    list is inserted into that list, untransformed, and the position
///    advances past it. A lone event younger than `maxAccumulateTime` is
///    held (nothing written) unless [flushHeld] is set.
/// 9. Build the item: a native destination gets a library-built envelope
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
/// `() => DateTime.now().toUtc()`. [registry], when given, is the registry
/// whose event store is over [backend]: the release step appends its
/// clearing and hold events through it, and a fill without it releases
/// nothing. [policy] is the retry budget in effect
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
  DestinationRegistry? registry,
}) async {
  final now = (clock ?? () => DateTime.now().toUtc())();
  final id = destination.id;

  // Perform every pending replay request before filling: a request recorded
  // while one is performed is performed next, so the position never
  // advances while a request is pending.
  _FillState state;
  for (;;) {
    state = await _FillState.read(backend, id);
    if (_fillHeld(state, registrationId, declaredFingerprint)) return;
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

  // Release each releasing key list from its front before the walk, whatever
  // the window (`EVS-DEV-destination-key-hold/V`); the walk then reads the
  // state the release left.
  if (registry != null &&
      state.keyLists.values.any((l) => l.state == KeyListState.releasing)) {
    await _releaseLists(
      destination,
      backend: backend,
      registry: registry,
      lock: lock,
      state: state,
      source: source,
      now: now,
      databaseId: databaseId,
      policy: policy,
      cadence: cadence,
      registrationId: registrationId,
      declaredFingerprint: declaredFingerprint,
    );
    state = await _FillState.read(backend, id);
    if (_fillHeld(state, registrationId, declaredFingerprint) ||
        state.request != null) {
      return;
    }
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

  final assembled = await _assembleBatch(
    destination,
    inWindow,
    0,
    _BlockKeys(destination),
  );
  final batch = assembled.batch;
  final key = assembled.key;

  // A listed key's batch is inserted into its list, untransformed, in place
  // of an item; nothing is held for a later event to join it.
  if (key != null && state.keyLists.containsKey(key)) {
    final lists = _routed(state.keyLists, key, batch);
    await _compareAndSet(backend, lock, id, state, (txn) async {
      await writeQueueItemsTxn(txn, backend, id, standalone);
      await backend.writeKeyListsTxn(txn, id, lists);
      await backend.writeFillCursorTxn(txn, id, batch.last.sequenceNumber);
      if (state.transformFailureRecord != null) {
        await backend.clearTransformFailureRecordTxn(txn, id);
      }
    }, cursorAfter: batch.last.sequenceNumber);
    return;
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
    blockKey: key,
    keyError: assembled.keyError,
    keyStackTrace: assembled.keyStackTrace,
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

/// Whether the fill writes nothing for [state]: the destination was
/// deleted, its head is wedged, its persisted registration is not
/// [registrationId], or a refill guard names [declaredFingerprint].
bool _fillHeld(
  _FillState state,
  String? registrationId,
  String? declaredFingerprint,
) {
  if (state.schedule == null || state.headWedged) return true;
  if (registrationId != null &&
      state.schedule!.registrationId != registrationId) {
    return true;
  }
  final guard = state.refillGuard;
  return guard != null && guard.fingerprint == declaredFingerprint;
}

/// The most events of a releasing list's front a release step reads from
/// the log; the step's batch is assembled from them.
const int _releaseFrontLimit = 500;

/// Run one release step ([_releaseStep]) for each of [state]'s releasing
/// key lists, in key order, each on state read afresh. Stops when the fill
/// is held or a replay request is pending.
Future<void> _releaseLists(
  Destination destination, {
  required StorageBackend backend,
  required DestinationRegistry registry,
  required DrainLock lock,
  required _FillState state,
  required Source source,
  required DateTime now,
  required String databaseId,
  required SyncPolicy policy,
  required Duration cadence,
  required String? registrationId,
  required String? declaredFingerprint,
}) async {
  final keys = <String>[
    for (final entry in state.keyLists.entries)
      if (entry.value.state == KeyListState.releasing) entry.key,
  ]..sort();
  for (var i = 0; i < keys.length; i++) {
    final current = i == 0
        ? state
        : await _FillState.read(backend, destination.id);
    if (_fillHeld(current, registrationId, declaredFingerprint) ||
        current.request != null) {
      return;
    }
    await _releaseStep(
      destination,
      registry: registry,
      lock: lock,
      state: current,
      key: keys[i],
      source: source,
      now: now,
      databaseId: databaseId,
      policy: policy,
      cadence: cadence,
    );
  }
}

/// Release the next item of [key]'s list in [state], when the list is
/// releasing.
///
/// The step takes the list's front events whose sequence number is at or
/// below the fill position (none while the position is below the front:
/// a recovery's re-walk may still insert an older event of the key ahead
/// of it), assembles one batch from them, evaluates their block keys and
/// runs the destination's transform, all outside any transaction. While
/// the key's release failure record's backoff after its last failure has
/// not elapsed, it does nothing.
///
/// On success it enqueues the item at the tail with the list's hold id,
/// removes exactly the batch's events from the list (the list itself, with
/// its clearing event, when that empties it) and removes the key's release
/// failure record, in one compare-and-set transaction. On a failure of the
/// transform or the block-key evaluation, or an event of the front missing
/// from the log, it records the failure in the key's release failure
/// record; once the recorded failures have spent [policy]'s retry budget,
/// it instead returns the list to held under a new hold id with cause
/// `transform_failed`, appending the hold event with no queue item and the
/// event it failed to build, and removes the record, in one transaction
/// that leaves the queue items and the wedge record unchanged.
// Implements: EVS-DEV-destination-key-hold/A+Q+R+T+W
// Implements: EVS-DEV-destination-retry-budget/E+F+G
// Implements: EVS-PRD-destination-key-hold/C+E+F+J
// a release queues the key's held events in sequence order, holds the key
//   again once its transform has failed past the retry budget, and appends
//   the clearing event when the list empties.
Future<void> _releaseStep(
  Destination destination, {
  required DestinationRegistry registry,
  required DrainLock lock,
  required _FillState state,
  required String key,
  required Source source,
  required DateTime now,
  required String databaseId,
  required SyncPolicy policy,
  required Duration cadence,
}) async {
  final backend = registry._backend;
  final id = destination.id;
  final list = state.keyLists[key];
  if (list == null || list.state != KeyListState.releasing) return;
  final record = state.releaseFailures[key];
  if (record != null &&
      record.failedAt.isNotEmpty &&
      now.isBefore(
        record.failedAt.last.add(policy.backoffFor(record.failedAt.length)),
      )) {
    return;
  }
  final front = list.events
      .takeWhile((e) => e.sequenceNumber <= state.cursor)
      .take(_releaseFrontLimit)
      .toList();
  if (front.isEmpty) return;
  final events = <StoredEvent>[];
  for (final entry in front) {
    final event = await backend.findEventById(entry.eventId);
    if (event == null) break;
    events.add(event);
  }

  BuiltQueueItem? item;
  var failedEventId = front.first.eventId;
  if (events.isEmpty) {
    libraryLog(
      'sync_cycle',
      'the release of key "$key" of destination "$id" names event '
          '$failedEventId, which is not in the log',
      level: LibraryLogLevel.severe,
    );
  } else {
    final assembled = await _assembleBatch(
      destination,
      events,
      0,
      _BlockKeys(destination),
    );
    failedEventId = assembled.batch.first.eventId;
    try {
      final keyError = assembled.keyError;
      if (keyError != null) {
        Error.throwWithStackTrace(
          keyError,
          assembled.keyStackTrace ?? StackTrace.empty,
        );
      }
      item = await buildQueueItem(
        destination,
        assembled.batch,
        source: source,
        now: now,
        channel: _channelOf(destination, state, databaseId),
        blockKey: assembled.key,
      );
    } on Object catch (e, st) {
      final failed = assembled.keyError != null ? 'block key' : 'transform';
      libraryLog(
        'sync_cycle',
        'the $failed of destination "$id" failed on event $failedEventId '
            'released from key "$key"',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  final failures = <String, ReleaseFailureRecord>{...state.releaseFailures}
    ..remove(key);
  if (item != null) {
    final built = item;
    final releasedIds = <String>{for (final e in built.batch) e.eventId};
    final remaining = <KeyListEvent>[
      for (final e in list.events)
        if (!releasedIds.contains(e.eventId)) e,
    ];
    final lists = <String, KeyList>{...state.keyLists}..remove(key);
    if (remaining.isNotEmpty) {
      lists[key] = KeyList(
        holdId: list.holdId,
        state: KeyListState.releasing,
        cause: list.cause,
        events: remaining,
      );
    }
    await _compareAndSetPublishing(registry, lock, id, state, (
      txn,
      collector,
    ) async {
      await backend.enqueueFifoTxn(
        txn,
        id,
        built.batch,
        wirePayload: built.wirePayload,
        nativeEnvelope: built.nativeEnvelope,
        blockKey: built.blockKey,
        releasedHoldId: list.holdId,
      );
      await backend.writeKeyListsTxn(txn, id, lists);
      if (record != null) {
        await backend.writeReleaseFailuresTxn(txn, id, failures);
      }
      if (remaining.isEmpty) {
        await registry._clearHoldInTxn(
          txn,
          collector,
          destinationId: id,
          key: key,
          holdId: list.holdId,
        );
      }
    });
    return;
  }

  final times = <DateTime>[...?record?.failedAt, now];
  if (retryBudgetSpentAt(times, policy, cadence)) {
    await _compareAndSetPublishing(registry, lock, id, state, (
      txn,
      collector,
    ) async {
      await backend.writeReleaseFailuresTxn(txn, id, failures);
      await registry._holdKeyInTxn(
        txn,
        collector,
        destinationId: id,
        key: key,
        cause: WedgeCause.transformFailed.wire,
        attemptCount: times.length,
        failedEventId: failedEventId,
      );
    });
    return;
  }
  failures[key] = ReleaseFailureRecord(
    failures: times.length,
    failedAt: times,
    lastEventId: failedEventId,
  );
  await _compareAndSet(
    backend,
    lock,
    id,
    state,
    (txn) => backend.writeReleaseFailuresTxn(txn, id, failures),
  );
}

/// Perform [request] under the compare-and-set: build its items outside any
/// transaction, then commit them, the routed key lists, the advanced
/// position, a pending transform failure record and (once every part of
/// the request is resolved) the cleared request together. Returns whether
/// it committed a resolved request.
///
/// Both portions decide their batches through the fill's transform failure
/// path (`EVS-DEV-destination-drain/Y`), and route a listed key's batches
/// into its list (`EVS-DEV-destination-key-hold/L`).
///
/// A gap batch whose transform or block-key evaluation is still backing
/// off, or has failed again without spending its retry budget, leaves the
/// gap unresolved: the call commits only the pending failure record (none
/// while backing off), no item and no list, so the next pass rebuilds the
/// whole gap and enqueues none of its events twice. The request stays
/// pending.
///
/// A first-activation batch that stops the same way stops the walk: the
/// request stays pending and events past the reached position are retried
/// at a later fill, while the items built before it commit. Its record
/// names a batch past the fill position (a gap batch's events lie at or
/// below it), which proves an earlier call already committed the gap
/// portion, so the gap is not rebuilt on a later retry and a stalled
/// activation's retries do not double-enqueue it. A pure gap replay never
/// clears a record past the fill position: that record belongs to an
/// unrelated live fill batch.
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
  final existingRecord = state.transformFailureRecord;
  final items = <BuiltQueueItem>[];
  var keyLists = state.keyLists;
  int? advanceTo;
  TransformFailureRecord? pendingRecord;
  var activationDone = true;
  final gapUpper = request.gapUpper;
  final startDate = schedule.startDate;
  final gapActive =
      gapUpper != null && startDate != null && startDate.isBefore(gapUpper);
  final activationStalled =
      request.firstActivation &&
      existingRecord != null &&
      existingRecord.sequenceRange.firstSeq > state.cursor;
  final runGapPortion = gapActive && !activationStalled;
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
    final gap = await buildGapReplayRows(
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
      keyLists: keyLists,
      policy: policy,
      cadence: cadence,
      existingFailureRecord: existingRecord,
    );
    if (gap.stoppedEarly) {
      final record = gap.pendingFailureRecord;
      if (record == null) return false;
      await _compareAndSet(backend, lock, id, state, (txn) async {
        await backend.writeTransformFailureRecordTxn(txn, id, record);
      });
      return false;
    }
    items.addAll(gap.items);
    keyLists = gap.keyLists;
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
      existingFailureRecord: existingRecord,
      alreadyQueuedChannelWide: alreadyQueuedChannelWide,
      keyLists: keyLists,
    );
    items.addAll(build.items);
    keyLists = build.keyLists;
    advanceTo = build.cursor;
    pendingRecord = build.pendingFailureRecord;
    activationDone = !build.stoppedEarly;
  }
  final cursor = advanceTo;
  // The record this function clears is one a batch of this request named:
  // whatever record a first activation's walk consulted, or a gap batch's,
  // at or below the fill position.
  final ownsRecord =
      existingRecord != null &&
      (request.firstActivation ||
          existingRecord.sequenceRange.lastSeq <= state.cursor);
  final clearsRecord = activationDone && pendingRecord == null && ownsRecord;
  final recordChanges = pendingRecord != null || clearsRecord;
  final listsChange = !const MapEquality<String, KeyList>().equals(
    keyLists,
    state.keyLists,
  );
  if (items.isEmpty &&
      cursor == null &&
      !recordChanges &&
      !listsChange &&
      !activationDone) {
    // Nothing at all would change: the same "still backing off, write
    // nothing" outcome the live fill returns without a transaction.
    return false;
  }
  final committed = await _compareAndSet(backend, lock, id, state, (txn) async {
    await writeQueueItemsTxn(txn, backend, id, items);
    if (listsChange) await backend.writeKeyListsTxn(txn, id, keyLists);
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
