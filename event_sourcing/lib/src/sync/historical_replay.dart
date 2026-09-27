// Implements: EVS-PRD-destinations/B
// (per-destination filter — both replay
//   builders evaluate destination.filter.matches on every candidate event,
//   using the same admission semantics as the live fill)
// Implements: EVS-PRD-destinations/C
// (FIFO order — the builders batch events in
//   sequence_number order)
// Implements: EVS-DEV-destination-drain/E
// (a replay is built only by the
//   drainer's fill, from the destination the drainer registers; a gap
//   replay admits only events at or below the fill position)
// Implements: EVS-DEV-destination-drain/G
// (the builders read nothing and write
//   nothing: they run the destination's transform outside any transaction,
//   and the fill commits their rows under its compare-and-set)

part of '../event_store.dart';

const _uuidGen = Uuid();

/// One queue item a replay or a fill built outside any transaction, ready to
/// be enqueued inside the fill's compare-and-set transaction.
@internal
class BuiltQueueItem {
  @internal
  const BuiltQueueItem({
    required this.batch,
    this.wirePayload,
    this.nativeEnvelope,
    this.transformFailed = false,
    this.transformFailures,
    this.wireFormat,
  });

  /// The events the item carries, in `sequence_number` order.
  final List<StoredEvent> batch;

  /// The transform's output, for a destination that owns its wire format.
  final WirePayload? wirePayload;

  /// The library-built envelope identity, for a native destination.
  final BatchEnvelopeMetadata? nativeEnvelope;

  /// True for a batch whose transform kept failing until the destination's
  /// retry budget was spent (`EVS-DEV-destination-drain/Y`): the item
  /// carries no payload and no envelope, and [transformFailures] and
  /// [wireFormat] are set instead.
  final bool transformFailed;

  /// The count of recorded transform failures, set only when
  /// [transformFailed] is true.
  final int? transformFailures;

  /// The destination's configured wire format, set only when
  /// [transformFailed] is true (a transform-failed item supplies no
  /// payload or envelope to infer it from).
  final String? wireFormat;
}

/// Build one queue item for [batch]: a native destination gets a
/// library-built envelope minted from [source] for its delivery [channel];
/// any other destination's `transform` runs here, outside any transaction.
// Implements: EVS-DEV-delivery-channel/A
// a destination that serializes natively is a delivery channel: its items
//   carry the channel and the delivery's attributes (empty in every
//   delivery the library sends), with no number, which the drainer assigns
//   at its pre-send fence.
@internal
Future<BuiltQueueItem> buildQueueItem(
  Destination destination,
  List<StoredEvent> batch, {
  required Source? source,
  required DateTime now,
  DeliveryChannel? channel,
}) async {
  if (destination.serializesNatively) {
    if (source == null) {
      throw ArgumentError(
        'destination "${destination.id}" declares serializesNatively == '
        'true but no source was supplied; native batches require a Source '
        'to stamp the envelope identity.',
      );
    }
    if (channel == null) {
      throw ArgumentError(
        'destination "${destination.id}" declares serializesNatively == '
        'true but no delivery channel was supplied; a native item is '
        'enqueued on its channel.',
      );
    }
    return BuiltQueueItem(
      batch: batch,
      nativeEnvelope: BatchEnvelopeMetadata(
        batchFormatVersion: DeliveryEnvelope.batchFormatVersion,
        batchId: _uuidGen.v4(),
        senderHop: source.hopId,
        senderIdentifier: source.identifier,
        senderSoftwareVersion: source.softwareVersion,
        sentAt: now,
        channel: channel,
        attributes: const <String, Object?>{},
      ),
    );
  }
  final transformed = destination.transform(batch);
  final seam = DeliveryTestHooks.current?.insideTransform;
  if (seam != null) {
    // Both futures are listened to at once, so a transform that fails while
    // the seam runs fails this call instead of escaping as an unhandled
    // error.
    await Future.wait<Object?>(<Future<Object?>>[
      transformed,
      seam(destination.id),
    ]);
  }
  return BuiltQueueItem(batch: batch, wirePayload: await transformed);
}

/// Split [events] into greedy batches with the destination's
/// `canAddToBatch` and build a queue item for each, to completion. The first
/// event of each batch is seeded unconditionally; `canAddToBatch` is
/// consulted from the second event onward.
Future<List<BuiltQueueItem>> _buildAll(
  Destination destination,
  List<StoredEvent> events, {
  required Source? source,
  required DateTime now,
  required DeliveryChannel? channel,
}) async {
  final items = <BuiltQueueItem>[];
  var i = 0;
  while (i < events.length) {
    final batch = <StoredEvent>[events[i]];
    i++;
    while (i < events.length && destination.canAddToBatch(batch, events[i])) {
      batch.add(events[i]);
      i++;
    }
    items.add(
      await buildQueueItem(
        destination,
        batch,
        source: source,
        now: now,
        channel: channel,
      ),
    );
  }
  return items;
}

/// What a historical replay built: its queue items, the fill position it
/// advances to (null when it decided no event), a transform failure record
/// still awaiting its retry budget (null when none is pending), and whether
/// the walk stopped before every admitted event was decided.
///
/// [stoppedEarly] is true when a batch's transform is still within its
/// retry curve's backoff, or has failed again without spending its budget:
/// the replay request stays pending, and the events past [cursor] are
/// retried at a later fill. It is false when the walk reached the end of
/// the admitted candidates, whatever the destination's transform reported
/// along the way — every batch either enqueued (as itself or as
/// transform-failed) or was permanently decided.
@internal
class HistoricalReplayBuild {
  @internal
  const HistoricalReplayBuild({
    required this.items,
    required this.cursor,
    this.pendingFailureRecord,
    this.stoppedEarly = false,
  });

  final List<BuiltQueueItem> items;
  final int? cursor;
  final TransformFailureRecord? pendingFailureRecord;
  final bool stoppedEarly;
}

/// Decide the next write for [batch]: a native destination's envelope is
/// built directly (it runs no transform, so it consults no failure
/// record). Any other destination's transform runs, gated by
/// [existingRecord] when it names this batch's exact sequence range: while
/// `now` is before the last recorded failure plus the retry curve's
/// backoff after that many failures, the transform does not rerun and
/// nothing is decided (a `skip` decision). On a transform failure the
/// failure is logged severe and recorded: once the recorded failures
/// (this one included) have spent [policy]'s retry budget, the batch
/// decides as one transform-failed item; otherwise nothing is decided and
/// the failure record to write is returned instead.
// Implements: EVS-DEV-destination-drain/Y
// Implements: EVS-DEV-destination-retry-budget/B
Future<_BatchDecision> _decideBatch(
  Destination destination,
  List<StoredEvent> batch, {
  required TransformFailureRecord? existingRecord,
  required Source? source,
  required DateTime now,
  required DeliveryChannel? channel,
  required SyncPolicy policy,
  required Duration cadence,
}) async {
  if (destination.serializesNatively) {
    final item = await buildQueueItem(
      destination,
      batch,
      source: source,
      now: now,
      channel: channel,
    );
    return (skip: false, item: item, pendingRecord: null);
  }
  final range = (
    firstSeq: batch.first.sequenceNumber,
    lastSeq: batch.last.sequenceNumber,
  );
  final matches =
      existingRecord != null && existingRecord.sequenceRange == range;
  if (matches) {
    final nextAllowed = existingRecord.failureTimes.last.add(
      policy.backoffFor(existingRecord.failureTimes.length),
    );
    if (now.isBefore(nextAllowed)) {
      return (skip: true, item: null, pendingRecord: null);
    }
  }
  try {
    final item = await buildQueueItem(
      destination,
      batch,
      source: source,
      now: now,
      channel: channel,
    );
    return (skip: false, item: item, pendingRecord: null);
  } on Object catch (e, st) {
    libraryLog(
      'sync_cycle',
      'the transform of destination "${destination.id}" failed on events '
          '${range.firstSeq}-${range.lastSeq}',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
    final failureTimes = <DateTime>[
      if (matches) ...existingRecord.failureTimes,
      now,
    ];
    if (retryBudgetSpentAt(failureTimes, policy, cadence)) {
      return (
        skip: false,
        item: BuiltQueueItem(
          batch: batch,
          transformFailed: true,
          transformFailures: failureTimes.length,
          wireFormat: destination.wireFormat,
        ),
        pendingRecord: null,
      );
    }
    return (
      skip: false,
      item: null,
      pendingRecord: TransformFailureRecord(
        failureTimes: failureTimes,
        sequenceRange: range,
      ),
    );
  }
}

/// The outcome of deciding one batch: `skip` true means the transform is
/// still within its retry curve's backoff (nothing to run or write this
/// pass); otherwise `item` is the batch's decided queue item (itself, or a
/// transform-failed marker) or, when the transform failed without yet
/// spending the retry budget, `pendingRecord` names the failure record to
/// write instead.
typedef _BatchDecision = ({
  bool skip,
  BuiltQueueItem? item,
  TransformFailureRecord? pendingRecord,
});

/// Build the historical replay a first activation requests: every event in
/// [candidates] (the events past the fill position, in `sequence_number`
/// order) whose client timestamp lies in `[startDate, upper]`, with
/// `upper = min(endDate, now)`, that the destination's filter admits,
/// batched to completion.
///
/// The walk has the fill's admission semantics: an event the filter rejects
/// or that precedes [startDate] is decided (the position may pass it); an
/// event past `upper` is deferred and stops the walk, so the position stays
/// in front of it. Unlike the live fill, a lone trailing event is not held
/// for `maxAccumulateTime`: historical events are not live arrivals.
///
/// Reads nothing and writes nothing; the caller commits the items, the
/// returned position and a pending failure record under its
/// compare-and-set.
///
/// [existingFailureRecord] is the destination's persisted transform
/// failure record, consulted only for the first batch the walk decides
/// (`EVS-DEV-destination-drain/Y`): a later batch in the same call starts
/// with no existing record, since only one batch's failure is ever
/// pending at a time and an earlier batch in this same walk cannot be the
/// one the persisted record names. The walk stops at the first batch that
/// is still backing off or whose failure does not yet spend [policy]'s
/// budget ([HistoricalReplayBuild.stoppedEarly]); a batch that spends the
/// budget decides as a transform-failed item and the walk continues past
/// it.
// Implements: EVS-DEV-destination-drain/X
// (channel-wide events — a security
//   finding or a succession event this database appended bypasses both the
//   destination's filter and its schedule window on a natively serializing
//   registration; the walk finds it beyond a dormant or not-yet-started
//   window, or beyond an event it defers, and enqueues it there without
//   moving the position, deduplicated against the destination's currently
//   queued items)
@internal
Future<HistoricalReplayBuild> buildHistoricalReplayRows(
  Destination destination,
  List<StoredEvent> candidates, {
  required DateTime? startDate,
  required DateTime? endDate,
  required DateTime now,
  required Source? source,
  required String databaseId,
  required SyncPolicy policy,
  required Duration cadence,
  required Set<String> alreadyQueuedChannelWide,
  TransformFailureRecord? existingFailureRecord,
  DeliveryChannel? channel,
}) async {
  final upper = endDate == null || endDate.isAfter(now) ? now : endDate;
  final windowDormant = startDate == null || startDate.isAfter(upper);
  if (windowDormant && !destination.serializesNatively) {
    return const HistoricalReplayBuild(items: <BuiltQueueItem>[], cursor: null);
  }
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
    return HistoricalReplayBuild(
      items: standalone,
      cursor: walk.lastDecidedSeq,
    );
  }
  final items = <BuiltQueueItem>[];
  var existing = existingFailureRecord;
  TransformFailureRecord? pendingFailureRecord;
  var stoppedEarly = false;
  int? cursor;
  var i = 0;
  while (i < inWindow.length) {
    final batch = <StoredEvent>[inWindow[i]];
    i++;
    while (i < inWindow.length &&
        destination.canAddToBatch(batch, inWindow[i])) {
      batch.add(inWindow[i]);
      i++;
    }
    final decision = await _decideBatch(
      destination,
      batch,
      existingRecord: existing,
      source: source,
      now: now,
      channel: channel,
      policy: policy,
      cadence: cadence,
    );
    if (decision.skip) {
      stoppedEarly = true;
      break;
    }
    if (decision.pendingRecord != null) {
      pendingFailureRecord = decision.pendingRecord;
      stoppedEarly = true;
      break;
    }
    items.add(decision.item!);
    cursor = batch.last.sequenceNumber;
    existing = null; // resolved: the next batch consults no earlier record.
  }
  items.addAll(standalone);
  return HistoricalReplayBuild(
    items: items,
    cursor: cursor ?? (stoppedEarly ? null : walk.lastDecidedSeq),
    pendingFailureRecord: pendingFailureRecord,
    stoppedEarly: stoppedEarly,
  );
}

/// Build the gap replay a backward start-date move requests: every event in
/// [events] whose sequence number is at or below [fillCursor], that the
/// destination's filter admits, whose client timestamp lies in
/// `[startDate, gapUpper)` — or, for a channel-wide own event on a natively
/// serializing destination not already covered by [alreadyQueuedChannelWide]
/// (`EVS-DEV-destination-drain/X`), whatever its client timestamp — batched
/// to completion.
///
/// Events past [fillCursor] are left to the fill, which evaluates them
/// against the new start date, so no event is enqueued by both. Reads
/// nothing and writes nothing; the gap replay does not move the fill
/// position.
@internal
Future<List<BuiltQueueItem>> buildGapReplayRows(
  Destination destination,
  List<StoredEvent> events, {
  required DateTime startDate,
  required DateTime gapUpper,
  required int fillCursor,
  required DateTime now,
  required Source? source,
  required String databaseId,
  required Set<String> alreadyQueuedChannelWide,
  DeliveryChannel? channel,
}) {
  final inGap = <StoredEvent>[];
  for (final e in events) {
    if (e.sequenceNumber > fillCursor || !e.isHeldAsAuthoredBy(databaseId)) {
      continue;
    }
    final channelWide =
        destination.serializesNatively && isChannelWideEntryType(e.entryType);
    if (channelWide) {
      if (!alreadyQueuedChannelWide.contains(e.eventId)) inGap.add(e);
      continue;
    }
    if (!destination.filter.matches(e)) continue;
    if (e.clientTimestamp.isBefore(startDate)) continue;
    if (!e.clientTimestamp.isBefore(gapUpper)) continue;
    inGap.add(e);
  }
  return _buildAll(
    destination,
    inGap,
    source: source,
    now: now,
    channel: channel,
  );
}

/// The event ids [entries] carry that still count as enqueued for
/// dedup — every event of a pending or wedged entry, and every event of a
/// sent entry acknowledged under [currentGeneration]. A tombstoned entry,
/// and a sent entry acknowledged under another generation, contribute
/// nothing: the channel-wide bypass (`EVS-DEV-destination-drain/X`) must
/// enqueue a succession event or a security finding again once a resume, a
/// new generation or an operator recovery has retired or removed the item
/// that carried it before.
@internal
Set<String> queuedChannelWideEventIds(
  List<FifoEntry> entries,
  int? currentGeneration,
) {
  final ids = <String>{};
  for (final entry in entries) {
    switch (entry.finalStatus) {
      case null:
      case FinalStatus.wedged:
        ids.addAll(entry.eventIds);
      case FinalStatus.sent:
        if (currentGeneration != null &&
            entry.deliveryGeneration == currentGeneration) {
          ids.addAll(entry.eventIds);
        }
      case FinalStatus.tombstoned:
        break;
    }
  }
  return ids;
}

/// What one admission walk over a natively-serializing-aware candidate list
/// decided: the ordinary events in the schedule's window
/// ([inWindow], batched and built by the caller as before), the
/// channel-wide own events (a succession event or a security finding) found
/// beyond the point the walk could otherwise decide
/// ([standaloneChannelWide] — enqueued on their own, never batched with an
/// ordinary event, and never moving the position), and the highest sequence
/// number the walk decided ([lastDecidedSeq], null when it decided
/// nothing).
///
/// `EVS-DEV-destination-drain/X`: a succession event or a security finding
/// this database appended bypasses both the destination's filter and its
/// schedule window on a natively serializing registration — precedes the
/// start date, follows a passed end date, or arrives while the schedule is
/// dormant or its window lies entirely in the future. Once the walk has met
/// an ordinary event it must defer (its client timestamp is after the
/// upper bound), the position cannot advance past it, so a channel-wide
/// event beyond that point is enqueued as its own item without moving
/// [lastDecidedSeq]; the walk starts already deferred for a dormant
/// schedule or a window entirely in the future, so every ordinary event is
/// left undecided while the walk still finds every channel-wide event. A
/// channel-wide event already covered by the destination's currently
/// queued items (`queuedChannelWideEventIds`) is decided like a filter
/// rejection instead of enqueued again, so the position may still pass it
/// once the walk is not deferred.
@internal
class AdmissionWalk {
  const AdmissionWalk({
    required this.inWindow,
    required this.standaloneChannelWide,
    required this.lastDecidedSeq,
  });

  final List<StoredEvent> inWindow;
  final List<StoredEvent> standaloneChannelWide;
  final int? lastDecidedSeq;
}

@internal
AdmissionWalk walkAdmission(
  Destination destination,
  List<StoredEvent> candidates, {
  required String databaseId,
  required DateTime? startDate,
  required DateTime upper,
  required Set<String> alreadyQueued,
  required bool initiallyDeferred,
}) {
  final inWindow = <StoredEvent>[];
  final standaloneChannelWide = <StoredEvent>[];
  int? lastDecidedSeq;
  var deferred = initiallyDeferred;
  for (final e in candidates) {
    if (!e.isHeldAsAuthoredBy(databaseId)) {
      // EVS-DEV-destination-drain/V: not this database's own event.
      if (!deferred) lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    final channelWide =
        destination.serializesNatively && isChannelWideEntryType(e.entryType);
    if (channelWide) {
      if (alreadyQueued.contains(e.eventId)) {
        if (!deferred) lastDecidedSeq = e.sequenceNumber;
        continue;
      }
      if (deferred) {
        standaloneChannelWide.add(e);
      } else {
        inWindow.add(e);
        lastDecidedSeq = e.sequenceNumber;
      }
      continue;
    }
    if (deferred) continue; // behind a deferred event: irrelevant this pass
    if (!destination.filter.matches(e)) {
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (startDate != null && e.clientTimestamp.isBefore(startDate)) {
      // A later backward start-date move records a gap replay for events
      // behind the position, so the fill need not keep these re-evaluable.
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (e.clientTimestamp.isAfter(upper)) {
      deferred = true;
      continue;
    }
    inWindow.add(e);
    lastDecidedSeq = e.sequenceNumber;
  }
  return AdmissionWalk(
    inWindow: inWindow,
    standaloneChannelWide: standaloneChannelWide,
    lastDecidedSeq: lastDecidedSeq,
  );
}

/// Build one queue item per event of [events] (a natively serializing
/// destination's channel-wide own events found beyond the point an
/// admission walk could decide): each is its own item, via [buildQueueItem]
/// directly (a native destination never runs a transform), never batched
/// with another event.
@internal
Future<List<BuiltQueueItem>> buildStandaloneChannelWideItems(
  Destination destination,
  List<StoredEvent> events, {
  required Source? source,
  required DateTime now,
  required DeliveryChannel? channel,
}) async => <BuiltQueueItem>[
  for (final e in events)
    await buildQueueItem(
      destination,
      <StoredEvent>[e],
      source: source,
      now: now,
      channel: channel,
    ),
];

/// Enqueue [items] on [destinationId]'s queue inside [txn], in order.
@internal
Future<void> writeQueueItemsTxn(
  Transaction txn,
  StorageBackend backend,
  String destinationId,
  List<BuiltQueueItem> items,
) async {
  for (final item in items) {
    await backend.enqueueFifoTxn(
      txn,
      destinationId,
      item.batch,
      wirePayload: item.wirePayload,
      nativeEnvelope: item.nativeEnvelope,
      transformFailed: item.transformFailed,
      transformFailures: item.transformFailures,
      wireFormat: item.transformFailed ? item.wireFormat : null,
    );
  }
}
