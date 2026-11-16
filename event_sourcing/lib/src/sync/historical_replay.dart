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
// Implements: EVS-DEV-destination-drain/Y
// (a gap, historical or first-activation
//   replay decides each batch through the fill's transform failure path:
//   a failing transform or block-key evaluation is recorded and retried
//   under the retry curve)

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
    this.blockKey,
  });

  /// The events the item carries, in `sequence_number` order.
  final List<StoredEvent> batch;

  /// The block key every event of [batch] carries, or null for an item
  /// with no block key.
  final String? blockKey;

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
  String? blockKey,
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
  return BuiltQueueItem(
    batch: batch,
    wirePayload: await transformed,
    blockKey: blockKey,
  );
}

/// The outcome of evaluating one event's block key: the key (null for an
/// event with no key), or the error the evaluation threw.
typedef _KeyOutcome = ({String? key, Object? error, StackTrace? stackTrace});

/// The block keys of a destination's events, each evaluated at most once
/// per fill pass, outside any transaction. A destination that is not keyed
/// evaluates nothing: every event has no key. A key containing `|` counts
/// as no key.
// Implements: EVS-DEV-destination-key-hold/A+D+E
// the fill and every replay evaluate a keyed destination's block key of
//   each event outside the fill's transaction; an unkeyed destination's
//   events carry no key, and a key containing `|` is queued keyless.
class _BlockKeys {
  _BlockKeys(this.destination);

  final Destination destination;
  final Map<String, _KeyOutcome> _outcomes = <String, _KeyOutcome>{};

  Future<_KeyOutcome> of(StoredEvent event) async {
    if (!destination.keyed) return (key: null, error: null, stackTrace: null);
    final known = _outcomes[event.eventId];
    if (known != null) return known;
    _KeyOutcome outcome;
    try {
      final key = await destination.blockKeyOf(event);
      outcome = (
        key: key != null && key.contains('|') ? null : key,
        error: null,
        stackTrace: null,
      );
    } on Object catch (e, st) {
      outcome = (key: null, error: e, stackTrace: st);
    }
    return _outcomes[event.eventId] = outcome;
  }
}

/// One batch assembled from a list of events: the batch, its block key,
/// and the error its first event's block-key evaluation threw (the batch is
/// then that event alone).
typedef _Assembled = ({
  List<StoredEvent> batch,
  String? key,
  Object? keyError,
  StackTrace? keyStackTrace,
});

/// Assemble the greedy batch that starts at [events]`[start]`: the first
/// event is seeded unconditionally; a later event joins while the
/// destination's `canAddToBatch` accepts it and its block key evaluates
/// to the first event's. An event whose key evaluation throws ends the
/// batch before it, and starts a batch of its own that fails.
// Implements: EVS-DEV-destination-key-hold/C
// a queue item carries only events of one block key, no block key
//   counting as one.
Future<_Assembled> _assembleBatch(
  Destination destination,
  List<StoredEvent> events,
  int start,
  _BlockKeys keys,
) async {
  final batch = <StoredEvent>[events[start]];
  final head = await keys.of(events[start]);
  if (head.error != null) {
    return (
      batch: batch,
      key: null,
      keyError: head.error,
      keyStackTrace: head.stackTrace,
    );
  }
  for (var i = start + 1; i < events.length; i++) {
    final candidate = events[i];
    if (!destination.canAddToBatch(batch, candidate)) break;
    final outcome = await keys.of(candidate);
    if (outcome.error != null || outcome.key != head.key) break;
    batch.add(candidate);
  }
  return (batch: batch, key: head.key, keyError: null, keyStackTrace: null);
}

/// [lists] with every event of [batch] inserted into [key]'s list at its
/// sequence position; an event the list already holds leaves it unchanged.
// Implements: EVS-DEV-destination-key-hold/L+M
// a listed key's events are inserted into its list at their sequence
//   position, untransformed, instead of being enqueued; an event the list
//   already holds leaves it unchanged.
Map<String, KeyList> _routed(
  Map<String, KeyList> lists,
  String key,
  List<StoredEvent> batch,
) {
  var list = lists[key]!;
  for (final e in batch) {
    list = list.withInserted(
      KeyListEvent(eventId: e.eventId, sequenceNumber: e.sequenceNumber),
    );
  }
  return <String, KeyList>{...lists, key: list};
}

/// What a replay's walk over its admitted events built: the queue items,
/// the key lists with the listed keys' events routed in, the last event
/// the walk resolved (enqueued or routed; null when none), a transform
/// failure record awaiting its retry budget, and whether the walk stopped
/// at a batch that is backing off or failed without spending the budget.
typedef _ReplayWalk = ({
  List<BuiltQueueItem> items,
  Map<String, KeyList> keyLists,
  int? lastResolvedSeq,
  TransformFailureRecord? pendingFailureRecord,
  bool stoppedEarly,
});

/// Walk [events] batch by batch ([_assembleBatch]): a batch of a key that
/// has a list in [keyLists] is routed into it; every other batch is decided
/// by [_decideBatch] against [existingRecord], which names at most one
/// batch, by its exact sequence range. The walk stops at the first batch
/// that is still backing off or whose failure does not yet spend
/// [policy]'s budget.
// Implements: EVS-DEV-destination-key-hold/A+B+C+G+L
// Implements: EVS-DEV-destination-drain/Y
Future<_ReplayWalk> _walkReplayBatches(
  Destination destination,
  List<StoredEvent> events, {
  required Map<String, KeyList> keyLists,
  required TransformFailureRecord? existingRecord,
  required Source? source,
  required DateTime now,
  required DeliveryChannel? channel,
  required SyncPolicy policy,
  required Duration cadence,
}) async {
  final keys = _BlockKeys(destination);
  final items = <BuiltQueueItem>[];
  var lists = keyLists;
  int? lastResolvedSeq;
  var i = 0;
  while (i < events.length) {
    final assembled = await _assembleBatch(destination, events, i, keys);
    final batch = assembled.batch;
    i += batch.length;
    final key = assembled.key;
    if (key != null && lists.containsKey(key)) {
      lists = _routed(lists, key, batch);
      lastResolvedSeq = batch.last.sequenceNumber;
      continue;
    }
    final decision = await _decideBatch(
      destination,
      batch,
      existingRecord: existingRecord,
      source: source,
      now: now,
      channel: channel,
      policy: policy,
      cadence: cadence,
      blockKey: key,
      keyError: assembled.keyError,
      keyStackTrace: assembled.keyStackTrace,
    );
    if (decision.skip || decision.pendingRecord != null) {
      return (
        items: items,
        keyLists: lists,
        lastResolvedSeq: lastResolvedSeq,
        pendingFailureRecord: decision.pendingRecord,
        stoppedEarly: true,
      );
    }
    items.add(decision.item!);
    lastResolvedSeq = batch.last.sequenceNumber;
  }
  return (
    items: items,
    keyLists: lists,
    lastResolvedSeq: lastResolvedSeq,
    pendingFailureRecord: null,
    stoppedEarly: false,
  );
}

/// What a historical or gap replay built: its queue items, the key lists
/// with the listed keys' events routed in, the fill position it advances
/// to (null when it decided no event, and always null for a gap replay,
/// which leaves the position), a transform failure record still awaiting
/// its retry budget (null when none is pending), and whether the walk
/// stopped before every admitted event was decided.
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
    required this.keyLists,
    this.pendingFailureRecord,
    this.stoppedEarly = false,
  });

  final List<BuiltQueueItem> items;
  final int? cursor;
  final Map<String, KeyList> keyLists;
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
///
/// [blockKey] is the batch's block key, stored on the item it decides.
/// [keyError] is the error the block-key evaluation of the batch threw:
/// the batch then fails as its transform would, without running it.
// Implements: EVS-DEV-destination-drain/Y
// Implements: EVS-DEV-destination-retry-budget/B
// Implements: EVS-DEV-destination-key-hold/B+G
// a failing block-key evaluation follows the transform's failure path;
//   the decided item carries the batch's block key.
Future<_BatchDecision> _decideBatch(
  Destination destination,
  List<StoredEvent> batch, {
  required TransformFailureRecord? existingRecord,
  required Source? source,
  required DateTime now,
  required DeliveryChannel? channel,
  required SyncPolicy policy,
  required Duration cadence,
  String? blockKey,
  Object? keyError,
  StackTrace? keyStackTrace,
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
    if (keyError != null) {
      Error.throwWithStackTrace(keyError, keyStackTrace ?? StackTrace.empty);
    }
    final item = await buildQueueItem(
      destination,
      batch,
      source: source,
      now: now,
      channel: channel,
      blockKey: blockKey,
    );
    return (skip: false, item: item, pendingRecord: null);
  } on Object catch (e, st) {
    final failed = keyError != null ? 'block key' : 'transform';
    libraryLog(
      'sync_cycle',
      'the $failed of destination "${destination.id}" failed on events '
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
          blockKey: blockKey,
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
/// failure record (`EVS-DEV-destination-drain/Y`), which names at most one
/// batch by its exact sequence range. The walk stops at the first batch
/// that is still backing off or whose failure does not yet spend
/// [policy]'s budget ([HistoricalReplayBuild.stoppedEarly]); a batch that
/// spends the budget decides as a transform-failed item and the walk
/// continues past it. A batch of a key that has a list in [keyLists] is
/// routed into it instead of enqueued (`EVS-DEV-destination-key-hold/L`).
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
  required Map<String, KeyList> keyLists,
  TransformFailureRecord? existingFailureRecord,
  DeliveryChannel? channel,
}) async {
  final upper = endDate == null || endDate.isAfter(now) ? now : endDate;
  final windowDormant = startDate == null || startDate.isAfter(upper);
  if (windowDormant && !destination.serializesNatively) {
    return HistoricalReplayBuild(
      items: const <BuiltQueueItem>[],
      cursor: null,
      keyLists: keyLists,
    );
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
      keyLists: keyLists,
    );
  }
  final built = await _walkReplayBatches(
    destination,
    inWindow,
    keyLists: keyLists,
    existingRecord: existingFailureRecord,
    source: source,
    now: now,
    channel: channel,
    policy: policy,
    cadence: cadence,
  );
  return HistoricalReplayBuild(
    items: <BuiltQueueItem>[...built.items, ...standalone],
    cursor:
        built.lastResolvedSeq ??
        (built.stoppedEarly ? null : walk.lastDecidedSeq),
    keyLists: built.keyLists,
    pendingFailureRecord: built.pendingFailureRecord,
    stoppedEarly: built.stoppedEarly,
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
/// position (the build's cursor is null).
///
/// Each batch decides as a historical replay's does
/// ([buildHistoricalReplayRows]): a listed key's batch is routed into its
/// list in [keyLists], and a failing transform or block-key evaluation is
/// gated and recorded against [existingFailureRecord] under [policy]'s
/// retry curve. A walk that stops early leaves the gap unresolved: its
/// caller commits none of the built items, only the pending record.
@internal
Future<HistoricalReplayBuild> buildGapReplayRows(
  Destination destination,
  List<StoredEvent> events, {
  required DateTime startDate,
  required DateTime gapUpper,
  required int fillCursor,
  required DateTime now,
  required Source? source,
  required String databaseId,
  required Set<String> alreadyQueuedChannelWide,
  required Map<String, KeyList> keyLists,
  required SyncPolicy policy,
  required Duration cadence,
  TransformFailureRecord? existingFailureRecord,
  DeliveryChannel? channel,
}) async {
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
  final built = await _walkReplayBatches(
    destination,
    inGap,
    keyLists: keyLists,
    existingRecord: existingFailureRecord,
    source: source,
    now: now,
    channel: channel,
    policy: policy,
    cadence: cadence,
  );
  return HistoricalReplayBuild(
    items: built.items,
    cursor: null,
    keyLists: built.keyLists,
    pendingFailureRecord: built.pendingFailureRecord,
    stoppedEarly: built.stoppedEarly,
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
      // Implements: EVS-DEV-destination-drain/V
      // Not this database's own event.
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
      blockKey: item.blockKey,
    );
  }
}
