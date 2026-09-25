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
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:uuid/uuid.dart';

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
  });

  /// The events the item carries, in `sequence_number` order.
  final List<StoredEvent> batch;

  /// The transform's output, for a destination that owns its wire format.
  final WirePayload? wirePayload;

  /// The library-built envelope identity, for a native destination.
  final BatchEnvelopeMetadata? nativeEnvelope;
}

/// Build one queue item for [batch]: a native destination gets a
/// library-built envelope minted from [source]; any other destination's
/// `transform` runs here, outside any transaction.
@internal
Future<BuiltQueueItem> buildQueueItem(
  Destination destination,
  List<StoredEvent> batch, {
  required Source? source,
  required DateTime now,
}) async {
  if (destination.serializesNatively) {
    if (source == null) {
      throw ArgumentError(
        'destination "${destination.id}" declares serializesNatively == '
        'true but no source was supplied; native batches require a Source '
        'to stamp the envelope identity.',
      );
    }
    return BuiltQueueItem(
      batch: batch,
      nativeEnvelope: BatchEnvelopeMetadata(
        batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
        batchId: _uuidGen.v4(),
        senderHop: source.hopId,
        senderIdentifier: source.identifier,
        senderSoftwareVersion: source.softwareVersion,
        sentAt: now,
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
      await buildQueueItem(destination, batch, source: source, now: now),
    );
  }
  return items;
}

/// What a historical replay built: its queue items and the fill position
/// it advances to (null when it decided no event).
@internal
class HistoricalReplayBuild {
  @internal
  const HistoricalReplayBuild({required this.items, required this.cursor});

  final List<BuiltQueueItem> items;
  final int? cursor;
}

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
/// Reads nothing and writes nothing; the caller commits the items and the
/// returned position under its compare-and-set.
@internal
Future<HistoricalReplayBuild> buildHistoricalReplayRows(
  Destination destination,
  List<StoredEvent> candidates, {
  required DateTime? startDate,
  required DateTime? endDate,
  required DateTime now,
  required Source? source,
}) async {
  if (startDate == null) {
    return const HistoricalReplayBuild(items: <BuiltQueueItem>[], cursor: null);
  }
  final upper = endDate == null || endDate.isAfter(now) ? now : endDate;
  if (startDate.isAfter(upper)) {
    return const HistoricalReplayBuild(items: <BuiltQueueItem>[], cursor: null);
  }
  final inWindow = <StoredEvent>[];
  int? lastDecidedSeq;
  for (final e in candidates) {
    if (!destination.filter.matches(e)) {
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (e.clientTimestamp.isBefore(startDate)) {
      lastDecidedSeq = e.sequenceNumber;
      continue;
    }
    if (e.clientTimestamp.isAfter(upper)) break;
    inWindow.add(e);
    lastDecidedSeq = e.sequenceNumber;
  }
  final items = await _buildAll(
    destination,
    inWindow,
    source: source,
    now: now,
  );
  // With items, the position advances to the last replayed event (as the
  // fill's does); with none, past the decided events.
  return HistoricalReplayBuild(
    items: items,
    cursor: inWindow.isEmpty ? lastDecidedSeq : inWindow.last.sequenceNumber,
  );
}

/// Build the gap replay a backward start-date move requests: every event in
/// [events] whose sequence number is at or below [fillCursor], whose client
/// timestamp lies in `[startDate, gapUpper)`, and that the destination's
/// filter admits, batched to completion.
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
}) {
  final inGap = <StoredEvent>[
    for (final e in events)
      if (e.sequenceNumber <= fillCursor &&
          destination.filter.matches(e) &&
          !e.clientTimestamp.isBefore(startDate) &&
          e.clientTimestamp.isBefore(gapUpper))
        e,
  ];
  return _buildAll(destination, inGap, source: source, now: now);
}

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
    );
  }
}
