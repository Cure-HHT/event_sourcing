// event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart
//
// Implements: EVS-PRD-materializer/A
// AggregateFold is the fold engine for
//   the AggregateProjectionSpec shape, one of the two shapes the library's
//   materializer provides.
// Implements: EVS-PRD-materializer/B
// the fold is a pure function of
//   (prior row, event); applying the same events in the same order to the
//   same initial state yields byte-identical results (Merge.applyDeepDelta
//   is deterministic; metadata stamps are taken from event fields, not wall
//   clock).
import 'package:event_sourcing/src/projections/primitives/merge.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';

/// Change record returned by [AggregateFold.applyEvent] and
/// [TableFold.applyEvent]. Collected by [ProjectionInterpreter.applyEvent]
/// and returned to [EventStore.append] for post-commit subscriber notification.
class AggregateFoldChange {
  final String viewName;
  final String aggregateId;
  final Map<String, Object?>? newValue; // null = tombstoned
  final int sequence;
  final String cause;
  final bool isTombstone;
  AggregateFoldChange({
    required this.viewName,
    required this.aggregateId,
    required this.newValue,
    required this.sequence,
    required this.cause,
    required this.isTombstone,
  });
}

class AggregateFold {
  /// Applies one [event] to its aggregate row under [spec], inside [txn].
  /// Implements the AggregateProjectionSpec fold mechanics:
  /// - read row by event.aggregateId (initialState if absent),
  /// - recursive merge of event.data with null-as-clear,
  /// - apply derived fields,
  /// - stamp metadata,
  /// - delete row if event.eventType is in spec.tombstoneEventTypes.
  ///
  /// Returns an [AggregateFoldChange] describing the mutation for subscriber
  /// notification, or `null` when the event was a tombstone for a row that
  /// did not exist (no change occurred).
  static Future<AggregateFoldChange?> applyEvent({
    required Transaction txn,
    required StorageBackend backend,
    required AggregateProjectionSpec spec,
    required StoredEvent event,
  }) async {
    if (spec.tombstoneEventTypes.contains(event.eventType)) {
      final existing = await backend.readViewRowInTxn(
        txn,
        spec.viewName,
        event.aggregateId,
      );
      await backend.deleteViewRowInTxn(txn, spec.viewName, event.aggregateId);
      if (existing == null) return null; // nothing to report
      return AggregateFoldChange(
        viewName: spec.viewName,
        aggregateId: event.aggregateId,
        newValue: null,
        sequence: event.sequenceNumber,
        cause: event.eventType,
        isTombstone: true,
      );
    }
    final priorRaw = await backend.readViewRowInTxn(
      txn,
      spec.viewName,
      event.aggregateId,
    );
    final prior = priorRaw ?? const <String, Object?>{};
    final firstEventTimestamp =
        (prior['firstEventTimestamp'] as String?) != null
        ? DateTime.parse(prior['firstEventTimestamp'] as String)
        : event.clientTimestamp;

    final merged = Merge.applyDeepDelta(prior, event.data);

    final next = Map<String, Object?>.from(merged);
    next['aggregateId'] = event.aggregateId;
    // Stamp the event's classifying metadata onto the row so consumers can read
    // it directly rather than inferring it from the payload shape. Two entry
    // types of the same aggregate (e.g. a day marker that may be a "no events"
    // or an "unknown" record, with identical payloads) are otherwise
    // indistinguishable once materialized.
    next['entryType'] = event.entryType;
    next['aggregateType'] = event.aggregateType;
    next['latestEventId'] = event.eventId;
    next['updatedAt'] = event.clientTimestamp.toUtc().toIso8601String();
    next['firstEventTimestamp'] = firstEventTimestamp.toUtc().toIso8601String();
    next['sequence'] = event.sequenceNumber;

    for (final df in spec.derivedFields) {
      next[df.fieldName] = df.computation.resolve(
        rowState: next,
        firstEventTimestamp: firstEventTimestamp,
      );
    }

    final immutableNext = Map<String, Object?>.unmodifiable(next);
    await backend.upsertViewRowInTxn(
      txn,
      spec.viewName,
      event.aggregateId,
      immutableNext,
    );
    return AggregateFoldChange(
      viewName: spec.viewName,
      aggregateId: event.aggregateId,
      newValue: immutableNext,
      sequence: event.sequenceNumber,
      cause: event.eventType,
      isTombstone: false,
    );
  }
}
