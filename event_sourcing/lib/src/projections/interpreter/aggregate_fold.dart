// event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

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
    required Txn txn,
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

    final merged = _deepMerge(prior, event.data);

    final next = Map<String, Object?>.from(merged);
    next['latestEventId'] = event.eventId;
    next['updatedAt'] = event.clientTimestamp.toUtc().toIso8601String();
    next['firstEventTimestamp'] = firstEventTimestamp.toUtc().toIso8601String();

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

  /// Recursively merges [delta] into [prior] with null-as-clear semantics.
  ///
  /// When both [prior] and [delta] have a Map value for the same key, the
  /// merge recurses into that nested map (so inner keys absent from the delta
  /// are preserved). When the delta value is non-null and non-Map, it
  /// overwrites. When the delta value is null, the key is cleared (set to
  /// null) in the result. Keys absent from the delta are preserved unchanged.
  static Map<String, Object?> _deepMerge(
    Map<String, Object?> prior,
    Map<String, Object?> delta,
  ) {
    final result = Map<String, Object?>.from(prior);
    for (final key in delta.keys) {
      final deltaVal = delta[key];
      final priorVal = prior[key];
      if (deltaVal is Map<String, Object?> &&
          priorVal is Map<String, Object?>) {
        result[key] = _deepMerge(priorVal, deltaVal);
      } else if (deltaVal is Map && priorVal is Map) {
        result[key] = _deepMerge(
          Map<String, Object?>.from(priorVal as Map),
          Map<String, Object?>.from(deltaVal as Map),
        );
      } else {
        result[key] = deltaVal;
      }
    }
    return result;
  }
}
