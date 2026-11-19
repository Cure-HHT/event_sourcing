// event_sourcing/lib/src/projections/interpreter/table_fold.dart
//
// Implements: EVS-PRD-materializer/A
// TableFold is the fold engine for the
//   TableProjectionSpec shape, one of the two shapes the library's
//   materializer provides.
// Implements: EVS-PRD-materializer/B
// the fold is deterministic: upsert on
//   insert, merge on update, delete on remove, no-op on an absent or
//   unmatched row; applying the same events in the same order from the
//   same state yields identical results.
// The row key and row data extractors are two of the four computation
//   sites the fold wraps as a FoldFailure (`EVS-DEV-view-convergence`
//   Terms); each runs before any row write.
import 'package:event_sourcing/src/projections/integrity_marks.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/fold_failure.dart';
import 'package:event_sourcing/src/projections/interpreter/view_row_access.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart' show internal;

class TableFold {
  /// The key of the row [event] inserts or removes in the table view of
  /// [spec]; throws a [FoldFailure] when [spec]'s row key extractor cannot
  /// extract one.
  @internal
  static String rowKeyOf(TableProjectionSpec spec, StoredEvent event) =>
      guardFold(
        FoldFailureReason.rowKeyFailed,
        () => spec.rowKey.extract(event),
      ).toString();

  /// The keys of the rows [event]'s fold into the table view of [spec]
  /// reads before it writes -- the row an update event merges into, the row
  /// a remove event deletes and the rows a listed removal deletes, the only
  /// rows a table fold reads by key -- or none when the fold reads no row
  /// by key or [spec]'s extractors cannot extract them: an insert
  /// overwrites its row unread.
  @internal
  static Iterable<String> readKeysOf(
    TableProjectionSpec spec,
    StoredEvent event,
  ) {
    final type = event.eventType;
    if (spec.insertEventTypes.contains(type)) return const <String>[];
    try {
      if (spec.updateEventTypes.contains(type) ||
          spec.removeEventTypes.contains(type)) {
        return <String>[rowKeyOf(spec, event)];
      }
      final listed = spec.listedRemovals;
      if (listed != null && listed.eventTypes.contains(type)) {
        return listed.extract(event);
      }
    } on Object {
      return const <String>[];
    }
    return const <String>[];
  }

  /// Whether [row] is the row [event] changes under [spec]'s match field:
  /// always when the spec sets none.
  static bool _matches(
    TableProjectionSpec spec,
    Map<String, dynamic> row,
    StoredEvent event,
  ) {
    final field = spec.matchField;
    if (field == null) return true;
    return row[field] == event.data[field];
  }

  /// Applies one [event] to the table view of [spec], inside [txn], and
  /// returns the changes it made: an insert event upserts the row its key
  /// extracts, stamped with [integrity], the findings that mark the event's
  /// aggregate, and indexed by the event's aggregate id as its producer
  /// (`upsertTableViewRowInTxn`), so the outstanding-finding refresh can
  /// later find the rows one aggregate produced without scanning the view;
  /// an update event merges its fields into the row at its key, stamped
  /// and indexed the same way; a remove event deletes it; a listed removal
  /// deletes every row it names. An update or a remove changes only a row
  /// that exists and, when [spec] sets a match field, matches the event.
  /// Reads and writes the row through [rows] when given, otherwise straight
  /// through [backend] in [txn] at [copyId].
  @internal
  static Future<List<AggregateFoldChange>> applyEvent({
    required Transaction txn,
    required StorageBackend backend,
    required TableProjectionSpec spec,
    required StoredEvent event,
    required List<String> integrity,
    required String copyId,
    ViewRowAccess? rows,
  }) async {
    final access = rows ?? DirectViewRowAccess(backend, txn, copyId);
    final type = event.eventType;
    if (spec.insertEventTypes.contains(type)) {
      final keyStr = rowKeyOf(spec, event);
      final rowData = guardFold(
        FoldFailureReason.rowDataFailed,
        () => spec.rowData.extract(event),
      );
      return <AggregateFoldChange>[
        await _write(access, spec, event, keyStr, rowData, integrity),
      ];
    }
    if (spec.updateEventTypes.contains(type)) {
      final keyStr = rowKeyOf(spec, event);
      final updateData = guardFold(
        FoldFailureReason.rowDataFailed,
        () => spec.updateData.extract(event),
      );
      final priorRow = await access.readRow(keyStr);
      if (priorRow == null || !_matches(spec, priorRow, event)) {
        return const <AggregateFoldChange>[];
      }
      return <AggregateFoldChange>[
        await _write(access, spec, event, keyStr, <String, Object?>{
          ...priorRow,
          ...updateData,
        }, integrity),
      ];
    }
    if (spec.removeEventTypes.contains(type)) {
      final keyStr = rowKeyOf(spec, event);
      final change = await _delete(access, spec, event, keyStr, match: true);
      return <AggregateFoldChange>[?change];
    }
    final listed = spec.listedRemovals;
    if (listed != null && listed.eventTypes.contains(type)) {
      final keys = guardFold(
        FoldFailureReason.rowKeyFailed,
        () => listed.extract(event),
      );
      final changes = <AggregateFoldChange>[];
      for (final keyStr in keys) {
        final change = await _delete(access, spec, event, keyStr, match: false);
        if (change != null) changes.add(change);
      }
      return changes;
    }
    // Filter narrowing should prevent reaching here; safe no-op.
    return const <AggregateFoldChange>[];
  }

  /// Upserts [rowData] as the row at [keyStr], stamped and indexed for
  /// [event], and returns its change.
  static Future<AggregateFoldChange> _write(
    ViewRowAccess access,
    TableProjectionSpec spec,
    StoredEvent event,
    String keyStr,
    Map<String, Object?> rowData,
    List<String> integrity,
  ) async {
    // Stamp the substrate-owned identity (`aggregateId`) and ordering
    // (`sequence`) fields into the row, mirroring AggregateFold. Without
    // them, TableProjectionSpec rows would violate the view-row contract
    // every consumer relies on — ViewBuilder's `aggregateIdOf` extractor
    // and subscribe()'s snapshot-replay `sequence` read both expect these
    // keys present on every materialized row, regardless of spec shape.
    // Stamped last so they win over any colliding payload key, as in
    // AggregateFold.
    final row = <String, Object?>{
      ...rowData,
      'aggregateId': keyStr,
      'sequence': event.sequenceNumber,
      // Implements: EVS-PRD-materializer/F
      // a table row carries `$integrity`, the ascending ids of the
      //   findings that mark the aggregate whose event produced its key.
      kIntegrityRowKey: integrityValue(integrity),
    };
    await access.upsertTableRow(
      keyStr,
      row,
      sourceAggregateId: event.aggregateId,
    );
    return AggregateFoldChange(
      viewName: spec.viewName,
      aggregateId: keyStr,
      newValue: row,
      sequence: event.sequenceNumber,
      cause: event.eventType,
      isTombstone: false,
    );
  }

  /// Deletes the row at [keyStr] for [event] and returns its tombstone
  /// change, or null when no row exists or, under [match], the row does
  /// not match the event.
  static Future<AggregateFoldChange?> _delete(
    ViewRowAccess access,
    TableProjectionSpec spec,
    StoredEvent event,
    String keyStr, {
    required bool match,
  }) async {
    // Only emit a tombstone change when the row actually existed; a
    // remove event targeting a nonexistent row is a silent no-op so
    // subscribers never receive a spurious Tombstone<T>.
    final priorRow = await access.readRow(keyStr);
    if (priorRow == null) return null;
    if (match && !_matches(spec, priorRow, event)) return null;
    await access.deleteRow(keyStr);
    return AggregateFoldChange(
      viewName: spec.viewName,
      aggregateId: keyStr,
      newValue: null,
      sequence: event.sequenceNumber,
      cause: event.eventType,
      isTombstone: true,
    );
  }
}
