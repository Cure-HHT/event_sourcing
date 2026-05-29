// event_sourcing/lib/src/projections/interpreter/table_fold.dart
//
// Implements: EVS-PRD-materializer/A — TableFold is the fold engine for the
//   TableProjectionSpec shape, one of the two shapes the library's
//   materializer provides.
// Implements: EVS-PRD-materializer/B — the fold is deterministic: upsert on
//   insert, delete on remove, no-op on absent row; applying the same events
//   in the same order from the same state yields identical results.
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class TableFold {
  static Future<AggregateFoldChange?> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required TableProjectionSpec spec,
    required StoredEvent event,
  }) async {
    if (spec.insertEventTypes.contains(event.eventType)) {
      final key = spec.rowKey.extract(event);
      final keyStr = key.toString();
      // Stamp the substrate-owned identity (`aggregateId`) and ordering
      // (`sequence`) fields into the row, mirroring AggregateFold. Without
      // them, TableProjectionSpec rows would violate the view-row contract
      // every consumer relies on — ViewBuilder's `aggregateIdOf` extractor
      // and subscribe()'s snapshot-replay `sequence` read both expect these
      // keys present on every materialized row, regardless of spec shape.
      // Stamped last so they win over any colliding payload key, as in
      // AggregateFold.
      final row = <String, Object?>{
        ...spec.rowData.extract(event),
        'aggregateId': keyStr,
        'sequence': event.sequenceNumber,
      };
      await backend.upsertViewRowInTxn(txn, spec.viewName, keyStr, row);
      return AggregateFoldChange(
        viewName: spec.viewName,
        aggregateId: keyStr,
        newValue: row,
        sequence: event.sequenceNumber,
        cause: event.eventType,
        isTombstone: false,
      );
    }
    if (spec.removeEventTypes.contains(event.eventType)) {
      final key = spec.rowKey.extract(event);
      final keyStr = key.toString();
      // Only emit a tombstone change when the row actually existed; a
      // remove event targeting a nonexistent row is a silent no-op so
      // subscribers never receive a spurious Tombstone<T>.
      final priorRow = await backend.readViewRowInTxn(
        txn,
        spec.viewName,
        keyStr,
      );
      if (priorRow == null) return null;
      await backend.deleteViewRowInTxn(txn, spec.viewName, keyStr);
      return AggregateFoldChange(
        viewName: spec.viewName,
        aggregateId: keyStr,
        newValue: null,
        sequence: event.sequenceNumber,
        cause: event.eventType,
        isTombstone: true,
      );
    }
    // Filter narrowing should prevent reaching here; safe no-op.
    return null;
  }
}
