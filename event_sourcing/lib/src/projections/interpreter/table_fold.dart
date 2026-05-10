// event_sourcing/lib/src/projections/interpreter/table_fold.dart
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
      final data = spec.rowData.extract(event);
      await backend.upsertViewRowInTxn(
        txn,
        spec.viewName,
        key.toString(),
        data,
      );
      return AggregateFoldChange(
        viewName: spec.viewName,
        aggregateId: key.toString(),
        newValue: data,
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
