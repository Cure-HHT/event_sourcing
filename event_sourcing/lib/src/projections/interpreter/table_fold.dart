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
      await backend.deleteViewRowInTxn(txn, spec.viewName, key.toString());
      return AggregateFoldChange(
        viewName: spec.viewName,
        aggregateId: key.toString(),
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
