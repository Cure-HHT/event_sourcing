// event_sourcing/lib/src/projections/interpreter/table_fold.dart
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class TableFold {
  static Future<void> applyEvent({
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
      return;
    }
    if (spec.removeEventTypes.contains(event.eventType)) {
      final key = spec.rowKey.extract(event);
      await backend.deleteViewRowInTxn(txn, spec.viewName, key.toString());
      return;
    }
    // Filter narrowing should prevent reaching here; safe no-op.
  }
}
