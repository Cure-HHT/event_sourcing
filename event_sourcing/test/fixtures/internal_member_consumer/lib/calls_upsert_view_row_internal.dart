import 'package:event_sourcing/event_sourcing.dart';

/// Writes a view row directly through the abstract contract.
Future<void> upsertDirectly(StorageBackend backend) => backend.transaction(
  (txn) => backend.upsertViewRowInTxn(txn, 'view', 'key', <String, dynamic>{}),
);
