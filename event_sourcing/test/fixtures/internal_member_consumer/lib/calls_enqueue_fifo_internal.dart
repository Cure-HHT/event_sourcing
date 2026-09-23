import 'package:event_sourcing/event_sourcing.dart';

/// Writes a queue row directly through the concrete backend.
Future<FifoEntry> enqueueDirectly(
  SembastBackend backend,
  Transaction txn,
  List<StoredEvent> batch,
) => backend.enqueueFifoTxn(txn, 'dest', batch);
