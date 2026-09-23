import 'package:event_sourcing/event_sourcing.dart';

/// Writes a queue row directly through the concrete backend.
Future<FifoEntry> enqueueDirectly(
  SembastBackend backend,
  List<StoredEvent> batch,
) => backend.enqueueFifo('dest', batch);
