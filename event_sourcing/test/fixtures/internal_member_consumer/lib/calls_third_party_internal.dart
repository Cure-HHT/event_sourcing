import 'package:event_sourcing/event_sourcing.dart';
import 'package:third_party_backend/third_party_backend.dart';

/// Writes a queue row through a third-party backend whose override of the
/// internal member is itself marked internal.
Future<FifoEntry> enqueueThroughThirdParty(List<StoredEvent> batch) =>
    const ThirdPartyBackend().enqueueFifo('dest', batch);
