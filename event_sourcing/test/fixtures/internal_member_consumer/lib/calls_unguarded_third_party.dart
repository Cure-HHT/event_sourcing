import 'package:event_sourcing/event_sourcing.dart';
import 'package:third_party_backend/third_party_backend.dart';

/// Writes a queue row through a third-party backend whose override carries
/// no `@internal`. This is not reported: the guard reaches a third-party
/// backend only through its author's annotations.
Future<FifoEntry> enqueueThroughUnguarded(List<StoredEvent> batch) =>
    const UnguardedThirdPartyBackend().enqueueFifo('dest', batch);
