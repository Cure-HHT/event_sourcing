import 'package:event_sourcing/event_sourcing.dart';

import 'third_party_backend.dart';

/// A backend whose override of an internal member carries no `@internal`.
/// A call through this concrete type from another package is not reported:
/// the analyzer guard reaches a third-party backend only through the
/// annotations its author writes.
class UnguardedThirdPartyBackend extends ThirdPartyBackend {
  const UnguardedThirdPartyBackend();

  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => super.enqueueFifo(
    destinationId,
    batch,
    wirePayload: wirePayload,
    nativeEnvelope: nativeEnvelope,
  );
}
