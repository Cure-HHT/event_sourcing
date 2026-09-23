import 'package:event_sourcing/event_sourcing.dart';

import 'third_party_backend.dart';

/// A backend whose override of an internal member carries no `@internal`.
/// A call through this concrete type from another package is not reported:
/// the analyzer guard reaches a third-party backend only through the
/// annotations its author writes.
class UnguardedThirdPartyBackend extends ThirdPartyBackend {
  const UnguardedThirdPartyBackend();

  @override
  Future<FifoEntry> enqueueFifoTxn(
    Transaction txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => super.enqueueFifoTxn(
    txn,
    destinationId,
    batch,
    wirePayload: wirePayload,
    nativeEnvelope: nativeEnvelope,
  );
}
