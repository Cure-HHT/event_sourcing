import 'package:event_sourcing/event_sourcing.dart';
import 'package:third_party_backend/third_party_backend.dart';

/// Writes a queue row through a third-party backend whose override of the
/// internal member is itself marked internal.
Future<FifoEntry> enqueueThroughThirdParty(
  Transaction txn,
  List<StoredEvent> batch,
) => const ThirdPartyBackend().enqueueFifoTxn(txn, 'dest', batch);
