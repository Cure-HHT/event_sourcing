import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show wedgeHeadInTxnForTest;

/// Reaches the event store the registry's drainer runs its outcome
/// transactions in.
EventStore reachStore(DestinationRegistry registry) => registry.eventStore;

/// Wedges a queue head through the test-only entry point to the
/// registry's wedge.
Future<void> wedgeDirectly(DestinationRegistry registry, EventStore store) =>
    store.runTransaction((txn, collector) async {
      await wedgeHeadInTxnForTest(
        registry,
        txn,
        collector,
        destinationId: 'dest',
        rowId: 'row',
        cause: WedgeCause.permanentRefusal,
        maxAttempts: 1,
        drainerEpoch: 1,
        configuration: null,
        configurationFingerprint: null,
      );
    });
