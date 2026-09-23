import 'package:event_sourcing/event_sourcing.dart';

/// Reaches the event store the registry's drainer runs its outcome
/// transactions in.
EventStore reachStore(DestinationRegistry registry) => registry.eventStore;

/// Wedges a queue head directly through the registry.
Future<void> wedgeDirectly(DestinationRegistry registry, EventStore store) =>
    store.runTransaction((txn, collector) async {
      await registry.wedgeHeadInTxn(
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

/// Honours a halt request directly through the registry.
Future<void> honourDirectly(DestinationRegistry registry, EventStore store) =>
    store.runTransaction((txn, collector) async {
      await registry.honourHaltInTxn(
        txn,
        collector,
        destinationId: 'dest',
        requestEventId: 'request',
        maxAttempts: 1,
        drainerEpoch: 1,
        configuration: null,
        configurationFingerprint: null,
      );
    });
