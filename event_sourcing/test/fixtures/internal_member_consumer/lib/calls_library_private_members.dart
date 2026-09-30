import 'package:event_sourcing/event_sourcing.dart';

/// Each member here is private to the Dart library that declares it, so a
/// consumer cannot name it: the analyzer reports it as undefined.
Future<void> reachPrivateMembers(
  EventStore store,
  EventStoreBundle bundle,
  DestinationRegistry registry,
  StoredEvent event,
) async {
  store.backend;
  store.deliveryTrigger = () async {};
  store.wakeDeliveryCycle();
  registry.backend;
  await bundle.setViewTargetVersion(
    'view',
    'entry',
    const EntryTypeVersion(1, 0),
  );
  await store.runTransaction((txn, collector) async {
    collector.add(event);
    await store.appendReservedInTxn(txn, collector);
    await registry.wedgeHeadInTxn(txn, collector);
  });
}
