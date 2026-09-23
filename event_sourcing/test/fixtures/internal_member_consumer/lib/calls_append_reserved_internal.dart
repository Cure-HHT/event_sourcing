import 'package:event_sourcing/event_sourcing.dart';

/// Appends a reserved system event outside the library.
Future<void> appendReservedDirectly(EventStore store) async {
  await store.appendReserved(
    entryType: kDestinationWedgedEntryType,
    aggregateId: 'install',
    aggregateType: 'system_destination',
    eventType: kDestinationWedgedEventType,
    data: const <String, Object?>{},
    initiator: const AutomationInitiator(service: 'consumer'),
  );
  await store.runTransaction((txn, collector) async {
    await store.appendReservedInTxn(
      txn,
      collector,
      entryType: kDestinationWedgedEntryType,
      aggregateId: 'install',
      aggregateType: 'system_destination',
      eventType: kDestinationWedgedEventType,
      data: const <String, Object?>{},
      initiator: const AutomationInitiator(service: 'consumer'),
    );
  });
}
