// Verifies: EVS-PRD-destinations/O
// the hub's delete action is refused while
//   the queue head is pending, and a deleted destination's delivered and
//   recovered items stay visible in a read-only panel, refreshed by the
//   queue watcher with no other trigger.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/widgets/deleted_fifo_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Hub {
  _Hub(this.backend, this.datastore, this.state, this.destination, this.cycle);
  final SembastBackend backend;
  final EventStoreBundle datastore;
  final AppState state;
  final DemoDestination destination;
  final SyncCycle cycle;
}

Future<_Hub> _mkHub(String name) async {
  final db = await newDatabaseFactoryMemory().openDatabase(name);
  final backend = SembastBackend(database: db);
  final destination = DemoDestination(
    id: 'Secondary',
    allowHardDelete: true,
    initialSendLatency: Duration.zero,
    filter: const SubscriptionFilter(entryTypes: <String>{'demo_note'}),
  );
  final datastore = await bootstrapEventStore(
    backend: backend,
    source: const Source(
      hopId: 'hub-server',
      identifier: '44444444-4444-4444-8444-444444444444',
      softwareVersion: 'test',
    ),
    entryTypes: allDemoEntryTypes,
    destinations: <Destination>[destination],
  );
  await datastore.destinations.setStartDate(
    'Secondary',
    DateTime.utc(2020, 1, 1),
    initiator: const AutomationInitiator(service: 'test'),
  );
  final policy = ValueNotifier<SyncPolicy>(demoDefaultSyncPolicy);
  return _Hub(
    backend,
    datastore,
    AppState(registry: datastore.destinations, policyNotifier: policy),
    destination,
    SyncCycle(
      registry: datastore.destinations,
      policyResolver: () => policy.value,
    ),
  );
}

Future<void> _note(_Hub hub, String id) => hub.datastore.eventStore.append(
  entryType: 'demo_note',
  aggregateId: id,
  aggregateType: 'Note',
  eventType: 'finalized',
  data: const <String, Object?>{
    'answers': <String, Object?>{'title': 't', 'body': 'b'},
  },
  initiator: const UserInitiator('demo-user-1'),
);

void main() {
  test('delete is refused while the head is pending', () async {
    final hub = await _mkHub('hub-delete-refused.db');
    // Nothing can be delivered: the head stays pending.
    hub.destination.connection.value = Connection.broken;
    await _note(hub, 'n1');
    await hub.cycle();
    final before = await hub.backend.listFifoEntries('Secondary');
    expect(before.single.finalStatus, isNull);

    await expectLater(
      hub.state.deleteDestination('Secondary'),
      throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('may be in delivery'),
        ),
      ),
    );
    expect(hub.state.deletedDestinationIds, isEmpty);
    expect(hub.state.destinations.map((d) => d.id), contains('Secondary'));
    expect(
      (await hub.backend.listFifoEntries('Secondary')).single.finalStatus,
      isNull,
    );
  });

  testWidgets('a deleted destination keeps its retained rows visible', (
    tester,
  ) async {
    late _Hub hub;
    await tester.runAsync(() async {
      hub = await _mkHub('hub-delete-retained.db');
      await _note(hub, 'n1');
      await hub.cycle(); // delivered
      hub.destination.connection.value = Connection.rejecting;
      await _note(hub, 'n2');
      await hub.cycle(); // wedged
    });

    await tester.runAsync(
      () => tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: SizedBox(
              height: 400,
              child: DeletedFifoPanel(
                destinationId: 'Secondary',
                backend: hub.backend,
              ),
            ),
          ),
        ),
      ),
    );
    // Let the panel's first snapshot arrive, then delete: the panel
    // refreshes from the queue watcher alone.
    for (var i = 0; i < 5; i++) {
      await tester.runAsync(
        () => Future<void>.delayed(const Duration(milliseconds: 10)),
      );
      await tester.pump();
    }
    expect(find.textContaining('wedged'), findsOneWidget);
    await tester.runAsync(() => hub.state.deleteDestination('Secondary'));
    for (var i = 0; i < 5; i++) {
      await tester.runAsync(
        () => Future<void>.delayed(const Duration(milliseconds: 10)),
      );
      await tester.pump();
    }

    expect(hub.state.deletedDestinationIds, <String>['Secondary']);
    expect(
      hub.state.destinations.map((d) => d.id),
      isNot(contains('Secondary')),
    );
    expect(find.text('Secondary (deleted)'), findsOneWidget);
    expect(find.textContaining('sent'), findsOneWidget);
    expect(find.textContaining('tombstoned'), findsOneWidget);
    await tester.runAsync(() => hub.backend.close());
  });
}
