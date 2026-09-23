// Verifies: EVS-PRD-destinations/P
// in the hub demo, a destination whose
//   connection rejects every send wedges its queue head, and the wedge event
//   is appended with it and shown in the hub's event stream.
// Verifies: EVS-PRD-destinations/Q
// the wedge event records the cause of a
//   rejection: a permanent refusal.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/widgets/event_stream_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  testWidgets('a rejecting connection wedges its head and the wedge event '
      'appears in the event stream', (tester) async {
    late SembastBackend backend;
    late EventStoreBundle datastore;
    late AppState state;
    late SyncCycle cycle;
    late DemoDestination destination;
    await tester.runAsync(() async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'hub-wedge-event.db',
      );
      backend = SembastBackend(database: db);
      destination = DemoDestination(
        id: 'Secondary',
        initialSendLatency: Duration.zero,
        initialConnection: Connection.rejecting,
        filter: const SubscriptionFilter(entryTypes: <String>{'demo_note'}),
      );
      datastore = await bootstrapEventStore(
        backend: backend,
        source: const Source(
          hopId: 'hub-server',
          identifier: '55555555-5555-4555-8555-555555555555',
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
      // A one-hour cadence: the test runs every pass it asserts on itself.
      state = AppState(
        registry: datastore.destinations,
        policyNotifier: policy,
        cadence: const Duration(hours: 1),
      );
      cycle = await state.startDelivery();
    });

    await tester.runAsync(
      () => tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: SizedBox(
              height: 600,
              child: EventStreamPanel(
                backend: backend,
                eventStore: datastore.eventStore,
                appState: state,
              ),
            ),
          ),
        ),
      ),
    );
    expect(find.textContaining('destination_wedged'), findsNothing);

    await tester.runAsync(() async {
      await datastore.eventStore.append(
        entryType: 'demo_note',
        aggregateId: 'n1',
        aggregateType: 'Note',
        eventType: 'finalized',
        data: const <String, Object?>{
          'answers': <String, Object?>{'title': 't', 'body': 'b'},
        },
        initiator: const UserInitiator('demo-user-1'),
      );
      await cycle();
    });
    // The panel re-reads the log after each event it is told about; wait,
    // within a bound, until it shows the wedge event.
    final wedgeRow = find.textContaining('destination_wedged');
    for (var i = 0; i < 500 && wedgeRow.evaluate().isEmpty; i++) {
      await tester.runAsync(
        () => Future<void>.delayed(const Duration(milliseconds: 10)),
      );
      await tester.pump();
    }

    late List<StoredEvent> wedges;
    late FifoEntry? head;
    await tester.runAsync(() async {
      wedges = await backend.findAllEvents(
        entryType: kDestinationWedgedEntryType,
      );
      head = await backend.readFifoHead('Secondary');
    });
    expect(head?.finalStatus, FinalStatus.wedged);
    expect(wedges, hasLength(1));
    expect(wedges.single.data['id'], 'Secondary');
    expect(wedges.single.data['row_id'], head!.entryId);
    expect(wedges.single.data['cause'], 'permanent_refusal');
    expect(wedgeRow, findsOneWidget);
    await tester.runAsync(() async {
      await state.stopDelivery();
      await backend.close();
    });
  });
}
