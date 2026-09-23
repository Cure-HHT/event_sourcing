// The hub's detail panel reads the library's default destination-wedges
// view beside the hub's own queues: a wedge on the mobile pane, forwarded
// with the mobile pane's system events, shows as a peer row, while the
// hub's own queues and its local view rows show no wedge. App-side
// behaviour: carries no requirement citation.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/native_demo_destination.dart';
import 'package:event_sourcing_demo/widgets/detail_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const Initiator _init = AutomationInitiator(service: 'demo-bootstrap');

Future<EventStoreBundle> _pane({
  required String dbName,
  required Source source,
  required List<Destination> destinations,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(dbName);
  final datastore = await bootstrapEventStore(
    backend: SembastBackend(database: db),
    source: source,
    entryTypes: allDemoEntryTypes,
    destinations: destinations,
  );
  for (final d in destinations) {
    await datastore.destinations.setStartDate(
      d.id,
      DateTime.utc(2020, 1, 1),
      initiator: _init,
    );
  }
  return datastore;
}

void main() {
  testWidgets('a mobile wedge forwarded to the hub shows as a peer row of '
      'the default destination-wedges view', (tester) async {
    late EventStoreBundle hub;
    late EventStoreBundle mobile;
    late SembastBackend hubBackend;
    late AppState hubState;
    final policy = ValueNotifier<SyncPolicy>(demoDefaultSyncPolicy);
    await tester.runAsync(() async {
      hub = await _pane(
        dbName: 'hub-wedges-view-hub.db',
        source: const Source(
          hopId: 'hub-server',
          identifier: '11111111-1111-4111-8111-111111111111',
          softwareVersion: 'test',
        ),
        destinations: <Destination>[
          DemoDestination(
            id: 'Primary',
            initialSendLatency: Duration.zero,
            filter: const SubscriptionFilter(entryTypes: <String>{'demo_note'}),
          ),
        ],
      );
      hubBackend = hub.eventStore.backend as SembastBackend;
      mobile = await _pane(
        dbName: 'hub-wedges-view-mobile.db',
        source: const Source(
          hopId: 'mobile-device',
          identifier: '22222222-2222-4222-8222-222222222222',
          softwareVersion: 'test',
        ),
        destinations: <Destination>[
          DemoDestination(
            id: 'Primary',
            initialSendLatency: Duration.zero,
            initialConnection: Connection.rejecting,
            filter: const SubscriptionFilter(entryTypes: <String>{'demo_note'}),
          ),
          NativeDemoDestination(
            id: 'NativeAudit',
            filter: const SubscriptionFilter(
              entryTypes: <String>{},
              includeSystemEvents: true,
            ),
            bridge: DownstreamBridge(hub.eventStore),
          ),
        ],
      );
      hubState = AppState(registry: hub.destinations, policyNotifier: policy);

      await mobile.eventStore.append(
        entryType: 'demo_note',
        aggregateId: 'n1',
        aggregateType: 'Note',
        eventType: 'finalized',
        data: const <String, Object?>{
          'answers': <String, Object?>{'title': 't', 'body': 'b'},
        },
        initiator: const UserInitiator('demo-user-1'),
      );
      final mobileCycle = SyncCycle(
        registry: mobile.destinations,
        source: const Source(
          hopId: 'mobile-device',
          identifier: '22222222-2222-4222-8222-222222222222',
          softwareVersion: 'test',
        ),
        policyResolver: () => policy.value,
      );
      // The first pass wedges the mobile Primary; a later pass forwards
      // the wedge event through NativeAudit to the hub.
      for (var i = 0; i < 5; i++) {
        await mobileCycle();
      }
      final hubCycle = SyncCycle(
        registry: hub.destinations,
        policyResolver: () => policy.value,
      );
      await hubCycle();
    });

    final mobileId = mobile.eventStore.databaseId;
    await tester.runAsync(
      () => tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: SizedBox(
              height: 600,
              child: DetailPanel(
                backend: hubBackend,
                databaseId: hub.eventStore.databaseId,
                appState: hubState,
                policyNotifier: policy,
              ),
            ),
          ),
        ),
      ),
    );
    final peerLine = find.textContaining(
      'Primary (permanent_refusal) from database ${mobileId.substring(0, 8)}',
    );
    for (var i = 0; i < 500 && peerLine.evaluate().isEmpty; i++) {
      await tester.runAsync(
        () => Future<void>.delayed(const Duration(milliseconds: 10)),
      );
      await tester.pump();
    }
    expect(peerLine, findsOneWidget);
    expect(
      find.textContaining('wedges view (this database): none'),
      findsOneWidget,
    );

    late List<WedgedFifoSummary> hubWedged;
    late List<Map<String, Object?>> rows;
    late FifoEntry? mobileHead;
    late List<FifoEntry> hubPrimary;
    await tester.runAsync(() async {
      hubWedged = await hubBackend.wedgedFifos();
      rows = await hubBackend.findViewRows(
        defaultDestinationWedgesSpec.viewName,
      );
      mobileHead = await mobile.eventStore.backend.readFifoHead('Primary');
      hubPrimary = await hubBackend.listFifoEntries('Primary');
    });
    expect(mobileHead?.finalStatus, FinalStatus.wedged);
    expect(hubWedged, isEmpty, reason: "the hub's own queues are not wedged");
    expect(rows, hasLength(1));
    expect(rows.single['aggregateId'], '$mobileId|Primary');
    expect(rows.single['row_id'], mobileHead!.entryId);
    expect(
      hubPrimary.where((e) => e.finalStatus == FinalStatus.wedged),
      isEmpty,
      reason: "the hub's Primary queue is unaffected",
    );
    await tester.runAsync(() async {
      await mobile.eventStore.close();
      await hub.eventStore.close();
    });
  });
}
