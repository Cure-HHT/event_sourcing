// Verifies: EVS-PRD-destinations/C
// Verifies: EVS-PRD-destinations/E
// Verifies: EVS-PRD-ingest/A+E
// Verifies: EVS-PRD-provenance/B+C
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/native_demo_destination.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Pane {
  _Pane({
    required this.datastore,
    required this.backend,
    required this.source,
    required this.policyNotifier,
    required this.cycle,
  });

  final EventStoreBundle datastore;
  final SembastBackend backend;
  final Source source;
  final ValueNotifier<SyncPolicy> policyNotifier;

  /// The pane's delivery cycle: fills every destination's queue from the
  /// log and drains it, with the pane's live policy.
  final SyncCycle cycle;

  Future<void> tick() => cycle();
}

Future<_Pane> _mkPane({
  required String dbName,
  required Source source,
  DownstreamBridge? bridge,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(dbName);
  final backend = SembastBackend(database: db);
  final policyNotifier = ValueNotifier<SyncPolicy>(demoDefaultSyncPolicy);

  // The demo's simulated network latency is dropped to zero: a pane's first
  // cycle performs each destination's activation replay, which enqueues and
  // sends every note appended so far in one pass.
  final primary = DemoDestination(
    id: 'Primary',
    initialSendLatency: Duration.zero,
    filter: const SubscriptionFilter(
      entryTypes: <String>{
        'demo_note',
        'red_button_pressed',
        'green_button_pressed',
      },
    ),
  );
  final secondary = DemoDestination(
    id: 'Secondary',
    initialSendLatency: Duration.zero,
    allowHardDelete: true,
    filter: const SubscriptionFilter(
      entryTypes: <String>{'green_button_pressed', 'blue_button_pressed'},
    ),
  );
  // Two parallel native destinations — user payloads vs system audits —
  // mirror the dual-pane example app's bridging surface. NativeUser ships
  // user button events; NativeAudit ships system audits via
  // `includeSystemEvents: true`. Both feed the same downstream bridge.
  final nativeUser = NativeDemoDestination(
    id: 'NativeUser',
    filter: const SubscriptionFilter(
      entryTypes: <String>{
        'demo_note',
        'red_button_pressed',
        'green_button_pressed',
        'blue_button_pressed',
      },
    ),
    bridge: bridge,
  );
  final nativeAudit = NativeDemoDestination(
    id: 'NativeAudit',
    filter: const SubscriptionFilter(
      entryTypes: <String>{},
      includeSystemEvents: true,
    ),
    bridge: bridge,
  );

  final datastore = await bootstrapEventStore(
    backend: backend,
    source: source,
    entryTypes: allDemoEntryTypes,
    destinations: <Destination>[primary, secondary, nativeUser, nativeAudit],
  );

  final now = DateTime.now().toUtc();
  for (final id in <String>[
    'Primary',
    'Secondary',
    'NativeUser',
    'NativeAudit',
  ]) {
    final schedule = await datastore.destinations.scheduleOf(id);
    if (schedule.startDate == null) {
      await datastore.destinations.setStartDate(
        id,
        now,
        initiator: const AutomationInitiator(service: 'demo-bootstrap'),
      );
    }
  }

  // A one-hour cadence: the test runs every pass it asserts on itself.
  final cycle = await SyncCycle.start(
    registry: datastore.destinations,
    policyResolver: () => policyNotifier.value,
    cadence: const Duration(hours: 1),
  );
  addTearDown(cycle.close);
  return _Pane(
    datastore: datastore,
    backend: backend,
    source: source,
    policyNotifier: policyNotifier,
    cycle: cycle,
  );
}

Future<void> _appendDemoNote(_Pane pane, String aggregateId) async {
  await pane.datastore.eventStore.append(
    entryType: 'demo_note',
    aggregateId: aggregateId,
    aggregateType: 'Note',
    eventType: 'finalized',
    data: const <String, Object?>{
      'answers': <String, Object?>{'title': 't', 'body': 'b'},
    },
    initiator: const UserInitiator('demo-user-1'),
  );
}

void main() {
  group('mobile -> hub one-way sync', () {
    test(
      'three demo_notes appended on mobile arrive in hub with hub-stamped provenance',
      () async {
        final hub = await _mkPane(
          dbName: 'hub-e2e.db',
          source: const Source(
            hopId: 'hub-server',
            identifier: '11111111-1111-4111-8111-111111111111',
            softwareVersion: 'test',
          ),
        );
        final bridge = DownstreamBridge(hub.datastore.eventStore);
        final mobile = await _mkPane(
          dbName: 'mobile-e2e.db',
          source: const Source(
            hopId: 'mobile-device',
            identifier: '22222222-2222-4222-8222-222222222222',
            softwareVersion: 'test',
          ),
          bridge: bridge,
        );

        await _appendDemoNote(mobile, 'agg-a');
        await _appendDemoNote(mobile, 'agg-b');
        await _appendDemoNote(mobile, 'agg-c');

        // Two ticks: tick 1 fills the FIFO + drains; the bridge ingests
        // into hub during drain. Tick 2 lets hub's own destinations
        // process the freshly-ingested events.
        await mobile.tick();
        await hub.tick();

        // Filter out the hub's own bootstrap-emitted system audit
        // events  so the assertion stays focused on
        // user payload arriving from mobile.
        final hubEvents = (await hub.backend.findAllEvents())
            .where((e) => !kReservedSystemEntryTypeIds.contains(e.entryType))
            .toList();
        expect(hubEvents.length, 3);
        for (final ev in hubEvents) {
          final provenance = (ev.metadata['provenance'] as List<Object?>)
              .cast<Map<String, Object?>>();
          final hops = provenance.map((p) => p['hop'] as String).toList();
          expect(
            hops,
            containsAllInOrder(<String>['mobile-device', 'hub-server']),
            reason: 'event ${ev.eventId} provenance hops: $hops',
          );
        }
      },
    );

    test('one SyncCycle pass enqueues rows for NativeUser and NativeAudit '
        '(the cycle stamps native batches with the pane Source)', () async {
      final mobile = await _mkPane(
        dbName: 'mobile-native-enqueue.db',
        source: const Source(
          hopId: 'mobile-device',
          identifier: '33333333-3333-4333-8333-333333333333',
          softwareVersion: 'test',
        ),
      );
      await _appendDemoNote(mobile, 'agg-native');
      await mobile.tick();

      final user = await mobile.backend.listFifoEntries('NativeUser');
      final audit = await mobile.backend.listFifoEntries('NativeAudit');
      expect(user, isNotEmpty, reason: 'NativeUser carries the demo_note');
      expect(
        audit,
        isNotEmpty,
        reason: 'NativeAudit carries the bootstrap system audits',
      );
      for (final row in <FifoEntry>[...user, ...audit]) {
        expect(
          row.envelopeMetadata?.senderIdentifier,
          '33333333-3333-4333-8333-333333333333',
        );
      }
    });

    test('events appended locally on hub do not flow back to mobile', () async {
      final hub = await _mkPane(
        dbName: 'hub-oneway.db',
        source: const Source(
          hopId: 'hub-server',
          identifier: '11111111-1111-4111-8111-111111111111',
          softwareVersion: 'test',
        ),
      );
      final bridge = DownstreamBridge(hub.datastore.eventStore);
      final mobile = await _mkPane(
        dbName: 'mobile-oneway.db',
        source: const Source(
          hopId: 'mobile-device',
          identifier: '22222222-2222-4222-8222-222222222222',
          softwareVersion: 'test',
        ),
        bridge: bridge,
      );

      await _appendDemoNote(hub, 'agg-hub-only');
      await hub.tick();
      await mobile.tick();

      // Filter out mobile's own bootstrap-emitted system audit
      // events  so this assertion stays focused on
      // whether hub-originated user payloads leaked back.
      final mobileEvents = (await mobile.backend.findAllEvents())
          .where((e) => !kReservedSystemEntryTypeIds.contains(e.entryType))
          .toList();
      expect(
        mobileEvents,
        isEmpty,
        reason: 'mobile must not receive events from hub (one-way sync)',
      );
    });

    test(
      'mobile.Native connection=broken keeps mobile FIFO pending and hub empty',
      () async {
        final hub = await _mkPane(
          dbName: 'hub-broken.db',
          source: const Source(
            hopId: 'hub-server',
            identifier: '11111111-1111-4111-8111-111111111111',
            softwareVersion: 'test',
          ),
        );
        final bridge = DownstreamBridge(hub.datastore.eventStore);
        final mobile = await _mkPane(
          dbName: 'mobile-broken.db',
          source: const Source(
            hopId: 'mobile-device',
            identifier: '22222222-2222-4222-8222-222222222222',
            softwareVersion: 'test',
          ),
          bridge: bridge,
        );

        // Flip every mobile native destination (NativeUser, NativeAudit)
        // to broken before the first tick.
        final mobileNatives = mobile.datastore.destinations
            .all()
            .whereType<NativeDemoDestination>();
        for (final n in mobileNatives) {
          n.connection.value = Connection.broken;
        }

        await _appendDemoNote(mobile, 'agg-stuck');
        await mobile.tick();
        await hub.tick();

        // Filter out hub's own bootstrap-emitted system audit events.
        final hubEvents = (await hub.backend.findAllEvents())
            .where((e) => !kReservedSystemEntryTypeIds.contains(e.entryType))
            .toList();
        expect(
          hubEvents,
          isEmpty,
          reason: 'broken link must not deliver to hub',
        );
        final mobileFifo = await mobile.backend.listFifoEntries('NativeUser');
        expect(
          mobileFifo,
          isNotEmpty,
          reason:
              'broken link must keep mobile.NativeUser FIFO row pending for retry',
        );
      },
    );
  });
}
