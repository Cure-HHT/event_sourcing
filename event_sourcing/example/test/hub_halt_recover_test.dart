// Verifies: EVS-PRD-destinations/S
// the WEDGED panel lists the default destination-wedges view: a wedge of
//   the pane's own database carries a Recover button, and a peer's wedge,
//   forwarded with the peer's system events, is labelled with its origin
//   database and carries no action; the row goes when the wedge ends.
// Verifies: EVS-PRD-destinations/U
// the Halt buttons request a halt that the drainer honours by wedging the
//   queue head for an operator halt, on a queue with a head or on an empty
//   one, where the first item enqueued is wedged.
// Verifies: EVS-PRD-destinations/M+O
// a recovery of a halt for reconfiguration is refused until the drainer
//   declares a changed configuration, and the refill then runs under it; a
//   deletion is refused on a pending head and accepted once a halt wedged
//   it, keeping the delivered items.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_knobs.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/native_demo_destination.dart';
import 'package:event_sourcing_demo/storage_watch.dart';
import 'package:event_sourcing_demo/widgets/deleted_fifo_panel.dart';
import 'package:event_sourcing_demo/widgets/detail_panel.dart';
import 'package:event_sourcing_demo/widgets/fifo_panel.dart';
import 'package:event_sourcing_demo/widgets/wedges_panel.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' hide Finder;

class _Pane {
  _Pane(this.backend, this.datastore, this.state);

  final SembastBackend backend;
  final EventStoreBundle datastore;
  final AppState state;

  EventStore get store => datastore.eventStore;

  /// Runs a pass of the pane's delivery cycle.
  Future<void> cycle() => state.cycle!();

  Future<void> close() async {
    await state.stopDelivery();
    await store.close();
  }
}

var _panes = 0;

Future<_Pane> _mkPane(
  List<Destination> destinations, {
  String identifier = '55555555-5555-4555-8555-555555555555',
  String hop = 'hub-server',
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'hub-halt-${_panes++}.db',
  );
  final backend = SembastBackend(database: db);
  final datastore = await bootstrapEventStore(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    source: Source(hopId: hop, identifier: identifier, softwareVersion: 'test'),
    entryTypes: allDemoEntryTypes,
    destinations: destinations,
  );
  for (final d in destinations) {
    await datastore.destinations.setStartDate(
      d.id,
      DateTime.utc(2020, 1, 1),
      initiator: const AutomationInitiator(service: 'test'),
    );
  }
  // A one-hour cadence: the tests run the passes they assert on.
  final state = AppState(
    registry: datastore.destinations,
    policyNotifier: ValueNotifier<SyncPolicy>(demoDefaultSyncPolicy),
    eventStore: datastore.eventStore,
    cadence: const Duration(hours: 1),
  );
  await state.startDelivery();
  return _Pane(backend, datastore, state);
}

DemoDestination _primary({
  Connection connection = Connection.ok,
  Set<String> entryTypes = const <String>{'demo_note'},
  bool allowHardDelete = false,
}) => DemoDestination(
  id: 'Primary',
  allowHardDelete: allowHardDelete,
  initialSendLatency: Duration.zero,
  initialConnection: connection,
  filter: SubscriptionFilter(entryTypes: entryTypes),
);

Future<StoredEvent> _note(_Pane pane, String id) async =>
    (await pane.store.append(
      entryType: 'demo_note',
      aggregateId: id,
      aggregateType: 'Note',
      eventType: 'finalized',
      data: const <String, Object?>{
        'answers': <String, Object?>{'title': 't', 'body': 'b'},
      },
      initiator: const UserInitiator('demo-user-1'),
    ))!;

Future<StoredEvent> _green(_Pane pane, String id) async =>
    (await pane.store.append(
      entryType: 'green_button_pressed',
      aggregateId: id,
      aggregateType: 'GreenButton',
      eventType: 'pressed',
      data: const <String, Object?>{'pressed': true},
      initiator: const UserInitiator('demo-user-1'),
    ))!;

/// Pumps, letting real asynchronous work run between frames, until
/// [condition] holds; fails naming [what] after about five seconds.
Future<void> _until(
  WidgetTester tester,
  bool Function() condition,
  String what,
) async {
  for (var i = 0; i < 500 && !condition(); i++) {
    await tester.runAsync(
      () => Future<void>.delayed(const Duration(milliseconds: 10)),
    );
    await tester.pump();
  }
  expect(condition(), isTrue, reason: 'waiting for $what');
}

/// Like [_until], for a [condition] read from the pane's database.
Future<void> _untilAsync(
  WidgetTester tester,
  Future<bool> Function() condition,
  String what,
) async {
  var holds = false;
  for (var i = 0; i < 500 && !holds; i++) {
    await tester.runAsync(() async {
      holds = await condition();
      if (!holds) await Future<void>.delayed(const Duration(milliseconds: 10));
    });
    await tester.pump();
  }
  expect(holds, isTrue, reason: 'waiting for $what');
}

bool _shown(Finder finder) => finder.evaluate().isNotEmpty;

/// Presses the button labelled [label] in the real asynchronous zone the
/// pane's database runs in, and lets that zone run briefly. The handler is
/// not awaited: a caller that depends on its effect waits for that effect.
Future<void> _press(WidgetTester tester, String label) async {
  final button = find.ancestor(
    of: find.text(label),
    matching: find.byWidgetPredicate((w) => w is ButtonStyleButton),
  );
  expect(button, findsOneWidget, reason: 'button $label');
  final onPressed = tester.widget<ButtonStyleButton>(button).onPressed!;
  await tester.runAsync(() async {
    onPressed();
    await Future<void>.delayed(const Duration(milliseconds: 50));
  });
  await tester.pump();
}

/// Mounts [pane]'s WEDGED panel and the FIFO panel of each destination.
Future<void> _mount(WidgetTester tester, _Pane pane) async {
  tester.view.physicalSize = const Size(3000, 1400);
  tester.view.devicePixelRatio = 1.0;
  addTearDown(tester.view.resetPhysicalSize);
  addTearDown(tester.view.resetDevicePixelRatio);
  await tester.runAsync(
    () => tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: Row(
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: <Widget>[
              SizedBox(
                width: 600,
                child: WedgesPanel(
                  watch: StorageWatch(pane.store),
                  databaseId: pane.store.databaseId,
                  appState: pane.state,
                ),
              ),
              SizedBox(
                width: 600,
                child: DetailPanel(
                  watch: StorageWatch(pane.store),
                  databaseId: pane.store.databaseId,
                  appState: pane.state,
                  policyNotifier: pane.state.policyNotifier,
                ),
              ),
              for (final d in pane.state.destinations)
                SizedBox(
                  width: 600,
                  child: FifoPanel(
                    destination: d,
                    watch: StorageWatch(pane.store),
                    appState: pane.state,
                  ),
                ),
            ],
          ),
        ),
      ),
    ),
  );
  await tester.pump();
  // Open each FIFO panel's operations drawer.
  for (final ops in find.text('ops ▸').evaluate().toList()) {
    await tester.tap(find.byWidget(ops.widget));
  }
  await tester.pump();
}

/// Lets the banner timers of the panels run out before the test ends.
Future<void> _finish(WidgetTester tester, _Pane pane) async {
  await tester.pump(const Duration(seconds: 3));
  await tester.runAsync(pane.close);
}

void main() {
  testWidgets('a refused delivery shows in the WEDGED panel, and Recover '
      'ends the wedge', (tester) async {
    late _Pane pane;
    final primary = _primary(connection: Connection.rejecting);
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[primary]);
      await _note(pane, 'n1');
      await pane.cycle();
    });
    await _mount(tester, pane);
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: permanent_refusal')),
      'the wedges view row',
    );
    expect(find.text('Recover'), findsOneWidget);

    primary.connection.value = Connection.ok;
    await _press(tester, 'Recover');
    await _until(
      tester,
      () => _shown(find.text('none')),
      'the row to leave the view',
    );
    late List<FifoEntry> rows;
    await tester.runAsync(() async {
      await pane.cycle();
      rows = await pane.backend.listFifoEntries('Primary');
    });
    expect(rows.first.finalStatus, FinalStatus.tombstoned);
    expect(rows.last.finalStatus, FinalStatus.sent);
    await _finish(tester, pane);
  });

  testWidgets('Halt wedges the head for an operator halt, and Recover '
      'resumes delivery', (tester) async {
    late _Pane pane;
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[_primary()]);
      await _note(pane, 'n1');
      await pane.cycle();
    });
    await _mount(tester, pane);

    await _press(tester, '[Halt]');
    await _until(
      tester,
      () => _shown(find.text('HALT REQUESTED (pause)')),
      'the open-request indicator',
    );
    await tester.runAsync(() async {
      await _note(pane, 'n2');
      await pane.cycle();
    });
    await _until(
      tester,
      () => _shown(
        find.textContaining('Primary: operator_halt, halt by demo-user-1'),
      ),
      'the operator-halt row',
    );
    expect(find.text('HALT REQUESTED (pause)'), findsNothing);
    // The WEDGED column lists the delivery events; the detail panel shows
    // the persisted status.
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: wedged (operator_halt)')),
      'the wedge in the delivery events',
    );
    expect(find.textContaining('Primary: halt requested'), findsOneWidget);
    await _until(
      tester,
      () => _shown(
        find.textContaining(
          'Primary: halt -, wedge operator_halt, refill guard -',
        ),
      ),
      'the detail status line',
    );
    expect(find.textContaining('delivery cycle: running'), findsOneWidget);
    expect(find.textContaining(RegExp(r'drainer: epoch \d+')), findsOneWidget);
    late StoredEvent wedge;
    await tester.runAsync(() async {
      wedge = (await pane.backend.findAllEvents(
        entryType: kDestinationWedgedEntryType,
      )).single;
    });
    expect(wedge.data['cause'], WedgeCause.operatorHalt.wire);
    expect(wedge.data['halt_purpose'], HaltPurpose.pause.wire);

    await _press(tester, 'Recover');
    await _until(
      tester,
      () => _shown(find.text('none')),
      'the row to leave the view',
    );
    late List<FifoEntry> rows;
    await tester.runAsync(() async {
      await pane.cycle();
      rows = await pane.backend.listFifoEntries('Primary');
    });
    expect(rows.last.finalStatus, FinalStatus.sent);
    expect(rows.last.eventIds, isNotEmpty);
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: recovered')),
      'the recovery in the delivery events',
    );
    await _finish(tester, pane);
  });

  testWidgets('Cancel halt closes the open request; with none open it is '
      'refused', (tester) async {
    late _Pane pane;
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[_primary()]);
    });
    await _mount(tester, pane);

    await _press(tester, '[Cancel halt]');
    await _until(
      tester,
      () => _shown(find.textContaining('cancel refused')),
      'the refusal',
    );

    await _press(tester, '[Halt]');
    await _until(
      tester,
      () => _shown(find.text('HALT REQUESTED (pause)')),
      'the open-request indicator',
    );
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: halt pause, wedge -')),
      'the open request in the detail status',
    );
    await _press(tester, '[Cancel halt]');
    await _until(
      tester,
      () => !_shown(find.text('HALT REQUESTED (pause)')),
      'the indicator to clear',
    );
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: halt cancelled')),
      'the cancellation in the delivery events',
    );
    late StoredEvent cancellation;
    late List<FifoEntry> rows;
    await tester.runAsync(() async {
      cancellation = (await pane.backend.findAllEvents(
        entryType: kDestinationHaltCancelledEntryType,
      )).single;
      await _note(pane, 'n1');
      await pane.cycle();
      rows = await pane.backend.listFifoEntries('Primary');
    });
    expect(cancellation.initiator, const UserInitiator('demo-user-1'));
    // Nothing wedges: the note is delivered.
    expect(rows.single.finalStatus, FinalStatus.sent);
    await _finish(tester, pane);
  });

  test('one drainer reconfiguration runs at a time, and the pane drains '
      'the registry it ends with', () async {
    final pane = await _mkPane(<Destination>[
      _primary(entryTypes: const <String>{'demo_note', 'green_button_pressed'}),
    ]);
    addTearDown(pane.close);
    final narrowed = narrowFilter(pane.state.registry.byId('Primary')!.filter)!;
    final first = pane.state.reconfigureDrainer('Primary', narrowed);
    expect(
      () => pane.state.reconfigureDrainer('Primary', narrowed),
      throwsStateError,
    );
    await first;
    final cycle = pane.state.cycle!;
    expect(cycle.state, SyncCycleState.running);
    expect(pane.state.reconfiguredDestinationIds, <String>{'Primary'});
    // The running cycle drains the pane's current registry: a note and a
    // green press, and only the note (the narrowed filter) is enqueued.
    await _green(pane, 'g1');
    final note = await _note(pane, 'n1');
    await pane.cycle();
    final rows = await pane.backend.listFifoEntries('Primary');
    expect(
      <String>[for (final r in rows) ...r.eventIds],
      <String>[note.eventId],
    );
  });

  test(
    'a refused reconfiguration leaves the pane draining its registry',
    () async {
      final pane = await _mkPane(<Destination>[_primary()]);
      addTearDown(pane.close);
      final before = pane.state.registry;
      await expectLater(
        pane.state.reconfigureDrainer('nowhere', const SubscriptionFilter()),
        throwsArgumentError,
      );
      expect(pane.state.cycle!.state, SyncCycleState.running);
      expect(identical(pane.state.registry, before), isTrue);
    },
  );

  testWidgets('a halt on an empty queue wedges the first item enqueued', (
    tester,
  ) async {
    late _Pane pane;
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[_primary()]);
    });
    await _mount(tester, pane);
    await _press(tester, '[Halt]');
    await _until(
      tester,
      () => _shown(find.text('HALT REQUESTED (pause)')),
      'the open-request indicator',
    );
    late List<FifoEntry> before;
    await tester.runAsync(() async {
      await pane.cycle();
      before = await pane.backend.listFifoEntries('Primary');
    });
    expect(before, isEmpty);
    expect(find.text('HALT REQUESTED (pause)'), findsOneWidget);

    late StoredEvent note;
    late List<FifoEntry> after;
    await tester.runAsync(() async {
      note = await _note(pane, 'first');
      await pane.cycle();
      after = await pane.backend.listFifoEntries('Primary');
    });
    expect(after, hasLength(1));
    expect(after.single.finalStatus, FinalStatus.wedged);
    expect(after.single.eventIds, <String>[note.eventId]);
    expect(after.single.attempts, isEmpty);
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: operator_halt')),
      'the operator-halt row',
    );
    expect(find.text('HALT REQUESTED (pause)'), findsNothing);
    await _finish(tester, pane);
  });

  testWidgets('a halt for reconfiguration is recovered only once the drainer '
      'runs a changed configuration, and the refill follows it', (
    tester,
  ) async {
    late _Pane pane;
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[
        _primary(
          entryTypes: const <String>{'demo_note', 'green_button_pressed'},
        ),
      ]);
      await _note(pane, 'n1');
      await pane.cycle();
    });
    await _mount(tester, pane);
    await _press(tester, '[Halt to reconfigure]');
    await _until(
      tester,
      () => _shown(find.text('HALT REQUESTED (reconfigure)')),
      'the open-request indicator',
    );
    late StoredEvent green;
    late StoredEvent note;
    await tester.runAsync(() async {
      green = await _green(pane, 'g1');
      note = await _note(pane, 'n2');
      await pane.cycle();
    });
    await _until(
      tester,
      () => _shown(find.textContaining('Primary: operator_halt')),
      'the operator-halt row',
    );

    // Refused while the drainer declares the configuration the halt
    // recorded.
    await _press(tester, 'Recover');
    await _until(
      tester,
      () => _shown(find.textContaining('recovery refused')),
      'the refusal',
    );
    expect(find.textContaining('reconfigure halt'), findsOneWidget);
    expect(find.textContaining('Primary: operator_halt'), findsOneWidget);

    // A new configuration: the drainer restarts with the filter narrowed
    // to notes.
    await _press(tester, '[Reconfigure drainer]');
    await _until(
      tester,
      () => pane.state.reconfiguredDestinationIds.contains('Primary'),
      'the drainer to restart',
    );
    await tester.runAsync(pane.cycle);
    await _press(tester, 'Recover');
    await _until(
      tester,
      () => _shown(find.text('none')),
      'the row to leave the view',
    );
    late List<FifoEntry> rows;
    await tester.runAsync(() async {
      await pane.cycle();
      rows = await pane.backend.listFifoEntries('Primary');
    });
    final tombstoned = rows.indexWhere(
      (r) => r.finalStatus == FinalStatus.tombstoned,
    );
    final refilled = <String>[
      for (final r in rows.skip(tombstoned + 1)) ...r.eventIds,
    ];
    expect(refilled, contains(note.eventId));
    expect(
      refilled,
      isNot(contains(green.eventId)),
      reason: 'the refill follows the narrowed filter',
    );
    await _finish(tester, pane);
  });

  testWidgets('delete is refused on a pending head and points at Halt; after '
      'a halt it deletes, keeping the delivered items', (tester) async {
    late _Pane pane;
    final primary = _primary(allowHardDelete: true);
    await tester.runAsync(() async {
      pane = await _mkPane(<Destination>[primary]);
      await _note(pane, 'n1');
      await pane.cycle();
      primary.connection.value = Connection.broken;
      await _note(pane, 'n2');
      await pane.cycle();
    });
    await _mount(tester, pane);

    await _press(tester, '[Delete destination]');
    await _until(
      tester,
      () => _shown(find.textContaining('delete refused')),
      'the refusal',
    );
    expect(find.textContaining('Use [Halt]'), findsOneWidget);
    expect(pane.state.deletedDestinationIds, isEmpty);

    // The request wakes the pane's delivery cycle, which honours it by
    // wedging the head; wait for that effect rather than for time.
    await _press(tester, '[Halt]');
    await _untilAsync(
      tester,
      () async =>
          (await pane.backend.readFifoHead('Primary'))?.finalStatus ==
          FinalStatus.wedged,
      'the halted head to wedge',
    );
    late List<StoredEvent> wedges;
    await tester.runAsync(() async {
      wedges = await pane.backend.findAllEvents(
        entryType: kDestinationWedgedEntryType,
      );
    });
    expect(
      wedges.map((e) => e.data['cause']).toList(),
      <Object?>['operator_halt'],
      reason: 'the head wedged for the operator halt, not a send failure',
    );

    await _press(tester, '[Delete destination]');
    await _until(
      tester,
      () => pane.state.deletedDestinationIds.contains('Primary'),
      'the deletion',
    );
    await tester.runAsync(
      () => tester.pumpWidget(
        MaterialApp(
          home: Scaffold(
            body: SizedBox(
              height: 400,
              child: DeletedFifoPanel(
                destinationId: 'Primary',
                watch: StorageWatch(pane.store),
              ),
            ),
          ),
        ),
      ),
    );
    await _until(
      tester,
      () => _shown(find.textContaining('sent')),
      'the retained delivered item',
    );
    expect(find.textContaining('tombstoned'), findsOneWidget);
    await _finish(tester, pane);
  });

  testWidgets("a peer's wedge shows in the hub as a peer row with no action, "
      'and leaves once the peer recovers', (tester) async {
    late _Pane hub;
    late _Pane mobile;
    final mobilePrimary = _primary(connection: Connection.rejecting);
    await tester.runAsync(() async {
      hub = await _mkPane(<Destination>[_primary()]);
      mobile = await _mkPane(
        <Destination>[
          mobilePrimary,
          NativeDemoDestination(
            id: 'NativeAudit',
            filter: const SubscriptionFilter(
              entryTypes: <String>{},
              includeSystemEvents: true,
            ),
            bridge: DownstreamBridge(hub.store),
          ),
        ],
        identifier: '66666666-6666-4666-8666-666666666666',
        hop: 'mobile-device',
      );
      await _note(mobile, 'n1');
      // The first pass wedges mobile's Primary; later passes forward the
      // wedge event through NativeAudit to the hub.
      for (var i = 0; i < 4; i++) {
        await mobile.cycle();
      }
      await hub.cycle();
    });
    await _mount(tester, hub);
    final mobileId = mobile.store.databaseId.substring(0, 8);
    await _until(
      tester,
      () => _shown(
        find.textContaining(
          'Primary: permanent_refusal, 1 attempts -- peer, from database '
          '$mobileId',
        ),
      ),
      'the peer row',
    );
    expect(find.text('Recover'), findsNothing);
    // The hub's own Primary is unaffected by the peer's wedge: it keeps
    // delivering, and the hub records no wedge of its own.
    late List<FifoEntry> hubRows;
    late DeliveryStatus hubStatus;
    await tester.runAsync(() async {
      await _note(hub, 'hub-note');
      await hub.cycle();
      hubRows = await hub.backend.listFifoEntries('Primary');
      hubStatus = await hub.state.readDeliveryStatus();
    });
    expect(hubRows, isNotEmpty);
    expect(hubRows.map((r) => r.finalStatus).toSet(), <FinalStatus?>{
      FinalStatus.sent,
    });
    expect(hubStatus.destinations['Primary']!.wedge, isNull);

    // Mobile recovers; the recovery event is forwarded and ends the row.
    await tester.runAsync(() async {
      mobilePrimary.connection.value = Connection.ok;
      final head = (await mobile.backend.readFifoHead('Primary'))!;
      await mobile.state.recover('Primary', head.entryId);
      for (var i = 0; i < 4; i++) {
        await mobile.cycle();
      }
    });
    await _until(
      tester,
      () => _shown(find.text('none')),
      'the peer row to leave',
    );
    await tester.pump(const Duration(seconds: 3));
    await tester.runAsync(() async {
      await mobile.close();
      await hub.close();
    });
  });
}
