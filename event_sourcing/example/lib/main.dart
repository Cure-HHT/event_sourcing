import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/app_state.dart';
import 'package:event_sourcing_demo/database_reset_notice.dart';
import 'package:event_sourcing_demo/demo_destination.dart';
import 'package:event_sourcing_demo/demo_sync_policy.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/dual_demo_app.dart';
import 'package:event_sourcing_demo/native_demo_destination.dart';
import 'package:flutter/material.dart';
import 'package:path/path.dart' as p;
import 'package:path_provider/path_provider.dart';
import 'package:uuid/uuid.dart';

/// Reads a persisted install UUID from [path], or mints + persists a new
/// UUIDv4 if the file does not exist.
Future<String> _readOrMintUUID(String path) async {
  final f = File(path);
  try {
    return (await f.readAsString()).trim();
  } on PathNotFoundException {
    final id = const Uuid().v4();
    await f.writeAsString(id);
    return id;
  }
}

class _PaneRuntime {
  _PaneRuntime({
    required this.datastore,
    required this.appState,
    required this.dbPath,
    required this.policyNotifier,
  });

  final EventStoreBundle datastore;
  final AppState appState;
  final String dbPath;
  final ValueNotifier<SyncPolicy> policyNotifier;
}

/// Bootstraps one datastore with its own destinations and starts its
/// delivery cycle. The optional [bridge] is wired into the Native
/// destination's `send()` so mobile's outgoing wire stream lands in
/// hub's `EventStore.ingestBatch`. The hub pane passes
/// `bridge: null` so its Native destination's `send()` is a no-op
/// simulator.
///
/// The pane's `SyncCycle` fills every destination's queue from the log and
/// drains it with the live policy from the pane's policyNotifier. Every
/// append and every committed registry operation wakes it, and it runs a
/// pass at least once a second.
Future<_PaneRuntime> _bootstrapPane({
  required String dbPath,
  required Source source,
  DownstreamBridge? bridge,
}) async {
  final policyNotifier = ValueNotifier<SyncPolicy>(demoDefaultSyncPolicy);

  final primary = DemoDestination(
    id: 'Primary',
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
    allowHardDelete: true,
    filter: const SubscriptionFilter(
      entryTypes: <String>{'green_button_pressed', 'blue_button_pressed'},
    ),
  );
  // Two native-wire destinations so the dual-pane UI shows both lanes
  // reaching the downstream bridge:
  //   - NativeUser ships user-payload events only (the demo's button
  //     entry types). Wedge isolation: a poison user event wedges this
  //     destination without affecting NativeAudit.
  //   - NativeAudit ships system audit events only
  //     (`includeSystemEvents: true` plus an empty entryTypes set),
  //     demonstrating the opt-in system-event path and its cross-hop
  //     forensic visibility.
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

  // Projection spec for notes view. AggregateProjectionSpec folds
  // demo_note events (aggregateType 'Note') into the notes view.
  // The tombstone event type deletes the row so the MATERIALIZED panel
  // reflects deletions live. Button-press entry types are excluded by the
  // interest filter (they use different aggregateTypes).
  final diaryProjections = ProjectionRegistry()
    ..register(
      const AggregateProjectionSpec(
        viewName: 'notes',
        interest: SubscriptionFilter(entryTypes: <String>{'demo_note'}),
        tombstoneEventTypes: <String>{'tombstone'},
      ),
    );

  // The library opens the database file, and closes it when the event
  // store closes or when the open fails.
  final datastore = await bootstrapEventStore(
    storage: SembastStorage.file(dbPath),
    source: source,
    entryTypes: allDemoEntryTypes,
    destinations: <Destination>[primary, secondary, nativeUser, nativeAudit],
    projections: diaryProjections,
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

  final appState = AppState(
    registry: datastore.destinations,
    policyNotifier: policyNotifier,
    eventStore: datastore.eventStore,
  );

  try {
    await appState.startDelivery();
  } on Object {
    await datastore.eventStore.close();
    rethrow;
  }

  return _PaneRuntime(
    datastore: datastore,
    appState: appState,
    dbPath: dbPath,
    policyNotifier: policyNotifier,
  );
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  final appSupportDir = await getApplicationSupportDirectory();
  final demoDir = Directory(p.join(appSupportDir.path, 'event_sourcing_demo'));
  runApp(await buildDemoApp(demoDir));
}

/// Opens both panes over the files in [demoDir] and returns the dual-pane
/// app, or, when a database file does not open under this build (an
/// earlier data format, or another data-format major), an app naming the
/// files to delete.
Future<Widget> buildDemoApp(Directory demoDir) async {
  await demoDir.create(recursive: true);

  final mobileInstallUUID = await _readOrMintUUID(
    p.join(demoDir.path, 'MOBILE.install.uuid'),
  );
  final hubInstallUUID = await _readOrMintUUID(
    p.join(demoDir.path, 'HUB.install.uuid'),
  );

  final mobileDbPath = p.join(demoDir.path, 'demo.db');
  final hubDbPath = p.join(demoDir.path, 'demo_hub.db');
  stdout
    ..writeln('[demo] mobile storage: $mobileDbPath')
    ..writeln('[demo] hub storage: $hubDbPath')
    ..writeln('[demo] mobile install UUID: $mobileInstallUUID')
    ..writeln('[demo] hub install UUID: $hubInstallUUID');

  // Hub must be bootstrapped first so the bridge can capture its
  // EventStore before mobile's NativeDemoDestination is constructed.
  _PaneRuntime? hub;
  final _PaneRuntime mobile;
  try {
    hub = await _bootstrapPane(
      dbPath: hubDbPath,
      source: Source(
        hopId: 'hub-server',
        identifier: hubInstallUUID,
        softwareVersion: 'event_sourcing_demo@0.1.0+1',
      ),
    );

    final bridge = DownstreamBridge(hub.datastore.eventStore);

    mobile = await _bootstrapPane(
      dbPath: mobileDbPath,
      source: Source(
        hopId: 'mobile-device',
        identifier: mobileInstallUUID,
        softwareVersion: 'event_sourcing_demo@0.1.0+1',
      ),
      bridge: bridge,
    );
  } on Object catch (e) {
    if (!needsDatabaseReset(e)) rethrow;
    if (hub != null) {
      await hub.appState.stopDelivery();
      await hub.datastore.eventStore.close();
    }
    stderr.writeln('[demo] $e');
    return DatabaseResetRequiredApp(
      message: databaseResetMessage(e, <String>[mobileDbPath, hubDbPath]),
    );
  }

  return DualDemoApp(
    top: DemoPaneConfig(
      datastore: mobile.datastore,
      appState: mobile.appState,
      dbPath: mobile.dbPath,
      policyNotifier: mobile.policyNotifier,
      paneLabel: 'MOBILE',
    ),
    bottom: DemoPaneConfig(
      datastore: hub.datastore,
      appState: hub.appState,
      dbPath: hub.dbPath,
      policyNotifier: hub.policyNotifier,
      paneLabel: 'HUB',
    ),
  );
}
