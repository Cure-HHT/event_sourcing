// Release probe for the library's test seams.
//
// Installs every test seam an in-memory Sembast database on io reaches,
// each recording that it fired and each failure injection set to fail, and
// substitutes another build's versions for the boot, then opens the event
// store, runs a registry operation, two delivery passes (one destination
// delivers, one refuses and wedges) and a heartbeat of a drain lock over an
// in-memory Sembast database. Run without assertions (`dart run --no-enable-asserts`,
// or a `dart compile exe` executable) it must print an empty list of fired
// seams and a completed delivery, and exit 0. Run with assertions enabled
// the same body reports the seams that fired, and the process exits 1. The
// seams only a Postgres backend or a browser reaches (the generation
// guard's lock session, provisioning, Web Locks) are read through the same
// `DeliveryTestHooks.current` gate this probe exercises, which is null
// without assertions for every seam alike. The probe also calls the
// test-only `EventStore.openForTest` over a fresh in-memory database: without
// assertions it must refuse with `StateError` and leave that database
// untouched.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:sembast/sembast_memory.dart';

/// Outcome of one probe run.
class ProbeOutcome {
  ProbeOutcome({
    required this.firedSeams,
    required this.delivered,
    required this.storedEndDate,
    required this.registryEndDate,
    required this.endDateSetEvents,
    required this.sequenceAdvance,
    required this.sentItems,
    required this.wedgeEvents,
    required this.recordedVersion,
    required this.recordedDataFormat,
    required this.openForTest,
    required this.openForTestWrites,
  });

  /// Seams that fired, in order.
  final List<String> firedSeams;

  /// Payloads the healthy destination received.
  final int delivered;

  /// The healthy destination's stored end date after the registry
  /// operation (ISO-8601), or null.
  final String? storedEndDate;

  /// The healthy destination's end date as the registry reports it after
  /// the registry operation (ISO-8601), or null.
  final String? registryEndDate;

  /// `system.destination_end_date_set` events in the log after the
  /// registry operation.
  final int endDateSetEvents;

  /// How far the registry operation advanced the sequence counter.
  final int sequenceAdvance;

  /// The healthy destination's queue items marked sent after the passes.
  final int sentItems;

  /// Wedge events in the log after the passes (the refusing destination
  /// wedges once when no injection takes effect).
  final int wedgeEvents;

  /// The package version the database's `lib_version_initialized` records.
  final String? recordedVersion;

  /// The data format the database's `lib_version_initialized` records.
  final DataFormatVersion? recordedDataFormat;

  /// What the test-only open did over a fresh database: `refused` (it threw
  /// `StateError`) or `opened`.
  final String openForTest;

  /// The records the fresh database held after the test-only open: its
  /// bookkeeping records and its events.
  final int openForTestWrites;

  /// True when no seam fired, the test-only open refused and wrote nothing,
  /// the passes delivered the one event, its outcome committed, and the
  /// refusing destination's wedge committed.
  bool get passed =>
      firedSeams.isEmpty &&
      openForTest == 'refused' &&
      openForTestWrites == 0 &&
      delivered == 1 &&
      sentItems == 1 &&
      wedgeEvents == 1 &&
      recordedVersion == LibVersion.version &&
      recordedDataFormat == LibVersion.dataFormat;

  Map<String, Object?> toJson() => <String, Object?>{
    'fired_seams': firedSeams,
    'delivered': delivered,
    'stored_end_date': storedEndDate,
    'registry_end_date': registryEndDate,
    'end_date_set_events': endDateSetEvents,
    'sequence_advance': sequenceAdvance,
    'sent_items': sentItems,
    'wedge_events': wedgeEvents,
    'recorded_version': recordedVersion,
    'recorded_data_format': recordedDataFormat?.toString(),
    'open_for_test': openForTest,
    'open_for_test_writes': openForTestWrites,
    'passed': passed,
  };
}

class _ProbeDestination extends Destination {
  _ProbeDestination(this.id, {this.failTransform = false, this.refuse = false});

  @override
  final String id;

  /// When true, `send` reports a permanent failure, so the drain wedges the
  /// head.
  final bool refuse;

  /// When true, `transform` throws, so the delivery cycle logs a fill
  /// failure for this destination.
  final bool failTransform;

  int sends = 0;

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'probe_event'});

  @override
  String get wireFormat => 'probe-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async {
    if (failTransform) throw StateError('probe transform failure');
    return WirePayload(
      bytes: Uint8List.fromList(
        utf8.encode(jsonEncode(<String, Object?>{'n': batch.length})),
      ),
      contentType: 'application/json',
      transformVersion: 'probe-1',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    sends += 1;
    return refuse
        ? const SendPermanent(error: 'probe refusal')
        : const SendOk();
  }
}

/// Calls the test-only open over a fresh in-memory database; returns what it
/// did and how many records the database then holds.
Future<({String outcome, int writes})> _probeOpenForTest() async {
  final db = await newDatabaseFactoryMemory().openDatabase('untouched.db');
  final backend = SembastBackend(database: db);
  String outcome;
  try {
    // The probe calls the test-only open as production code would, to show
    // that a build without assertions refuses it.
    // ignore: invalid_use_of_visible_for_testing_member
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: EntryTypeRegistry(),
      source: const Source(
        hopId: 'probe',
        identifier: 'probe-untouched',
        softwareVersion: 'probe@1',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
    outcome = 'opened';
    await store.close();
    // The refusal the probe checks for is a StateError.
    // ignore: avoid_catching_errors
  } on StateError {
    outcome = 'refused';
  }
  final writes =
      await StoreRef<String, Object?>('backend_state').count(db) +
      (await backend.findAllEvents()).length;
  await backend.close();
  return (outcome: outcome, writes: writes);
}

/// Runs the probe body: the test-only open over a fresh database, then
/// seams installed around a registry operation and a delivery pass.
Future<ProbeOutcome> runHooksReleaseProbe() async {
  final openForTest = await _probeOpenForTest();
  final db = await newDatabaseFactoryMemory().openDatabase('probe.db');
  final backend = SembastBackend(database: db);
  final healthy = _ProbeDestination('probe_healthy');
  final broken = _ProbeDestination('probe_broken', failTransform: true);
  final refusing = _ProbeDestination('probe_refusing', refuse: true);
  final fired = <String>[];
  // The boot runs with its seams installed and another build's versions
  // substituted; without assertions it records the compiled versions.
  final bootHooks = DeliveryTestHooks(
    onBootBodyRun: () => fired.add('onBootBodyRun'),
    afterBootVersionEvent: () {
      fired.add('afterBootVersionEvent');
      return false;
    },
    buildDeclaration: (
      version: '9.9.9',
      dataFormat: DataFormatVersion(LibVersion.dataFormat.major, 9),
    ),
  );
  final bundle = await runWithDeliveryTestHooks(
    bootHooks,
    () => bootstrapEventStore(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      source: const Source(
        hopId: 'probe',
        identifier: 'probe-install',
        softwareVersion: 'probe@1',
      ),
      entryTypes: const <EntryTypeDefinition>[
        EntryTypeDefinition(
          id: 'probe_event',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'Probe event',
        ),
      ],
      destinations: <Destination>[healthy, broken, refusing],
    ),
  );
  final initialized = (await backend.findAllEvents(
    entryType: kLibVersionInitializedEntryType,
  )).single;
  const initiator = AutomationInitiator(service: 'hooks-release-probe');
  final start = DateTime.utc(2000);
  for (final d in <Destination>[healthy, broken, refusing]) {
    await bundle.destinations.setStartDate(d.id, start, initiator: initiator);
  }
  await bundle.eventStore.append(
    entryType: 'probe_event',
    aggregateId: 'probe-1',
    aggregateType: 'Probe',
    eventType: 'finalized',
    data: const <String, Object?>{'n': 1},
    initiator: initiator,
  );

  var fillFailures = 0;
  var wedgeFailures = 0;
  final hooks = DeliveryTestHooks(
    onLog: (record) => fired.add('onLog ${record.name}: ${record.message}'),
    // Fails every registry operation's audit, but not the wedge event's
    // append, so the wedge reaches afterWedgeHeadInTxn.
    failRegistryAuditAppend: (entryType) {
      fired.add('failRegistryAuditAppend $entryType');
      return entryType != kDestinationWedgedEntryType;
    },
    // The first pass's wedge fails inside its transaction, so the refusal
    // is recorded alone; the second pass wedges the head from that record,
    // and that wedge commits but reports failure.
    afterWedgeHeadInTxn: (destinationId) {
      fired.add('afterWedgeHeadInTxn $destinationId');
      return wedgeFailures++ == 0;
    },
    afterWedgeTransaction: (destinationId) {
      fired.add('afterWedgeTransaction $destinationId');
      return true;
    },
    // The healthy destination's first fill transaction fails; its second
    // pass fills, sends and then fails the delivery's outcome transaction.
    // Keyed to the healthy destination because a pass fills its
    // destinations concurrently, so "the first fill" is not deterministic.
    failFillTransaction: (destinationId) {
      fired.add('failFillTransaction $destinationId');
      return destinationId == healthy.id && fillFailures++ == 0;
    },
    failOutcomeTransaction: (destinationId, outcome) {
      fired.add('failOutcomeTransaction $destinationId $outcome');
      return outcome == 'ok';
    },
    beforeRegistryTransaction: (op) async {
      fired.add('beforeRegistryTransaction $op');
    },
    onRegistryBodyRun: (op) => fired.add('onRegistryBodyRun $op'),
    insideTransform: (destinationId) async {
      fired.add('insideTransform $destinationId');
    },
    afterFillReads: (destinationId) async {
      fired.add('afterFillReads $destinationId');
    },
    afterHaltLoopTopRead: (destinationId) async {
      fired.add('afterHaltLoopTopRead $destinationId');
    },
    beforeSendFence: (destinationId) async {
      fired.add('beforeSendFence $destinationId');
    },
    onFenceBodyRun: (destinationId) =>
        fired.add('onFenceBodyRun $destinationId'),
    // Observed only: every pass reads the persisted schedules.
    failListSchedules: () {
      fired.add('failListSchedules');
      return false;
    },
    // Observed only: the cycle takes the drain lock, so its passes run.
    failLockAcquisition: () {
      fired.add('failLockAcquisition');
      return false;
    },
    failAfterExclusionObtained: () {
      fired.add('failAfterExclusionObtained');
      return false;
    },
    beforeGrantDelivered: () async {
      fired.add('beforeGrantDelivered');
    },
    // Fails the heartbeat the probe runs after the passes.
    failNextHeartbeat: () {
      fired.add('failNextHeartbeat');
      return true;
    },
    onInboundPoll: () => fired.add('onInboundPoll'),
    beforeQueueWrites: (destinationId) async {
      fired.add('beforeQueueWrites $destinationId');
    },
    afterSendBeforeOutcome: (destinationId) async {
      fired.add('afterSendBeforeOutcome $destinationId');
    },
    timerFactory: (period, callback) {
      fired.add('timerFactory');
      return Timer.periodic(period, callback);
    },
    afterCommitBeforePublish: () async {
      fired.add('afterCommitBeforePublish');
    },
    onDeliveryWake: (cycleWoken) => fired.add('onDeliveryWake $cycleWoken'),
  );
  late final String? storedEndDate;
  late final String? registryEndDate;
  late final int endDateSetEvents;
  late final int sequenceAdvance;
  await runWithDeliveryTestHooks(hooks, () async {
    final sequenceBefore = await backend.readSequenceCounter();
    try {
      await bundle.destinations.setEndDate(
        healthy.id,
        DateTime.utc(2999),
        initiator: initiator,
      );
    } on Object catch (e) {
      fired.add('registry operation failed: $e');
    }
    sequenceAdvance = await backend.readSequenceCounter() - sequenceBefore;
    storedEndDate = (await backend.readSchedule(
      healthy.id,
    ))?.endDate?.toIso8601String();
    registryEndDate = (await bundle.destinations.scheduleOf(
      healthy.id,
    )).endDate?.toIso8601String();
    endDateSetEvents = (await backend.findAllEvents(
      entryType: 'system.destination_end_date_set',
    )).length;
    final cycle = await SyncCycle.start(
      registry: bundle.destinations,
      cadence: const Duration(hours: 1),
    );
    await cycle();
    await cycle();
    await cycle.close();
    // An append after the close wakes no cycle; the wake is observed.
    await bundle.eventStore.append(
      entryType: 'probe_event',
      aggregateId: 'probe-2',
      aggregateType: 'Probe',
      eventType: 'finalized',
      data: const <String, Object?>{'n': 2},
      initiator: initiator,
    );
    // The cycle took the free lock directly and beats it on its cadence,
    // which the probe does not wait for; the probe requests a drain lock of
    // its own, as a waiting cycle does, and beats it.
    final lock = (await backend
        .requestDrainLock(
          databaseId: bundle.eventStore.databaseId,
          retryInterval: const Duration(milliseconds: 10),
        )
        .granted)!;
    try {
      await lock.heartbeat();
    } on Object catch (e) {
      fired.add('drain lock heartbeat failed: $e');
    } finally {
      await lock.release();
    }
  });
  final sentItems = (await backend.listFifoEntries(
    healthy.id,
  )).where((e) => e.finalStatus == FinalStatus.sent).length;
  final wedgeEvents = (await backend.findAllEvents(
    entryType: kDestinationWedgedEntryType,
  )).length;
  await backend.close();
  return ProbeOutcome(
    firedSeams: fired,
    delivered: healthy.sends,
    storedEndDate: storedEndDate,
    registryEndDate: registryEndDate,
    endDateSetEvents: endDateSetEvents,
    sequenceAdvance: sequenceAdvance,
    sentItems: sentItems,
    wedgeEvents: wedgeEvents,
    recordedVersion: initialized.data['version'] as String?,
    recordedDataFormat: DataFormatVersion.fromJson(
      initialized.data['data_format'],
    ),
    openForTest: openForTest.outcome,
    openForTestWrites: openForTest.writes,
  );
}

Future<void> main() async {
  final outcome = await runHooksReleaseProbe();
  stdout.writeln(jsonEncode(outcome.toJson()));
  exit(outcome.passed ? 0 : 1);
}
