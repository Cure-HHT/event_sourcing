// Every transaction body of the drain outcome, the fill and the destination
// registry keeps what describes a run inside the run: when the storage layer
// runs a body twice (RerunningSembastBackend rolls the first run back and
// commits the second, as Postgres does after a serialization conflict and
// sembast_web after another tab commits first), what the operation returns,
// records and appends reflects only the committed run.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/drain_wedge_conformance.dart'
    show expectWedgeRecordMatchesLog;
import '../test_support/fake_destination.dart';
import '../test_support/queue_test_support.dart';
import '../test_support/rerunning_sembast_backend.dart';
import '../test_support/wedges_view_invariant.dart';

const _init = AutomationInitiator(service: 'rerun');
const _source = Source(
  hopId: 'mobile-device',
  identifier: 'rerun-install',
  softwareVersion: 'test@1.0.0',
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

void main() {
  late RerunningSembastBackend backend;
  late EventStore store;
  late DestinationRegistry registry;

  setUp(() async {
    backend = await RerunningSembastBackend.openInMemory('queue-rerun')
      ..rerunEnabled = false;
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: 'rerun_note',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'rerun_note',
      ),
    );
    store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: SembastSecurityContextStore(backend: backend),
      clock: () => DateTime.utc(2026, 3, 1),
    );
    registry = DestinationRegistry(eventStore: store);
    backend.rerunEnabled = true;
  });

  tearDown(() => backend.close());

  Future<List<StoredEvent>> audits(String entryType) async => <StoredEvent>[
    for (final e in await backend.findAllEvents())
      if (e.entryType == entryType) e,
  ];

  Future<void> note(String id) => store.append(
    entryType: 'rerun_note',
    aggregateId: id,
    aggregateType: 'note',
    eventType: 'noted',
    data: <String, Object?>{'id': id},
    initiator: const UserInitiator('u'),
  );

  // Verifies: EVS-PRD-event-log/G
  // the registration's identity is the
  //   committed run's registration event, appended once.
  // Verifies: EVS-DEV-destination-drain/A
  // the registration writes its schedule in
  //   the committed run only.
  test("addDestination records the committed run's registration", () async {
    await registry.addDestination(FakeDestination(id: 'x'), initiator: _init);
    final registered = await audits(kDestinationRegisteredEntryType);
    expect(registered, hasLength(1));
    expect(
      (await backend.readSchedule('x'))!.registrationId,
      registered.single.eventId,
    );
  });

  // Verifies: EVS-PRD-event-log/G
  // the date operations append one audit and
  //   report the committed run's result.
  // Verifies: EVS-DEV-destination-drain/E
  // the replay request is decided from the
  //   committed run's reads.
  test('setStartDate and setEndDate report the committed run', () async {
    await registry.addDestination(FakeDestination(id: 'x'), initiator: _init);
    final runs = <String>[];
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(onRegistryBodyRun: runs.add),
      () => registry.setStartDate(
        'x',
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      ),
    );
    expect(runs, <String>['setStartDate', 'setStartDate']);
    expect(await audits(kDestinationStartDateSetEntryType), hasLength(1));
    expect(
      await backend.transaction((t) => backend.readReplayRequestTxn(t, 'x')),
      const ReplayRequest(firstActivation: true),
    );
    final result = await registry.setEndDate(
      'x',
      DateTime.utc(2020, 1, 1),
      initiator: _init,
    );
    expect(result, SetEndDateResult.closed);
    expect(await audits(kDestinationEndDateSetEntryType), hasLength(1));
  });

  // Verifies: EVS-DEV-destination-drain/U
  // a refusal is decided in each run, the
  //   committed run writes the check record, and the operation throws once
  //   after the commit.
  test('a refusal is thrown once, after the committed run', () async {
    final runs = <String>[];
    await expectLater(
      runWithDeliveryTestHooks(
        DeliveryTestHooks(onRegistryBodyRun: runs.add),
        () => registry.setEndDate(
          'ghost',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        ),
      ),
      throwsArgumentError,
    );
    expect(runs, hasLength(2));
    final check = await backend.transaction(backend.readRegistryCheckTxn);
    expect(check?.op, 'setEndDate');
    expect(check?.outcome, 'refused_unknown_destination');
  });

  // Verifies: EVS-DEV-destination-drain/U
  // the refusal of an invalid destination identifier is decided in each
  //   run, the committed run writes the one check record, and the operation
  //   throws once after the commit.
  // Verifies: EVS-DEV-destination-drain/K
  // registration refuses a destination identifier containing `|` under
  //   re-runs, with nothing written but the check record.
  test(
    'an identifier refusal is thrown once, after the committed run',
    () async {
      final runs = <String>[];
      final before = await backend.findAllEvents();
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(onRegistryBodyRun: runs.add),
          () => registry.addDestination(
            FakeDestination(id: 'a|b'),
            initiator: _init,
          ),
        ),
        throwsArgumentError,
      );
      expect(runs, hasLength(2));
      final check = await backend.transaction(backend.readRegistryCheckTxn);
      expect(check?.op, 'addDestination');
      expect(check?.outcome, 'refused_invalid_identifier');
      expect(
        (await backend.findAllEvents()).map((e) => e.eventId),
        before.map((e) => e.eventId),
      );
      expect(registry.byId('a|b'), isNull);
    },
  );

  // Verifies: EVS-PRD-event-log/G
  // the fill enqueues each item once, the
  //   drain records one attempt, and recovery and deletion each append one
  //   event and report the committed run's counts.
  // Verifies: EVS-DEV-destination-drain/C
  // the drain outcome commits once.
  test('fill, drain outcome, recovery and deletion under re-runs', () async {
    final d = FakeDestination(id: 'x', allowHardDelete: true);
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      'x',
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
    await note('n1');
    await note('n2');
    await fillBatch(d, backend: backend, source: _source, clock: _fillNow);
    final rows = await backend.listFifoEntries('x');
    expect(rows, hasLength(2));

    await drain(
      FakeDestination(
        id: 'x',
        script: <SendResult>[const SendPermanent(error: 'no')],
      ),
      registry: registry,
    );
    final wedged = (await backend.readFifoHead('x'))!;
    expect(wedged.finalStatus, FinalStatus.wedged);
    expect(wedged.attempts, hasLength(1));
    await expectWedgesViewMatchesQueue(store);

    final result = await registry.tombstoneAndRefill(
      'x',
      wedged.entryId,
      initiator: _init,
    );
    expect(result.deletedTrailCount, 1);
    expect(await audits(kDestinationWedgeRecoveredEntryType), hasLength(1));
    await expectWedgesViewMatchesQueue(store);

    await fillBatch(d, backend: backend, source: _source, clock: _fillNow);
    await fillBatch(d, backend: backend, source: _source, clock: _fillNow);
    await wedgeHeadForTest(registry, 'x');
    await registry.deleteDestination('x', initiator: _init);
    final deleted = await audits(kDestinationDeletedEntryType);
    expect(deleted, hasLength(1));
    expect(deleted.single.data['deleted_pending_count'], 1);
    expect(deleted.single.data['tombstoned_row_id'], isNotNull);
    await expectWedgesViewMatchesQueue(store);
  });

  Future<WedgeRecord?> wedgeRecord(String destId) =>
      backend.transaction((txn) => backend.readWedgeRecordTxn(txn, destId));

  Future<FakeDestination> queued(String id) async {
    final d = FakeDestination(
      id: id,
      script: <SendResult>[const SendPermanent(error: 'no')],
    );
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(id, DateTime.utc(2026, 1, 1), initiator: _init);
    await note('$id-n1');
    await fillBatch(d, backend: backend, source: _source, clock: _fillNow);
    return d;
  }

  // Verifies: EVS-PRD-event-log/G
  // a wedge whose transaction body runs
  //   twice appends, records and publishes only the committed run's wedge
  //   event.
  // Verifies: EVS-PRD-destinations/P
  // the committed run's attempt, wedged
  //   status, event and record are present once each.
  test('a wedge under re-runs records the committed run', () async {
    final d = await queued('x');
    final published = <StoredEvent>[];
    final sub = store
        .subscribe<StoredEvent>(
          const SubscriptionFilter(
            entryTypes: <String>{},
            includeSystemEvents: true,
            eventTypes: <String>{kDestinationWedgedEventType},
          ),
          const Events(),
        )
        .listen((u) {
          if (u is Delta<StoredEvent>) published.add(u.value);
        });
    final runsBefore = backend.bodyRuns;
    await drain(d, registry: registry);
    await pumpEventQueue();
    await sub.cancel();
    expect(backend.bodyRuns - runsBefore, greaterThanOrEqualTo(2));
    final events = await audits(kDestinationWedgedEntryType);
    expect(events, hasLength(1));
    expect(published.map((e) => e.eventId), [events.single.eventId]);
    expect((await wedgeRecord('x'))?.wedgeEventId, events.single.eventId);
    final head = (await backend.readFifoHead('x'))!;
    expect(head.finalStatus, FinalStatus.wedged);
    expect(head.attempts, hasLength(1));
    await expectWedgeRecordMatchesLog(store, 'x');
    await expectWedgesViewMatchesQueue(store);
  });

  // Verifies: EVS-PRD-event-log/G
  // the wedge the next pass derives from the
  //   recorded attempts appends and records only the committed run's event.
  // Verifies: EVS-DEV-destination-drain/J
  // the derived wedge adds no attempt.
  test('a derived wedge under re-runs records the committed run', () async {
    final d = await queued('x');
    backend.rerunEnabled = false;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(afterWedgeHeadInTxn: (id) => true),
      () => drain(d, registry: registry),
    );
    expect((await backend.readFifoHead('x'))!.finalStatus, isNull);
    await expectWedgeRecordMatchesLog(store, 'x');
    backend.rerunEnabled = true;
    await drain(d, registry: registry);
    final events = await audits(kDestinationWedgedEntryType);
    expect(events, hasLength(1));
    expect((await wedgeRecord('x'))?.wedgeEventId, events.single.eventId);
    final head = (await backend.readFifoHead('x'))!;
    expect(head.finalStatus, FinalStatus.wedged);
    expect(head.attempts, hasLength(1));
    expect(d.sent, hasLength(1));
    await expectWedgeRecordMatchesLog(store, 'x');
    await expectWedgesViewMatchesQueue(store);
  });
}
