// Backend-agnostic scenarios for the admission checks a historical replay
// (buildHistoricalReplayRows) and a gap replay (buildGapReplayRows) apply
// while building their queue items: only the database's own events are
// enqueued, whatever the destination's filter, and a security finding is
// enqueued on every natively serializing destination's channel whatever
// its filter. Both replays are driven through the fill, the way a
// destination's first activation and a backward start-date move drive them
// in production. Run on Sembast by
// test/sync/replay_admission_sembast_test.dart.
//
// This file exposes [runReplayAdmissionScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show buildGapReplayRows, recordFindingInTxnForTest;
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show forkEvidence;
import 'deliveries.dart';
import 'fake_destination.dart';
import 'ingest_record_findings_conformance.dart'
    show relayedRecord, sealedRecord;
import 'native_destination.dart';
import 'queue_test_support.dart';
import 'test_backends.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const String _noteType = 'admission_note';

/// A filter that admits the fixtures' user entry type but (leaving
/// `includeSystemEvents` at its default `false`) no system entry type: a
/// destination carrying it enqueues a security finding only through the
/// channel-wide bypass.
const SubscriptionFilter _admitsOnlyUserEvents = SubscriptionFilter(
  entryTypes: <String>{_noteType},
);

/// The client timestamp every fixture event in these scenarios carries.
final DateTime _fixtureAt = DateTime.utc(2026, 1, 1);

/// The fill clock every scenario runs its fills at: after every fixture.
DateTime _fillNow() => DateTime.utc(2030, 1, 1);

/// The four events a scenario builds: an event the local database holds as
/// its own, a received event another database originated, a
/// held-but-relayed event (two provenance entries, neither naming the
/// local database), and a security finding the local database authored
/// locally.
class _Fixtures {
  const _Fixtures({
    required this.own,
    required this.received,
    required this.relayed,
    required this.finding,
  });

  final StoredEvent own;
  final StoredEvent received;
  final StoredEvent relayed;
  final StoredEvent finding;
}

Future<_Fixtures> _buildFixtures(EventStore store, EventStore peer) async {
  // Every excluded fixture is appended before the admitted one, so a
  // single historical (or gap) replay pass decides all of them: the
  // builder's own cursor advances only to the last item it actually
  // replays, so an excluded fixture appended after it would fall to the
  // plain fill's own admission check on the next pass instead of the
  // replay builder under test.
  final peerEvent = (await peer.append(
    entryType: _noteType,
    aggregateId: 'peer-agg',
    aggregateType: 'note',
    eventType: 'noted',
    data: const <String, Object?>{'id': 'received'},
    initiator: const UserInitiator('u'),
  ))!;
  await deliverEventsTo(store, <StoredEvent>[peerEvent]);
  final received = (await store.reader.findEventById(peerEvent.eventId))!;

  final relayedRaw = relayedRecord(
    sealedRecord(
      entryType: _noteType,
      aggregateId: 'relayed-agg',
      data: const <String, Object?>{'id': 'relayed'},
    ),
  );
  await deliverTo(store, <Map<String, Object?>>[relayedRaw]);
  final relayed = (await store.reader.findEventById(
    relayedRaw['event_id']! as String,
  ))!;

  final finding = (await store.runTransaction(
    (txn, collector) => recordFindingInTxnForTest(
      store,
      txn,
      collector,
      role: FindingRole.walk,
      kind: FindingKind.forkUnrecorded,
      evidence: forkEvidence(db: store.databaseId),
      aggregates: const <String>[],
    ),
  ))!;

  final own = (await store.append(
    entryType: _noteType,
    aggregateId: 'own-agg',
    aggregateType: 'note',
    eventType: 'noted',
    data: const <String, Object?>{'id': 'own'},
    initiator: const UserInitiator('u'),
  ))!;

  return _Fixtures(
    own: own,
    received: received,
    relayed: relayed,
    finding: finding,
  );
}

/// The event ids enqueued on [destinationId]'s queue, across every item, in
/// queue order.
Future<List<String>> _enqueuedEventIds(
  StorageBackend backend,
  String destinationId,
) async => <String>[
  for (final entry in await backend.listFifoEntries(destinationId))
    ...entry.eventIds,
];

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runReplayAdmissionScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('replay admission ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open({Clock? clock}) async {
      final db = (await openDatabase())!;
      databases.add(db);
      final backend = await db.openBackend();
      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
        entryTypes: EntryTypeRegistry()
          ..register(
            const EntryTypeDefinition(
              id: _noteType,
              registeredVersion: EntryTypeVersion(1, 0),
              name: _noteType,
            ),
          ),
        source: Source(
          hopId: 'hop-${opened.length}',
          identifier: 'install-${opened.length}',
          softwareVersion: 'test@1.0.0',
        ),
        clock: clock ?? () => _fixtureAt,
      );
      opened.add(store);
      trackTestBackend(store, backend);
      return store;
    }

    tearDown(() async {
      for (final s in opened.reversed) {
        await s.close();
      }
      opened.clear();
      for (final d in databases.reversed) {
        await d.close();
      }
      databases.clear();
    });

    /// Registers [destination] on [registry], activates it at [startDate]
    /// and runs fills to completion.
    Future<void> activateAndFill(
      DestinationRegistry registry,
      StorageBackend backend,
      Destination destination, {
      required DateTime startDate,
    }) async {
      await registry.addDestination(
        destination,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await registry.setStartDate(
        destination.id,
        startDate,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      for (var i = 0; i < 20; i++) {
        final before = (await backend.listFifoEntries(destination.id)).length;
        await fillForTest(destination, backend: backend, clock: _fillNow);
        final after = (await backend.listFifoEntries(destination.id)).length;
        if (after == before) break;
      }
    }

    /// Asserts that [destinationId]'s queue carries [own]'s event and, when
    /// [expectFindingBypass] is true, [fixtures.finding]'s event, and
    /// nothing else of [fixtures].
    Future<void> expectAdmission(
      StorageBackend backend,
      String destinationId,
      _Fixtures fixtures, {
      required bool expectFindingBypass,
    }) async {
      final enqueued = await _enqueuedEventIds(backend, destinationId);
      expect(
        enqueued,
        contains(fixtures.own.eventId),
        reason: "the database's own event is enqueued",
      );
      expect(
        enqueued,
        isNot(contains(fixtures.received.eventId)),
        reason:
            'EVS-DEV-destination-drain/V: a received event is not '
            "this database's own",
      );
      expect(
        enqueued,
        isNot(contains(fixtures.relayed.eventId)),
        reason:
            'EVS-DEV-destination-drain/V: a held-but-relayed event is '
            "not this database's own",
      );
      if (expectFindingBypass) {
        expect(
          enqueued,
          contains(fixtures.finding.eventId),
          reason:
              'EVS-DEV-destination-drain/X: a security finding is '
              'enqueued on a native channel whatever the filter',
        );
      } else {
        expect(
          enqueued,
          isNot(contains(fixtures.finding.eventId)),
          reason: "a non-native destination's filter is not bypassed",
        );
      }
    }

    // Verifies: EVS-DEV-destination-drain/V
    // Verifies: EVS-DEV-destination-drain/X
    test("a first-activation historical replay admits only the database's "
        'own events, and enqueues a finding on a native channel whatever the '
        'filter', () async {
      final store = await open();
      final peer = await open();
      final backend = testBackendOf(store);
      final fixtures = await _buildFixtures(store, peer);
      final registry = DestinationRegistry(eventStore: store);

      final native = NativeDestination(
        id: 'native-ha',
        filter: _admitsOnlyUserEvents,
      );
      await activateAndFill(
        registry,
        backend,
        native,
        startDate: DateTime.utc(2020, 1, 1),
      );
      await expectAdmission(
        backend,
        native.id,
        fixtures,
        expectFindingBypass: true,
      );

      final plain = FakeDestination(
        id: 'plain-ha',
        filter: _admitsOnlyUserEvents,
      );
      await activateAndFill(
        registry,
        backend,
        plain,
        startDate: DateTime.utc(2020, 1, 1),
      );
      await expectAdmission(
        backend,
        plain.id,
        fixtures,
        expectFindingBypass: false,
      );
    });

    // Verifies: EVS-DEV-destination-drain/V
    // Verifies: EVS-DEV-destination-drain/X
    test('a gap replay from a start date moved earlier admits only the '
        "database's own events, and enqueues a finding on a native channel "
        'whatever the filter', () async {
      final store = await open();
      final peer = await open();
      final backend = testBackendOf(store);
      final fixtures = await _buildFixtures(store, peer);
      final registry = DestinationRegistry(eventStore: store);

      Future<void> runGapScenario(
        Destination destination, {
        required bool expectFindingBypass,
      }) async {
        // Activate after every fixture's timestamp: the first-activation
        // replay decides `own`, `received` and `relayed` as preceding the
        // lower bound, so none of them is enqueued yet, and the fill
        // cursor advances past them. A natively serializing destination's
        // security findings bypass the schedule window too
        // (`EVS-DEV-destination-drain/X`), so they are enqueued already,
        // before the gap that later admits `own`.
        await activateAndFill(
          registry,
          backend,
          destination,
          startDate: DateTime.utc(2029, 1, 1),
        );
        final afterActivation = await _enqueuedEventIds(
          backend,
          destination.id,
        );
        expect(
          afterActivation,
          isNot(contains(fixtures.own.eventId)),
          reason: 'own precedes the start date and is not yet enqueued',
        );
        if (expectFindingBypass) {
          expect(
            afterActivation,
            contains(fixtures.finding.eventId),
            reason:
                'EVS-DEV-destination-drain/X: a security finding bypasses '
                "a native destination's schedule window as well as its "
                'filter',
          );
        } else {
          expect(afterActivation, isEmpty);
        }

        // Moving the start date back before every fixture opens a gap
        // ending at the prior start date; the fill runs it as a gap
        // replay.
        await registry.setStartDate(
          destination.id,
          DateTime.utc(2020, 1, 1),
          initiator: const AutomationInitiator(service: 'replay-admission'),
        );
        await fillForTest(destination, backend: backend, clock: _fillNow);
      }

      final native = NativeDestination(
        id: 'native-gap',
        filter: _admitsOnlyUserEvents,
      );
      await runGapScenario(native, expectFindingBypass: true);
      await expectAdmission(
        backend,
        native.id,
        fixtures,
        expectFindingBypass: true,
      );

      final plain = FakeDestination(
        id: 'plain-gap',
        filter: _admitsOnlyUserEvents,
      );
      await runGapScenario(plain, expectFindingBypass: false);
      await expectAdmission(
        backend,
        plain.id,
        fixtures,
        expectFindingBypass: false,
      );
    });

    // Verifies: EVS-DEV-destination-drain/X
    test('a dormant schedule enqueues a security finding on a native '
        'channel; activation does not enqueue it a second time', () async {
      final store = await open();
      final peer = await open();
      final backend = testBackendOf(store);
      final fixtures = await _buildFixtures(store, peer);
      final registry = DestinationRegistry(eventStore: store);

      final native = NativeDestination(
        id: 'native-dormant',
        filter: _admitsOnlyUserEvents,
      );
      await registry.addDestination(
        native,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      // No setStartDate: the schedule is dormant (startDate null).
      await fillForTest(native, backend: backend, clock: _fillNow);

      final whileDormant = await _enqueuedEventIds(backend, native.id);
      expect(
        whileDormant,
        contains(fixtures.finding.eventId),
        reason:
            'EVS-DEV-destination-drain/X: a security finding bypasses a '
            "dormant schedule on a native destination's channel",
      );
      expect(
        whileDormant,
        isNot(contains(fixtures.own.eventId)),
        reason: 'a dormant schedule enqueues no ordinary event',
      );
      expect(
        await backend.readFillCursor(native.id),
        -1,
        reason: 'a standalone channel-wide item does not move the position',
      );

      await registry.setStartDate(
        native.id,
        DateTime.utc(2000, 1, 1),
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      for (var i = 0; i < 20; i++) {
        final before = (await backend.listFifoEntries(native.id)).length;
        await fillForTest(native, backend: backend, clock: _fillNow);
        final after = (await backend.listFifoEntries(native.id)).length;
        if (after == before) break;
      }
      final afterActivation = await _enqueuedEventIds(backend, native.id);
      expect(
        afterActivation.where((id) => id == fixtures.finding.eventId),
        hasLength(1),
        reason:
            'the finding already enqueued while dormant is not enqueued '
            'again once the schedule activates',
      );
      expect(afterActivation, contains(fixtures.own.eventId));
    });

    // Verifies: EVS-DEV-destination-drain/X
    test('a security finding past a passed end date is enqueued behind a '
        'deferred ordinary event without moving the position; extending '
        'the end date later enqueues the deferred event and does not '
        'enqueue the finding twice', () async {
      var storeNow = DateTime.utc(2026, 6, 1);
      final store = await open(clock: () => storeNow);
      final backend = testBackendOf(store);

      final own = (await store.append(
        entryType: _noteType,
        aggregateId: 'deferred-own-agg',
        aggregateType: 'note',
        eventType: 'noted',
        data: const <String, Object?>{'id': 'deferred-own'},
        initiator: const UserInitiator('u'),
      ))!;

      final finding = (await store.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          store,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: forkEvidence(db: store.databaseId),
          aggregates: const <String>[],
        ),
      ))!;

      final registry = DestinationRegistry(eventStore: store);
      final native = NativeDestination(
        id: 'native-deferred',
        filter: _admitsOnlyUserEvents,
      );
      await registry.addDestination(
        native,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await registry.setStartDate(
        native.id,
        DateTime.utc(2020, 1, 1),
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await registry.setEndDate(
        native.id,
        DateTime.utc(2026, 1, 1), // before own's client timestamp: own defers
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      storeNow = DateTime.utc(2030, 1, 1); // the fill's `now`, far ahead
      await fillForTest(native, backend: backend, clock: () => storeNow);

      final afterFirstPass = await _enqueuedEventIds(backend, native.id);
      expect(
        afterFirstPass,
        <String>[finding.eventId],
        reason:
            'EVS-DEV-destination-drain/X: the finding is enqueued past a '
            "passed end date, but own's deferral holds the position back",
      );
      expect(
        await backend.readFillCursor(native.id),
        lessThan(own.sequenceNumber),
        reason:
            'the deferred ordinary event keeps the position from '
            'moving past the finding it stands behind',
      );

      await registry.setEndDate(
        native.id,
        DateTime.utc(2027, 1, 1), // now past own's client timestamp
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await fillForTest(native, backend: backend, clock: () => storeNow);

      final afterExtension = await _enqueuedEventIds(backend, native.id);
      expect(
        afterExtension,
        containsAll(<String>[finding.eventId, own.eventId]),
      );
      expect(
        afterExtension.where((id) => id == finding.eventId),
        hasLength(1),
        reason: 'extending the end date does not enqueue the finding twice',
      );
    });

    // Verifies: EVS-DEV-destination-drain/X
    test('a security finding retired by an operator recovery is enqueued '
        'again on the next fill', () async {
      final store = await open();
      final backend = testBackendOf(store);
      final finding = (await store.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          store,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: forkEvidence(db: store.databaseId),
          aggregates: const <String>[],
        ),
      ))!;
      final registry = DestinationRegistry(eventStore: store);
      final native = NativeDestination(id: 'native-recovery');
      await registry.addDestination(
        native,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      // No setStartDate: the schedule is dormant, so only the finding's
      // channel-wide bypass enqueues anything.
      await fillForTest(native, backend: backend, clock: _fillNow);

      final firstPass = await _enqueuedEventIds(backend, native.id);
      expect(firstPass, <String>[finding.eventId]);

      final rowId = (await store.reader.readFifoHead(native.id))!.entryId;
      await wedgeHeadForTest(registry, native.id);
      await registry.tombstoneAndRefill(
        native.id,
        rowId,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );

      await fillForTest(native, backend: backend, clock: _fillNow);

      final afterRecovery = await _enqueuedEventIds(backend, native.id);
      expect(
        afterRecovery.where((id) => id == finding.eventId),
        hasLength(2),
        reason:
            'EVS-DEV-destination-drain/X: an operator recovery retires the '
            'item that carried the finding (queuedChannelWideEventIds '
            'counts nothing for a tombstoned entry), so the next fill '
            'enqueues the finding again',
      );
      final entries = await backend.listFifoEntries(native.id);
      final statuses = <FinalStatus?>[
        for (final e in entries)
          if (e.eventIds.contains(finding.eventId)) e.finalStatus,
      ];
      expect(
        statuses,
        containsAll(<FinalStatus?>[FinalStatus.tombstoned, null]),
        reason:
            'the retired item stays tombstoned; the newly enqueued one is '
            'pending',
      );
    });

    // Verifies: EVS-DEV-destination-drain/X
    test('a security finding sent under a generation a channel regression '
        'retires is enqueued again once the channel starts a new '
        'generation', () async {
      final store = await open();
      final backend = testBackendOf(store);
      final finding = (await store.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          store,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: forkEvidence(db: store.databaseId),
          aggregates: const <String>[],
        ),
      ))!;
      final registry = DestinationRegistry(eventStore: store);
      final native = NativeDestination(id: 'native-generation');
      await registry.addDestination(
        native,
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await registry.setStartDate(
        native.id,
        DateTime.utc(2020, 1, 1),
        initiator: const AutomationInitiator(service: 'replay-admission'),
      );
      await fillForTest(
        native,
        backend: backend,
        source: testFillSource,
        clock: _fillNow,
      );
      await drainForTest(native, registry: registry, clock: _fillNow);

      final firstPass = await _enqueuedEventIds(backend, native.id);
      expect(firstPass, <String>[finding.eventId]);
      final sentEntry = (await backend.listFifoEntries(native.id)).single;
      expect(sentEntry.finalStatus, FinalStatus.sent);
      expect(sentEntry.deliveryGeneration, 1);

      final gen = (await store.append(
        entryType: _noteType,
        aggregateId: 'gen-agg',
        aggregateType: 'note',
        eventType: 'noted',
        data: const <String, Object?>{'id': 'gen'},
        initiator: const UserInitiator('u'),
      ))!;

      final schedule = await backend.transaction(
        (txn) => backend.readScheduleTxn(txn, native.id),
      );
      final channel = DeliveryChannel(
        senderDatabaseId: store.databaseId,
        destinationId: native.id,
        registrationId: schedule!.registrationId!,
        generation: 1,
      );
      final ahead = DeliveryRecord(deliveryNumber: 5, deliveryHash: 'a' * 64);
      native.enqueueScript(
        SendAnswered(
          ReceiverRefusal(
            channel: channel,
            receiverDatabaseId: native.receiverDatabaseId,
            record: ahead,
            refusal: RefusalKind.outOfSequence,
          ),
        ),
      );
      await fillForTest(
        native,
        backend: backend,
        source: testFillSource,
        clock: _fillNow,
      );
      await drainForTest(native, registry: registry, clock: _fillNow);

      final senderRecord = await backend.transaction(
        (txn) => backend.readSenderChannelRecordTxn(txn, native.id),
      );
      expect(
        senderRecord!.generation,
        2,
        reason: 'the out-of-sequence record ahead starts a new generation',
      );

      await fillForTest(
        native,
        backend: backend,
        source: testFillSource,
        clock: _fillNow,
      );

      final afterNewGeneration = await _enqueuedEventIds(backend, native.id);
      expect(
        afterNewGeneration.where((id) => id == finding.eventId),
        hasLength(2),
        reason:
            'EVS-DEV-destination-drain/X: the finding was sent under '
            'generation 1 (queuedChannelWideEventIds counts a sent entry '
            "only under the channel's current generation), so once the "
            'channel starts generation 2 the finding is enqueued again',
      );
      expect(afterNewGeneration, contains(gen.eventId));
    });

    // A gap replay's own channel-wide bypass (the branch in
    // buildGapReplayRows that admits a channel-wide own event at or below
    // the fill cursor, independent of alreadyQueuedChannelWide) can be
    // driven through the full registry only when the event is already at
    // or below the fill cursor and not counted as queued. Every operation
    // that stops counting an item as queued (an operator recovery, a new
    // channel generation) also rewinds the fill cursor to at or below that
    // same event, so the very next fill always re-admits it through the
    // ordinary walk (`walkAdmission`), not through this function — the two
    // tests above cover that path end to end. This gap replay's own branch
    // is exercised directly instead, against the contract its doc comment
    // states.
    // Verifies: EVS-DEV-destination-drain/X
    test('buildGapReplayRows admits an own channel-wide event at or below '
        'the fill cursor whatever the filter and the gap window, once it '
        'is not already queued', () async {
      final store = await open();
      final finding = (await store.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          store,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: forkEvidence(db: store.databaseId),
          aggregates: const <String>[],
        ),
      ))!;

      final native = NativeDestination(
        id: 'native-gap-direct',
        filter: _admitsOnlyUserEvents,
      );
      final channel = DeliveryChannel(
        senderDatabaseId: store.databaseId,
        destinationId: native.id,
        registrationId: 'reg-1',
        generation: 1,
      );
      // A window that excludes the finding's client timestamp entirely:
      // only the channel-wide bypass, not the ordinary
      // filter-and-window check, can admit it here.
      final window = (
        startDate: DateTime.utc(2050, 1, 1),
        gapUpper: DateTime.utc(2050, 1, 2),
      );

      final admitted = await buildGapReplayRows(
        native,
        <StoredEvent>[finding],
        startDate: window.startDate,
        gapUpper: window.gapUpper,
        fillCursor: finding.sequenceNumber,
        now: _fillNow(),
        source: testFillSource,
        databaseId: store.databaseId,
        alreadyQueuedChannelWide: const <String>{},
        channel: channel,
      );
      expect(
        admitted.map((i) => i.batch.map((e) => e.eventId)).expand((e) => e),
        <String>[finding.eventId],
        reason:
            "EVS-DEV-destination-drain/X: the gap replay's own "
            'channel-wide bypass admits the finding whatever the '
            "destination's filter and whatever the gap window, once it is "
            'not already queued',
      );

      final deduped = await buildGapReplayRows(
        native,
        <StoredEvent>[finding],
        startDate: window.startDate,
        gapUpper: window.gapUpper,
        fillCursor: finding.sequenceNumber,
        now: _fillNow(),
        source: testFillSource,
        databaseId: store.databaseId,
        alreadyQueuedChannelWide: <String>{finding.eventId},
        channel: channel,
      );
      expect(
        deduped,
        isEmpty,
        reason:
            'already covered by the currently queued items: not built '
            'again',
      );
    });
  });
}
