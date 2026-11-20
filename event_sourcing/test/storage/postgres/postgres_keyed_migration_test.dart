// A database migrated in place from schema 9 to 10 with a populated queue,
// then served by a keyed destination: the shape of a deployment that adopts
// keyed holds over a queue an older build left wedged. Gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:flutter_test/flutter_test.dart';

import '../../test_support/declarations.dart';
import '../../test_support/fake_destination.dart';
import '../../test_support/queue_test_support.dart'
    show drainForTest, fillForTest;
import '../../test_support/test_backends.dart';
import 'test_postgres_url.dart';

const Initiator _init = AutomationInitiator(service: 'keyed-migration');
const String _noteType = 'keyed_migration_note';
const Source _source = Source(
  hopId: 'portal-server',
  identifier: 'keyed-migration',
  softwareVersion: 'test@1.0.0',
);

const SendPermanent _keyRefusal = SendPermanent(
  error: 'refused for this subject',
  scope: WedgeScope.key,
);

String? _subject(StoredEvent e) => e.data['subject'] as String?;

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  tearDownAll(db.drop);

  final opened = <(EventStore, PostgresBackend)>[];
  var eventTime = DateTime.utc(2026, 3, 1);

  Future<(EventStore, DestinationRegistry)> openStore() async {
    final backend = await db.open();
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
        declarations: <EventTypeDeclaration>[EligibleVersion('finalized')],
      ),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: PostgresSecurityContextStore(backend: backend),
      clock: () => eventTime,
    );
    trackTestBackend(store, backend);
    opened.add((store, backend));
    return (store, DestinationRegistry(eventStore: store));
  }

  Future<void> closeAll() async {
    for (final (store, backend) in opened) {
      await store.close();
      await backend.close();
    }
    opened.clear();
  }

  tearDown(closeAll);

  Future<StoredEvent> note(EventStore store, String id, String subject) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    return (await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id, 'subject': subject},
      initiator: _init,
    ))!;
  }

  Future<void> fill(DestinationRegistry r, Destination d) async {
    for (var i = 0; i < 10; i++) {
      await fillForTest(
        d,
        backend: testBackendOf(r.eventStore),
        clock: () => DateTime.utc(2027),
        registry: r,
      );
    }
  }

  Future<void> drain(DestinationRegistry r, Destination d) =>
      drainForTest(d, registry: r, clock: () => DateTime.utc(2100));

  /// Puts the provisioned schema back to version 9 with its queue rows in
  /// place: the two columns version 10 adds are dropped, every schedule's
  /// recorded keying is removed (a build of schema 9 records none), and the
  /// stored version pair is 9's.
  Future<void> rewindToSchema9() async {
    final admin = await db.connectAdmin();
    try {
      await admin.execute(
        'ALTER TABLE fifo_entries DROP COLUMN block_key, '
        'DROP COLUMN released_hold_id',
      );
      await admin.execute(
        "UPDATE backend_state SET value = value - 'keying' "
        "WHERE left(key, 9) = 'schedule_'",
      );
      await admin.execute(
        "UPDATE backend_state SET value = '9'::jsonb "
        "WHERE key = 'schema_version'",
      );
      await admin.execute(
        "UPDATE backend_state SET value = '8'::jsonb "
        "WHERE key = 'min_compatible_schema_version'",
      );
    } finally {
      await admin.close();
    }
  }

  Future<(int, int)> storedPair() async {
    final admin = await db.connectAdmin();
    try {
      final rows = await admin.execute(
        'SELECT key, value::numeric::int FROM backend_state '
        "WHERE key IN ('schema_version', 'min_compatible_schema_version')",
      );
      final pair = <String, int>{
        for (final r in rows) r[0]! as String: r[1]! as int,
      };
      return (pair['schema_version']!, pair['min_compatible_schema_version']!);
    } finally {
      await admin.close();
    }
  }

  // Verifies: EVS-DEV-destination-key-hold-guards/I
  // Verifies: EVS-DEV-destination-key-hold/B+H+I+X
  // Verifies: EVS-PRD-destination-key-hold/A+B+D
  test('a populated schema-9 queue migrated to 10 wedges destination-wide '
      'while its keyless items are pending, and holds one key once a '
      'recovery rebuilds them with keys', () async {
    await db.reset(provision: true);

    // An older build's queues: 'qa' wedged at its head with items pending
    // behind it, 'live' with items pending only. Neither is keyed.
    var (store, registry) = await openStore();
    final qaOld = FakeDestination(
      id: 'qa',
      script: <SendResult>[const SendPermanent(error: 'folder missing')],
    );
    final liveOld = FakeDestination(id: 'live');
    for (final d in <Destination>[qaOld, liveOld]) {
      await registry.addDestination(d, initiator: _init);
      await registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      await fill(registry, d);
    }
    final p1 = await note(store, 'p1', 'P');
    final q1 = await note(store, 'q1', 'Q');
    final p2 = await note(store, 'p2', 'P');
    final q2 = await note(store, 'q2', 'Q');
    await fill(registry, qaOld);
    await fill(registry, liveOld);
    await drain(registry, qaOld);
    final backend = testBackendOf(store);
    final qaBefore = await backend.listFifoEntries('qa');
    final liveBefore = await backend.listFifoEntries('live');
    expect(qaBefore.first.finalStatus, FinalStatus.wedged);
    expect(qaBefore.skip(1).map((i) => i.finalStatus), <FinalStatus?>[
      null,
      null,
      null,
    ]);
    expect(liveBefore, hasLength(4));
    await closeAll();

    await rewindToSchema9();
    expect(await storedPair(), (9, 8));

    // Provisioning the new build migrates the populated schema.
    await db.provision();
    expect(await storedPair(), (
      postgresSchemaVersion,
      postgresMinCompatibleSchemaVersion,
    ));
    (store, registry) = await openStore();
    final migrated = testBackendOf(store);
    final qaRows = await migrated.listFifoEntries('qa');
    expect(qaRows.map((i) => i.entryId), qaBefore.map((i) => i.entryId));
    expect(qaRows.map((i) => i.blockKey), everyElement(isNull));
    expect(qaRows.map((i) => i.releasedHoldId), everyElement(isNull));
    expect((await migrated.readSchedule('qa'))!.keying, isNull);

    // The new build registers both keyed: a schedule that records no
    // keying takes the registration's.
    const keying = DestinationKeying(keyed: true, keyScheme: 'participant-v1');
    final qa = FakeDestination(
      id: 'qa',
      keyOf: _subject,
      keyScheme: 'participant-v1',
      script: <SendResult>[
        _keyRefusal,
        for (var i = 0; i < 10; i++) const SendOk(),
      ],
    );
    final live = FakeDestination(
      id: 'live',
      keyOf: _subject,
      keyScheme: 'participant-v1',
      script: <SendResult>[
        _keyRefusal,
        _keyRefusal,
        for (var i = 0; i < 10; i++) const SendOk(),
      ],
    );
    await registry.addDestination(qa, initiator: _init);
    await registry.addDestination(live, initiator: _init);
    expect((await migrated.readSchedule('qa'))!.keying, keying);
    expect((await migrated.readSchedule('live'))!.keying, keying);

    // 'live': a key-scoped refusal while its keyless items are pending
    // wedges the whole destination.
    final p3 = await note(store, 'p3', 'P');
    await fill(registry, live);
    final liveMixed = await migrated.listFifoEntries('live');
    expect(liveMixed.map((i) => i.blockKey), <String?>[
      null,
      null,
      null,
      null,
      'P',
    ]);
    await drain(registry, live);
    final liveWedged = (await migrated.listFifoEntries('live')).first;
    expect(liveWedged.finalStatus, FinalStatus.wedged);
    expect(liveWedged.attempts.single.scope, WedgeScope.key);
    expect(
      await migrated.findAllEvents(entryType: kDestinationHeldEntryType),
      isEmpty,
    );

    // One recovery of each rebuilds the swept items with keys.
    for (final (d, head) in <(Destination, FifoEntry)>[
      (qa, qaRows.first),
      (live, liveWedged),
    ]) {
      await registry.tombstoneAndRefill(d.id, head.entryId, initiator: _init);
      await fill(registry, d);
      final rebuilt = [
        for (final i in await migrated.listFifoEntries(d.id))
          if (i.finalStatus == null) i,
      ];
      expect(rebuilt, isNotEmpty);
      expect(rebuilt.map((i) => i.blockKey), everyElement(isNotNull));
    }

    // The next refusal holds only its key; the other key delivers.
    for (final d in <Destination>[qa, live]) {
      await drain(registry, d);
      final lists = await migrated.transaction(
        (txn) => migrated.readKeyListsTxn(txn, d.id),
      );
      expect(lists.keys, <String>['P'], reason: d.id);
      expect(
        await migrated.transaction(
          (txn) => migrated.readWedgeRecordTxn(txn, d.id),
        ),
        isNull,
        reason: d.id,
      );
      final sent = <String>[
        for (final i in await migrated.listFifoEntries(d.id))
          if (i.finalStatus == FinalStatus.sent) ...i.eventIds,
      ];
      expect(sent, <String>[q1.eventId, q2.eventId], reason: d.id);
      expect(lists['P']!.events.map((e) => e.eventId), <String>[
        p1.eventId,
        p2.eventId,
        p3.eventId,
      ], reason: d.id);
    }
  });
}
