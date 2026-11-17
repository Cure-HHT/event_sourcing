// Backend-agnostic scenarios for the default destination-holds view: its
// rows and key, the fold of the hold, release and clearing events, the
// deletion that drops a destination's rows, and its registration at open.
// Sembast runs them from test/destinations/destination_holds_view_test.dart
// and Postgres from
// test/storage/postgres/postgres_destination_holds_view_test.dart.
//
// This file exposes [runDestinationHoldsViewScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'declarations.dart';
import 'deliveries.dart' show ingestEventForTest;
import 'destination_wedges_view_conformance.dart' show forgedEvent;
import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show drainForTest, fillForTest;
import 'test_backends.dart';

const Initiator _init = AutomationInitiator(service: 'holds-view-scenarios');
const String _noteType = 'holds_view_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'holds-view-install',
  softwareVersion: 'test@1.0.0',
);

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: _noteType,
  registeredVersion: EntryTypeVersion(1, 0),
  name: _noteType,
  declarations: <EventTypeDeclaration>[EligibleVersion('finalized')],
);

/// A key-scoped permanent failure.
const SendPermanent _keyRefusal = SendPermanent(
  error: 'refused for this key',
  scope: WedgeScope.key,
);

/// A policy with no backoff and no jitter.
SyncPolicy _policy() => const SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: 1000000,
  maxRetryTime: Duration(hours: 24),
);

/// The block key of a note: its `key` data field.
String? _noteKey(StoredEvent e) => e.data['key'] as String?;

/// Every row of the default destination-holds view, keyed by row key.
Future<Map<String, Map<String, Object?>>> holdsViewRows(
  EventStore store,
) async => <String, Map<String, Object?>>{
  for (final row in (await store.reader.findViewRows(
    defaultDestinationHoldsSpec.viewName,
  )).rows)
    row['aggregateId']! as String: Map<String, Object?>.of(row),
};

/// Asserts a rebuild of the default destination-holds view from the whole
/// log derives the rows [store]'s view holds.
Future<void> expectHoldsViewReplays(EventStore store) async {
  final before = await holdsViewRows(store);
  await rebuildView(
    store: store,
    viewName: defaultDestinationHoldsSpec.viewName,
    deadline: DateTime.now().toUtc().add(const Duration(seconds: 20)),
  );
  expect(
    await holdsViewRows(store),
    before,
    reason: 'a replay of the whole log derives the same rows',
  );
}

class _World {
  _World(this.db);
  final QueueTestDatabase db;
  DateTime eventTime = DateTime.utc(2026, 3, 1);
  late StorageBackend backend;
  late EventStore store;
  late DestinationRegistry registry;

  Future<EventStore> open({
    EntryTypeRegistry? entryTypes,
    ProjectionRegistry? projections,
  }) async {
    final b = await db.openBackend();
    final s = await EventStore.openForTest(
      storage: b,
      entryTypes: entryTypes ?? (EntryTypeRegistry()..register(_noteDef)),
      projections: projections,
      source: _source,
      securityContexts: db.securityFor(b),
      clock: () => eventTime,
    );
    trackTestBackend(s, b);
    return s;
  }

  Future<void> start() async {
    store = await open();
    backend = testBackendOf(store);
    registry = DestinationRegistry(eventStore: store);
  }

  Future<StoredEvent> note(String id, {String? key}) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    return (await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id, 'key': ?key},
      initiator: _init,
    ))!;
  }

  Future<void> activate(Destination d) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
    await fill(d);
  }

  Future<void> fill(Destination d) => fillForTest(
    d,
    backend: backend,
    clock: () => DateTime.utc(2027, 1, 1),
    policy: _policy(),
    cadence: const Duration(seconds: 15),
    registry: registry,
  );

  Future<void> fillAll(Destination d) async {
    for (var i = 0; i < 10; i++) {
      await fill(d);
    }
  }

  Future<void> drain(Destination d) => drainForTest(
    d,
    registry: registry,
    clock: () => DateTime.utc(2100),
    policy: _policy(),
  );

  Future<List<StoredEvent>> events(String entryType) =>
      backend.findAllEvents(entryType: entryType);

  /// Queues every event the caller appended for [d] and drains once, which
  /// holds the head's key; returns the hold event.
  Future<StoredEvent> hold(Destination d) async {
    await fillAll(d);
    await drain(d);
    return (await events(kDestinationHeldEntryType)).last;
  }

  /// The row key of [key] of destination [id] of this store's database.
  String rowKey(String id, String key) => '${store.databaseId}|$id|$key';
}

/// A hold, release or clearing event of a peer database, as a peer sends
/// it.
StoredEvent _peerAudit(
  String entryType,
  String eventType,
  Map<String, Object?> data,
) => forgedEvent(
  entryType: entryType,
  aggregateType: kDestinationAuditAggregateType,
  eventType: eventType,
  data: <String, Object?>{'id': 'p', 'database_id': 'peer-db', ...data},
);

StoredEvent _peerHeld(String key, String holdId) =>
    _peerAudit(kDestinationHeldEntryType, kDestinationHeldEventType, {
      'key': key,
      'hold_id': holdId,
      'row_id': 'peer-row',
      'failed_event_id': null,
      'event_ids': <String>['e1', 'e2', 'e3'],
      'cause': 'permanent_refusal',
      'attempt_count': 1,
    });

StoredEvent _peerReleased(String key, String holdId) => _peerAudit(
  kDestinationHoldReleasedEntryType,
  kDestinationHoldReleasedEventType,
  {'key': key, 'hold_id': holdId, 'request_id': 'r'},
);

StoredEvent _peerCleared(String key, String holdId) => _peerAudit(
  kDestinationHoldClearedEntryType,
  kDestinationHoldClearedEventType,
  {'key': key, 'hold_id': holdId},
);

/// Run every destination-holds view scenario against a database
/// [databaseFactory] builds fresh for each test (a null database skips the
/// test).
void runDestinationHoldsViewScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('destination-holds view scenarios ($label)', () {
    late _World w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = _World(db);
      await w.start();
    });

    tearDown(() async {
      if (!available) return;
      await w.db.close();
    });

    // Verifies: EVS-PRD-destination-key-hold/K
    // a key is held from the hold event that names it, releasing from the
    //   release event that names that hold and no longer held from the
    //   clearing event that names that hold; a replay of the log derives
    //   the same rows at every step.
    // Verifies: EVS-DEV-destination-drain/M
    // the view keys its rows by the database identity, the destination and
    //   the block key.
    test('holds view folds held → releasing → removed', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: _noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activate(d);
      await w.note('a1', key: 'A');
      await w.note('a2', key: 'A');
      final held = await w.hold(d);
      final holdId = held.data['hold_id']! as String;

      var rows = await holdsViewRows(w.store);
      expect(rows.keys, <String>[w.rowKey('keyed', 'A')]);
      final row = rows[w.rowKey('keyed', 'A')]!;
      expect(row['state'], 'held');
      expect(row['id'], 'keyed');
      expect(row['database_id'], w.store.databaseId);
      expect(row['key'], 'A');
      expect(row['hold_id'], holdId);
      expect(row['cause'], 'permanent_refusal');
      expect(row['row_id'], held.data['row_id']);
      expect(row['failed_event_id'], isNull);
      expect(row['attempt_count'], held.data['attempt_count']);
      expect(row['event_count'], 2);
      expect(row['held_at'], held.clientTimestamp.toUtc().toIso8601String());
      await expectHoldsViewReplays(w.store);

      await w.registry.releaseHold(
        'keyed',
        'A',
        holdId: holdId,
        initiator: _init,
        requestId: 'request-1',
      );
      rows = await holdsViewRows(w.store);
      expect(rows.keys, <String>[w.rowKey('keyed', 'A')]);
      final releasing = rows[w.rowKey('keyed', 'A')]!;
      expect(releasing['state'], 'releasing');
      expect(releasing['hold_id'], holdId);
      expect(releasing['event_count'], 2);
      expect(releasing['held_at'], row['held_at']);
      await expectHoldsViewReplays(w.store);

      await w.fillAll(d);
      expect(await w.events(kDestinationHoldClearedEntryType), hasLength(1));
      expect(await holdsViewRows(w.store), isEmpty);
      await expectHoldsViewReplays(w.store);
    });

    // Verifies: EVS-PRD-destination-key-hold/K
    // a hold event for a key that has a row, a re-hold during its release,
    //   replaces the row: the new hold id, in state held.
    test('a re-hold replaces the row with the new hold', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: _noteKey,
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      await w.activate(d);
      await w.note('a1', key: 'A');
      await w.note('a2', key: 'A');
      final first = await w.hold(d);
      await w.registry.releaseHold(
        'keyed',
        'A',
        holdId: first.data['hold_id']! as String,
        initiator: _init,
        requestId: 'request-1',
      );
      await w.fill(d);
      await w.drain(d);
      final again = (await w.events(kDestinationHeldEntryType)).last;
      expect(again.data['hold_id'], isNot(first.data['hold_id']));
      final rows = await holdsViewRows(w.store);
      expect(rows.keys, <String>[w.rowKey('keyed', 'A')]);
      expect(rows.values.single['hold_id'], again.data['hold_id']);
      expect(rows.values.single['state'], 'held');
      expect(rows.values.single['row_id'], again.data['row_id']);
      await expectHoldsViewReplays(w.store);
    });

    // Verifies: EVS-PRD-destination-key-hold/K
    // a peer's hold, release and clearing events fold under the peer's
    //   database identity; a release or a clearing that names another hold
    //   of the key leaves the row as it is.
    test('a release or clearing naming another hold leaves the row', () async {
      if (!available) return;
      Future<void> ingest(StoredEvent e) async {
        final outcome = await ingestEventForTest(w.store, e);
        expect(outcome.outcome, IngestOutcome.ingested, reason: e.eventType);
      }

      await ingest(_peerHeld('K', 'h1'));
      const key = 'peer-db|p|K';
      Future<Map<String, Object?>?> row() async =>
          (await holdsViewRows(w.store))[key];
      expect((await row())!['state'], 'held');
      expect((await row())!['event_count'], 3);

      await ingest(_peerReleased('K', 'h0'));
      expect((await row())!['state'], 'held');
      await ingest(_peerCleared('K', 'h0'));
      expect((await row())!['hold_id'], 'h1');

      await ingest(_peerReleased('K', 'h1'));
      expect((await row())!['state'], 'releasing');
      await ingest(_peerCleared('K', 'h1'));
      expect(await row(), isNull);

      // A release or clearing of a key with no row creates none.
      await ingest(_peerReleased('L', 'h2'));
      await ingest(_peerCleared('L', 'h2'));
      expect(await holdsViewRows(w.store), isEmpty);
      await expectHoldsViewReplays(w.store);
    });

    // Verifies: EVS-DEV-destination-drain/A
    // a deletion names the destination's key lists in its deletion event,
    //   and the view drops that destination's rows on it, leaving every
    //   other destination's rows.
    test("a deletion drops the destination's rows", () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: _noteKey,
        allowHardDelete: true,
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      final other = FakeDestination(
        id: 'other',
        keyOf: _noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activate(d);
      await w.activate(other);
      await w.note('a1', key: 'A');
      await w.note('b1', key: 'B');
      await w.hold(d);
      await w.drain(d);
      StoredEvent holdOf(List<StoredEvent> held, String key) => held
          .singleWhere((e) => e.data['id'] == 'keyed' && e.data['key'] == key);
      final held = await w.events(kDestinationHeldEntryType);
      final holdA = holdOf(held, 'A');
      final holdB = holdOf(held, 'B');
      await w.hold(other);
      expect((await holdsViewRows(w.store)).keys.toSet(), <String>{
        w.rowKey('keyed', 'A'),
        w.rowKey('keyed', 'B'),
        w.rowKey('other', 'A'),
      });

      await w.registry.deleteDestination('keyed', initiator: _init);

      final deleted = (await w.events(kDestinationDeletedEntryType)).single;
      expect(deleted.data['key_lists'], <Object?>[
        <String, Object?>{'key': 'A', 'hold_id': holdA.data['hold_id']},
        <String, Object?>{'key': 'B', 'hold_id': holdB.data['hold_id']},
      ]);
      expect((await holdsViewRows(w.store)).keys, <String>[
        w.rowKey('other', 'A'),
      ]);
      await expectHoldsViewReplays(w.store);
    });

    group('registration at open', () {
      // Verifies: EVS-DEV-destination-drain/M
      // an open registers the default destination-holds view beside the
      //   destination-wedges view, and creates its copy.
      test('an open registers the view', () async {
        if (!available) return;
        expect(
          identical(
            w.store.projections.lookup(defaultDestinationHoldsSpec.viewName),
            defaultDestinationHoldsSpec,
          ),
          isTrue,
        );
        expect(
          () => w.store.copyIdOf(defaultDestinationHoldsSpec.viewName),
          returnsNormally,
        );
        final projections = ProjectionRegistry()
          ..register(defaultDestinationWedgesSpec)
          ..register(defaultDestinationHoldsSpec);
        await w.open(projections: projections);
        await w.open(projections: projections);
      });

      // Verifies: EVS-DEV-destination-drain/M
      // a sealed projection registry that lacks the holds view is refused
      //   naming the view, before anything is written.
      test('a sealed registry without the view is refused', () async {
        if (!available) return;
        final before = await w.backend.findAllEvents();
        final projections = ProjectionRegistry()
          ..register(defaultDestinationWedgesSpec)
          ..seal();
        await expectLater(
          w.open(projections: projections),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(
                contains('sealed'),
                contains(defaultDestinationHoldsSpec.viewName),
              ),
            ),
          ),
        );
        expect(projections.all(), hasLength(1));
        expect(
          [for (final e in await w.backend.findAllEvents()) e.eventId],
          [for (final e in before) e.eventId],
        );
      });

      // Verifies: EVS-DEV-destination-drain/M
      // a consumer spec registered under the holds view's name is refused
      //   with the reserved-view message, before anything is written.
      test('another spec under the view name is refused', () async {
        if (!available) return;
        final before = await w.backend.findAllEvents();
        final projections = ProjectionRegistry()
          ..register(
            TableProjectionSpec(
              viewName: defaultDestinationHoldsSpec.viewName,
              interest: const SubscriptionFilter(entryTypes: {_noteType}),
              insertEventTypes: const {'finalized'},
              removeEventTypes: const <String>{},
              rowKey: const AggregateIdKey(),
              rowData: const WholePayload(),
            ),
          );
        await expectLater(
          w.open(projections: projections),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(
                contains('reserved'),
                contains(defaultDestinationHoldsSpec.viewName),
              ),
            ),
          ),
        );
        expect(
          [for (final e in await w.backend.findAllEvents()) e.eventId],
          [for (final e in before) e.eventId],
        );
      });
    });
  });
}
