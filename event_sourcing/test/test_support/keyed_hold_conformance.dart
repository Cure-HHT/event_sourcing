// Backend-agnostic scenarios for a keyed destination's queue: the block key
// the fill and every replay evaluate and store on each queue item, the
// routing of a listed key's events into its key list, and the failure path
// of a block-key evaluation that throws. Sembast runs them from
// test/sync/keyed_hold_sembast_test.dart and Postgres from
// test/storage/postgres/postgres_keyed_hold_test.dart.
//
// This file exposes one function, [runKeyedHoldScenarios], and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'declarations.dart';
import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show fillForTest;
import 'test_backends.dart';

const Initiator _init = AutomationInitiator(service: 'keyed-hold-scenarios');
const String _noteType = 'keyed_hold_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'keyed-hold-install',
  softwareVersion: 'test@1.0.0',
);

/// The clock every fill of these scenarios runs at: after every event.
DateTime _fillTime() => DateTime.utc(2027, 1, 1);

/// A policy with no backoff and no jitter, spent after [maxAttempts]
/// failures.
SyncPolicy _policy({int maxAttempts = 1000000}) => SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: maxAttempts,
  maxRetryTime: const Duration(hours: 24),
);

/// The block key of a note: its `key` data field.
String? noteKey(StoredEvent e) => e.data['key'] as String?;

/// A held key list holding [events], under [holdId].
KeyList heldList(List<StoredEvent> events, {String holdId = 'hold-1'}) =>
    KeyList(
      holdId: holdId,
      state: KeyListState.held,
      cause: 'permanent_refusal',
      events: <KeyListEvent>[for (final e in events) listEventOf(e)],
    );

/// The key list entry of [e].
KeyListEvent listEventOf(StoredEvent e) =>
    KeyListEvent(eventId: e.eventId, sequenceNumber: e.sequenceNumber);

class _Process {
  _Process(this.backend, this.store, this.registry) {
    trackTestBackend(store, backend);
  }
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

class _World {
  _World(this.db);
  final QueueTestDatabase db;
  DateTime eventTime = DateTime.utc(2026, 3, 1);
  late _Process a;

  StorageBackend get backend => a.backend;
  DestinationRegistry get registry => a.registry;

  Future<_Process> openProcess() async {
    final backend = await db.openBackend();
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
      securityContexts: db.securityFor(backend),
      clock: () => eventTime,
    );
    return _Process(backend, store, DestinationRegistry(eventStore: store));
  }

  /// Appends a note whose block key (under [noteKey]) is [key].
  Future<StoredEvent> note(String id, {String? key}) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    final event = await a.store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id, 'key': ?key},
      initiator: _init,
    );
    return event!;
  }

  Future<void> activate(Destination d, {DateTime? startDate}) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      startDate ?? DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  }

  /// Activates [d] and resolves its first-activation replay request
  /// against a log holding no event this destination admits, so later
  /// fills run through the live fill.
  Future<void> activateResolved(Destination d) async {
    await activate(d);
    await fill(d);
  }

  Future<void> fill(
    Destination d, {
    SyncPolicy? policy,
    DateTime Function()? clock,
  }) => fillForTest(
    d,
    backend: backend,
    clock: clock ?? _fillTime,
    policy: policy ?? _policy(),
    cadence: const Duration(seconds: 15),
  );

  Future<List<FifoEntry>> items(String id) => backend.listFifoEntries(id);

  Future<Map<String, KeyList>> keyLists(String id) =>
      backend.transaction((txn) => backend.readKeyListsTxn(txn, id));

  Future<void> writeKeyLists(String id, Map<String, KeyList> lists) =>
      backend.transaction((txn) => backend.writeKeyListsTxn(txn, id, lists));

  Future<TransformFailureRecord?> failureRecord(String id) => backend
      .transaction((txn) => backend.readTransformFailureRecordTxn(txn, id));

  Future<ReplayRequest?> replayRequest(String id) =>
      backend.transaction((txn) => backend.readReplayRequestTxn(txn, id));
}

/// Run every keyed-hold scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
void runKeyedHoldScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('keyed hold scenarios ($label)', () {
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
      w.a = await w.openProcess();
    });

    tearDown(() async {
      if (!available) return;
      await w.db.close();
    });

    // Verifies: EVS-DEV-destination-key-hold/A+B
    test("fill stores each item's block key", () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'B');
      final e3 = await w.note('n3');
      for (var i = 0; i < 3; i++) {
        await w.fill(d);
      }
      final items = await w.items(d.id);
      expect(items.map((i) => i.eventIds), <List<String>>[
        <String>[e1.eventId],
        <String>[e2.eventId],
        <String>[e3.eventId],
      ]);
      expect(items.map((i) => i.blockKey), <String?>['A', 'B', null]);
    });

    // Verifies: EVS-DEV-destination-key-hold/D
    test('a destination that is not keyed stores no block key', () async {
      if (!available) return;
      final d = FakeDestination(id: 'unkeyed');
      await w.activateResolved(d);
      await w.note('n1', key: 'A');
      await w.fill(d);
      final items = await w.items(d.id);
      expect(items, hasLength(1));
      expect(items.single.blockKey, isNull);
      expect(d.blockKeyCalls, 0, reason: 'no key is evaluated');
    });

    // Verifies: EVS-DEV-destination-key-hold/C
    test('a batch never mixes keys', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey, batchCapacity: 3);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'B');
      final e3 = await w.note('n3', key: 'A');
      await w.fill(d);
      var items = await w.items(d.id);
      expect(items.single.eventIds, <String>[e1.eventId]);
      expect(items.single.blockKey, 'A');
      await w.fill(d);
      await w.fill(d);
      items = await w.items(d.id);
      expect(items.map((i) => i.eventIds), <List<String>>[
        <String>[e1.eventId],
        <String>[e2.eventId],
        <String>[e3.eventId],
      ]);
    });

    // Verifies: EVS-DEV-destination-key-hold/C
    test('events of one key batch together, keyless ones too', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey, batchCapacity: 3);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'A');
      final e3 = await w.note('n3');
      final e4 = await w.note('n4');
      await w.fill(d);
      await w.fill(d);
      final items = await w.items(d.id);
      expect(items.map((i) => i.eventIds), <List<String>>[
        <String>[e1.eventId, e2.eventId],
        <String>[e3.eventId, e4.eventId],
      ]);
      expect(items.map((i) => i.blockKey), <String?>['A', null]);
    });

    // Verifies: EVS-DEV-destination-key-hold/L
    test('an event of a listed key is inserted into its list, not queued, '
        'untransformed', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'A');
      final e3 = await w.note('n3', key: 'B');
      await w.writeKeyLists(d.id, <String, KeyList>{
        'A': heldList(<StoredEvent>[e1]),
      });
      await w.fill(d);
      await w.fill(d);
      expect(
        d.transformCalls,
        0,
        reason: "a listed key's events are routed untransformed",
      );
      expect(await w.items(d.id), isEmpty);
      expect(await w.backend.readFillCursor(d.id), e2.sequenceNumber);
      expect((await w.keyLists(d.id))['A'], heldList(<StoredEvent>[e1, e2]));
      await w.fill(d);
      expect(d.transformCalls, 1);
      final items = await w.items(d.id);
      expect(items.single.eventIds, <String>[e3.eventId]);
      expect(items.single.blockKey, 'B');
    });

    // Verifies: EVS-DEV-destination-key-hold/L
    test('an event routed ahead of a later listed event lands at its '
        'sequence position', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'A');
      await w.writeKeyLists(d.id, <String, KeyList>{
        'A': heldList(<StoredEvent>[e2]),
      });
      await w.fill(d);
      final list = (await w.keyLists(d.id))['A']!;
      expect(list.events, <KeyListEvent>[listEventOf(e1), listEventOf(e2)]);
    });

    // Verifies: EVS-DEV-destination-key-hold/M
    test('an event its list already holds leaves the list unchanged', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'A');
      final list = heldList(<StoredEvent>[e1]);
      await w.writeKeyLists(d.id, <String, KeyList>{'A': list});
      await w.fill(d);
      expect(await w.items(d.id), isEmpty);
      expect(await w.backend.readFillCursor(d.id), e1.sequenceNumber);
      expect(await w.keyLists(d.id), <String, KeyList>{'A': list});
    });

    // Verifies: EVS-DEV-destination-key-hold/E
    test('a key containing | is queued keyless', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final e1 = await w.note('n1', key: 'a|b');
      await w.fill(d);
      final items = await w.items(d.id);
      expect(items.single.eventIds, <String>[e1.eventId]);
      expect(items.single.blockKey, isNull);
    });

    // Verifies: EVS-DEV-destination-drain/G
    test('a fill computed before a list changes commits nothing', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final cursorBefore = await w.backend.readFillCursor(d.id);
      final e1 = await w.note('n1', key: 'A');
      final concurrent = <String, KeyList>{
        'A': heldList(<StoredEvent>[], holdId: 'concurrent'),
      };
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterFillReads: (id) => w.writeKeyLists(id, concurrent),
        ),
        () => w.fill(d),
      );
      expect(
        d.transformCalls,
        1,
        reason: 'the item was built before the list changed',
      );
      expect(await w.items(d.id), isEmpty, reason: 'its commit wrote nothing');
      expect(await w.backend.readFillCursor(d.id), cursorBefore);
      expect(await w.keyLists(d.id), concurrent);
      await w.fill(d);
      expect(await w.items(d.id), isEmpty);
      expect(
        (await w.keyLists(d.id))['A']!.events,
        <KeyListEvent>[listEventOf(e1)],
        reason: 'the next fill routes the event into the new list',
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/A+B+L
    test("a first-activation replay routes a listed key's events into its "
        'list', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activate(d);
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'B');
      await w.writeKeyLists(d.id, <String, KeyList>{
        'A': heldList(<StoredEvent>[], holdId: 'h'),
      });
      await w.fill(d);
      expect(await w.replayRequest(d.id), isNull);
      expect(d.transformCalls, 1);
      final items = await w.items(d.id);
      expect(items.single.eventIds, <String>[e2.eventId]);
      expect(items.single.blockKey, 'B');
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(e1),
      ]);
      expect(await w.backend.readFillCursor(d.id), e2.sequenceNumber);
    });

    // Verifies: EVS-DEV-destination-key-hold/A+B+L+M
    test("a gap replay routes a listed key's events into its list", () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activate(d, startDate: DateTime.utc(2026, 6, 1));
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'B');
      final e3 = await w.note('n3', key: 'A');
      await w.fill(d);
      expect(await w.items(d.id), isEmpty, reason: 'before the start date');
      await w.writeKeyLists(d.id, <String, KeyList>{
        'A': heldList(<StoredEvent>[e3]),
      });
      await w.registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      await w.fill(d);
      expect(await w.replayRequest(d.id), isNull);
      expect(d.transformCalls, 1, reason: 'only the B event is transformed');
      final items = await w.items(d.id);
      expect(items.single.eventIds, <String>[e2.eventId]);
      expect(items.single.blockKey, 'B');
      expect(
        (await w.keyLists(d.id))['A'],
        heldList(<StoredEvent>[e1, e3]),
        reason: 'e1 is inserted ahead of e3, which the list already held',
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/G
    // Verifies: EVS-DEV-destination-drain/Y
    test('a blockKeyOf throw in the normal fill follows the transform failure '
        'record', () async {
      if (!available) return;
      var throwing = false;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: (e) {
          if (throwing) throw StateError('scripted key failure');
          return noteKey(e);
        },
      );
      await w.activateResolved(d);
      throwing = true;
      final e1 = await w.note('n1', key: 'A');
      final policy = _policy(maxAttempts: 3);
      final cursorBefore = await w.backend.readFillCursor(d.id);
      await w.fill(d, policy: policy);
      expect(await w.items(d.id), isEmpty);
      expect(await w.backend.readFillCursor(d.id), cursorBefore);
      final record = await w.failureRecord(d.id);
      expect(record!.failureTimes, <DateTime>[_fillTime()]);
      expect(record.sequenceRange, (
        firstSeq: e1.sequenceNumber,
        lastSeq: e1.sequenceNumber,
      ));
      expect(d.transformCalls, 0, reason: 'no transform runs without a key');
      await w.fill(d, policy: policy);
      await w.fill(d, policy: policy);
      final item = (await w.items(d.id)).single;
      expect(item.transformFailed, isTrue);
      expect(item.transformFailures, 3);
      expect(item.eventIds, <String>[e1.eventId]);
      expect(item.blockKey, isNull);
      expect(await w.failureRecord(d.id), isNull);
      expect(await w.backend.readFillCursor(d.id), e1.sequenceNumber);
    });

    // Verifies: EVS-DEV-destination-key-hold/G
    // Verifies: EVS-DEV-destination-drain/Y
    test('a blockKeyOf throw in a gap replay records a transform failure and '
        'retries', () async {
      if (!available) return;
      var throwing = false;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: (e) {
          if (throwing && e.data['id'] == 'n2') {
            throw StateError('scripted key failure');
          }
          return noteKey(e);
        },
      );
      await w.activate(d, startDate: DateTime.utc(2026, 6, 1));
      final e1 = await w.note('n1', key: 'A');
      final e2 = await w.note('n2', key: 'B');
      await w.fill(d);
      throwing = true;
      await w.registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      final policy = _policy(maxAttempts: 3);
      await w.fill(d, policy: policy);
      expect(
        await w.items(d.id),
        isEmpty,
        reason: 'a gap replay that stops commits none of its items',
      );
      expect(await w.replayRequest(d.id), isNotNull, reason: 'still pending');
      final record = await w.failureRecord(d.id);
      expect(record!.failureTimes, <DateTime>[_fillTime()]);
      expect(record.sequenceRange, (
        firstSeq: e2.sequenceNumber,
        lastSeq: e2.sequenceNumber,
      ));
      await w.fill(d, policy: policy);
      expect((await w.failureRecord(d.id))!.failureTimes, hasLength(2));
      await w.fill(d, policy: policy);
      final items = await w.items(d.id);
      expect(items.map((i) => i.eventIds), <List<String>>[
        <String>[e1.eventId],
        <String>[e2.eventId],
      ]);
      expect(items.first.transformFailed, isFalse);
      expect(items.first.blockKey, 'A');
      expect(items.last.transformFailed, isTrue);
      expect(items.last.transformFailures, 3);
      expect(await w.replayRequest(d.id), isNull);
      expect(await w.failureRecord(d.id), isNull);
    });

    // Verifies: EVS-DEV-destination-drain/Y
    test('a transform throw in a gap replay records a transform failure and '
        'the replay completes once it recovers', () async {
      if (!available) return;
      final d = _ThrowingTransformDestination(id: 'keyed', keyOf: noteKey);
      await w.activate(d, startDate: DateTime.utc(2026, 6, 1));
      final e1 = await w.note('n1', key: 'A');
      await w.fill(d);
      d.throwing = true;
      await w.registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      await w.fill(d);
      expect(await w.items(d.id), isEmpty);
      expect(await w.replayRequest(d.id), isNotNull);
      expect((await w.failureRecord(d.id))!.sequenceRange, (
        firstSeq: e1.sequenceNumber,
        lastSeq: e1.sequenceNumber,
      ));
      d.throwing = false;
      await w.fill(d);
      final item = (await w.items(d.id)).single;
      expect(item.eventIds, <String>[e1.eventId]);
      expect(item.transformFailed, isFalse);
      expect(await w.replayRequest(d.id), isNull);
      expect(await w.failureRecord(d.id), isNull);
    });
  });
}

/// A keyed [FakeDestination] whose transform throws while [throwing].
class _ThrowingTransformDestination extends FakeDestination {
  _ThrowingTransformDestination({required super.id, super.keyOf});

  bool throwing = false;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    if (throwing) {
      transformCalls += 1;
      return Future<WirePayload>.error(StateError('scripted transform'));
    }
    return super.transform(batch);
  }
}
