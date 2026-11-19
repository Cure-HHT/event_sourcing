// Backend-agnostic scenarios for a keyed destination's queue: the block key
// the fill and every replay evaluate and store on each queue item, the
// routing of a listed key's events into its key list, the failure path
// of a block-key evaluation that throws, and the hold a key-scoped
// permanent failure commits. Sembast runs them from
// test/sync/keyed_hold_sembast_test.dart and Postgres from
// test/storage/postgres/postgres_keyed_hold_test.dart.
//
// This file exposes one function, [runKeyedHoldScenarios], and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show holdKeyInTxnForTest;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'declarations.dart';
import 'deliveries.dart' show ingestEventForTest;
import 'destination_wedges_view_conformance.dart' show forgedEvent;
import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart'
    show drainForTest, fillForTest, honourHaltForTest, withTestDrainLock;
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

  /// Runs [racers] concurrently under one harness drain lock, which every
  /// fill and drain among them shares, so none of them takes the lock
  /// afresh while another releases it.
  Future<void> race(List<Future<void> Function()> racers) => withTestDrainLock(
    backend,
    (_) => Future.wait(<Future<void>>[for (final r in racers) r()]),
    databaseId: a.store.databaseId,
  );

  /// [activateResolved] with a start date after every note appended so
  /// far, so [d] admits only later ones.
  Future<void> activateFromNow(Destination d) async {
    await activate(d, startDate: eventTime.add(const Duration(seconds: 1)));
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
    registry: registry,
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

  Future<WedgeRecord?> wedgeRecord(String id) =>
      backend.transaction((txn) => backend.readWedgeRecordTxn(txn, id));

  /// One drain pass of [d] through the current process, at a time after
  /// every backoff.
  Future<void> drain(Destination d, {SyncPolicy? policy}) => drainForTest(
    d,
    registry: registry,
    clock: () => DateTime.utc(2100),
    policy: policy ?? _policy(),
  );

  /// Fills [d] until every event of the log is queued or listed.
  Future<void> fillAll(Destination d, {int passes = 10}) async {
    for (var i = 0; i < passes; i++) {
      await fill(d);
    }
  }

  Future<List<StoredEvent>> heldEvents() =>
      backend.findAllEvents(entryType: kDestinationHeldEntryType);

  Future<List<StoredEvent>> wedgeEvents() =>
      backend.findAllEvents(entryType: kDestinationWedgedEntryType);

  Future<List<StoredEvent>> releasedEvents() =>
      backend.findAllEvents(entryType: kDestinationHoldReleasedEntryType);

  Future<List<StoredEvent>> clearedEvents() =>
      backend.findAllEvents(entryType: kDestinationHoldClearedEntryType);

  Future<Map<String, ReleaseFailureRecord>> releaseFailures(String id) =>
      backend.transaction((txn) => backend.readReleaseFailuresTxn(txn, id));

  Future<void> writeFillCursor(String id, int position) => backend.transaction(
    (txn) => backend.writeFillCursorTxn(txn, id, position),
  );

  /// Releases [key] of [d] under [holdId].
  Future<ReleaseHoldResult> release(
    Destination d,
    String key,
    String holdId, {
    String requestId = 'request-1',
  }) => registry.releaseHold(
    d.id,
    key,
    holdId: holdId,
    initiator: _init,
    requestId: requestId,
  );

  /// Holds key A of [d], whose script opens with a key-scoped refusal:
  /// queues the events the caller appended, drains once, and returns the
  /// hold id.
  Future<String> holdA(Destination d) async {
    await fillAll(d);
    await drain(d);
    final held = (await heldEvents()).last;
    return held.data['hold_id']! as String;
  }

  /// Checks [id]'s queue and key lists against [notes], every note appended
  /// to the log: no event is in two places (a pending or sent item, or a
  /// key list); each key's events, in sent items, then pending items in
  /// queue order, then the key's list, are in sequence order; and while a
  /// key has a list, every pending item of the key is one a release
  /// enqueued, ahead of the list's front. With [settled], every note is in
  /// exactly one place.
  Future<void> expectPerKeyOrder(
    String id,
    List<StoredEvent> notes, {
    bool settled = false,
  }) async {
    final items = await this.items(id);
    final lists = await keyLists(id);
    final seqOf = <String, int>{
      for (final n in notes) n.eventId: n.sequenceNumber,
    };
    final keyOf = <String, String?>{
      for (final n in notes) n.eventId: noteKey(n),
    };
    final placed = <String>[];
    final order = <String, List<int>>{};
    void place(String eventId) {
      placed.add(eventId);
      final seq = seqOf[eventId];
      if (seq == null) return;
      (order[keyOf[eventId] ?? ''] ??= <int>[]).add(seq);
    }

    for (final status in <FinalStatus?>[FinalStatus.sent, null]) {
      for (final item in items) {
        if (item.finalStatus != status) continue;
        item.eventIds.forEach(place);
      }
    }
    for (final entry in lists.entries) {
      for (final e in entry.value.events) {
        place(e.eventId);
      }
      final front = entry.value.events.isEmpty
          ? null
          : entry.value.events.first.sequenceNumber;
      for (final item in items) {
        if (item.finalStatus != null || item.blockKey != entry.key) continue;
        expect(
          item.releasedHoldId,
          isNotNull,
          reason:
              'a pending item of listed key ${entry.key} entered the '
              'queue other than by its release',
        );
        if (front != null) {
          expect(item.sequenceRange.lastSeq, lessThan(front));
        }
      }
    }
    expect(
      placed.toSet(),
      hasLength(placed.length),
      reason: 'an event is queued or listed twice',
    );
    for (final entry in order.entries) {
      final seqs = entry.value;
      for (var i = 1; i < seqs.length; i++) {
        expect(
          seqs[i],
          greaterThan(seqs[i - 1]),
          reason: 'key "${entry.key}" is out of order: $seqs',
        );
      }
    }
    if (settled) {
      expect(placed.toSet(), <String>{for (final n in notes) n.eventId});
    }
  }

  /// The items of [id] a release enqueued.
  Future<List<FifoEntry>> releasedItems(String id) async => <FifoEntry>[
    for (final i in await items(id))
      if (i.releasedHoldId != null) i,
  ];
}

/// The data keys of a hold event.
const Set<String> _holdEventKeys = <String>{
  'id',
  'database_id',
  'key',
  'hold_id',
  'row_id',
  'failed_event_id',
  'event_ids',
  'cause',
  'attempt_count',
};

/// The data keys of a release event.
const Set<String> _releaseEventKeys = <String>{
  'id',
  'database_id',
  'key',
  'hold_id',
  'request_id',
};

/// The data keys of a clearing event.
const Set<String> _clearedEventKeys = <String>{
  'id',
  'database_id',
  'key',
  'hold_id',
};

/// A key-scoped permanent failure.
const SendPermanent _keyRefusal = SendPermanent(
  error: 'refused for this key',
  scope: WedgeScope.key,
);

/// Run every keyed-hold scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
///
/// With [serializableConflicts] (a backend whose transactions abort with a
/// serialization failure on a concurrent update, and re-run), the scenario
/// that provokes one on a hold runs too.
void runKeyedHoldScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
  bool serializableConflicts = false,
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

    // ------------------------------------------------------------------
    // The hold a key-scoped permanent failure commits
    // ------------------------------------------------------------------

    // Verifies: EVS-DEV-destination-key-hold/H+J+K
    // Verifies: EVS-DEV-destination-drain/B+C+D
    test('SendPermanent(scope: key) holds the key', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal, const SendOk()],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      await w.fillAll(d);
      final queued = await w.items(d.id);
      expect(queued.map((i) => i.blockKey), <String?>['A', 'B', 'A']);
      await w.drain(d);
      final items = await w.items(d.id);
      expect(
        items.map((i) => i.entryId),
        <String>[queued[0].entryId, queued[1].entryId],
        reason: "K's pending item behind the head is deleted",
      );
      final head = items.first;
      expect(head.finalStatus, FinalStatus.tombstoned);
      expect(head.attempts, hasLength(1));
      expect(head.attempts.single.outcome, 'permanent');
      expect(head.attempts.single.scope, WedgeScope.key);
      final held = (await w.heldEvents()).single;
      expect(held.data.keys.toSet(), _holdEventKeys);
      expect(held.data['id'], d.id);
      expect(held.data['database_id'], w.a.store.databaseId);
      expect(held.data['key'], 'A');
      expect(held.data['row_id'], head.entryId);
      expect(held.data['failed_event_id'], isNull);
      expect(held.data['event_ids'], <String>[a1.eventId, a2.eventId]);
      expect(held.data['cause'], 'permanent_refusal');
      expect(held.data['attempt_count'], 1);
      final holdId = held.data['hold_id']! as String;
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.held,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      expect(await w.wedgeRecord(d.id), isNull);
      expect(await w.wedgeEvents(), isEmpty);
      expect(items.last.eventIds, <String>[b1.eventId]);
    });

    // Verifies: EVS-DEV-destination-key-hold/H
    test("another key's next item is sent after a hold", () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal, const SendOk(), const SendOk()],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final c1 = await w.note('c1', key: 'C');
      await w.fillAll(d);
      await w.drain(d);
      expect(d.sent, hasLength(3), reason: 'the pass goes on after the hold');
      final items = await w.items(d.id);
      expect(items.map((i) => i.finalStatus), <FinalStatus?>[
        FinalStatus.tombstoned,
        FinalStatus.sent,
        FinalStatus.sent,
      ]);
      expect(items[1].eventIds, <String>[b1.eventId]);
      expect(items[2].eventIds, <String>[c1.eventId]);
    });

    // Verifies: EVS-DEV-destination-key-hold/H+L
    test("the held key's later events go to its list", () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      await w.fillAll(d);
      await w.drain(d);
      final a2 = await w.note('a2', key: 'A');
      await w.fillAll(d);
      expect(await w.items(d.id), hasLength(1), reason: 'only the held head');
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(a1),
        listEventOf(a2),
      ]);
    });

    // Verifies: EVS-PRD-destination-key-hold/L
    test('a destination registered without a key while it has key lists is '
        'refused, and registers once they are deleted', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'subjects',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.holdA(d);
      final lists = await w.keyLists(d.id);
      expect(lists, isNotEmpty);
      final registered = await w.backend.findAllEvents(
        entryType: kDestinationRegisteredEntryType,
      );

      // A new process registers the destination without its key.
      w.a = await w.openProcess();
      await expectLater(
        w.registry.addDestination(FakeDestination(id: d.id), initiator: _init),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.toString(),
            'message',
            allOf(
              contains(d.id),
              contains('keyed'),
              contains('deleteDestination'),
            ),
          ),
        ),
      );
      expect(w.registry.byId(d.id), isNull);
      expect(await w.keyLists(d.id), lists);
      expect(
        await w.backend.findAllEvents(
          entryType: kDestinationRegisteredEntryType,
        ),
        hasLength(registered.length),
      );

      // Registered keyed, it is accepted.
      w.a = await w.openProcess();
      await w.registry.addDestination(
        FakeDestination(id: d.id, keyOf: noteKey, allowHardDelete: true),
        initiator: _init,
      );
      expect(w.registry.byId(d.id), isNotNull);

      // Deleted, its key lists go with it and it registers without a key.
      await w.registry.deleteDestination(d.id, initiator: _init);
      expect(await w.keyLists(d.id), isEmpty);
      await w.registry.addDestination(
        FakeDestination(id: d.id),
        initiator: _init,
      );
      expect(w.registry.byId(d.id), isNotNull);
    });

    // Verifies: EVS-DEV-destination-key-hold/I
    test('a key-scoped failure while any keyless pending item exists wedges '
        'the destination', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.note('n1');
      await w.fillAll(d);
      await w.drain(d);
      final head = (await w.items(d.id)).first;
      expect(head.finalStatus, FinalStatus.wedged);
      expect(head.attempts.single.scope, WedgeScope.key);
      expect((await w.wedgeRecord(d.id))!.cause, WedgeCause.permanentRefusal);
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.heldEvents(), isEmpty);
      expect(await w.wedgeEvents(), hasLength(1));
    });

    // Verifies: EVS-DEV-destination-key-hold/I
    test(
      'a key-scoped failure on a keyless item wedges the destination',
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[_keyRefusal],
        );
        await w.activateResolved(d);
        await w.note('n1');
        await w.note('a1', key: 'A');
        await w.fillAll(d);
        await w.drain(d);
        final items = await w.items(d.id);
        expect(items.first.blockKey, isNull);
        expect(items.first.finalStatus, FinalStatus.wedged);
        expect((await w.wedgeRecord(d.id))!.cause, WedgeCause.permanentRefusal);
        expect(await w.keyLists(d.id), isEmpty);
        expect(await w.heldEvents(), isEmpty);
      },
    );

    // Verifies: EVS-DEV-destination-key-hold/J
    // Verifies: EVS-DEV-destination-drain/J
    test('SendPermanent with destination scope on a keyed item wedges as '
        'today', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[const SendPermanent(error: 'refused')],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.note('a2', key: 'A');
      await w.fillAll(d);
      await w.drain(d);
      final items = await w.items(d.id);
      expect(items, hasLength(2));
      expect(items.first.finalStatus, FinalStatus.wedged);
      expect(items.first.attempts.single.scope, WedgeScope.destination);
      expect(items.last.finalStatus, isNull);
      expect((await w.wedgeRecord(d.id))!.cause, WedgeCause.permanentRefusal);
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.heldEvents(), isEmpty);
    });

    // Verifies: EVS-DEV-destination-key-hold/H+J
    // Verifies: EVS-DEV-destination-drain/C+J
    test('restart after a key-scoped attempt re-derives the hold', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      await w.fillAll(d);
      var failed = false;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          failOutcomeTransaction: (id, outcome) {
            if (failed) return false;
            failed = true;
            return true;
          },
        ),
        () => w.drain(d),
      );
      expect(failed, isTrue);
      var items = await w.items(d.id);
      expect(items, hasLength(3), reason: 'the hold did not commit');
      expect(items.first.finalStatus, isNull);
      expect(items.first.attempts.single.scope, WedgeScope.key);
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.heldEvents(), isEmpty);

      // A new process, whose drainer sends only the other key's item.
      w.a = await w.openProcess();
      final after = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[const SendOk()],
      );
      await w.drain(after);
      expect(after.sent, hasLength(1), reason: 'the hold needs no send');
      items = await w.items(d.id);
      expect(items.map((i) => i.finalStatus), <FinalStatus?>[
        FinalStatus.tombstoned,
        FinalStatus.sent,
      ]);
      expect(items.last.eventIds, <String>[b1.eventId]);
      final held = (await w.heldEvents()).single;
      expect(held.data['attempt_count'], 1);
      expect(held.data['event_ids'], <String>[a1.eventId, a2.eventId]);
      expect((await w.keyLists(d.id))['A']!.holdId, held.data['hold_id']);
    });

    // Verifies: EVS-DEV-destination-key-hold/H+Y
    // Verifies: EVS-DEV-destination-drain/C
    test('honouring a halt by id holds a key-scoped head and wedges the next '
        'head', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      await w.fillAll(d);
      var failed = false;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          failOutcomeTransaction: (id, outcome) {
            if (failed) return false;
            failed = true;
            return true;
          },
        ),
        () => w.drain(d),
      );
      expect(failed, isTrue);
      expect((await w.items(d.id)).first.attempts.single.scope, WedgeScope.key);
      final requestId = await w.registry.requestHalt(
        d.id,
        initiator: _init,
        purpose: HaltPurpose.pause,
      );

      // A process that does not register the destination honours the halt.
      w.a = await w.openProcess();
      await honourHaltForTest(d.id, registry: w.registry);
      final items = await w.items(d.id);
      expect(items.map((i) => i.finalStatus), <FinalStatus?>[
        FinalStatus.tombstoned,
        FinalStatus.wedged,
      ]);
      expect(items.last.eventIds, <String>[b1.eventId]);
      final held = (await w.heldEvents()).single;
      expect(held.data['event_ids'], <String>[a1.eventId]);
      expect((await w.keyLists(d.id))['A']!.holdId, held.data['hold_id']);
      final wedge = await w.wedgeRecord(d.id);
      expect(wedge?.rowId, items.last.entryId);
      expect(wedge?.cause, WedgeCause.operatorHalt);
      expect(
        (await w.wedgeEvents()).single.data['halt_request_event_id'],
        requestId,
      );
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readHaltRequestTxn(txn, d.id),
        ),
        isNull,
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/H
    test('a hold merges with an existing list in sequence order', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final a3 = await w.note('a3', key: 'A');
      await w.fill(d);
      await w.fill(d);
      // An existing list of the key, holding the latest event.
      final existing = KeyList(
        holdId: 'earlier-hold',
        state: KeyListState.held,
        cause: 'permanent_refusal',
        events: <KeyListEvent>[listEventOf(a3)],
      );
      await w.writeKeyLists(d.id, <String, KeyList>{'A': existing});
      await w.fillAll(d);
      expect(await w.items(d.id), hasLength(2));
      await w.drain(d);
      final held = (await w.heldEvents()).single;
      final holdId = held.data['hold_id']! as String;
      expect(holdId, isNot('earlier-hold'));
      expect(held.data['event_ids'], <String>[
        a1.eventId,
        a2.eventId,
        a3.eventId,
      ]);
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.held,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[
            listEventOf(a1),
            listEventOf(a2),
            listEventOf(a3),
          ],
        ),
      });
      expect(await w.items(d.id), hasLength(1));
    });

    // Verifies: EVS-DEV-destination-drain/J
    test(
      'retry-budget exhaustion on a keyed item wedges the destination',
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[const SendTransient(error: 'busy')],
        );
        await w.activateResolved(d);
        await w.note('a1', key: 'A');
        await w.fillAll(d);
        await w.drain(d, policy: _policy(maxAttempts: 1));
        final head = (await w.items(d.id)).single;
        expect(head.finalStatus, FinalStatus.wedged);
        expect(
          (await w.wedgeRecord(d.id))!.cause,
          WedgeCause.retryBudgetExhausted,
        );
        expect(await w.keyLists(d.id), isEmpty);
        expect(await w.heldEvents(), isEmpty);
      },
    );

    // Verifies: EVS-DEV-destination-key-hold/H
    // Verifies: EVS-DEV-destination-drain/J
    test(
      'a key-scoped refusal that spends the retry budget holds the key',
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[
            const SendTransient(error: 'busy'),
            _keyRefusal,
          ],
        );
        await w.activateResolved(d);
        await w.note('a1', key: 'A');
        await w.fillAll(d);
        final policy = _policy(maxAttempts: 2);
        await w.drain(d, policy: policy);
        await w.drain(d, policy: policy);
        final head = (await w.items(d.id)).single;
        expect(head.attempts, hasLength(2));
        expect(head.finalStatus, FinalStatus.tombstoned);
        expect(await w.wedgeRecord(d.id), isNull);
        final held = (await w.heldEvents()).single;
        expect(held.data['attempt_count'], 2);
        expect(held.data['cause'], 'permanent_refusal');
      },
    );

    // Verifies: EVS-DEV-destination-key-hold/K+W
    test('a hold with no queue item names the event a release failed to '
        'build', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      await w.writeKeyLists(d.id, <String, KeyList>{
        'A': KeyList(
          holdId: 'earlier-hold',
          state: KeyListState.releasing,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      final holdId = await w.a.store.runTransaction(
        (txn, collector) => holdKeyInTxnForTest(
          w.registry,
          txn,
          collector,
          destinationId: d.id,
          key: 'A',
          failedEventId: a1.eventId,
          cause: 'transform_failed',
          attemptCount: 3,
        ),
      );
      final held = (await w.heldEvents()).single;
      expect(held.data.keys.toSet(), _holdEventKeys);
      expect(held.data['hold_id'], holdId);
      expect(held.data['row_id'], isNull);
      expect(held.data['failed_event_id'], a1.eventId);
      expect(held.data['event_ids'], <String>[a1.eventId, a2.eventId]);
      expect(held.data['cause'], 'transform_failed');
      expect(held.data['attempt_count'], 3);
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.held,
          cause: 'transform_failed',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      expect(await w.items(d.id), isEmpty);
    });

    // Verifies: EVS-DEV-destination-key-hold/H
    test('a hold with no queue item refuses a call that does not name exactly '
        'one of a head and a failed event, or names no list', () async {
      if (!available) return;
      final d = FakeDestination(id: 'keyed', keyOf: noteKey);
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      await w.fill(d);
      final head = (await w.items(d.id)).single;
      Future<String> hold({FifoEntry? head, String? failedEventId}) =>
          w.a.store.runTransaction(
            (txn, collector) => holdKeyInTxnForTest(
              w.registry,
              txn,
              collector,
              destinationId: d.id,
              key: 'A',
              head: head,
              failedEventId: failedEventId,
              cause: 'transform_failed',
              attemptCount: 1,
            ),
          );
      await expectLater(hold(), throwsStateError);
      await expectLater(
        hold(head: head, failedEventId: a1.eventId),
        throwsStateError,
      );
      await expectLater(
        hold(failedEventId: a1.eventId),
        throwsStateError,
        reason: 'no list for the key',
      );
      expect(await w.heldEvents(), isEmpty);
      expect(await w.keyLists(d.id), isEmpty);
      expect((await w.items(d.id)).single.finalStatus, isNull);
    });

    // ------------------------------------------------------------------
    // The release of a held key
    // ------------------------------------------------------------------

    // Verifies: EVS-DEV-destination-key-hold/O
    // Verifies: EVS-PRD-destination-key-hold/I
    test('releaseHold moves the list to releasing and appends one release '
        'event', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      final before = await w.keyLists(d.id);
      final result = await w.release(d, 'A', holdId, requestId: 'req-7');
      final released = (await w.releasedEvents()).single;
      expect(result, ReleaseHoldApplied(released.eventId));
      expect(released.data.keys.toSet(), _releaseEventKeys);
      expect(released.data['id'], d.id);
      expect(released.data['database_id'], w.a.store.databaseId);
      expect(released.data['key'], 'A');
      expect(released.data['hold_id'], holdId);
      expect(released.data['request_id'], 'req-7');
      expect(released.initiator.toJson(), _init.toJson());
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.releasing,
          cause: before['A']!.cause,
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
    });

    // Verifies: EVS-DEV-destination-key-hold/P
    test('releaseHold twice with one holdId returns AlreadyReleasing and '
        'appends nothing', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      final holdId = await w.holdA(d);
      final first = await w.release(d, 'A', holdId);
      final lists = await w.keyLists(d.id);
      final second = await w.release(d, 'A', holdId, requestId: 'request-2');
      final released = (await w.releasedEvents()).single;
      expect(first, ReleaseHoldApplied(released.eventId));
      expect(second, ReleaseHoldAlreadyReleasing(released.eventId));
      expect(await w.keyLists(d.id), lists);
    });

    // Verifies: EVS-DEV-destination-key-hold/N
    test('releaseHold with a stale holdId returns Stale', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      final holdId = await w.holdA(d);
      final lists = await w.keyLists(d.id);
      expect(await w.release(d, 'A', 'another-hold'), ReleaseHoldStale(holdId));
      expect(await w.release(d, 'B', holdId), const ReleaseHoldStale(null));
      expect(await w.releasedEvents(), isEmpty);
      expect(await w.keyLists(d.id), lists);
    });

    // Verifies: EVS-DEV-destination-key-hold/A+Q+R+S+T
    // Verifies: EVS-PRD-destination-key-hold/C+E+J
    test('release re-queues the list front in sequence order at the tail with '
        'released_hold_id set, and clears the list with one cleared event when '
        'emptied', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal, const SendOk()],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      final queuedBefore = await w.items(d.id);
      expect(queuedBefore.every((i) => i.releasedHoldId == null), isTrue);
      await w.release(d, 'A', holdId);
      await w.fill(d);
      var items = await w.items(d.id);
      expect(items, hasLength(queuedBefore.length + 1));
      final first = items.last;
      expect(first.eventIds, <String>[a1.eventId]);
      expect(first.blockKey, 'A');
      expect(first.releasedHoldId, holdId);
      expect(
        first.sequenceInQueue,
        greaterThan(queuedBefore.last.sequenceInQueue),
      );
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(a2),
      ]);
      expect(await w.clearedEvents(), isEmpty);
      await w.fill(d);
      items = await w.items(d.id);
      expect(items.last.eventIds, <String>[a2.eventId]);
      expect(items.last.releasedHoldId, holdId);
      expect(await w.keyLists(d.id), isEmpty);
      final cleared = (await w.clearedEvents()).single;
      expect(cleared.data.keys.toSet(), _clearedEventKeys);
      expect(cleared.data['id'], d.id);
      expect(cleared.data['database_id'], w.a.store.databaseId);
      expect(cleared.data['key'], 'A');
      expect(cleared.data['hold_id'], holdId);
      await w.fillAll(d);
      expect(await w.items(d.id), hasLength(items.length));
      expect(await w.clearedEvents(), hasLength(1));
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(b1.eventId)),
        hasLength(1),
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/T+U+X
    test("a peer database's hold and clearing events under the same hold id "
        'leave the clearing and a recovery to this database', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendPermanent(error: 'refused'),
        ],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final holdId = await w.holdA(d);
      Future<void> ingestPeer(
        String entryType,
        String eventType,
        Map<String, Object?> data,
      ) async {
        final outcome = await ingestEventForTest(
          w.a.store,
          forgedEvent(
            entryType: entryType,
            aggregateType: kDestinationAuditAggregateType,
            eventType: eventType,
            data: <String, Object?>{
              'id': d.id,
              'database_id': 'peer-db',
              'key': 'A',
              'hold_id': holdId,
              ...data,
            },
          ),
        );
        expect(outcome.outcome, IngestOutcome.ingested, reason: eventType);
      }

      await ingestPeer(kDestinationHeldEntryType, kDestinationHeldEventType, {
        'row_id': 'peer-row',
        'failed_event_id': null,
        'event_ids': <String>['e1'],
        'cause': 'transform_failed',
        'attempt_count': 1,
      });
      await ingestPeer(
        kDestinationHoldClearedEntryType,
        kDestinationHoldClearedEventType,
        const <String, Object?>{},
      );

      await w.release(d, 'A', holdId);
      await w.fill(d);
      expect(await w.keyLists(d.id), isEmpty);
      final cleared = [
        for (final e in await w.clearedEvents())
          if (e.data['database_id'] == w.a.store.databaseId) e,
      ];
      expect(cleared, hasLength(1));
      expect(cleared.single.data['hold_id'], holdId);

      // A recovery of the released item creates its list again under the
      // cause of this database's hold event.
      await w.drain(d);
      final head = (await w.releasedItems(d.id)).single;
      expect(head.finalStatus, FinalStatus.wedged);
      await w.registry.tombstoneAndRefill(d.id, head.entryId, initiator: _init);
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.releasing,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1)],
        ),
      });
    });

    // Verifies: EVS-DEV-destination-key-hold/Q
    test('release waits while the fill position is below the list front or a '
        'replay request is pending', () async {
      if (!available) return;
      final d = _ThrowingTransformDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      // The window opens after the first note, which a later start date
      // moved earlier lets in through a gap replay.
      await w.activate(d, startDate: DateTime.utc(2026, 3, 1, 0, 1, 30));
      await w.fill(d);
      final x1 = await w.note('x1', key: 'X');
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);

      // A fill position below the list's front: nothing is released until
      // the walk has passed it again.
      await w.writeFillCursor(d.id, a1.sequenceNumber - 1);
      await w.fill(d);
      expect(await w.releasedItems(d.id), isEmpty);
      expect(await w.backend.readFillCursor(d.id), a1.sequenceNumber);
      await w.fill(d);
      expect((await w.releasedItems(d.id)).single.eventIds, <String>[
        a1.eventId,
      ]);

      // A replay request that stays pending holds the release.
      d
        ..throwing = true
        ..throwsFor = <String>{'x1'};
      await w.registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      await w.fill(d);
      expect(await w.replayRequest(d.id), isNotNull);
      expect(await w.releasedItems(d.id), hasLength(1));
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(a2),
      ]);
      d.throwing = false;
      await w.fill(d);
      expect(await w.replayRequest(d.id), isNull);
      final released = await w.releasedItems(d.id);
      expect(released.last.eventIds, <String>[a2.eventId]);
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(x1.eventId)),
        hasLength(1),
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/L+Q
    test('events of the key arriving during release join the list', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      final a3 = await w.note('a3', key: 'A');
      await w.fill(d);
      final list = (await w.keyLists(d.id))['A']!;
      expect(list.state, KeyListState.releasing);
      expect(list.holdId, holdId);
      expect(list.events, <KeyListEvent>[listEventOf(a2), listEventOf(a3)]);
      await w.fillAll(d);
      expect(
        (await w.releasedItems(d.id)).map((i) => i.eventIds),
        <List<String>>[
          <String>[a1.eventId],
          <String>[a2.eventId],
          <String>[a3.eventId],
        ],
      );
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.clearedEvents(), hasLength(1));
    });

    // Verifies: EVS-DEV-destination-key-hold/H
    test('a refusal of a released item re-holds the key with the remaining '
        'list behind it', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        batchCapacity: 1,
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      await w.fill(d);
      final released = (await w.releasedItems(d.id)).single;
      expect(released.eventIds, <String>[a1.eventId]);
      expect(
        (await w.keyLists(d.id))['A']!.events,
        <KeyListEvent>[listEventOf(a2)],
        reason: 'a2 is still in the list when a1 is refused',
      );
      await w.drain(d);
      expect((await w.items(d.id)).last.finalStatus, FinalStatus.tombstoned);
      final held = await w.heldEvents();
      expect(held, hasLength(2));
      final again = held.last;
      expect(again.data['row_id'], released.entryId);
      expect(again.data['event_ids'], <String>[a1.eventId, a2.eventId]);
      final newHoldId = again.data['hold_id']! as String;
      expect(newHoldId, isNot(holdId));
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: newHoldId,
          state: KeyListState.held,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      expect(await w.clearedEvents(), isEmpty);
      expect(await w.wedgeRecord(d.id), isNull);
    });

    // Verifies: EVS-DEV-destination-key-hold/H+Q+R
    test('a released item carries the list key even when the key evaluates '
        'otherwise at release, and its refusal holds that key again', () async {
      if (!available) return;
      var keyNow = 'A';
      final d = FakeDestination(
        id: 'keyed',
        keyOf: (e) => e.data['id'] == 'a1' ? keyNow : noteKey(e),
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      keyNow = 'Z';
      await w.release(d, 'A', holdId);
      await w.fill(d);
      final released = (await w.releasedItems(d.id)).single;
      expect(released.eventIds, <String>[a1.eventId]);
      expect(released.blockKey, 'A');
      await w.drain(d);
      final again = (await w.heldEvents()).last;
      expect(again.data['key'], 'A');
      expect(again.data['row_id'], released.entryId);
      final lists = await w.keyLists(d.id);
      expect(lists.keys, <String>['A']);
      expect(lists['A']!.state, KeyListState.held);
      expect(lists['A']!.holdId, again.data['hold_id']);
      expect(lists['A']!.events, <KeyListEvent>[
        listEventOf(a1),
        listEventOf(a2),
      ]);
      expect(await w.wedgeRecord(d.id), isNull);
    });

    // Verifies: EVS-DEV-destination-key-hold/K+W
    // Verifies: EVS-DEV-destination-retry-budget/E+F+G
    // Verifies: EVS-PRD-destination-key-hold/F
    test('a release transform failure retries under the per-key record and, '
        'once the budget is spent, re-holds with cause transform_failed and '
        'row_id null, never wedging the destination', () async {
      if (!available) return;
      final d = _ThrowingTransformDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      d
        ..throwing = true
        ..throwsFor = <String>{'a1'};
      final b1 = await w.note('b1', key: 'B');
      final policy = _policy(maxAttempts: 3);
      final itemsBefore = (await w.items(d.id)).length;
      var times = <DateTime>[];
      for (var i = 1; i <= 2; i++) {
        final at = DateTime.utc(2027, 1, 1, 0, i);
        times = <DateTime>[...times, at];
        await w.fill(d, policy: policy, clock: () => at);
        final record = (await w.releaseFailures(d.id))['A']!;
        expect(record.failures, i);
        expect(record.failedAt, times);
        expect(record.lastEventId, a1.eventId);
        expect((await w.keyLists(d.id))['A']!.state, KeyListState.releasing);
      }
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(b1.eventId)),
        hasLength(1),
        reason: "the other key's events keep queuing",
      );
      final itemsAtSpend = await w.items(d.id);
      expect(itemsAtSpend, hasLength(itemsBefore + 1));
      await w.fill(d, policy: policy, clock: () => DateTime.utc(2027, 1, 1, 1));
      expect(await w.releaseFailures(d.id), isEmpty);
      final held = (await w.heldEvents()).last;
      expect(held.data.keys.toSet(), _holdEventKeys);
      expect(held.data['row_id'], isNull);
      expect(held.data['failed_event_id'], a1.eventId);
      expect(held.data['cause'], 'transform_failed');
      expect(held.data['attempt_count'], 3);
      expect(held.data['event_ids'], <String>[a1.eventId, a2.eventId]);
      final newHoldId = held.data['hold_id']! as String;
      expect(newHoldId, isNot(holdId));
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: newHoldId,
          state: KeyListState.held,
          cause: 'transform_failed',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      expect(await w.items(d.id), itemsAtSpend);
      expect(await w.wedgeRecord(d.id), isNull);
      expect(await w.wedgeEvents(), isEmpty);
    });

    // Verifies: EVS-DEV-destination-key-hold/V
    test(
      "release runs while the destination's window is dormant or ended",
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[_keyRefusal],
        );
        await w.activateResolved(d);
        final a1 = await w.note('a1', key: 'A');
        final a2 = await w.note('a2', key: 'A');
        final holdId = await w.holdA(d);
        await w.release(d, 'A', holdId);
        await w.registry.setEndDate(
          d.id,
          DateTime.utc(2026, 1, 2),
          initiator: _init,
        );
        await w.fill(d);
        expect((await w.releasedItems(d.id)).single.eventIds, <String>[
          a1.eventId,
        ]);

        // A dormant destination: registered, never started.
        final dormant = FakeDestination(id: 'dormant', keyOf: noteKey);
        await w.registry.addDestination(dormant, initiator: _init);
        await w.writeFillCursor(dormant.id, a2.sequenceNumber);
        await w.writeKeyLists(dormant.id, <String, KeyList>{
          'A': KeyList(
            holdId: 'dormant-hold',
            state: KeyListState.releasing,
            cause: 'permanent_refusal',
            events: <KeyListEvent>[listEventOf(a2)],
          ),
        });
        await w.fill(dormant);
        final item = (await w.items(dormant.id)).single;
        expect(item.eventIds, <String>[a2.eventId]);
        expect(item.releasedHoldId, 'dormant-hold');
        expect(await w.keyLists(dormant.id), isEmpty);
      },
    );

    // Verifies: EVS-PRD-destination-key-hold/B+E
    test("no other key's delivered event is sent again by a release", () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendOk(),
          const SendOk(),
          const SendOk(),
        ],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      expect(d.sent, hasLength(2), reason: 'a1 refused, b1 sent');
      await w.release(d, 'A', holdId);
      await w.fillAll(d);
      await w.drain(d);
      expect(d.sent, hasLength(4));
      List<Object?> idsOf(WirePayload p) =>
          (jsonDecode(utf8.decode(p.bytes)) as Map)['event_ids'] as List;
      expect(d.sent.skip(2).map(idsOf), <List<Object?>>[
        <Object?>[a1.eventId],
        <Object?>[a2.eventId],
      ]);
      expect(
        d.sent.map(idsOf).where((ids) => ids.contains(b1.eventId)),
        hasLength(1),
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/X
    // Verifies: EVS-DEV-destination-drain/F
    // Verifies: EVS-PRD-destinations/N
    test('a recovery returns swept released items to their key lists and does '
        'not rewind for them', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendOk(),
          const SendPermanent(error: 'refused'),
        ],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      final c1 = await w.note('c1', key: 'C');
      await w.fill(d);
      await w.release(d, 'A', holdId);
      await w.fill(d);
      await w.fill(d);
      expect(await w.releasedItems(d.id), hasLength(2));
      expect(await w.keyLists(d.id), isEmpty);
      await w.drain(d);
      final head = (await w.items(
        d.id,
      )).firstWhere((i) => i.finalStatus == FinalStatus.wedged);
      expect(head.eventIds, <String>[c1.eventId]);

      final result = await w.registry.tombstoneAndRefill(
        d.id,
        head.entryId,
        initiator: _init,
      );
      expect(result.deletedTrailCount, 2);
      expect(result.rewoundTo, c1.sequenceNumber - 1);
      expect(await w.backend.readFillCursor(d.id), c1.sequenceNumber - 1);
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.releasing,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      expect(
        (await w.items(d.id)).where((i) => i.finalStatus == null),
        isEmpty,
      );

      await w.fillAll(d);
      final pending = <FifoEntry>[
        for (final i in await w.items(d.id))
          if (i.finalStatus == null) i,
      ];
      expect(
        pending.where((i) => i.releasedHoldId != null).map((i) => i.eventIds),
        <List<String>>[
          <String>[a1.eventId],
          <String>[a2.eventId],
        ],
      );
      expect(
        pending.where((i) => i.eventIds.contains(c1.eventId)),
        hasLength(1),
      );
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(b1.eventId)),
        hasLength(1),
        reason: "the other key's delivered event is not queued again",
      );
      expect(await w.keyLists(d.id), isEmpty);
    });

    // Verifies: EVS-DEV-destination-key-hold/X
    // Verifies: EVS-DEV-destination-drain/F
    // Verifies: EVS-PRD-destinations/N
    test('a recovery whose wedged head was itself released returns it to its '
        'list', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendOk(),
          const SendPermanent(error: 'refused'),
        ],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      final c1 = await w.note('c1', key: 'C');
      await w.fill(d);
      final head = (await w.releasedItems(d.id)).single;
      expect(head.eventIds, <String>[a1.eventId]);
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(a2),
      ]);
      await w.drain(d);
      expect(
        (await w.backend.readFifoRow(d.id, head.entryId))!.finalStatus,
        FinalStatus.wedged,
      );

      final result = await w.registry.tombstoneAndRefill(
        d.id,
        head.entryId,
        initiator: _init,
      );
      expect(result.rewoundTo, c1.sequenceNumber - 1);
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.releasing,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });

      await w.fillAll(d);
      final pending = <FifoEntry>[
        for (final i in await w.items(d.id))
          if (i.finalStatus == null) i,
      ];
      expect(
        pending.where((i) => i.releasedHoldId != null).map((i) => i.eventIds),
        <List<String>>[
          <String>[a1.eventId],
          <String>[a2.eventId],
        ],
      );
      expect(
        pending.where((i) => i.eventIds.contains(c1.eventId)),
        hasLength(1),
      );
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(b1.eventId)),
        hasLength(1),
      );
    });

    // Verifies: EVS-DEV-destination-key-hold/X
    // Verifies: EVS-DEV-destination-drain/F
    test('with only released items swept, the cursor is not rewound', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendOk(),
          const SendPermanent(error: 'refused'),
        ],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final b1 = await w.note('b1', key: 'B');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      await w.fill(d);
      await w.fill(d);
      final released = await w.releasedItems(d.id);
      expect(released, hasLength(2));
      await w.drain(d);
      final cursorBefore = await w.backend.readFillCursor(d.id);

      final result = await w.registry.tombstoneAndRefill(
        d.id,
        released.first.entryId,
        initiator: _init,
      );
      expect(result.deletedTrailCount, 1);
      expect(result.rewoundTo, cursorBefore);
      expect(await w.backend.readFillCursor(d.id), cursorBefore);
      final recovered = (await w.backend.findAllEvents(
        entryType: kDestinationWedgeRecoveredEntryType,
      )).single;
      expect(recovered.data['rewound_to'], cursorBefore);
      expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
        listEventOf(a1),
        listEventOf(a2),
      ]);
      await w.fillAll(d);
      expect(
        (await w.items(d.id)).where((i) => i.eventIds.contains(b1.eventId)),
        hasLength(1),
      );
    });

    // Verifies: EVS-DEV-destination-drain/F
    // Verifies: EVS-PRD-destinations/N
    test(
      'keyless and gap-replay items still count toward the rewind',
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[
            _keyRefusal,
            const SendPermanent(error: 'refused'),
          ],
        );
        // The window opens after the first note, which a start date moved
        // earlier later lets in through a gap replay.
        await w.activate(d, startDate: DateTime.utc(2026, 3, 1, 0, 1, 30));
        await w.fill(d);
        final x1 = await w.note('x1', key: 'X');
        final a1 = await w.note('a1', key: 'A');
        final a2 = await w.note('a2', key: 'A');
        final holdId = await w.holdA(d);
        await w.release(d, 'A', holdId);
        final n1 = await w.note('n1');
        await w.fill(d);
        await w.registry.setStartDate(
          d.id,
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fill(d);
        final pendingBefore = <FifoEntry>[
          for (final i in await w.items(d.id))
            if (i.finalStatus == null) i,
        ];
        expect(pendingBefore.map((i) => i.eventIds), <List<String>>[
          <String>[a1.eventId],
          <String>[n1.eventId],
          <String>[x1.eventId],
          <String>[a2.eventId],
        ]);
        expect(pendingBefore[1].blockKey, isNull);
        expect(pendingBefore[2].releasedHoldId, isNull);
        expect(pendingBefore[3].releasedHoldId, holdId);
        await w.drain(d);

        final result = await w.registry.tombstoneAndRefill(
          d.id,
          pendingBefore.first.entryId,
          initiator: _init,
        );
        expect(result.rewoundTo, x1.sequenceNumber - 1);
        expect((await w.keyLists(d.id))['A']!.events, <KeyListEvent>[
          listEventOf(a1),
          listEventOf(a2),
        ]);
        await w.fillAll(d);
        final pending = <FifoEntry>[
          for (final i in await w.items(d.id))
            if (i.finalStatus == null) i,
        ];
        for (final e in <StoredEvent>[x1, a1, a2, n1]) {
          expect(
            pending.where((i) => i.eventIds.contains(e.eventId)),
            hasLength(1),
            reason: '${e.data['id']} is queued again once',
          );
        }
      },
    );

    // Verifies: EVS-DEV-destination-key-hold/L+M+Q
    test("a rewind below a list's front inserts the older event in sorted "
        'position and the release waits for the re-walk', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[
          _keyRefusal,
          const SendOk(),
          const SendPermanent(error: 'refused'),
        ],
      );
      await w.activate(d, startDate: DateTime.utc(2026, 3, 1, 0, 1, 30));
      await w.fill(d);
      final x1 = await w.note('x1', key: 'X');
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      await w.fill(d);
      await w.drain(d);
      final sent = (await w.releasedItems(d.id)).single;
      expect(sent.eventIds, <String>[a1.eventId]);
      expect(sent.finalStatus, FinalStatus.sent);
      await w.registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
      await w.fill(d);
      expect(await w.keyLists(d.id), isEmpty);
      expect((await w.releasedItems(d.id)).last.eventIds, <String>[a2.eventId]);
      await w.drain(d);
      final head = (await w.items(
        d.id,
      )).firstWhere((i) => i.finalStatus == FinalStatus.wedged);
      expect(head.eventIds, <String>[x1.eventId]);
      expect(head.finalStatus, FinalStatus.wedged);

      final result = await w.registry.tombstoneAndRefill(
        d.id,
        head.entryId,
        initiator: _init,
      );
      expect(result.rewoundTo, x1.sequenceNumber - 1);
      // The re-walk reaches a1 over several fills, routing it into the list
      // ahead of a2; until it has, the release queues nothing.
      for (var pass = 0; pass < 10; pass++) {
        if (await w.backend.readFillCursor(d.id) >= a1.sequenceNumber) break;
        await w.fill(d);
        expect(
          await w.releasedItems(d.id),
          hasLength(1),
          reason: 'the release waits until the walk has passed the front',
        );
      }
      expect(await w.keyLists(d.id), <String, KeyList>{
        'A': KeyList(
          holdId: holdId,
          state: KeyListState.releasing,
          cause: 'permanent_refusal',
          events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
        ),
      });
      await w.fillAll(d);
      expect(
        (await w.releasedItems(d.id)).map((i) => i.eventIds),
        <List<String>>[
          <String>[a1.eventId],
          <String>[a1.eventId],
          <String>[a2.eventId],
        ],
      );
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.clearedEvents(), hasLength(1));
    });

    // Verifies: EVS-DEV-destination-key-hold/T+U+X
    test(
      'an existing list keeps its state and hold id; a missing list is '
      "created releasing under the item's released_hold_id; emptying it "
      'appends a cleared event only if none exists for that hold id',
      () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'keyed',
          keyOf: noteKey,
          script: <SendResult>[
            _keyRefusal,
            const SendOk(),
            const SendPermanent(error: 'refused'),
            const SendPermanent(error: 'refused'),
            const SendPermanent(error: 'refused'),
          ],
        );
        await w.activateResolved(d);
        final a1 = await w.note('a1', key: 'A');
        await w.note('b1', key: 'B');
        final a2 = await w.note('a2', key: 'A');
        final holdId = await w.holdA(d);
        await w.release(d, 'A', holdId);
        await w.fill(d);

        // An existing list keeps its state and hold id.
        final other = KeyList(
          holdId: 'hold-2',
          state: KeyListState.held,
          cause: 'transform_failed',
          events: <KeyListEvent>[listEventOf(a2)],
        );
        await w.writeKeyLists(d.id, <String, KeyList>{'A': other});
        await w.drain(d);
        var head = (await w.releasedItems(d.id)).single;
        await w.registry.tombstoneAndRefill(
          d.id,
          head.entryId,
          initiator: _init,
        );
        expect(await w.keyLists(d.id), <String, KeyList>{
          'A': KeyList(
            holdId: 'hold-2',
            state: KeyListState.held,
            cause: 'transform_failed',
            events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
          ),
        });

        // A missing list whose hold has no cleared event: re-created
        // releasing, and its emptying appends one.
        await w.writeKeyLists(d.id, <String, KeyList>{
          'A': KeyList(
            holdId: holdId,
            state: KeyListState.releasing,
            cause: 'permanent_refusal',
            events: <KeyListEvent>[listEventOf(a1), listEventOf(a2)],
          ),
        });
        await w.fill(d);
        await w.writeKeyLists(d.id, <String, KeyList>{});
        await w.drain(d);
        head = (await w.releasedItems(d.id)).last;
        expect(head.eventIds, <String>[a1.eventId]);
        expect(head.finalStatus, FinalStatus.wedged);
        await w.registry.tombstoneAndRefill(
          d.id,
          head.entryId,
          initiator: _init,
        );
        expect(await w.keyLists(d.id), <String, KeyList>{
          'A': KeyList(
            holdId: holdId,
            state: KeyListState.releasing,
            cause: 'permanent_refusal',
            events: <KeyListEvent>[listEventOf(a1)],
          ),
        });
        expect(await w.clearedEvents(), isEmpty);
        await w.fill(d);
        expect(await w.keyLists(d.id), isEmpty);
        expect((await w.clearedEvents()).single.data['hold_id'], holdId);

        // A missing list whose hold was cleared: re-created releasing, and its
        // emptying appends no second cleared event.
        await w.drain(d);
        head = (await w.releasedItems(d.id)).last;
        expect(head.finalStatus, FinalStatus.wedged);
        await w.registry.tombstoneAndRefill(
          d.id,
          head.entryId,
          initiator: _init,
        );
        expect((await w.keyLists(d.id))['A'], isNotNull);
        expect((await w.keyLists(d.id))['A']!.state, KeyListState.releasing);
        expect((await w.keyLists(d.id))['A']!.holdId, holdId);
        await w.fillAll(d);
        expect(await w.keyLists(d.id), isEmpty);
        expect(await w.clearedEvents(), hasLength(1));
        expect((await w.releasedItems(d.id)).last.releasedHoldId, holdId);
      },
    );

    // Verifies: EVS-DEV-destination-key-hold-guards/A+C
    // Verifies: EVS-PRD-destination-key-hold/M
    test('a registration that changes the key scheme while key lists exist is '
        'refused, writing only the check record', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.holdA(d);
      final lists = await w.keyLists(d.id);
      final schedule = await w.backend.readSchedule(d.id);
      expect(
        schedule!.keying,
        const DestinationKeying(keyed: true, keyScheme: 'test-v1'),
      );
      final registered = await w.backend.findAllEvents(
        entryType: kDestinationRegisteredEntryType,
      );
      expect(registered.single.data['keyed'], isTrue);
      expect(registered.single.data['key_scheme'], 'test-v1');

      w.a = await w.openProcess();
      await expectLater(
        w.registry.addDestination(
          FakeDestination(id: d.id, keyOf: noteKey, keyScheme: 'test-v2'),
          initiator: _init,
        ),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.toString(),
            'message',
            allOf(contains('test-v1'), contains('test-v2')),
          ),
        ),
      );
      expect(
        (await w.backend.transaction(w.backend.readRegistryCheckTxn))!.outcome,
        'refused_keying_changed',
      );
      expect(w.registry.byId(d.id), isNull);
      expect(await w.backend.readSchedule(d.id), schedule);
      expect(await w.keyLists(d.id), lists);
      expect(
        await w.backend.findAllEvents(
          entryType: kDestinationRegisteredEntryType,
        ),
        hasLength(1),
      );

      // The same keying registers.
      await w.registry.addDestination(
        FakeDestination(id: d.id, keyOf: noteKey),
        initiator: _init,
      );
      expect(w.registry.byId(d.id), isNotNull);
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/B
    test('a keyed destination that names no key scheme is refused before '
        'anything is written', () async {
      if (!available) return;
      final before = await w.backend.findAllEvents();
      for (final scheme in <String?>[null, '']) {
        await expectLater(
          w.registry.addDestination(
            FakeDestination(id: 'keyed', keyOf: noteKey, keyScheme: scheme),
            initiator: _init,
          ),
          throwsA(isA<ArgumentError>()),
        );
      }
      expect(await w.backend.readSchedule('keyed'), isNull);
      expect(await w.backend.findAllEvents(), hasLength(before.length));
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/C+D+E
    // Verifies: EVS-PRD-destination-key-hold/C
    test('a process registered keyed before another registered the '
        'destination unkeyed neither fills nor holds', () async {
      if (!available) return;
      final keyed = FakeDestination(
        id: 'subjects',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(keyed);
      final a = w.a;
      await w.note('a1', key: 'A');
      await w.note('b1', key: 'B');
      await w.fillAll(keyed);
      final queued = await w.items(keyed.id);
      expect(queued.map((i) => i.blockKey), <String?>['A', 'B']);

      // No key lists exist, so another process (a rolling deploy that
      // reverts keying) registers it unkeyed, and its keying is recorded.
      final b = await w.openProcess();
      await b.registry.addDestination(
        FakeDestination(id: keyed.id),
        initiator: _init,
      );
      expect(
        (await w.backend.readSchedule(keyed.id))!.keying,
        DestinationKeying.unkeyed,
      );

      // The keyed process still drains: its key-scoped refusal wedges the
      // destination rather than holding A, which B would fill past.
      w.a = a;
      await w.drain(keyed);
      expect(await w.keyLists(keyed.id), isEmpty);
      expect(await w.heldEvents(), isEmpty);
      expect((await w.items(keyed.id)).first.finalStatus, FinalStatus.wedged);
      expect(
        (await w.wedgeRecord(keyed.id))!.cause,
        WedgeCause.permanentRefusal,
      );

      // And its fill writes nothing under the keying no longer recorded.
      final cursor = await w.backend.transaction(
        (txn) => w.backend.readFillCursorTxn(txn, keyed.id),
      );
      final itemCount = (await w.items(keyed.id)).length;
      await w.note('a2', key: 'A');
      await w.fillAll(keyed);
      expect(await w.items(keyed.id), hasLength(itemCount));
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readFillCursorTxn(txn, keyed.id),
        ),
        cursor,
      );
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/C+D
    // Verifies: EVS-PRD-destination-key-hold/C
    test('a process under the old key scheme does not fill past a hold the '
        'new scheme made', () async {
      if (!available) return;
      final v1 = FakeDestination(id: 'subjects', keyOf: noteKey);
      await w.activateResolved(v1);
      final old = w.a;

      // A second process registers the destination under a new scheme, with
      // a key function that gives A's events key 'P-A', while no list
      // exists, then holds P-A.
      final v2 = FakeDestination(
        id: v1.id,
        keyOf: (e) => noteKey(e) == null ? null : 'P-${noteKey(e)}',
        keyScheme: 'test-v2',
        script: <SendResult>[_keyRefusal],
      );
      w.a = await w.openProcess();
      await w.registry.addDestination(v2, initiator: _init);
      final a1 = await w.note('a1', key: 'A');
      await w.fillAll(v2);
      await w.drain(v2);
      expect((await w.keyLists(v1.id)).keys, <String>['P-A']);

      // The old process's fill would key a2 'A', match no list and queue it
      // ahead of a1: it writes nothing.
      final a2 = await w.note('a2', key: 'A');
      final items = await w.items(v1.id);
      final newProcess = w.a;
      w.a = old;
      await w.fillAll(v1);
      expect(await w.items(v1.id), items);

      // The new process routes it into the list.
      w.a = newProcess;
      await w.fillAll(v2);
      expect(await w.items(v1.id), items);
      expect((await w.keyLists(v1.id))['P-A']!.events, <KeyListEvent>[
        listEventOf(a1),
        listEventOf(a2),
      ]);
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/I
    test('a schedule that records no keying, as on a database migrated in '
        'place, takes the keying of the next registration', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'subjects',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.note('n1');
      await w.fillAll(d);
      final items = await w.items(d.id);
      expect(items, hasLength(2));
      final migrated = (await w.backend.readSchedule(d.id))!;
      await w.backend.transaction(
        (txn) => w.backend.writeScheduleTxn(
          txn,
          d.id,
          DestinationSchedule(
            startDate: migrated.startDate,
            endDate: migrated.endDate,
            registrationId: migrated.registrationId,
            allowHardDelete: migrated.allowHardDelete,
          ),
        ),
      );
      expect((await w.backend.readSchedule(d.id))!.keying, isNull);

      w.a = await w.openProcess();
      await w.registry.addDestination(
        FakeDestination(id: d.id, keyOf: noteKey, keyScheme: 'participant-v1'),
        initiator: _init,
      );
      final recorded = (await w.backend.readSchedule(d.id))!;
      expect(
        recorded.keying,
        const DestinationKeying(keyed: true, keyScheme: 'participant-v1'),
      );
      expect(recorded.registrationId, migrated.registrationId);
      expect(recorded.startDate, migrated.startDate);
      expect(await w.items(d.id), items);
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/H
    test("a re-hold by a refusal removes the key's release failure record, "
        'so the new release gets the whole budget', () async {
      if (!available) return;
      final d = _ThrowingTransformDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      await w.activateResolved(d);
      final a1 = await w.note('a1', key: 'A');
      final a2 = await w.note('a2', key: 'A');
      final holdId = await w.holdA(d);
      await w.release(d, 'A', holdId);
      final policy = _policy(maxAttempts: 2);
      final t0 = DateTime.utc(2027, 1, 1);
      await w.fill(d, policy: policy, clock: () => t0);
      final released = (await w.releasedItems(d.id)).single;
      expect(released.eventIds, <String>[a1.eventId]);
      d
        ..throwing = true
        ..throwsFor = <String>{'a2'};
      final t1 = t0.add(const Duration(minutes: 1));
      await w.fill(d, policy: policy, clock: () => t1);
      expect((await w.releaseFailures(d.id))['A']!.failures, 1);

      // The released a1 is refused: A is held again, with no record.
      await w.drain(d, policy: policy);
      final rehold = (await w.heldEvents()).last;
      final newHoldId = rehold.data['hold_id']! as String;
      expect(newHoldId, isNot(holdId));
      expect(await w.releaseFailures(d.id), isEmpty);

      // One failure of the new release does not spend a budget of two.
      await w.release(d, 'A', newHoldId, requestId: 'request-2');
      d.throwsFor = <String>{'a1'};
      final t2 = t1.add(const Duration(minutes: 1));
      await w.fill(d, policy: policy, clock: () => t2);
      expect((await w.releaseFailures(d.id))['A']!.failures, 1);
      final list = (await w.keyLists(d.id))['A']!;
      expect(list.state, KeyListState.releasing);
      expect(list.holdId, newHoldId);
      expect(list.events, <KeyListEvent>[listEventOf(a1), listEventOf(a2)]);
      expect(await w.heldEvents(), hasLength(2));
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/F
    test('a hold that throws rolls back and wedges the head '
        'destination-wide', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal],
      );
      await w.activateResolved(d);
      await w.note('a1', key: 'A');
      await w.note('a2', key: 'A');
      await w.fillAll(d);
      final queued = await w.items(d.id);
      expect(queued, hasLength(2));
      // An item behind the head that carries attempts makes the hold's
      // deletion of A's pending items throw.
      await w.backend.transaction(
        (txn) => w.backend.appendAttemptTxn(
          txn,
          d.id,
          queued[1].entryId,
          AttemptResult(
            attemptedAt: DateTime.utc(2027),
            outcome: 'transient',
            errorMessage: 'an attempt the queue should not hold',
          ),
        ),
      );
      await w.drain(d);
      final items = await w.items(d.id);
      expect(items.first.finalStatus, FinalStatus.wedged);
      expect(items.first.attempts, hasLength(1));
      expect(items.first.attempts.single.scope, WedgeScope.key);
      expect(items[1].finalStatus, isNull);
      expect((await w.wedgeRecord(d.id))!.cause, WedgeCause.permanentRefusal);
      expect(await w.wedgeEvents(), hasLength(1));
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.heldEvents(), isEmpty);
    });

    // Verifies: EVS-DEV-destination-key-hold-guards/G
    test('a recovery that cannot return a released item to its list is '
        'refused, writing only the check record', () async {
      if (!available) return;
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[const SendPermanent(error: 'refused')],
      );
      await w.activateResolved(d);
      await w.note('n1');
      await w.fillAll(d);
      final a1 = await w.note('a1', key: 'A');
      // A released item whose hold the log has no event for.
      await w.backend.transaction(
        (txn) => w.backend.enqueueFifoTxn(
          txn,
          d.id,
          <StoredEvent>[a1],
          wirePayload: WirePayload(
            bytes: Uint8List.fromList(utf8.encode('{}')),
            contentType: d.wireFormat,
            transformVersion: null,
          ),
          blockKey: 'A',
          releasedHoldId: 'no-such-hold',
        ),
      );
      await w.drain(d);
      final head = (await w.items(d.id)).first;
      expect(head.finalStatus, FinalStatus.wedged);
      final before = await w.items(d.id);
      final cursor = await w.backend.transaction(
        (txn) => w.backend.readFillCursorTxn(txn, d.id),
      );
      await expectLater(
        w.registry.tombstoneAndRefill(d.id, head.entryId, initiator: _init),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            allOf(contains('no-such-hold'), contains('deleteDestination')),
          ),
        ),
      );
      expect(
        (await w.backend.transaction(w.backend.readRegistryCheckTxn))!.outcome,
        'refused_released_item_unreturnable',
      );
      expect(await w.items(d.id), before);
      expect(await w.keyLists(d.id), isEmpty);
      expect(await w.wedgeRecord(d.id), isNotNull);
      expect(
        await w.backend.transaction(
          (txn) => w.backend.readFillCursorTxn(txn, d.id),
        ),
        cursor,
      );
    });

    // Verifies: EVS-DEV-destination-drain/G
    // Verifies: EVS-DEV-destination-key-hold/H+L
    // Verifies: EVS-PRD-destination-key-hold/B+C
    test('a fill racing a hold keeps each key in order and enqueues each '
        'event once', () async {
      if (!available) return;
      for (var round = 0; round < 4; round++) {
        final d = FakeDestination(
          id: 'race-hold-$round',
          keyOf: noteKey,
          script: <SendResult>[
            _keyRefusal,
            for (var i = 0; i < 20; i++) const SendOk(),
          ],
        );
        await w.activateFromNow(d);
        final notes = <StoredEvent>[
          await w.note('a1-$round', key: 'A'),
          await w.note('b1-$round', key: 'B'),
        ];
        await w.fillAll(d, passes: 2);
        notes.addAll(<StoredEvent>[
          await w.note('a2-$round', key: 'A'),
          await w.note('b2-$round', key: 'B'),
          await w.note('a3-$round', key: 'A'),
        ]);
        await w.race(<Future<void> Function()>[
          () => w.fillAll(d, passes: 4),
          () => w.drain(d),
          () => w.fillAll(d, passes: 4),
        ]);
        await w.expectPerKeyOrder(d.id, notes);
        await w.fillAll(d);
        await w.expectPerKeyOrder(d.id, notes, settled: true);
        expect(
          (await w.keyLists(d.id))['A']!.events.map((e) => e.eventId),
          <String>[notes[0].eventId, notes[2].eventId, notes[4].eventId],
        );
        expect(await w.heldEvents(), hasLength(round + 1));
      }
    });

    // Verifies: EVS-DEV-destination-drain/G
    // Verifies: EVS-DEV-destination-key-hold/N+O+Q
    // Verifies: EVS-PRD-destination-key-hold/C+E
    test('a fill racing a release from another process keeps each key in '
        'order and enqueues each event once', () async {
      if (!available) return;
      for (var round = 0; round < 4; round++) {
        final d = FakeDestination(
          id: 'race-release-$round',
          keyOf: noteKey,
          script: <SendResult>[_keyRefusal],
        );
        await w.activateFromNow(d);
        final notes = <StoredEvent>[await w.note('a1-$round', key: 'A')];
        final holdId = await w.holdA(d);
        notes.addAll(<StoredEvent>[
          await w.note('a2-$round', key: 'A'),
          await w.note('b1-$round', key: 'B'),
          await w.note('a3-$round', key: 'A'),
        ]);
        final other = await w.openProcess();
        final releases = <ReleaseHoldResult>[];
        await w.race(<Future<void> Function()>[
          () => w.fillAll(d, passes: 4),
          () async {
            releases.add(
              await other.registry.releaseHold(
                d.id,
                'A',
                holdId: holdId,
                initiator: _init,
                requestId: 'race-$round',
              ),
            );
          },
          () => w.fillAll(d, passes: 4),
        ]);
        expect(releases.single, isA<ReleaseHoldApplied>());
        await w.expectPerKeyOrder(d.id, notes);
        await w.fillAll(d);
        await w.expectPerKeyOrder(d.id, notes, settled: true);
        expect(await w.keyLists(d.id), isEmpty);
        // A's events reach the queue in order: a1 by the release, and a2
        // and a3 by the release too or, once the list emptied ahead of
        // them, by the fill.
        final queuedA = <String>[
          for (final i in await w.items(d.id))
            if (i.blockKey == 'A' && i.finalStatus == null) ...i.eventIds,
        ];
        expect(queuedA, <String>[
          notes[0].eventId,
          notes[1].eventId,
          notes[3].eventId,
        ]);
        expect((await w.releasedItems(d.id)).first.eventIds, <String>[
          notes[0].eventId,
        ]);
      }
    });

    // Verifies: EVS-DEV-destination-drain/F+G
    // Verifies: EVS-DEV-destination-key-hold/L+X
    test('a fill racing a recovery from another process keeps each key in '
        'order and enqueues each event once', () async {
      if (!available) return;
      for (var round = 0; round < 4; round++) {
        final d = FakeDestination(
          id: 'race-recover-$round',
          keyOf: noteKey,
          script: <SendResult>[const SendPermanent(error: 'refused')],
        );
        await w.activateFromNow(d);
        final notes = <StoredEvent>[
          await w.note('b1-$round', key: 'B'),
          await w.note('a1-$round', key: 'A'),
          await w.note('b2-$round', key: 'B'),
        ];
        await w.fillAll(d);
        await w.drain(d);
        final head = (await w.items(d.id)).first;
        expect(head.finalStatus, FinalStatus.wedged);
        notes.addAll(<StoredEvent>[
          await w.note('a2-$round', key: 'A'),
          await w.note('b3-$round', key: 'B'),
        ]);
        final other = await w.openProcess();
        await w.race(<Future<void> Function()>[
          () => w.fillAll(d, passes: 4),
          () => other.registry
              .tombstoneAndRefill(d.id, head.entryId, initiator: _init)
              .then((_) {}),
          () => w.fillAll(d, passes: 4),
        ]);
        await w.fillAll(d);
        await w.expectPerKeyOrder(d.id, notes, settled: true);
        final pending = <String>[
          for (final i in await w.items(d.id))
            if (i.finalStatus == null) ...i.eventIds,
        ];
        expect(pending, <String>[for (final n in notes) n.eventId]);
      }
    });

    // Verifies: EVS-DEV-destination-drain/A+G
    test(
      'a fill racing a deletion from another process enqueues nothing '
      'after the deletion, and keeps each key in order when it wins',
      () async {
        if (!available) return;
        var deletions = 0;
        for (var round = 0; round < 4; round++) {
          final d = FakeDestination(
            id: 'race-delete-$round',
            keyOf: noteKey,
            allowHardDelete: true,
            script: <SendResult>[_keyRefusal],
          );
          await w.activateFromNow(d);
          final notes = <StoredEvent>[await w.note('a1-$round', key: 'A')];
          await w.holdA(d);
          // In even rounds only the held key's events arrive, which the fill
          // routes into its list, so the queue holds no pending head and the
          // deletion always proceeds. In odd rounds another key's events
          // arrive too, and deletion refuses a pending head, so a fill that
          // queues b1 first wins: the destination stays, in order.
          notes.addAll(<StoredEvent>[
            await w.note('a2-$round', key: 'A'),
            if (round.isOdd) await w.note('b1-$round', key: 'B'),
            if (round.isOdd) await w.note('b2-$round', key: 'B'),
          ]);
          final other = await w.openProcess();
          var deleted = true;
          await w.race(<Future<void> Function()>[
            () => w.fillAll(d, passes: 4),
            () async {
              try {
                await other.registry.deleteDestination(d.id, initiator: _init);
                // A refusal is a StateError the registry throws by design.
                // ignore: avoid_catching_errors
              } on StateError catch (e) {
                expect(e.message, contains('is pending'));
                deleted = false;
              }
            },
            () => w.fillAll(d, passes: 4),
          ]);
          await w.fillAll(d);
          if (!deleted) {
            expect(await w.backend.readSchedule(d.id), isNotNull);
            await w.expectPerKeyOrder(d.id, notes, settled: true);
            continue;
          }
          deletions += 1;
          expect(await w.backend.readSchedule(d.id), isNull);
          expect(await w.keyLists(d.id), isEmpty);
          expect(await w.releaseFailures(d.id), isEmpty);
          expect(
            (await w.items(d.id)).where((i) => i.finalStatus == null),
            isEmpty,
            reason: 'no fill commits an item after the deletion',
          );
        }
        expect(deletions, greaterThanOrEqualTo(2));
      },
    );

    // Verifies: EVS-DEV-destination-key-hold/H+K
    // Verifies: EVS-DEV-destination-drain/C
    test('a hold re-run after a serialization failure appends one hold event '
        'under one hold id', () async {
      if (!available) return;
      if (!serializableConflicts) {
        markTestSkipped('$label does not abort a concurrent update');
        return;
      }
      final d = FakeDestination(
        id: 'keyed',
        keyOf: noteKey,
        script: <SendResult>[_keyRefusal, _keyRefusal],
      );
      await w.activateResolved(d);
      // Z is held first, so the key lists row exists for the concurrent
      // writer to update.
      await w.note('z1', key: 'Z');
      await w.fillAll(d);
      await w.drain(d);
      final lists = await w.keyLists(d.id);
      expect(lists.keys, <String>['Z']);
      final a1 = await w.note('a1', key: 'A');
      await w.fillAll(d);
      final head = (await w.items(d.id)).last;
      expect(head.eventIds, <String>[a1.eventId]);

      final other = await w.openProcess();
      var armed = false;
      var conflicts = 0;
      var holdRuns = 0;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterSendBeforeOutcome: (_) async => armed = true,
          beforeQueueWrites: (_) async {
            if (!armed) return;
            armed = false;
            conflicts += 1;
            // Commits, from another connection, an update of the key lists
            // row the hold's transaction reads and then writes.
            await other.backend.transaction(
              (txn) => other.backend.writeKeyListsTxn(txn, d.id, lists),
            );
          },
          onRegistryBodyRun: (op) {
            if (op == 'holdKeyInTxn') holdRuns += 1;
          },
        ),
        () => w.drain(d),
      );
      expect(conflicts, 1);
      expect(holdRuns, 2, reason: 'the hold ran again after the abort');
      final held = (await w.heldEvents())
          .where((e) => e.data['key'] == 'A')
          .toList();
      expect(held, hasLength(1));
      final after = await w.keyLists(d.id);
      expect(after['A']!.holdId, held.single.data['hold_id']);
      expect(after['A']!.events, <KeyListEvent>[listEventOf(a1)]);
      expect(after['Z'], lists['Z']);
      final tombstoned = (await w.items(
        d.id,
      )).singleWhere((i) => i.entryId == head.entryId);
      expect(tombstoned.finalStatus, FinalStatus.tombstoned);
      expect(tombstoned.attempts, hasLength(1));
    });
  });
}

/// A keyed [FakeDestination] whose transform throws while [throwing], on
/// every batch or, when [throwsFor] is not empty, on a batch holding an
/// event whose data `id` it names.
class _ThrowingTransformDestination extends FakeDestination {
  _ThrowingTransformDestination({required super.id, super.keyOf, super.script});

  bool throwing = false;
  Set<String> throwsFor = <String>{};

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    if (throwing &&
        (throwsFor.isEmpty ||
            batch.any((e) => throwsFor.contains(e.data['id'])))) {
      transformCalls += 1;
      return Future<WirePayload>.error(StateError('scripted transform'));
    }
    return super.transform(batch);
  }
}
