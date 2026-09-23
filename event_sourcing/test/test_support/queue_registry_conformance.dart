// Backend-agnostic scenarios for the destination registry's operations on a
// queue and for the fill and drain transactions that change it: recovery,
// deletion and re-registration, the hard-delete opt-in in effect, operations
// run from a registry that does not register the destination, the registry
// check record, rollback of every multi-write transaction, replay requests,
// the fill's compare-and-set, and at-least-once re-sends. Sembast runs them
// from test/destinations/queue_registry_sembast_test.dart and Postgres from
// test/storage/postgres/postgres_queue_registry_test.dart.
//
// This file exposes one function, [runQueueRegistryScenarios], and registers
// no `main()` of its own. Traceability lives on the individual tests.
import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fake_destination.dart';
import 'fifo_entry_helpers.dart';
import 'queue_test_support.dart';

/// One test database for the scenarios.
abstract class QueueTestDatabase {
  /// Open a backend. The first call in a test opens a fresh, empty
  /// database; every later call in the same test opens another backend over
  /// that same database (another process).
  Future<StorageBackend> openBackend();

  /// The security-context store beside [backend]'s events.
  MutableSecurityContextStore securityFor(StorageBackend backend);

  /// Every key in the database's `backend_state` records.
  Future<Set<String>> backendStateKeys();

  /// Close everything the test opened.
  Future<void> close();
}

const Initiator _init = AutomationInitiator(service: 'queue-scenarios');
const String _noteType = 'queue_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'queue-install',
  softwareVersion: 'test@1.0.0',
);

/// The fill clock: after every event the scenarios append.
DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// A destination whose transform tags its payload, so a test can tell which
/// registry's configuration built a queue item.
class _TaggedDestination extends FakeDestination {
  _TaggedDestination({required super.id, required this.tag});

  final String tag;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) {
    transformCalls += 1;
    return Future<WirePayload>.value(
      WirePayload(
        bytes: Uint8List.fromList(
          utf8.encode(
            jsonEncode(<String, Object?>{
              'tag': tag,
              'event_ids': batch.map((e) => e.eventId).toList(),
            }),
          ),
        ),
        contentType: 'application/json',
        transformVersion: 'tagged-v1',
      ),
    );
  }
}

/// One process in a scenario: a backend, an event store over it and a
/// registry.
class _Process {
  _Process(this.backend, this.store, this.registry);
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

class _World {
  _World(this.db);
  final QueueTestDatabase db;

  /// Client timestamp stamped on the next appended event.
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
      ),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: db.securityFor(backend),
      clock: () => eventTime,
    );
    return _Process(
      backend,
      store,
      DestinationRegistry(backend: backend, eventStore: store),
    );
  }

  Future<void> open() async {
    a = await openProcess();
  }

  /// Append a user event stamped [at] (or [eventTime]).
  Future<StoredEvent> note(String id, {DateTime? at}) async {
    if (at != null) eventTime = at;
    final event = await a.store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'noted',
      data: <String, Object?>{'id': id},
      initiator: const UserInitiator('u'),
    );
    return event!;
  }

  Future<void> fill(Destination d, {StorageBackend? on}) =>
      fillBatch(d, backend: on ?? backend, source: _source, clock: _fillNow);

  /// Run fills until one enqueues nothing new (at most [max] passes).
  Future<void> fillAll(
    Destination d, {
    StorageBackend? on,
    int max = 20,
  }) async {
    final b = on ?? backend;
    for (var i = 0; i < max; i++) {
      final before = (await b.listFifoEntries(d.id)).length;
      final cursorBefore = await b.readFillCursor(d.id);
      await fill(d, on: b);
      final after = (await b.listFifoEntries(d.id)).length;
      if (after == before && await b.readFillCursor(d.id) == cursorBefore) {
        return;
      }
    }
  }

  /// The note ids of the note events carried by the queue's items whose
  /// status is [status], in queue order.
  Future<List<String>> eventsIn(String destId, {FinalStatus? status}) async {
    final noteIds = <String, String>{
      for (final e in await backend.findAllEvents())
        if (e.entryType == _noteType) e.eventId: e.data['id'] as String,
    };
    final rows = await backend.listFifoEntries(destId);
    return <String>[
      for (final r in rows)
        if (r.finalStatus == status)
          for (final id in r.eventIds)
            if (noteIds.containsKey(id)) noteIds[id]!,
    ];
  }

  /// Every item as (note id of its first note event, status), in queue
  /// order.
  Future<List<(String, FinalStatus?)>> items(String destId) async {
    final noteIds = <String, String>{
      for (final e in await backend.findAllEvents())
        if (e.entryType == _noteType) e.eventId: e.data['id'] as String,
    };
    return <(String, FinalStatus?)>[
      for (final r in await backend.listFifoEntries(destId))
        (noteIds[r.eventIds.first] ?? r.eventIds.first, r.finalStatus),
    ];
  }

  /// The note ids carried by pending items, in queue order.
  Future<List<String>> pendingNotes(String destId) => eventsIn(destId);

  Future<List<StoredEvent>> audits(String entryType) async => <StoredEvent>[
    for (final e in await backend.findAllEvents())
      if (e.entryType == entryType) e,
  ];

  Future<ReplayRequest?> request(String destId) =>
      backend.transaction((txn) => backend.readReplayRequestTxn(txn, destId));

  Future<RegistryCheck?> check() =>
      backend.transaction(backend.readRegistryCheckTxn);

  /// Everything a registry operation could change about [destId], and the
  /// log, except the registry check record.
  Future<Map<String, Object?>> snapshot(String destId) async => {
    'rows': <Object?>[
      for (final r in await backend.listFifoEntries(destId)) r.toJson(),
    ],
    'schedule': (await backend.readSchedule(destId))?.toJson(),
    'cursor': await backend.readFillCursor(destId),
    'request': (await request(destId))?.toJson(),
    'events': <String>[
      for (final e in await backend.findAllEvents()) e.eventId,
    ],
    'state_keys': <String>[
      for (final k in await db.backendStateKeys())
        if (k != 'registry_check') k,
    ]..sort(),
  };

  /// Assert [snapshot] equals [before] and the registry check record names
  /// [op] and [outcome].
  Future<void> expectOnlyCheckWritten(
    String destId,
    Map<String, Object?> before, {
    required String op,
    required String outcome,
  }) async {
    expect(await snapshot(destId), before);
    final written = await check();
    expect(written?.op, op);
    expect(written?.destinationId, destId);
    expect(written?.outcome, outcome);
  }
}

/// Run every queue-registry scenario against a database [databaseFactory]
/// builds fresh for each test (a null database skips the test).

void runQueueRegistryScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('queue registry scenarios ($label)', () {
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
      await w.open();
    });

    tearDown(() async {
      if (!available) return;
      await w.db.close();
    });

    /// Register [d] on [w.registry], activate it at [start] and fill.
    Future<void> activate(Destination d, {DateTime? start}) async {
      await w.registry.addDestination(d, initiator: _init);
      await w.registry.setStartDate(
        d.id,
        start ?? DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
    }

    // ------------------------------------------------------------------
    // Recovery
    // ------------------------------------------------------------------

    group('recovery', () {
      // Verifies: EVS-PRD-destinations/M
      // recovery of a pending head is refused;
      //   the row stays pending, the cursor and trail are unchanged and no
      //   recovery event is appended.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal writes only the registry
      //   check record and throws after the commit.
      test('a pending head is refused and nothing else changes', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        await activate(d);
        await w.note('n1');
        await w.note('n2');
        await w.fillAll(d);
        final head = await w.backend.readFifoHead('r');
        expect(head!.finalStatus, isNull);
        final before = await w.snapshot('r');
        await expectLater(
          w.registry.tombstoneAndRefill('r', head.entryId, initiator: _init),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains('wedged head'),
            ),
          ),
        );
        await w.expectOnlyCheckWritten(
          'r',
          before,
          op: 'tombstoneAndRefill',
          outcome: 'refused_pending_head',
        );
        expect(await w.audits(kDestinationWedgeRecoveredEntryType), isEmpty);
      });

      // Verifies: EVS-PRD-destinations/M
      // a target that is not the head is
      //   refused with nothing written but the check record.
      test('a target that is not the head is refused', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        await activate(d);
        await w.note('n1');
        await w.note('n2');
        await w.fillAll(d);
        await wedgeHeadForTest(w.backend, 'r');
        final trail = (await w.backend.listFifoEntries('r')).last;
        final before = await w.snapshot('r');
        await expectLater(
          w.registry.tombstoneAndRefill('r', trail.entryId, initiator: _init),
          throwsArgumentError,
        );
        await w.expectOnlyCheckWritten(
          'r',
          before,
          op: 'tombstoneAndRefill',
          outcome: 'refused_not_head',
        );
      });

      // Verifies: EVS-PRD-destinations/N
      // recovery rewinds below the lowest event
      //   of every removed item, including a gap replay's item whose events
      //   lie below the wedged head's, and the next fill re-enqueues every
      //   removed event.
      // Verifies: EVS-DEV-destination-drain/F
      // the recovery reads the head, sweeps
      //   and rewinds in one transaction and records rewound_to.
      test('rewinds below a swept gap-replay item; the refill re-enqueues '
          'every removed event', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        final early = await w.note('early', at: DateTime.utc(2026, 1, 5));
        await w.note('mid', at: DateTime.utc(2026, 2, 5));
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.fillAll(d);
        // 'mid' is enqueued; 'early' precedes the start date.
        expect(await w.pendingNotes('r'), ['mid']);
        // Moving the start date earlier while the head is pending records a
        // gap replay, whose item for 'early' lands behind 'mid'.
        await w.registry.setStartDate(
          'r',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(d);
        expect(await w.pendingNotes('r'), ['mid', 'early']);
        await wedgeHeadForTest(w.backend, 'r');
        final head = (await w.backend.readFifoHead('r'))!;
        final result = await w.registry.tombstoneAndRefill(
          'r',
          head.entryId,
          initiator: _init,
        );
        expect(result.deletedTrailCount, 1);
        expect(result.rewoundTo, early.sequenceNumber - 1);
        final audit = (await w.audits(
          kDestinationWedgeRecoveredEntryType,
        )).last;
        expect(audit.data['rewound_to'], early.sequenceNumber - 1);
        expect(audit.data['deleted_trail_count'], 1);
        await w.fillAll(d);
        // Both removed events are enqueued again, each once.
        expect(await w.pendingNotes('r'), ['early', 'mid']);
      });

      // Verifies: EVS-PRD-destinations/N
      // the refill evaluates the removed events
      //   against the filter of the destination the drainer registers.
      test("the refill uses the drainer registry's narrowed filter", () async {
        if (!available) return;
        final wide = FakeDestination(id: 'r');
        await activate(wide);
        await w.note('keep-1');
        await w.note('drop-1');
        await w.note('keep-2');
        await w.fillAll(wide);
        await wedgeHeadForTest(w.backend, 'r');
        await w.registry.tombstoneAndRefill(
          'r',
          (await w.backend.readFifoHead('r'))!.entryId,
          initiator: _init,
        );
        // The drainer now registers the destination with a narrower filter.
        final narrow = FakeDestination(
          id: 'r',
          filter: SubscriptionFilter(
            predicate: (e) => (e.data['id'] as String).startsWith('keep'),
          ),
        );
        await w.fillAll(narrow);
        expect(await w.pendingNotes('r'), ['keep-1', 'keep-2']);
      });

      // Verifies: EVS-DEV-destination-drain/F
      // a gap replay committed between the
      //   recovery call and its transaction is still re-enqueued: the
      //   recovery reads the head and sweeps inside its transaction.
      test('a gap replay committed before the recovery transaction is '
          're-enqueued', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        await w.note('early', at: DateTime.utc(2026, 1, 5));
        await w.note('mid', at: DateTime.utc(2026, 2, 5));
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.fillAll(d);
        await wedgeHeadForTest(w.backend, 'r');
        final head = (await w.backend.readFifoHead('r'))!;
        // The gap replay's item lands behind the wedged head (set up through
        // the storage contract: the registry refuses a backward move while
        // the head is wedged, and the fill writes nothing behind a wedged
        // head).
        var ran = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeRegistryTransaction: (op) async {
              if (ran || op != 'tombstoneAndRefill') return;
              ran = true;
              final events = await w.backend.findAllEvents();
              final early = events.firstWhere((e) => e.data['id'] == 'early');
              await w.backend.transaction(
                (txn) => w.backend.enqueueFifoTxn(txn, 'r', <StoredEvent>[
                  early,
                ], wirePayload: wirePayloadJson(const {'gap': true})),
              );
            },
          ),
          () => w.registry.tombstoneAndRefill(
            'r',
            head.entryId,
            initiator: _init,
          ),
        );
        expect(ran, isTrue);
        final early = (await w.backend.findAllEvents()).firstWhere(
          (e) => e.data['id'] == 'early',
        );
        expect(await w.backend.readFillCursor('r'), early.sequenceNumber - 1);
        await w.registry.setStartDate(
          'r',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(d);
        expect(await w.pendingNotes('r'), ['early', 'mid']);
      });

      // Verifies: EVS-PRD-destinations/N
      // a rewind below a sent item re-sends
      //   that item's events (at-least-once) and keeps the sent item.
      test('a rewind below a sent item enqueues its events again', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'r',
          script: <SendResult>[const SendOk()],
        );
        await w.note('early', at: DateTime.utc(2026, 1, 5));
        await w.note('sent-1', at: DateTime.utc(2026, 2, 5));
        await w.note('mid', at: DateTime.utc(2026, 2, 6));
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.fillAll(d);
        // Deliver sent-1; mid stays pending at the head.
        await drain(
          FakeDestination(id: 'r', script: <SendResult>[const SendOk()]),
          backend: w.backend,
        );
        expect(await w.eventsIn('r', status: FinalStatus.sent), ['sent-1']);
        // Gap replay of 'early' lands behind 'mid'.
        await w.registry.setStartDate(
          'r',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(d);
        expect(await w.pendingNotes('r'), ['mid', 'early']);
        await wedgeHeadForTest(w.backend, 'r');
        await w.registry.tombstoneAndRefill(
          'r',
          (await w.backend.readFifoHead('r'))!.entryId,
          initiator: _init,
        );
        await w.fillAll(d);
        // sent-1 lies above the rewind point, so it is enqueued again; the
        // sent item is kept.
        expect(await w.pendingNotes('r'), ['early', 'sent-1', 'mid']);
        expect(await w.eventsIn('r', status: FinalStatus.sent), ['sent-1']);
      });

      // Verifies: EVS-PRD-destinations/M
      // an injected failure after the
      //   recovery's last write rolls back the tombstone, the sweep and the
      //   rewind, and no recovery event is appended.
      // Verifies: EVS-PRD-destinations/N
      // on success the head is tombstoned,
      //   the trail swept, the position rewound and one event appended.
      // Verifies: EVS-DEV-destination-drain/F
      // the recovery transaction is atomic.
      test('recovery rolls back on an injected failure; succeeds '
          'otherwise', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        await activate(d);
        await w.note('n1');
        await w.note('n2');
        await w.note('n3');
        await w.fillAll(d);
        final headId = await wedgeHeadForTest(w.backend, 'r');
        final before = await w.snapshot('r');
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) =>
                  t == kDestinationWedgeRecoveredEntryType,
            ),
            () => w.registry.tombstoneAndRefill('r', headId, initiator: _init),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot('r'), before);

        final result = await w.registry.tombstoneAndRefill(
          'r',
          headId,
          initiator: _init,
        );
        expect(result.deletedTrailCount, 2);
        final rows = await w.backend.listFifoEntries('r');
        expect(rows.map((r) => r.finalStatus), [FinalStatus.tombstoned]);
        final n1 = (await w.backend.findAllEvents()).firstWhere(
          (e) => e.data['id'] == 'n1',
        );
        expect(await w.backend.readFillCursor('r'), n1.sequenceNumber - 1);
        expect(
          await w.audits(kDestinationWedgeRecoveredEntryType),
          hasLength(1),
        );
      });

      // Verifies: EVS-DEV-destination-drain/E
      // recovery while a gap request is
      //   pending: the request survives, is bounded by the rewound position,
      //   and no event lands in two items.
      test('recovery while a gap request is pending enqueues no event '
          'twice', () async {
        if (!available) return;
        final d = FakeDestination(id: 'r');
        await w.note('early', at: DateTime.utc(2026, 1, 5));
        await w.note('mid', at: DateTime.utc(2026, 2, 5));
        await w.note('late', at: DateTime.utc(2026, 2, 6));
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.fillAll(d);
        // Backward move while the head is pending: a gap request.
        await w.registry.setStartDate(
          'r',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        expect((await w.request('r'))?.gapUpper, DateTime.utc(2026, 2, 1));
        // The drainer wedges the head before any fill performs the request.
        await wedgeHeadForTest(w.backend, 'r');
        await w.registry.tombstoneAndRefill(
          'r',
          (await w.backend.readFifoHead('r'))!.entryId,
          initiator: _init,
        );
        expect(await w.request('r'), isNotNull);
        await w.fillAll(d);
        final pending = await w.pendingNotes('r');
        expect(pending.toSet(), {'early', 'mid', 'late'});
        expect(pending, hasLength(3));
      });
    });

    // ------------------------------------------------------------------
    // Deletion and re-registration
    // ------------------------------------------------------------------

    group('deletion', () {
      // Verifies: EVS-PRD-destinations/O
      // deletion is refused while the head is
      //   pending; nothing changes and no event is appended.
      // Verifies: EVS-DEV-destination-drain/A
      // the deletion reads the head in its
      //   transaction and refuses a pending head.
      test('a pending head is refused and nothing else changes', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        final before = await w.snapshot('x');
        await expectLater(
          w.registry.deleteDestination('x', initiator: _init),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              contains('wedged head'),
            ),
          ),
        );
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'deleteDestination',
          outcome: 'refused_pending_head',
        );
        expect(w.registry.byId('x'), isNotNull);
      });

      // Verifies: EVS-PRD-destinations/O
      // deletion retains every delivered,
      //   wedged or recovered item and records what it removed.
      // Verifies: EVS-DEV-destination-drain/A
      // the wedged head is tombstoned, the
      //   pending items deleted, the cursor, schedule and replay request
      //   removed, and the only per-destination record left is the
      //   sequence_in_queue counter.
      test('retains sent items, tombstones the wedged head, removes pending '
          'items and every per-destination record but the counter', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.note('s1');
        await w.note('h1');
        await w.note('p1');
        await w.note('p2');
        await w.fillAll(d);
        await drain(
          FakeDestination(id: 'x', script: <SendResult>[const SendOk()]),
          backend: w.backend,
        );
        // A pending replay request exists at deletion time (recorded while
        // the head was still pending).
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2025, 1, 1),
          initiator: _init,
        );
        expect(await w.request('x'), isNotNull);
        final headId = await wedgeHeadForTest(w.backend, 'x');
        final keysBefore = await w.db.backendStateKeys();
        expect(
          keysBefore.where((k) => k.endsWith('_x')).toSet(),
          containsAll(<String>[
            'fill_cursor_x',
            'schedule_x',
            'replay_request_x',
            'fifo_seq_counter_x',
          ]),
        );

        await w.registry.deleteDestination('x', initiator: _init);

        final rows = await w.backend.listFifoEntries('x');
        expect(await w.items('x'), [
          ('s1', FinalStatus.sent),
          ('h1', FinalStatus.tombstoned),
        ]);
        expect(rows[1].entryId, headId);
        expect(await w.backend.readSchedule('x'), isNull);
        expect(await w.backend.readFillCursor('x'), -1);
        expect(await w.request('x'), isNull);
        final keys = await w.db.backendStateKeys();
        expect(keys.where((k) => k.endsWith('_x')).toList(), [
          'fifo_seq_counter_x',
        ]);
        final audit = (await w.audits(kDestinationDeletedEntryType)).single;
        expect(audit.data['tombstoned_row_id'], headId);
        expect(audit.data['deleted_pending_count'], 2);
        expect(audit.data['allow_hard_delete'], isTrue);
        expect(w.registry.byId('x'), isNull);
      });

      // Verifies: EVS-PRD-destinations/O
      // deletion of an empty queue succeeds.
      test('an empty queue is deleted', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await w.registry.addDestination(d, initiator: _init);
        await w.registry.deleteDestination('x', initiator: _init);
        expect(await w.backend.readSchedule('x'), isNull);
        final audit = (await w.audits(kDestinationDeletedEntryType)).single;
        expect(audit.data['tombstoned_row_id'], isNull);
        expect(audit.data['deleted_pending_count'], 0);
      });

      // Verifies: EVS-PRD-destinations/O
      // a destination registered again under
      //   the same identifier delivers events appended after its
      //   registration; its items continue above the retained ones.
      test('re-added under the same id, it delivers new events above the '
          'retained items', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.note('old');
        await w.fillAll(d);
        await wedgeHeadForTest(w.backend, 'x');
        await w.registry.deleteDestination('x', initiator: _init);
        final retained = await w.backend.listFifoEntries('x');

        final again = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk(), const SendOk()],
        );
        await w.registry.addDestination(again, initiator: _init);
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 6, 1),
          initiator: _init,
        );
        await w.note('new', at: DateTime.utc(2026, 6, 2));
        final cycle = SyncCycle(
          backend: w.backend,
          registry: w.registry,
          clock: _fillNow,
        );
        await cycle();
        final rows = await w.backend.listFifoEntries('x');
        final fresh = rows.skip(retained.length).toList();
        expect((await w.items('x')).skip(retained.length), [
          ('new', FinalStatus.sent),
        ]);
        for (final r in fresh) {
          expect(r.sequenceInQueue, greaterThan(retained.last.sequenceInQueue));
        }
      });

      // Verifies: EVS-PRD-destinations/O
      // re-adding and activating in the past
      //   enqueues again events the prior registration delivered
      //   (at-least-once).
      test("re-add with a past start date re-sends the prior registration's "
          'delivered events', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk()],
        );
        await activate(d);
        await w.note('delivered');
        await w.fillAll(d);
        await drain(d, backend: w.backend);
        expect(await w.eventsIn('x', status: FinalStatus.sent), ['delivered']);
        await w.registry.deleteDestination('x', initiator: _init);
        final again = FakeDestination(id: 'x', allowHardDelete: true);
        await w.registry.addDestination(again, initiator: _init);
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(again);
        expect(await w.pendingNotes('x'), ['delivered']);
        expect(await w.eventsIn('x', status: FinalStatus.sent), ['delivered']);
      });

      // Verifies: EVS-PRD-destinations/O
      // an injected failure after the
      //   deletion's last write leaves every item, the schedule, the cursor
      //   and the replay request as they were, and appends no event.
      // Verifies: EVS-DEV-destination-drain/A
      // the deletion transaction is atomic.
      test('deletion rolls back on an injected failure', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.note('s1');
        await w.note('h1');
        await w.note('p1');
        await w.fillAll(d);
        await drain(
          FakeDestination(id: 'x', script: <SendResult>[const SendOk()]),
          backend: w.backend,
        );
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2025, 1, 1),
          initiator: _init,
        );
        await wedgeHeadForTest(w.backend, 'x');
        final before = await w.snapshot('x');
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              failRegistryAuditAppend: (t) => t == kDestinationDeletedEntryType,
            ),
            () => w.registry.deleteDestination('x', initiator: _init),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot('x'), before);
        expect(w.registry.byId('x'), isNotNull);
      });

      // Verifies: EVS-PRD-destinations/O
      // a delete request while a send is in
      //   flight is refused; the send completes and its attempt is recorded.
      // Verifies: EVS-DEV-destination-drain/D
      // the drainer's attempt on the head it
      //   sent is recorded with no error.
      test('deletion during an in-flight send is refused; the send is '
          'recorded', () async {
        if (!available) return;
        final gate = Completer<void>();
        final entered = Completer<void>();
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk()],
          blockBeforeSend: () {
            entered.complete();
            return gate.future;
          },
        );
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        final head = (await w.backend.readFifoHead('x'))!;
        final draining = drain(d, backend: w.backend);
        await entered.future;
        await expectLater(
          w.registry.deleteDestination('x', initiator: _init),
          throwsStateError,
        );
        gate.complete();
        await draining;
        final row = await w.backend.readFifoRow('x', head.entryId);
        expect(row!.finalStatus, FinalStatus.sent);
        expect(row.attempts.single.outcome, 'ok');
      });
    });

    // ------------------------------------------------------------------
    // The opt-in in effect and operations from any registry
    // ------------------------------------------------------------------

    group('persisted state', () {
      // Verifies: EVS-DEV-destination-drain/A
      // the latest registration's opt-in is
      //   the one in effect; a deletion acts on it and records it.
      // Verifies: EVS-PRD-destinations/O
      // deletion is refused while the opt-in in
      //   effect is false.
      test("the latest registration's opt-in is in effect", () async {
        if (!available) return;
        final b = await w.openProcess();
        final c = await w.openProcess();
        await w.registry.addDestination(
          FakeDestination(id: 'x', allowHardDelete: true),
          initiator: _init,
        );
        await b.registry.addDestination(
          FakeDestination(id: 'x'),
          initiator: _init,
        );
        expect(
          (await w.audits(
            kDestinationRegisteredEntryType,
          )).last.data['allow_hard_delete'],
          isFalse,
        );
        final before = await w.snapshot('x');
        for (final r in <DestinationRegistry>[w.registry, b.registry]) {
          await expectLater(
            r.deleteDestination('x', initiator: _init),
            throwsStateError,
          );
          await w.expectOnlyCheckWritten(
            'x',
            before,
            op: 'deleteDestination',
            outcome: 'refused_not_opted_in',
          );
        }
        await c.registry.addDestination(
          FakeDestination(id: 'x', allowHardDelete: true),
          initiator: _init,
        );
        await b.registry.deleteDestination('x', initiator: _init);
        final audit = (await w.audits(kDestinationDeletedEntryType)).single;
        expect(audit.data['allow_hard_delete'], isTrue);
      });

      // Verifies: EVS-DEV-destination-drain/A
      // the date, recovery and deletion
      //   operations act on persisted state from a registry that never
      //   registered the destination.
      test('a registry with no local destination runs every persisted-state '
          'operation', () async {
        if (!available) return;
        final b = await w.openProcess();
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await w.registry.addDestination(d, initiator: _init);
        expect(b.registry.byId('x'), isNull);

        await b.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        expect(
          (await w.backend.readSchedule('x'))!.startDate,
          DateTime.utc(2026, 1, 1),
        );
        expect(await w.request('x'), isNotNull);
        final end = DateTime.utc(2028, 1, 1);
        await b.registry.setEndDate('x', end, initiator: _init);
        expect((await w.backend.readSchedule('x'))!.endDate, end);

        await w.note('n1');
        await w.fillAll(d);
        final headId = await wedgeHeadForTest(w.backend, 'x');
        await b.registry.tombstoneAndRefill('x', headId, initiator: _init);
        expect(
          (await w.backend.readFifoRow('x', headId))!.finalStatus,
          FinalStatus.tombstoned,
        );
        await b.registry.deleteDestination('x', initiator: _init);
        expect(await w.backend.readSchedule('x'), isNull);
        expect(await w.audits(kDestinationDeletedEntryType), hasLength(1));
      });

      // Verifies: EVS-DEV-destination-drain/A
      // each operation on an unknown
      //   destination is refused with nothing written but the check record.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal is decided in a writing
      //   transaction.
      test('an unknown destination is refused by every persisted-state '
          'operation', () async {
        if (!available) return;
        final b = await w.openProcess();
        final ops = <String, Future<Object?> Function()>{
          'setStartDate': () => b.registry.setStartDate(
            'ghost',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
          'setEndDate': () => b.registry.setEndDate(
            'ghost',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
          'tombstoneAndRefill': () =>
              b.registry.tombstoneAndRefill('ghost', 'row', initiator: _init),
          'deleteDestination': () =>
              b.registry.deleteDestination('ghost', initiator: _init),
        };
        for (final entry in ops.entries) {
          final before = await w.snapshot('ghost');
          await expectLater(entry.value(), throwsArgumentError);
          await w.expectOnlyCheckWritten(
            'ghost',
            before,
            op: entry.key,
            outcome: 'refused_unknown_destination',
          );
        }
      });

      // Verifies: EVS-DEV-destination-drain/U
      // a start date already in effect writes
      //   only the check record and returns; each registry body runs in a
      //   transaction that commits a write.
      test('an unchanged start date writes only the check record', () async {
        if (!available) return;
        await activate(FakeDestination(id: 'x'));
        final runs = <String>[];
        final before = await w.snapshot('x');
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onRegistryBodyRun: runs.add),
          () => w.registry.setStartDate(
            'x',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
        );
        expect(runs, isNotEmpty);
        expect(runs.toSet(), {'setStartDate'});
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'setStartDate',
          outcome: 'unchanged',
        );
      });

      // Verifies: EVS-DEV-destination-drain/A
      // a registry that still holds a deleted
      //   destination accepts it again with a new registration and delivers;
      //   a second registration in the same registry is refused; a local
      //   deletion forgets the destination.
      // Verifies: EVS-PRD-destinations/O
      // re-registration works from any
      //   registry with no restart.
      test('a stale local entry is replaced on re-add', () async {
        if (!available) return;
        final b = await w.openProcess();
        final first = FakeDestination(id: 'x', allowHardDelete: true);
        await w.registry.addDestination(first, initiator: _init);
        final firstRegistration = (await w.backend.readSchedule(
          'x',
        ))!.registrationId;
        await b.registry.deleteDestination('x', initiator: _init);

        final again = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          script: <SendResult>[const SendOk()],
        );
        await w.registry.addDestination(again, initiator: _init);
        final second = (await w.backend.readSchedule('x'))!.registrationId;
        expect(second, isNot(firstRegistration));
        expect(w.registry.byId('x'), same(again));
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 6, 1),
          initiator: _init,
        );
        await w.note('after', at: DateTime.utc(2026, 6, 2));
        await SyncCycle(
          backend: w.backend,
          registry: w.registry,
          clock: _fillNow,
        )();
        expect(await w.eventsIn('x', status: FinalStatus.sent), ['after']);

        final before = await w.snapshot('x');
        await expectLater(
          w.registry.addDestination(
            FakeDestination(id: 'x', allowHardDelete: true),
            initiator: _init,
          ),
          throwsArgumentError,
        );
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'addDestination',
          outcome: 'refused_already_registered',
        );

        await w.registry.deleteDestination('x', initiator: _init);
        expect(w.registry.byId('x'), isNull);
        await w.registry.addDestination(
          FakeDestination(id: 'x'),
          initiator: _init,
        );
        expect(w.registry.byId('x'), isNotNull);
      });
    });

    // ------------------------------------------------------------------
    // Rollback of the date and registration operations
    // ------------------------------------------------------------------

    group('date and registration rollback', () {
      Future<void> expectRollback(
        String destId,
        String auditType,
        Future<Object?> Function() op,
      ) async {
        final before = await w.snapshot(destId);
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(failRegistryAuditAppend: (t) => t == auditType),
            op,
          ),
          throwsA(isA<InjectedFailure>()),
        );
        expect(await w.snapshot(destId), before);
      }

      // Verifies: EVS-DEV-destination-drain/E
      // a first activation's schedule, replay
      //   request and audit commit together.
      // Verifies: EVS-DEV-destination-drain/A
      // the operation acts on the persisted
      //   schedule atomically.
      test('setStartDate first activation', () async {
        if (!available) return;
        await w.registry.addDestination(
          FakeDestination(id: 'x'),
          initiator: _init,
        );
        Future<void> op() => w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await expectRollback('x', kDestinationStartDateSetEntryType, op);
        await op();
        expect(
          (await w.backend.readSchedule('x'))!.startDate,
          DateTime.utc(2026, 1, 1),
        );
        expect(
          await w.request('x'),
          const ReplayRequest(firstActivation: true),
        );
        expect(await w.audits(kDestinationStartDateSetEntryType), hasLength(1));
      });

      // Verifies: EVS-DEV-destination-drain/E
      // a backward move merges into a pending
      //   request, keeping its larger bound and its first-activation flag;
      //   the merge rolls back with the transaction.
      test('setStartDate backward move merging a pending request', () async {
        if (!available) return;
        await w.registry.addDestination(
          FakeDestination(id: 'x'),
          initiator: _init,
        );
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 3, 1),
          initiator: _init,
        );
        // Perform the first-activation request, then move earlier twice.
        await w.fillAll(FakeDestination(id: 'x'));
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 2, 1),
          initiator: _init,
        );
        expect(
          await w.request('x'),
          ReplayRequest(gapUpper: DateTime.utc(2026, 3, 1)),
        );
        Future<void> op() => w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await expectRollback('x', kDestinationStartDateSetEntryType, op);
        await op();
        expect(
          await w.request('x'),
          ReplayRequest(gapUpper: DateTime.utc(2026, 3, 1)),
        );
        expect(
          (await w.backend.readSchedule('x'))!.startDate,
          DateTime.utc(2026, 1, 1),
        );
      });

      // Verifies: EVS-DEV-destination-drain/E
      // a forward move is refused and writes
      //   nothing but the check record.
      test('setStartDate forward move', () async {
        if (!available) return;
        await activate(FakeDestination(id: 'x'));
        final before = await w.snapshot('x');
        await expectLater(
          w.registry.setStartDate(
            'x',
            DateTime.utc(2026, 5, 1),
            initiator: _init,
          ),
          throwsStateError,
        );
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'setStartDate',
          outcome: 'refused_forward_move',
        );
      });

      // Verifies: EVS-DEV-destination-drain/A
      // a re-registration that flips the
      //   opt-in rolls back whole: the persisted schedule is unchanged and
      //   no registration event is appended, so the latest registration
      //   event still states the opt-in in effect.
      test('addDestination re-registration with a flipped opt-in', () async {
        if (!available) return;
        await w.registry.addDestination(
          FakeDestination(id: 'x', allowHardDelete: true),
          initiator: _init,
        );
        final b = await w.openProcess();
        Future<void> op() => b.registry.addDestination(
          FakeDestination(id: 'x'),
          initiator: _init,
        );
        await expectRollback('x', kDestinationRegisteredEntryType, op);
        expect(
          (await w.audits(
            kDestinationRegisteredEntryType,
          )).last.data['allow_hard_delete'],
          isTrue,
        );
        await op();
        final schedule = (await w.backend.readSchedule('x'))!;
        expect(schedule.allowHardDelete, isFalse);
        expect(
          schedule.registrationId,
          (await w.audits(kDestinationRegisteredEntryType)).first.eventId,
        );
        expect(await w.audits(kDestinationRegisteredEntryType), hasLength(2));
      });

      // Verifies: EVS-DEV-destination-drain/A
      // the end-date write and its audit
      //   commit together.
      test('setEndDate', () async {
        if (!available) return;
        await activate(FakeDestination(id: 'x'));
        final end = DateTime.utc(2029, 1, 1);
        Future<void> op() => w.registry.setEndDate('x', end, initiator: _init);
        await expectRollback('x', kDestinationEndDateSetEntryType, op);
        await op();
        expect((await w.backend.readSchedule('x'))!.endDate, end);
        expect(await w.audits(kDestinationEndDateSetEntryType), hasLength(1));
      });
    });

    // ------------------------------------------------------------------
    // Replay requests: only the drainer's fill enqueues
    // ------------------------------------------------------------------

    group('replay requests', () {
      // Verifies: EVS-DEV-destination-drain/E
      // setStartDate enqueues nothing itself
      //   and records a request; the next fill performs it.
      test('setStartDate records a request; the fill enqueues', () async {
        if (!available) return;
        await w.note('n1');
        await w.note('n2');
        final d = FakeDestination(id: 'x');
        await activate(d);
        expect(await w.backend.listFifoEntries('x'), isEmpty);
        expect(
          await w.request('x'),
          const ReplayRequest(firstActivation: true),
        );
        await w.fill(d);
        expect(await w.pendingNotes('x'), ['n1', 'n2']);
        expect(await w.request('x'), isNull);
      });

      // Verifies: EVS-DEV-destination-drain/E
      // rows built after another registry's
      //   setStartDate carry the drainer's transform output.
      test("the drainer's configuration builds the replay", () async {
        if (!available) return;
        final b = await w.openProcess();
        final drainer = _TaggedDestination(id: 'x', tag: 'drainer');
        await w.registry.addDestination(drainer, initiator: _init);
        await b.registry.addDestination(
          _TaggedDestination(id: 'x', tag: 'other'),
          initiator: _init,
        );
        await w.note('n1');
        await b.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fill(drainer);
        final rows = await w.backend.listFifoEntries('x');
        expect(rows.single.wirePayload!['tag'], 'drainer');
      });

      // Verifies: EVS-DEV-destination-drain/E
      // an event with an old timestamp
      //   appended past the fill position lands in exactly one item after a
      //   backward move: the gap replay admits only events at or below the
      //   position, and the fill only events above it.
      test('the gap replay and the fill partition the log', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.note('in', at: DateTime.utc(2026, 2, 5));
        await w.fillAll(d);
        // An event whose client timestamp is before the start date, appended
        // after the position.
        await w.note('old', at: DateTime.utc(2026, 1, 10));
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(d);
        final notes = await w.pendingNotes('x');
        expect(notes.where((n) => n == 'old'), hasLength(1));
        expect(notes.toSet(), {'in', 'old'});
      });

      // Verifies: EVS-DEV-destination-drain/E
      // a backward move is refused while the
      //   head is wedged, with nothing written but the check record.
      test('a backward move on a wedged head is refused', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d, start: DateTime.utc(2026, 2, 1));
        await w.note('n1', at: DateTime.utc(2026, 2, 5));
        await w.fillAll(d);
        await wedgeHeadForTest(w.backend, 'x');
        final before = await w.snapshot('x');
        await expectLater(
          w.registry.setStartDate(
            'x',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
          throwsStateError,
        );
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'setStartDate',
          outcome: 'refused_wedged_head',
        );
      });

      // Verifies: EVS-DEV-destination-drain/E
      // two backward moves before a fill replay
      //   the union of their gaps once.
      test('two backward moves replay the union once', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await w.note('jan', at: DateTime.utc(2026, 1, 10));
        await w.note('feb', at: DateTime.utc(2026, 2, 10));
        await w.note('mar', at: DateTime.utc(2026, 3, 10));
        await activate(d, start: DateTime.utc(2026, 3, 1));
        await w.fillAll(d);
        expect(await w.pendingNotes('x'), ['mar']);
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 2, 1),
          initiator: _init,
        );
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: _init,
        );
        await w.fillAll(d);
        expect(await w.pendingNotes('x'), ['mar', 'jan', 'feb']);
      });
    });

    // ------------------------------------------------------------------
    // The fill's compare-and-set
    // ------------------------------------------------------------------

    group('fill compare-and-set', () {
      // Verifies: EVS-DEV-destination-drain/G
      // a deletion, re-registration and
      //   activation that commit while the fill runs the transform make the
      //   fill write nothing; the next fill enqueues each event once for the
      //   new registration.
      test('re-registration while the transform runs', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        await drain(
          FakeDestination(id: 'x', script: <SendResult>[const SendOk()]),
          backend: w.backend,
        );
        final cursorBefore = await w.backend.readFillCursor('x');
        await w.note('n2');
        final b = await w.openProcess();
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            insideTransform: (id) async {
              if (fired) return;
              fired = true;
              await b.registry.deleteDestination('x', initiator: _init);
              await b.registry.addDestination(
                FakeDestination(id: 'x', allowHardDelete: true),
                initiator: _init,
              );
              await b.registry.setStartDate(
                'x',
                DateTime.utc(2026, 1, 1),
                initiator: _init,
              );
            },
          ),
          () => w.fill(d),
        );
        expect(fired, isTrue);
        // The fill wrote nothing: no pending item, and no cursor from the
        // old registration's computation.
        expect(await w.pendingNotes('x'), isEmpty);
        expect(await w.backend.readFillCursor('x'), -1);
        expect(cursorBefore, greaterThan(-1));
        await w.fillAll(d);
        final notes = await w.pendingNotes('x');
        expect(notes, ['n1', 'n2']);
      });

      // Verifies: EVS-DEV-destination-drain/G
      // an end date moved into the past while
      //   the transform runs is honoured: no item carries an event past it.
      test('an end date set while the transform runs', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d);
        await w.note('before', at: DateTime.utc(2026, 3, 1));
        await w.note('after', at: DateTime.utc(2026, 5, 1));
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            insideTransform: (id) async {
              if (fired) return;
              fired = true;
              await w.registry.setEndDate(
                'x',
                DateTime.utc(2026, 4, 1),
                initiator: _init,
              );
            },
          ),
          () => w.fill(d),
        );
        expect(fired, isTrue);
        // The interrupted fill wrote nothing; the next fills see the end date.
        expect(await w.pendingNotes('x'), isEmpty);
        await w.fillAll(d);
        expect(await w.pendingNotes('x'), ['before']);
      });

      // Verifies: EVS-DEV-destination-drain/G
      // a deletion committed after the fill's
      //   reads and before a no-candidate advance leaves no fill position.
      test('a deletion before a no-candidate advance', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          allowHardDelete: true,
          filter: const SubscriptionFilter(entryTypes: <String>{}),
        );
        await activate(d);
        await w.fillAll(d);
        await w.note('rejected');
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            afterFillReads: (id) async {
              if (fired) return;
              fired = true;
              await w.registry.deleteDestination('x', initiator: _init);
            },
          ),
          () => w.fill(d),
        );
        expect(fired, isTrue);
        final keys = await w.db.backendStateKeys();
        expect(keys, isNot(contains('fill_cursor_x')));
      });

      // Verifies: EVS-DEV-destination-drain/G
      // the fill compares the head status: a
      //   head another drainer wedged while the transform ran makes the
      //   fill write nothing, so no item is enqueued behind the wedged head.
      test('a head wedged while the transform runs', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        await w.note('n2');
        final cursorBefore = await w.backend.readFillCursor('x');
        final b = await w.openProcess();
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            insideTransform: (id) async {
              if (fired) return;
              fired = true;
              await wedgeHeadForTest(b.backend, 'x');
            },
          ),
          () => w.fill(d),
        );
        expect(fired, isTrue);
        expect(await w.items('x'), <(String, FinalStatus?)>[
          ('n1', FinalStatus.wedged),
        ]);
        expect(await w.backend.readFillCursor('x'), cursorBefore);
      });

      // Verifies: EVS-DEV-destination-drain/G
      // the fill compares the replay request:
      //   another process's fill that performs the pending gap request
      //   after this fill's reads (the head, the schedule and the fill
      //   position unchanged) makes this fill write nothing, so every event
      //   lies in exactly one item.
      test('a replay request performed by another fill', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d, start: DateTime.utc(2026, 1, 1));
        await w.note('head', at: DateTime.utc(2026, 3, 1));
        await w.note('old', at: DateTime.utc(2025, 6, 1));
        await w.fillAll(d);
        expect(await w.pendingNotes('x'), ['head']);
        await w.registry.setStartDate(
          'x',
          DateTime.utc(2025, 1, 1),
          initiator: _init,
        );
        expect((await w.request('x'))?.gapUpper, DateTime.utc(2026, 1, 1));
        final cursorBefore = await w.backend.readFillCursor('x');
        final b = await w.openProcess();
        var fired = false;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            afterFillReads: (id) async {
              if (fired) return;
              fired = true;
              // B performs the gap request (its first compare-and-set) and
              // its fill position advance past the start-date event is
              // failed, so only the request differs from A's reads.
              var commits = 0;
              await expectLater(
                runWithDeliveryTestHooks(
                  DeliveryTestHooks(
                    failFillTransaction: (id) => (commits += 1) > 1,
                  ),
                  () => w.fill(d, on: b.backend),
                ),
                throwsA(isA<InjectedFailure>()),
              );
              expect(await w.request('x'), isNull);
              expect(await w.backend.readFillCursor('x'), cursorBefore);
            },
          ),
          () => w.fill(d),
        );
        expect(fired, isTrue);
        expect(await w.request('x'), isNull);
        expect(await w.pendingNotes('x'), ['head', 'old']);
        expect(await w.backend.readFillCursor('x'), cursorBefore);
      });
    });

    // ------------------------------------------------------------------
    // Fill transaction atomicity
    // ------------------------------------------------------------------

    group('fill transaction', () {
      Future<void> failingFill(Destination d) => expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(failFillTransaction: (id) => true),
          () => w.fill(d),
        ),
        throwsA(isA<InjectedFailure>()),
      );

      // Verifies: EVS-DEV-destination-drain/E
      // the first-activation replay's items,
      //   the advanced fill position and the cleared request commit
      //   together: a failure after the writes leaves all three unchanged.
      // Verifies: EVS-PRD-destinations/D
      // the enqueue and the fill position
      //   advance are one durable step.
      test('a first-activation replay commits its items, position and '
          'cleared request together', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await w.note('n1');
        await w.note('n2');
        await activate(d);
        expect((await w.request('x'))?.firstActivation, isTrue);
        final before = await w.snapshot('x');
        await failingFill(d);
        expect(await w.snapshot('x'), before);
        await w.fill(d);
        expect(await w.pendingNotes('x'), ['n1', 'n2']);
        expect(await w.request('x'), isNull);
        expect(await w.backend.readFillCursor('x'), greaterThan(-1));
      });

      // Verifies: EVS-DEV-destination-drain/E
      // the gap replay's items and the cleared
      //   request commit together; the fill position is not moved.
      // Verifies: EVS-DEV-destination-drain/G
      // the fill's writes commit in its
      //   compare-and-set transaction or not at all.
      test(
        'a gap replay commits its items and cleared request together',
        () async {
          if (!available) return;
          final d = FakeDestination(id: 'x');
          await activate(d, start: DateTime.utc(2026, 1, 1));
          await w.note('head', at: DateTime.utc(2026, 3, 1));
          await w.note('old', at: DateTime.utc(2025, 6, 1));
          await w.fillAll(d);
          await w.registry.setStartDate(
            'x',
            DateTime.utc(2025, 1, 1),
            initiator: _init,
          );
          final before = await w.snapshot('x');
          await failingFill(d);
          expect(await w.snapshot('x'), before);
          await w.fill(d);
          expect(await w.pendingNotes('x'), ['head', 'old']);
          expect(await w.request('x'), isNull);
        },
      );

      // Verifies: EVS-PRD-destinations/D
      // a live batch's item and the fill
      //   position commit together.
      // Verifies: EVS-DEV-destination-drain/G
      // the fill's writes commit in its
      //   compare-and-set transaction or not at all.
      test('a live batch commits its item and position together', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x');
        await activate(d);
        await w.fillAll(d);
        await w.note('n1');
        final before = await w.snapshot('x');
        await failingFill(d);
        expect(await w.snapshot('x'), before);
        await w.fill(d);
        expect(await w.pendingNotes('x'), ['n1']);
        final n1 = (await w.backend.findAllEvents()).last;
        expect(await w.backend.readFillCursor('x'), n1.sequenceNumber);
      });
    });

    // ------------------------------------------------------------------
    // Decisions made inside the operation's transaction
    // ------------------------------------------------------------------

    group('decisions inside the transaction', () {
      // Verifies: EVS-DEV-destination-drain/A
      // two registrations of one id started
      //   together on one registry: exactly one registers, the other is
      //   refused, and one registration event is appended.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal is decided inside a
      //   transaction that writes the registry check record.
      test('concurrent registrations of one id register it once', () async {
        if (!available) return;
        final first = FakeDestination(id: 'x');
        final second = FakeDestination(id: 'x');
        final outcomes = await Future.wait(<Future<Object?>>[
          for (final d in <Destination>[first, second])
            w.registry
                .addDestination(d, initiator: _init)
                .then<Object?>((_) => d, onError: (Object e) => e),
        ]);
        final registered = outcomes.whereType<Destination>().toList();
        final refused = outcomes.whereType<ArgumentError>().toList();
        expect(registered, hasLength(1));
        expect(refused, hasLength(1));
        expect(w.registry.byId('x'), same(registered.single));
        expect(await w.audits(kDestinationRegisteredEntryType), hasLength(1));
        final check = await w.check();
        expect(check?.op, 'addDestination');
        expect(check?.outcome, 'refused_already_registered');
      });

      // Verifies: EVS-DEV-destination-drain/A
      // the deletion reads the head inside its
      //   transaction: a pending head that a fill committed after the call
      //   began, and before its transaction opened, is refused.
      // Verifies: EVS-DEV-destination-drain/U
      // the refusal writes only the registry
      //   check record.
      test('a deletion sees a head filled before its transaction', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        await activate(d);
        await w.fillAll(d);
        expect(await w.backend.readFifoHead('x'), isNull);
        late Map<String, Object?> before;
        var fired = false;
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              beforeRegistryTransaction: (op) async {
                if (op != 'deleteDestination' || fired) return;
                fired = true;
                await w.note('n1');
                await w.fillAll(d);
                before = await w.snapshot('x');
              },
            ),
            () => w.registry.deleteDestination('x', initiator: _init),
          ),
          throwsA(isA<StateError>()),
        );
        expect(fired, isTrue);
        expect((await w.backend.readFifoHead('x'))?.finalStatus, isNull);
        await w.expectOnlyCheckWritten(
          'x',
          before,
          op: 'deleteDestination',
          outcome: 'refused_pending_head',
        );
      });

      // Verifies: EVS-DEV-destination-drain/U
      // an observing seam that throws does
      //   not change the operation's outcome: the operation commits.
      test(
        'a throwing body-run observer leaves the operation unchanged',
        () async {
          if (!available) return;
          final d = FakeDestination(id: 'x');
          await activate(d);
          final end = DateTime.utc(2026, 6, 1);
          var runs = 0;
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              onRegistryBodyRun: (op) {
                runs += 1;
                throw StateError('observer failure');
              },
            ),
            () => w.registry.setEndDate('x', end, initiator: _init),
          );
          expect(runs, greaterThan(0));
          expect((await w.backend.readSchedule('x'))?.endDate, end);
          expect(await w.audits(kDestinationEndDateSetEntryType), hasLength(1));
        },
      );
    });

    // ------------------------------------------------------------------
    // Drain outcome atomicity
    // ------------------------------------------------------------------

    group('drain outcomes', () {
      // Verifies: EVS-DEV-destination-drain/C
      // the attempt and the sent status commit
      //   together: a failure injected after both writes leaves neither.
      // Verifies: EVS-PRD-destinations/J
      // a committed outcome records its
      //   attempt.
      test('attempt and sent commit together', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[const SendOk(), const SendOk()],
        );
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        final head = (await w.backend.readFifoHead('x'))!;
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(failOutcomeTransaction: (id, outcome) => true),
            () => drain(d, backend: w.backend),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.attempts, isEmpty);
        expect(row.finalStatus, isNull);
        await drain(d, backend: w.backend);
        final sent = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(sent.finalStatus, FinalStatus.sent);
        expect(sent.attempts.single.outcome, 'ok');
      });

      // Verifies: EVS-DEV-destination-drain/C
      // the attempt and the wedged status
      //   commit together.
      test('attempt and wedged commit together', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            const SendPermanent(error: 'no'),
            const SendPermanent(error: 'no'),
          ],
        );
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        final head = (await w.backend.readFifoHead('x'))!;
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(failOutcomeTransaction: (id, outcome) => true),
            () => drain(d, backend: w.backend),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.attempts, isEmpty);
        expect(row.finalStatus, isNull);
        await drain(d, backend: w.backend);
        final wedged = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(wedged.finalStatus, FinalStatus.wedged);
        expect(wedged.attempts.single.outcome, 'permanent');
      });

      const twoAttempts = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 2,
        periodicInterval: Duration(minutes: 15),
      );

      // Verifies: EVS-DEV-destination-drain/C
      // a transient failure below the attempt
      //   cap commits its attempt alone: a failure injected in the outcome
      //   transaction leaves no attempt; on success one attempt is recorded
      //   and the head stays pending.
      test('a transient attempt below the cap commits alone', () async {
        if (!available) return;
        final d = FakeDestination(
          id: 'x',
          script: <SendResult>[
            const SendTransient(error: 'busy'),
            const SendTransient(error: 'busy'),
          ],
        );
        await activate(d);
        await w.note('n1');
        await w.fillAll(d);
        final head = (await w.backend.readFifoHead('x'))!;
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(failOutcomeTransaction: (id, outcome) => true),
            () => drain(d, backend: w.backend, policy: twoAttempts),
          ),
          throwsA(isA<InjectedFailure>()),
        );
        final failed = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(failed.attempts, isEmpty);
        expect(failed.finalStatus, isNull);
        await drain(d, backend: w.backend, policy: twoAttempts);
        final row = (await w.backend.readFifoRow('x', head.entryId))!;
        expect(row.attempts.single.outcome, 'transient');
        expect(row.finalStatus, isNull);
      });

      // Verifies: EVS-DEV-destination-drain/C
      // the attempt that spends the retry
      //   budget and the wedged status it produces commit together.
      test(
        'an attempt that exhausts the budget and wedged commit together',
        () async {
          if (!available) return;
          final d = FakeDestination(
            id: 'x',
            script: <SendResult>[
              const SendTransient(error: 'busy'),
              const SendTransient(error: 'busy'),
              const SendTransient(error: 'busy'),
            ],
          );
          await activate(d);
          await w.note('n1');
          await w.fillAll(d);
          final head = (await w.backend.readFifoHead('x'))!;
          await drain(d, backend: w.backend, policy: twoAttempts);
          await expectLater(
            runWithDeliveryTestHooks(
              DeliveryTestHooks(failOutcomeTransaction: (id, outcome) => true),
              () => drain(d, backend: w.backend, policy: twoAttempts),
            ),
            throwsA(isA<InjectedFailure>()),
          );
          final failed = (await w.backend.readFifoRow('x', head.entryId))!;
          expect(failed.attempts, hasLength(1));
          expect(failed.finalStatus, isNull);
          await drain(d, backend: w.backend, policy: twoAttempts);
          final row = (await w.backend.readFifoRow('x', head.entryId))!;
          expect(row.attempts, hasLength(2));
          expect(row.attempts.last.outcome, 'transient');
          expect(row.finalStatus, FinalStatus.wedged);
        },
      );
    });
  });
}
