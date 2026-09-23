// Backend-agnostic scenarios for the default destination-wedges view and
// the authority over reserved system events: the view's rows and key, the
// events that end a wedge, the database identity on every destination
// audit, the refusal of reserved appends through the public append
// operations, the ingest refusals of reserved events whose shape the
// library does not append, and the automatic registration of the reserved
// entry types and the view at open. Sembast runs them from
// test/destinations/destination_wedges_view_test.dart and Postgres from
// test/storage/postgres/postgres_destination_wedges_view_test.dart.
//
// This file exposes [runDestinationWedgesViewScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kDestinationAuditAggregateType,
        kDestinationAuditEntryTypes,
        kIngestAuditEntryType,
        kReservedEventShapes;
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'fake_destination.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart';
import 'wedges_view_invariant.dart';

const Initiator _init = AutomationInitiator(service: 'wedges-view-scenarios');
const String _noteType = 'view_note';
const Source _source = Source(
  hopId: 'server',
  identifier: 'receiver-install',
  softwareVersion: 'test@1.0.0',
);
const Source _peerSource = Source(
  hopId: 'mobile-device',
  identifier: 'peer-install',
  softwareVersion: 'test@1.0.0',
);

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: _noteType,
  registeredVersion: EntryTypeVersion(1, 0),
  name: _noteType,
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// One event store over a backend, with its registry.
class _Store {
  _Store(this.backend, this.store, this.registry);
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
  DateTime eventTime = DateTime.utc(2026, 3, 1);

  Future<StoredEvent> note(String id) async {
    eventTime = eventTime.add(const Duration(minutes: 1));
    return (await store.append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'id': id},
      initiator: _init,
    ))!;
  }

  /// Register and activate [d], append a note and fill until the queue
  /// holds an item.
  Future<void> queued(FakeDestination d) async {
    if (registry.byId(d.id) == null) {
      await registry.addDestination(d, initiator: _init);
      await registry.setStartDate(
        d.id,
        DateTime.utc(2026, 1, 1),
        initiator: _init,
      );
    }
    await note('${d.id}-${eventTime.microsecondsSinceEpoch}');
    for (var i = 0; i < 20; i++) {
      final head = await backend.readFifoHead(d.id);
      if (head != null && head.finalStatus == null) return;
      await fillBatch(d, backend: backend, source: _source, clock: _fillNow);
    }
    throw StateError('queued(${d.id}): no pending head after the fill');
  }

  /// Queue an item for [d] and wedge it the way the drainer does.
  Future<String> wedge(FakeDestination d) async {
    await queued(d);
    return wedgeHeadForTest(registry, d.id);
  }

  Future<List<StoredEvent>> events({String? entryType}) =>
      backend.findAllEvents(entryType: entryType);

  /// What a refused operation must leave unchanged: the log, the view and
  /// the queue heads.
  Future<Map<String, Object?>> snapshot() async => <String, Object?>{
    'events': <String>[for (final e in await events()) e.eventId],
    'view': await wedgesViewRows(backend),
    'wedged': <String>[
      for (final s in await backend.wedgedFifos())
        '${s.destinationId}/${s.headEntryId}',
    ],
  };

  /// Asserts the sequence counter was not advanced by anything that did not
  /// commit: the next append takes the number after the last stored event.
  Future<void> expectCounterUnchanged() async {
    final all = await events();
    final last = all.isEmpty ? 0 : all.last.sequenceNumber;
    final probe = await note('counter-probe-${all.length}');
    expect(
      probe.sequenceNumber,
      last + 1,
      reason: 'nothing that rolled back advanced the sequence counter',
    );
  }
}

/// A peer on its own in-memory database, with its own database identity.
Future<_Store> _openPeer(int n) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'wedges-view-peer-$n.db',
  );
  final backend = SembastBackend(database: db);
  final entryTypes = EntryTypeRegistry()..register(_noteDef);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: _peerSource,
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
  return _Store(backend, store, DestinationRegistry(eventStore: store));
}

var _forged = 0;

/// An event as a peer sends it, with one origin provenance entry, so the
/// receiver has no hop link to verify: the shape of [data], [entryType],
/// [aggregateType] and [eventType] is whatever the test gives.
StoredEvent forgedEvent({
  required String entryType,
  required String aggregateType,
  required String eventType,
  required Map<String, Object?> data,
}) {
  _forged += 1;
  final now = DateTime.utc(2026, 9, 1, 12, 0, _forged % 60);
  return StoredEvent.fromMap(<String, Object?>{
    'event_id': 'forged-$_forged-${DateTime.now().microsecondsSinceEpoch}',
    'aggregate_id': _peerSource.identifier,
    'aggregate_type': aggregateType,
    'entry_type': entryType,
    'entry_type_version': const EntryTypeVersion(1, 0).toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': eventType,
    'sequence_number': 5000 + _forged,
    'data': data,
    'metadata': <String, Object?>{
      'change_reason': 'initial',
      'provenance': <Map<String, Object?>>[
        ProvenanceEntry(
          hop: _peerSource.hopId,
          receivedAt: now,
          identifier: _peerSource.identifier,
          softwareVersion: _peerSource.softwareVersion,
        ).toJson(),
      ],
    },
    'initiator': const AutomationInitiator(service: 'peer').toJson(),
    'flow_token': null,
    'client_timestamp': now.toIso8601String(),
    'event_hash': 'forged-hash-$_forged',
    'previous_event_hash': null,
  }, 0);
}

/// A wedge event's data as the drainer writes it, for [destinationId] of
/// [databaseId], with [overrides] applied (a null value removes the key
/// when [remove] names it).
Map<String, Object?> wedgeData({
  required String destinationId,
  required Object? databaseId,
  Map<String, Object?> overrides = const <String, Object?>{},
  Set<String> remove = const <String>{},
}) {
  final data = <String, Object?>{
    'id': destinationId,
    'database_id': databaseId,
    'row_id': 'peer-row-1',
    'event_ids': <String>['peer-event-1'],
    'first_seq': 1,
    'last_seq': 1,
    'sequence_in_queue': 1,
    'cause': 'permanent_refusal',
    'attempt_count': 1,
    'max_attempts': 3,
    'last_outcome': 'permanent',
    'http_status': null,
    'wire_format': 'fake-v1',
    'transform_version': 'fake-v1',
    'halt_request_event_id': null,
    'halt_requested_by': null,
    'halt_purpose': null,
    'drainer_epoch': null,
    'configuration_fingerprint': null,
    'configuration': null,
    ...overrides,
  };
  for (final key in remove) {
    data.remove(key);
  }
  return data;
}

Uint8List _batchOf(List<StoredEvent> events) => BatchEnvelope(
  batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
  batchId: 'wedges-view-batch-${events.first.eventId}',
  senderHop: _peerSource.hopId,
  senderIdentifier: _peerSource.identifier,
  senderSoftwareVersion: _peerSource.softwareVersion,
  sentAt: DateTime.utc(2026, 9, 1, 12),
  events: <Map<String, Object?>>[
    for (final e in events) Map<String, Object?>.from(e.toMap()),
  ],
).encode();

/// The two ingest entry points, each taking the whole list of events: the
/// batch in one envelope, or each event in its own `ingestEvent` call.
final Map<String, Future<void> Function(EventStore, List<StoredEvent>)>
_ingestPaths = <String, Future<void> Function(EventStore, List<StoredEvent>)>{
  'ingestBatch': (store, events) async {
    await store.ingestBatch(
      _batchOf(events),
      wireFormat: BatchEnvelope.wireFormat,
    );
  },
  'ingestEvent': (store, events) async {
    for (final e in events) {
      await store.ingestEvent(e);
    }
  },
};

Matcher _refused(ReservedEventRefusal reason, {String? eventId}) =>
    isA<IngestReservedEventRefused>()
        .having((e) => e.reason, 'reason', reason)
        .having((e) => e.eventId, 'eventId', eventId ?? anything);

/// Run every destination-wedges view scenario against a database
/// [databaseFactory] builds fresh for each test (a null database skips the
/// test).
void runDestinationWedgesViewScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
}) {
  group('destination-wedges view scenarios ($label)', () {
    late QueueTestDatabase db;
    late _Store r;
    var available = false;
    var peers = 0;

    Future<_Store> openStore({
      EntryTypeRegistry? entryTypes,
      ProjectionRegistry? projections,
      Source source = _source,
    }) async {
      final backend = await db.openBackend();
      final store = await EventStore.openForTest(
        storage: backend,
        entryTypes: entryTypes ?? (EntryTypeRegistry()..register(_noteDef)),
        projections: projections,
        source: source,
        securityContexts: db.securityFor(backend),
      );
      return _Store(backend, store, DestinationRegistry(eventStore: store));
    }

    Future<_Store> peer() async {
      peers += 1;
      return _openPeer(peers);
    }

    setUp(() async {
      final opened = await databaseFactory();
      if (opened == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      db = opened;
      r = await openStore();
    });

    tearDown(() async {
      if (!available) return;
      await db.close();
    });

    // ------------------------------------------------------------------
    // The view's rows
    // ------------------------------------------------------------------

    group('rows', () {
      // Verifies: EVS-PRD-destinations/S
      // a wedge inserts a row keyed by the database identity and the
      //   destination, carrying the wedge event's fields; a recovery removes
      //   it; a new wedge after the recovery inserts it again; a deletion of
      //   the wedged destination removes it.
      // Verifies: EVS-DEV-destination-drain/M
      // the view exists without any registration by the consumer, and its
      //   rows are keyed `<database identity>|<destination>`.
      test('wedge, recovery, wedge again, deletion', () async {
        if (!available) return;
        final d = FakeDestination(id: 'x', allowHardDelete: true);
        final key = '${r.store.databaseId}|x';
        expect(await wedgesViewRows(r.backend), isEmpty);

        final rowId = await r.wedge(d);
        final wedge = (await r.events(
          entryType: kDestinationWedgedEntryType,
        )).single;
        expect(await wedgesViewRows(r.backend), <String, Object?>{
          key: <String, Object?>{
            ...wedge.data,
            'aggregateId': key,
            'sequence': wedge.sequenceNumber,
          },
        });
        expect(wedge.data['row_id'], rowId);
        await expectWedgesViewMatchesQueue(r.store);

        await r.registry.tombstoneAndRefill('x', rowId, initiator: _init);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await expectWedgesViewMatchesQueue(r.store);

        await fillBatch(
          d,
          backend: r.backend,
          source: _source,
          clock: _fillNow,
        );
        final second = await wedgeHeadForTest(r.registry, 'x');
        expect(second, isNot(rowId));
        final rows = await wedgesViewRows(r.backend);
        expect(rows.keys, <String>[key]);
        expect(rows[key]!['row_id'], second);
        await expectWedgesViewMatchesQueue(r.store);

        await r.registry.deleteDestination('x', initiator: _init);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await expectWedgesViewMatchesQueue(r.store);
        await expectReservedShapes(r.store);
      });

      // Verifies: EVS-PRD-destinations/S
      // the view derives its rows from the wedge, recovery and deletion
      //   events alone: a registration and start-date audits leave it
      //   empty, a user event carrying the wedge event's event type and
      //   aggregate type creates no row, and a recovery for a destination
      //   with no row is a no-op.
      test('events that do not start a wedge touch no row', () async {
        if (!available) return;
        final d = FakeDestination(id: 'quiet');
        await r.queued(d);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await r.store.append(
          entryType: _noteType,
          aggregateId: 'lookalike',
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgedEventType,
          data: wedgeData(
            destinationId: 'quiet',
            databaseId: r.store.databaseId,
          ),
          initiator: _init,
        );
        expect(await wedgesViewRows(r.backend), isEmpty);
        final p = await peer();
        final recovery = forgedEvent(
          entryType: kDestinationWedgeRecoveredEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgeRecoveredEventType,
          data: <String, Object?>{
            'id': 'nothing-wedged',
            'database_id': p.store.databaseId,
            'row_id': 'r',
          },
        );
        await r.store.ingestEvent(recovery);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await expectWedgesViewMatchesQueue(r.store);
      });
    });

    // ------------------------------------------------------------------
    // The events that end a wedge
    // ------------------------------------------------------------------

    group('events that end a wedge', () {
      // Verifies: EVS-PRD-destinations/T
      // a recovery appends one recovery event naming the item, and a
      //   deletion of a wedged destination appends one deletion event naming
      //   the item it retires.
      test('recovery and deletion each append one event', () async {
        if (!available) return;
        final a = FakeDestination(id: 'a', allowHardDelete: true);
        final b = FakeDestination(id: 'b', allowHardDelete: true);
        final aRow = await r.wedge(a);
        final bRow = await r.wedge(b);
        await expectWedgesViewMatchesQueue(r.store);

        await r.registry.tombstoneAndRefill('a', aRow, initiator: _init);
        final recoveries = await r.events(
          entryType: kDestinationWedgeRecoveredEntryType,
        );
        expect(recoveries, hasLength(1));
        expect(recoveries.single.data['id'], 'a');
        expect(recoveries.single.data['row_id'], aRow);
        await expectWedgesViewMatchesQueue(r.store);

        await r.registry.deleteDestination('b', initiator: _init);
        final deletions = await r.events(
          entryType: kDestinationDeletedEntryType,
        );
        expect(deletions, hasLength(1));
        expect(deletions.single.data['id'], 'b');
        expect(deletions.single.data['tombstoned_row_id'], bRow);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await expectWedgesViewMatchesQueue(r.store);
      });

      // Verifies: EVS-PRD-destinations/T
      // deleting a destination that never wedged records no retired item
      //   and leaves the view unchanged.
      test('deleting a destination that never wedged', () async {
        if (!available) return;
        final wedged = FakeDestination(id: 'w');
        await r.wedge(wedged);
        final never = FakeDestination(id: 'n', allowHardDelete: true);
        await r.registry.addDestination(never, initiator: _init);
        final before = await wedgesViewRows(r.backend);
        await r.registry.deleteDestination('n', initiator: _init);
        final deletion = (await r.events(
          entryType: kDestinationDeletedEntryType,
        )).single;
        expect(deletion.data['tombstoned_row_id'], isNull);
        expect(await wedgesViewRows(r.backend), before);
        await expectWedgesViewMatchesQueue(r.store);
      });
    });

    // ------------------------------------------------------------------
    // The database identity on destination audits
    // ------------------------------------------------------------------

    group('database identity', () {
      // Verifies: EVS-DEV-destination-drain/K
      // every kind of destination audit event the library appends carries
      //   the appending database's identity.
      test(
        'every destination audit kind carries the database identity',
        () async {
          if (!available) return;
          final d = FakeDestination(id: 'k', allowHardDelete: true);
          final row = await r.wedge(d);
          await r.registry.setEndDate(
            'k',
            DateTime.utc(2100),
            initiator: _init,
          );
          await r.registry.tombstoneAndRefill('k', row, initiator: _init);
          await fillBatch(
            d,
            backend: r.backend,
            source: _source,
            clock: _fillNow,
          );
          await r.registry.requestHalt(
            'k',
            initiator: _init,
            purpose: HaltPurpose.pause,
          );
          await r.registry.cancelHalt('k', initiator: _init);
          await wedgeHeadForTest(r.registry, 'k');
          await r.registry.deleteDestination('k', initiator: _init);
          final audits = <StoredEvent>[
            for (final e in await r.events())
              if (kDestinationAuditEntryTypes.contains(e.entryType)) e,
          ];
          expect(
            audits.map((e) => e.entryType).toSet(),
            kDestinationAuditEntryTypes.toSet(),
            reason: 'one audit of every kind',
          );
          for (final audit in audits) {
            expect(
              audit.data['database_id'],
              r.store.databaseId,
              reason: audit.entryType,
            );
          }
          await expectReservedShapes(r.store);
        },
      );

      // Verifies: EVS-DEV-destination-drain/K
      // destinations of two databases with the same identifier get two
      //   rows; a local recovery removes only the local row.
      test('two databases, one destination identifier', () async {
        if (!available) return;
        final p = await peer();
        final d = FakeDestination(id: 'shared');
        await p.wedge(d);
        final peerWedge = (await p.events(
          entryType: kDestinationWedgedEntryType,
        )).single;
        await r.store.ingestEvent(peerWedge);
        final localRow = await r.wedge(FakeDestination(id: 'shared'));
        expect((await wedgesViewRows(r.backend)).keys.toSet(), <String>{
          '${p.store.databaseId}|shared',
          '${r.store.databaseId}|shared',
        });
        await expectWedgesViewMatchesQueue(r.store);

        await r.registry.tombstoneAndRefill(
          'shared',
          localRow,
          initiator: _init,
        );
        expect((await wedgesViewRows(r.backend)).keys, <String>[
          '${p.store.databaseId}|shared',
        ]);
        await expectWedgesViewMatchesQueue(r.store);
      });

      // Verifies: EVS-PRD-destinations/S
      // a peer's recovery and a peer's deletion, ingested after the peer's
      //   wedges, each remove only that peer's row; the receiver's own row
      //   for the same destination identifier stays.
      test("a peer's recovery and deletion remove only its rows", () async {
        if (!available) return;
        final p = await peer();
        final shared = FakeDestination(id: 'shared');
        final gone = FakeDestination(id: 'gone', allowHardDelete: true);
        final peerRow = await p.wedge(shared);
        await p.wedge(gone);
        for (final e in await p.events(
          entryType: kDestinationWedgedEntryType,
        )) {
          await r.store.ingestEvent(e);
        }
        await r.wedge(FakeDestination(id: 'shared'));
        final peerKey = '${p.store.databaseId}|';
        final localKey = '${r.store.databaseId}|shared';
        expect((await wedgesViewRows(r.backend)).keys.toSet(), <String>{
          '${peerKey}shared',
          '${peerKey}gone',
          localKey,
        });

        await p.registry.tombstoneAndRefill(
          'shared',
          peerRow,
          initiator: _init,
        );
        await r.store.ingestEvent(
          (await p.events(
            entryType: kDestinationWedgeRecoveredEntryType,
          )).single,
        );
        expect((await wedgesViewRows(r.backend)).keys.toSet(), <String>{
          '${peerKey}gone',
          localKey,
        });

        await p.registry.deleteDestination('gone', initiator: _init);
        await r.store.ingestEvent(
          (await p.events(entryType: kDestinationDeletedEntryType)).single,
        );
        expect((await wedgesViewRows(r.backend)).keys, <String>[localKey]);
        await expectWedgesViewMatchesQueue(r.store);
        await expectWedgesViewMatchesQueue(p.store);
      });

      // Verifies: EVS-PRD-destinations/S
      // a peer's recovery that arrives before the wedge it ends changes
      //   nothing, and the wedge that arrives after it inserts a row that
      //   stays: rows for another database reflect its queue only when its
      //   wedges arrive before the events that end them.
      test("a peer's recovery ingested before its wedge", () async {
        if (!available) return;
        final p = await peer();
        final row = await p.wedge(FakeDestination(id: 'late'));
        await p.registry.tombstoneAndRefill('late', row, initiator: _init);
        final wedge = (await p.events(
          entryType: kDestinationWedgedEntryType,
        )).single;
        final recovery = (await p.events(
          entryType: kDestinationWedgeRecoveredEntryType,
        )).single;
        await r.store.ingestEvent(recovery);
        expect(await wedgesViewRows(r.backend), isEmpty);
        await r.store.ingestEvent(wedge);
        expect((await wedgesViewRows(r.backend)).keys, <String>[
          '${p.store.databaseId}|late',
        ]);
        expect(await p.backend.wedgedFifos(), isEmpty);
        await expectWedgesViewMatchesQueue(r.store);
      });

      // Verifies: EVS-PRD-destinations/S
      // a peer's wedge event carrying only the destination identifier and
      //   the database identity is admitted, and its row carries only the
      //   fields the event has: rows for another database hold what the peer
      //   asserts.
      test("a peer's wedge event with only the key fields", () async {
        if (!available) return;
        final sparse = forgedEvent(
          entryType: kDestinationWedgedEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgedEventType,
          data: const <String, Object?>{'id': 's', 'database_id': 'peer-db'},
        );
        await r.store.ingestEvent(sparse);
        final stored = (await r.events(
          entryType: kDestinationWedgedEntryType,
        )).single;
        expect(await wedgesViewRows(r.backend), <String, Object?>{
          'peer-db|s': <String, Object?>{
            'id': 's',
            'database_id': 'peer-db',
            'aggregateId': 'peer-db|s',
            'sequence': stored.sequenceNumber,
          },
        });
        await expectWedgesViewMatchesQueue(r.store);
      });

      for (final bad in <String>['b|c', '']) {
        // Verifies: EVS-DEV-destination-drain/K
        // a destination identifier that is empty or contains `|` is refused
        //   by registration, with nothing written but the registry check
        //   record.
        test('registration refuses the identifier "$bad"', () async {
          if (!available) return;
          final before = await r.snapshot();
          final schedulesBefore = await r.backend.transaction(
            (txn) => r.backend.readScheduleTxn(txn, bad),
          );
          await expectLater(
            r.registry.addDestination(
              FakeDestination(id: bad),
              initiator: _init,
            ),
            throwsA(
              isA<ArgumentError>().having(
                (e) => e.message.toString(),
                'message',
                contains('destination identifier'),
              ),
            ),
          );
          expect(await r.snapshot(), before);
          expect(schedulesBefore, isNull);
          expect(
            await r.backend.transaction(
              (txn) => r.backend.readScheduleTxn(txn, bad),
            ),
            isNull,
          );
          expect(r.registry.byId(bad), isNull);
          final check = await r.backend.transaction(
            r.backend.readRegistryCheckTxn,
          );
          expect(check?.op, 'addDestination');
          expect(check?.outcome, 'refused_invalid_identifier');
          await r.expectCounterUnchanged();
        });
      }
    });

    // ------------------------------------------------------------------
    // Reserved appends through the public operations
    // ------------------------------------------------------------------

    group('public appends of reserved entry types', () {
      for (final id in kReservedSystemEntryTypeIds) {
        // Verifies: EVS-DEV-destination-drain/L
        // `append` and `appendInTxn` refuse every reserved system entry type
        //   (the table is read from the reserved set) and change nothing: no
        //   event, no view row, the sequence counter unchanged.
        test('append refuses $id', () async {
          if (!available) return;
          await r.wedge(FakeDestination(id: 'x'));
          final before = await r.snapshot();
          final shape = kReservedEventShapes[id]!;
          await expectLater(
            r.store.append(
              entryType: id,
              aggregateId: r.store.source.identifier,
              aggregateType: shape.aggregateType,
              eventType: shape.eventTypes.first,
              data: wedgeData(
                destinationId: 'x',
                databaseId: r.store.databaseId,
              ),
              initiator: _init,
            ),
            throwsA(
              isA<ArgumentError>().having(
                (e) => e.message.toString(),
                'message',
                contains('reserved'),
              ),
            ),
          );
          expect(await r.snapshot(), before);
          await r.expectCounterUnchanged();
        });

        // Verifies: EVS-DEV-destination-drain/L
        // `append` and `appendInTxn` refuse every reserved system entry type
        //   (the table is read from the reserved set) and change nothing: no
        //   event, no view row, the sequence counter unchanged.
        test('appendInTxn refuses $id', () async {
          if (!available) return;
          final before = await r.snapshot();
          final shape = kReservedEventShapes[id]!;
          await expectLater(
            r.store.runTransaction((txn, collector) async {
              await r.store.appendInTxn(
                txn,
                collector: collector,
                entryType: id,
                aggregateId: r.store.source.identifier,
                aggregateType: shape.aggregateType,
                eventType: shape.eventTypes.first,
                data: wedgeData(
                  destinationId: 'x',
                  databaseId: r.store.databaseId,
                ),
                initiator: _init,
                flowToken: null,
                metadata: null,
                security: null,
                checkpointReason: null,
                changeReason: null,
                dedupeByContent: false,
              );
            }),
            throwsA(isA<ArgumentError>()),
          );
          expect(await r.snapshot(), before);
          await r.expectCounterUnchanged();
        });
      }
    });

    // ------------------------------------------------------------------
    // Ingest refusals
    // ------------------------------------------------------------------

    group('ingest refusals', () {
      final malformed = <String, Map<String, Object?> Function(String)>{
        'missing data.id': (dbId) => wedgeData(
          destinationId: 'm',
          databaseId: 'peer-db',
          remove: {'id'},
        ),
        'missing data.database_id': (dbId) => wedgeData(
          destinationId: 'm',
          databaseId: null,
          remove: {'database_id'},
        ),
        'null data.database_id': (dbId) =>
            wedgeData(destinationId: 'm', databaseId: null),
        'an empty data.id': (dbId) =>
            wedgeData(destinationId: '', databaseId: 'peer-db'),
        'a numeric data.database_id': (dbId) =>
            wedgeData(destinationId: 'm', databaseId: 42),
        'a data.id containing |': (dbId) =>
            wedgeData(destinationId: 'a|b', databaseId: 'peer-db'),
        // A peer's database identity that begins with the receiver's own
        // key prefix: its row key would read as a local row.
        'a data.database_id containing |': (dbId) =>
            wedgeData(destinationId: 'm', databaseId: '$dbId|z'),
      };

      for (final path in _ingestPaths.entries) {
        for (final c in malformed.entries) {
          // Verifies: EVS-DEV-destination-drain/L
          // an ingested reserved destination audit whose destination
          //   identifier or database identity is missing, empty, not a
          //   string, or contains `|` is refused as malformed, before any
          //   write, and the same batch without it ingests.
          test('${path.key} refuses a wedge event with ${c.key}', () async {
            if (!available) return;
            final good = forgedEvent(
              entryType: kDestinationWedgedEntryType,
              aggregateType: kDestinationAuditAggregateType,
              eventType: kDestinationWedgedEventType,
              data: wedgeData(destinationId: 'good', databaseId: 'peer-db'),
            );
            final bad = forgedEvent(
              entryType: kDestinationWedgedEntryType,
              aggregateType: kDestinationAuditAggregateType,
              eventType: kDestinationWedgedEventType,
              data: c.value(r.store.databaseId),
            );
            // Through ingestEvent each event commits on its own, so the good
            // one commits before the bad one is refused.
            final batch = path.key == 'ingestBatch';
            if (!batch) await path.value(r.store, <StoredEvent>[good]);
            final before = await r.snapshot();
            await expectLater(
              path.value(r.store, <StoredEvent>[if (batch) good, bad]),
              throwsA(
                _refused(ReservedEventRefusal.malformed, eventId: bad.eventId),
              ),
            );
            expect(await r.snapshot(), before);
            await r.expectCounterUnchanged();
            if (batch) await path.value(r.store, <StoredEvent>[good]);
            expect(
              (await wedgesViewRows(r.backend)).keys,
              contains('peer-db|good'),
            );
            await expectWedgesViewMatchesQueue(r.store);
          });
        }
      }

      // The reserved entry types that are not destination audits.
      final nonAudit = <String>[
        for (final id in kReservedSystemEntryTypeIds)
          if (!kDestinationAuditEntryTypes.contains(id)) id,
      ];

      for (final path in _ingestPaths.entries) {
        for (final withId in <bool>[true, false]) {
          // Verifies: EVS-DEV-destination-drain/L
          // a reserved entry type carrying an aggregate type or event type
          //   the library does not declare for it (here the wedge event's
          //   pair, naming a healthy local destination) is refused as a
          //   shape mismatch before any write, whether or not it carries a
          //   destination identifier; the view gains no row.
          test(
            '${path.key} refuses other reserved types in the wedge '
            "event's shape (${withId ? 'with' : 'without'} data.id)",
            () async {
              if (!available) return;
              await r.queued(FakeDestination(id: 'healthy'));
              for (final id in nonAudit) {
                final before = await r.snapshot();
                final forged = forgedEvent(
                  entryType: id,
                  aggregateType: kDestinationAuditAggregateType,
                  eventType: kDestinationWedgedEventType,
                  data: wedgeData(
                    destinationId: 'healthy',
                    databaseId: r.store.databaseId,
                    remove: withId ? const <String>{} : const <String>{'id'},
                  ),
                );
                await expectLater(
                  path.value(r.store, <StoredEvent>[forged]),
                  throwsA(
                    _refused(
                      ReservedEventRefusal.shapeMismatch,
                      eventId: forged.eventId,
                    ),
                  ),
                  reason: id,
                );
                expect(await r.snapshot(), before, reason: id);
                expect(await wedgesViewRows(r.backend), isEmpty, reason: id);
              }
              await r.expectCounterUnchanged();
              await expectWedgesViewMatchesQueue(r.store);
            },
          );
        }

        // Verifies: EVS-DEV-destination-drain/L
        // a reserved destination audit type under a foreign aggregate type
        //   is refused as a shape mismatch.
        test('${path.key} refuses a wedge event under another aggregate '
            'type', () async {
          if (!available) return;
          final before = await r.snapshot();
          final forged = forgedEvent(
            entryType: kDestinationWedgedEntryType,
            aggregateType: 'note',
            eventType: kDestinationWedgedEventType,
            data: wedgeData(destinationId: 'x', databaseId: 'peer-db'),
          );
          await expectLater(
            path.value(r.store, <StoredEvent>[forged]),
            throwsA(_refused(ReservedEventRefusal.shapeMismatch)),
          );
          expect(await r.snapshot(), before);
        });

        // Verifies: EVS-DEV-destination-drain/L
        // an ingested destination audit naming the receiver's own database,
        //   which the receiver does not hold, is refused and the view row
        //   stays.
        test(
          '${path.key} refuses a recovery naming the receiver database',
          () async {
            if (!available) return;
            final row = await r.wedge(FakeDestination(id: 'x'));
            final before = await r.snapshot();
            final forged = forgedEvent(
              entryType: kDestinationWedgeRecoveredEntryType,
              aggregateType: kDestinationAuditAggregateType,
              eventType: kDestinationWedgeRecoveredEventType,
              data: <String, Object?>{
                'id': 'x',
                'database_id': r.store.databaseId,
                'row_id': row,
              },
            );
            await expectLater(
              path.value(r.store, <StoredEvent>[forged]),
              throwsA(
                _refused(
                  ReservedEventRefusal.namesReceiverDatabase,
                  eventId: forged.eventId,
                ),
              ),
            );
            expect(await r.snapshot(), before);
            expect((await wedgesViewRows(r.backend)).keys, <String>[
              '${r.store.databaseId}|x',
            ]);
            await r.expectCounterUnchanged();
            await expectWedgesViewMatchesQueue(r.store);
          },
        );

        // Verifies: EVS-DEV-destination-drain/L
        // an event the receiver already holds is not refused by the
        //   reserved-event checks: a peer's wedge event delivered twice is a
        //   duplicate.
        test('${path.key} accepts the redelivery of a held event', () async {
          if (!available) return;
          final p = await peer();
          await p.wedge(FakeDestination(id: 'x'));
          final peerWedge = (await p.events(
            entryType: kDestinationWedgedEntryType,
          )).single;
          await path.value(r.store, <StoredEvent>[peerWedge]);
          final rows = await wedgesViewRows(r.backend);
          await path.value(r.store, <StoredEvent>[peerWedge]);
          expect(await wedgesViewRows(r.backend), rows);
          expect(
            await r.events(entryType: kDestinationWedgedEntryType),
            hasLength(1),
          );
          await expectWedgesViewMatchesQueue(r.store);
        });

        // Verifies: EVS-DEV-destination-drain/L
        // the receiver's own destination audit presented back to it is held,
        //   so the receiver-database refusal does not apply; the existing
        //   identity check decides it.
        test(
          '${path.key} leaves a held own audit to the identity check',
          () async {
            if (!available) return;
            await r.wedge(FakeDestination(id: 'x'));
            final own = (await r.events(
              entryType: kDestinationWedgedEntryType,
            )).single;
            await expectLater(
              path.value(r.store, <StoredEvent>[own]),
              throwsA(isA<IngestIdentityMismatch>()),
            );
          },
        );

        // Verifies: EVS-DEV-destination-drain/L
        // a user entry type under the destination audit aggregate type,
        //   naming the receiver's database, ingests normally and creates no
        //   view row: the checks apply to reserved entry types only.
        test('${path.key} ingests a user event in the audit shape', () async {
          if (!available) return;
          final lookalike = forgedEvent(
            entryType: _noteType,
            aggregateType: kDestinationAuditAggregateType,
            eventType: kDestinationWedgedEventType,
            data: wedgeData(destinationId: 'x', databaseId: r.store.databaseId),
          );
          await path.value(r.store, <StoredEvent>[lookalike]);
          expect(
            (await r.events()).map((e) => e.eventId),
            contains(lookalike.eventId),
          );
          expect(await wedgesViewRows(r.backend), isEmpty);
        });

        // Verifies: EVS-DEV-destination-drain/L
        // a peer's library-version event in the shape the library appends
        //   it is admitted.
        test('${path.key} admits a peer library-version event', () async {
          if (!available) return;
          final initialized = forgedEvent(
            entryType: kLibVersionInitializedEntryType,
            aggregateType: '_lib',
            eventType: kLibVersionInitializedEntryType,
            data: <String, Object?>{
              'version': '0.0.1',
              'data_format': LibVersion.dataFormat.toJson(),
              'database_id': 'peer-db',
              'initializedAt': '2026-09-01T12:00:00.000Z',
            },
          );
          await path.value(r.store, <StoredEvent>[initialized]);
          expect(
            (await r.events()).map((e) => e.eventId),
            contains(initialized.eventId),
          );
        });
      }

      for (final missing in <String>['id', 'database_id']) {
        // Verifies: EVS-DEV-destination-drain/L
        // a rebuild of the default view over a log into which a wedge event
        //   missing its destination identifier, or its database identity,
        //   was written outside the library fails naming the event, and
        //   leaves the view unchanged.
        test('rebuild over a wedge event missing data.$missing', () async {
          if (!available) return;
          await r.wedge(FakeDestination(id: 'ok'));
          final before = await wedgesViewRows(r.backend);
          final written = await r.backend.transaction((txn) async {
            final seq = await r.backend.nextSequenceNumber(txn);
            final previous = await r.backend.readLatestEventHash(txn);
            final base = forgedEvent(
              entryType: kDestinationWedgedEntryType,
              aggregateType: kDestinationAuditAggregateType,
              eventType: kDestinationWedgedEventType,
              data: wedgeData(
                destinationId: 'raw',
                databaseId: 'raw-db',
                remove: <String>{missing},
              ),
            );
            final map = Map<String, Object?>.from(base.toMap())
              ..['sequence_number'] = seq
              ..['previous_event_hash'] = previous;
            final event = StoredEvent.fromMap(map, seq);
            await r.backend.appendEvent(txn, event);
            return event;
          });
          await expectLater(
            rebuildView(
              store: r.store,
              viewName: defaultDestinationWedgesSpec.viewName,
              targetVersionByEntryType: wedgesViewTargets(r.store),
            ),
            throwsA(
              isA<StateError>().having(
                (e) => e.message,
                'message',
                contains(written.eventId),
              ),
            ),
          );
          expect(await wedgesViewRows(r.backend), before);
        });
      }
    });

    // ------------------------------------------------------------------
    // Library emitters
    // ------------------------------------------------------------------

    group('library emitters', () {
      // Verifies: EVS-DEV-destination-drain/L
      // with the public appends refusing reserved entry types, bootstrap
      //   still appends the registry audit through the library's own path,
      //   and a reboot with the same registry appends none (dedupe by
      //   content), although destination audits were appended after it in
      //   the same aggregate.
      test(
        'bootstrap appends the registry audit once per registry state',
        () async {
          if (!available) return;
          const source = Source(
            hopId: 'server',
            identifier: 'bootstrap-install',
            softwareVersion: 'test@1.0.0',
          );
          Future<EventStoreBundle> boot() async => bootstrapEventStore(
            backend: await db.openBackend(),
            source: source,
            entryTypes: const <EntryTypeDefinition>[_noteDef],
            destinations: const <Destination>[],
          );
          final first = await boot();
          Future<List<StoredEvent>> audits() => first.eventStore.backend
              .findAllEvents(entryType: kEntryTypeRegistryInitializedEntryType);
          expect(await audits(), hasLength(1));
          await first.destinations.addDestination(
            FakeDestination(id: 'between'),
            initiator: _init,
          );
          await first.destinations.setStartDate(
            'between',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          );
          final registryAudit = (await audits()).single;
          final after = <StoredEvent>[
            for (final e in await first.eventStore.backend.findAllEvents())
              if (e.aggregateId == source.identifier &&
                  e.sequenceNumber > registryAudit.sequenceNumber)
                e,
          ];
          expect(registryAudit.aggregateId, source.identifier);
          expect(after.map((e) => e.entryType), <String>[
            kDestinationRegisteredEntryType,
            kDestinationStartDateSetEntryType,
          ]);
          await boot();
          expect(await audits(), hasLength(1));
          await expectReservedShapes(first.eventStore);
        },
      );

      // Verifies: EVS-DEV-destination-drain/L
      // every reserved event the library's emitters append (the library
      //   version, the registry audit, destination audits, the halt request
      //   and cancellation, the wedge event,
      //   the redaction, compaction, purge and retention audits, and the
      //   ingest audits) carries the aggregate type and an event type the
      //   library declares for its entry type; each security-context audit
      //   carries its own event type.
      test('every emitter appends a declared shape', () async {
        if (!available) return;
        final bundle = await bootstrapEventStore(
          backend: await db.openBackend(),
          source: const Source(
            hopId: 'server',
            identifier: 'emitters-install',
            softwareVersion: 'test@1.0.0',
          ),
          entryTypes: const <EntryTypeDefinition>[_noteDef],
          destinations: const <Destination>[],
        );
        final store = bundle.eventStore;
        final s = _Store(store.backend, store, bundle.destinations);
        final d = FakeDestination(id: 'e', allowHardDelete: true);
        await s.queued(d);
        await s.registry.requestHalt(
          'e',
          initiator: _init,
          purpose: HaltPurpose.pause,
        );
        await s.registry.cancelHalt('e', initiator: _init);
        final row = await wedgeHeadForTest(s.registry, 'e');
        await s.registry.setEndDate('e', DateTime.utc(2100), initiator: _init);
        await s.registry.tombstoneAndRefill('e', row, initiator: _init);
        await s.registry.deleteDestination('e', initiator: _init);
        final secured = await store.append(
          entryType: _noteType,
          aggregateId: 'secured',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{},
          initiator: _init,
          security: const SecurityDetails(ipAddress: '10.0.0.1'),
        );
        await store.clearSecurityContext(
          secured!.eventId,
          reason: 'test',
          redactedBy: _init,
        );
        // An unredacted context for the retention sweep to compact and purge.
        await store.append(
          entryType: _noteType,
          aggregateId: 'swept',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{},
          initiator: _init,
          security: const SecurityDetails(ipAddress: '10.0.0.2'),
        );
        await Future<void>.delayed(const Duration(milliseconds: 2));
        await store.applyRetentionPolicy(
          policy: const SecurityRetentionPolicy(
            fullRetention: Duration.zero,
            truncatedRetention: Duration.zero,
          ),
        );
        final p = await peer();
        final peerNote = await p.note('peer-note');
        await store.ingestEvent(peerNote);
        await store.ingestEvent(peerNote);
        final bytes = _batchOf(<StoredEvent>[peerNote]);
        await store.logRejectedBatch(
          bytes,
          wireFormat: BatchEnvelope.wireFormat,
          reason: 'test',
        );
        final kinds = <String>{
          for (final e in await store.backend.findAllEvents())
            if (kReservedSystemEntryTypeIds.contains(e.entryType)) e.entryType,
        };
        expect(
          kinds,
          containsAll(<String>[
            kLibVersionInitializedEntryType,
            kEntryTypeRegistryInitializedEntryType,
            ...kDestinationAuditEntryTypes,
            kSecurityContextRedactedEntryType,
            kSecurityContextCompactedEntryType,
            kSecurityContextPurgedEntryType,
            kRetentionPolicyAppliedEntryType,
            kIngestAuditEntryType,
          ]),
        );
        await expectReservedShapes(store);
        for (final pair in <String, String>{
          kSecurityContextRedactedEntryType: kSecurityContextRedactedEventType,
          kSecurityContextCompactedEntryType:
              kSecurityContextCompactedEventType,
          kSecurityContextPurgedEntryType: kSecurityContextPurgedEventType,
        }.entries) {
          expect(
            (await store.backend.findAllEvents(
              entryType: pair.key,
            )).map((e) => e.eventType).toSet(),
            <String>{pair.value},
            reason: pair.key,
          );
        }
      });
    });

    // ------------------------------------------------------------------
    // Registration at open
    // ------------------------------------------------------------------

    group('registration at open', () {
      // Verifies: EVS-DEV-destination-drain/M
      // an open with a registry holding no reserved entry type registers
      //   them all and the default view before the boot, so the store can
      //   wedge, the view exists without any registration by the consumer,
      //   and the generation record carries the reserved types' majors.
      test('an open registers the reserved types and the view', () async {
        if (!available) return;
        for (final d in kSystemEntryTypes) {
          expect(identical(r.store.entryTypes.byId(d.id), d), isTrue);
        }
        expect(
          identical(
            r.store.projections.lookup(defaultDestinationWedgesSpec.viewName),
            defaultDestinationWedgesSpec,
          ),
          isTrue,
        );
        final targets = await r.backend.transaction(
          (txn) => r.backend.readAllViewTargetVersionsInTxn(
            txn,
            defaultDestinationWedgesSpec.viewName,
          ),
        );
        expect(targets, wedgesViewTargets(r.store));
        final generation = await r.backend.transaction(
          r.backend.readDataGenerationTxn,
        );
        expect(generation, isNotNull);
        expect(
          generation!.entryTypeMajors,
          <String, int>{
            _noteType: _noteDef.registeredVersion.major,
            for (final d in kSystemEntryTypes) d.id: d.registeredVersion.major,
          },
          reason: 'the reserved types are part of the data generation',
        );
        await r.wedge(FakeDestination(id: 'x'));
        expect(await wedgesViewRows(r.backend), hasLength(1));
      });

      // Verifies: EVS-DEV-destination-drain/M
      // a caller registry holding the library's own definitions is
      //   accepted, and two opens with the same registries both succeed.
      test('the library definitions, and the same registries twice', () async {
        if (!available) return;
        final entryTypes = EntryTypeRegistry()..register(_noteDef);
        for (final d in kSystemEntryTypes) {
          entryTypes.register(d);
        }
        final projections = ProjectionRegistry()
          ..register(defaultDestinationWedgesSpec);
        final first = await openStore(
          entryTypes: entryTypes,
          projections: projections,
        );
        final second = await openStore(
          entryTypes: entryTypes,
          projections: projections,
        );
        expect(second.store.databaseId, first.store.databaseId);
        expect(entryTypes.all(), hasLength(kSystemEntryTypes.length + 1));
        await second.wedge(FakeDestination(id: 'twice'));
        await expectWedgesViewMatchesQueue(first.store);
      });

      for (final variant in <String, EntryTypeDefinition>{
        'another version': const EntryTypeDefinition(
          id: kDestinationWedgedEntryType,
          registeredVersion: EntryTypeVersion(2, 0),
          name: 'Destination Wedged',
        ),
        'an equal copy': EntryTypeDefinition(
          id: kDestinationWedgedEntryType,
          registeredVersion: const EntryTypeVersion(1, 0),
          name: kSystemEntryTypes
              .firstWhere((d) => d.id == kDestinationWedgedEntryType)
              .name,
        ),
      }.entries) {
        // Verifies: EVS-DEV-destination-drain/M
        // a caller registry holding a reserved entry type under a
        //   definition that is not the library's (another version, or an
        //   equal copy) is refused before anything is written.
        test('a reserved entry type under ${variant.key} is refused', () async {
          if (!available) return;
          final before = await r.snapshot();
          final entryTypes = EntryTypeRegistry()
            ..register(_noteDef)
            ..register(variant.value);
          await expectLater(
            openStore(entryTypes: entryTypes),
            throwsA(
              isA<ArgumentError>().having(
                (e) => e.message.toString(),
                'message',
                allOf(
                  contains('reserved'),
                  contains(kDestinationWedgedEntryType),
                ),
              ),
            ),
          );
          expect(entryTypes.all(), hasLength(2), reason: 'registry untouched');
          expect(await r.snapshot(), before);
        });
      }

      // Verifies: EVS-DEV-destination-drain/M
      // a sealed projection registry that lacks the default view is refused
      //   naming the view, before either registry changes and before
      //   anything is written.
      test('a sealed registry without the view is refused', () async {
        if (!available) return;
        final before = await r.snapshot();
        final entryTypes = EntryTypeRegistry()..register(_noteDef);
        final projections = ProjectionRegistry()..seal();
        await expectLater(
          openStore(entryTypes: entryTypes, projections: projections),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(
                contains('sealed'),
                contains(defaultDestinationWedgesSpec.viewName),
              ),
            ),
          ),
        );
        expect(entryTypes.all(), hasLength(1), reason: 'registry untouched');
        expect(projections.all(), isEmpty);
        expect(await r.snapshot(), before);
      });

      // Verifies: EVS-DEV-destination-drain/M
      // a consumer spec registered under the default view's name is
      //   refused with the reserved-view message, before anything is
      //   written.
      test('another spec under the view name is refused', () async {
        if (!available) return;
        final before = await r.snapshot();
        final projections = ProjectionRegistry()
          ..register(
            TableProjectionSpec(
              viewName: defaultDestinationWedgesSpec.viewName,
              interest: const SubscriptionFilter(entryTypes: {_noteType}),
              insertEventTypes: const {'finalized'},
              removeEventTypes: const <String>{},
              rowKey: const AggregateIdKey(),
              rowData: const WholePayload(),
            ),
          );
        await expectLater(
          openStore(projections: projections),
          throwsA(
            isA<ArgumentError>().having(
              (e) => e.message.toString(),
              'message',
              allOf(
                contains('reserved'),
                contains(defaultDestinationWedgesSpec.viewName),
              ),
            ),
          ),
        );
        expect(await r.snapshot(), before);
      });
    });
  });
}
