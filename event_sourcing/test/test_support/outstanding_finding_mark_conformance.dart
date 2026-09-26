// Backend-agnostic scenarios for the outstanding-finding mark the default
// views carry: every row holds `$integrity`, the security findings that
// mark its aggregate; every view folds every finding whatever its interest;
// a received finding marks only what its originating database authored; a
// reused origin position or a fork marks the aggregates of the forked
// database's events at or above it, events folded later included; a
// rebuild derives the rows the incremental fold did; and the `$` prefix is
// refused on append. Run on Sembast by
// test/projections/outstanding_finding_mark_test.dart and on Postgres by
// test/storage/postgres/postgres_outstanding_finding_mark_test.dart.
//
// This file exposes [runOutstandingFindingMarkScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'ingest_chain_findings_conformance.dart' show chained, originChain;
import 'ingest_record_findings_conformance.dart' show envelopeOf, sealedRecord;
import 'record_fixtures.dart' show kPeerDatabaseId;
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const String _kType = 'finding_note';
const String _kNotes = 'marked_notes';
const String _kSlots = 'marked_slots';

/// An aggregate view whose interest holds only the application's entry
/// type, so no security finding is in it.
const AggregateProjectionSpec _kNotesSpec = AggregateProjectionSpec(
  viewName: _kNotes,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{'deleted'},
);

/// A table view keyed by a payload field, so a row's key is not its
/// aggregate.
const TableProjectionSpec _kSlotsSpec = TableProjectionSpec(
  viewName: _kSlots,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{'deleted'},
  rowKey: CompositeKey(<String>['data.title']),
  rowData: WholePayload(),
);

const Source _receiverSource = Source(
  hopId: 'receiver-hop',
  identifier: 'receiver-install',
  softwareVersion: 'receiver-app@1.0.0',
);

/// The `$integrity` value of a row marked by [findingIds].
Map<String, Object?> integrityOf(List<String> findingIds) => <String, Object?>{
  'security_findings': findingIds,
};

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runOutstandingFindingMarkScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('outstanding-finding mark ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open() async {
      final db = (await openDatabase())!;
      databases.add(db);
      final backend = await db.openBackend();
      final store = await EventStore.open(
        storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
        entryTypes: EntryTypeRegistry()
          ..register(
            const EntryTypeDefinition(
              id: _kType,
              registeredVersion: EntryTypeVersion(1, 0),
              name: _kType,
            ),
          ),
        projections: ProjectionRegistry()
          ..register(_kNotesSpec)
          ..register(_kSlotsSpec),
        source: _receiverSource,
      );
      opened.add(store);
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

    Future<IngestBatchResult> deliver(
      EventStore store,
      List<Map<String, Object?>> records,
    ) => store.ingestBatch(
      envelopeOf(records).encode(),
      wireFormat: BatchEnvelope.wireFormat,
    );

    Future<Object?> integrityOfRow(EventStore store, String aggregateId) async {
      final rows = await store.reader.readViewRowsByKeys(_kNotes, <String>{
        aggregateId,
      });
      expect(rows[aggregateId], isNotNull, reason: 'row of $aggregateId');
      return rows[aggregateId]![r'$integrity'];
    }

    Future<Object?> integrityOfSlot(EventStore store, String slot) async {
      final rows = await store.reader.readViewRowsByKeys(_kSlots, <String>{
        slot,
      });
      expect(rows[slot], isNotNull, reason: 'slot row $slot');
      return rows[slot]![r'$integrity'];
    }

    String ingestFindingId(
      EventStore store,
      String kind,
      Map<String, Object?> evidence,
    ) => expectedFindingId(
      databaseId: store.databaseId,
      role: 'ingest',
      kind: kind,
      evidence: evidence,
    );

    String agg(Map<String, Object?> record) =>
        record['aggregate_id']! as String;

    String title(Map<String, Object?> record) =>
        (record['data']! as Map)['title']! as String;

    // Verifies: EVS-PRD-materializer/F
    test('every row of an aggregate view and a table view carries '
        r'$integrity with an empty list when no finding marks it', () async {
      final store = await open();
      await store.append(
        entryType: _kType,
        aggregateId: 'note-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': 'slot-1'},
        initiator: const UserInitiator('u1'),
      );
      final note = (await store.reader.findViewRows(_kNotes)).single;
      expect(note[r'$integrity'], integrityOf(const <String>[]));
      expect(note['title'], 'slot-1');
      final slot = (await store.reader.findViewRows(_kSlots)).single;
      expect(slot[r'$integrity'], integrityOf(const <String>[]));
      expect(slot['aggregateId'], 'slot-1');
    });

    // Verifies: EVS-PRD-materializer/D
    // Verifies: EVS-PRD-materializer/G
    test('a hash_mismatch ingest records marks its aggregate in a view '
        'whose interest excludes findings, and the table row its event '
        'produced; other rows stay unmarked', () async {
      final store = await open();
      final clean = sealedRecord(entryType: _kType);
      final sealed = sealedRecord(entryType: _kType);
      final tampered = <String, Object?>{
        ...sealed,
        'data': <String, Object?>{'title': 'changed after sealing'},
      };
      await deliver(store, <Map<String, Object?>>[clean, tampered]);
      final id = ingestFindingId(store, 'hash_mismatch', <String, Object?>{
        'event_id': sealed['event_id'],
        'carried_hash': sealed['event_hash'],
        'recomputed_hash': canonicalEventHash(tampered),
      });
      expect(
        await integrityOfRow(store, agg(tampered)),
        integrityOf(<String>[id]),
      );
      expect(
        await integrityOfSlot(store, 'changed after sealing'),
        integrityOf(<String>[id]),
      );
      expect(
        await integrityOfRow(store, agg(clean)),
        integrityOf(const <String>[]),
      );
      expect(
        await integrityOfSlot(store, title(clean)),
        integrityOf(const <String>[]),
      );
    });

    // Verifies: EVS-PRD-materializer/E
    test('a position_reused finding about a database marks the aggregates '
        'of its events at or above the position, an event folded after the '
        'finding included, and not those below it', () async {
      final store = await open();
      const db = 'reused-db';
      final chain = originChain(db, 3);
      await deliver(store, chain);
      final second = chained(db, 2, previous: chain[0]);
      await deliver(store, <Map<String, Object?>>[second]);
      final id = ingestFindingId(store, 'position_reused', <String, Object?>{
        'database_id': db,
        'origin_sequence_number': 2,
      });
      final later = chained(db, 4, previous: chain[2]);
      await deliver(store, <Map<String, Object?>>[later]);

      expect(
        await integrityOfRow(store, agg(chain[0])),
        integrityOf(const <String>[]),
      );
      for (final record in <Map<String, Object?>>[
        chain[1],
        chain[2],
        second,
        later,
      ]) {
        expect(
          await integrityOfRow(store, agg(record)),
          integrityOf(<String>[id]),
          reason: 'origin position ${record['sequence_number']}',
        );
        expect(
          await integrityOfSlot(store, title(record)),
          integrityOf(<String>[id]),
          reason: 'slot of origin position ${record['sequence_number']}',
        );
      }
      expect(
        await integrityOfSlot(store, title(chain[0])),
        integrityOf(const <String>[]),
      );
    });

    // Verifies: EVS-PRD-materializer/E
    test('a fork_unrecorded finding marks the aggregates of the forked '
        "database's events at or above the lowest position among the "
        'events carrying the named predecessor', () async {
      final store = await open();
      const db = 'forked-db';
      final chain = originChain(db, 3);
      await deliver(store, chain);
      final fork = chained(db, 5, previous: chain[0]);
      await deliver(store, <Map<String, Object?>>[fork]);
      final id = ingestFindingId(store, 'fork_unrecorded', <String, Object?>{
        'database_id': db,
        'previous_event_hash': chain[0]['event_hash'],
      });
      expect(
        await integrityOfRow(store, agg(chain[0])),
        integrityOf(const <String>[]),
      );
      for (final record in <Map<String, Object?>>[chain[1], chain[2], fork]) {
        expect(
          await integrityOfRow(store, agg(record)),
          integrityOf(<String>[id]),
          reason: 'origin position ${record['sequence_number']}',
        );
      }
    });

    // Verifies: EVS-PRD-materializer/D
    test('a received finding marks an aggregate its originating database '
        "authored and not a third database's aggregate it names", () async {
      final store = await open();
      const thirdDb = 'third-db';
      final ownByPeer = sealedRecord(entryType: _kType);
      final third = sealedRecord(databaseId: thirdDb, entryType: _kType);
      await deliver(store, <Map<String, Object?>>[ownByPeer, third]);

      final aggregates = <String>[agg(ownByPeer), agg(third)]..sort();
      const receivedId = 'received-finding-of-the-peer';
      final received = sealedRecord(
        entryType: kSecurityFindingEntryType,
        aggregateType: 'security_finding',
        eventType: 'security_finding_recorded',
        aggregateId: receivedId,
        data: <String, Object?>{
          'finding_id': receivedId,
          'kind': 'hash_mismatch',
          'evidence': <String, Object?>{
            'event_id': 'e',
            'carried_hash': 'a',
            'recomputed_hash': 'b',
          },
          'aggregates': aggregates,
          'detector': <String, Object?>{
            'database_id': kPeerDatabaseId,
            'role': 'ingest',
            'library_version': '0.0.0',
          },
        },
      );
      final result = await deliver(store, <Map<String, Object?>>[received]);
      expect(result.events.single.outcome, IngestOutcome.ingested);

      expect(
        await integrityOfRow(store, agg(ownByPeer)),
        integrityOf(const <String>[receivedId]),
      );
      expect(
        await integrityOfRow(store, agg(third)),
        integrityOf(const <String>[]),
      );
    });

    // Verifies: EVS-PRD-materializer/B
    test('a rebuild derives byte-identical rows, marks included', () async {
      final store = await open();
      const db = 'rebuilt-db';
      final chain = originChain(db, 3);
      await deliver(store, chain);
      await deliver(store, <Map<String, Object?>>[
        chained(db, 2, previous: chain[0]),
      ]);
      await deliver(store, <Map<String, Object?>>[
        chained(db, 4, previous: chain[2]),
      ]);
      final sealed = sealedRecord(entryType: _kType);
      await deliver(store, <Map<String, Object?>>[
        <String, Object?>{
          ...sealed,
          'data': <String, Object?>{'title': 'tampered slot'},
        },
      ]);
      await store.append(
        entryType: _kType,
        aggregateId: agg(chain[1]),
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': 'local slot'},
        initiator: const UserInitiator('u1'),
      );

      for (final view in <String>[_kNotes, _kSlots]) {
        final before = jsonEncode(await store.reader.findViewRows(view));
        expect(before, contains('security_findings'));
        await rebuildView(
          store: store,
          viewName: view,
          targetVersionByEntryType: const <String, EntryTypeVersion>{
            _kType: EntryTypeVersion(1, 0),
          },
        );
        expect(
          jsonEncode(await store.reader.findViewRows(view)),
          before,
          reason: 'view $view',
        );
      }
    });

    // Verifies: EVS-PRD-materializer/H
    test(r'an append whose data holds a top-level key beginning with $ is '
        'refused by name before any write', () async {
      final store = await open();
      final before = (await store.reader.findAllEvents()).length;
      await expectLater(
        store.append(
          entryType: _kType,
          aggregateId: 'note-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: <String, Object?>{'title': 't', r'$integrity': 1},
          initiator: const UserInitiator('u1'),
        ),
        throwsA(
          isA<ArgumentError>().having(
            (e) => e.message.toString(),
            'message',
            contains(r'$integrity'),
          ),
        ),
      );
      expect((await store.reader.findAllEvents()).length, before);
      expect(await store.reader.findViewRows(_kNotes), isEmpty);
    });
  });
}
