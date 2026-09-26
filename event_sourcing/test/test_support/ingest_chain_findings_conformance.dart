// Backend-agnostic scenarios for the findings ingest records about the
// structure of an originating database's origin chain: a held predecessor
// of another database or at a position not below the incoming event's
// (`predecessor_break`), a second event at a held origin position
// (`position_reused`), and a second successor of a held predecessor at
// another origin position (`fork_unrecorded`). Every event is stored as
// received and the rest of the delivery is admitted. Run on Sembast by
// test/ingest/ingest_chain_findings_test.dart and on Postgres by
// test/storage/postgres/postgres_ingest_chain_findings_test.dart.
//
// This file exposes [runIngestChainFindingScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'ingest_record_findings_conformance.dart'
    show envelopeOf, resealed, sealedRecord;
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const String _kType = 'finding_note';

const Source _receiverSource = Source(
  hopId: 'receiver-hop',
  identifier: 'receiver-install',
  softwareVersion: 'receiver-app@1.0.0',
);

/// A record the database [databaseId] sealed at origin position
/// [position], naming [previous] (a record of that database) as its
/// predecessor, or [previousHash] when given, or no predecessor.
Map<String, Object?> chained(
  String databaseId,
  int position, {
  Map<String, Object?>? previous,
  String? previousHash,
  String? aggregateId,
}) => resealed(
  sealedRecord(databaseId: databaseId, aggregateId: aggregateId),
  <String, Object?>{
    'sequence_number': position,
    'previous_event_hash': previousHash ?? previous?['event_hash'],
  },
);

/// Records 1 to [count] of one origin chain of [databaseId], each naming
/// the one before it.
List<Map<String, Object?>> originChain(String databaseId, int count) {
  final records = <Map<String, Object?>>[];
  for (var position = 1; position <= count; position++) {
    records.add(
      chained(
        databaseId,
        position,
        previous: records.isEmpty ? null : records.last,
      ),
    );
  }
  return records;
}

String _agg(Map<String, Object?> record) => record['aggregate_id']! as String;

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runIngestChainFindingScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group(
    'ingest findings about chain structure ($backendLabel)',
    skip: skip,
    () {
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

      /// The findings [store] holds as authored, in log order.
      Future<List<Map<String, Object?>>> ownFindings(EventStore store) async =>
          <Map<String, Object?>>[
            for (final e in await store.reader.findAllEvents(
              entryType: kSecurityFindingEntryType,
            ))
              if ((e.metadata['provenance']! as List).length == 1)
                Map<String, Object?>.from(e.data),
          ];

      Future<IngestBatchResult> deliver(
        EventStore store,
        List<Map<String, Object?>> records,
      ) => store.ingestBatch(
        envelopeOf(records).encode(),
        wireFormat: BatchEnvelope.wireFormat,
      );

      /// The finding [store]'s ingest records of [kind] with [evidence],
      /// naming [aggregates].
      Map<String, Object?> finding(
        EventStore store,
        String kind,
        Map<String, Object?> evidence,
        List<String> aggregates,
      ) => <String, Object?>{
        'finding_id': expectedFindingId(
          databaseId: store.databaseId,
          role: 'ingest',
          kind: kind,
          evidence: evidence,
        ),
        'kind': kind,
        'evidence': evidence,
        'aggregates': aggregates,
        'detector': <String, Object?>{
          'database_id': store.databaseId,
          'role': 'ingest',
          'library_version': LibVersion.version,
        },
      };

      Map<String, Object?> reused(
        EventStore store,
        String db,
        int position,
        List<String> aggregates,
      ) => finding(store, 'position_reused', <String, Object?>{
        'database_id': db,
        'origin_sequence_number': position,
      }, aggregates);

      Map<String, Object?> fork(
        EventStore store,
        String db,
        String? previousEventHash,
        List<String> aggregates,
      ) => finding(store, 'fork_unrecorded', <String, Object?>{
        'database_id': db,
        'previous_event_hash': previousEventHash,
      }, aggregates);

      Map<String, Object?> predecessorBreak(
        EventStore store,
        Map<String, Object?> incoming,
        String db,
        List<String> aggregates,
      ) => finding(store, 'predecessor_break', <String, Object?>{
        'database_id': db,
        'event_hash': incoming['event_hash'],
        'previous_event_hash': incoming['previous_event_hash'],
      }, aggregates);

      /// Checks that every one of [records] is stored as received.
      Future<void> expectStored(
        EventStore store,
        List<Map<String, Object?>> records,
      ) async {
        for (final record in records) {
          final held = await store.reader.findEventById(
            record['event_id']! as String,
          );
          expect(held, isNotNull, reason: 'stored as received');
          expect(held!.data, record['data']);
          expect(
            ((held.metadata['provenance']! as List).last
                as Map)['arrival_hash'],
            record['event_hash'],
          );
        }
      }

      // Verifies: EVS-DEV-chain-verification/L
      // Verifies: EVS-DEV-security-findings/K
      // Verifies: EVS-PRD-ingest/J
      // Verifies: EVS-PRD-hash-chain-integrity/J
      test('a regressed database appending again at positions 4 and 5 gives '
          'one position_reused per position and no fork_unrecorded', () async {
        final store = await open();
        const db = 'regressed-db';
        final e = originChain(db, 5);
        await deliver(store, e);
        expect(await ownFindings(store), isEmpty, reason: 'one intact chain');

        final f4 = chained(db, 4, previous: e[2]);
        final f5 = chained(db, 5, previous: f4);
        final result = await deliver(store, <Map<String, Object?>>[f4, f5]);

        final expected = <Map<String, Object?>>[
          reused(store, db, 4, <String>[_agg(e[3]), _agg(f4)]..sort()),
          reused(store, db, 5, <String>[_agg(e[4]), _agg(f5)]..sort()),
        ];
        expect(await ownFindings(store), expected);
        await expectStored(store, <Map<String, Object?>>[f4, f5]);
        expect(result.events.map((o) => o.outcome), <IngestOutcome>[
          IngestOutcome.ingestedWithFinding,
          IngestOutcome.ingestedWithFinding,
        ]);
        expect(result.events.first.findingIds, <Object?>[
          expected.first['finding_id'],
        ]);
        expect(result.events.last.findingIds, <Object?>[
          expected.last['finding_id'],
        ]);

        await deliver(store, <Map<String, Object?>>[f4, f5]);
        expect(
          await ownFindings(store),
          expected,
          reason: 're-presenting the events is the duplicate, not a fork',
        );
      });

      // Verifies: EVS-DEV-chain-verification/L
      test('a receiver whose filter dropped the lost event at 4 still records '
          'the reuse of 5', () async {
        final store = await open();
        const db = 'regressed-db';
        final e = originChain(db, 5);
        await deliver(store, <Map<String, Object?>>[e[0], e[1], e[2], e[4]]);
        expect(await ownFindings(store), isEmpty);

        final f4 = chained(db, 4, previous: e[2]);
        final f5 = chained(db, 5, previous: f4);
        await deliver(store, <Map<String, Object?>>[f4, f5]);

        expect(await ownFindings(store), <Map<String, Object?>>[
          reused(store, db, 5, <String>[_agg(e[4]), _agg(f5)]..sort()),
        ]);
        await expectStored(store, <Map<String, Object?>>[f4, f5]);
      });

      // Verifies: EVS-DEV-chain-verification/L
      // Verifies: EVS-DEV-security-findings/J
      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-PRD-ingest/J
      test('a second successor at another origin position gives one '
          'fork_unrecorded naming the aggregates in ascending order', () async {
        final store = await open();
        const db = 'forked-db';
        final e1 = chained(db, 1, aggregateId: 'agg-m');
        final e2 = chained(db, 2, previous: e1, aggregateId: 'agg-m');
        final e = <Map<String, Object?>>[
          e1,
          e2,
          chained(db, 3, previous: e2, aggregateId: 'agg-z'),
        ];
        await deliver(store, e);

        final g = chained(db, 5, previous: e[1], aggregateId: 'agg-a');
        final after = chained(db, 9, previousHash: 'unheld', aggregateId: 'b');
        final result = await deliver(store, <Map<String, Object?>>[g, after]);

        final expected = fork(
          store,
          db,
          e[1]['event_hash']! as String,
          <String>['agg-a', 'agg-z'],
        );
        expect(await ownFindings(store), <Map<String, Object?>>[expected]);
        await expectStored(store, <Map<String, Object?>>[g, after]);
        expect(result.events.first.outcome, IngestOutcome.ingestedWithFinding);
        expect(result.events.first.findingIds, <Object?>[
          expected['finding_id'],
        ]);
        expect(
          result.events.last.outcome,
          IngestOutcome.ingested,
          reason: 'the rest of the batch is admitted',
        );
      });

      // Verifies: EVS-DEV-chain-verification/L
      test('two root events of one database at different positions are a '
          'fork on the null predecessor', () async {
        final store = await open();
        const db = 'twice-rooted-db';
        final root = chained(db, 1, aggregateId: 'agg-1');
        final again = chained(db, 3, aggregateId: 'agg-3');
        await deliver(store, <Map<String, Object?>>[root]);
        await deliver(store, <Map<String, Object?>>[again]);
        expect(await ownFindings(store), <Map<String, Object?>>[
          fork(store, db, null, <String>['agg-1', 'agg-3']),
        ]);
      });

      // Verifies: EVS-DEV-chain-verification/L
      test(
        'both events of a fork arriving in one batch give one finding',
        () async {
          final store = await open();
          const db = 'forked-db';
          final e = originChain(db, 3);
          await deliver(store, e);

          final e4 = chained(db, 4, previous: e[2]);
          final g = chained(db, 6, previous: e[2]);
          await deliver(store, <Map<String, Object?>>[e4, g]);

          expect(await ownFindings(store), <Map<String, Object?>>[
            fork(
              store,
              db,
              e[2]['event_hash']! as String,
              <String>[_agg(e4), _agg(g)]..sort(),
            ),
          ]);
          await expectStored(store, <Map<String, Object?>>[e4, g]);
        },
      );

      // Verifies: EVS-DEV-chain-verification/L
      // Verifies: EVS-DEV-security-findings/B
      test('a later successor of a recorded fork appends nothing and carries '
          'the recorded finding', () async {
        final store = await open();
        const db = 'forked-db';
        final e = originChain(db, 3);
        await deliver(store, e);
        final g = chained(db, 5, previous: e[1]);
        await deliver(store, <Map<String, Object?>>[g]);
        final recorded = await ownFindings(store);
        expect(recorded, <Map<String, Object?>>[
          fork(
            store,
            db,
            e[1]['event_hash']! as String,
            <String>[_agg(e[2]), _agg(g)]..sort(),
          ),
        ]);

        final h = chained(db, 7, previous: e[1]);
        final result = await deliver(store, <Map<String, Object?>>[h]);
        expect(await ownFindings(store), recorded);
        await expectStored(store, <Map<String, Object?>>[h]);
        expect(result.events.single.outcome, IngestOutcome.ingestedWithFinding);
        expect(result.events.single.findingIds, <Object?>[
          recorded.single['finding_id'],
        ]);
      });

      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-DEV-chain-verification/L
      test('a fork finding names every held successor of its predecessor, '
          'those at one position included', () async {
        final store = await open();
        const db = 'forked-db';
        final e = originChain(db, 3);
        await deliver(store, e);
        final f3 = chained(db, 3, previous: e[1]);
        await deliver(store, <Map<String, Object?>>[f3]);
        expect(
          await ownFindings(store),
          <Map<String, Object?>>[
            reused(store, db, 3, <String>[_agg(e[2]), _agg(f3)]..sort()),
          ],
          reason: 'successors at one position are recorded only as the reuse',
        );

        final g = chained(db, 5, previous: e[1]);
        await deliver(store, <Map<String, Object?>>[g]);
        expect(
          (await ownFindings(store)).last,
          fork(
            store,
            db,
            e[1]['event_hash']! as String,
            <String>[_agg(e[2]), _agg(f3), _agg(g)]..sort(),
          ),
        );
      });

      // Verifies: EVS-DEV-chain-verification/K
      // Verifies: EVS-DEV-chain-verification/A
      // Verifies: EVS-DEV-security-findings/M
      // Verifies: EVS-PRD-ingest/I
      test('a predecessor another database authored, held re-stamped by the '
          'receiver, gives predecessor_break', () async {
        final store = await open();
        final x = chained('other-db', 1, aggregateId: 'agg-x');
        await deliver(store, <Map<String, Object?>>[x]);
        final held = await store.reader.findEventById(x['event_id']! as String);
        expect(
          held!.eventHash,
          isNot(x['event_hash']),
          reason: 'the receiver holds the predecessor under its own hash',
        );

        final y = chained('author-db', 1, previous: x, aggregateId: 'agg-b');
        final result = await deliver(store, <Map<String, Object?>>[y]);

        final expected = predecessorBreak(store, y, 'author-db', <String>[
          'agg-b',
          'agg-x',
        ]);
        expect(await ownFindings(store), <Map<String, Object?>>[expected]);
        await expectStored(store, <Map<String, Object?>>[y]);
        expect(result.events.single.findingIds, <Object?>[
          expected['finding_id'],
        ]);
      });

      // Verifies: EVS-DEV-chain-verification/K
      // Verifies: EVS-PRD-ingest/I
      test('a predecessor of the same database at a later origin position '
          'gives predecessor_break', () async {
        final store = await open();
        const db = 'author-db';
        final e = originChain(db, 3);
        await deliver(store, <Map<String, Object?>>[e[0], e[2]]);

        final y = chained(db, 2, previous: e[2]);
        await store.ingestEvent(StoredEvent.fromMap(y, 0));

        expect(await ownFindings(store), <Map<String, Object?>>[
          predecessorBreak(store, y, db, <String>[_agg(e[2]), _agg(y)]..sort()),
        ]);
        await expectStored(store, <Map<String, Object?>>[y]);
      });

      // Verifies: EVS-DEV-chain-verification/K
      test('an unheld predecessor gives no finding', () async {
        final store = await open();
        const db = 'filtered-db';
        final e = originChain(db, 4);
        final result = await deliver(store, <Map<String, Object?>>[
          e[0],
          e[2],
          e[3],
        ]);
        expect(await ownFindings(store), isEmpty);
        expect(
          result.events.map((o) => o.outcome),
          everyElement(IngestOutcome.ingested),
        );
      });
    },
  );
}
