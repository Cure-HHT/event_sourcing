// Backend-agnostic scenarios for the chain verification: the log is
// changed behind the library through a raw write of the test database, one
// scenario per finding kind, and the verification's verdict, the finding
// it records under role `walk`, its range handling and its reads are
// checked. Run on Sembast by test/event_store/chain_verification_test.dart
// and on Postgres by
// test/storage/postgres/postgres_chain_verification_test.dart.
//
// This file exposes [runChainVerificationScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show verifyChainsForTest;
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart' show expectedFindingId;
import 'ingest_chain_findings_conformance.dart' show chained, originChain;
import 'ingest_record_findings_conformance.dart'
    show envelopeOf, relayedRecord, resealed;
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

/// A database whose stored log a test changes as a write outside the
/// library would.
abstract class ChainTestDatabase implements VersionTestDatabase {
  /// Replaces the stored record at local sequence number [sequenceNumber]
  /// with [record], exactly as given.
  Future<void> rewriteEvent(int sequenceNumber, Map<String, Object?> record);

  /// Removes the stored record at local sequence number [sequenceNumber].
  Future<void> deleteEvent(int sequenceNumber);
}

const String _kType = 'finding_note';

const Source _source = Source(
  hopId: 'walk-hop',
  identifier: 'walk-install',
  softwareVersion: 'walk-app@1.0.0',
);

const Initiator _init = AutomationInitiator(service: 'chain-walk-tests');

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runChainVerificationScenarios({
  required Future<ChainTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('chain verification ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <ChainTestDatabase>[];
    late ChainTestDatabase db;

    Future<EventStore> open() async {
      db = (await openDatabase())!;
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
        source: _source,
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

    Future<StoredEvent> note(EventStore store, String aggregateId) async =>
        (await store.append(
          entryType: _kType,
          aggregateId: aggregateId,
          aggregateType: 'note',
          eventType: 'finalized',
          data: <String, Object?>{'title': aggregateId},
          initiator: _init,
        ))!;

    Future<void> deliver(
      EventStore store,
      List<Map<String, Object?>> records,
    ) async {
      await store.ingestBatch(
        envelopeOf(records).encode(),
        wireFormat: BatchEnvelope.wireFormat,
      );
    }

    Future<StoredEvent> held(EventStore store, String eventId) async =>
        (await store.reader.findEventById(eventId))!;

    /// The findings [store] holds as authored under [role], in log order.
    Future<List<Map<String, Object?>>> ownFindings(
      EventStore store, {
      String role = 'walk',
    }) async => <Map<String, Object?>>[
      for (final e in await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      ))
        if ((e.metadata['provenance']! as List).length == 1 &&
            (e.data['detector']! as Map)['role'] == role)
          Map<String, Object?>.from(e.data),
    ];

    Map<String, Object?> walkFinding(
      EventStore store,
      String kind,
      Map<String, Object?> evidence,
      List<String> aggregates,
    ) => <String, Object?>{
      'finding_id': expectedFindingId(
        databaseId: store.databaseId,
        role: 'walk',
        kind: kind,
        evidence: evidence,
      ),
      'kind': kind,
      'evidence': evidence,
      'aggregates': aggregates,
      'detector': <String, Object?>{
        'database_id': store.databaseId,
        'role': 'walk',
        'library_version': LibVersion.version,
      },
    };

    /// The last local sequence number of [store]'s log.
    Future<int> lastSequence(EventStore store) async =>
        (await store.reader.findAllEvents()).last.sequenceNumber;

    /// Walks [store]'s whole log through its reader and through the store,
    /// checks the two verdicts agree and that the reader recorded nothing,
    /// and returns the store's verdict.
    Future<ChainVerificationVerdict> walk(EventStore store) async {
      final before = await lastSequence(store);
      final fromReader = await store.reader.verifyChains();
      expect(
        await lastSequence(store),
        before,
        reason: 'the reader-only verification appends nothing',
      );
      final verdict = await store.verifyChains();
      expect(verdict, fromReader, reason: 'the reader computes the verdict');
      return verdict;
    }

    /// Walks [store] and checks that the verdict lists exactly [kind] with
    /// [evidence], that the store recorded exactly that finding under role
    /// `walk` naming [aggregates], and that a second walk records nothing
    /// more.
    Future<ChainVerificationVerdict> expectOneWalkFinding(
      EventStore store,
      String kind,
      Map<String, Object?> evidence,
      List<String> aggregates,
    ) async {
      final verdict = await walk(store);
      expect(verdict.isValid, isFalse);
      expect(
        verdict.findings.map((f) => f.toJson()).toList(),
        <Map<String, Object?>>[
          <String, Object?>{'kind': kind, 'evidence': evidence},
        ],
      );
      final expected = <Map<String, Object?>>[
        walkFinding(store, kind, evidence, aggregates),
      ];
      expect(await ownFindings(store), expected);
      await store.verifyChains();
      expect(
        await ownFindings(store),
        expected,
        reason: 'a second walk records nothing more',
      );
      return verdict;
    }

    /// [event]'s stored record with [change] applied to a copy.
    Map<String, Object?> changed(
      StoredEvent event,
      void Function(Map<String, Object?> record) change,
    ) {
      final record = _deepCopy(event.toMap());
      change(record);
      return record;
    }

    /// [event]'s stored record with [change] applied and its `event_hash`
    /// recomputed, as a write that recomputed the hash would leave it.
    Map<String, Object?> changedAndResealed(
      StoredEvent event,
      void Function(Map<String, Object?> record) change,
    ) => resealed(changed(event, change), const <String, Object?>{});

    Map<String, Object?> lastEntry(Map<String, Object?> record) =>
        ((record['metadata']! as Map)['provenance']! as List).last
            as Map<String, Object?>;

    // Verifies: EVS-PRD-hash-chain-integrity/C
    // Verifies: EVS-DEV-chain-verification/I
    test('an intact log gives a valid verdict over the whole log and records '
        'nothing', () async {
      final store = await open();
      await note(store, 'agg-a');
      await note(store, 'agg-a');
      await deliver(store, originChain('peer-db', 2));
      final last = await lastSequence(store);

      final verdict = await walk(store);

      expect(verdict.isValid, isTrue);
      expect(verdict.findings, isEmpty);
      expect(verdict.from, 1);
      expect(verdict.to, last);
      expect(
        verdict.unresolvedPredecessors,
        0,
        reason: 'the peer chain is held from its root',
      );
      expect(await ownFindings(store), isEmpty);
      expect(await lastSequence(store), last);
    });

    // Verifies: EVS-DEV-chain-verification/D
    // Verifies: EVS-DEV-chain-verification/R
    // Verifies: EVS-DEV-security-findings/L
    // Verifies: EVS-DEV-security-findings/Q
    // Verifies: EVS-PRD-hash-chain-integrity/J
    test('an event whose content changed without its hash gives one '
        'hash_mismatch', () async {
      final store = await open();
      final a = await note(store, 'agg-a');
      await note(store, 'agg-b');
      final tampered = changed(a, (r) => r['data'] = {'title': 'rewritten'});
      await db.rewriteEvent(a.sequenceNumber, tampered);

      await expectOneWalkFinding(
        store,
        'hash_mismatch',
        <String, Object?>{
          'event_id': a.eventId,
          'carried_hash': a.eventHash,
          'recomputed_hash': canonicalEventHash(tampered),
        },
        <String>['agg-a'],
      );
    });

    // Verifies: EVS-DEV-chain-verification/D
    test('a receiver entry whose arrival hash does not recompute gives one '
        'hash_mismatch', () async {
      final store = await open();
      final relayed = relayedRecord(chained('origin-db', 1));
      await deliver(store, <Map<String, Object?>>[relayed]);
      final stored = await held(store, relayed['event_id']! as String);
      expect(stored.metadata['provenance'], hasLength(3));
      await db.rewriteEvent(
        stored.sequenceNumber,
        changedAndResealed(
          stored,
          (r) => lastEntry(r)['arrival_hash'] = 'a' * 64,
        ),
      );

      await expectOneWalkFinding(
        store,
        'hash_mismatch',
        <String, Object?>{
          'event_id': stored.eventId,
          'carried_hash': 'a' * 64,
          'recomputed_hash': relayed['event_hash'],
        },
        <String>[stored.aggregateId],
      );
    });

    // Verifies: EVS-DEV-chain-verification/D
    // Verifies: EVS-PRD-hash-chain-integrity/E
    test(
      'a changed previous_ingest_hash gives one storage_link_break',
      () async {
        final store = await open();
        await note(store, 'agg-a');
        final b = await note(store, 'agg-b');
        final before = await held(store, b.eventId);
        final expectedLink = lastEntry(before.toMap())['previous_ingest_hash'];
        await db.rewriteEvent(
          b.sequenceNumber,
          changedAndResealed(
            b,
            (r) => lastEntry(r)['previous_ingest_hash'] = 'rewritten-link',
          ),
        );

        await expectOneWalkFinding(
          store,
          'storage_link_break',
          <String, Object?>{
            'local_sequence_number': b.sequenceNumber,
            'event_id': b.eventId,
            'field': 'previous_ingest_hash',
            'expected': expectedLink,
            'actual': 'rewritten-link',
          },
          <String>['agg-b'],
        );
      },
    );

    // Verifies: EVS-DEV-chain-verification/D
    test(
      'a changed ingest_sequence_number gives one storage_link_break',
      () async {
        final store = await open();
        final a = await note(store, 'agg-a');
        await db.rewriteEvent(
          a.sequenceNumber,
          changedAndResealed(
            a,
            (r) =>
                lastEntry(r)['ingest_sequence_number'] = a.sequenceNumber + 7,
          ),
        );

        await expectOneWalkFinding(
          store,
          'storage_link_break',
          <String, Object?>{
            'local_sequence_number': a.sequenceNumber,
            'event_id': a.eventId,
            'field': 'ingest_sequence_number',
            'expected': a.sequenceNumber,
            'actual': a.sequenceNumber + 7,
          },
          <String>['agg-a'],
        );
      },
    );

    // Verifies: EVS-DEV-chain-verification/E
    // Verifies: EVS-DEV-chain-verification/I
    test('a deleted event gives one sequence_missing and no link break at '
        'the event after it, whose unheld predecessor is counted', () async {
      final store = await open();
      final chain = originChain('peer-db', 3);
      await deliver(store, chain);
      final middle = await held(store, chain[1]['event_id']! as String);
      await db.deleteEvent(middle.sequenceNumber);

      final verdict = await expectOneWalkFinding(
        store,
        'sequence_missing',
        <String, Object?>{'local_sequence_number': middle.sequenceNumber},
        const <String>[],
      );
      expect(verdict.unresolvedPredecessors, 1);
    });

    // Verifies: EVS-DEV-chain-verification/F
    // Verifies: EVS-DEV-security-findings/M
    test('an event held as authored whose predecessor names no held event '
        'gives one predecessor_break', () async {
      final store = await open();
      await note(store, 'agg-a');
      final b = await note(store, 'agg-b');
      final rewritten = changedAndResealed(
        b,
        (r) => r['previous_event_hash'] = 'f' * 64,
      );
      await db.rewriteEvent(b.sequenceNumber, rewritten);

      final verdict = await expectOneWalkFinding(
        store,
        'predecessor_break',
        <String, Object?>{
          'database_id': store.databaseId,
          'event_hash': rewritten['event_hash'],
          'previous_event_hash': 'f' * 64,
        },
        <String>['agg-b'],
      );
      expect(verdict.unresolvedPredecessors, 1);
    });

    // Verifies: EVS-DEV-chain-verification/F
    // Verifies: EVS-DEV-security-findings/Q
    test('a predecessor another database authored gives a predecessor_break '
        'under role walk beside the one ingest recorded', () async {
      final store = await open();
      final a = originChain('db-a', 1);
      await deliver(store, a);
      final b = chained(
        'db-b',
        1,
        previousHash: a.single['event_hash']! as String,
        aggregateId: 'agg-b',
      );
      await deliver(store, <Map<String, Object?>>[b]);
      expect(await ownFindings(store, role: 'ingest'), hasLength(1));

      await expectOneWalkFinding(
        store,
        'predecessor_break',
        <String, Object?>{
          'database_id': 'db-b',
          'event_hash': b['event_hash'],
          'previous_event_hash': a.single['event_hash'],
        },
        <String>[a.single['aggregate_id']! as String, 'agg-b']..sort(),
      );
      expect(
        await ownFindings(store, role: 'ingest'),
        hasLength(1),
        reason: "ingest's finding stands beside the walk's",
      );
    });

    // Verifies: EVS-DEV-chain-verification/G
    // Verifies: EVS-DEV-security-findings/K
    // Verifies: EVS-PRD-hash-chain-integrity/G
    test('a fork whose successors sit at one origin position is one '
        'position_reused and no fork_unrecorded', () async {
      final store = await open();
      const peer = 'regressed-db';
      final e = originChain(peer, 3);
      await deliver(store, e);
      final f3 = chained(peer, 3, previous: e[1]);
      await deliver(store, <Map<String, Object?>>[f3]);

      await expectOneWalkFinding(
        store,
        'position_reused',
        <String, Object?>{'database_id': peer, 'origin_sequence_number': 3},
        <String>[e[2]['aggregate_id']! as String, f3['aggregate_id']! as String]
          ..sort(),
      );
    });

    // Verifies: EVS-DEV-chain-verification/G
    // Verifies: EVS-DEV-security-findings/J
    test('a fork whose successors sit at different origin positions is one '
        'fork_unrecorded, reported once for both successors', () async {
      final store = await open();
      const peer = 'forked-db';
      final e = originChain(peer, 3);
      await deliver(store, e);
      final g = chained(peer, 5, previous: e[1]);
      await deliver(store, <Map<String, Object?>>[g]);

      await expectOneWalkFinding(
        store,
        'fork_unrecorded',
        <String, Object?>{
          'database_id': peer,
          'previous_event_hash': e[1]['event_hash'],
        },
        <String>[e[2]['aggregate_id']! as String, g['aggregate_id']! as String]
          ..sort(),
      );
    });

    // Verifies: EVS-DEV-security-findings/B
    test('a fork finding the walk records names the aggregates of every '
        'successor held when it records, one stored after the walk started '
        'included', () async {
      final store = await open();
      const peer = 'forked-db';
      final e = originChain(peer, 3);
      await deliver(store, e);
      final g = chained(peer, 5, previous: e[1], aggregateId: 'agg-g');
      await deliver(store, <Map<String, Object?>>[g]);
      final late = chained(peer, 7, previous: e[1], aggregateId: 'agg-late');
      var paused = false;

      final verdict = await verifyChainsForTest(
        store,
        pageSize: 1,
        afterPage: () async {
          if (paused) return;
          paused = true;
          await deliver(store, <Map<String, Object?>>[late]);
        },
      );

      final evidence = <String, Object?>{
        'database_id': peer,
        'previous_event_hash': e[1]['event_hash'],
      };
      final walked = verdict.findings.singleWhere(
        (f) => f.kind == FindingKind.forkUnrecorded,
      );
      expect(walked.evidence, evidence);
      expect(
        walked.aggregates,
        <String>[e[2]['aggregate_id']! as String, 'agg-g']..sort(),
        reason: 'the verdict reads only up to its fixed upper bound',
      );
      expect(await ownFindings(store), <Map<String, Object?>>[
        walkFinding(
          store,
          'fork_unrecorded',
          evidence,
          <String>[e[2]['aggregate_id']! as String, 'agg-g', 'agg-late']
            ..sort(),
        ),
      ]);
    });

    group('an invalid parent', () {
      /// A peer chain on one aggregate whose third event names, as its
      /// parent, the second event with [parent]'s identity, after
      /// [second] reshaped the second.
      Future<void> scenario({
        required String reason,
        Map<String, Object?> Function(Map<String, Object?> second)? second,
        Map<String, Object?> Function(Map<String, Object?> second)? parent,
        String secondAggregate = 'agg-p',
      }) async {
        final store = await open();
        const peer = 'parent-db';
        final p1 = chained(peer, 1, aggregateId: 'agg-p');
        var p2 = chained(peer, 2, previous: p1, aggregateId: secondAggregate);
        if (second != null) p2 = second(p2);
        final named = <String, Object?>{
          'event_id': p2['event_id'],
          'event_hash': p2['event_hash'],
          ...?parent?.call(p2),
        };
        final p3 = resealed(
          chained(peer, 3, previous: p2, aggregateId: 'agg-p'),
          <String, Object?>{
            'causal': <String, Object?>{
              'kind': 'version',
              'eligible': true,
              'parents': <Object?>[named],
            },
          },
        );
        await deliver(store, <Map<String, Object?>>[p1, p2, p3]);
        final stored = await held(store, p3['event_id']! as String);

        await expectOneWalkFinding(
          store,
          'parent_invalid',
          <String, Object?>{
            'local_sequence_number': stored.sequenceNumber,
            'event_id': p3['event_id'],
            'parent': named,
            'reason': reason,
          },
          <String>{'agg-p', secondAggregate}.toList()..sort(),
        );
      }

      Map<String, Object?> withCausal(
        Map<String, Object?> record,
        String kind,
        bool eligible,
      ) => resealed(record, <String, Object?>{
        'causal': <String, Object?>{
          'kind': kind,
          'eligible': eligible,
          'parents': const <Object?>[],
        },
      });

      // Verifies: EVS-DEV-causal-parents/K
      // Verifies: EVS-PRD-hash-chain-integrity/I
      test('of another aggregate', () async {
        await scenario(reason: 'other_aggregate', secondAggregate: 'agg-q');
      });

      // Verifies: EVS-DEV-causal-parents/K
      test('that is an annotation', () async {
        await scenario(
          reason: 'annotation',
          second: (r) => withCausal(r, 'annotation', false),
        );
      });

      // Verifies: EVS-DEV-causal-parents/K
      test('that is an ineligible version', () async {
        await scenario(
          reason: 'ineligible',
          second: (r) => withCausal(r, 'version', false),
        );
      });

      // Verifies: EVS-DEV-causal-parents/K
      test('held under another hash', () async {
        await scenario(
          reason: 'held_under_other_hash',
          parent: (_) => <String, Object?>{'event_hash': 'e' * 64},
        );
      });
    });

    // Verifies: EVS-DEV-causal-parents/L
    // Verifies: EVS-PRD-hash-chain-integrity/I
    test('an authored event naming other parents than the stamping rule '
        'yields gives one parents_not_stamped', () async {
      final store = await open();
      final a = await note(store, 'agg-x');
      final b = await note(store, 'agg-x');
      expect(b.causal!.parents.single.eventId, a.eventId);
      await db.rewriteEvent(
        b.sequenceNumber,
        changedAndResealed(
          b,
          (r) => r['causal'] = <String, Object?>{
            'kind': 'version',
            'eligible': true,
            'parents': const <Object?>[],
          },
        ),
      );

      await expectOneWalkFinding(
        store,
        'parents_not_stamped',
        <String, Object?>{
          'local_sequence_number': b.sequenceNumber,
          'event_id': b.eventId,
          'expected': <Object?>[
            <String, Object?>{'event_id': a.eventId, 'event_hash': a.eventHash},
          ],
          'actual': const <Object?>[],
        },
        <String>['agg-x'],
      );
    });

    group('its range', () {
      // Verifies: EVS-DEV-chain-verification/D
      // Verifies: EVS-PRD-hash-chain-integrity/F
      test('a range starting on a changed link checks that link against '
          'the event before the range', () async {
        final store = await open();
        await note(store, 'agg-a');
        final b = await note(store, 'agg-b');
        await db.rewriteEvent(
          b.sequenceNumber,
          changedAndResealed(
            b,
            (r) => lastEntry(r)['previous_ingest_hash'] = 'rewritten-link',
          ),
        );

        final verdict = await store.reader.verifyChains(
          from: b.sequenceNumber,
          to: b.sequenceNumber,
        );

        expect(verdict.from, b.sequenceNumber);
        expect(verdict.to, b.sequenceNumber);
        expect(verdict.findings.map((f) => f.kind.wire), <String>[
          'storage_link_break',
        ]);
      });

      // Verifies: EVS-DEV-chain-verification/J
      test('a lower bound of 0 or none is the first local sequence number, '
          'and an upper bound above the log is the last stored', () async {
        final store = await open();
        await note(store, 'agg-a');
        final last = await lastSequence(store);
        for (final verdict in <ChainVerificationVerdict>[
          await store.reader.verifyChains(from: 0, to: last + 50),
          await store.reader.verifyChains(),
        ]) {
          expect(verdict.from, 1);
          expect(verdict.to, last);
          expect(verdict.isValid, isTrue);
        }
      });

      // Verifies: EVS-DEV-chain-verification/J
      test('a negative bound or an inverted range is refused', () async {
        final store = await open();
        await note(store, 'agg-a');
        for (final range in <(int?, int?)>[(-1, null), (null, -1), (3, 2)]) {
          await expectLater(
            store.verifyChains(from: range.$1, to: range.$2),
            throwsArgumentError,
          );
          await expectLater(
            store.reader.verifyChains(from: range.$1, to: range.$2),
            throwsArgumentError,
          );
        }
      });
    });

    group('its reads', () {
      // Verifies: EVS-DEV-chain-verification/S
      // Verifies: EVS-PRD-hash-chain-integrity/K
      test('an append completes while the verification is paused between '
          'pages, and is not walked', () async {
        final store = await open();
        for (var i = 0; i < 3; i++) {
          await note(store, 'agg-$i');
        }
        final last = await lastSequence(store);
        StoredEvent? appended;
        var pages = 0;

        final verdict = await verifyChainsForTest(
          store,
          pageSize: 1,
          afterPage: () async {
            pages += 1;
            if (pages == 1) {
              appended = await note(
                store,
                'agg-late',
              ).timeout(const Duration(seconds: 10));
            }
          },
        );

        expect(appended, isNotNull);
        expect(appended!.sequenceNumber, greaterThan(last));
        expect(verdict.to, last, reason: 'the upper bound is fixed at start');
        expect(pages, last, reason: 'one page per event walked, no more');
        expect(verdict.isValid, isTrue);
      });
    });
  });
}

Map<String, Object?> _deepCopy(Map<String, Object?> value) =>
    value.map((k, v) => MapEntry(k, _copy(v)));

Object? _copy(Object? value) {
  if (value is Map) {
    return <String, Object?>{
      for (final e in value.entries) e.key as String: _copy(e.value),
    };
  }
  if (value is List) return <Object?>[for (final v in value) _copy(v)];
  return value;
}
