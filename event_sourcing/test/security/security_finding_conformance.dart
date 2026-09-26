// Backend-agnostic scenarios for the reserved security finding: the event
// the library appends when a detection point records an integrity anomaly,
// its shape and stable identity, the rule that records each anomaly once
// per detector, and the forward-compatible reserved events a later release
// of the data-format major adds. Run on Sembast by
// test/security/security_finding_test.dart and on Postgres by
// test/storage/postgres/postgres_security_finding_test.dart.
//
// Traceability lives on the individual tests below.
import 'dart:convert';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/destination_wedges_view_conformance.dart'
    show forgedEvent, wedgeData;
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

const _kType = 'finding_note';

/// The evidence of a `fork_unrecorded` finding about database [db].
Map<String, Object?> forkEvidence({String db = 'peer-db'}) => <String, Object?>{
  'database_id': db,
  'previous_event_hash': 'h-pred',
};

/// The finding identity the library computes: the SHA-256, in lowercase
/// hexadecimal, of the canonical JSON of the detecting database, the
/// detector's role, the kind and the evidence.
String expectedFindingId({
  required String databaseId,
  required String role,
  required String kind,
  required Map<String, Object?> evidence,
}) => sha256
    .convert(
      utf8.encode(
        canonicalize(<String, Object?>{
          'database_id': databaseId,
          'role': role,
          'kind': kind,
          'evidence': evidence,
        }),
      ),
    )
    .toString();

/// The refusal of an entry type in the reserved namespace, as opposed to
/// the refusal of an entry type the registry does not hold.
final Matcher _reservedNamespaceRefusal = isA<ArgumentError>().having(
  (e) => e.message.toString(),
  'message',
  contains('reserved entry-type namespace'),
);

/// Runs the security-finding scenarios. [openDatabase] returns a fresh
/// database for each store; [skip] skips the group when set.
void runSecurityFindingScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('security findings ($backendLabel)', skip: skip, () {
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
        source: const Source(
          hopId: 'finding-hop',
          identifier: 'finding-install',
          softwareVersion: 'finding-app@1.0.0',
        ),
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

    Future<StoredEvent?> record(
      EventStore store, {
      FindingRole role = FindingRole.ingest,
      FindingKind kind = FindingKind.forkUnrecorded,
      Map<String, Object?>? evidence,
      List<String> aggregates = const <String>[],
    }) => store.runTransaction(
      (txn, collector) => recordFindingInTxnForTest(
        store,
        txn,
        collector,
        role: role,
        kind: kind,
        evidence: evidence ?? forkEvidence(),
        aggregates: aggregates,
      ),
    );

    Future<List<StoredEvent>> findings(EventStore store) =>
        store.reader.findAllEvents(entryType: kSecurityFindingEntryType);

    // Verifies: EVS-DEV-security-findings/A
    // Verifies: EVS-DEV-security-findings/B
    // Verifies: EVS-DEV-security-findings/C
    // Verifies: EVS-DEV-security-findings/J
    test('a finding is a security_finding_recorded event whose data has '
        'exactly its keys, its identity recomputed from the detector, the '
        'kind and the evidence', () async {
      final store = await open();
      final appended = await record(
        store,
        aggregates: const <String>['note-b', 'note-a', 'note-b'],
      );
      expect(appended, isNotNull);
      final held = await findings(store);
      expect(held, hasLength(1));
      final event = held.single;
      expect(event.eventId, appended!.eventId);
      expect(event.entryType, 'system.security_finding');
      expect(event.eventType, 'security_finding_recorded');
      expect(event.aggregateType, 'security_finding');
      final id = expectedFindingId(
        databaseId: store.databaseId,
        role: 'ingest',
        kind: 'fork_unrecorded',
        evidence: forkEvidence(),
      );
      expect(event.aggregateId, id);
      expect(
        event.data.keys,
        unorderedEquals(<String>[
          'finding_id',
          'kind',
          'evidence',
          'aggregates',
          'detector',
        ]),
      );
      expect(event.data['finding_id'], id);
      expect(event.data['kind'], 'fork_unrecorded');
      expect(event.data['evidence'], forkEvidence());
      expect(event.data['aggregates'], <String>['note-a', 'note-b']);
      expect(event.data['detector'], <String, Object?>{
        'database_id': store.databaseId,
        'role': 'ingest',
        'library_version': LibVersion.version,
      });
      expect(event.causal!.kind, CausalKind.annotation);
      expect(event.causal!.eligible, isFalse);
    });

    // Verifies: EVS-DEV-security-findings/E
    // Verifies: EVS-DEV-security-findings/C
    test('recording one anomaly twice under one detector appends one '
        'finding', () async {
      final store = await open();
      expect(await record(store), isNotNull);
      expect(await record(store, aggregates: const <String>['x']), isNull);
      expect(await findings(store), hasLength(1));
    });

    // Verifies: EVS-DEV-security-findings/C
    // Verifies: EVS-DEV-security-findings/E
    // Verifies: EVS-DEV-security-findings/Q
    test('the same anomaly under another detector role appends a second '
        'finding', () async {
      final store = await open();
      await record(store);
      await record(store, role: FindingRole.walk);
      final held = await findings(store);
      expect(held, hasLength(2));
      expect(
        <Object?>{
          for (final e in held)
            (e.data['detector']! as Map<String, Object?>)['role'],
        },
        <String>{'ingest', 'walk'},
      );
      expect(held[0].data['finding_id'], isNot(held[1].data['finding_id']));
    });

    // Verifies: EVS-DEV-security-findings/C
    // Verifies: EVS-DEV-security-findings/E
    test('the same anomaly met by another library version of the detector '
        'appends nothing', () async {
      final store = await open();
      await record(store);
      final again = await runWithDeliveryTestHooks(
        const DeliveryTestHooks(
          buildDeclaration: (
            version: '${LibVersion.version}-later',
            dataFormat: LibVersion.dataFormat,
          ),
        ),
        () => record(store),
      );
      expect(again, isNull);
      expect(await findings(store), hasLength(1));
    });

    // Verifies: EVS-DEV-security-findings/E
    // Verifies: EVS-DEV-security-findings/I
    test('a received finding carrying the same identity does not suppress '
        "the detector's own", () async {
      final store = await open();
      final id = expectedFindingId(
        databaseId: store.databaseId,
        role: 'ingest',
        kind: 'fork_unrecorded',
        evidence: forkEvidence(),
      );
      final forged = forgedEvent(
        entryType: kSecurityFindingEntryType,
        aggregateType: kSecurityFindingAggregateType,
        eventType: kSecurityFindingRecordedEventType,
        data: <String, Object?>{
          'finding_id': id,
          'kind': 'fork_unrecorded',
          'evidence': forkEvidence(),
          'aggregates': const <String>[],
          'detector': <String, Object?>{
            'database_id': store.databaseId,
            'role': 'ingest',
            'library_version': LibVersion.version,
          },
        },
      );
      final outcome = await store.ingestEvent(forged);
      expect(outcome.outcome, IngestOutcome.ingested);
      expect(await record(store), isNotNull);
      final held = await findings(store);
      expect(held, hasLength(2));
      expect(
        <Object?>{for (final e in held) e.data['finding_id']},
        <String>{id},
      );
      expect(await record(store), isNull, reason: 'its own is held now');
    });

    // Verifies: EVS-DEV-security-findings/F
    test('a finding recorded in a transaction that does not commit is not '
        'stored', () async {
      final store = await open();
      await expectLater(
        store.runTransaction((txn, collector) async {
          await recordFindingInTxnForTest(
            store,
            txn,
            collector,
            role: FindingRole.ingest,
            kind: FindingKind.forkUnrecorded,
            evidence: forkEvidence(),
            aggregates: const <String>[],
          );
          throw StateError('the detection point fails');
        }),
        throwsStateError,
      );
      expect(await findings(store), isEmpty);
      expect(await record(store), isNotNull, reason: 'nothing suppresses it');
    });

    // Verifies: EVS-DEV-security-findings/H
    // Verifies: EVS-DEV-security-findings/R
    test('a finding of an unknown kind, or with evidence outside its kind, '
        'is refused and appends nothing', () async {
      final store = await open();
      await expectLater(
        record(store, kind: FindingKind.fromWire('future_kind')),
        throwsArgumentError,
      );
      await expectLater(
        record(
          store,
          evidence: <String, Object?>{...forkEvidence(), 'at': 'now'},
        ),
        throwsArgumentError,
      );
      expect(await findings(store), isEmpty);
    });

    group('forward-compatible reserved events', () {
      // Verifies: EVS-DEV-destination-drain/L
      // the public append operations refuse every entry type in the
      //   reserved namespace, one no release declares included.
      test(
        'the public appends refuse an undeclared system. entry type',
        () async {
          final store = await open();
          await expectLater(
            store.append(
              entryType: 'system.anything_new',
              aggregateId: 'a',
              aggregateType: 'thing',
              eventType: 'happened',
              data: const <String, Object?>{},
              initiator: const UserInitiator('u'),
            ),
            throwsA(_reservedNamespaceRefusal),
          );
          await expectLater(
            store.runTransaction(
              (txn, collector) => store.appendInTxn(
                txn,
                entryType: 'system.anything_new',
                aggregateId: 'a',
                aggregateType: 'thing',
                eventType: 'happened',
                data: const <String, Object?>{},
                initiator: const UserInitiator('u'),
                flowToken: null,
                metadata: null,
                security: null,
                checkpointReason: null,
                changeReason: null,
                dedupeByContent: false,
                collector: collector,
              ),
            ),
            throwsA(_reservedNamespaceRefusal),
          );
          expect(
            await store.reader.findAllEvents(entryType: 'system.anything_new'),
            isEmpty,
          );
        },
      );

      // Verifies: EVS-DEV-destination-drain/L
      // ingest stores, as received and recording no finding, an event of an
      //   entry type in the namespace the release does not declare, and a
      //   reserved event whose enumerated field holds a value the release
      //   does not know; the default view folds the unknown value verbatim.
      test('ingest stores an undeclared reserved entry type and an unknown '
          'wedge cause as received, recording no finding', () async {
        final store = await open();
        final undeclared = forgedEvent(
          entryType: 'system.x',
          aggregateType: 'system_x',
          eventType: 'x_happened',
          data: const <String, Object?>{'value': 1},
        );
        final futureCause = forgedEvent(
          entryType: kDestinationWedgedEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgedEventType,
          data: wedgeData(
            destinationId: 'peer-dest',
            databaseId: 'peer-db',
            overrides: const <String, Object?>{'cause': 'future_cause'},
          ),
        );
        final futureKind = forgedEvent(
          entryType: kSecurityFindingEntryType,
          aggregateType: kSecurityFindingAggregateType,
          eventType: kSecurityFindingRecordedEventType,
          data: <String, Object?>{
            'finding_id': 'f' * 64,
            'kind': 'future_kind',
            'evidence': const <String, Object?>{'anything': 1},
            'aggregates': const <String>[],
            'detector': const <String, Object?>{
              'database_id': 'peer-db',
              'role': 'future_role',
              'library_version': '9.0.0',
            },
          },
        );
        for (final event in <StoredEvent>[
          undeclared,
          futureCause,
          futureKind,
        ]) {
          final outcome = await store.ingestEvent(event);
          expect(
            outcome.outcome,
            IngestOutcome.ingested,
            reason: event.entryType,
          );
          final stored = await store.reader.findEventById(event.eventId);
          expect(stored, isNotNull, reason: event.entryType);
          expect(stored!.data, event.data, reason: event.entryType);
        }
        final held = await findings(store);
        expect(held, hasLength(1), reason: 'only the received finding');
        expect(held.single.eventId, futureKind.eventId);
        final rows = await store.reader.findViewRows(
          defaultDestinationWedgesSpec.viewName,
        );
        expect(<Object?>[
          for (final r in rows) r['cause'],
        ], contains('future_cause'));
      });
    });
  });
}
