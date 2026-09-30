// Backend-agnostic scenarios for the findings ingest records about a
// received record itself: a hash that does not recompute, an identifier
// held under another sealed hash, a record the library does not store as an
// event, and an event the receiving database's own identity originated.
// Each anomaly is the middle record of a three-record delivery on the
// peer's channel: ingest records exactly one finding under role `ingest`
// (followed by the delivery's `foreign_event` finding when the peer neither
// authored nor stored the record last), stores or keeps the record as the
// requirement says, stores the other two, and records no second finding of
// that kind when the records are delivered again. Run on Sembast by
// test/ingest/ingest_record_findings_test.dart and on Postgres by
// test/storage/postgres/postgres_ingest_record_findings_test.dart.
//
// This file exposes [runIngestRecordFindingScenarios] and registers no
// `main()` of its own. Traceability lives on the individual tests.
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:event_sourcing/src/security/security_finding.dart'
    show findingRecordEvidence;
import 'package:flutter_test/flutter_test.dart';

import '../security/security_finding_conformance.dart'
    show expectedFindingId, forkEvidence;
import 'deliveries.dart';
import 'destination_wedges_view_conformance.dart' show wedgeData;
import 'record_fixtures.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const String _kType = 'finding_note';

const Source _receiverSource = Source(
  hopId: 'receiver-hop',
  identifier: 'receiver-install',
  softwareVersion: 'receiver-app@1.0.0',
);

var _built = 0;

/// A record as the database [databaseId] sealed it, with one originator
/// provenance entry naming that database and an `event_hash` that is the
/// canonical hash of the record. Its origin position and its predecessor
/// hash are its own, and the predecessor names no event, so records built
/// here form no fork and reuse no origin position among themselves.
Map<String, Object?> sealedRecord({
  String databaseId = kPeerDatabaseId,
  String entryType = _kType,
  String aggregateType = 'note',
  String eventType = 'finalized',
  String? aggregateId,
  Map<String, Object?>? data,
}) {
  _built += 1;
  final at = DateTime.utc(2026, 9, 1, 12, 0, _built % 60).toIso8601String();
  final record = <String, Object?>{
    'event_id': 'finding-rec-$_built-${DateTime.now().microsecondsSinceEpoch}',
    'aggregate_id': aggregateId ?? 'finding-agg-$_built',
    'aggregate_type': aggregateType,
    'entry_type': entryType,
    'entry_type_version': const EntryTypeVersion(1, 0).toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': eventType,
    'sequence_number': 100 + _built,
    'data': data ?? <String, Object?>{'title': 'note $_built'},
    'metadata': <String, Object?>{
      'provenance': <Map<String, Object?>>[
        <String, Object?>{
          'hop': 'peer-hop',
          'received_at': at,
          'identifier': 'peer-install',
          'software_version': 'peer-app@1.0.0',
          'database_id': databaseId,
          'library_version': kPeerLibraryVersion,
        },
      ],
    },
    'initiator': const UserInitiator('peer-user').toJson(),
    'flow_token': null,
    'client_timestamp': at,
    'previous_event_hash': 'unheld-predecessor-$_built',
    'causal': kRootVersionCausalJson,
  };
  record['event_hash'] = canonicalEventHash(record);
  return record;
}

/// [record] with [changes] laid over it and its `event_hash` recomputed.
Map<String, Object?> resealed(
  Map<String, Object?> record,
  Map<String, Object?> changes,
) {
  final changed = <String, Object?>{...record, ...changes};
  changed['event_hash'] = canonicalEventHash(changed);
  return changed;
}

/// [record] with its originator entry's [field] set to [value], or removed
/// when [value] is null, and resealed.
Map<String, Object?> withOriginatorField(
  Map<String, Object?> record,
  String field,
  Object? value,
) {
  final metadata = Map<String, Object?>.from(record['metadata']! as Map);
  final entry = Map<String, Object?>.from(
    (metadata['provenance']! as List).first as Map,
  );
  if (value == null) {
    entry.remove(field);
  } else {
    entry[field] = value;
  }
  metadata['provenance'] = <Object?>[entry];
  return resealed(record, <String, Object?>{'metadata': metadata});
}

/// [origin] as a relay database stored it: the relay's receiver entry after
/// the originator's, carrying [arrivalHash] (by default the origin's
/// `event_hash`) as its arrival hash, and the relay's local sequence number,
/// sealed by the relay.
Map<String, Object?> relayedRecord(
  Map<String, Object?> origin, {
  Object? arrivalHash = _originHash,
}) {
  final metadata = Map<String, Object?>.from(origin['metadata']! as Map);
  final entry =
      ProvenanceEntry(
          hop: 'relay-hop',
          receivedAt: DateTime.utc(2026, 9, 1, 13),
          identifier: 'relay-install',
          softwareVersion: 'relay-app@1.0.0',
          arrivalHash: 'placeholder',
          previousIngestHash: null,
          ingestSequenceNumber: 42,
          originSequenceNumber: origin['sequence_number']! as int,
          databaseId: 'relay-db',
          libraryVersion: kPeerLibraryVersion,
        ).toJson()
        ..['arrival_hash'] = identical(arrivalHash, _originHash)
            ? origin['event_hash']
            : arrivalHash;
  if (entry['arrival_hash'] == null) entry.remove('arrival_hash');
  metadata['provenance'] = <Object?>[
    ...(metadata['provenance']! as List),
    entry,
  ];
  return resealed(origin, <String, Object?>{
    'sequence_number': 42,
    'metadata': metadata,
  });
}

const Object _originHash = Object();

/// The channel the scenarios deliver on: from the peer database that
/// [sealedRecord] seals records as by default.
final DeliveryChannel peerChannel = testChannel(kPeerDatabaseId);

/// Whether a receiver records a `foreign_event` finding for [record] on
/// [peerChannel]: it carries an identifier, and its originator entry or its
/// last provenance entry does not name the peer.
bool isForeignOnPeerChannel(Map<String, Object?> record) {
  if (record['event_id'] is! String) return false;
  final metadata = record['metadata'];
  final provenance = metadata is Map ? metadata['provenance'] : null;
  if (provenance is! List || provenance.isEmpty) return true;
  String? databaseOf(Object? entry) {
    final id = entry is Map ? entry['database_id'] : null;
    return id is String ? id : null;
  }

  return databaseOf(provenance.first) != kPeerDatabaseId ||
      databaseOf(provenance.last) != kPeerDatabaseId;
}

/// [record] as the receiver decodes it from a delivery: JSON-encoded and
/// decoded again.
Map<String, Object?> asReceived(Map<String, Object?> record) =>
    (jsonDecode(jsonEncode(record)) as Map).cast<String, Object?>();

/// Runs the scenarios. [openDatabase] returns a fresh database for each
/// store; [skip] skips the group when set.
void runIngestRecordFindingScenarios({
  required Future<VersionTestDatabase?> Function() openDatabase,
  required String backendLabel,
  String? skip,
}) {
  group('ingest findings about the record ($backendLabel)', skip: skip, () {
    final opened = <EventStore>[];
    final databases = <VersionTestDatabase>[];

    Future<EventStore> open({Source source = _receiverSource}) async {
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
        source: source,
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

    /// The findings [store] holds as authored.
    Future<List<Map<String, Object?>>> ownFindings(EventStore store) async =>
        <Map<String, Object?>>[
          for (final e in await store.reader.findAllEvents(
            entryType: kSecurityFindingEntryType,
          ))
            if ((e.metadata['provenance']! as List).length == 1)
              Map<String, Object?>.from(e.data),
        ];

    Future<TestDelivery> deliver(
      EventStore store,
      List<Map<String, Object?>> records,
    ) => deliverTo(store, records, channel: peerChannel);

    /// Checks that the findings [store] recorded after [before] are
    /// exactly the `foreign_event` finding [delivery] meets for [record]
    /// when it is foreign on the peer channel, and none otherwise.
    Future<void> expectForeignFindingOnly(
      EventStore store,
      List<Map<String, Object?>> before,
      TestDelivery delivery,
      Map<String, Object?> record,
    ) async {
      final added = (await ownFindings(store)).sublist(before.length);
      if (!isForeignOnPeerChannel(record)) {
        expect(added, isEmpty);
        return;
      }
      expect(added, hasLength(1));
      expect(added.single['kind'], 'foreign_event');
      expect(
        (added.single['evidence']! as Map)['delivery_number'],
        delivery.envelope.deliveryNumber,
      );
      expect(
        (added.single['evidence']! as Map)['event_id'],
        record['event_id'],
      );
    }

    /// Delivers [a], [anomaly] and [c] on the peer channel, checks that
    /// exactly one finding of [kind] with [evidence] naming [aggregates] is
    /// recorded under role `ingest` (followed by the delivery's
    /// `foreign_event` finding for [anomaly] when it is foreign on the
    /// channel), that [a] and [c] are stored, that [anomaly] is stored
    /// exactly when [stored] and has [outcome], and that delivering the
    /// three again records no second finding of [kind].
    Future<void> expectOneFinding(
      EventStore store, {
      required Map<String, Object?> anomaly,
      required String kind,
      required Map<String, Object?> evidence,
      required List<String> aggregates,
      required bool stored,
      required IngestOutcome outcome,
    }) async {
      final a = sealedRecord();
      final c = sealedRecord();
      final before = await ownFindings(store);
      final delivery = await deliver(store, <Map<String, Object?>>[
        a,
        anomaly,
        c,
      ]);
      expect(delivery.accepted, isTrue);
      final findings = await ownFindings(store);
      final foreign = isForeignOnPeerChannel(anomaly);
      expect(
        findings.length - before.length,
        foreign ? 2 : 1,
        reason: 'one finding, and a foreign_event finding for a foreign record',
      );
      final finding = findings[before.length];
      if (foreign) {
        expect(findings.last['kind'], 'foreign_event');
        expect(
          (findings.last['evidence']! as Map)['event_id'],
          anomaly['event_id'],
        );
      }
      final id = expectedFindingId(
        databaseId: store.databaseId,
        role: 'ingest',
        kind: kind,
        evidence: evidence,
      );
      expect(finding, <String, Object?>{
        'finding_id': id,
        'kind': kind,
        'evidence': evidence,
        'aggregates': aggregates,
        'detector': <String, Object?>{
          'database_id': store.databaseId,
          'role': 'ingest',
          'library_version': LibVersion.version,
        },
      });
      for (final other in <Map<String, Object?>>[a, c]) {
        expect(
          await store.reader.findEventById(other['event_id']! as String),
          isNotNull,
          reason: 'the rest of the delivery is admitted',
        );
      }
      final anomalyId = anomaly['event_id'];
      if (anomalyId is String) {
        final held = await store.reader.findEventById(anomalyId);
        if (stored) {
          expect(held, isNotNull, reason: 'stored as received');
          expect(held!.data, anomaly['data']);
          if (outcome != IngestOutcome.duplicate) {
            expect(
              ((held.metadata['provenance']! as List).last
                  as Map)['arrival_hash'],
              anomaly['event_hash'],
              reason: 'the receiver entry records the hash it arrived with',
            );
          }
        } else if (held != null) {
          expect(
            held.data,
            isNot(anomaly['data']),
            reason: 'the received record is not stored',
          );
        }
      }
      expect(await recordOutcomes(store, delivery), <IngestOutcome>[
        IngestOutcome.ingested,
        outcome,
        IngestOutcome.ingested,
      ]);

      final again = await deliver(store, <Map<String, Object?>>[a, anomaly, c]);
      expect(again.accepted, isTrue);
      await expectForeignFindingOnly(store, findings, again, anomaly);
      expect(
        (await recordOutcomes(store, again)).first,
        IngestOutcome.duplicate,
      );
    }

    // Verifies: EVS-DEV-chain-verification/P
    // Verifies: EVS-PRD-ingest/D
    // Verifies: EVS-PRD-ingest/G
    // Verifies: EVS-DEV-security-findings/F
    // Verifies: EVS-DEV-security-findings/Q
    test('an event whose event_hash does not recompute is stored as '
        'received with one hash_mismatch finding', () async {
      final store = await open();
      final sealed = sealedRecord();
      final tampered = <String, Object?>{
        ...sealed,
        'data': <String, Object?>{'title': 'changed after sealing'},
      };
      await expectOneFinding(
        store,
        anomaly: tampered,
        kind: 'hash_mismatch',
        evidence: <String, Object?>{
          'event_id': sealed['event_id'],
          'carried_hash': sealed['event_hash'],
          'recomputed_hash': canonicalEventHash(tampered),
        },
        aggregates: <String>[sealed['aggregate_id']! as String],
        stored: true,
        outcome: IngestOutcome.ingestedWithFinding,
      );
    });

    // Verifies: EVS-DEV-chain-verification/P
    // Verifies: EVS-PRD-ingest/D
    test('a relayed event whose receiver arrival hash does not recompute is '
        'stored as received with one hash_mismatch finding', () async {
      final store = await open();
      final origin = sealedRecord();
      final relayed = relayedRecord(origin, arrivalHash: 'not-the-hash');
      await expectOneFinding(
        store,
        anomaly: relayed,
        kind: 'hash_mismatch',
        evidence: <String, Object?>{
          'event_id': origin['event_id'],
          'carried_hash': 'not-the-hash',
          'recomputed_hash': origin['event_hash'],
        },
        aggregates: <String>[origin['aggregate_id']! as String],
        stored: true,
        outcome: IngestOutcome.ingestedWithFinding,
      );
    });

    // Verifies: EVS-DEV-security-findings/G
    // Verifies: EVS-PRD-ingest/G
    test('an identifier held under another sealed hash is kept in full in '
        'one identity_mismatch finding and not stored', () async {
      final store = await open();
      final held = sealedRecord();
      await deliver(store, <Map<String, Object?>>[held]);
      final other = resealed(held, <String, Object?>{
        'data': <String, Object?>{'title': 'a second history'},
      });
      await expectOneFinding(
        store,
        anomaly: other,
        kind: 'identity_mismatch',
        evidence: <String, Object?>{
          'event_id': held['event_id'],
          'held_hash': held['event_hash'],
          'record': asReceived(other),
        },
        aggregates: <String>[held['aggregate_id']! as String],
        stored: false,
        outcome: IngestOutcome.keptInFinding,
      );
      final stored = await store.reader.findEventById(
        held['event_id']! as String,
      );
      expect(stored!.data, held['data'], reason: 'the held copy stays');
    });

    // Verifies: EVS-DEV-security-findings/G
    test('a copy of a held relayed event arriving by another path is a '
        'duplicate, not an identity mismatch', () async {
      final store = await open();
      final origin = sealedRecord();
      final relayed = relayedRecord(origin);
      final first = await deliver(store, <Map<String, Object?>>[relayed]);
      await expectForeignFindingOnly(store, const [], first, relayed);
      final before = await ownFindings(store);
      final second = await deliver(store, <Map<String, Object?>>[origin]);
      expect(await recordOutcomes(store, second), <IngestOutcome>[
        IngestOutcome.duplicate,
      ]);
      expect(
        await ownFindings(store),
        before,
        reason: 'the copy by another path records no finding',
      );
    });

    final malformed = <String, Map<String, Object?> Function()>{
      'no causal object': () {
        final r = sealedRecord()..remove('causal');
        return resealed(r, const <String, Object?>{});
      },
      'a causal object with a key outside its shape': () =>
          resealed(sealedRecord(), <String, Object?>{
            'causal': <String, Object?>{...kRootVersionCausalJson, 'extra': 1},
          }),
      'a provenance entry lacking library_version': () =>
          withOriginatorField(sealedRecord(), 'library_version', null),
      'a provenance entry with an empty database_id': () =>
          withOriginatorField(sealedRecord(), 'database_id', ''),
      r'a top-level data key beginning with $': () => sealedRecord(
        data: <String, Object?>{r'$integrity': 'forged', 'title': 't'},
      ),
      'a client timestamp without an offset': () => resealed(
        sealedRecord(),
        const <String, Object?>{'client_timestamp': '2026-09-01T12:00:00'},
      ),
      'a provenance received_at without an offset': () => withOriginatorField(
        sealedRecord(),
        'received_at',
        '2026-09-01T12:00:00',
      ),
      'no metadata': () =>
          resealed(sealedRecord(), const <String, Object?>{'metadata': null}),
      'a receiver entry without an arrival hash': () =>
          relayedRecord(sealedRecord(), arrivalHash: null),
      'no aggregate_id': () {
        final r = sealedRecord()..remove('aggregate_id');
        return resealed(r, const <String, Object?>{});
      },
    };

    for (final c in malformed.entries) {
      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-DEV-security-findings/O
      // Verifies: EVS-DEV-causal-parents/B
      // Verifies: EVS-DEV-event-record/H
      // Verifies: EVS-PRD-materializer/H
      // Verifies: EVS-PRD-ingest/G
      test('a record with ${c.key} is kept in full in one event_malformed '
          'finding (record_malformed) and not stored', () async {
        final store = await open();
        final record = c.value();
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'event_malformed',
          evidence: <String, Object?>{
            'reason': 'record_malformed',
            'record': asReceived(record),
          },
          aggregates: const <String>[],
          stored: false,
          outcome: IngestOutcome.keptInFinding,
        );
        final id = record['event_id']! as String;
        expect(await store.reader.findEventById(id), isNull);
      });
    }

    final unstorableCharacter = <String, Map<String, Object?> Function()>{
      'a top-level data value': () =>
          sealedRecord(data: <String, Object?>{'note': 'x\u0000y'}),
      'a nested map value': () => sealedRecord(
        data: <String, Object?>{
          'nested': <String, Object?>{'note': 'x\u0000y'},
        },
      ),
      'a nested map key': () => sealedRecord(
        data: <String, Object?>{
          'nested': <String, Object?>{'k\u0000ey': 1},
        },
      ),
      'a list element': () => sealedRecord(
        data: <String, Object?>{
          'items': <Object?>['a', 'x\u0000y'],
        },
      ),
    };

    for (final c in unstorableCharacter.entries) {
      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-DEV-security-findings/O
      // Verifies: EVS-DEV-security-findings/R
      // Verifies: EVS-DEV-security-findings/U
      // Verifies: EVS-DEV-event-record/L
      // Verifies: EVS-PRD-ingest/G
      test('a record with U+0000 in ${c.key} is kept in full, encoded, in '
          'one event_malformed finding (unstorable_character) and not '
          'stored', () async {
        final store = await open();
        final record = c.value();
        final received = asReceived(record);
        final encoded = findingRecordEvidence(received);
        expect(
          encoded,
          isA<String>(),
          reason: 'a record carrying U+0000 is kept encoded, not verbatim',
        );
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'event_malformed',
          evidence: <String, Object?>{
            'reason': 'unstorable_character',
            'record': encoded,
          },
          aggregates: const <String>[],
          stored: false,
          outcome: IngestOutcome.keptInFinding,
        );
        final id = record['event_id']! as String;
        expect(await store.reader.findEventById(id), isNull);
        // The encoding round-trips independently of findingRecordEvidence:
        // base64 -> UTF-8 -> canonical JSON decodes back to the received
        // record.
        final decoded = jsonDecode(
          utf8.decode(base64.decode(encoded as String)),
        );
        expect(decoded, received);
      });
    }

    // Verifies: EVS-DEV-security-findings/B
    // Verifies: EVS-DEV-security-findings/O
    test('a malformed record whose identifier the receiver holds is kept in '
        'one event_malformed finding naming the held aggregate', () async {
      final store = await open();
      final held = sealedRecord();
      await deliver(store, <Map<String, Object?>>[held]);
      final record = resealed(
        Map<String, Object?>.from(held)..remove('causal'),
        const <String, Object?>{
          'data': <String, Object?>{'title': 'a malformed copy'},
        },
      );
      await expectOneFinding(
        store,
        anomaly: record,
        kind: 'event_malformed',
        evidence: <String, Object?>{
          'reason': 'record_malformed',
          'record': asReceived(record),
        },
        aggregates: <String>[held['aggregate_id']! as String],
        stored: false,
        outcome: IngestOutcome.keptInFinding,
      );
    });

    // Verifies: EVS-DEV-security-findings/B
    // Verifies: EVS-DEV-security-findings/O
    // Verifies: EVS-DEV-destination-drain/L
    test(
      'a declared reserved entry type with an undeclared aggregate type is '
      'kept in one event_malformed finding (reserved_type_undeclared)',
      () async {
        final store = await open();
        final record = sealedRecord(
          entryType: kDestinationWedgedEntryType,
          aggregateType: 'note',
          eventType: kDestinationWedgedEventType,
          aggregateId: 'peer-install',
          data: wedgeData(destinationId: 'd', databaseId: kPeerDatabaseId),
        );
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'event_malformed',
          evidence: <String, Object?>{
            'reason': 'reserved_type_undeclared',
            'record': asReceived(record),
          },
          aggregates: const <String>[],
          stored: false,
          outcome: IngestOutcome.keptInFinding,
        );
      },
    );

    final badAudits = <String, Map<String, Object?>>{
      'a destination identifier containing |': wedgeData(
        destinationId: 'a|b',
        databaseId: kPeerDatabaseId,
      ),
      'a missing database identity': wedgeData(
        destinationId: 'd',
        databaseId: null,
        remove: const <String>{'database_id'},
      ),
      'a database identity other than its originating database': wedgeData(
        destinationId: 'd',
        databaseId: 'another-db',
      ),
    };
    for (final c in badAudits.entries) {
      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-DEV-security-findings/O
      // Verifies: EVS-DEV-destination-drain/L
      test('a destination audit with ${c.key} is kept in one event_malformed '
          'finding (audit_identity_invalid)', () async {
        final store = await open();
        final record = sealedRecord(
          entryType: kDestinationWedgedEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgedEventType,
          aggregateId: 'peer-install',
          data: c.value,
        );
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'event_malformed',
          evidence: <String, Object?>{
            'reason': 'audit_identity_invalid',
            'record': asReceived(record),
          },
          aggregates: const <String>[],
          stored: false,
          outcome: IngestOutcome.keptInFinding,
        );
      });
    }

    group("the receiver's own events", () {
      // Verifies: EVS-DEV-chain-verification/Q
      // Verifies: EVS-PRD-ingest/H
      test('an own event the receiver does not hold is stored as received '
          'with one own_event_ingested finding carrying no record', () async {
        final store = await open();
        final own = sealedRecord(databaseId: store.databaseId);
        expect(
          (own['data']! as Map).containsKey('database_id'),
          isFalse,
          reason: 'the originator entry, not the data, names the database',
        );
        await expectOneFinding(
          store,
          anomaly: own,
          kind: 'own_event_ingested',
          evidence: <String, Object?>{
            'event_id': own['event_id'],
            'sealed_hash': own['event_hash'],
            'record': null,
          },
          aggregates: <String>[own['aggregate_id']! as String],
          stored: true,
          outcome: IngestOutcome.ingestedWithFinding,
        );
      });

      // Verifies: EVS-DEV-chain-verification/Q
      // Verifies: EVS-PRD-ingest/H
      test('an own event the receiver holds records one own_event_ingested '
          'finding carrying no record, and is not stored again', () async {
        final store = await open();
        final appended = (await store.append(
          entryType: _kType,
          aggregateId: 'own-held',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'title': 'mine'},
          initiator: const UserInitiator('u'),
        ))!;
        final record = Map<String, Object?>.from(appended.toMap());
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'own_event_ingested',
          evidence: <String, Object?>{
            'event_id': appended.eventId,
            'sealed_hash': appended.eventHash,
            'record': null,
          },
          aggregates: const <String>['own-held'],
          stored: true,
          outcome: IngestOutcome.duplicate,
        );
        expect(
          (await store.reader.findEventById(appended.eventId))!.eventHash,
          appended.eventHash,
        );
        final copies = await store.reader.findAllEvents(entryType: _kType);
        expect(
          copies.where((e) => e.eventId == appended.eventId),
          hasLength(1),
        );
        expect(
          (copies
                      .firstWhere((e) => e.eventId == appended.eventId)
                      .metadata['provenance']!
                  as List)
              .length,
          1,
          reason: 'the held copy is the one the receiver appended',
        );
      });

      // Verifies: EVS-DEV-security-findings/B
      // Verifies: EVS-DEV-chain-verification/Q
      // Verifies: EVS-DEV-security-findings/O
      test('an own event the receiver cannot store is kept in full in one '
          'own_event_ingested finding, and no event_malformed', () async {
        final store = await open();
        final own = sealedRecord(databaseId: store.databaseId)
          ..remove('causal');
        final record = resealed(own, const <String, Object?>{});
        await expectOneFinding(
          store,
          anomaly: record,
          kind: 'own_event_ingested',
          evidence: <String, Object?>{
            'event_id': record['event_id'],
            'sealed_hash': record['event_hash'],
            'record': asReceived(record),
          },
          aggregates: const <String>[],
          stored: false,
          outcome: IngestOutcome.keptInFinding,
        );
      });

      // Verifies: EVS-DEV-destination-drain/L
      // Verifies: EVS-PRD-ingest/H
      test("a well-formed destination audit of the receiver's own identity "
          'it does not hold is stored with one own_event_ingested finding, '
          'and the default wedges view does not fold it', () async {
        final store = await open();
        final audit = sealedRecord(
          databaseId: store.databaseId,
          entryType: kDestinationWedgedEntryType,
          aggregateType: kDestinationAuditAggregateType,
          eventType: kDestinationWedgedEventType,
          aggregateId: 'receiver-install',
          data: wedgeData(destinationId: 'd', databaseId: store.databaseId),
        );
        await expectOneFinding(
          store,
          anomaly: audit,
          kind: 'own_event_ingested',
          evidence: <String, Object?>{
            'event_id': audit['event_id'],
            'sealed_hash': audit['event_hash'],
            'record': null,
          },
          aggregates: const <String>['receiver-install'],
          stored: true,
          outcome: IngestOutcome.ingestedWithFinding,
        );
        expect(
          (await store.reader.findViewRows(
            defaultDestinationWedgesSpec.viewName,
          )).rows,
          isEmpty,
        );
      });
    });

    // Verifies: EVS-DEV-security-findings/I
    test('a finding another database originated is stored as any event, '
        'recording no finding of the receiver', () async {
      final peer = await open(
        source: const Source(
          hopId: 'peer-hop',
          identifier: 'peer-install',
          softwareVersion: 'peer-app@1.0.0',
        ),
      );
      final peerFinding = (await peer.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          peer,
          txn,
          collector,
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: forkEvidence(),
          aggregates: const <String>[],
        ),
      ))!;
      final store = await open();
      // The finding travels on its originating database's channel, with
      // the peer's other events.
      final a = sealedRecord(databaseId: peer.databaseId);
      final c = sealedRecord(databaseId: peer.databaseId);
      final delivery = await deliverTo(store, <Map<String, Object?>>[
        a,
        Map<String, Object?>.from(peerFinding.toMap()),
        c,
      ]);
      expect(await recordOutcomes(store, delivery), <IngestOutcome>[
        IngestOutcome.ingested,
        IngestOutcome.ingested,
        IngestOutcome.ingested,
      ]);
      expect(await ownFindings(store), isEmpty);
      final held = await store.reader.findEventById(peerFinding.eventId);
      expect(held!.data, peerFinding.data);
    });

    // Verifies: EVS-DEV-security-findings/O
    // Verifies: EVS-PRD-ingest/G
    test('ingestEvent keeps an event it cannot store in one event_malformed '
        'finding carrying the event as a record', () async {
      final store = await open();
      final event = StoredEvent.fromMap(sealedRecord(), 0);
      final unstorable = StoredEvent(
        key: 0,
        eventId: event.eventId,
        aggregateId: event.aggregateId,
        aggregateType: event.aggregateType,
        entryType: event.entryType,
        entryTypeVersion: event.entryTypeVersion,
        libFormatVersion: event.libFormatVersion,
        eventType: event.eventType,
        sequenceNumber: event.sequenceNumber,
        data: event.data,
        metadata: event.metadata,
        initiator: event.initiator,
        clientTimestamp: event.clientTimestamp,
        eventHash: event.eventHash,
      );
      final outcome = await ingestEventForTest(store, unstorable);
      expect(outcome.outcome, IngestOutcome.keptInFinding);
      expect(outcome.resultHash, isNull);
      final findings = await ownFindings(store);
      expect(findings, hasLength(1));
      expect(findings.single['kind'], 'event_malformed');
      expect(findings.single['evidence'], <String, Object?>{
        'reason': 'record_malformed',
        'record': asReceived(Map<String, Object?>.from(unstorable.toMap())),
      });
      expect(await store.reader.findEventById(event.eventId), isNull);
    });
  });
}
