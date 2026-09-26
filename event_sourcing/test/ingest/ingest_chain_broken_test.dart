// Chain verification at the ingest boundary: an event whose receiver
// arrival hash does not recompute is stored as received with a
// hash_mismatch finding, and a record whose receiver entry carries no
// arrival hash is kept in an event_malformed finding.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/record_fixtures.dart';

// ---------------------------------------------------------------------------
// Test fixture helpers
// ---------------------------------------------------------------------------

var _dbCounter = 0;

class _Fixture {
  _Fixture({required this.store, required this.backend});
  final EventStore store;
  final SembastBackend backend;
  Future<void> close() => backend.close();
}

Future<_Fixture> _openStore({
  String hopId = 'mobile-device',
  String identifier = 'device-1',
  String softwareVersion = 'my_app@1.0.0',
}) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'ingest-chain-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Epistaxis Event',
      ),
    );
  final securityContexts = SembastSecurityContextStore(backend: backend);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: softwareVersion,
    ),
    securityContexts: securityContexts,
  );
  return _Fixture(store: store, backend: backend);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  Future<List<StoredEvent>> findings(_Fixture f) =>
      f.backend.findAllEvents(entryType: kSecurityFindingEntryType);

  group('EventStore.ingestEvent — chain broken', () {
    // Verifies: EVS-PRD-ingest/D
    // Verifies: EVS-DEV-chain-verification/P
    // Verifies: EVS-PRD-hash-chain-integrity/C
    test('an event with a tampered arrival_hash at hop 1 is stored as '
        'received with one hash_mismatch finding naming both hashes', () async {
      // Simulate a 2-hop chain:
      //   originator → intermediate → (attempt to go to) third
      //
      // Step 1: originator produces an event.
      // Step 2: intermediate ingests it (provenance grows to length 2).
      // Step 3: tamper provenance[1].arrival_hash on the intermediate copy.
      // Step 4: third destination ingests the tampered event and records
      //         the broken link.

      final orig = await _openStore(
        hopId: 'mobile-device',
        identifier: 'device-1',
      );
      final inter = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );
      final third = await _openStore(
        hopId: 'archive-server',
        identifier: 'archive-1',
        softwareVersion: 'archive@0.1.0',
      );

      try {
        // 1. Originate.
        final original = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-chain',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(original, isNotNull);

        // 2. Intermediate ingests — stored copy has 2-entry provenance.
        final outcome1 = await inter.store.ingestEvent(original!);
        expect(outcome1.outcome, equals(IngestOutcome.ingested));

        // Read the intermediate's stored copy.
        final intermediateStored = await inter.backend.transaction(
          (txn) async =>
              inter.backend.findEventByIdInTxn(txn, original.eventId),
        );
        expect(
          intermediateStored!.metadata['provenance'] as List<Object?>,
          hasLength(2),
        );

        // 3. Build tampered copy: corrupt provenance[1].arrival_hash.
        final tamperedMap = intermediateStored.toMap();
        final metadataOrig = tamperedMap['metadata'] as Map<String, Object?>;
        final metadata = Map<String, Object?>.from(metadataOrig);
        final provList = (metadata['provenance'] as List<Object?>).toList();
        final hop1 = Map<String, Object?>.from(
          provList[1] as Map<String, Object?>,
        );
        hop1['arrival_hash'] = 'tampered-arrival-hash-00000000000000000000';
        provList[1] = hop1;
        metadata['provenance'] = provList;
        tamperedMap['metadata'] = metadata;
        // Reseal the record so its own hash verifies and the broken arrival
        // hash is what the receiver refuses.
        tamperedMap['event_hash'] = canonicalEventHash(tamperedMap);
        final tampered = StoredEvent.fromMap(tamperedMap, 0);

        // 4. Third destination stores the event as received and records
        //    the broken arrival hash.
        final outcome = await third.store.ingestEvent(tampered);
        expect(outcome.outcome, IngestOutcome.ingestedWithFinding);
        final stored = await third.backend.findEventById(original.eventId);
        expect(stored, isNotNull);
        final recorded = await findings(third);
        expect(recorded, hasLength(1));
        expect(recorded.single.data['kind'], 'hash_mismatch');
        expect(recorded.single.data['evidence'], <String, Object?>{
          'event_id': original.eventId,
          'carried_hash': 'tampered-arrival-hash-00000000000000000000',
          'recomputed_hash': original.eventHash,
        });
        expect(recorded.single.data['aggregates'], <String>['agg-chain']);
      } finally {
        await orig.close();
        await inter.close();
        await third.close();
      }
    });

    // Verifies: EVS-DEV-security-findings/O
    test('a hand-crafted event with no arrival_hash at hop 1 is kept in one '
        'event_malformed finding and not stored', () async {
      // Construct a synthetic 2-hop provenance where provenance[1] has
      // no arrival_hash key (null-equivalent for a receiver entry).
      final dest = await _openStore(hopId: 'control-server');

      try {
        final now = DateTime.utc(2026, 4, 24, 12);
        final provenanceList = [
          <String, Object?>{
            'hop': 'mobile-device',
            'received_at': now.toIso8601String(),
            'identifier': 'device-1',
            'software_version': 'my_app@1.0.0',
            'database_id': kPeerDatabaseId,
            'library_version': kPeerLibraryVersion,
          },
          <String, Object?>{
            // Missing 'arrival_hash' — invalid for a receiver hop.
            'hop': 'intermediate',
            'received_at': now.toIso8601String(),
            'identifier': 'intermediate-1',
            'software_version': 'intermediate@1.0.0',
            'database_id': 'intermediate-database',
            'library_version': kPeerLibraryVersion,
          },
        ];
        final metadata = <String, Object?>{
          'change_reason': 'initial',
          'provenance': provenanceList,
        };
        final recordMap = <String, Object?>{
          'event_id': 'test-chain-null-arrival',
          'aggregate_id': 'agg-chain-null',
          'aggregate_type': 'note',
          'entry_type': 'epistaxis_event',
          'entry_type_version': <String, Object?>{'major': 1, 'minor': 0},
          'lib_format_version': LibVersion.dataFormat.toJson(),
          'event_type': 'finalized',
          'sequence_number': 1,
          'data': const <String, Object?>{},
          'metadata': metadata,
          'initiator': const <String, Object?>{'type': 'user', 'user_id': 'u1'},
          'flow_token': null,
          'client_timestamp': now.toIso8601String(),
          'previous_event_hash': null,
          'causal': kRootVersionCausalJson,
        };
        recordMap['event_hash'] = canonicalEventHash(recordMap);
        final syntheticEvent = StoredEvent.fromMap(recordMap, 0);

        final outcome = await dest.store.ingestEvent(syntheticEvent);
        expect(outcome.outcome, IngestOutcome.keptInFinding);
        expect(
          await dest.backend.findEventById('test-chain-null-arrival'),
          isNull,
        );
        final recorded = await findings(dest);
        expect(recorded, hasLength(1));
        expect(recorded.single.data['kind'], 'event_malformed');
        expect(
          (recorded.single.data['evidence']! as Map)['reason'],
          'record_malformed',
        );
      } finally {
        await dest.close();
      }
    });
  });
}
