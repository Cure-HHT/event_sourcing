// An event arriving under an identifier the receiver holds under another
// sealed hash is kept in full in an identity_mismatch finding; the held copy
// stays.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

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
    'ingest-mismatch-$_dbCounter.db',
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
  group('EventStore.ingestEvent — identity mismatch', () {
    // Verifies: EVS-DEV-security-findings/G
    // Verifies: EVS-PRD-ingest/F
    test('same event_id under a different sealed hash is kept in full in one '
        'identity_mismatch finding and not stored', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        // 1. Originate an event and ingest it.
        final e1 = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-mismatch',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {
            'answers': {'severity': 'mild'},
          },
          initiator: const UserInitiator('u1'),
        );
        expect(e1, isNotNull);
        await dest.store.ingestEvent(e1!);

        // 2. Build a divergent copy: same event_id, different content,
        //    sealed with the canonical hash of that content.
        final tamperedMap = e1.toMap();
        tamperedMap['data'] = const {
          'answers': {'severity': 'severe'},
        };
        final divergentHash = canonicalEventHash(tamperedMap);
        tamperedMap['event_hash'] = divergentHash;
        final tampered = StoredEvent.fromMap(tamperedMap, 0);

        // 3. Re-ingest with the divergent copy: kept in a finding.
        final outcome = await dest.store.ingestEvent(tampered);
        expect(outcome.outcome, IngestOutcome.keptInFinding);
        expect(outcome.resultHash, isNull);
        final findings = await dest.backend.findAllEvents(
          entryType: kSecurityFindingEntryType,
        );
        expect(findings, hasLength(1));
        expect(findings.single.data['kind'], 'identity_mismatch');
        expect(findings.single.data['evidence'], <String, Object?>{
          'event_id': e1.eventId,
          'held_hash': e1.eventHash,
          'record': tampered.toMap(),
        });
        expect(findings.single.data['aggregates'], <String>['agg-mismatch']);
        expect(
          (findings.single.data['evidence']! as Map)['record'],
          containsPair('event_hash', divergentHash),
        );

        // 4. The held copy stays: its data is the first history's.
        final held = await dest.backend.findEventById(e1.eventId);
        expect(held!.data, e1.data);
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    // Verifies: EVS-DEV-security-findings/G
    test('identity mismatch: no ingest-audit events emitted, since the '
        'record is not a duplicate', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        final e1 = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-mismatch2',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e1, isNotNull);
        await dest.store.ingestEvent(e1!);

        final tamperedMap = e1.toMap();
        tamperedMap['data'] = const {
          'answers': {'severity': 'severe'},
        };
        tamperedMap['event_hash'] = canonicalEventHash(tamperedMap);
        final tampered = StoredEvent.fromMap(tamperedMap, 0);

        final outcome = await dest.store.ingestEvent(tampered);
        expect(outcome.outcome, IngestOutcome.keptInFinding);

        // No ingest.duplicate_received events (finding path, not dup path).
        final auditEvents = await dest.backend.findEventsForAggregate(
          'ingest-audit:control-server',
        );
        expect(auditEvents, isEmpty);
      } finally {
        await orig.close();
        await dest.close();
      }
    });
  });
}
