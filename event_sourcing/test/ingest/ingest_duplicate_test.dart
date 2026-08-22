// Verifies: EVS-PRD-ingest/F
// idempotency: re-presenting an already-admitted
//   event returns IngestOutcome.duplicate and does not mutate the stored subject
// Verifies: EVS-PRD-ingest/A
// ingest.duplicate_received audit event is
//   emitted under the ingest-audit aggregate for each duplicate re-presentation
// Verifies: EVS-PRD-hash-chain-integrity/C
// verifyEventChain passes on a
//   receiver-originated duplicate_received event (length-1 provenance trivially
//   valid)

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
    'ingest-dup-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  // Auto-register every reserved system entry type (includes
  // `ingest-audit`, security-context lifecycle entry types, etc.).
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: 'epistaxis_event',
      registeredVersion: 1,
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
  group('EventStore.ingestEvent — duplicate', () {
    test('second ingest of identical event returns duplicate outcome and '
        'does not mutate the stored subject', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        // Originate event.
        final e = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-dup',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e, isNotNull);

        // First ingest — should be ingested.
        final first = await dest.store.ingestEvent(e!);
        expect(first.outcome, equals(IngestOutcome.ingested));
        final hashAfterFirst = first.resultHash;

        // Read stored copy after first ingest.
        final storedAfterFirst = await dest.backend.transaction(
          (txn) async => dest.backend.findEventByIdInTxn(txn, e.eventId),
        );

        // Second ingest of same event — should be duplicate.
        final second = await dest.store.ingestEvent(e);
        expect(second.outcome, equals(IngestOutcome.duplicate));
        // Result hash is unchanged (stored copy not mutated).
        expect(second.resultHash, equals(hashAfterFirst));
        expect(second.eventId, equals(e.eventId));

        // Stored subject is identical after second ingest.
        final storedAfterSecond = await dest.backend.transaction(
          (txn) async => dest.backend.findEventByIdInTxn(txn, e.eventId),
        );
        expect(
          storedAfterSecond!.eventHash,
          equals(storedAfterFirst!.eventHash),
        );
        final provBefore =
            storedAfterFirst.metadata['provenance'] as List<Object?>;
        final provAfter =
            storedAfterSecond.metadata['provenance'] as List<Object?>;
        expect(provAfter.length, equals(provBefore.length));
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    test(
      'duplicate ingest emits ingest.duplicate_received under ingest-audit aggregate',
      () async {
        final orig = await _openStore(hopId: 'mobile-device');
        final dest = await _openStore(
          hopId: 'control-server',
          identifier: 'control-1',
          softwareVersion: 'control@0.1.0',
        );

        try {
          final e = await orig.store.append(
            entryType: 'epistaxis_event',
            aggregateId: 'agg-dup2',
            aggregateType: 'note',
            eventType: 'finalized',
            data: const {'answers': {}},
            initiator: const UserInitiator('u1'),
          );
          expect(e, isNotNull);

          await dest.store.ingestEvent(e!);
          await dest.store.ingestEvent(e);

          // Query the ingest-audit aggregate.
          const auditAggId = 'ingest-audit:control-server';
          final auditEvents = await dest.backend.findEventsForAggregate(
            auditAggId,
          );
          expect(auditEvents, hasLength(1));
          expect(
            auditEvents.first.eventType,
            equals('ingest.duplicate_received'),
          );
          expect(auditEvents.first.data['subject_event_id'], equals(e.eventId));
        } finally {
          await orig.close();
          await dest.close();
        }
      },
    );

    test('duplicate_received event carries batchContext absent (null) for '
        'ingestEvent path', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        final e = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-dup3',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e, isNotNull);

        await dest.store.ingestEvent(e!);
        await dest.store.ingestEvent(e);

        // The audit event's provenance[0].batchContext must be absent/null.
        const auditAggId = 'ingest-audit:control-server';
        final auditEvents = await dest.backend.findEventsForAggregate(
          auditAggId,
        );
        expect(auditEvents, hasLength(1));

        final prov = (auditEvents.first.metadata['provenance'] as List<Object?>)
            .cast<Map<String, Object?>>();
        expect(prov, hasLength(1));
        expect(prov[0].containsKey('batch_context'), isFalse);
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    test('verifyEventChain passes on an ingest.duplicate_received audit event '
        '', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        // 1. Originate an event.
        final e = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-dup-chain-verify',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e, isNotNull);

        // 2. First ingest — lands the subject event.
        await dest.store.ingestEvent(e!);

        // 3. Second ingest of same event — emits ingest.duplicate_received.
        await dest.store.ingestEvent(e);

        // 4. Query the ingest-audit aggregate for the duplicate_received event.
        const auditAggId = 'ingest-audit:control-server';
        final auditEvents = await dest.backend.findEventsForAggregate(
          auditAggId,
        );
        final dupEvents = auditEvents
            .where((ev) => ev.eventType == 'ingest.duplicate_received')
            .toList();
        expect(dupEvents, hasLength(1));

        final dupEvent = dupEvents.first;

        // 5. The duplicate_received event is receiver-originated, so its
        //    provenance has exactly one entry (the receiver hop). The walk
        //    loop in verifyEventChain iterates from length-1 down to k=1;
        //    for length-1 (k stops at 1, i.e. never executes), it returns
        //    trivially ok=true. Confirm this.
        final provenance = (dupEvent.metadata['provenance'] as List<Object?>)
            .cast<Map<String, Object?>>();
        expect(
          provenance,
          hasLength(1),
          reason: 'receiver-originated event has a single-entry provenance',
        );

        final verdict = await dest.store.verifyEventChain(dupEvent);
        expect(verdict.isValid, isTrue);
        expect(verdict.failures, isEmpty);
      } finally {
        await orig.close();
        await dest.close();
      }
    });

    test('consecutive re-ingests emit one duplicate_received each', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        final e = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-dup4',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e, isNotNull);

        await dest.store.ingestEvent(e!);
        await dest.store.ingestEvent(e);
        await dest.store.ingestEvent(e);

        const auditAggId = 'ingest-audit:control-server';
        final auditEvents = await dest.backend.findEventsForAggregate(
          auditAggId,
        );
        expect(
          auditEvents,
          hasLength(2),
        ); // Two re-ingests => two audit events.
      } finally {
        await orig.close();
        await dest.close();
      }
    });
  });
}
