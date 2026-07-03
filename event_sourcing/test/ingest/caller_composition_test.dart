// Verifies: EVS-PRD-ingest/F — idempotency: identity-mismatching re-present
//   rolls back entirely; original stored event is not overwritten
// Verifies: EVS-PRD-ingest/D — IngestIdentityMismatch thrown when incoming
//   hash diverges from stored arrival hash
// Verifies: EVS-PRD-hash-chain-integrity/B — Chain 2 previous_ingest_hash
//   on the rejection audit event links back to the pre-ingest stored hash

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:uuid/uuid.dart';

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
    'caller-comp-$_dbCounter.db',
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

/// Build a [BatchEnvelope] bytes payload containing the given [events].
BatchEnvelope _buildEnvelope(List<StoredEvent> events) {
  return BatchEnvelope(
    batchFormatVersion: '1',
    batchId: const Uuid().v4(),
    senderHop: 'mobile-device',
    senderIdentifier: 'device-1',
    senderSoftwareVersion: 'my_app@1.0.0',
    sentAt: DateTime.now().toUtc(),
    events: events.map((e) => Map<String, Object?>.from(e.toMap())).toList(),
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('Caller-composed ingestBatch + logRejectedBatch pattern', () {
    test(
      'caller composes ingestBatch + logRejectedBatch on identity mismatch: '
      'subject events do NOT land, rejection audit event IS recorded',
      () async {
        final orig = await _openStore(hopId: 'mobile-device');
        final dest = await _openStore(
          hopId: 'control-server',
          identifier: 'control-1',
          softwareVersion: 'control@0.1.0',
        );

        try {
          // 1. Originate e1 and pre-ingest it at destination.
          final e1 = await orig.store.append(
            entryType: 'epistaxis_event',
            aggregateId: 'agg-caller-comp-1',
            aggregateType: 'note',
            eventType: 'finalized',
            data: const {
              'answers': {'severity': 'mild'},
            },
            initiator: const UserInitiator('u1'),
          );
          expect(e1, isNotNull);
          await dest.store.ingestEvent(e1!);

          // Capture the destination's local sequence counter after the
          // first ingest. Under the unified event store, this counter is
          // also the Chain 2 tail position.
          final seqAfterFirstIngest = await dest.backend.readSequenceCounter();

          // 2. Build batch with a tampered e1' — same event_id, different hash.
          final e1TamperedMap = e1.toMap();
          e1TamperedMap['event_hash'] =
              'tampered-hash-abcdef1234567890abcdef01';
          final e1Tampered = StoredEvent.fromMap(e1TamperedMap, 0);

          final envelope = _buildEnvelope([e1Tampered]);
          final bytes = envelope.encode();

          // 3. Caller-composed pattern: catch IngestIdentityMismatch and log it.
          IngestIdentityMismatch? caught;
          try {
            await dest.store.ingestBatch(
              bytes,
              wireFormat: BatchEnvelope.wireFormat,
            );
          } on IngestIdentityMismatch catch (e) {
            caught = e;
            await dest.store.logRejectedBatch(
              bytes,
              wireFormat: BatchEnvelope.wireFormat,
              reason: 'identityMismatch',
              failedEventId: e.eventId,
              errorDetail: e.toString(),
            );
          }

          // 4a. ingestBatch threw IngestIdentityMismatch.
          expect(caught, isNotNull);
          expect(caught!.eventId, equals(e1.eventId));

          // 4b. No subject events from the rejected batch landed.
          //     The destination's local seq counter advanced exactly once
          //     past the pre-ingest of e1 — the rolled-back ingestBatch
          //     consumed no slots, and the rejection audit event added one.
          final seqAfterRejection = await dest.backend.readSequenceCounter();
          expect(
            seqAfterRejection,
            equals(seqAfterFirstIngest + 1),
            reason:
                'Local seq advanced only once (for rejection audit), not '
                'by the rolled-back ingestBatch',
          );

          // 4c. Exactly one ingest.batch_rejected event with expected fields.
          const auditAggId = 'ingest-audit:control-server';
          final auditEvents = await dest.backend.findEventsForAggregate(
            auditAggId,
          );
          final rejectedEvents = auditEvents
              .where((e) => e.eventType == 'ingest.batch_rejected')
              .toList();
          expect(rejectedEvents, hasLength(1));

          final rejData = rejectedEvents[0].data;
          expect(rejData['reason'], equals('identityMismatch'));
          expect(rejData['failed_event_id'], equals(e1.eventId));
          expect(rejData['wire_format'], equals(BatchEnvelope.wireFormat));

          // 4d. The original e1 is still stored with its original content
          //     (not overwritten by the identity-mismatching e1').
          final storedE1 = await dest.backend.transaction(
            (txn) async => dest.backend.findEventByIdInTxn(txn, e1.eventId),
          );
          expect(storedE1, isNotNull);
          // The stored hash must differ from the tampered hash.
          expect(
            storedE1!.eventHash,
            isNot(equals('tampered-hash-abcdef1234567890abcdef01')),
          );
          // The stored data must match e1's original data.
          expect(storedE1.data['answers'], equals({'severity': 'mild'}));
        } finally {
          await orig.close();
          await dest.close();
        }
      },
    );

    test('rejection audit Chain 2 previous_ingest_hash links back to '
        'the pre-ingest event hash (not null on second Chain 2 entry)', () async {
      final orig = await _openStore(hopId: 'mobile-device');
      final dest = await _openStore(
        hopId: 'control-server',
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
      );

      try {
        // 1. Pre-ingest a clean e1.
        final e1 = await orig.store.append(
          entryType: 'epistaxis_event',
          aggregateId: 'agg-chain2-link-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        );
        expect(e1, isNotNull);
        final ingestResult = await dest.store.ingestEvent(e1!);

        // 2. Build a tampered batch and trigger rejection.
        final e1TamperedMap = e1.toMap();
        e1TamperedMap['event_hash'] = 'tampered-hash-abcdef1234567890abcdef02';
        final e1Tampered = StoredEvent.fromMap(e1TamperedMap, 0);
        final envelope = _buildEnvelope([e1Tampered]);
        final bytes = envelope.encode();

        try {
          await dest.store.ingestBatch(
            bytes,
            wireFormat: BatchEnvelope.wireFormat,
          );
        } on IngestIdentityMismatch catch (e) {
          await dest.store.logRejectedBatch(
            bytes,
            wireFormat: BatchEnvelope.wireFormat,
            reason: 'identityMismatch',
            failedEventId: e.eventId,
            errorDetail: e.toString(),
          );
        }

        // 3. The rejection audit event's provenance[0].previous_ingest_hash
        //    == ingestResult.resultHash (the stored hash of the pre-ingested e1).
        const auditAggId = 'ingest-audit:control-server';
        final auditEvents = await dest.backend.findEventsForAggregate(
          auditAggId,
        );
        final rejectedEvents = auditEvents
            .where((e) => e.eventType == 'ingest.batch_rejected')
            .toList();
        expect(rejectedEvents, hasLength(1));

        final prov = (rejectedEvents[0].metadata['provenance'] as List<Object?>)
            .cast<Map<String, Object?>>();
        expect(prov, hasLength(1));
        expect(
          prov[0]['previous_ingest_hash'],
          equals(ingestResult.resultHash),
        );
      } finally {
        await orig.close();
        await dest.close();
      }
    });
  });
}
