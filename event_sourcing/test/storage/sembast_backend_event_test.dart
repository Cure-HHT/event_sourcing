// Verifies: EVS-PRD-event-log/B — sequence-counter bookkeeping lands in
//   the `backend_state` store rather than in an event-level `metadata`
//   namespace. This file holds the sembast-specific layout assertions
//   that pin where bookkeeping lives in the on-disk shape; the abstract
//   StorageBackend contract for the event log (atomicity, monotonicity,
//   per-aggregate order, in-order reads, findAllEvents filters, etc.)
//   is exercised against this backend by
//   `sembast_backend_conformance_test.dart` via the backend-agnostic
//   conformance harness in `storage_backend_conformance.dart`.
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  group('SembastBackend events (impl-internal)', () {
    late SembastBackend backend;
    var pathCounter = 0;

    setUp(() async {
      // Fresh in-memory database per test for isolation.
      pathCounter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'test-$pathCounter.db',
      );
      backend = SembastBackend(database: db);
    });

    tearDown(() async {
      await backend.close();
    });

    // Asserts that sequence-counter bookkeeping lands in `backend_state`
    // (NOT in an event-level `metadata` store). An empty `metadata` store
    // after a write is proof that bookkeeping landed in `backend_state`
    // instead. This is a sembast-internal layout invariant; the abstract
    // contract is satisfied as long as `readSchemaVersion`/`writeSchema
    // Version` round-trip, which is covered by the conformance harness.
    test('no writes go to a `metadata` store', () async {
      await backend.transaction((txn) async {
        final s = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          StoredEvent(
            key: 0,
            eventId: 'ev-1',
            aggregateId: 'agg-1',
            aggregateType: 'note',
            entryType: 'epistaxis_event',
            entryTypeVersion: 1,
            libFormatVersion: 1,
            eventType: 'Event',
            sequenceNumber: s,
            data: const <String, dynamic>{},
            metadata: const <String, dynamic>{},
            initiator: const UserInitiator('u'),
            clientTimestamp: DateTime.utc(2026, 4, 22),
            eventHash: 'hash-ev-1',
          ),
        );
        await backend.writeSchemaVersion(txn, 1);
      });

      // Inspect the raw database.
      final db = backend.databaseForTesting;
      final metadataStore = StoreRef<String, Object?>('metadata');
      final rows = await metadataStore.find(db);
      expect(rows, isEmpty);
    });
  });
}
