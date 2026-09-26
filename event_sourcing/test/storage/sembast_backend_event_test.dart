// Verifies: EVS-PRD-event-log/B
// sequence-counter bookkeeping lands in
//   the `backend_state` store rather than in an event-level `metadata`
//   namespace. This file holds the sembast-specific layout assertions
//   that pin where bookkeeping lives in the on-disk shape; the abstract
//   StorageBackend contract for the event log (atomicity, monotonicity,
//   per-aggregate order, in-order reads, findAllEvents filters, etc.)
//   is exercised against this backend by
//   `sembast_backend_conformance_test.dart` via the backend-agnostic
//   conformance harness in `storage_backend_conformance.dart`.
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/record_fixtures.dart';

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
            entryTypeVersion: const EntryTypeVersion(1, 0),
            libFormatVersion: LibVersion.dataFormat,
            eventType: 'Event',
            sequenceNumber: s,
            data: const <String, dynamic>{},
            metadata: const <String, dynamic>{},
            initiator: const UserInitiator('u'),
            clientTimestamp: DateTime.utc(2026, 4, 22),
            eventHash: 'hash-ev-1',
            causal: kRootVersionCausal,
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

    // Verifies: EVS-DEV-event-record/H
    // Verifies: EVS-DEV-causal-parents/B
    test('a read refuses a stored record whose provenance entry lacks '
        'library_version, or that carries no causal object, naming the '
        'field', () async {
      final record = StoredEvent.synthetic(
        eventId: 'ev-raw',
        aggregateId: 'agg-raw',
        entryType: 'epistaxis_event',
        sequenceNumber: 1,
        initiator: const UserInitiator('u'),
        clientTimestamp: DateTime.utc(2026, 4, 22),
        eventHash: 'hash-raw',
        metadata: <String, dynamic>{
          'provenance': <Map<String, Object?>>[
            <String, Object?>{
              'hop': 'mobile-device',
              'received_at': '2026-04-22T00:00:00.000Z',
              'identifier': 'device-1',
              'software_version': 'app@1.0.0',
              'database_id': kPeerDatabaseId,
              'library_version': kPeerLibraryVersion,
            },
          ],
        },
      ).toMap();
      final cases = <String, Map<String, Object?>>{
        '"library_version"': Map<String, Object?>.from(record)
          ..['metadata'] = <String, Object?>{
            'provenance': <Map<String, Object?>>[
              Map<String, Object?>.from(
                ((record['metadata']! as Map)['provenance']! as List).single
                    as Map,
              )..remove('library_version'),
            ],
          },
        '"causal"': Map<String, Object?>.from(record)..remove('causal'),
      };
      final events = intMapStoreFactory.store('events');
      for (final c in cases.entries) {
        await events.delete(backend.databaseForTesting);
        await events.add(backend.databaseForTesting, c.value);
        await expectLater(
          backend.findAllEvents(),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains(c.key),
            ),
          ),
          reason: c.key,
        );
      }
    });
  });
}
