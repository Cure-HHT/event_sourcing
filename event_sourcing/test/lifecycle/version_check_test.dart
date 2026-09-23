// The in-transaction reader of the library-version events a database
// appended itself.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/local_event.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/lib_version_seed.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'vc-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

Future<LocalLibVersionHistory> _read(SembastBackend backend) =>
    backend.transaction((txn) => VersionCheck.readLocalInTxn(backend, txn));

/// A copy of [event] as a receiver stores it after ingest: with a receiver
/// provenance entry carrying the arrival hash.
StoredEvent _asIngested(StoredEvent event) => StoredEvent.synthetic(
  eventId: '${event.eventId}-ingested',
  aggregateId: event.aggregateId,
  aggregateType: event.aggregateType,
  entryType: event.entryType,
  eventType: event.eventType,
  eventHash: 'ingested-${event.eventHash}',
  initiator: event.initiator,
  clientTimestamp: event.clientTimestamp,
  data: Map<String, dynamic>.from(event.data),
  metadata: <String, dynamic>{
    'provenance': <Map<String, Object?>>[
      ...(event.metadata['provenance'] as List).cast<Map<String, Object?>>(),
      ProvenanceEntry(
        hop: 'receiver',
        receivedAt: DateTime.utc(2026, 5),
        identifier: 'receiver-install',
        softwareVersion: 'receiver',
        arrivalHash: event.eventHash,
        ingestSequenceNumber: 9,
      ).toJson(),
    ],
  },
);

void main() {
  group('VersionCheck.readLocalInTxn', () {
    // Verifies: EVS-DEV-event-store-open/B
    test('reads no event when no version events exist', () async {
      final backend = await _openBackend();
      final result = await _read(backend);
      expect(result.events, isEmpty);
      expect(result.latest, isNull);
      expect(result.firstInitialized, isNull);
      await backend.close();
    });

    // Verifies: EVS-DEV-event-store-open/B
    // Verifies: EVS-DEV-event-store-open/F
    test('reads the initialization with its version, data format and '
        'identity', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '0.4.0',
        dataFormat: const DataFormatVersion(2, 0),
        databaseId: 'db-1',
      );
      final result = await _read(backend);
      expect(result.latest?.packageVersion, '0.4.0');
      expect(result.latest?.dataFormat, const DataFormatVersion(2, 0));
      expect(result.firstInitialized?.databaseId, 'db-1');
      await backend.close();
    });

    // Verifies: EVS-DEV-event-store-open/C
    test('the latest is a change recorded after the initialization', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '0.4.0',
        dataFormat: const DataFormatVersion(2, 0),
      );
      await seedLibVersionEventForTest(
        backend,
        eventType: LibVersionEvents.changed,
        version: '0.4.1',
        dataFormat: const DataFormatVersion(2, 1),
      );
      final result = await _read(backend);
      expect(result.events, hasLength(2));
      expect(result.latest?.packageVersion, '0.4.1');
      expect(result.latest?.dataFormat, const DataFormatVersion(2, 1));
      expect(result.firstInitialized?.packageVersion, '0.4.0');
      await backend.close();
    });

    // Verifies: EVS-DEV-event-store-open/F
    test('an ingested library-version event is left out, however late it '
        'was stored', () async {
      final backend = await _openBackend();
      final local = await seedLibVersionEventForTest(
        backend,
        version: '0.4.0',
        dataFormat: const DataFormatVersion(2, 0),
        databaseId: 'db-local',
      );
      final peer = await _openBackend();
      final peerEvent = await seedLibVersionEventForTest(
        peer,
        version: '0.9.0',
        dataFormat: const DataFormatVersion(2, 3),
        databaseId: 'db-peer',
      );
      final ingested = _asIngested(peerEvent);
      expect(isLocallyAppended(local), isTrue);
      expect(isLocallyAppended(ingested), isFalse);
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          StoredEvent.synthetic(
            eventId: ingested.eventId,
            aggregateId: ingested.aggregateId,
            aggregateType: ingested.aggregateType,
            entryType: ingested.entryType,
            eventType: ingested.eventType,
            sequenceNumber: seq,
            eventHash: ingested.eventHash,
            initiator: ingested.initiator,
            clientTimestamp: ingested.clientTimestamp,
            data: Map<String, dynamic>.from(ingested.data),
            metadata: Map<String, dynamic>.from(ingested.metadata),
          ),
        );
      });
      final result = await _read(backend);
      expect(result.events, hasLength(1));
      expect(result.latest?.packageVersion, '0.4.0');
      expect(result.firstInitialized?.databaseId, 'db-local');
      await backend.close();
      await peer.close();
    });

    // Verifies: EVS-DEV-event-store-open/B
    test('reads an append made earlier in the same transaction', () async {
      final backend = await _openBackend();
      final seen = await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          StoredEvent.synthetic(
            eventId: 'in-txn',
            aggregateId: '_lib',
            aggregateType: '_lib',
            entryType: LibVersionEvents.initialized,
            eventType: LibVersionEvents.initialized,
            sequenceNumber: seq,
            eventHash: 'h-in-txn',
            initiator: const AutomationInitiator(service: 'event_sourcing'),
            clientTimestamp: DateTime.utc(2026, 5),
            data: <String, dynamic>{
              'version': '0.5.0',
              'data_format': LibVersion.dataFormat.toJson(),
              'database_id': 'db-in-txn',
            },
          ),
        );
        return VersionCheck.readLocalInTxn(backend, txn);
      });
      expect(seen.latest?.event.eventId, 'in-txn');
      await backend.close();
    });
  });
}
