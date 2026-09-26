// Verifies: EVS-PRD-event-log/C
// originatorHop exposes provenance[0] for
//   per-aggregate-per-authority discrimination; StateError on missing/empty
//   provenance signals a malformed event record.
// Verifies: EVS-DEV-chain-verification/A
// a copy's originating database, sealed hash and origin position read the
//   same at every holder: from the copy itself when its provenance holds
//   one entry, otherwise from its first and second entries.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../test_support/record_fixtures.dart';

StoredEvent _eventWithProvenance(
  List<Map<String, Object?>> provenance, {
  int sequenceNumber = 1,
  String eventHash = 'hash-1',
}) => StoredEvent(
  key: 0,
  eventId: 'ev-1',
  aggregateId: 'agg-1',
  aggregateType: 'note',
  entryType: 'epistaxis_event',
  entryTypeVersion: const EntryTypeVersion(1, 0),
  libFormatVersion: LibVersion.dataFormat,
  eventType: 'finalized',
  sequenceNumber: sequenceNumber,
  data: const <String, Object?>{},
  metadata: <String, Object?>{
    'change_reason': 'initial',
    'provenance': provenance,
  },
  initiator: const UserInitiator('u1'),
  clientTimestamp: DateTime.utc(2026, 4, 26),
  eventHash: eventHash,
  causal: kRootVersionCausal,
);

void main() {
  // materialized from metadata.provenance[0].
  test('originatorHop returns provenance.first', () {
    final event = _eventWithProvenance(<Map<String, Object?>>[
      <String, Object?>{
        'hop': 'mobile-device',
        'received_at': '2026-04-26T00:00:00.000Z',
        'identifier': 'install-A',
        'software_version': 'my_app@1.0.0',
        'database_id': kPeerDatabaseId,
        'library_version': kPeerLibraryVersion,
      },
      <String, Object?>{
        'hop': 'control-server',
        'received_at': '2026-04-26T00:00:01.000Z',
        'identifier': 'control-1',
        'software_version': 'control@0.1.0',
        'database_id': 'control-database',
        'library_version': kPeerLibraryVersion,
      },
    ]);

    final originator = event.originatorHop;
    expect(originator.identifier, 'install-A');
    expect(originator.hop, 'mobile-device');
  });

  test('empty provenance throws StateError', () {
    final event = _eventWithProvenance(const <Map<String, Object?>>[]);
    expect(() => event.originatorHop, throwsStateError);
  });

  group('origin of a copy', () {
    final originator = <String, Object?>{
      'hop': 'mobile-device',
      'received_at': '2026-04-26T00:00:00.000Z',
      'identifier': 'install-A',
      'software_version': 'my_app@1.0.0',
      'database_id': 'database-A',
      'library_version': '0.9.0',
      'ingest_sequence_number': 3,
    };
    Map<String, Object?> receiver({
      required String databaseId,
      required String arrivalHash,
      required int originSequenceNumber,
      required int ingestSequenceNumber,
    }) => <String, Object?>{
      'hop': 'server',
      'received_at': '2026-04-26T00:00:01.000Z',
      'identifier': 'server-1',
      'software_version': 'server@1.0.0',
      'database_id': databaseId,
      'library_version': '0.9.0',
      'arrival_hash': arrivalHash,
      'origin_sequence_number': originSequenceNumber,
      'ingest_sequence_number': ingestSequenceNumber,
    };

    test('a copy held as authored reads its own hash and position', () {
      final event = _eventWithProvenance(
        <Map<String, Object?>>[originator],
        sequenceNumber: 3,
        eventHash: 'sealed-hash',
      );
      expect(event.originatingDatabaseId, 'database-A');
      expect(event.sealedHash, 'sealed-hash');
      expect(event.originPosition, 3);
      expect(event.isHeldAsAuthoredBy('database-A'), isTrue);
      expect(event.isHeldAsAuthoredBy('database-B'), isFalse);
    });

    test('a received copy reads the hash and position its second entry '
        'records, whatever later hops re-stamped', () {
      final event = _eventWithProvenance(
        <Map<String, Object?>>[
          originator,
          receiver(
            databaseId: 'database-B',
            arrivalHash: 'sealed-hash',
            originSequenceNumber: 3,
            ingestSequenceNumber: 40,
          ),
          receiver(
            databaseId: 'database-C',
            arrivalHash: 'hash-at-B',
            originSequenceNumber: 40,
            ingestSequenceNumber: 900,
          ),
        ],
        sequenceNumber: 900,
        eventHash: 'hash-at-C',
      );
      expect(event.originatingDatabaseId, 'database-A');
      expect(event.sealedHash, 'sealed-hash');
      expect(event.originPosition, 3);
      expect(event.isHeldAsAuthoredBy('database-A'), isFalse);
      expect(event.isHeldAsAuthoredBy('database-C'), isFalse);
    });

    test('a copy recovered by its author is not held as authored', () {
      final event = _eventWithProvenance(
        <Map<String, Object?>>[
          originator,
          receiver(
            databaseId: 'database-A',
            arrivalHash: 'sealed-hash',
            originSequenceNumber: 3,
            ingestSequenceNumber: 12,
          ),
        ],
        sequenceNumber: 12,
        eventHash: 'hash-recovered',
      );
      expect(event.originatingDatabaseId, 'database-A');
      expect(event.sealedHash, 'sealed-hash');
      expect(event.isHeldAsAuthoredBy('database-A'), isFalse);
    });
  });
}
