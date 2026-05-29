// Verifies: EVS-PRD-destinations/E — exercises BatchEnvelopeMetadata: the
// library-native batch format for destinations that serializesNatively;
// verifies round-trip serialization and deterministic re-encoding at drain.
import 'dart:convert';

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('BatchEnvelopeMetadata', () {
    final fixture = BatchEnvelopeMetadata(
      batchFormatVersion: '1',
      batchId: 'b-001',
      senderHop: 'mobile-1',
      senderIdentifier: 'device-uuid',
      senderSoftwareVersion: 'diary@1.2.3',
      sentAt: DateTime.utc(2026, 4, 25, 12, 0, 0),
    );

    test('round-trip via toMap / fromMap is value-equal', () {
      final map = fixture.toMap();
      final restored = BatchEnvelopeMetadata.fromMap(map);
      expect(restored, fixture);
    });

    test('fromEnvelope copies metadata, drops events', () {
      final env = BatchEnvelope(
        batchFormatVersion: '1',
        batchId: 'b-001',
        senderHop: 'mobile-1',
        senderIdentifier: 'device-uuid',
        senderSoftwareVersion: 'diary@1.2.3',
        sentAt: DateTime.utc(2026, 4, 25, 12, 0, 0),
        events: <Map<String, Object?>>[
          <String, Object?>{'event_id': 'e1'},
          <String, Object?>{'event_id': 'e2'},
        ],
      );
      final meta = BatchEnvelopeMetadata.fromEnvelope(env);
      expect(meta, fixture);
    });

    test('toEnvelope reattaches events; encode is byte-equal '
        'across two calls (RFC 8785 JCS determinism)', () {
      final events = <Map<String, Object?>>[
        <String, Object?>{'event_id': 'e1', 'sequence_number': 1},
        <String, Object?>{'event_id': 'e2', 'sequence_number': 2},
      ];
      final bytes1 = fixture.toEnvelope(events).encode();
      final bytes2 = fixture.toEnvelope(events).encode();
      expect(bytes1, bytes2);
      // Sanity: parseable as JSON.
      final decoded = jsonDecode(utf8.decode(bytes1)) as Map<String, Object?>;
      expect(decoded['batch_id'], 'b-001');
      expect((decoded['events']! as List).length, 2);
    });

    test('equality and hashCode are value-based', () {
      final a = BatchEnvelopeMetadata.fromMap(fixture.toMap());
      final b = BatchEnvelopeMetadata.fromMap(fixture.toMap());
      expect(a, b);
      expect(a.hashCode, b.hashCode);
    });
  });
}
