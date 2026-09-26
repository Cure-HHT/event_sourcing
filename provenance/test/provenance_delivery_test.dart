// Verifies: EVS-PRD-provenance/A
// a receiver-hop entry carries an optional delivery object naming the
//   channel and the delivery number the event arrived in.
// Verifies: EVS-PRD-provenance/C
// the delivery object serializes and deserializes without loss, and an
//   entry without it encodes no delivery key.
import 'dart:convert';

import 'package:provenance/provenance.dart';
import 'package:test/test.dart';

const ProvenanceDelivery _delivery = ProvenanceDelivery(
  senderDatabaseId: 'db-sender',
  destinationId: 'primary',
  registrationId: 'reg-1',
  generation: 2,
  deliveryNumber: 7,
);

Map<String, Object?> _deliveryJson() => <String, Object?>{
  'channel': <String, Object?>{
    'sender_database_id': 'db-sender',
    'destination_id': 'primary',
    'registration_id': 'reg-1',
    'generation': 2,
  },
  'delivery_number': 7,
};

ProvenanceEntry _entry({ProvenanceDelivery? delivery}) => ProvenanceEntry(
  hop: 'receiver',
  receivedAt: DateTime.utc(2026, 9, 26),
  identifier: 'server-1',
  softwareVersion: 'server@1.0.0',
  libraryVersion: 'event_sourcing@0.5.0',
  databaseId: 'db-receiver',
  delivery: delivery,
);

void main() {
  group('ProvenanceEntry delivery', () {
    test('encodes exactly the channel and the delivery number', () {
      expect(_delivery.toJson(), _deliveryJson());
      expect(_entry(delivery: _delivery).toJson()['delivery'], _deliveryJson());
    });

    test('round-trips through JSON', () {
      final entry = _entry(delivery: _delivery);
      final decoded = ProvenanceEntry.fromJson(
        jsonDecode(jsonEncode(entry.toJson())) as Map<String, Object?>,
      );
      expect(decoded.delivery, _delivery);
      expect(decoded, entry);
      expect(decoded.toJson(), entry.toJson());
    });

    test('an entry without it encodes no delivery key and re-encodes '
        'byte-identically', () {
      final entry = _entry();
      expect(entry.delivery, isNull);
      final encoded = jsonEncode(entry.toJson());
      expect(entry.toJson().containsKey('delivery'), isFalse);
      final decoded = ProvenanceEntry.fromJson(
        jsonDecode(encoded) as Map<String, Object?>,
      );
      expect(decoded.delivery, isNull);
      expect(jsonEncode(decoded.toJson()), encoded);
    });

    test('equality and hashCode include the delivery', () {
      expect(_entry(delivery: _delivery), _entry(delivery: _delivery));
      expect(
        _entry(delivery: _delivery).hashCode,
        _entry(delivery: _delivery).hashCode,
      );
      expect(_entry(delivery: _delivery), isNot(_entry()));
      expect(_entry(delivery: _delivery).toString(), contains('delivery'));
    });

    test('a delivery object without exactly its keys is refused', () {
      final bad = <Map<String, Object?>>[
        <String, Object?>{..._deliveryJson(), 'extra': 1},
        <String, Object?>{..._deliveryJson()}..remove('delivery_number'),
        <String, Object?>{..._deliveryJson(), 'delivery_number': 0},
        <String, Object?>{
          ..._deliveryJson(),
          'channel': <String, Object?>{'sender_database_id': 'db'},
        },
        <String, Object?>{
          ..._deliveryJson(),
          'channel': <String, Object?>{
            ...(_deliveryJson()['channel']! as Map<String, Object?>),
            'generation': 0,
          },
        },
      ];
      for (final json in bad) {
        expect(
          () => ProvenanceEntry.fromJson(<String, Object?>{
            ..._entry().toJson(),
            'delivery': json,
          }),
          throwsFormatException,
          reason: '$json',
        );
      }
      expect(
        () => ProvenanceEntry.fromJson(<String, Object?>{
          ..._entry().toJson(),
          'delivery': 'x',
        }),
        throwsFormatException,
      );
    });
  });
}
