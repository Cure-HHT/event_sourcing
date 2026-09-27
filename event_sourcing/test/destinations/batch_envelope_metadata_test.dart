// Verifies: EVS-PRD-destinations/E
// exercises BatchEnvelopeMetadata: the envelope fields a queue item of a
// destination that serializes natively persists, so the drainer rebuilds
// its delivery from them and the events the item names; verifies
// round-trip serialization and value equality.
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/ingest/delivery_envelope.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('BatchEnvelopeMetadata', () {
    const channel = DeliveryChannel(
      senderDatabaseId: 'db-1',
      destinationId: 'dest',
      registrationId: 'reg-1',
      generation: 2,
    );
    final fixture = BatchEnvelopeMetadata(
      batchFormatVersion: '3',
      batchId: 'b-001',
      senderHop: 'mobile-1',
      senderIdentifier: 'device-uuid',
      senderSoftwareVersion: 'diary@1.2.3',
      sentAt: DateTime.utc(2026, 4, 25, 12, 0, 0),
      channel: channel,
      attributes: const <String, Object?>{'later_fact': 1},
    );

    test('round-trip via toMap / fromMap is value-equal', () {
      final map = fixture.toMap();
      final restored = BatchEnvelopeMetadata.fromMap(map);
      expect(restored, fixture);
      expect(restored.channel, channel);
      expect(restored.attributes, const <String, Object?>{'later_fact': 1});
    });

    test('an item of a delivery channel is in the native delivery format', () {
      expect(fixture.wireFormat, DeliveryEnvelope.wireFormat);
    });

    test('equality and hashCode are value-based', () {
      final a = BatchEnvelopeMetadata.fromMap(fixture.toMap());
      final b = BatchEnvelopeMetadata.fromMap(fixture.toMap());
      expect(a, b);
      expect(a.hashCode, b.hashCode);
      final other = BatchEnvelopeMetadata.fromMap(<String, Object?>{
        ...fixture.toMap(),
        'attributes': const <String, Object?>{'later_fact': 2},
      });
      expect(other, isNot(a));
    });
  });
}
