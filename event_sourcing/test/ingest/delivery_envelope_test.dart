// Verifies: EVS-PRD-delivery-channel/A
// a channel is identified by the sending database, the destination, the
//   registration and the generation.
// Verifies: EVS-PRD-delivery-channel/B
// the delivery hash covers the channel, the number, the link, the hash of
//   every event carried and the delivery's attributes.
// Verifies: EVS-DEV-delivery-channel/C
// the delivery hash is the lowercase-hex SHA-256 of the canonical JSON of
//   exactly channel, delivery_number, previous_delivery_hash, event_hashes
//   and attributes, the attributes as carried.
// Verifies: EVS-DEV-delivery-channel/K
// the native batch envelope carries exactly its keys, batch format "3",
//   at least one event and an attributes object.
// Verifies: EVS-DEV-delivery-receiver/A
// a batch that is not in the native format, one whose attributes is not an
//   object and one that carries no event are refused by name.
import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

const DeliveryChannel _channel = DeliveryChannel(
  senderDatabaseId: 'db-sender',
  destinationId: 'primary',
  registrationId: 'reg-1',
  generation: 1,
);

// The golden vectors are the SHA-256 of these canonical strings, computed
// outside the library:
//
//   {"attributes":{},"channel":{"destination_id":"primary","generation":1,
//    "registration_id":"reg-1","sender_database_id":"db-sender"},
//    "delivery_number":2,"event_hashes":["h1","h2"],
//    "previous_delivery_hash":"prevhash"}
const String _goldenHash =
    '8747cc08c9bd932600d475654fcbdbbe0eb185885b7a47cbdd056f972ee343cb';

//   {"attributes":{},"channel":{"destination_id":"primary","generation":3,
//    "registration_id":"reg-1","sender_database_id":"db-sender"},
//    "delivery_number":1,"event_hashes":["h1"],
//    "previous_delivery_hash":null}
const String _goldenFirstHash =
    'ac8628e3adcde431d69eb6fee7027ce500be832ce1a6d08e24a25339f410f197';

//   {"attributes":{"later_fact":{"a":[1,"x"],"b":2}},"channel":{...as the
//    first...},"delivery_number":2,"event_hashes":["h1","h2"],
//    "previous_delivery_hash":"prevhash"}
const String _goldenAttributesHash =
    '2f8e07b3e638fee552e231f722ff0f49d561a7ceddabd12c686aa76e6cbcad55';

const Set<String> _envelopeKeys = <String>{
  'batch_format_version',
  'batch_id',
  'sender_hop',
  'sender_identifier',
  'sender_software_version',
  'sent_at',
  'channel',
  'delivery_number',
  'previous_delivery_hash',
  'delivery_hash',
  'events',
  'attributes',
};

const Set<String> _channelKeys = <String>{
  'sender_database_id',
  'destination_id',
  'registration_id',
  'generation',
};

Map<String, Object?> _event(String id, String hash) => <String, Object?>{
  'event_id': id,
  'event_hash': hash,
  'data': <String, Object?>{'n': id},
};

DeliveryEnvelope _sealed({
  Map<String, Object?> attributes = const <String, Object?>{},
  List<Map<String, Object?>>? events,
}) => DeliveryEnvelope.seal(
  batchId: 'batch-1',
  senderHop: 'mobile-device',
  senderIdentifier: 'device-1',
  senderSoftwareVersion: 'app@1.0.0',
  sentAt: DateTime.utc(2026, 9, 26, 12),
  channel: _channel,
  deliveryNumber: 2,
  previousDeliveryHash: 'prevhash',
  events:
      events ?? <Map<String, Object?>>[_event('e1', 'h1'), _event('e2', 'h2')],
  attributes: attributes,
);

Map<String, Object?> _wire(DeliveryEnvelope envelope) =>
    jsonDecode(utf8.decode(envelope.encode())) as Map<String, Object?>;

Uint8List _bytes(Object? json) =>
    Uint8List.fromList(utf8.encode(jsonEncode(json)));

Matcher _refusedWith(String reason) => throwsA(
  isA<IngestDecodeFailure>().having((e) => e.reason, 'reason', reason),
);

void main() {
  group('delivery hash', () {
    test('matches the golden vector', () {
      expect(
        computeDeliveryHash(
          channel: _channel,
          deliveryNumber: 2,
          previousDeliveryHash: 'prevhash',
          eventHashes: const <String>['h1', 'h2'],
          attributes: const <String, Object?>{},
        ),
        _goldenHash,
      );
    });

    test('hashes a null link for delivery 1 and the generation', () {
      expect(
        computeDeliveryHash(
          channel: const DeliveryChannel(
            senderDatabaseId: 'db-sender',
            destinationId: 'primary',
            registrationId: 'reg-1',
            generation: 3,
          ),
          deliveryNumber: 1,
          previousDeliveryHash: null,
          eventHashes: const <String>['h1'],
          attributes: const <String, Object?>{},
        ),
        _goldenFirstHash,
      );
    });

    test('a sealed envelope carries the hash of its fields', () {
      final envelope = _sealed();
      expect(envelope.deliveryHash, _goldenHash);
      expect(envelope.recomputedDeliveryHash, _goldenHash);
      expect(envelope.eventHashes, <Object?>['h1', 'h2']);
    });

    test('an unknown attribute key is covered by the hash', () {
      final envelope = _sealed(
        attributes: <String, Object?>{
          'later_fact': <String, Object?>{
            'b': 2,
            'a': <Object?>[1, 'x'],
          },
        },
      );
      expect(envelope.deliveryHash, _goldenAttributesHash);
      expect(envelope.deliveryHash, isNot(_goldenHash));
    });

    test('every hashed field changes the hash', () {
      final base = computeDeliveryHash(
        channel: _channel,
        deliveryNumber: 2,
        previousDeliveryHash: 'prevhash',
        eventHashes: const <String>['h1', 'h2'],
        attributes: const <String, Object?>{},
      );
      final variants = <String>[
        computeDeliveryHash(
          channel: const DeliveryChannel(
            senderDatabaseId: 'db-sender',
            destinationId: 'primary',
            registrationId: 'reg-1',
            generation: 2,
          ),
          deliveryNumber: 2,
          previousDeliveryHash: 'prevhash',
          eventHashes: const <String>['h1', 'h2'],
          attributes: const <String, Object?>{},
        ),
        computeDeliveryHash(
          channel: _channel,
          deliveryNumber: 3,
          previousDeliveryHash: 'prevhash',
          eventHashes: const <String>['h1', 'h2'],
          attributes: const <String, Object?>{},
        ),
        computeDeliveryHash(
          channel: _channel,
          deliveryNumber: 2,
          previousDeliveryHash: 'other',
          eventHashes: const <String>['h1', 'h2'],
          attributes: const <String, Object?>{},
        ),
        computeDeliveryHash(
          channel: _channel,
          deliveryNumber: 2,
          previousDeliveryHash: 'prevhash',
          eventHashes: const <String>['h2', 'h1'],
          attributes: const <String, Object?>{},
        ),
      ];
      for (final v in variants) {
        expect(v, isNot(base));
      }
    });
  });

  group('envelope keys', () {
    test('the envelope carries exactly its keys', () {
      final wire = _wire(_sealed());
      expect(wire.keys.toSet(), _envelopeKeys);
      expect(wire['batch_format_version'], '3');
      expect((wire['channel']! as Map).keys.toSet(), _channelKeys);
      expect(wire['attributes'], <String, Object?>{});
      expect(wire['delivery_hash'], _goldenHash);
      expect(DeliveryEnvelope.wireFormat, 'esd/batch@3');
    });

    test('delivery 1 carries a null link under its key', () {
      final envelope = DeliveryEnvelope.seal(
        batchId: 'b',
        senderHop: 'h',
        senderIdentifier: 'i',
        senderSoftwareVersion: 'v',
        sentAt: DateTime.utc(2026),
        channel: _channel,
        deliveryNumber: 1,
        previousDeliveryHash: null,
        events: <Map<String, Object?>>[_event('e1', 'h1')],
      );
      final wire = _wire(envelope);
      expect(wire.containsKey('previous_delivery_hash'), isTrue);
      expect(wire['previous_delivery_hash'], isNull);
    });

    test('the channel is keyed by its four identifying fields', () {
      expect(_channel.toJson(), <String, Object?>{
        'sender_database_id': 'db-sender',
        'destination_id': 'primary',
        'registration_id': 'reg-1',
        'generation': 1,
      });
      expect(DeliveryChannel.fromJson(_channel.toJson()), _channel);
      expect(
        () => DeliveryChannel.fromJson(<String, Object?>{
          ..._channel.toJson(),
          'extra': 1,
        }),
        throwsFormatException,
      );
      expect(
        () => DeliveryChannel.fromJson(<String, Object?>{
          ..._channel.toJson(),
          'generation': 0,
        }),
        throwsFormatException,
      );
    });

    test('a record before the first delivery is number 0 and a null hash', () {
      expect(DeliveryRecord.none.deliveryNumber, 0);
      expect(DeliveryRecord.none.deliveryHash, isNull);
      expect(DeliveryRecord.none.toJson(), <String, Object?>{
        'delivery_number': 0,
        'delivery_hash': null,
      });
      expect(
        DeliveryRecord.fromJson(const <String, Object?>{
          'delivery_number': 4,
          'delivery_hash': 'h',
        }),
        const DeliveryRecord(deliveryNumber: 4, deliveryHash: 'h'),
      );
    });
  });

  group('decode', () {
    test('round-trips a sealed envelope', () {
      final envelope = _sealed();
      final decoded = DeliveryEnvelope.decode(envelope.encode());
      expect(decoded.channel, _channel);
      expect(decoded.deliveryNumber, 2);
      expect(decoded.previousDeliveryHash, 'prevhash');
      expect(decoded.deliveryHash, _goldenHash);
      expect(decoded.recomputedDeliveryHash, _goldenHash);
      expect(decoded.events, envelope.events);
      expect(decoded.encode(), envelope.encode());
    });

    test('an unknown attribute key is kept verbatim and hashed', () {
      final attributes = <String, Object?>{
        'later_fact': <String, Object?>{
          'b': 2,
          'a': <Object?>[1, 'x'],
        },
      };
      final decoded = DeliveryEnvelope.decode(
        _sealed(attributes: attributes).encode(),
      );
      expect(decoded.attributes, attributes);
      expect(decoded.recomputedDeliveryHash, _goldenAttributesHash);
      expect(decoded.deliveryHash, _goldenAttributesHash);
    });

    test('a changed field recomputes to another hash than it carries', () {
      final wire = _wire(_sealed())..['delivery_number'] = 3;
      final decoded = DeliveryEnvelope.decode(_bytes(wire));
      expect(decoded.deliveryHash, _goldenHash);
      expect(decoded.recomputedDeliveryHash, isNot(_goldenHash));
    });

    test('a batch in another format is refused by name', () {
      final v2 = BatchEnvelope(
        batchFormatVersion: '2',
        batchId: 'b',
        senderHop: 'h',
        senderIdentifier: 'i',
        senderSoftwareVersion: 'v',
        sentAt: DateTime.utc(2026),
        events: <Map<String, Object?>>[_event('e1', 'h1')],
      );
      expect(
        () => DeliveryEnvelope.decode(v2.encode()),
        _refusedWith(IngestDecodeFailure.formatUnsupported),
      );
      final wire = _wire(_sealed())..['batch_format_version'] = '4';
      expect(
        () => DeliveryEnvelope.decode(_bytes(wire)),
        _refusedWith(IngestDecodeFailure.formatUnsupported),
      );
    });

    test('attributes that are not an object are refused by name', () {
      for (final value in <Object?>[null, <Object?>[], 'x', 1]) {
        final wire = _wire(_sealed())..['attributes'] = value;
        expect(
          () => DeliveryEnvelope.decode(_bytes(wire)),
          _refusedWith(IngestDecodeFailure.attributesNotObject),
          reason: '$value',
        );
      }
    });

    test('a batch that carries no event is refused by name', () {
      final wire = _wire(_sealed())..['events'] = <Object?>[];
      expect(
        () => DeliveryEnvelope.decode(_bytes(wire)),
        _refusedWith(IngestDecodeFailure.noEvents),
      );
    });

    test('an envelope without exactly its keys is refused as malformed', () {
      for (final key in _envelopeKeys.difference(<String>{
        'batch_format_version',
      })) {
        final wire = _wire(_sealed())..remove(key);
        expect(
          () => DeliveryEnvelope.decode(_bytes(wire)),
          _refusedWith(IngestDecodeFailure.malformed),
          reason: 'without $key',
        );
      }
      final extra = _wire(_sealed())..['parent_withheld'] = false;
      expect(
        () => DeliveryEnvelope.decode(_bytes(extra)),
        _refusedWith(IngestDecodeFailure.malformed),
      );
    });

    test('malformed fields are refused as malformed', () {
      final cases = <String, Object?>{
        'channel': <String, Object?>{'sender_database_id': 'x'},
        'delivery_number': 0,
        'previous_delivery_hash': 7,
        'delivery_hash': null,
        'events': <Object?>['not an object'],
        'sent_at': 'not a time',
      };
      for (final c in cases.entries) {
        final wire = _wire(_sealed())..[c.key] = c.value;
        expect(
          () => DeliveryEnvelope.decode(_bytes(wire)),
          _refusedWith(IngestDecodeFailure.malformed),
          reason: c.key,
        );
      }
      expect(
        () => DeliveryEnvelope.decode(Uint8List.fromList(<int>[0xff])),
        _refusedWith(IngestDecodeFailure.malformed),
      );
    });

    test('sealing refuses a delivery without events', () {
      expect(
        () => _sealed(events: const <Map<String, Object?>>[]),
        throwsArgumentError,
      );
    });
  });
}
