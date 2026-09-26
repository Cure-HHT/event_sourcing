// Implements: EVS-PRD-delivery-channel/A
// a delivery channel is identified by the sending database's identity, the
//   destination identifier, the registration and the generation.
// Implements: EVS-PRD-delivery-channel/B
// the delivery hash covers the channel, the number, the link, the hash of
//   every event the delivery carries and the delivery's attributes.
// Implements: EVS-DEV-delivery-channel/C
// the delivery hash is the lowercase-hex SHA-256 of the canonical JSON of
//   an object with exactly channel, delivery_number, previous_delivery_hash,
//   event_hashes and attributes.
// Implements: EVS-PRD-portability/C
// pure Dart value types and hashing; identical on every Dart runtime.
import 'dart:convert';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:meta/meta.dart' show immutable, internal;

/// One delivery channel: one generation of one registration of a
/// destination that serializes natively, on one sending database.
///
/// Encoded as an object with exactly `sender_database_id`,
/// `destination_id`, `registration_id` and `generation`.
@immutable
final class DeliveryChannel {
  const DeliveryChannel({
    required this.senderDatabaseId,
    required this.destinationId,
    required this.registrationId,
    required this.generation,
  });

  /// Decodes a channel object, refusing with a [FormatException] one
  /// without exactly its keys, with a non-string identity or with a
  /// generation below 1.
  factory DeliveryChannel.fromJson(Object? json) {
    final map = requireExactObject(json, 'channel', _keys);
    return DeliveryChannel(
      senderDatabaseId: requireString(map, 'sender_database_id', 'channel'),
      destinationId: requireString(map, 'destination_id', 'channel'),
      registrationId: requireString(map, 'registration_id', 'channel'),
      generation: requirePositiveInt(map, 'generation', 'channel'),
    );
  }

  static const Set<String> _keys = <String>{
    'sender_database_id',
    'destination_id',
    'registration_id',
    'generation',
  };

  /// The identity of the sending database.
  final String senderDatabaseId;

  /// The destination identifier.
  final String destinationId;

  /// The registration identifier: the event identifier of the
  /// registration event.
  final String registrationId;

  /// The registration's channel counter, 1 for the first.
  final int generation;

  Map<String, Object?> toJson() => <String, Object?>{
    'sender_database_id': senderDatabaseId,
    'destination_id': destinationId,
    'registration_id': registrationId,
    'generation': generation,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DeliveryChannel &&
          senderDatabaseId == other.senderDatabaseId &&
          destinationId == other.destinationId &&
          registrationId == other.registrationId &&
          generation == other.generation;

  @override
  int get hashCode =>
      Object.hash(senderDatabaseId, destinationId, registrationId, generation);

  @override
  String toString() =>
      'DeliveryChannel($senderDatabaseId, $destinationId, '
      '$registrationId, generation $generation)';
}

/// The number and hash of the last delivery accepted on a channel: a
/// receiver's record of the channel, or the one a sender established.
///
/// Encoded as an object with exactly `delivery_number` and `delivery_hash`.
/// Before the first delivery the number is 0 and the hash null ([none]).
@immutable
final class DeliveryRecord {
  const DeliveryRecord({required this.deliveryNumber, this.deliveryHash});

  /// Decodes a record object, refusing with a [FormatException] one without
  /// exactly its keys, a negative number, or a hash that is null at a
  /// number above 0 or present at number 0.
  factory DeliveryRecord.fromJson(Object? json) {
    final map = requireExactObject(json, 'record', const <String>{
      'delivery_number',
      'delivery_hash',
    });
    final number = map['delivery_number'];
    if (number is! int || number < 0) {
      throw const FormatException(
        'record: "delivery_number" must be a non-negative integer',
      );
    }
    final hash = map['delivery_hash'];
    if (number == 0 ? hash != null : hash is! String) {
      throw const FormatException(
        'record: "delivery_hash" is null exactly when "delivery_number" is 0',
      );
    }
    return DeliveryRecord(
      deliveryNumber: number,
      deliveryHash: hash as String?,
    );
  }

  /// The record before the first delivery.
  static const DeliveryRecord none = DeliveryRecord(deliveryNumber: 0);

  /// The number of the last accepted delivery; 0 before the first.
  final int deliveryNumber;

  /// The delivery hash of the last accepted delivery; null before the
  /// first.
  final String? deliveryHash;

  Map<String, Object?> toJson() => <String, Object?>{
    'delivery_number': deliveryNumber,
    'delivery_hash': deliveryHash,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DeliveryRecord &&
          deliveryNumber == other.deliveryNumber &&
          deliveryHash == other.deliveryHash;

  @override
  int get hashCode => Object.hash(deliveryNumber, deliveryHash);

  @override
  String toString() => 'DeliveryRecord($deliveryNumber, $deliveryHash)';
}

/// The delivery hash of a delivery: the SHA-256, in lowercase hexadecimal,
/// of the canonical JSON (RFC 8785) of an object with exactly `channel`,
/// `delivery_number`, `previous_delivery_hash` ([previousDeliveryHash],
/// null for delivery 1), `event_hashes` (the `event_hash` of each event the
/// delivery carries, in its order, as carried) and `attributes` (as
/// carried, whatever names it holds).
String computeDeliveryHash({
  required DeliveryChannel channel,
  required int deliveryNumber,
  required String? previousDeliveryHash,
  required List<Object?> eventHashes,
  required Map<String, Object?> attributes,
}) => sha256
    .convert(
      utf8.encode(
        canonicalize(<String, Object?>{
          'channel': channel.toJson(),
          'delivery_number': deliveryNumber,
          'previous_delivery_hash': previousDeliveryHash,
          'event_hashes': eventHashes,
          'attributes': attributes,
        }),
      ),
    )
    .toString();

/// Returns [json] as an object carrying exactly [keys], or throws a
/// [FormatException] naming [what].
@internal
Map<String, Object?> requireExactObject(
  Object? json,
  String what,
  Set<String> keys,
) {
  if (json is! Map<String, Object?>) {
    throw FormatException('$what must be a JSON object');
  }
  final actual = json.keys.toSet();
  if (actual.length != keys.length || !actual.containsAll(keys)) {
    throw FormatException(
      '$what must carry exactly ${(keys.toList()..sort()).join(', ')}; got '
      '${(actual.toList()..sort()).join(', ')}',
    );
  }
  return json;
}

/// Returns the string at [key] of [map], or throws a [FormatException]
/// naming [what].
@internal
String requireString(Map<String, Object?> map, String key, String what) {
  final value = map[key];
  if (value is! String) {
    throw FormatException('$what: "$key" must be a string');
  }
  return value;
}

/// Returns the integer of at least 1 at [key] of [map], or throws a
/// [FormatException] naming [what].
@internal
int requirePositiveInt(Map<String, Object?> map, String key, String what) {
  final value = map[key];
  if (value is! int || value < 1) {
    throw FormatException('$what: "$key" must be an integer of at least 1');
  }
  return value;
}
