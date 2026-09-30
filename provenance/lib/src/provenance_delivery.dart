// Implements: EVS-PRD-provenance/A
// (ProvenanceDelivery is the value type recorded in the
//   ProvenanceEntry.delivery field: the channel and the delivery number an
//   ingested event arrived in)
// Implements: EVS-PRD-provenance/C
// (JSON serialization and deserialization of the delivery object without
//   loss of information)

/// The delivery a receiver ingested an event from: the delivery channel
/// (sending database, destination, registration and generation) and the
/// delivery's number on it.
///
/// Stamped into the receiver-hop `ProvenanceEntry.delivery` field of each
/// event a receiver ingests from a native delivery; null on every other
/// entry. Encoded as an object with exactly `channel` (an object with
/// exactly `sender_database_id`, `destination_id`, `registration_id` and
/// `generation`) and `delivery_number`.
class ProvenanceDelivery {
  const ProvenanceDelivery({
    required this.senderDatabaseId,
    required this.destinationId,
    required this.registrationId,
    required this.generation,
    required this.deliveryNumber,
  });

  /// Decodes a delivery object, refusing with a [FormatException] one that
  /// does not carry exactly its keys, a generation or a delivery number
  /// below 1, or a non-string identity.
  factory ProvenanceDelivery.fromJson(Map<String, Object?> json) {
    _requireExactKeys(json, const <String>{'channel', 'delivery_number'});
    final channel = json['channel'];
    if (channel is! Map<String, Object?>) {
      throw const FormatException(
        'ProvenanceDelivery: "channel" must be an object',
      );
    }
    _requireExactKeys(channel, const <String>{
      'sender_database_id',
      'destination_id',
      'registration_id',
      'generation',
    });
    return ProvenanceDelivery(
      senderDatabaseId: _requireString(channel, 'sender_database_id'),
      destinationId: _requireString(channel, 'destination_id'),
      registrationId: _requireString(channel, 'registration_id'),
      generation: _requirePositiveInt(channel, 'generation'),
      deliveryNumber: _requirePositiveInt(json, 'delivery_number'),
    );
  }

  /// The identity of the sending database.
  final String senderDatabaseId;

  /// The destination identifier the delivery was sent through.
  final String destinationId;

  /// The identifier of the destination's registration.
  final String registrationId;

  /// The channel's generation, 1 for the first.
  final int generation;

  /// The delivery's number on its channel, counted from 1.
  final int deliveryNumber;

  Map<String, Object?> toJson() => <String, Object?>{
    'channel': <String, Object?>{
      'sender_database_id': senderDatabaseId,
      'destination_id': destinationId,
      'registration_id': registrationId,
      'generation': generation,
    },
    'delivery_number': deliveryNumber,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ProvenanceDelivery &&
          senderDatabaseId == other.senderDatabaseId &&
          destinationId == other.destinationId &&
          registrationId == other.registrationId &&
          generation == other.generation &&
          deliveryNumber == other.deliveryNumber;

  @override
  int get hashCode => Object.hash(
    senderDatabaseId,
    destinationId,
    registrationId,
    generation,
    deliveryNumber,
  );

  @override
  String toString() =>
      'ProvenanceDelivery(senderDatabaseId: $senderDatabaseId, '
      'destinationId: $destinationId, registrationId: $registrationId, '
      'generation: $generation, deliveryNumber: $deliveryNumber)';
}

void _requireExactKeys(Map<String, Object?> json, Set<String> keys) {
  final actual = json.keys.toSet();
  if (actual.length != keys.length || !actual.containsAll(keys)) {
    throw FormatException(
      'ProvenanceDelivery: expected exactly the keys '
      '${(keys.toList()..sort()).join(', ')}; got '
      '${(actual.toList()..sort()).join(', ')}',
    );
  }
}

String _requireString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw FormatException('ProvenanceDelivery: missing or non-string "$key"');
  }
  return value;
}

int _requirePositiveInt(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! int || value < 1) {
    throw FormatException(
      'ProvenanceDelivery: "$key" must be an integer of at least 1',
    );
  }
  return value;
}
