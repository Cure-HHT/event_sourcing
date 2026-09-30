// Implements: EVS-DEV-delivery-channel/K
// the native batch envelope carries exactly batch_format_version ("3"),
//   batch_id, sender_hop, sender_identifier, sender_software_version,
//   sent_at, channel, delivery_number, previous_delivery_hash,
//   delivery_hash, events (at least one) and attributes (an object).
// Implements: EVS-DEV-delivery-receiver/A
// the decoder refuses, by name and before anything else reads the batch,
//   a batch that is not in the native batch format, one whose attributes is
//   not an object and one that carries no event.
// Implements: EVS-PRD-hash-chain-integrity/D
// the envelope is encoded in JCS canonical form, so every observer
//   reproduces its bytes.
import 'dart:convert';
import 'dart:typed_data';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/ingest/ingest_errors.dart';

/// One delivery on a delivery channel, in the library's native batch
/// format (`esd/batch@3`).
///
/// The envelope names its channel, its delivery number, its link (the
/// delivery hash of the delivery it follows; null for delivery 1) and its
/// own delivery hash ([computeDeliveryHash]), and carries its events as
/// raw stored-event JSON and an [attributes] object. The library sends
/// [attributes] empty; a later release of the data-format major may add
/// per-delivery facts to it, which the hash covers and every receiver
/// keeps as carried.
final class DeliveryEnvelope {
  const DeliveryEnvelope({
    required this.batchId,
    required this.senderHop,
    required this.senderIdentifier,
    required this.senderSoftwareVersion,
    required this.sentAt,
    required this.channel,
    required this.deliveryNumber,
    required this.previousDeliveryHash,
    required this.deliveryHash,
    required this.events,
    required this.attributes,
  });

  /// Builds a delivery whose [deliveryHash] is computed from its channel,
  /// number, link, the `event_hash` of each event and its attributes.
  /// Throws [ArgumentError] for a delivery without events or with a number
  /// below 1.
  factory DeliveryEnvelope.seal({
    required String batchId,
    required String senderHop,
    required String senderIdentifier,
    required String senderSoftwareVersion,
    required DateTime sentAt,
    required DeliveryChannel channel,
    required int deliveryNumber,
    required String? previousDeliveryHash,
    required List<Map<String, Object?>> events,
    Map<String, Object?> attributes = const <String, Object?>{},
  }) {
    if (events.isEmpty) {
      throw ArgumentError.value(events, 'events', 'a delivery carries events');
    }
    if (deliveryNumber < 1) {
      throw ArgumentError.value(
        deliveryNumber,
        'deliveryNumber',
        'delivery numbers count from 1',
      );
    }
    return DeliveryEnvelope(
      batchId: batchId,
      senderHop: senderHop,
      senderIdentifier: senderIdentifier,
      senderSoftwareVersion: senderSoftwareVersion,
      sentAt: sentAt,
      channel: channel,
      deliveryNumber: deliveryNumber,
      previousDeliveryHash: previousDeliveryHash,
      deliveryHash: computeDeliveryHash(
        channel: channel,
        deliveryNumber: deliveryNumber,
        previousDeliveryHash: previousDeliveryHash,
        eventHashes: _eventHashesOf(events),
        attributes: attributes,
      ),
      events: events,
      attributes: attributes,
    );
  }

  /// Parses wire bytes as a native delivery. Throws [IngestDecodeFailure]
  /// naming the refusal: [IngestDecodeFailure.formatUnsupported] for a
  /// batch whose `batch_format_version` is not [batchFormatVersion],
  /// [IngestDecodeFailure.attributesNotObject] for an `attributes` that is
  /// not an object, [IngestDecodeFailure.noEvents] for an empty `events`,
  /// and [IngestDecodeFailure.malformed] for anything else that does not
  /// decode, a missing or an extra key included.
  ///
  /// The decoder does not check [deliveryHash]; a receiver compares it with
  /// [recomputedDeliveryHash].
  factory DeliveryEnvelope.decode(Uint8List bytes) {
    final Object? decoded;
    try {
      decoded = jsonDecode(utf8.decode(bytes));
    } on FormatException catch (e) {
      throw IngestDecodeFailure('not valid UTF-8 JSON: ${e.message}');
    }
    if (decoded is! Map<String, Object?>) {
      throw const IngestDecodeFailure('envelope must be a JSON object');
    }
    final version = decoded['batch_format_version'];
    if (version != batchFormatVersion) {
      throw IngestDecodeFailure(
        'unsupported batch_format_version: got ${version ?? "(missing)"}; '
        'expected "$batchFormatVersion"',
        reason: IngestDecodeFailure.formatUnsupported,
      );
    }
    final keys = decoded.keys.toSet();
    if (keys.length != _keys.length || !keys.containsAll(_keys)) {
      throw IngestDecodeFailure(
        'envelope must carry exactly ${(_keys.toList()..sort()).join(', ')}; '
        'got ${(keys.toList()..sort()).join(', ')}',
      );
    }
    final attributes = decoded['attributes'];
    if (attributes is! Map<String, Object?>) {
      throw const IngestDecodeFailure(
        'attributes must be a JSON object',
        reason: IngestDecodeFailure.attributesNotObject,
      );
    }
    final eventsRaw = decoded['events'];
    if (eventsRaw is! List) {
      throw const IngestDecodeFailure('events must be a JSON array');
    }
    if (eventsRaw.isEmpty) {
      throw const IngestDecodeFailure(
        'a delivery carries at least one event',
        reason: IngestDecodeFailure.noEvents,
      );
    }
    final events = <Map<String, Object?>>[];
    for (var i = 0; i < eventsRaw.length; i++) {
      final e = eventsRaw[i];
      if (e is! Map<String, Object?>) {
        throw IngestDecodeFailure('events[$i] must be a JSON object');
      }
      events.add(e);
    }
    final DeliveryChannel channel;
    try {
      channel = DeliveryChannel.fromJson(decoded['channel']);
    } on FormatException catch (e) {
      throw IngestDecodeFailure(e.message);
    }
    final number = decoded['delivery_number'];
    if (number is! int || number < 1) {
      throw const IngestDecodeFailure(
        'delivery_number must be an integer of at least 1',
      );
    }
    final link = decoded['previous_delivery_hash'];
    if (link != null && link is! String) {
      throw const IngestDecodeFailure(
        'previous_delivery_hash must be a string or null',
      );
    }
    final deliveryHash = decoded['delivery_hash'];
    final batchId = decoded['batch_id'];
    final senderHop = decoded['sender_hop'];
    final senderIdentifier = decoded['sender_identifier'];
    final senderSoftwareVersion = decoded['sender_software_version'];
    final sentAtRaw = decoded['sent_at'];
    for (final (key, value) in <(String, Object?)>[
      ('delivery_hash', deliveryHash),
      ('batch_id', batchId),
      ('sender_hop', senderHop),
      ('sender_identifier', senderIdentifier),
      ('sender_software_version', senderSoftwareVersion),
      ('sent_at', sentAtRaw),
    ]) {
      if (value is! String) {
        throw IngestDecodeFailure('missing or non-string "$key"');
      }
    }
    final sentAt = DateTime.tryParse(sentAtRaw! as String);
    if (sentAt == null) {
      throw const IngestDecodeFailure('sent_at is not an ISO 8601 date-time');
    }
    return DeliveryEnvelope(
      batchId: batchId! as String,
      senderHop: senderHop! as String,
      senderIdentifier: senderIdentifier! as String,
      senderSoftwareVersion: senderSoftwareVersion! as String,
      sentAt: sentAt,
      channel: channel,
      deliveryNumber: number,
      previousDeliveryHash: link as String?,
      deliveryHash: deliveryHash! as String,
      events: events,
      attributes: attributes,
    );
  }

  /// The identifier of the native batch format.
  static const String wireFormat = 'esd/batch@3';

  /// The `batch_format_version` every native delivery carries.
  static const String batchFormatVersion = '3';

  static const Set<String> _keys = <String>{
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

  final String batchId;
  final String senderHop;
  final String senderIdentifier;
  final String senderSoftwareVersion;
  final DateTime sentAt;

  /// The channel the delivery is sent on.
  final DeliveryChannel channel;

  /// The delivery's number on its channel, counted from 1.
  final int deliveryNumber;

  /// The link: the delivery hash of the delivery this one follows; null for
  /// delivery 1.
  final String? previousDeliveryHash;

  /// The delivery hash the envelope carries.
  final String deliveryHash;

  /// Raw stored-event JSON, in the order the delivery carries it.
  final List<Map<String, Object?>> events;

  /// The delivery's attributes, as carried.
  final Map<String, Object?> attributes;

  /// The `event_hash` of each carried event, as carried and in order.
  List<Object?> get eventHashes => _eventHashesOf(events);

  /// The delivery hash of the envelope's channel, number, link, events and
  /// attributes, as they are carried.
  String get recomputedDeliveryHash => computeDeliveryHash(
    channel: channel,
    deliveryNumber: deliveryNumber,
    previousDeliveryHash: previousDeliveryHash,
    eventHashes: eventHashes,
    attributes: attributes,
  );

  /// The JSON object the envelope is encoded as.
  Map<String, Object?> toJson() => <String, Object?>{
    'batch_format_version': batchFormatVersion,
    'batch_id': batchId,
    'sender_hop': senderHop,
    'sender_identifier': senderIdentifier,
    'sender_software_version': senderSoftwareVersion,
    'sent_at': sentAt.toUtc().toIso8601String(),
    'channel': channel.toJson(),
    'delivery_number': deliveryNumber,
    'previous_delivery_hash': previousDeliveryHash,
    'delivery_hash': deliveryHash,
    'events': events,
    'attributes': attributes,
  };

  /// JCS-canonicalizes the envelope into wire bytes.
  Uint8List encode() => Uint8List.fromList(canonicalizeBytes(toJson()));
}

List<Object?> _eventHashesOf(List<Map<String, Object?>> events) => <Object?>[
  for (final e in events) e['event_hash'],
];
