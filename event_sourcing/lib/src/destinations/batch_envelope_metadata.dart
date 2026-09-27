// Implements: EVS-PRD-destinations/E
// BatchEnvelopeMetadata: metadata
// persisted on FIFO rows for library-native destinations (serializesNatively)
// so the drain path can reconstruct wire bytes deterministically at send
// time without requiring the app to supply a transform.
import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/ingest/delivery_envelope.dart';

/// The envelope fields of a queue item of a destination that serializes
/// natively, minus its events. Persisted on the item as
/// `envelope_metadata`, so that the drainer rebuilds the wire bytes
/// deterministically from them and the events the item names.
///
/// An item of a delivery channel (`esd/batch@3`) carries its [channel] and
/// its delivery [attributes]; the drainer assigns the delivery number, the
/// link and the delivery hash when it sends it.
///
/// The fields are immutable once set — they are part of the FIFO row's
/// identity for retry determinism, and a resend of a delivery copies them
/// unchanged.
class BatchEnvelopeMetadata {
  const BatchEnvelopeMetadata({
    required this.batchFormatVersion,
    required this.batchId,
    required this.senderHop,
    required this.senderIdentifier,
    required this.senderSoftwareVersion,
    required this.sentAt,
    required this.channel,
    required this.attributes,
  });

  factory BatchEnvelopeMetadata.fromMap(Map<String, Object?> m) {
    return BatchEnvelopeMetadata(
      batchFormatVersion: m['batch_format_version']! as String,
      batchId: m['batch_id']! as String,
      senderHop: m['sender_hop']! as String,
      senderIdentifier: m['sender_identifier']! as String,
      senderSoftwareVersion: m['sender_software_version']! as String,
      sentAt: DateTime.parse(m['sent_at']! as String),
      channel: DeliveryChannel.fromJson(
        Map<String, Object?>.from(m['channel']! as Map),
      ),
      attributes: Map<String, Object?>.from(m['attributes']! as Map),
    );
  }

  final String batchFormatVersion;
  final String batchId;
  final String senderHop;
  final String senderIdentifier;
  final String senderSoftwareVersion;
  final DateTime sentAt;

  /// The delivery channel the item is delivered on.
  final DeliveryChannel channel;

  /// The delivery attributes the item's delivery carries, as the delivery
  /// hash covers them.
  final Map<String, Object?> attributes;

  /// The wire format of the item: the native delivery format.
  String get wireFormat => DeliveryEnvelope.wireFormat;

  Map<String, Object?> toMap() => <String, Object?>{
    'batch_format_version': batchFormatVersion,
    'batch_id': batchId,
    'sender_hop': senderHop,
    'sender_identifier': senderIdentifier,
    'sender_software_version': senderSoftwareVersion,
    'sent_at': sentAt.toUtc().toIso8601String(),
    'channel': channel.toJson(),
    'attributes': attributes,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is BatchEnvelopeMetadata &&
          batchFormatVersion == other.batchFormatVersion &&
          batchId == other.batchId &&
          senderHop == other.senderHop &&
          senderIdentifier == other.senderIdentifier &&
          senderSoftwareVersion == other.senderSoftwareVersion &&
          sentAt == other.sentAt &&
          channel == other.channel &&
          const DeepCollectionEquality().equals(attributes, other.attributes);

  @override
  int get hashCode => Object.hash(
    batchFormatVersion,
    batchId,
    senderHop,
    senderIdentifier,
    senderSoftwareVersion,
    sentAt,
    channel,
    const DeepCollectionEquality().hash(attributes),
  );

  @override
  String toString() =>
      'BatchEnvelopeMetadata(batchId: $batchId, '
      'senderHop: $senderHop, sentAt: $sentAt, channel: $channel, '
      'attributes: $attributes)';
}
