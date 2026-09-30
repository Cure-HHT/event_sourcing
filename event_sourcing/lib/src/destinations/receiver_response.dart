// Implements: EVS-PRD-portability/C
// pure Dart codecs; identical bytes on every Dart runtime.
import 'dart:convert';
import 'dart:typed_data';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:collection/collection.dart';
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:meta/meta.dart' show immutable;

const DeepCollectionEquality _deep = DeepCollectionEquality();

/// How a receiver acknowledged a delivery.
enum AcknowledgementOutcome {
  /// The receiver accepted the delivery.
  accepted('accepted'),

  /// The delivery re-presented the last one the receiver accepted on its
  /// channel; the receiver acknowledged it and wrote nothing.
  represented('represented');

  const AcknowledgementOutcome(this.wire);

  /// The value carried in the `outcome` key.
  final String wire;
}

/// Why a receiver refused a delivery.
enum RefusalKind {
  /// The delivery does not follow the receiver's record of the channel.
  outOfSequence('out_of_sequence'),

  /// The delivery's hash does not recompute from its fields.
  deliveryHashMismatch('delivery_hash_mismatch'),

  /// Every refusal ingest names by another reason: an undecodable batch,
  /// the application's validation, an unsupported data-format major.
  rejected('rejected');

  const RefusalKind(this.wire);

  /// The value carried in the `refusal` key.
  final String wire;
}

/// A receiver's answer to a delivery: an acknowledgement or a refusal, each
/// carrying the channel, the answering receiver's database identity and
/// the receiver's record of the channel.
@immutable
sealed class ReceiverResponse {
  const ReceiverResponse({
    required this.channel,
    required this.receiverDatabaseId,
    required this.record,
  });

  /// Decodes an acknowledgement or refusal body. Throws [FormatException]
  /// for a body that is not exactly one of the two shapes.
  factory ReceiverResponse.decode(Uint8List body) {
    final Object? json;
    try {
      json = jsonDecode(utf8.decode(body));
    } on FormatException catch (e) {
      throw FormatException('receiver response: ${e.message}');
    }
    if (json is Map<String, Object?> && json.containsKey('outcome')) {
      return ReceiverAcknowledgement._fromJson(json);
    }
    return ReceiverRefusal._fromJson(json);
  }

  /// The channel the answered delivery was sent on.
  final DeliveryChannel channel;

  /// The identity of the database that answered.
  final String receiverDatabaseId;

  /// The receiver's record of the channel after the answer.
  final DeliveryRecord record;

  /// The body's JSON object.
  Map<String, Object?> toJson();

  /// JCS-canonicalizes the body into bytes.
  Uint8List encode() => Uint8List.fromList(canonicalizeBytes(toJson()));
}

/// A receiver's acknowledgement of an accepted or re-presented delivery.
/// Encoded with exactly `channel`, `receiver_database_id`, `record` and
/// `outcome`.
// Implements: EVS-DEV-delivery-receiver/L
// the acknowledgement carries exactly channel, receiver_database_id, record
//   (exactly delivery_number and delivery_hash) and outcome (accepted or
//   represented).
final class ReceiverAcknowledgement extends ReceiverResponse {
  const ReceiverAcknowledgement({
    required super.channel,
    required super.receiverDatabaseId,
    required super.record,
    required this.outcome,
  });

  factory ReceiverAcknowledgement._fromJson(Map<String, Object?> json) {
    final map = requireExactObject(json, 'acknowledgement', const <String>{
      'channel',
      'receiver_database_id',
      'record',
      'outcome',
    });
    final outcome = AcknowledgementOutcome.values.firstWhere(
      (o) => o.wire == map['outcome'],
      orElse: () => throw FormatException(
        'acknowledgement: unknown outcome ${map['outcome']}',
      ),
    );
    return ReceiverAcknowledgement(
      channel: DeliveryChannel.fromJson(map['channel']),
      receiverDatabaseId: requireString(
        map,
        'receiver_database_id',
        'acknowledgement',
      ),
      record: DeliveryRecord.fromJson(map['record']),
      outcome: outcome,
    );
  }

  /// Whether the delivery was accepted or re-presented.
  final AcknowledgementOutcome outcome;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'channel': channel.toJson(),
    'receiver_database_id': receiverDatabaseId,
    'record': record.toJson(),
    'outcome': outcome.wire,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ReceiverAcknowledgement &&
          channel == other.channel &&
          receiverDatabaseId == other.receiverDatabaseId &&
          record == other.record &&
          outcome == other.outcome;

  @override
  int get hashCode => Object.hash(channel, receiverDatabaseId, record, outcome);

  @override
  String toString() =>
      'ReceiverAcknowledgement(${outcome.wire}, $channel, '
      '$receiverDatabaseId, $record)';
}

/// A receiver's refusal of a delivery. Encoded with exactly `channel`,
/// `receiver_database_id`, `record`, `refusal`, `reason` and
/// `refused_event_id`; [reason] and [refusedEventId] are null unless the
/// refusal is [RefusalKind.rejected], which always names its reason.
// Implements: EVS-DEV-delivery-receiver/M
// the refusal carries exactly channel, receiver_database_id, record,
//   refusal (out_of_sequence, delivery_hash_mismatch or rejected), reason
//   and refused_event_id, the last two null unless the refusal is rejected.
final class ReceiverRefusal extends ReceiverResponse {
  ReceiverRefusal({
    required super.channel,
    required super.receiverDatabaseId,
    required super.record,
    required this.refusal,
    this.reason,
    this.refusedEventId,
  }) {
    if (refusal == RefusalKind.rejected) {
      if (reason == null) {
        throw ArgumentError.value(
          reason,
          'reason',
          'a rejected refusal names its reason',
        );
      }
    } else if (reason != null || refusedEventId != null) {
      throw ArgumentError.value(
        refusal.wire,
        'refusal',
        'only a rejected refusal carries a reason or a refused event',
      );
    }
  }

  factory ReceiverRefusal._fromJson(Object? json) {
    final map = requireExactObject(json, 'refusal', const <String>{
      'channel',
      'receiver_database_id',
      'record',
      'refusal',
      'reason',
      'refused_event_id',
    });
    final refusal = RefusalKind.values.firstWhere(
      (k) => k.wire == map['refusal'],
      orElse: () =>
          throw FormatException('refusal: unknown refusal ${map['refusal']}'),
    );
    final reason = map['reason'];
    final refusedEventId = map['refused_event_id'];
    if ((reason != null && reason is! String) ||
        (refusedEventId != null && refusedEventId is! String)) {
      throw const FormatException(
        'refusal: "reason" and "refused_event_id" are strings or null',
      );
    }
    if (refusal == RefusalKind.rejected
        ? reason == null
        : (reason != null || refusedEventId != null)) {
      throw const FormatException(
        'refusal: only a rejected refusal carries a reason or a refused '
        'event, and it always carries a reason',
      );
    }
    return ReceiverRefusal(
      channel: DeliveryChannel.fromJson(map['channel']),
      receiverDatabaseId: requireString(map, 'receiver_database_id', 'refusal'),
      record: DeliveryRecord.fromJson(map['record']),
      refusal: refusal,
      reason: reason as String?,
      refusedEventId: refusedEventId as String?,
    );
  }

  /// Why the receiver refused the delivery.
  final RefusalKind refusal;

  /// For a rejected refusal, the reason ingest named; otherwise null.
  final String? reason;

  /// For a rejected refusal, the event the refusal concerns, or null when
  /// it concerns the whole delivery; otherwise null.
  final String? refusedEventId;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'channel': channel.toJson(),
    'receiver_database_id': receiverDatabaseId,
    'record': record.toJson(),
    'refusal': refusal.wire,
    'reason': reason,
    'refused_event_id': refusedEventId,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ReceiverRefusal &&
          channel == other.channel &&
          receiverDatabaseId == other.receiverDatabaseId &&
          record == other.record &&
          refusal == other.refusal &&
          reason == other.reason &&
          refusedEventId == other.refusedEventId;

  @override
  int get hashCode => Object.hash(
    channel,
    receiverDatabaseId,
    record,
    refusal,
    reason,
    refusedEventId,
  );

  @override
  String toString() =>
      'ReceiverRefusal(${refusal.wire}, $channel, $receiverDatabaseId, '
      '$record, reason: $reason, refusedEventId: $refusedEventId)';
}

/// Maps the body of a receiver's answer to a native delivery to the send
/// outcome it states. A destination that serializes natively passes every
/// body its transport reports as the receiver's answer through this
/// decoder; how a transport failure (no answer) maps to a [SendResult] is
/// the destination's judgement.
///
/// - An acknowledgement, and an `out_of_sequence` refusal, map to
///   [SendAnswered] carrying the response: the receiver's record, which the
///   drainer reads.
/// - A `delivery_hash_mismatch` refusal maps to [SendTransient]: the same
///   delivery is sent again, replacing a copy damaged in transit.
/// - A `rejected` refusal maps to [SendPermanent] naming its reason.
/// - A body that is not a receiver response maps to [SendOk]: an answer
///   carrying no receiver record, on which the drainer wedges the head with
///   `acknowledgement_invalid`.
// Implements: EVS-DEV-delivery-channel/L
// the library's decoder of an acknowledgement or refusal body to the send
//   outcome it states.
// Implements: EVS-DEV-delivery-channel/S
// a delivery_hash_mismatch refusal maps to a transient failure.
SendResult decodeReceiverAnswer(Uint8List body) {
  final ReceiverResponse response;
  try {
    response = ReceiverResponse.decode(body);
  } on FormatException {
    return const SendOk();
  }
  return switch (response) {
    ReceiverAcknowledgement() => SendAnswered(response),
    ReceiverRefusal(refusal: RefusalKind.outOfSequence) => SendAnswered(
      response,
    ),
    ReceiverRefusal(refusal: RefusalKind.deliveryHashMismatch) =>
      const SendTransient(error: 'receiver refused: delivery_hash_mismatch'),
    ReceiverRefusal(
      refusal: RefusalKind.rejected,
      :final reason,
      :final refusedEventId,
    ) =>
      SendPermanent(
        error:
            'receiver rejected the delivery: $reason'
            '${refusedEventId == null ? '' : ' (event $refusedEventId)'}',
      ),
  };
}

// ---------------------------------------------------------------------------
// Pull
// ---------------------------------------------------------------------------

/// A request to a receiver endpoint's pull: the channels of a sender
/// database ([ChannelListingPull]) or a range of a channel's deliveries
/// ([DeliveryRangePull]).
@immutable
sealed class PullRequest {
  const PullRequest();

  /// Decodes a pull request. Throws [FormatException] for one that is not
  /// exactly one of the two shapes.
  factory PullRequest.fromJson(Object? json) {
    if (json is Map<String, Object?> && json['pull'] == 'channels') {
      final map = requireExactObject(json, 'channel listing pull', const {
        'pull',
        'sender_database_id',
      });
      return ChannelListingPull(
        senderDatabaseId: requireString(
          map,
          'sender_database_id',
          'channel listing pull',
        ),
      );
    }
    if (json is Map<String, Object?> && json['pull'] == 'deliveries') {
      final map = requireExactObject(json, 'delivery range pull', const {
        'pull',
        'channel',
        'from_delivery_number',
        'to_delivery_number',
      });
      final from = requirePositiveInt(
        map,
        'from_delivery_number',
        'delivery range pull',
      );
      final to = requirePositiveInt(
        map,
        'to_delivery_number',
        'delivery range pull',
      );
      if (to < from) {
        throw const FormatException(
          'delivery range pull: to_delivery_number is below '
          'from_delivery_number',
        );
      }
      return DeliveryRangePull(
        channel: DeliveryChannel.fromJson(map['channel']),
        fromDeliveryNumber: from,
        toDeliveryNumber: to,
      );
    }
    throw const FormatException(
      'pull request: "pull" must be "channels" or "deliveries"',
    );
  }

  /// The request's JSON object.
  Map<String, Object?> toJson();
}

/// Asks for every channel the receiver holds of a sender database and of
/// the identities in that sender's succession lineage, each with the
/// receiver's record of it. Encoded with exactly `pull` (`"channels"`) and
/// `sender_database_id`.
final class ChannelListingPull extends PullRequest {
  const ChannelListingPull({required this.senderDatabaseId});

  final String senderDatabaseId;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'pull': 'channels',
    'sender_database_id': senderDatabaseId,
  };

  @override
  bool operator ==(Object other) =>
      other is ChannelListingPull && other.senderDatabaseId == senderDatabaseId;

  @override
  int get hashCode => Object.hash(ChannelListingPull, senderDatabaseId);

  @override
  String toString() => 'ChannelListingPull($senderDatabaseId)';
}

/// Asks for the deliveries numbered [fromDeliveryNumber] to
/// [toDeliveryNumber] of [channel]. Encoded with exactly `pull`
/// (`"deliveries"`), `channel`, `from_delivery_number` and
/// `to_delivery_number`.
final class DeliveryRangePull extends PullRequest {
  const DeliveryRangePull({
    required this.channel,
    required this.fromDeliveryNumber,
    required this.toDeliveryNumber,
  });

  final DeliveryChannel channel;
  final int fromDeliveryNumber;
  final int toDeliveryNumber;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'pull': 'deliveries',
    'channel': channel.toJson(),
    'from_delivery_number': fromDeliveryNumber,
    'to_delivery_number': toDeliveryNumber,
  };

  @override
  bool operator ==(Object other) =>
      other is DeliveryRangePull &&
      other.channel == channel &&
      other.fromDeliveryNumber == fromDeliveryNumber &&
      other.toDeliveryNumber == toDeliveryNumber;

  @override
  int get hashCode => Object.hash(
    DeliveryRangePull,
    channel,
    fromDeliveryNumber,
    toDeliveryNumber,
  );

  @override
  String toString() =>
      'DeliveryRangePull($channel, $fromDeliveryNumber..$toDeliveryNumber)';
}

/// What a receiver endpoint's pull served: a [ChannelListing] or a
/// [DeliveryRange].
@immutable
sealed class PullResponse {
  const PullResponse({required this.receiverDatabaseId});

  /// The identity of the database that served the pull.
  final String receiverDatabaseId;

  /// The body's JSON object.
  Map<String, Object?> toJson();

  /// JCS-canonicalizes the body into bytes.
  Uint8List encode() => Uint8List.fromList(canonicalizeBytes(toJson()));
}

/// One channel a receiver lists, with its record of it. Encoded with
/// exactly `channel` and `record`.
@immutable
final class ListedChannel {
  const ListedChannel({required this.channel, required this.record});

  factory ListedChannel._fromJson(Object? json) {
    final map = requireExactObject(json, 'listed channel', const {
      'channel',
      'record',
    });
    return ListedChannel(
      channel: DeliveryChannel.fromJson(map['channel']),
      record: DeliveryRecord.fromJson(map['record']),
    );
  }

  final DeliveryChannel channel;
  final DeliveryRecord record;

  Map<String, Object?> toJson() => <String, Object?>{
    'channel': channel.toJson(),
    'record': record.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      other is ListedChannel &&
      other.channel == channel &&
      other.record == record;

  @override
  int get hashCode => Object.hash(channel, record);

  @override
  String toString() => 'ListedChannel($channel, $record)';
}

/// The channels a receiver holds of a sender database and its succession
/// lineage. Encoded with exactly `receiver_database_id`,
/// `sender_database_id` and `channels`.
final class ChannelListing extends PullResponse {
  const ChannelListing({
    required super.receiverDatabaseId,
    required this.senderDatabaseId,
    required this.channels,
  });

  factory ChannelListing._fromJson(Map<String, Object?> json) {
    final map = requireExactObject(json, 'channel listing', const {
      'receiver_database_id',
      'sender_database_id',
      'channels',
    });
    final channels = map['channels'];
    if (channels is! List) {
      throw const FormatException('channel listing: "channels" is a list');
    }
    return ChannelListing(
      receiverDatabaseId: requireString(
        map,
        'receiver_database_id',
        'channel listing',
      ),
      senderDatabaseId: requireString(
        map,
        'sender_database_id',
        'channel listing',
      ),
      channels: <ListedChannel>[
        for (final c in channels) ListedChannel._fromJson(c),
      ],
    );
  }

  /// The sender database the listing was asked for.
  final String senderDatabaseId;

  final List<ListedChannel> channels;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'receiver_database_id': receiverDatabaseId,
    'sender_database_id': senderDatabaseId,
    'channels': <Object?>[for (final c in channels) c.toJson()],
  };

  @override
  bool operator ==(Object other) =>
      other is ChannelListing &&
      other.receiverDatabaseId == receiverDatabaseId &&
      other.senderDatabaseId == senderDatabaseId &&
      const ListEquality<ListedChannel>().equals(other.channels, channels);

  @override
  int get hashCode => Object.hash(
    receiverDatabaseId,
    senderDatabaseId,
    const ListEquality<ListedChannel>().hash(channels),
  );

  @override
  String toString() =>
      'ChannelListing($receiverDatabaseId, $senderDatabaseId, $channels)';
}

/// One delivery a pull served: its number, link, hash, attributes and the
/// stored record of each event its accepted-delivery audit names. Encoded
/// with exactly `delivery_number`, `previous_delivery_hash`,
/// `delivery_hash`, `attributes` and `events`.
@immutable
final class ServedDelivery {
  const ServedDelivery({
    required this.deliveryNumber,
    required this.previousDeliveryHash,
    required this.deliveryHash,
    required this.attributes,
    required this.events,
  });

  factory ServedDelivery._fromJson(Object? json) {
    final map = requireExactObject(json, 'served delivery', const {
      'delivery_number',
      'previous_delivery_hash',
      'delivery_hash',
      'attributes',
      'events',
    });
    final link = map['previous_delivery_hash'];
    final attributes = map['attributes'];
    final events = map['events'];
    if ((link != null && link is! String) ||
        attributes is! Map<String, Object?> ||
        events is! List ||
        events.any((e) => e is! Map<String, Object?>)) {
      throw const FormatException('served delivery: malformed field');
    }
    return ServedDelivery(
      deliveryNumber: requirePositiveInt(
        map,
        'delivery_number',
        'served delivery',
      ),
      previousDeliveryHash: link as String?,
      deliveryHash: requireString(map, 'delivery_hash', 'served delivery'),
      attributes: attributes,
      events: <Map<String, Object?>>[
        for (final e in events) e as Map<String, Object?>,
      ],
    );
  }

  final int deliveryNumber;
  final String? previousDeliveryHash;
  final String deliveryHash;
  final Map<String, Object?> attributes;
  final List<Map<String, Object?>> events;

  Map<String, Object?> toJson() => <String, Object?>{
    'delivery_number': deliveryNumber,
    'previous_delivery_hash': previousDeliveryHash,
    'delivery_hash': deliveryHash,
    'attributes': attributes,
    'events': events,
  };

  @override
  bool operator ==(Object other) =>
      other is ServedDelivery && _deep.equals(other.toJson(), toJson());

  @override
  int get hashCode => _deep.hash(toJson());

  @override
  String toString() => 'ServedDelivery($deliveryNumber, $deliveryHash)';
}

/// A range of a channel's deliveries a pull served, with the receiver's
/// record of the channel and, when the receiver cannot serve a delivery the
/// range asked for, the number of the first such delivery. Encoded with
/// exactly `receiver_database_id`, `channel`, `record`, `deliveries` and
/// `unservable_delivery_number`.
final class DeliveryRange extends PullResponse {
  const DeliveryRange({
    required super.receiverDatabaseId,
    required this.channel,
    required this.record,
    required this.deliveries,
    this.unservableDeliveryNumber,
  });

  factory DeliveryRange._fromJson(Map<String, Object?> json) {
    final map = requireExactObject(json, 'delivery range', const {
      'receiver_database_id',
      'channel',
      'record',
      'deliveries',
      'unservable_delivery_number',
    });
    final deliveries = map['deliveries'];
    if (deliveries is! List) {
      throw const FormatException('delivery range: "deliveries" is a list');
    }
    final unservable = map['unservable_delivery_number'];
    if (unservable != null) {
      requirePositiveInt(map, 'unservable_delivery_number', 'delivery range');
    }
    return DeliveryRange(
      receiverDatabaseId: requireString(
        map,
        'receiver_database_id',
        'delivery range',
      ),
      channel: DeliveryChannel.fromJson(map['channel']),
      record: DeliveryRecord.fromJson(map['record']),
      deliveries: <ServedDelivery>[
        for (final d in deliveries) ServedDelivery._fromJson(d),
      ],
      unservableDeliveryNumber: unservable as int?,
    );
  }

  final DeliveryChannel channel;

  /// The receiver's record of [channel].
  final DeliveryRecord record;

  /// The served deliveries, in ascending order of number.
  final List<ServedDelivery> deliveries;

  /// The first delivery of the range the receiver cannot serve, or null
  /// when it served the whole range.
  final int? unservableDeliveryNumber;

  @override
  Map<String, Object?> toJson() => <String, Object?>{
    'receiver_database_id': receiverDatabaseId,
    'channel': channel.toJson(),
    'record': record.toJson(),
    'deliveries': <Object?>[for (final d in deliveries) d.toJson()],
    'unservable_delivery_number': unservableDeliveryNumber,
  };

  @override
  bool operator ==(Object other) =>
      other is DeliveryRange && _deep.equals(other.toJson(), toJson());

  @override
  int get hashCode => _deep.hash(toJson());

  @override
  String toString() =>
      'DeliveryRange($receiverDatabaseId, $channel, $record, '
      '${deliveries.length} deliveries, unservable: '
      '$unservableDeliveryNumber)';
}

/// Why a receiver endpoint refused a pull.
enum PullRefusalKind {
  /// The receiver cannot serve the pull now; it is worth repeating.
  unavailable('unavailable'),

  /// The receiver will not serve the pull as asked.
  rejected('rejected');

  const PullRefusalKind(this.wire);

  final String wire;
}

/// A receiver endpoint's refusal of a pull. Encoded with exactly
/// `receiver_database_id`, `pull_refusal` and `reason`.
@immutable
final class PullRefusal {
  const PullRefusal({
    required this.receiverDatabaseId,
    required this.refusal,
    required this.reason,
  });

  factory PullRefusal._fromJson(Map<String, Object?> json) {
    final map = requireExactObject(json, 'pull refusal', const {
      'receiver_database_id',
      'pull_refusal',
      'reason',
    });
    return PullRefusal(
      receiverDatabaseId: requireString(
        map,
        'receiver_database_id',
        'pull refusal',
      ),
      refusal: PullRefusalKind.values.firstWhere(
        (k) => k.wire == map['pull_refusal'],
        orElse: () => throw FormatException(
          'pull refusal: unknown refusal ${map['pull_refusal']}',
        ),
      ),
      reason: requireString(map, 'reason', 'pull refusal'),
    );
  }

  final String receiverDatabaseId;
  final PullRefusalKind refusal;
  final String reason;

  Map<String, Object?> toJson() => <String, Object?>{
    'receiver_database_id': receiverDatabaseId,
    'pull_refusal': refusal.wire,
    'reason': reason,
  };

  /// JCS-canonicalizes the body into bytes.
  Uint8List encode() => Uint8List.fromList(canonicalizeBytes(toJson()));
}

/// The outcome of a destination's pull: [PullServed], [PullTransient] or
/// [PullPermanent].
@immutable
sealed class PullOutcome {
  const PullOutcome();
}

/// The receiver served the pull.
final class PullServed extends PullOutcome {
  const PullServed(this.response);

  final PullResponse response;

  @override
  bool operator ==(Object other) =>
      other is PullServed && other.response == response;

  @override
  int get hashCode => Object.hash(PullServed, response);

  @override
  String toString() => 'PullServed($response)';
}

/// The pull failed and is worth repeating.
final class PullTransient extends PullOutcome {
  const PullTransient({required this.error});

  final String error;

  @override
  bool operator ==(Object other) =>
      other is PullTransient && other.error == error;

  @override
  int get hashCode => Object.hash(PullTransient, error);

  @override
  String toString() => 'PullTransient($error)';
}

/// The pull failed and repeating it would not change that.
final class PullPermanent extends PullOutcome {
  const PullPermanent({required this.error});

  final String error;

  @override
  bool operator ==(Object other) =>
      other is PullPermanent && other.error == error;

  @override
  int get hashCode => Object.hash(PullPermanent, error);

  @override
  String toString() => 'PullPermanent($error)';
}

/// A destination's pull operation: sends [request] to the receiver
/// endpoint and reports what it answered through [decodePullResponse], or
/// a transport failure as [PullTransient] or [PullPermanent].
typedef ChannelPull = Future<PullOutcome> Function(PullRequest request);

/// Maps the body of a receiver endpoint's answer to a pull to its outcome:
/// a channel listing or a delivery range is [PullServed]; a pull refusal is
/// [PullTransient] when the receiver is unavailable and [PullPermanent]
/// when it rejected the pull; a body that is none of these is
/// [PullPermanent].
// Implements: EVS-DEV-delivery-channel/R
// the library's decoder of a pull response to served, a transient failure
//   or a permanent failure.
PullOutcome decodePullResponse(Uint8List body) {
  try {
    final json = jsonDecode(utf8.decode(body));
    if (json is Map<String, Object?>) {
      if (json.containsKey('pull_refusal')) {
        final refusal = PullRefusal._fromJson(json);
        final error =
            'receiver refused the pull (${refusal.refusal.wire}): '
            '${refusal.reason}';
        return switch (refusal.refusal) {
          PullRefusalKind.unavailable => PullTransient(error: error),
          PullRefusalKind.rejected => PullPermanent(error: error),
        };
      }
      if (json.containsKey('channels')) {
        return PullServed(ChannelListing._fromJson(json));
      }
      if (json.containsKey('deliveries')) {
        return PullServed(DeliveryRange._fromJson(json));
      }
    }
    return const PullPermanent(error: 'pull response is not a known body');
  } on FormatException catch (e) {
    return PullPermanent(error: 'pull response does not decode: ${e.message}');
  }
}
