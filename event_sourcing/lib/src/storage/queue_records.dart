// Implements: EVS-PRD-portability/C
// pure Dart value types; serialise
//   identically on every Dart-supported runtime.
// Implements: EVS-DEV-destination-drain/A+E+F+U
// the records the storage contract
//   returns or keeps beside a destination's queue: the trail sweep's result
//   (lowest event it removed, for the recovery rewind), a queue's retirement
//   on deletion, a pending replay request, and the database-wide registry
//   check record.
// Implements: EVS-DEV-destination-drain/I
// the wedge record the drainer writes
//   beside the wedge event it appends: the wedged item, the event and the
//   cause, for the destination's open wedge, the purpose of the halt
//   request the wedge consumed, the drain epoch of the wedging drainer and
//   the fingerprint of the configuration it declared.
// Implements: EVS-DEV-destination-drain/N
// the halt request the registry writes and
//   clears in the transaction of the event that opens or closes it, and the
//   send fence the drainer writes immediately before each send.
// Implements: EVS-DEV-destination-drain/Y
// the transform failure record the fill
//   keeps for a batch its transform failed on: each failure's time and the
//   batch's sequence range, so a retry evaluates the same batch and the
//   budget it spends is auditable once the batch enqueues as one
//   transform-failed item.
import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/destinations/halt_purpose.dart';
import 'package:event_sourcing/src/destinations/wedge_cause.dart';
import 'package:event_sourcing/src/ingest/delivery_channel.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart' show SequenceRange;

/// What a trail sweep removed from a destination's queue: the number of
/// pending items it deleted and the lowest event sequence number any of them
/// carried (`null` when it deleted none).
class TrailSweepResult {
  const TrailSweepResult({required this.deletedCount, this.minFirstSeq});

  /// Number of pending items deleted.
  final int deletedCount;

  /// Lowest `event_id_range.first_seq` among the deleted items, or `null`
  /// when none was deleted.
  final int? minFirstSeq;

  @override
  bool operator ==(Object other) =>
      other is TrailSweepResult &&
      other.deletedCount == deletedCount &&
      other.minFirstSeq == minFirstSeq;

  @override
  int get hashCode => Object.hash(deletedCount, minFirstSeq);

  @override
  String toString() =>
      'TrailSweepResult(deletedCount: $deletedCount, '
      'minFirstSeq: $minFirstSeq)';
}

/// What retiring a destination's queue on deletion did: the wedged head it
/// tombstoned (`null` for an empty queue) and the number of pending items it
/// deleted behind that head.
class QueueRetirement {
  const QueueRetirement({
    required this.tombstonedRowId,
    required this.deletedPendingCount,
  });

  /// `entry_id` of the wedged head the retirement tombstoned, or `null` when
  /// the queue had no head.
  final String? tombstonedRowId;

  /// Number of pending items deleted.
  final int deletedPendingCount;

  @override
  bool operator ==(Object other) =>
      other is QueueRetirement &&
      other.tombstonedRowId == tombstonedRowId &&
      other.deletedPendingCount == deletedPendingCount;

  @override
  int get hashCode => Object.hash(tombstonedRowId, deletedPendingCount);

  @override
  String toString() =>
      'QueueRetirement(tombstonedRowId: $tombstonedRowId, '
      'deletedPendingCount: $deletedPendingCount)';
}

/// A replay the next fill of a destination performs before it fills.
///
/// A registry operation that widens a destination's window records one; only
/// the drainer's fill enqueues, so the replay runs under the configuration of
/// the destination the drainer registers.
///
/// - [firstActivation]: the destination was activated; the fill replays
///   every event past its fill position, to completion, in one pass.
/// - [gapUpper]: the start date moved earlier; the fill replays the events at
///   or below its fill position whose client timestamp lies in
///   `[startDate, gapUpper)`.
class ReplayRequest {
  const ReplayRequest({this.firstActivation = false, this.gapUpper});

  /// Decode from the persisted JSON form.
  factory ReplayRequest.fromJson(Map<String, Object?> json) {
    final first = json['first_activation'];
    if (first is! bool) {
      throw const FormatException(
        'ReplayRequest: missing or non-bool "first_activation"',
      );
    }
    final gap = json['gap_upper'];
    if (gap != null && gap is! String) {
      throw const FormatException('ReplayRequest: non-string "gap_upper"');
    }
    return ReplayRequest(
      firstActivation: first,
      gapUpper: gap == null ? null : DateTime.parse(gap as String).toUtc(),
    );
  }

  /// True when the fill replays every event past its fill position.
  final bool firstActivation;

  /// Exclusive upper client-timestamp bound of a gap replay, or `null` when
  /// no gap replay is pending.
  final DateTime? gapUpper;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'first_activation': firstActivation,
    'gap_upper': gapUpper?.toUtc().toIso8601String(),
  };

  @override
  bool operator ==(Object other) =>
      other is ReplayRequest &&
      other.firstActivation == firstActivation &&
      (other.gapUpper == null
          ? gapUpper == null
          : gapUpper != null && other.gapUpper!.isAtSameMomentAs(gapUpper!));

  @override
  int get hashCode =>
      Object.hash(firstActivation, gapUpper?.microsecondsSinceEpoch);

  @override
  String toString() =>
      'ReplayRequest(firstActivation: $firstActivation, gapUpper: $gapUpper)';
}

/// The database-wide record a destination-registry operation writes when its
/// outcome writes nothing else (a refusal, or an outcome that changes
/// nothing).
///
/// Writing it makes the operation's transaction a writing one, so a backend
/// that validates a transaction against other writers only when it writes
/// (a browser database shared by several tabs) decides the outcome on fresh
/// data. One record serves the whole database; each such operation overwrites
/// it.
class RegistryCheck {
  const RegistryCheck({
    required this.op,
    required this.destinationId,
    required this.outcome,
    required this.at,
  });

  /// Decode from the persisted JSON form.
  factory RegistryCheck.fromJson(Map<String, Object?> json) {
    String field(String name) {
      final value = json[name];
      if (value is! String) {
        throw FormatException('RegistryCheck: missing or non-string "$name"');
      }
      return value;
    }

    return RegistryCheck(
      op: field('op'),
      destinationId: field('destination_id'),
      outcome: field('outcome'),
      at: DateTime.parse(field('at')).toUtc(),
    );
  }

  /// The registry operation that wrote the record.
  final String op;

  /// The destination the operation named.
  final String destinationId;

  /// The outcome the operation decided (for example `refused_unknown_destination`).
  final String outcome;

  /// When the operation decided it.
  final DateTime at;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'op': op,
    'destination_id': destinationId,
    'outcome': outcome,
    'at': at.toUtc().toIso8601String(),
  };

  @override
  bool operator ==(Object other) =>
      other is RegistryCheck &&
      other.op == op &&
      other.destinationId == destinationId &&
      other.outcome == outcome &&
      other.at.isAtSameMomentAs(at);

  @override
  int get hashCode =>
      Object.hash(op, destinationId, outcome, at.microsecondsSinceEpoch);

  @override
  String toString() =>
      'RegistryCheck(op: $op, destinationId: $destinationId, '
      'outcome: $outcome, at: $at)';
}

/// A destination's open wedge: the record the drainer writes, in the
/// transaction that wedges the queue head, beside the wedge event it
/// appends. An operator recovery and a deletion remove it in the
/// transaction that ends the wedge, so it exists exactly while the log
/// holds a wedge event for the destination that no recovery or deletion
/// followed.
///
/// Persisted under `backend_state` key `wedge_<destinationId>`.
class WedgeRecord {
  const WedgeRecord({
    required this.rowId,
    required this.wedgeEventId,
    required this.cause,
    this.haltPurpose,
    this.drainerEpoch,
    this.configurationFingerprint,
  });

  /// Decode from the persisted JSON form.
  factory WedgeRecord.fromJson(Map<String, Object?> json) {
    String field(String name) {
      final value = json[name];
      if (value is! String) {
        throw FormatException('WedgeRecord: missing or non-string "$name"');
      }
      return value;
    }

    final haltPurpose = json['halt_purpose'];
    if (haltPurpose != null && haltPurpose is! String) {
      throw const FormatException('WedgeRecord: non-string "halt_purpose"');
    }
    final epoch = json['drainer_epoch'];
    if (epoch != null && epoch is! int) {
      throw const FormatException('WedgeRecord: non-integer "drainer_epoch"');
    }
    final fingerprint = json['configuration_fingerprint'];
    if (fingerprint != null && fingerprint is! String) {
      throw const FormatException(
        'WedgeRecord: non-string "configuration_fingerprint"',
      );
    }
    return WedgeRecord(
      rowId: field('row_id'),
      wedgeEventId: field('wedge_event_id'),
      cause: WedgeCause.fromWire(field('cause')),
      haltPurpose: haltPurpose == null
          ? null
          : HaltPurpose.fromWire(haltPurpose as String),
      drainerEpoch: epoch as int?,
      configurationFingerprint: fingerprint as String?,
    );
  }

  /// `entry_id` of the wedged queue item.
  final String rowId;

  /// `event_id` of the wedge event appended with the wedge.
  final String wedgeEventId;

  /// Why the drainer wedged the item.
  final WedgeCause cause;

  /// The purpose of the halt request the wedge consumed, or null when the
  /// wedge consumed none. A wedge of any cause consumes the request open at
  /// the wedge.
  final HaltPurpose? haltPurpose;

  /// The drain epoch of the lock the wedging drainer held.
  final int? drainerEpoch;

  /// The fingerprint of the configuration the wedging drainer declared for
  /// the destination, or null when the draining process did not register
  /// it.
  final String? configurationFingerprint;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'row_id': rowId,
    'wedge_event_id': wedgeEventId,
    'cause': cause.wire,
    'halt_purpose': haltPurpose?.wire,
    'drainer_epoch': drainerEpoch,
    'configuration_fingerprint': configurationFingerprint,
  };

  @override
  bool operator ==(Object other) =>
      other is WedgeRecord &&
      other.rowId == rowId &&
      other.wedgeEventId == wedgeEventId &&
      other.cause == cause &&
      other.haltPurpose == haltPurpose &&
      other.drainerEpoch == drainerEpoch &&
      other.configurationFingerprint == configurationFingerprint;

  @override
  int get hashCode => Object.hash(
    rowId,
    wedgeEventId,
    cause,
    haltPurpose,
    drainerEpoch,
    configurationFingerprint,
  );

  @override
  String toString() =>
      'WedgeRecord(rowId: $rowId, wedgeEventId: $wedgeEventId, '
      'cause: ${cause.wire}, haltPurpose: ${haltPurpose?.wire}, '
      'drainerEpoch: $drainerEpoch, '
      'configurationFingerprint: $configurationFingerprint)';
}

/// A destination's failing transform, kept while the fill retries it: the
/// times the transform has failed on the batch it would enqueue next, and
/// the sequence range of that batch.
///
/// The transform itself runs outside any transaction; the fill writes this
/// record, in a transaction of its own, the first time the transform fails
/// on a batch, and updates it on every later failure of the same batch.
/// Once the recorded failures have spent the destination's retry
/// budget, the fill enqueues the batch as one transform-failed item and
/// clears the record in the same transaction. Deletion, an operator
/// recovery, a receiver-behind resume and a new channel generation each
/// remove it too, in their own transaction, since each rewinds the fill
/// position below the batch the record names.
///
/// Persisted under `backend_state` key `transform_failure_<destinationId>`.
class TransformFailureRecord {
  const TransformFailureRecord({
    required this.failureTimes,
    required this.sequenceRange,
  });

  /// Decode from the persisted JSON form.
  factory TransformFailureRecord.fromJson(Map<String, Object?> json) {
    final times = json['failure_times'];
    if (times is! List) {
      throw const FormatException(
        'TransformFailureRecord: missing or non-list "failure_times"',
      );
    }
    final firstSeq = json['first_seq'];
    if (firstSeq is! int) {
      throw const FormatException(
        'TransformFailureRecord: missing or non-integer "first_seq"',
      );
    }
    final lastSeq = json['last_seq'];
    if (lastSeq is! int) {
      throw const FormatException(
        'TransformFailureRecord: missing or non-integer "last_seq"',
      );
    }
    return TransformFailureRecord(
      failureTimes: <DateTime>[
        for (final t in times)
          if (t is String)
            DateTime.parse(t).toUtc()
          else
            throw const FormatException(
              'TransformFailureRecord: non-string entry in "failure_times"',
            ),
      ],
      sequenceRange: (firstSeq: firstSeq, lastSeq: lastSeq),
    );
  }

  /// When the transform failed on this batch, oldest first.
  final List<DateTime> failureTimes;

  /// The sequence range of the batch the transform failed on.
  final SequenceRange sequenceRange;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'failure_times': <String>[
      for (final t in failureTimes) t.toUtc().toIso8601String(),
    ],
    'first_seq': sequenceRange.firstSeq,
    'last_seq': sequenceRange.lastSeq,
  };

  @override
  bool operator ==(Object other) =>
      other is TransformFailureRecord &&
      other.sequenceRange == sequenceRange &&
      other.failureTimes.length == failureTimes.length &&
      Iterable<int>.generate(
        failureTimes.length,
      ).every((i) => other.failureTimes[i].isAtSameMomentAs(failureTimes[i]));

  @override
  int get hashCode => Object.hash(
    sequenceRange,
    Object.hashAll(failureTimes.map((t) => t.toUtc().microsecondsSinceEpoch)),
  );

  @override
  String toString() =>
      'TransformFailureRecord(failureTimes: $failureTimes, '
      'sequenceRange: $sequenceRange)';
}

/// A destination's open halt request: the working copy of the latest
/// `system.destination_halt_requested` event that no cancellation, wedge or
/// deletion has closed.
///
/// The destination registry writes it in the transaction that appends the
/// request event, and the transaction that closes the request (a
/// cancellation, a wedge of any cause, or a deletion) clears it. The log is
/// authoritative: the drainer honours the record only after it finds the
/// request event it cites.
///
/// Persisted under `backend_state` key `halt_request_<destinationId>`.
class HaltRequest {
  const HaltRequest({
    required this.requestEventId,
    required this.requestedAt,
    required this.purpose,
    required this.requestedBy,
  });

  /// Decode from the persisted JSON form.
  factory HaltRequest.fromJson(Map<String, Object?> json) {
    final eventId = json['request_event_id'];
    if (eventId is! String || eventId.isEmpty) {
      throw const FormatException(
        'HaltRequest: missing or non-string "request_event_id"',
      );
    }
    final at = json['requested_at'];
    if (at is! String) {
      throw const FormatException(
        'HaltRequest: missing or non-string "requested_at"',
      );
    }
    final purpose = json['purpose'];
    if (purpose is! String) {
      throw const FormatException(
        'HaltRequest: missing or non-string "purpose"',
      );
    }
    final by = json['requested_by'];
    if (by is! Map) {
      throw const FormatException(
        'HaltRequest: missing or non-map "requested_by"',
      );
    }
    return HaltRequest(
      requestEventId: eventId,
      requestedAt: DateTime.parse(at).toUtc(),
      purpose: HaltPurpose.fromWire(purpose),
      requestedBy: Map<String, Object?>.from(by),
    );
  }

  /// `event_id` of the `system.destination_halt_requested` event.
  final String requestEventId;

  /// The request event's client timestamp.
  final DateTime requestedAt;

  /// Why the operator requested the halt.
  final HaltPurpose purpose;

  /// The request event's initiator, in its JSON form.
  final Map<String, Object?> requestedBy;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'request_event_id': requestEventId,
    'requested_at': requestedAt.toUtc().toIso8601String(),
    'purpose': purpose.wire,
    'requested_by': Map<String, Object?>.of(requestedBy),
  };

  @override
  bool operator ==(Object other) =>
      other is HaltRequest &&
      other.requestEventId == requestEventId &&
      other.requestedAt.isAtSameMomentAs(requestedAt) &&
      other.purpose == purpose &&
      const DeepCollectionEquality().equals(other.requestedBy, requestedBy);

  @override
  int get hashCode => Object.hash(
    requestEventId,
    requestedAt.microsecondsSinceEpoch,
    purpose,
    const DeepCollectionEquality().hash(requestedBy),
  );

  @override
  String toString() =>
      'HaltRequest(requestEventId: $requestEventId, requestedAt: '
      '$requestedAt, purpose: ${purpose.wire}, requestedBy: $requestedBy)';
}

/// The last send the drainer started on a destination: the queue item and
/// the attempt count it found in the pre-send fence transaction, written in
/// that transaction immediately before the send, and, on a delivery channel,
/// the number and hash of the delivery the send carries.
///
/// Writing it makes the fence a writing transaction, so a backend that
/// validates a transaction against other writers only when it writes (a
/// browser database shared by several tabs) checks the fence against every
/// commit ordered before it. It also records the send the drainer started
/// last, for diagnostics.
///
/// Persisted under `backend_state` key `send_fence_<destinationId>`.
class SendFence {
  const SendFence({
    required this.entryId,
    required this.attemptCount,
    required this.at,
    this.deliveryNumber,
    this.deliveryHash,
  }) : assert(
         (deliveryNumber == null) == (deliveryHash == null),
         'a send fence names both the delivery number and hash, or neither',
       );

  /// Decode from the persisted JSON form.
  factory SendFence.fromJson(Map<String, Object?> json) {
    final entryId = json['entry_id'];
    if (entryId is! String) {
      throw const FormatException(
        'SendFence: missing or non-string "entry_id"',
      );
    }
    final count = json['attempt_count'];
    if (count is! int) {
      throw const FormatException(
        'SendFence: missing or non-integer "attempt_count"',
      );
    }
    final at = json['at'];
    if (at is! String) {
      throw const FormatException('SendFence: missing or non-string "at"');
    }
    final number = json['delivery_number'];
    final hash = json['delivery_hash'];
    if (number != null && number is! int ||
        hash != null && hash is! String ||
        (number == null) != (hash == null)) {
      throw const FormatException(
        'SendFence: "delivery_number" (an integer) and "delivery_hash" (a '
        'string) are both present or both absent',
      );
    }
    return SendFence(
      entryId: entryId,
      attemptCount: count,
      at: DateTime.parse(at).toUtc(),
      deliveryNumber: number as int?,
      deliveryHash: hash as String?,
    );
  }

  /// `entry_id` of the queue item the send carries.
  final String entryId;

  /// Attempts recorded on the item when the fence ran (before this send).
  final int attemptCount;

  /// When the fence ran, by the drainer's clock.
  final DateTime at;

  /// The number of the delivery the send carries, on a delivery channel;
  /// null for a destination that is no channel.
  // Implements: EVS-DEV-delivery-channel/I
  // the send fence record names the delivery number and delivery hash of
  //   the delivery in flight.
  final int? deliveryNumber;

  /// The hash of the delivery the send carries, on a delivery channel; null
  /// for a destination that is no channel.
  final String? deliveryHash;

  /// Persisted JSON form. The delivery keys are present only for a send of
  /// a delivery on a channel.
  Map<String, Object?> toJson() => <String, Object?>{
    'entry_id': entryId,
    'attempt_count': attemptCount,
    'at': at.toUtc().toIso8601String(),
    if (deliveryNumber != null) 'delivery_number': deliveryNumber,
    if (deliveryHash != null) 'delivery_hash': deliveryHash,
  };

  @override
  bool operator ==(Object other) =>
      other is SendFence &&
      other.entryId == entryId &&
      other.attemptCount == attemptCount &&
      other.at.isAtSameMomentAs(at) &&
      other.deliveryNumber == deliveryNumber &&
      other.deliveryHash == deliveryHash;

  @override
  int get hashCode => Object.hash(
    entryId,
    attemptCount,
    at.microsecondsSinceEpoch,
    deliveryNumber,
    deliveryHash,
  );

  @override
  String toString() =>
      'SendFence(entryId: $entryId, attemptCount: $attemptCount, at: $at, '
      'deliveryNumber: $deliveryNumber, deliveryHash: $deliveryHash)';
}

/// The sender's record of one registration's delivery channel: the current
/// generation, the receiver record the sender last established on it (the
/// number and hash of the last delivery the receiver accepted, as far as
/// the sender knows) and the database identity of the receiver that
/// answered on the current generation.
///
/// Registering a destination that serializes natively writes it with
/// [initial]; deleting the destination removes it. Between the two only the
/// drainer changes it, in the transaction that commits a send outcome, a
/// resume or a new generation of the registration.
///
/// Persisted under `backend_state` key `sender_channel_<destinationId>`.
// Implements: EVS-DEV-delivery-channel/D
// the sender channel record: the generation, the number and hash of the
//   receiver record the sender last established, and the receiver identity
//   that answered on the current generation.
final class SenderChannelRecord {
  const SenderChannelRecord({
    required this.generation,
    required this.receiverRecord,
    this.receiverDatabaseId,
  });

  /// Decode from the persisted JSON form. Throws [FormatException] for a
  /// generation below 1, a record whose hash is null exactly when its
  /// number is not 0, or a non-string receiver identity.
  factory SenderChannelRecord.fromJson(Map<String, Object?> json) {
    final generation = json['generation'];
    if (generation is! int || generation < 1) {
      throw const FormatException(
        'SenderChannelRecord: "generation" must be an integer of at least 1',
      );
    }
    final receiverDatabaseId = json['receiver_database_id'];
    if (receiverDatabaseId != null && receiverDatabaseId is! String) {
      throw const FormatException(
        'SenderChannelRecord: non-string "receiver_database_id"',
      );
    }
    return SenderChannelRecord(
      generation: generation,
      receiverRecord: DeliveryRecord.fromJson(<String, Object?>{
        'delivery_number': json['delivery_number'],
        'delivery_hash': json['delivery_hash'],
      }),
      receiverDatabaseId: receiverDatabaseId as String?,
    );
  }

  /// The record a registration starts with: generation 1, number 0, a null
  /// hash and no receiver identity.
  static const SenderChannelRecord initial = SenderChannelRecord(
    generation: 1,
    receiverRecord: DeliveryRecord.none,
  );

  /// The registration's current generation, 1 for the first.
  final int generation;

  /// The number and hash of the receiver record the sender last
  /// established on the current generation.
  final DeliveryRecord receiverRecord;

  /// The database identity of the receiver that answered on the current
  /// generation, or null before any answered.
  final String? receiverDatabaseId;

  /// Persisted JSON form.
  Map<String, Object?> toJson() => <String, Object?>{
    'generation': generation,
    'delivery_number': receiverRecord.deliveryNumber,
    'delivery_hash': receiverRecord.deliveryHash,
    'receiver_database_id': receiverDatabaseId,
  };

  @override
  bool operator ==(Object other) =>
      other is SenderChannelRecord &&
      other.generation == generation &&
      other.receiverRecord == receiverRecord &&
      other.receiverDatabaseId == receiverDatabaseId;

  @override
  int get hashCode =>
      Object.hash(generation, receiverRecord, receiverDatabaseId);

  @override
  String toString() =>
      'SenderChannelRecord(generation: $generation, receiverRecord: '
      '$receiverRecord, receiverDatabaseId: $receiverDatabaseId)';
}
