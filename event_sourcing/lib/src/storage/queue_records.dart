// Implements: EVS-PRD-portability/C
// pure Dart value types; serialise
//   identically on every Dart-supported runtime.
// Implements: EVS-DEV-destination-drain/A+E+F+U
// the records the storage contract
//   returns or keeps beside a destination's queue: the trail sweep's result
//   (lowest event it removed, for the recovery rewind), a queue's retirement
//   on deletion, a pending replay request, and the database-wide registry
//   check record.

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
