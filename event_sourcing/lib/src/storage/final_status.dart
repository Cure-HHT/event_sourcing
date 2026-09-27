import 'package:meta/meta.dart' show internal;

/// Terminal state of a FifoEntry within its destination's FIFO.
///
/// A FifoEntry's `finalStatus` is nullable: `null` means "not yet
/// terminal" (drain may attempt the row), and a non-null value is one
/// of three terminal states below. Once a FIFO entry's `finalStatus` is
/// non-null it is retained for the database's lifetime as the delivery
/// record, including after its destination is deleted. Only rows whose
/// `finalStatus` is `null` and that carry no attempt are ever deleted: by an
/// operator recovery's trail sweep (`tombstoneAndRefill`), by a
/// destination's deletion, and by a resume or a new generation of a
/// delivery channel.
///
/// The legal transitions are exactly `null -> sent` and `null -> wedged`
/// (the drainer's outcomes), `wedged -> tombstoned` (a recovery or a
/// deletion retiring a wedged head) and, for a row carrying attempts,
/// `null -> tombstoned` (a resume or a new generation of its delivery
/// channel retiring it).
// Implements: EVS-PRD-portability/C
// pure Dart enum; platform-independent
//   serialisation via name-based toJson/fromJson.
enum FinalStatus {
  sent,
  wedged,
  tombstoned;

  /// Parse a wire-format string; throws [FormatException] on unknown input.
  factory FinalStatus.fromJson(String raw) {
    for (final v in values) {
      if (v.name == raw) return v;
    }
    throw FormatException(
      'FinalStatus: unknown value "$raw" '
      '(legal values: sent | wedged | tombstoned)',
    );
  }

  /// Serialize to the wire-format string used in persisted records.
  String toJson() => name;
}

/// Whether a queue item's status may change from [current] to [next]:
/// exactly `null -> sent`, `null -> wedged`, `wedged -> tombstoned` and,
/// for an item that carries attempts ([hasAttempts]), `null -> tombstoned`
/// (a resume or a new generation of its delivery channel retires it). The
/// shipped backends' `setFinalStatusTxn` refuses every other change.
// Implements: EVS-DEV-destination-drain/B
// the one table of legal status changes.
@internal
bool isLegalFinalStatusTransition(
  FinalStatus? current,
  FinalStatus next, {
  required bool hasAttempts,
}) =>
    (current == null &&
        (next == FinalStatus.sent ||
            next == FinalStatus.wedged ||
            (next == FinalStatus.tombstoned && hasAttempts))) ||
    (current == FinalStatus.wedged && next == FinalStatus.tombstoned);
