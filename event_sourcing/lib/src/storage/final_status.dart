/// Terminal state of a FifoEntry within its destination's FIFO.
///
/// A FifoEntry's `finalStatus` is nullable: `null` means "not yet
/// terminal" (drain may attempt the row), and a non-null value is one
/// of three terminal states below. Once a FIFO entry's `finalStatus` is
/// non-null it is retained forever as an audit record; the FIFO never
/// deletes it. The sole code path that deletes a FIFO row is the
/// `tombstoneAndRefill` trail sweep, and that path only deletes rows
/// whose `finalStatus` is `null`.
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
