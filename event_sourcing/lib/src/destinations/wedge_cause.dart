// Implements: EVS-PRD-destinations/Q
// the wedge event records why the drainer
//   wedged the head: a permanent failure the delivery implementation
//   reported, an exhausted retry budget, or an operator halt.
// Implements: EVS-PRD-portability/C
// a pure Dart value type; serialises
//   identically on every Dart-supported runtime.

/// Why the drainer wedged a destination's queue head. Recorded as `cause`
/// on the wedge event and in the destination's wedge record.
enum WedgeCause {
  /// The delivery implementation reported a permanent failure
  /// (`SendPermanent`) for the head.
  permanentRefusal('permanent_refusal'),

  /// The head's recorded attempts reached the retry budget in effect
  /// (`SyncPolicy.maxAttempts`) without a delivery.
  retryBudgetExhausted('retry_budget_exhausted'),

  /// An operator requested a halt (`DestinationRegistry.requestHalt`), and
  /// the drainer honoured the request by wedging the head before its next
  /// send, with no delivery failure behind the wedge: a head whose last
  /// attempt reported a permanent failure is wedged with
  /// [permanentRefusal] instead, a wedge that consumes the request all the
  /// same.
  operatorHalt('operator_halt');

  const WedgeCause(this.wire);

  /// The string recorded in the wedge event and the wedge record. The set of
  /// these strings is part of the library's data format: adding, removing or
  /// renaming a cause is a data-format major step.
  final String wire;

  /// The cause whose [wire] string is [value]. Throws [FormatException] for
  /// any other value.
  static WedgeCause fromWire(String value) {
    for (final cause in values) {
      if (cause.wire == value) return cause;
    }
    throw FormatException('WedgeCause: unknown cause "$value"');
  }
}
