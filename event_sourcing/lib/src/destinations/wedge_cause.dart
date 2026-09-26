// Implements: EVS-PRD-destinations/Q
// the wedge event records why the drainer
//   wedged the head: a permanent failure the delivery implementation
//   reported, an exhausted retry budget, or an operator halt.
// Implements: EVS-PRD-portability/C
// a pure Dart value type; serialises
//   identically on every Dart-supported runtime.
import 'package:meta/meta.dart' show immutable;

/// Why the drainer wedged a destination's queue head. Recorded as `cause`
/// on the wedge event and in the destination's wedge record.
///
/// The cause is an open value: a later release of the data-format major may
/// add a cause, and a wedge event or wedge record of a cause this build does
/// not know (written by a newer build sharing the database, or ingested
/// from a peer) reads with its string verbatim ([WedgeCause.fromWire] never throws;
/// [isKnown] is false for it). This build wedges only with the causes in
/// [values].
// Implements: EVS-DEV-destination-drain/L
// the wedge cause is an open value: a value this build does not know is
//   carried verbatim wherever it is read.
@immutable
final class WedgeCause {
  const WedgeCause._(this.wire, {required this.isKnown});

  /// The cause whose [wire] string is [value]: one of [values], or, for a
  /// string this build does not know, a cause carrying it verbatim.
  factory WedgeCause.fromWire(String value) {
    for (final cause in values) {
      if (cause.wire == value) return cause;
    }
    return WedgeCause._(value, isKnown: false);
  }

  /// The delivery implementation reported a permanent failure
  /// (`SendPermanent`) for the head.
  static const permanentRefusal = WedgeCause._(
    'permanent_refusal',
    isKnown: true,
  );

  /// The head's recorded attempts reached the retry budget in effect
  /// (`SyncPolicy.maxAttempts`) without a delivery.
  static const retryBudgetExhausted = WedgeCause._(
    'retry_budget_exhausted',
    isKnown: true,
  );

  /// An operator requested a halt (`DestinationRegistry.requestHalt`), and
  /// the drainer honoured the request by wedging the head before its next
  /// send, with no delivery failure behind the wedge: a head whose last
  /// attempt reported a permanent failure is wedged with
  /// [permanentRefusal] instead, a wedge that consumes the request all the
  /// same.
  static const operatorHalt = WedgeCause._('operator_halt', isKnown: true);

  /// Every cause this build wedges with.
  static const List<WedgeCause> values = <WedgeCause>[
    permanentRefusal,
    retryBudgetExhausted,
    operatorHalt,
  ];

  /// The string recorded in the wedge event and the wedge record. A later
  /// release of the data-format major may add a cause; none is removed or
  /// renamed within a major.
  final String wire;

  /// Whether this build knows the cause.
  final bool isKnown;

  @override
  bool operator ==(Object other) => other is WedgeCause && other.wire == wire;

  @override
  int get hashCode => wire.hashCode;

  @override
  String toString() => 'WedgeCause($wire)';
}
