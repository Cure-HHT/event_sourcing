// Implements: EVS-DEV-destination-drain/Q
// a halt request records why the operator
//   asked for it: to pause delivery, or to halt it before a new delivery
//   configuration is deployed.
// Implements: EVS-PRD-portability/C
// a pure Dart value type; serialises
//   identically on every Dart-supported runtime.
import 'package:meta/meta.dart' show immutable;

/// Why an operator requested a halt (`DestinationRegistry.requestHalt`).
/// Recorded as `purpose` on the halt request event, and as `halt_purpose`
/// on the wedge event and in the wedge record of the wedge that consumes the
/// request.
///
/// The purpose is an open value: a later release of the data-format major
/// may add a purpose, and a halt request, wedge event or wedge record of a
/// purpose this build does not know reads with its string verbatim
/// ([HaltPurpose.fromWire] never throws; [isKnown] is false for it). This build honours
/// such a request as a halt, and recovers the wedge it made without the
/// configuration check of [reconfigure]. It requests halts only with the
/// purposes in [values].
// Implements: EVS-DEV-destination-drain/L
// the halt purpose is an open value: a value this build does not know is
//   carried verbatim wherever it is read.
@immutable
final class HaltPurpose {
  const HaltPurpose._(this.wire, {required this.isKnown});

  /// The purpose whose [wire] string is [value]: one of [values], or, for a
  /// string this build does not know, a purpose carrying it verbatim.
  factory HaltPurpose.fromWire(String value) {
    for (final purpose in values) {
      if (purpose.wire == value) return purpose;
    }
    return HaltPurpose._(value, isKnown: false);
  }

  /// Stop delivery, for example to rebuild the destination's pending items
  /// under a delivery configuration that is already deployed.
  static const pause = HaltPurpose._('pause', isKnown: true);

  /// Stop delivery before a new delivery configuration for the destination
  /// is deployed, so that the recovery that follows refills under it.
  static const reconfigure = HaltPurpose._('reconfigure', isKnown: true);

  /// Every purpose this build requests a halt with.
  static const List<HaltPurpose> values = <HaltPurpose>[pause, reconfigure];

  /// The string recorded in the halt request event, the wedge event and the
  /// wedge record. A later release of the data-format major may add a
  /// purpose; none is removed or renamed within a major.
  final String wire;

  /// Whether this build knows the purpose.
  final bool isKnown;

  @override
  bool operator ==(Object other) => other is HaltPurpose && other.wire == wire;

  @override
  int get hashCode => wire.hashCode;

  @override
  String toString() => 'HaltPurpose($wire)';
}
