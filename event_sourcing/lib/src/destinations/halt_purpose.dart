// Implements: EVS-DEV-destination-drain/Q
// a halt request records why the operator
//   asked for it: to pause delivery, or to halt it before a new delivery
//   configuration is deployed.
// Implements: EVS-PRD-portability/C
// a pure Dart value type; serialises
//   identically on every Dart-supported runtime.

/// Why an operator requested a halt (`DestinationRegistry.requestHalt`).
/// Recorded as `purpose` on the halt request event, and as `halt_purpose`
/// on the wedge event and in the wedge record of the wedge that consumes the
/// request.
enum HaltPurpose {
  /// Stop delivery, for example to rebuild the destination's pending items
  /// under a delivery configuration that is already deployed.
  pause('pause'),

  /// Stop delivery before a new delivery configuration for the destination
  /// is deployed, so that the recovery that follows refills under it.
  reconfigure('reconfigure');

  const HaltPurpose(this.wire);

  /// The string recorded in the halt request event, the wedge event and the
  /// wedge record. The set of these strings is part of the library's data
  /// format: adding, removing or renaming a purpose is a data-format major
  /// step.
  final String wire;

  /// The purpose whose [wire] string is [value]. Throws [FormatException]
  /// for any other value.
  static HaltPurpose fromWire(String value) {
    for (final purpose in values) {
      if (purpose.wire == value) return purpose;
    }
    throw FormatException('HaltPurpose: unknown purpose "$value"');
  }
}
