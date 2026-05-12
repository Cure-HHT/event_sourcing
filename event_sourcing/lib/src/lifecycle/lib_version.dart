// Implements: EVS-DEV-event-store-open/B/C/D
/// Substrate-level library version metadata. The version is recorded in
/// the event log via `lib_version_initialized` and `lib_version_changed`
/// events; the boot flow refuses to start when the log was last processed
/// by a newer version than this constant.
class LibVersion {
  /// The version of the event_sourcing library compiled into this build.
  /// Update in lockstep with `pubspec.yaml`'s `version` field.
  static const String current = '0.4.0';

  /// Returns negative if [a] < [b], positive if [a] > [b], 0 if equal.
  /// Compares dot-separated integer components left to right; trailing
  /// missing components count as zero.
  ///
  /// Both [a] and [b] must be dot-separated non-negative integers
  /// (e.g. `'0.4.0'`, `'1.10.3'`). Segments that are empty or contain
  /// non-numeric characters cause [int.parse] to throw a [FormatException].
  /// The lib only ever passes its own version constants and values from its
  /// own `lib_version_*` events through this comparator, so the strict-int
  /// contract is intentional.
  static int compare(String a, String b) {
    final aParts = a.split('.').map(int.parse).toList();
    final bParts = b.split('.').map(int.parse).toList();
    final maxLen = aParts.length > bParts.length
        ? aParts.length
        : bParts.length;
    for (var i = 0; i < maxLen; i++) {
      final av = i < aParts.length ? aParts[i] : 0;
      final bv = i < bParts.length ? bParts[i] : 0;
      if (av != bv) return av - bv;
    }
    return 0;
  }
}

class LibVersionEvents {
  static const String initialized = 'lib_version_initialized';
  static const String changed = 'lib_version_changed';
}
