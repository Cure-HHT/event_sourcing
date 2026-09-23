// Implements: EVS-DEV-event-store-open/B+C+D
// LibVersion.version and LibVersion.dataFormat are the two versions every
//   library-version event records.
// Implements: EVS-DEV-version-compatibility/C
// LibVersion.dataFormat is the library's data-format version, distinct from
//   the package version.
import 'package:event_sourcing/src/versions.dart';

/// This build's library versions: its package version ([version]) and its
/// data-format version ([dataFormat]).
///
/// `EventStore.open` records both in the `lib_version_initialized` or
/// `lib_version_changed` event it appends, so the log states which builds
/// opened the database, and in what order. The data format decides: a
/// build opens a database last opened by a build of the same data-format
/// major, older or newer, and refuses one of another major. A deployment
/// pipeline compares a release's `dataFormat.major` with the one its
/// serving revision runs: the same major deploys beside it (a canary,
/// scale-out, a rollback); another major is deployed stop-then-start.
class LibVersion {
  /// The version of the event_sourcing library compiled into this build.
  /// Update in lockstep with `pubspec.yaml`'s `version` field. Recorded in
  /// the log; it decides nothing.
  static const String version = '0.5.0';

  /// The data-format version of this build: what it stores and sends,
  /// distinct from [version]. Stamped on every event the library appends.
  /// Builds whose data-format majors are equal are compatible and share a
  /// database in any mix; builds of different majors are deployed
  /// stop-then-start, every instance of the old major stopping before the
  /// first of the new one opens the database, and recovery after such a
  /// deployment is a restore from a backup taken before the switch, or a
  /// roll-forward. See [DataFormatVersion] for the rule that decides a minor
  /// or a major bump.
  static const DataFormatVersion dataFormat = DataFormatVersion(2, 0);

  /// Returns negative if [a] < [b], positive if [a] > [b], 0 if equal.
  /// Compares dot-separated integer components left to right; trailing
  /// missing components count as zero.
  ///
  /// Both [a] and [b] must be dot-separated non-negative integers
  /// (e.g. `'0.4.0'`, `'1.10.3'`). Segments that are empty or contain
  /// non-numeric characters cause [int.parse] to throw a [FormatException].
  /// Package versions order releases for display and audit; compatibility
  /// is decided by [dataFormat], never by this order.
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
