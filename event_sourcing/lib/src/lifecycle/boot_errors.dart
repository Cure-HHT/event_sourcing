// Implements: EVS-DEV-event-store-open/D
// DataFormatIncompatibleError carries the recorded package version and data
//   format beside this build's, and names the ways out.
// Implements: EVS-DEV-event-store-open/F
// DatabaseResetRequiredError and DatabaseIdentityMismatchError are the two
//   distinct identity refusals.
import 'package:event_sourcing/src/versions.dart';

/// Thrown by `EventStore.open` when the data-format major recorded in the
/// database's latest locally appended library-version event differs from
/// the data-format major of the build that opens it. Nothing is written.
///
/// An older build cannot open a database of a newer data-format major, and
/// a build of a newer major has no reader for an older one. The way out is
/// the build that wrote the database (the recorded package version), or a
/// restore of a backup taken before a build of another major opened it.
class DataFormatIncompatibleError extends Error {
  DataFormatIncompatibleError({
    required this.recordedPackageVersion,
    required this.recordedDataFormat,
    required this.packageVersion,
    required this.dataFormat,
  });

  /// The package version recorded in the latest locally appended
  /// library-version event.
  final String recordedPackageVersion;

  /// The data format recorded in the latest locally appended
  /// library-version event.
  final DataFormatVersion recordedDataFormat;

  /// The package version of the build that was refused.
  final String packageVersion;

  /// The data format of the build that was refused.
  final DataFormatVersion dataFormat;

  @override
  String toString() =>
      'DataFormatIncompatibleError: the database was last opened by library '
      '$recordedPackageVersion (data format $recordedDataFormat); this build '
      'is library $packageVersion (data format $dataFormat). Builds open a '
      'database only within one data-format major: an older build cannot '
      'open a database of a newer data-format major, and a build of a newer '
      'major has no reader for an older one. Open it with a build of data '
      'format ${recordedDataFormat.major}.x (such as '
      '$recordedPackageVersion), or restore a backup taken before a build of '
      'another major opened it.';
}

/// Thrown by `EventStore.open` when the database was written by a library
/// build whose stored shapes this build does not read: its events or view
/// targets carry an earlier data format's version shape, or its
/// library-version events record no database identity or no data format.
/// Nothing is written. The database must be reset (deleted and created
/// again); no migration from those builds exists.
class DatabaseResetRequiredError extends StateError {
  DatabaseResetRequiredError(this.reason)
    : super(
        'The database was written by a library build that this build does '
        'not read ($reason); it must be reset (deleted and created again).',
      );

  /// What this build found in the database.
  final String reason;

  @override
  String toString() => 'DatabaseResetRequiredError: $message';
}

/// Thrown by `EventStore.open` when the database identity stored beside
/// the log is missing or differs from the identity recorded in the
/// database's first locally appended `lib_version_initialized` event.
/// Nothing is written.
///
/// The identity is written once, at the first open, and never changes, so
/// a missing or different identity means the storage lost or changed a
/// record the library wrote: a storage durability failure, a restore that
/// mixed two databases, or tampering.
class DatabaseIdentityMismatchError extends StateError {
  DatabaseIdentityMismatchError({
    required this.recordedDatabaseId,
    required this.storedDatabaseId,
  }) : super(
         storedDatabaseId == null
             ? 'The database identity $recordedDatabaseId recorded in the '
                   'log has no stored counterpart: a storage durability or '
                   'tampering failure.'
             : 'The stored database identity $storedDatabaseId differs '
                   'from the identity $recordedDatabaseId recorded in the '
                   'log: a storage durability or tampering failure.',
       );

  /// The identity the first locally appended `lib_version_initialized`
  /// event records.
  final String recordedDatabaseId;

  /// The identity stored beside the log, or null when none is stored.
  final String? storedDatabaseId;

  @override
  String toString() => 'DatabaseIdentityMismatchError: $message';
}
