// Implements: EVS-DEV-postgres-backend/H
// PostgresSchemaIncompatibleException names provisioning as the way to a
//   database whose schema this build does not support.
// Implements: EVS-DEV-postgres-backend/J
// LockSessionConfigurationException names the lock-session requirement.

/// Thrown by `PostgresBackend.open`, by the generation guard's boot lock,
/// and by `PostgresBackend.provision`, when the database's schema is not one
/// this build supports: no schema is provisioned, its schema version is
/// below this build's, or its minimum compatible schema version is above
/// this build's schema version. Nothing is written.
///
/// A database with no schema, or one at an older schema version, is brought
/// to this build's version by `PostgresBackend.provision` (or
/// `PostgresBackend.open(provisionSchema: true)`), run once per deployment
/// before the instances open it.
class PostgresSchemaIncompatibleException implements Exception {
  /// A refusal giving [reason], with the stored pair (null when absent) and
  /// the build's schema version.
  const PostgresSchemaIncompatibleException({
    required this.reason,
    required this.storedSchemaVersion,
    required this.storedMinCompatibleSchemaVersion,
    required this.buildSchemaVersion,
  });

  /// What the build found.
  final String reason;

  /// The schema version stored in the database, or null when none is.
  final int? storedSchemaVersion;

  /// The minimum compatible schema version stored in the database, or null
  /// when none is.
  final int? storedMinCompatibleSchemaVersion;

  /// The schema version this build requires and provisions.
  final int buildSchemaVersion;

  @override
  String toString() =>
      'PostgresSchemaIncompatibleException: $reason (stored schema version '
      '${storedSchemaVersion ?? 'none'}, stored minimum compatible version '
      "${storedMinCompatibleSchemaVersion ?? 'none'}, this build's schema "
      'version $buildSchemaVersion). Provision the database with '
      'PostgresBackend.provision (or PostgresBackend.open(provisionSchema: '
      'true)) from a build whose schema version is at least the stored '
      'minimum, before opening it.';
}

/// Thrown by `PostgresBackend.open` and `PostgresBackend.provision` when
/// the lock connection is not one server session reaching the pool's
/// database and schema.
///
/// The library holds its generation and drain locks on a dedicated lock
/// session, which must be a direct connection to the database or go
/// through a session-mode proxy that resets sessions on release. A
/// transaction-mode pooler routes separate statements to different server
/// connections, so a lock taken through it lands on a connection the
/// library cannot address again; it is not supported for the lock
/// connection. The check the library runs can miss a pooler that happens to
/// hand back the same server connection every time.
class LockSessionConfigurationException implements Exception {
  /// A refusal giving [message].
  const LockSessionConfigurationException(this.message);

  /// What the check observed.
  final String message;

  @override
  String toString() =>
      'LockSessionConfigurationException: $message. The lock connection '
      '(lockUrl, or the url when no lockUrl is given) must be a direct '
      'connection to the database, or go through a session-mode proxy that '
      'resets sessions on release, and must reach the same database and '
      'schema as the pool; a transaction-mode pooler is not supported for '
      'it.';
}
