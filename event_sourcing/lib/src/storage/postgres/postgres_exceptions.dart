// Implements: EVS-DEV-postgres-backend/H
// PostgresSchemaIncompatibleException names provisioning as the way to a
//   database whose schema this build does not support.
// Implements: EVS-DEV-postgres-backend/J
// LockSessionConfigurationException names the lock-session requirement.
// Implements: EVS-DEV-postgres-backend/M+N+P
// PostgresRoleRefusedException names each role open refuses and the
//   privilege, attribute, membership or missing declaration it refuses it
//   for.

/// Thrown by `PostgresBackend.open`, by the generation guard's boot lock,
/// and by `PostgresBackend.provision`, when the database's schema is not one
/// this build supports: no schema is provisioned, its schema version is
/// below this build's, or its minimum compatible schema version is above
/// this build's schema version. Nothing is written.
///
/// A database with no schema, or one at an older schema version, is brought
/// to this build's version by `PostgresBackend.provision`, run as the
/// owner once per deployment before the instances open it.
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
      'PostgresBackend.provision from a build whose schema version is at '
      'least the stored minimum, before opening it.';
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

/// Thrown by `PostgresBackend.open` and `PostgresBackend.provision` when the
/// current schema inside a library transaction is not the schema the
/// caller named: the named schema does not exist, or the connecting role
/// cannot use it. Nothing is written and no generation is registered.
///
/// Every library transaction sets its search path to the named schema,
/// `pg_catalog` and `pg_temp`, so the server skips a named schema it cannot
/// find and resolves the library's tables nowhere. The deployment creates
/// the schema, owned by the role that provisions it, and grants the runtime
/// role `USAGE` on it.
class PostgresSchemaMismatchException implements Exception {
  /// A refusal of [describedSchema], in place of which the library's search
  /// path resolved to [currentSchema].
  const PostgresSchemaMismatchException({
    required this.describedSchema,
    required this.currentSchema,
  });

  /// The schema the caller named.
  final String describedSchema;

  /// The current schema inside a library transaction: `pg_catalog` when
  /// the named schema does not exist or the role lacks `USAGE` on it, since
  /// the server skips such a schema in the search path; null when no schema
  /// on the path resolves.
  final String? currentSchema;

  @override
  String toString() =>
      'PostgresSchemaMismatchException: the library schema is '
      '"$describedSchema", but the current schema inside a library '
      "transaction is ${currentSchema == null ? 'none' : '"$currentSchema"'}; "
      'the schema does not exist or the role cannot use it. Create the '
      'schema, owned by the role that provisions it, and grant the runtime '
      'role USAGE on it.';
}

/// One reason `PostgresBackend.open` refuses a database: [role] holds
/// [privilege], as [detail] says.
final class PostgresRoleRefusal {
  /// A refusal of [role] for [privilege], described by [detail].
  const PostgresRoleRefusal({
    required this.role,
    required this.privilege,
    required this.detail,
  });

  /// The role refused: a role name, or `PUBLIC` for every role.
  final String role;

  /// What the role is refused for. A table, column, sequence or schema
  /// privilege (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `TRIGGER`,
  /// `REFERENCES`, `USAGE`, `CREATE`, or, for `PUBLIC`, any privilege); a
  /// role attribute (`SUPERUSER`, `CREATEROLE`); `OWNER` for a pool or lock
  /// role that owns the library's schema or a table in it; `MEMBER` for a
  /// membership through which the role can inherit the privileges of, or
  /// set its role to, a role it must not act as; or `UNDECLARED` for a pool
  /// or lock role provisioning did not declare as a role of that kind.
  final String privilege;

  /// Where the role holds [privilege], or through which role.
  final String detail;

  @override
  String toString() => '$role: $privilege $detail';
}

/// Thrown by `PostgresBackend.open` when a role outside the library's own
/// may write the library's tables or act as one of its roles, or when the
/// role its pool or its lock session connects as is undeclared or could
/// change the schema. It lists every [refusals] it found; nothing is
/// written, no generation is registered and no lock is held.
///
/// The library's own roles are the owner of its tables and the runtime and
/// lock roles `PostgresBackend.provision` declared. `open` refuses:
///
/// - a pool role provisioning did not declare as a runtime role, or a lock
///   role it did not declare as a lock role (a lock session opened without
///   a lock URL connects as the pool role, which is then declared as a lock
///   role too);
/// - a pool or lock role that owns the schema or one of its tables, that
///   can inherit the privileges of or set its role to the owner of the
///   library's tables, or that holds `SUPERUSER` or `CREATEROLE` directly or
///   through a role it can inherit or set;
/// - a role other than the owner and the declared roles holding `INSERT`,
///   `UPDATE`, `DELETE`, `TRUNCATE`, `TRIGGER` or `REFERENCES` on a table of
///   the library's schema or on a column of one, or `USAGE` or `UPDATE` on a
///   sequence in it; any role but the owner holding a write privilege on
///   the declared roles' table;
/// - such a role able to inherit the privileges of, or set its role to,
///   `pg_write_all_data`, the owner or a declared role;
/// - `PUBLIC` holding any privilege on a table of the library's schema;
/// - a role other than the owner, `PUBLIC` included, holding `CREATE` on
///   the library's schema.
///
/// `SELECT` held by a role other than `PUBLIC` is admitted, and a
/// membership held only with the admin option grants neither inheritance
/// nor set and is admitted. Revoke the privilege or membership named, or
/// declare the role by provisioning again, then open again.
class PostgresRoleRefusedException implements Exception {
  /// A refusal listing [refusals].
  const PostgresRoleRefusedException(this.refusals);

  /// Every reason found, at least one.
  final List<PostgresRoleRefusal> refusals;

  @override
  String toString() =>
      'PostgresRoleRefusedException: the database grants a role outside '
      "the library's own a privilege to write its tables or to act as one "
      'of its roles, or the pool or lock role is undeclared or could change '
      "the schema: ${refusals.join('; ')}. Revoke what is named, or declare "
      'the role with PostgresBackend.provision, before opening the database.';
}
