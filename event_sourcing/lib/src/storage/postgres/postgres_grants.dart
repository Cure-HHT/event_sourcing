// Implements: EVS-DEV-postgres-backend/K
// the privileges the Postgres runtime role needs on each table the
//   library provisions, stated once for the deployment's grants and for the
//   documentation.
// Implements: EVS-DEV-postgres-backend/O
// beside the runtime role's privileges, the setup for an application's own
//   tables: a schema and a role of its own, at most SELECT on a library
//   table, and no membership in a declared role, the owner or
//   pg_write_all_data.

/// The table privileges the role an application's `PostgresBackend` runs
/// as (the runtime role) needs on each table provisioning creates, and no
/// more: every library operation other than provisioning works for a role
/// that holds exactly these privileges, `USAGE` on the schema, and neither
/// owns nor can create the tables. Provisioning creates the tables and runs
/// as the role that owns the schema.
///
/// The deployment grants them, as the role that owns the schema, after
/// each provisioning and before the new build's instances start, for
/// example `GRANT SELECT, INSERT ON events TO runtime_role`. The log is
/// append-only for the runtime role (`SELECT` and `INSERT` on `events`),
/// and the declared roles' table (`library_roles`) is read-only for it;
/// the owner-only operations (provisioning, which declares the runtime and
/// lock roles, and changing or dropping the queue table's guard) stay with
/// the owning role. The runtime role must not be able to become the owner:
/// it owns nothing in the schema, is not a member of the owning role, holds
/// neither `SUPERUSER` nor `CREATEROLE` nor membership in a role that
/// carries them, and has no `CREATE` on the schema. For a lock connection
/// opened as another role (`PostgresBackend.open`'s `lockUrl`), `USAGE` on
/// the schema and these privileges on `backend_state` suffice. Every role a
/// backend connects as is declared when the database is provisioned
/// (`PostgresBackend.provision`'s `runtimeRoles` and `lockRoles`), and
/// `PostgresBackend.open` checks all of this.
///
/// No other role may write the library's tables. `PostgresBackend.open`
/// refuses a database on which a role other than the owner and the declared
/// roles holds a write privilege on a library table, a column of one or a
/// sequence in the schema, or can act as `pg_write_all_data`, the owner or a
/// declared role, on which `PUBLIC` holds any privilege on a library table,
/// or on which a role other than the owner holds `CREATE` on the schema.
/// `SELECT` is admitted, so a read-only reporting role keeps working.
///
/// ## An application's own tables
///
/// An application that keeps tables of its own in the library's database
/// (an idempotency store of its own, or a job table):
///
/// - creates a schema of its own, which the library does not provision,
///   and keeps its tables there;
/// - connects under an application role of its own, through a pool it
///   opens itself, never the library's roles or connections;
/// - grants that application role no privilege on a library table beyond
///   `SELECT` (`SELECT` only, or nothing);
/// - grants it no membership through which it can inherit the privileges
///   of, or set its role to, a declared library role, the owner of the
///   library's tables or `pg_write_all_data` (no membership in any of
///   them, nor in a role that holds one);
/// - on a server before Postgres 15 with the library in the `public`
///   schema, revokes `CREATE` on it from `PUBLIC`.
///
/// The database then refuses the application role every write to a library
/// table, and a grant that would allow one makes `PostgresBackend.open`
/// refuse the database.
const Map<String, Set<String>> postgresRuntimeRoleGrants =
    <String, Set<String>>{
      'events': <String>{'SELECT', 'INSERT'},
      'view_rows': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'view_target_versions': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'fifo_entries': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'backend_state': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'security_context': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'idempotency': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'library_roles': <String>{'SELECT'},
    };
