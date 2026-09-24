// Implements: EVS-DEV-postgres-backend/K
// the privileges the Postgres runtime role needs on each table the
//   library provisions, stated once for the deployment's grants and for the
//   documentation.

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
/// and the owner-only operations (provisioning, and changing or dropping
/// the queue table's guard) stay with the owning role. The runtime role
/// must not be able to become the owner: it owns nothing in the schema, is
/// not a member of the owning role, holds neither `SUPERUSER` nor
/// `CREATEROLE` nor membership in a role that carries them, and has no
/// `CREATE` on the schema. For a lock connection opened as another role
/// (`PostgresBackend.open`'s `lockUrl`), `USAGE` on the schema and these
/// privileges on `backend_state` suffice.
const Map<String, Set<String>> postgresRuntimeRoleGrants =
    <String, Set<String>>{
      'events': <String>{'SELECT', 'INSERT'},
      'view_rows': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'view_target_versions': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'fifo_entries': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'backend_state': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'security_context': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
      'idempotency': <String>{'SELECT', 'INSERT', 'UPDATE', 'DELETE'},
    };
