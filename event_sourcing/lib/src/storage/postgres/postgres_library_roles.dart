// Implements: EVS-DEV-postgres-backend/P
// provisioning records the declared runtime and lock roles in the
//   owner-only `library_roles` table.
// Implements: EVS-DEV-postgres-backend/M+N
// the catalog reads open refuses a database by: grants on the library's
//   tables, columns, sequences and schema, memberships walked transitively,
//   ownership and role attributes.
import 'package:event_sourcing/src/storage/postgres/postgres_exceptions.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresLibraryTables;
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart';

/// The kind of a declared library role: `runtime` for a role the pool
/// connects as, `lock` for a role the lock session connects as.
@internal
const String libraryRoleRuntime = 'runtime';

/// See [libraryRoleRuntime].
@internal
const String libraryRoleLock = 'lock';

/// Throws [ArgumentError] unless [runtimeRoles] and [lockRoles] each name at
/// least one role and every name is non-empty.
@internal
void checkDeclaredLibraryRoles(
  Set<String> runtimeRoles,
  Set<String> lockRoles,
) {
  for (final (name, roles) in <(String, Set<String>)>[
    ('runtimeRoles', runtimeRoles),
    ('lockRoles', lockRoles),
  ]) {
    if (roles.isEmpty) {
      throw ArgumentError.value(
        roles,
        name,
        'declare at least one role: open refuses every role not declared',
      );
    }
    if (roles.any((r) => r.isEmpty || r.contains('\u0000'))) {
      throw ArgumentError.value(
        roles,
        name,
        'a role name is non-empty and holds no NUL character',
      );
    }
  }
}

/// Replaces, in a provisioning transaction [tx] run as the owner, the
/// declared library roles with [runtimeRoles] and [lockRoles].
@internal
Future<void> recordDeclaredLibraryRoles(
  Session tx, {
  required Set<String> runtimeRoles,
  required Set<String> lockRoles,
}) async {
  await tx.execute('DELETE FROM library_roles');
  for (final (kind, roles) in <(String, Set<String>)>[
    (libraryRoleRuntime, runtimeRoles),
    (libraryRoleLock, lockRoles),
  ]) {
    for (final role in roles) {
      await tx.execute(
        Sql.named(
          'INSERT INTO library_roles (role_name, kind) VALUES (@r, @k)',
        ),
        parameters: <String, Object?>{'r': role, 'k': kind},
      );
    }
  }
}

/// The role names a session connects and runs as: its session user and its
/// current user (one name when they are the same).
@internal
Future<Set<String>> readSessionRoles(Session session) async {
  final row = (await session.execute(
    'SELECT session_user::text, current_user::text',
  )).first;
  return <String>{row[0]! as String, row[1]! as String};
}

// The common table expressions every check below starts from, run inside a
// library transaction whose current schema is the library's: the schema,
// the owners of the library's tables, the declared roles, and the roles the
// library admits (the owners and the declared roles).
const String _scope = '''
WITH RECURSIVE lib AS (
  SELECT n.oid FROM pg_catalog.pg_namespace n
  WHERE n.nspname = pg_catalog.current_schema()
),
owners AS (
  SELECT DISTINCT c.relowner AS oid
  FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
  WHERE c.relkind IN ('r', 'p') AND c.relname = ANY(@tables)
),
declared AS (
  SELECT r.oid FROM pg_catalog.pg_roles r
  WHERE r.rolname IN (SELECT role_name FROM library_roles)
)''';

// A membership through which its member can act as the role: it inherits
// the role's privileges, or it can set its role to it. A membership held
// only with the admin option does neither.
const String _actsAs = '(m.inherit_option OR m.set_option)';

// The relation kinds a library table can be.
const String _tableKinds = "('r', 'p', 'v', 'm', 'f')";

// The grantee's name, `PUBLIC` for the grantee 0.
const String _granteeName =
    "CASE WHEN a.grantee = 0 THEN 'PUBLIC' "
    'ELSE pg_catalog.pg_get_userbyid(a.grantee)::text END';

// A write privilege on a table of the library's schema held by a role that
// is neither an owner nor, except on the declared roles' table, a declared
// role.
const String _tableWriteGrants =
    '''
$_scope
SELECT $_granteeName, a.privilege_type, 'on table ' || c.relname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
WHERE c.relkind IN $_tableKinds
  AND a.privilege_type IN
    ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'TRIGGER', 'REFERENCES')
  AND a.grantee NOT IN (SELECT oid FROM owners)
  AND (c.relname = 'library_roles'
       OR a.grantee NOT IN (SELECT oid FROM declared))
''';

// A write privilege on a column of a table of the library's schema, held as
// the table write grants above.
const String _columnWriteGrants =
    '''
$_scope
SELECT $_granteeName, a.privilege_type,
  'on column ' || c.relname || '.' || att.attname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
JOIN pg_catalog.pg_attribute att
  ON att.attrelid = c.oid AND att.attnum > 0 AND NOT att.attisdropped
CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
WHERE c.relkind IN $_tableKinds
  AND a.privilege_type IN ('INSERT', 'UPDATE', 'REFERENCES')
  AND a.grantee NOT IN (SELECT oid FROM owners)
  AND (c.relname = 'library_roles'
       OR a.grantee NOT IN (SELECT oid FROM declared))
''';

// `USAGE` or `UPDATE` on a sequence of the library's schema, held by a role
// that is neither an owner nor a declared role.
const String _sequenceGrants =
    '''
$_scope
SELECT $_granteeName, a.privilege_type, 'on sequence ' || c.relname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
WHERE c.relkind = 'S'
  AND a.privilege_type IN ('USAGE', 'UPDATE')
  AND a.grantee NOT IN (SELECT oid FROM owners)
  AND a.grantee NOT IN (SELECT oid FROM declared)
''';

// Any privilege of `PUBLIC` on a table of the library's schema or on a
// column of one.
const String _publicTableGrants =
    '''
$_scope
SELECT 'PUBLIC', a.privilege_type, 'on table ' || c.relname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
WHERE c.relkind IN $_tableKinds AND a.grantee = 0
UNION ALL
SELECT 'PUBLIC', a.privilege_type,
  'on column ' || c.relname || '.' || att.attname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
JOIN pg_catalog.pg_attribute att
  ON att.attrelid = c.oid AND att.attnum > 0 AND NOT att.attisdropped
CROSS JOIN LATERAL pg_catalog.aclexplode(att.attacl) a
WHERE c.relkind IN $_tableKinds AND a.grantee = 0
''';

// `CREATE` on the library's schema held by any role but an owner, `PUBLIC`
// included. A schema with no explicit privileges grants its owner's
// defaults.
const String _schemaCreateGrants =
    '''
$_scope
SELECT $_granteeName, a.privilege_type, 'on schema ' || n.nspname
FROM pg_catalog.pg_namespace n JOIN lib ON n.oid = lib.oid
CROSS JOIN LATERAL pg_catalog.aclexplode(
  coalesce(n.nspacl, pg_catalog.acldefault('n', n.nspowner))) a
WHERE a.privilege_type = 'CREATE'
  AND a.grantee NOT IN (SELECT oid FROM owners)
''';

// A role that is neither an owner nor a declared role and can act, through
// a chain of memberships, as `pg_write_all_data`, an owner or a declared
// role.
const String _foreignMemberships =
    '''
$_scope,
targets AS (
  SELECT oid FROM owners
  UNION SELECT oid FROM declared
  UNION SELECT r.oid FROM pg_catalog.pg_roles r
        WHERE r.rolname = 'pg_write_all_data'
),
up(member, target) AS (
  SELECT m.member, m.roleid FROM pg_catalog.pg_auth_members m
  WHERE m.roleid IN (SELECT oid FROM targets) AND $_actsAs
  UNION
  SELECT m.member, up.target
  FROM pg_catalog.pg_auth_members m JOIN up ON m.roleid = up.member
  WHERE $_actsAs
)
SELECT pg_catalog.pg_get_userbyid(up.member)::text, 'MEMBER',
  'of role ' || pg_catalog.pg_get_userbyid(up.target)::text
FROM up
WHERE up.member NOT IN (SELECT oid FROM owners)
  AND up.member NOT IN (SELECT oid FROM declared)
''';

// What the role @role owns in the library's schema: the schema itself, or a
// relation in it.
const String _ownedByRole =
    '''
$_scope
SELECT 'of schema ' || n.nspname
FROM pg_catalog.pg_namespace n JOIN lib ON n.oid = lib.oid
WHERE pg_catalog.pg_get_userbyid(n.nspowner) = @role
UNION ALL
SELECT 'of ' || CASE WHEN c.relkind IN ('r', 'p') THEN 'table '
                     WHEN c.relkind = 'S' THEN 'sequence '
                     ELSE 'relation ' END || c.relname
FROM pg_catalog.pg_class c JOIN lib ON c.relnamespace = lib.oid
WHERE pg_catalog.pg_get_userbyid(c.relowner) = @role
''';

// Every role @role can act as through a chain of memberships, itself
// included, with its attributes and whether it owns a library table.
const String _reachedByRole =
    '''
$_scope,
reach(oid) AS (
  SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = @role
  UNION
  SELECT m.roleid
  FROM pg_catalog.pg_auth_members m JOIN reach ON m.member = reach.oid
  WHERE $_actsAs
)
SELECT r.rolname::text, r.rolsuper, r.rolcreaterole,
  r.oid IN (SELECT oid FROM owners)
FROM reach JOIN pg_catalog.pg_roles r ON r.oid = reach.oid
''';

/// Every reason to refuse the database, read in [tx], a library transaction
/// on the pool whose current schema is the library's: [poolRoles] and
/// [lockRoles] are the roles the pool and the lock session connect and run
/// as.
// Implements: EVS-DEV-postgres-backend/P
// a pool role that is not a declared runtime role, and a lock role that is
//   not a declared lock role, are refused.
// Implements: EVS-DEV-postgres-backend/N
// a pool or lock role that owns the schema or one of its tables, can act as
//   the owner of the library's tables, or reaches SUPERUSER or CREATEROLE
//   through a membership it can inherit or set is refused.
// Implements: EVS-DEV-postgres-backend/M
// a write grant on a library table, column or sequence, or a membership
//   able to act as pg_write_all_data, the owner or a declared role, held by
//   a role outside the owner and the declared roles; any privilege of
//   PUBLIC on a library table; and CREATE on the schema for any role but
//   the owner, are refused.
// Implements: EVS-PRD-storage-barrier/F
// a database on which a role outside the owner and the declared roles may
//   write a library table, or act as one of those roles, is refused.
@internal
Future<List<PostgresRoleRefusal>> findLibraryRoleRefusals(
  Session tx, {
  required Set<String> poolRoles,
  required Set<String> lockRoles,
}) async {
  final tables = TypedValue<List<String>>(
    Type.textArray,
    postgresLibraryTables,
  );
  final refusals = <String, PostgresRoleRefusal>{};
  void refuse(String role, String privilege, String detail) {
    final refusal = PostgresRoleRefusal(
      role: role,
      privilege: privilege,
      detail: detail,
    );
    refusals.putIfAbsent(refusal.toString(), () => refusal);
  }

  final declared = <String, Set<String>>{};
  for (final row in await tx.execute(
    'SELECT role_name, kind FROM library_roles',
  )) {
    (declared[row[1]! as String] ??= <String>{}).add(row[0]! as String);
  }
  for (final (kind, roles) in <(String, Set<String>)>[
    (libraryRoleRuntime, poolRoles),
    (libraryRoleLock, lockRoles),
  ]) {
    for (final role in roles) {
      if (!(declared[kind]?.contains(role) ?? false)) {
        refuse(
          role,
          'UNDECLARED',
          'as a $kind role: provisioning did not declare it one (declare '
              'the pool role as a lock role too when the lock session runs '
              'as it)',
        );
      }
    }
  }

  for (final role in <String>{...poolRoles, ...lockRoles}) {
    final parameters = <String, Object?>{'tables': tables, 'role': role};
    for (final row in await tx.execute(
      Sql.named(_ownedByRole),
      parameters: parameters,
    )) {
      refuse(role, 'OWNER', row[0]! as String);
    }
    for (final row in await tx.execute(
      Sql.named(_reachedByRole),
      parameters: parameters,
    )) {
      final reached = row[0]! as String;
      final through = reached == role ? 'directly' : 'through role $reached';
      if (row[1] == true) refuse(role, 'SUPERUSER', through);
      if (row[2] == true) refuse(role, 'CREATEROLE', through);
      if (row[3] == true && reached != role) {
        refuse(role, 'MEMBER', 'of role $reached, the owner of the tables');
      }
    }
  }

  for (final query in <String>[
    _tableWriteGrants,
    _columnWriteGrants,
    _sequenceGrants,
    _publicTableGrants,
    _schemaCreateGrants,
    _foreignMemberships,
  ]) {
    for (final row in await tx.execute(
      Sql.named(query),
      parameters: <String, Object?>{'tables': tables},
    )) {
      refuse(row[0]! as String, row[1]! as String, row[2]! as String);
    }
  }
  return refusals.values.toList();
}
