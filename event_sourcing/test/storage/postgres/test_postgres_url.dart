// The Postgres test database: PG_TEST_URL, and the roles and schema every
// Postgres-gated test runs the library under. This file declares no tests,
// so it carries no citation.

import 'dart:io' show Platform, pid;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart' show addTearDown;

/// Returns the Postgres URL the conformance harness should connect to,
/// or `null` when the test environment has not provided one. Tests that
/// receive `null` SHALL skip themselves rather than fail.
String? testPostgresUrl() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) return null;
  return url;
}

/// [name] quoted as a Postgres identifier.
String quoteIdent(String name) => '"${name.replaceAll('"', '""')}"';

/// [url] with its credentials replaced by [role]'s (every test role's
/// password is `evs`).
String postgresUrlAsRole(String url, String role) =>
    Uri.parse(url).replace(userInfo: '$role:evs').toString();

/// Opens a connection to [url] without SSL.
Future<Connection> connectPostgres(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

/// A library database set up as a deployment sets one up, on the server
/// PG_TEST_URL reaches.
///
/// Three roles of this process -- the owner, the runtime role and the lock
/// role -- and one schema the owner owns. The schema is provisioned as the
/// owner, declaring the runtime role as a runtime and a lock role and the
/// lock role as a lock role; the runtime role holds `USAGE` on it and exactly
/// `postgresRuntimeRoleGrants`, and the lock role holds `USAGE` and the
/// runtime role's privileges on `backend_state`. The library opens as the
/// runtime role. No role's search path is set: the library sets its own in
/// every transaction.
///
/// The administrative role of PG_TEST_URL creates the roles and the schema
/// and runs a test's own raw SQL ([connectAdmin]); it must be able to
/// create roles. Roles are cluster-global, so every name carries [tag] and
/// this process's id.
class PostgresTestDatabase {
  PostgresTestDatabase(this.adminUrl, {this.tag = 'lib', String? schema})
    : schema = schema ?? 'evs_${tag}_$pid';

  /// A database over PG_TEST_URL, or null when it is not set.
  static PostgresTestDatabase? fromEnvironment({
    String tag = 'lib',
    String? schema,
  }) {
    final url = testPostgresUrl();
    return url == null
        ? null
        : PostgresTestDatabase(url, tag: tag, schema: schema);
  }

  /// PG_TEST_URL: the administrative role.
  final String adminUrl;

  /// Distinguishes the roles and the schema of several databases in one
  /// process.
  final String tag;

  /// The schema holding the library's tables.
  final String schema;

  String get owner => 'evs_${tag}_owner_$pid';
  String get runtime => 'evs_${tag}_runtime_$pid';
  String get lock => 'evs_${tag}_lock_$pid';

  String get ownerUrl => postgresUrlAsRole(adminUrl, owner);
  String get runtimeUrl => postgresUrlAsRole(adminUrl, runtime);
  String get lockUrl => postgresUrlAsRole(adminUrl, lock);

  List<String> get _roles => <String>[lock, runtime, owner];

  bool _rolesCreated = false;

  /// Creates the roles afresh, dropping any a previous run of this process
  /// id left behind.
  Future<void> createRoles() async {
    await asAdmin((admin) async {
      await _dropAll(admin);
      for (final role in _roles.reversed) {
        await admin.execute(
          "CREATE ROLE ${quoteIdent(role)} LOGIN PASSWORD 'evs' "
          'NOSUPERUSER NOCREATEDB NOCREATEROLE',
        );
      }
    });
    _rolesCreated = true;
  }

  /// Drops the schema and the roles.
  Future<void> drop() async {
    await asAdmin(_dropAll);
    _rolesCreated = false;
  }

  Future<void> _dropAll(Connection admin) async {
    await admin.execute('DROP SCHEMA IF EXISTS ${quoteIdent(schema)} CASCADE');
    for (final role in _roles) {
      final exists = await admin.execute(
        Sql.named('SELECT 1 FROM pg_roles WHERE rolname = @r'),
        parameters: <String, Object?>{'r': role},
      );
      if (exists.isEmpty) continue;
      await admin.execute('DROP OWNED BY ${quoteIdent(role)}');
      await admin.execute('DROP ROLE ${quoteIdent(role)}');
    }
  }

  /// Recreates the schema, empty, as the owner's: `CREATE` revoked from
  /// `PUBLIC`, `USAGE` granted to the runtime and lock roles. With
  /// [provision] it is then provisioned as the owner and the privileges are
  /// granted ([grant]). Creates the roles first when this process has not.
  Future<void> reset({
    bool provision = false,
    Map<String, Set<String>> grants = postgresRuntimeRoleGrants,
  }) async {
    if (!_rolesCreated) await createRoles();
    final s = quoteIdent(schema);
    await asAdmin((admin) async {
      await admin.execute('DROP SCHEMA IF EXISTS $s CASCADE');
      await admin.execute(
        'CREATE SCHEMA $s AUTHORIZATION ${quoteIdent(owner)}',
      );
      await admin.execute('REVOKE CREATE ON SCHEMA $s FROM PUBLIC');
      await admin.execute('GRANT USAGE ON SCHEMA $s TO ${quoteIdent(runtime)}');
      await admin.execute('GRANT USAGE ON SCHEMA $s TO ${quoteIdent(lock)}');
    });
    if (provision) {
      await this.provision(grant: false);
      await grant(grants);
    }
  }

  /// Provisions the schema as the owner, declaring [runtimeRoles] (the
  /// runtime role by default) and [lockRoles] (the runtime and lock roles
  /// by default, since a backend opened without a lock URL runs its lock
  /// session as the runtime role), and, with [grant], grants the runtime
  /// and lock roles their privileges ([grant]) on the tables it holds.
  Future<void> provision({
    bool grant = true,
    Duration bootLockWait = const Duration(seconds: 60),
    Set<String>? runtimeRoles,
    Set<String>? lockRoles,
  }) async {
    await PostgresBackend.provision(
      ownerUrl,
      schema: schema,
      runtimeRoles: runtimeRoles ?? <String>{runtime},
      lockRoles: lockRoles ?? <String>{runtime, lock},
      sslMode: SslMode.disable,
      bootLockWait: bootLockWait,
    );
    if (grant) await this.grant();
  }

  /// Grants the runtime role [grants] and the lock role the runtime role's
  /// privileges on `backend_state`, on every table the schema holds.
  Future<void> grant([
    Map<String, Set<String>> grants = postgresRuntimeRoleGrants,
  ]) => asAdmin((admin) async {
    final s = quoteIdent(schema);
    for (final MapEntry(key: table, value: privileges) in grants.entries) {
      if (privileges.isEmpty) continue;
      await admin.execute(
        'GRANT ${privileges.join(', ')} ON $s.${quoteIdent(table)} '
        'TO ${quoteIdent(runtime)}',
      );
    }
    await admin.execute(
      'GRANT ${postgresRuntimeRoleGrants['backend_state']!.join(', ')} '
      'ON $s.backend_state TO ${quoteIdent(lock)}',
    );
  });

  /// Opens a backend as the runtime role. With [provision] the schema is
  /// first provisioned as the owner and the privileges granted (a no-op on
  /// a provisioned schema). The lock session runs as the runtime role
  /// unless [lockUrl] names another.
  Future<PostgresBackend> open({
    bool provision = false,
    String? lockUrl,
    Duration lockQueryTimeout = const Duration(seconds: 5),
    Duration lockHeartbeat = const Duration(seconds: 5),
    Duration bootLockWait = const Duration(seconds: 60),
  }) async {
    if (provision) await this.provision(bootLockWait: bootLockWait);
    return PostgresBackend.open(
      url: runtimeUrl,
      schema: schema,
      lockUrl: lockUrl,
      sslMode: SslMode.disable,
      lockQueryTimeout: lockQueryTimeout,
      lockHeartbeat: lockHeartbeat,
      bootLockWait: bootLockWait,
    );
  }

  /// Opens an administrative connection whose search path is the schema,
  /// for a test's own raw SQL.
  Future<Connection> connectAdmin() => _connectInSchema(adminUrl);

  /// Opens a connection of the owner's whose search path is the schema, for
  /// a test's own raw SQL as the owner.
  Future<Connection> connectOwner() => _connectInSchema(ownerUrl);

  /// Opens a connection of the runtime role's whose search path is the
  /// schema, for a test's own raw SQL as the runtime role.
  Future<Connection> connectRuntime() => _connectInSchema(runtimeUrl);

  Future<Connection> _connectInSchema(String url) async {
    final c = await connectPostgres(url);
    try {
      await c.execute('SET search_path TO ${quoteIdent(schema)}');
    } catch (_) {
      await c.close();
      rethrow;
    }
    return c;
  }

  /// Runs [body] on an administrative connection.
  Future<T> asAdmin<T>(Future<T> Function(Connection admin) body) async {
    final c = await connectPostgres(adminUrl);
    try {
      return await body(c);
    } finally {
      await c.close();
    }
  }

  /// Runs [body] on a connection of [role]'s.
  Future<T> asRole<T>(
    String role,
    Future<T> Function(Connection c) body,
  ) async {
    final c = await connectPostgres(postgresUrlAsRole(adminUrl, role));
    try {
      return await body(c);
    } finally {
      await c.close();
    }
  }
}

/// The idempotency store the library builds over [backend]: the one an event
/// store opened over it hands out. The event store is opened with the
/// test-only open, so it appends no library-version event, and it closes
/// when the calling test ends; [backend] stays the caller's to close.
Future<IdempotencyStore> idempotencyStoreOver(PostgresBackend backend) async {
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry(),
    source: const Source(
      hopId: 'idempotency-test',
      identifier: 'idempotency-test-install',
      softwareVersion: 'test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
  );
  addTearDown(store.close);
  return store.idempotencyStore!;
}
