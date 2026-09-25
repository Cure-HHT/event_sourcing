// The demo's Postgres deployment step: the part a deployment runs as the
// role that owns the schema before its servers start. It creates the schema,
// provisions it declaring the roles the servers connect as, and grants those
// roles their privileges. Application code; cites no library requirement.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';

/// The schema the demo keeps the library's tables in.
const String demoPostgresSchema = 'demo';

/// The runtime role the compose file creates and the demo's servers connect
/// as.
const String demoPostgresRuntimeRole = 'evs_runtime';

String _quote(String name) => '"${name.replaceAll('"', '""')}"';

/// The endpoint of a `postgres://user:password@host:port/database` URL.
Endpoint _endpoint(String url) {
  final uri = Uri.parse(url);
  final user = uri.userInfo.split(':');
  return Endpoint(
    host: uri.host,
    port: uri.hasPort ? uri.port : 5432,
    database: uri.pathSegments.isEmpty ? '' : uri.pathSegments.first,
    username: uri.userInfo.isEmpty ? null : Uri.decodeComponent(user.first),
    password: user.length < 2
        ? null
        : Uri.decodeComponent(user.sublist(1).join(':')),
  );
}

/// Runs the deployment step on the database at [url], as its role, which
/// becomes the owner of [schema] and of the library's tables:
///
/// 1. creates [schema] when it does not exist, grants `CREATE` on it to no
///    role but the owner, and grants `USAGE` on it to every role in
///    [runtimeRoles] and [lockRoles];
/// 2. provisions it (`PostgresBackend.provision`), declaring [runtimeRoles]
///    and [lockRoles];
/// 3. grants each runtime role `postgresRuntimeRoleGrants` and each lock
///    role the runtime role's privileges on `backend_state`.
///
/// The roles must exist; creating them is the database administrator's
/// step (the compose file's init script creates [demoPostgresRuntimeRole]).
Future<void> runDemoPostgresDeploymentStep({
  required String url,
  required String schema,
  required Set<String> runtimeRoles,
  required Set<String> lockRoles,
  String? lockUrl,
  SslMode sslMode = SslMode.require,
}) async {
  final s = _quote(schema);
  Future<void> asOwner(Future<void> Function(Connection c) body) async {
    final c = await Connection.open(
      _endpoint(url),
      settings: ConnectionSettings(sslMode: sslMode),
    );
    try {
      await body(c);
    } finally {
      await c.close();
    }
  }

  await asOwner((c) async {
    await c.execute('CREATE SCHEMA IF NOT EXISTS $s');
    await c.execute('REVOKE CREATE ON SCHEMA $s FROM PUBLIC');
    for (final role in <String>{...runtimeRoles, ...lockRoles}) {
      await c.execute('GRANT USAGE ON SCHEMA $s TO ${_quote(role)}');
    }
  });
  await PostgresBackend.provision(
    url,
    schema: schema,
    runtimeRoles: runtimeRoles,
    lockRoles: lockRoles,
    lockUrl: lockUrl,
    sslMode: sslMode,
  );
  await asOwner((c) async {
    for (final role in runtimeRoles) {
      for (final MapEntry(key: table, value: privileges)
          in postgresRuntimeRoleGrants.entries) {
        await c.execute(
          'GRANT ${privileges.join(', ')} ON $s.${_quote(table)} '
          'TO ${_quote(role)}',
        );
      }
    }
    for (final role in lockRoles) {
      await c.execute(
        'GRANT ${postgresRuntimeRoleGrants['backend_state']!.join(', ')} '
        'ON $s.backend_state TO ${_quote(role)}',
      );
    }
  });
}
