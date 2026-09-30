// The demo's Postgres test database over PG_TEST_URL, set up as the demo's
// deployment step sets it up: PG_TEST_URL's role owns the schema and
// provisions it, declaring the runtime role, and the servers connect as that
// role. Declares no tests, so it carries no citation.

import 'dart:io' show Platform;

import 'package:action_permissions_demo/server/postgres_setup.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';

/// The demo database over PG_TEST_URL, whose role must be able to create
/// roles; it provisions, and so owns the schema and the library's tables.
class DemoPostgres {
  DemoPostgres(this.adminUrl);

  /// The database over PG_TEST_URL, or null when it is not set.
  static DemoPostgres? fromEnvironment() {
    final url = Platform.environment['PG_TEST_URL'];
    return url == null || url.isEmpty ? null : DemoPostgres(url);
  }

  /// PG_TEST_URL: the role that provisions and owns the schema.
  final String adminUrl;

  /// The schema the demo keeps the library's tables in.
  String get schema => demoPostgresSchema;

  /// The declared runtime role the servers connect as.
  String get runtimeRole => demoPostgresRuntimeRole;

  /// [adminUrl] with the runtime role's credentials (password `evs`, as
  /// the compose file's init script creates it).
  String get runtimeUrl =>
      Uri.parse(adminUrl).replace(userInfo: '$runtimeRole:evs').toString();

  /// The command-line options of the demo server's `--provision` for this
  /// database.
  List<String> get provisionArgs => <String>[
    '--backend=postgres',
    '--postgres-url=$adminUrl',
    '--postgres-ssl-mode=disable',
    '--provision',
    '--postgres-runtime-role=$runtimeRole',
    '--postgres-lock-role=$runtimeRole',
  ];

  /// Creates the runtime role when it does not exist and drops the schema;
  /// with [provision], then runs the demo's deployment step.
  Future<void> reset({bool provision = true}) async {
    final c = await connectAdmin(inSchema: false);
    try {
      final exists = await c.execute(
        Sql.named('SELECT 1 FROM pg_roles WHERE rolname = @r'),
        parameters: <String, Object?>{'r': runtimeRole},
      );
      if (exists.isEmpty) {
        await c.execute(
          'CREATE ROLE "$runtimeRole" LOGIN PASSWORD \'evs\' '
          'NOSUPERUSER NOCREATEDB NOCREATEROLE',
        );
      }
      await c.execute('DROP SCHEMA IF EXISTS "$schema" CASCADE');
    } finally {
      await c.close();
    }
    if (provision) await this.provision();
  }

  /// Runs the demo's deployment step as the owner.
  Future<void> provision() => runDemoPostgresDeploymentStep(
    url: adminUrl,
    schema: schema,
    runtimeRoles: <String>{runtimeRole},
    lockRoles: <String>{runtimeRole},
    sslMode: SslMode.disable,
  );

  /// Opens a backend as the runtime role.
  Future<PostgresBackend> open() => PostgresBackend.open(
    url: runtimeUrl,
    schema: schema,
    sslMode: SslMode.disable,
  );

  /// Opens a connection as the owner, whose search path is the schema
  /// unless [inSchema] is false.
  Future<Connection> connectAdmin({bool inSchema = true}) async {
    final c = await Connection.open(
      PostgresBackend.endpointFromUrl(adminUrl),
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    if (inSchema) {
      try {
        await c.execute('SET search_path TO "$schema"');
      } catch (_) {
        await c.close();
        rethrow;
      }
    }
    return c;
  }
}
