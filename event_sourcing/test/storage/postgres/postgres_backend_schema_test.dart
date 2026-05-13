// Verifies: EVS-DEV-postgres-backend/A — PostgresBackend.open emits the
// schema DDL (every expected CREATE TABLE) and is idempotent on re-open
// (the second open against a provisioned database is a no-op on the
// schema). Both tests gated on PG_TEST_URL; tests skip themselves when
// PG_TEST_URL is unset so they're inert in CI/dev environments that
// don't have a Postgres available.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }

  group('PostgresBackend schema', () {
    setUp(() async {
      // Clean slate: drop+recreate public schema so each test sees an
      // empty database. Done outside any PostgresBackend to keep the
      // DDL-emission path itself under test. The two statements are
      // issued separately because the postgres client's extended-query
      // protocol rejects multi-statement strings.
      final conn = await _connect(url);
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
    });

    test('open() emits CREATE TABLE for every expected table', () async {
      final backend = await PostgresBackend.open(url: url);
      addTearDown(backend.close);

      final conn = await _connect(url);
      addTearDown(conn.close);

      final tables = await _listPublicTables(conn);
      expect(
        tables,
        containsAll(<String>[
          'events',
          'view_rows',
          'view_target_versions',
          'fifo_entries',
          'backend_state',
          'security_context',
          'idempotency',
        ]),
      );
    });

    test('open() is idempotent (second open is a no-op on schema)', () async {
      final b1 = await PostgresBackend.open(url: url);
      await b1.close();
      final b2 = await PostgresBackend.open(url: url);
      addTearDown(b2.close);
      // No throw is the assertion. Implicitly: second open SHALL not
      // raise "relation already exists".
    });
  });
}

// TODO(CUR-1330): once @visibleForTesting'd, replace with
// PostgresBackend.endpointFromUrl + Connection.open(...).
Future<Connection> _connect(String url) async {
  final uri = Uri.parse(url);
  final userInfoParts = uri.userInfo.isEmpty
      ? const <String>[]
      : uri.userInfo.split(':');
  return Connection.open(
    Endpoint(
      host: uri.host,
      port: uri.port == 0 ? 5432 : uri.port,
      database: uri.pathSegments.isEmpty ? '' : uri.pathSegments.first,
      username: userInfoParts.isEmpty ? null : userInfoParts.first,
      password: userInfoParts.length < 2
          ? null
          : userInfoParts.sublist(1).join(':'),
    ),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
}

Future<List<String>> _listPublicTables(Connection conn) async {
  final result = await conn.execute(
    'SELECT table_name FROM information_schema.tables '
    "WHERE table_schema = 'public' ORDER BY table_name",
  );
  return result.map((row) => row[0]! as String).toList();
}
