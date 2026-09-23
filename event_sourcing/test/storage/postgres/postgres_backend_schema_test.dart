// Verifies: EVS-DEV-postgres-backend/G
// provisioning creates every table the backend reads or writes.
// Verifies: EVS-DEV-postgres-backend/B
// view_rows stored as a single JSONB-blob
//   table keyed by (view_name, row_key).
// Both tests are gated on PG_TEST_URL and skip themselves when it is unset.

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
      // empty database, provisioned by the test itself. The two statements are
      // issued separately because the postgres client's extended-query
      // protocol rejects multi-statement strings.
      final conn = await _connect(url);
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
    });

    test('provisioning creates every expected table', () async {
      await PostgresBackend.provision(url, sslMode: SslMode.disable);

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

    test('view_rows has the JSONB-blob shape with composite PK', () async {
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
      addTearDown(backend.close);
      final conn = await _connect(url);
      addTearDown(conn.close);

      // Columns + types.
      final cols = await conn.execute(
        'SELECT column_name, data_type FROM information_schema.columns '
        "WHERE table_schema = 'public' AND table_name = 'view_rows'",
      );
      final types = {for (final r in cols) r[0]! as String: r[1]! as String};
      expect(types['view_name'], 'text');
      expect(types['row_key'], 'text');
      expect(types['row_data'], 'jsonb');
      expect(types['updated_at'], 'timestamp with time zone');

      // Primary key is exactly (view_name, row_key) IN THAT ORDER. Order by
      // the index's own key ordinality (unnest WITH ORDINALITY over indkey),
      // not alphabetically — composite-PK column order is semantically
      // load-bearing and an alphabetical sort would mask a wrong definition.
      final pk = await conn.execute(
        'SELECT a.attname FROM pg_index i '
        'JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true '
        'JOIN pg_attribute a '
        '  ON a.attrelid = i.indrelid AND a.attnum = k.attnum '
        "WHERE i.indrelid = 'view_rows'::regclass AND i.indisprimary "
        'ORDER BY k.ord',
      );
      final pkCols = pk.map((r) => r[0]! as String).toList();
      expect(pkCols, ['view_name', 'row_key']);
    });
  });
}

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

Future<List<String>> _listPublicTables(Connection conn) async {
  final result = await conn.execute(
    'SELECT table_name FROM information_schema.tables '
    "WHERE table_schema = 'public' ORDER BY table_name",
  );
  return result.map((row) => row[0]! as String).toList();
}
