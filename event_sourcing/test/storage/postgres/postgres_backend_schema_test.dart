// Verifies: EVS-DEV-postgres-backend/A — PostgresBackend.open emits the
// schema DDL (every expected CREATE TABLE) and is idempotent on re-open
// (the second open against a provisioned database is a no-op on the
// schema). Both tests gated on PG_TEST_URL; tests skip themselves when
// PG_TEST_URL is unset so they're inert in CI/dev environments that
// don't have a Postgres available.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show ensurePostgresSchema;
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
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
      );
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
      final b1 = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
      await b1.close();
      final b2 = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
      addTearDown(b2.close);
      // No throw is the assertion. Implicitly: second open SHALL not
      // raise "relation already exists".
    });

    test('ensurePostgresSchema backfills raw_input_canonical_json on a '
        'pre-existing idempotency table (idempotent ALTER)', () async {
      // Provision the idempotency table without raw_input_canonical_json,
      // simulating a database provisioned before that column was added.
      // CREATE TABLE IF NOT EXISTS in ensurePostgresSchema is a no-op against
      // a pre-existing table, so ensurePostgresSchema must backfill the column
      // via an idempotent ALTER — otherwise lookup()'s SELECT of it would raise
      // "column does not exist".
      final conn = await _connect(url);
      addTearDown(conn.close);
      await conn.execute('''
          CREATE TABLE idempotency (
            action_name      TEXT         NOT NULL,
            principal_id     TEXT         NOT NULL,
            idempotency_key  TEXT         NOT NULL,
            result_json      JSONB        NOT NULL,
            emitted_event_ids JSONB       NOT NULL,
            recorded_at      TIMESTAMPTZ  NOT NULL,
            expires_at       TIMESTAMPTZ  NOT NULL,
            PRIMARY KEY (action_name, principal_id, idempotency_key)
          )
        ''');

      // Run the schema ensure (idempotent ALTER backfills the column).
      await conn.runTx(ensurePostgresSchema);

      // The column now exists.
      final cols = await conn.execute(
        'SELECT column_name FROM information_schema.columns '
        "WHERE table_schema = 'public' AND table_name = 'idempotency'",
      );
      final names = cols.map((r) => r[0]! as String).toSet();
      expect(names, contains('raw_input_canonical_json'));

      // And a lookup that SELECTs the column succeeds (no "column does
      // not exist") rather than erroring — a clean miss on empty data.
      final pool = Pool<void>.withEndpoints(
        [PostgresBackend.endpointFromUrl(url)],
        settings: const PoolSettings(
          maxConnectionCount: 1,
          sslMode: SslMode.disable,
        ),
      );
      addTearDown(pool.close);
      final store = PostgresIdempotencyStore.over(pool);
      final hit = await store.lookup('a', 'p', 'k');
      expect(hit, isNull);
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
