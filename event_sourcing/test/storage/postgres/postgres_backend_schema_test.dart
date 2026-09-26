// Verifies: EVS-DEV-postgres-backend/G
// provisioning creates every table the backend reads or writes.
// Verifies: EVS-DEV-postgres-backend/B
// view_rows stored as a single JSONB-blob
//   table keyed by (view_name, row_key).
// Both tests are gated on PG_TEST_URL and skip themselves when it is unset.

@TestOn('vm')
library;

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  tearDownAll(db.drop);

  group('PostgresBackend schema', () {
    setUp(() async {
      // Clean slate: drop and recreate the test schema so each test sees an
      // empty one, provisioned by the test itself. The admin connection's
      // search path is that schema, so current_schema() names it.
      await db.reset();
    });

    test('provisioning creates every expected table', () async {
      await db.provision();

      final conn = await db.connectAdmin();
      addTearDown(conn.close);

      final tables = await _listSchemaTables(conn);
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

    // Verifies: EVS-DEV-postgres-backend/G
    // Verifies: EVS-DEV-chain-verification/B
    // Verifies: EVS-DEV-causal-parents/H
    test('provisioning creates the non-unique event-table indexes the chain '
        'lookups and the latest eligible version read', () async {
      await db.provision();

      final conn = await db.connectAdmin();
      addTearDown(conn.close);

      final rows = await conn.execute(
        Sql.named(
          'SELECT indexname, indexdef FROM pg_indexes '
          'WHERE schemaname = current_schema() AND tablename = @t',
        ),
        parameters: {'t': 'events'},
      );
      final defs = <String, String>{
        for (final r in rows) r[0]! as String: r[1]! as String,
      };
      const expected = <String, List<String>>{
        'events_sealed_hash_idx': ['sealed_hash', 'sequence_number'],
        'events_predecessor_idx': [
          'origin_database_id',
          'previous_event_hash',
          'sequence_number',
        ],
        'events_origin_position_idx': [
          'origin_database_id',
          'origin_position',
          'sequence_number',
        ],
        'events_held_as_authored_idx': [
          'held_as_authored_by',
          'sequence_number',
        ],
        'events_latest_eligible_idx': ['aggregate_id', 'sequence_number'],
      };
      for (final entry in expected.entries) {
        final def = defs[entry.key];
        expect(def, isNotNull, reason: '${entry.key} is missing');
        expect(def, isNot(contains('UNIQUE')), reason: entry.key);
        for (final column in entry.value) {
          expect(def, contains(column), reason: entry.key);
        }
      }
      expect(defs['events_latest_eligible_idx'], contains("'version'"));
    });

    // Verifies: EVS-DEV-security-findings/E
    // the once-per-detector lookup is served by a non-unique partial index
    //   over the finding events' holder and identity.
    test(
      'provisioning creates the non-unique security-finding index',
      () async {
        await db.provision();

        final conn = await db.connectAdmin();
        addTearDown(conn.close);

        final rows = await conn.execute(
          Sql.named(
            'SELECT indexdef FROM pg_indexes '
            'WHERE schemaname = current_schema() AND tablename = @t '
            'AND indexname = @i',
          ),
          parameters: {'t': 'events', 'i': 'events_security_finding_idx'},
        );
        expect(rows, hasLength(1), reason: 'events_security_finding_idx');
        final def = rows.single[0]! as String;
        expect(def, isNot(contains('UNIQUE')));
        expect(def, contains('held_as_authored_by'));
        expect(def, contains('finding_id'));
        expect(def, contains("'system.security_finding'"));
      },
    );

    test('view_rows has the JSONB-blob shape with composite PK', () async {
      final backend = await db.open(provision: true);
      addTearDown(backend.close);
      final conn = await db.connectAdmin();
      addTearDown(conn.close);

      // Columns + types.
      final cols = await conn.execute(
        'SELECT column_name, data_type FROM information_schema.columns '
        "WHERE table_schema = current_schema() AND table_name = 'view_rows'",
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

Future<List<String>> _listSchemaTables(Connection conn) async {
  final result = await conn.execute(
    'SELECT table_name FROM information_schema.tables '
    'WHERE table_schema = current_schema() ORDER BY table_name',
  );
  return result.map((row) => row[0]! as String).toList();
}
