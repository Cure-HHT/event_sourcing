// The candidate read and the log-head read of PostgresBackend, held to the
// backend-agnostic conformance, and the server's plan of the candidate read
// over a large log. Gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/candidate_read_conformance.dart';
import 'test_postgres_url.dart';

const _kSource = Source(
  hopId: 'candidate-hop',
  identifier: 'candidate-install',
  softwareVersion: 'candidate-test',
);

/// Inserts [count] events straight into the events table through [conn]:
/// one in a hundred of entry type `bulk_note` and event type `noted`, the
/// rest `bulk_sync` / `synced`, so a filtered read selects few of them.
Future<void> _bulkInsert(Connection conn, int count) => conn.execute(
  Sql.named('''
    INSERT INTO events (
      sequence_number, event_id, aggregate_id, aggregate_type, entry_type,
      entry_type_version_major, entry_type_version_minor,
      lib_format_version_major, lib_format_version_minor,
      entry_type_version_json, lib_format_version_json, event_type, data,
      metadata, initiator, client_timestamp, client_timestamp_text,
      event_hash, unknown_fields)
    SELECT g, 'bulk-' || g, 'agg-' || (g % 500),
      CASE WHEN g % 100 = 0 THEN 'note' ELSE 'site' END,
      CASE WHEN g % 100 = 0 THEN 'bulk_note' ELSE 'bulk_sync' END,
      1, 0, 3, 0, '{}'::jsonb, '{}'::jsonb,
      CASE WHEN g % 100 = 0 THEN 'noted' ELSE 'synced' END,
      '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, now(), 'now', 'hash-' || g,
      '{}'::jsonb
    FROM generate_series(1, @n) AS g
  '''),
  parameters: {'n': count},
);

Future<String> _plan(
  Connection conn,
  ({String sql, Map<String, Object?> parameters}) query,
) async {
  final rows = await conn.execute(
    Sql.named('EXPLAIN ${query.sql}'),
    parameters: query.parameters,
  );
  return [for (final r in rows) r[0]! as String].join('\n');
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'cand');
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
  });

  tearDown(() async {
    for (final backend in backends.reversed) {
      await backend.close();
    }
    backends.clear();
  });

  group('PostgresBackend candidate reads', () {
    if (db == null) return;
    runCandidateReadConformance(
      openBackend: () async {
        final backend = await PostgresBackend.open(
          url: db.runtimeUrl,
          schema: db.schema,
          sslMode: SslMode.disable,
        );
        backends.add(backend);
        return backend;
      },
      openStore: (backend) => EventStore.open(
        storage: ApplicationSuppliedStorage(
          backend,
          PostgresSecurityContextStore(backend: backend as PostgresBackend),
        ),
        entryTypes: candidateReadEntryTypes(),
        source: _kSource,
        projections: ProjectionRegistry(),
      ),
    );
  });

  group('PostgresBackend candidate windows', () {
    if (db == null) return;
    runCandidateWindowBoundaryConformance(
      openBackend: () async {
        final backend = await PostgresBackend.open(
          url: db.runtimeUrl,
          schema: db.schema,
          sslMode: SslMode.disable,
        );
        backends.add(backend);
        return backend;
      },
      openStore: (backend, {required entryTypes, required projections}) {
        final registry = ProjectionRegistry();
        projections.forEach(registry.register);
        return EventStore.open(
          storage: ApplicationSuppliedStorage(
            backend,
            PostgresSecurityContextStore(backend: backend as PostgresBackend),
          ),
          entryTypes: entryTypes,
          source: _kSource,
          projections: registry,
        );
      },
    );
  });

  // The candidate read's indexes keep a filtered read of a long log from
  // scanning it; the measured bounds stand on that.
  test('the candidate read of an event-type or entry-type filter over a '
      '50k-event log is planned through an index', () async {
    if (db == null) return;
    final conn = await db.connectAdmin();
    addTearDown(conn.close);
    await _bulkInsert(conn, 50000);
    await conn.execute('ANALYZE events');

    final byEventType = await _plan(
      conn,
      PostgresBackend.candidateEventsQuery(
        afterSequence: 0,
        throughSequence: 50000,
        limit: 200,
        eventTypes: const <String>{'noted'},
      ),
    );
    final byEntryType = await _plan(
      conn,
      PostgresBackend.candidateEventsQuery(
        afterSequence: 0,
        throughSequence: 50000,
        limit: 200,
        entryTypes: const <String>{'bulk_note'},
      ),
    );
    // ignore: avoid_print
    print('event-type filter plan:\n$byEventType');
    // ignore: avoid_print
    print('entry-type filter plan:\n$byEntryType');
    for (final plan in [byEventType, byEntryType]) {
      expect(plan, contains('Index'));
      expect(plan, isNot(contains('Seq Scan')));
    }
  });
}
