// PostgresIdempotencyStore runs the backend-agnostic IdempotencyStore
// conformance harness — the same suite InMemoryIdempotencyStore passes —
// through the transactions of a PostgresBackend (the store an event store
// over the backend builds). It reads
// and writes the idempotency table PostgresBackend.provision creates, keyed
// by (action_name, principal_id, idempotency_key). The harness's assertions
// are cited on its own tests rather than here.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../idempotency_store_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];
  tearDown(() async {
    for (final backend in backends) {
      await backend.close();
    }
    backends.clear();
  });

  var reset = false;
  runIdempotencyStoreConformanceTests(() async {
    if (db == null) return null;
    if (!reset) {
      await db.reset(provision: true);
      reset = true;
    }
    final backend = await db.open();
    backends.add(backend);
    final tmp = await db.connectAdmin();
    await tmp.execute('TRUNCATE idempotency');
    await tmp.close();
    return idempotencyStoreOver(backend);
  }, label: 'postgres, through the backend');

  // Verifies: EVS-DEV-postgres-backend/F
  // the store the library builds over the backend persists a dispatch
  //   outcome in the backend's database, and stops serving once the backend
  //   is closed.
  test('the idempotency store over the backend round-trips an outcome, and '
      'fails once the backend is closed', () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
    final backend = await db.open();
    final store = await idempotencyStoreOver(backend);
    await store.record(
      actionName: 'for_backend',
      principalId: 'p',
      key: 'k',
      resultJson: const <String, Object?>{'ok': true},
      emittedEventIds: const <String>['e1'],
      expiresAt: DateTime.now().toUtc().add(const Duration(hours: 1)),
    );
    final hit = await store.lookup('for_backend', 'p', 'k');
    expect(hit, isNotNull);
    expect(hit!.resultJson, <String, Object?>{'ok': true});
    expect(hit.emittedEventIds, <String>['e1']);

    await backend.close();
    await expectLater(store.lookup('for_backend', 'p', 'k'), throwsA(anything));
  });

  // Verifies: EVS-DEV-storage-capability/I
  // the event store builds the Postgres idempotency store over the storage
  //   it runs on: outcomes persist in that database, in its fenced
  //   transactions, and the store is built once.
  test('the event store builds the idempotency store over its own '
      'storage', () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
    final backend = await db.open();
    backends.add(backend);
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: EntryTypeRegistry(),
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-0000000001de',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );
    try {
      final idempotency = store.idempotencyStore;
      expect(idempotency, isA<PostgresIdempotencyStore>());
      expect(identical(store.idempotencyStore, idempotency), isTrue);
      await idempotency!.record(
        actionName: 'built_by_store',
        principalId: 'p',
        key: 'k',
        resultJson: const <String, Object?>{'ok': true},
        emittedEventIds: const <String>['e1'],
        expiresAt: DateTime.now().toUtc().add(const Duration(hours: 1)),
      );
      final admin = await db.connectAdmin();
      try {
        final rows = await admin.execute(
          'SELECT count(*) FROM idempotency WHERE action_name = '
          "'built_by_store'",
        );
        expect(rows.first[0], 1);
      } finally {
        await admin.close();
      }
      expect(
        (await idempotency.lookup('built_by_store', 'p', 'k'))?.resultJson,
        <String, Object?>{'ok': true},
      );
    } finally {
      await store.close();
    }
  });

  // Verifies: EVS-DEV-storage-capability/I
  // an idempotency store over a pool the application opened stays
  //   available to the application.
  test('over a pool the application opened, an outcome round-trips', () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
    final pool = Pool<void>.withEndpoints(
      <Endpoint>[PostgresBackend.endpointFromUrl(db.runtimeUrl)],
      settings: PoolSettings(
        sslMode: SslMode.disable,
        maxConnectionCount: 1,
        onOpen: (c) => c.execute('SET search_path TO ${quoteIdent(db.schema)}'),
      ),
    );
    try {
      final idempotency = PostgresIdempotencyStore.over(pool);
      await idempotency.record(
        actionName: 'over_pool',
        principalId: 'p',
        key: 'k',
        resultJson: const <String, Object?>{'ok': true},
        emittedEventIds: const <String>['e1'],
        expiresAt: DateTime.now().toUtc().add(const Duration(hours: 1)),
      );
      expect(
        (await idempotency.lookup('over_pool', 'p', 'k'))?.emittedEventIds,
        <String>['e1'],
      );
    } finally {
      await pool.close();
    }
  });
}
