// PostgresIdempotencyStore runs the backend-agnostic IdempotencyStore
// conformance harness — the same suite InMemoryIdempotencyStore passes —
// twice: over a pool the caller owns, and through the transactions of a
// PostgresBackend (`forBackend`). It reads and writes the idempotency table
// PostgresBackend.provision creates, keyed by (action_name,
// principal_id, idempotency_key). The harness's assertions are cited on
// its own tests rather than here.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../idempotency_store_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  runIdempotencyStoreConformanceTests(() async {
    if (url == null) return null;
    // Truncate before each test so the per-test view is empty.
    // We don't drop the whole schema like the StorageBackend
    // conformance does, because this test scopes to one table —
    // truncating idempotency keeps the per-test cost minimal and
    // leaves any sibling schema state untouched.
    final endpoint = PostgresBackend.endpointFromUrl(url);
    final tmp = await Connection.open(
      endpoint,
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    // Provisioning leaves a provisioned schema untouched, so a re-run is
    // harmless.
    await PostgresBackend.provision(url, sslMode: SslMode.disable);
    await tmp.execute('TRUNCATE idempotency');
    await tmp.close();

    final pool = Pool<void>.withEndpoints(
      [endpoint],
      settings: const PoolSettings(
        maxConnectionCount: 2,
        sslMode: SslMode.disable,
      ),
    );
    // The caller owns the pool: it is closed when the test ends.
    addTearDown(pool.close);
    return PostgresIdempotencyStore.over(pool);
  }, label: 'postgres');

  final backends = <PostgresBackend>[];
  tearDown(() async {
    for (final backend in backends) {
      await backend.close();
    }
    backends.clear();
  });

  runIdempotencyStoreConformanceTests(() async {
    if (url == null) return null;
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    backends.add(backend);
    final tmp = await Connection.open(
      PostgresBackend.endpointFromUrl(url),
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    await tmp.execute('TRUNCATE idempotency');
    await tmp.close();
    return PostgresIdempotencyStore.forBackend(backend);
  }, label: 'postgres, through the backend');

  // Verifies: EVS-DEV-postgres-backend/F
  // a store built over the backend persists a dispatch outcome in the
  //   backend's database, and stops serving once the backend is closed.
  test('forBackend round-trips an outcome, and fails once the backend is '
      'closed', () async {
    if (url == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    final store = PostgresIdempotencyStore.forBackend(backend);
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
}
