// Verifies: EVS-DEV-postgres-backend/E — idempotency table schema:
//   PostgresIdempotencyStore reads and writes the same idempotency
//   table provisioned by ensurePostgresSchema (Task 1) with primary
//   key (action_name, principal_id, idempotency_key).
// Verifies: EVS-DEV-postgres-backend/F — PostgresIdempotencyStore
//   passes the same conformance harness as InMemoryIdempotencyStore.
// Verifies: EVS-PRD-action-dispatch/D — IdempotencyStore contract:
//   lookup miss/hit, tuple-keyed separation, expiry semantics, and
//   sweepExpired all behave identically on the Postgres impl.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show ensurePostgresSchema;
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
    // First-run safety: ensurePostgresSchema is idempotent so a
    // re-run is harmless. We invoke it inside a transaction to match
    // PostgresBackend.open's contract (Task 1).
    await tmp.runTx(ensurePostgresSchema);
    await tmp.execute('TRUNCATE idempotency');
    await tmp.close();

    final pool = Pool<void>.withEndpoints(
      [endpoint],
      settings: const PoolSettings(
        maxConnectionCount: 2,
        sslMode: SslMode.disable,
      ),
    );
    return PostgresIdempotencyStore.over(pool);
  }, label: 'postgres');
}
