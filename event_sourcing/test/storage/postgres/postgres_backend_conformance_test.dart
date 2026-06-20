// Verifies: EVS-DEV-postgres-backend/C — transaction<T> runs at SERIALIZABLE
//   isolation (conflicting concurrent txns retry/serialize); rollback on throw,
//   commit on return, handle invalidated after body.
//   The conformance harness 'transaction' group covers rollback-on-throw,
//   commit-on-return, and handle-invalidation-after-body clauses.
// Verifies: EVS-DEV-postgres-backend/D — PostgresBackend SHALL pass the
// backend-agnostic conformance harness. Same suite SembastBackend passes;
// run is gated on PG_TEST_URL.
// Verifies: EVS-PRD-portability/D — second concrete StorageBackend impl
// passes the same contract.
// Verifies: EVS-DEV-find-all-events-extended-filters/A, EVS-DEV-find-all-events-extended-filters/B, EVS-DEV-find-all-events-extended-filters/C
//   — entryType + client-timestamp filters AND-compose on findAllEvents and
//   findAllEventsInTxn; exercised by the conformance harness 'findAllEvents
//   extended filters' group. (Assertion D names the reference SembastBackend's
//   single shared _composeFindAllEventsFilter helper — it does not apply to
//   PostgresBackend and is covered by find_all_events_shared_filter_test.dart.)

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../storage_backend_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  runStorageBackendConformanceTests(() async {
    if (url == null) return null;
    // Fresh schema per test: drop+recreate public so each test sees an
    // empty database. Split into two execute calls because postgres
    // v3.5 rejects multi-statement strings in Session.execute.
    final endpoint = PostgresBackend.endpointFromUrl(url);
    final tmp = await Connection.open(
      endpoint,
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    await tmp.execute('DROP SCHEMA public CASCADE');
    await tmp.execute('CREATE SCHEMA public');
    await tmp.close();
    return PostgresBackend.open(url: url, sslMode: SslMode.disable);
  }, backendLabel: 'postgres');
}
