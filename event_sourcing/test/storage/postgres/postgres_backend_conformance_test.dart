// PostgresBackend runs the backend-agnostic conformance harness — the same
// suite SembastBackend passes; the run is gated on PG_TEST_URL. The
// harness's assertions are cited on its own tests rather than here.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../storage_backend_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  runStorageBackendConformanceTests(
    () async {
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
      return PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
    },
    reopen: (_) => PostgresBackend.open(url: url!, sslMode: SslMode.disable),
    backendLabel: 'postgres',
    securityStoreOf: (backend) =>
        PostgresSecurityContextStore(backend: backend as PostgresBackend),
  );

  // Verifies: EVS-DEV-postgres-backend/L
  test('a security-context store refuses a transaction from another '
      'backend and accepts one from its own backend', () async {
    if (url == null) {
      markTestSkipped('PG_TEST_URL is not set');
      return;
    }
    final own = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    addTearDown(own.close);
    final other = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
    );
    addTearDown(other.close);
    final store = PostgresSecurityContextStore(backend: own);
    await other.transaction((foreignTxn) async {
      await expectLater(
        store.readInTxn(foreignTxn, 'no-such-event'),
        throwsStateError,
      );
    });
    final found = await own.transaction(
      (txn) => store.readInTxn(txn, 'no-such-event'),
    );
    expect(found, isNull);
  });
}
