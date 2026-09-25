// PostgresBackend runs the backend-agnostic conformance harness — the same
// suite SembastBackend passes; the run is gated on PG_TEST_URL. The
// harness's assertions are cited on its own tests rather than here.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

import '../storage_backend_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runStorageBackendConformanceTests(
    () async {
      if (db == null) return null;
      // Fresh schema per test, so each test sees an empty database.
      await db.reset();
      return db.open(provision: true);
    },
    reopen: (_) => db!.open(),
    backendLabel: 'postgres',
    securityStoreOf: (backend) =>
        PostgresSecurityContextStore(backend: backend as PostgresBackend),
  );

  // Verifies: EVS-DEV-postgres-backend/L
  test('a security-context store refuses a transaction from another '
      'backend and accepts one from its own backend', () async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL is not set');
      return;
    }
    final own = await db.open(provision: true);
    addTearDown(own.close);
    final other = await db.open();
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
