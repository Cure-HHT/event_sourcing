// Verifies: EVS-DEV-postgres-backend/D — demo routes (session/start,
//   dispatch, healthz, inspect) run against PostgresBackend, satisfying
//   the conformance harness alongside the sembast flavor in
//   demo_routes_test.dart.
//
// The operator routes under /demo/delivery/ (delivery_routes_test.dart) run
// here against Postgres too: status, halt, cancellation, recovery, the 409
// refusals and the 403 with nothing written.
//
// Gated on PG_TEST_URL. Drops the demo schema and runs the demo's deployment
// step in the per-test factory, so each call returns a deterministic empty
// database opened as the declared runtime role.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import 'delivery_routes_test.dart' show runDeliveryRoutesTests;
import 'demo_routes_test.dart' show runDemoRoutesTests;
import 'support/demo_bootstrap.dart';
import 'support/demo_postgres.dart';

void main() {
  final db = DemoPostgres.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping postgres demo routes tests');
    });
    return;
  }

  Future<DemoBackends> factory() async {
    await db.reset();
    final pg = await db.open();
    addTearDown(pg.close);
    return DemoBackends(backend: pg);
  }

  runDemoRoutesTests(factory, label: 'postgres');
  runDeliveryRoutesTests(factory, label: 'postgres');
}
