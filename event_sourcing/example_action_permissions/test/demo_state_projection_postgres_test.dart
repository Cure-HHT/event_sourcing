// Verifies: EVS-DEV-postgres-backend/D
// demo state projection
//   (matrix grants, directory, idempotency cache, events stream) runs
//   against PostgresBackend, satisfying the conformance harness
//   alongside the sembast flavor in demo_state_projection_test.dart.
//
// Gated on PG_TEST_URL. Drops the demo schema and runs the demo's deployment
// step in the per-test factory, so each call returns a deterministic empty
// database opened as the declared runtime role.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import 'demo_state_projection_test.dart' show runDemoStateProjectionTests;
import 'support/demo_bootstrap.dart';
import 'support/demo_postgres.dart';

void main() {
  final db = DemoPostgres.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped(
        'PG_TEST_URL unset; skipping postgres demo projection tests',
      );
    });
    return;
  }

  Future<DemoBackends> factory() async {
    await db.reset();
    final pg = await db.open();
    addTearDown(pg.close);
    return DemoBackends(backend: pg);
  }

  runDemoStateProjectionTests(factory, label: 'postgres');
}
