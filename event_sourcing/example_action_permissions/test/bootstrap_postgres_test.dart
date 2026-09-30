// Verifies: EVS-DEV-postgres-backend/D
// demo bootstrap runs against
//   PostgresBackend, satisfying the conformance harness alongside the
//   sembast flavor in bootstrap_test.dart.
// Verifies: EVS-PRD-destinations/V
// two server instances booted the way the demo server boots share one
//   database: each starts a delivery cycle over its registry, one drains
//   and the other stands by; once the draining instance closes its cycle,
//   another instance's cycle drains.
//
// Gated on PG_TEST_URL. Drops the demo schema and runs the demo's deployment
// step in the per-test factory, so each call returns a deterministic empty
// database opened as the declared runtime role.

@TestOn('vm')
library;

import 'dart:isolate';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/postgres_setup.dart';

import 'bootstrap_test.dart' show runBootstrapTests;
import 'support/demo_bootstrap.dart';
import 'support/demo_postgres.dart';

void main() {
  final db = DemoPostgres.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped(
        'PG_TEST_URL unset; skipping postgres demo bootstrap tests',
      );
    });
    return;
  }
  final url = db.runtimeUrl;

  Future<DemoBackends> factory() async {
    await db.reset();
    final pg = await db.open();
    // Close on test exit regardless of whether the test body cleaned
    // up. `close()` is idempotent (PostgresBackend `_closed` flag) so
    // a double-close from the test body and tearDown is safe.
    addTearDown(pg.close);
    return DemoBackends(backend: pg);
  }

  runBootstrapTests(factory, label: 'postgres');

  test('two server instances: one drains, the other stands by', () async {
    final first = await factory();
    final a = await _boot(
      first.backend,
      'aaaa0001-0000-4000-8000-0000000000a1',
    );
    final cycleA = await SyncCycle.start(
      registry: a.destinations,
      cadence: const Duration(milliseconds: 200),
    );
    try {
      expect(cycleA.state, SyncCycleState.running);
      expect(await Isolate.run(() => _otherInstanceCycleState(url)), 'standby');
      await cycleA.close();
      expect(await Isolate.run(() => _otherInstanceCycleState(url)), 'running');
    } finally {
      await cycleA.close();
    }
  });
}

Future<DemoServerComponents> _boot(
  StorageBackend backend,
  String installIdentifier,
) => bootstrapDemoServer(
  storage: demoStorageOver(backend),
  permissionsYaml: validPermissionsYaml,
  usersYaml: validUsersYaml,
  installIdentifier: installIdentifier,
);

/// Boots a second server instance on [url] the way the demo server boots,
/// starts its delivery cycle, and returns the cycle's state.
Future<String> _otherInstanceCycleState(String url) async {
  final backend = await PostgresBackend.open(
    url: url,
    schema: demoPostgresSchema,
    sslMode: SslMode.disable,
  );
  final b = await _boot(backend, 'aaaa0001-0000-4000-8000-0000000000b1');
  final cycle = await SyncCycle.start(
    registry: b.destinations,
    cadence: const Duration(milliseconds: 200),
  );
  final state = cycle.state.name;
  await cycle.close();
  await b.eventStore.close();
  return state;
}
