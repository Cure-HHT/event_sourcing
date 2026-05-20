// Verifies: EVS-DEV-postgres-backend/D — demo bootstrap runs against
//   PostgresBackend, satisfying the conformance harness alongside the
//   sembast flavor in bootstrap_test.dart.
//
// Gated on PG_TEST_URL. Drops + recreates the `public` schema in the
// per-test factory so each call returns a deterministic empty database
// — same discipline as `postgres_integration_test.dart` and the
// StorageBackend conformance harness.

@TestOn('vm')
library;

import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:postgres/postgres.dart';

import 'bootstrap_test.dart' show runBootstrapTests;
import 'support/demo_bootstrap.dart';

void main() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped(
        'PG_TEST_URL unset; skipping postgres demo bootstrap tests',
      );
    });
    return;
  }

  Future<DemoBackends> factory() async {
    // Drop + recreate `public` schema for per-test isolation. Split
    // into two execute calls because postgres v3.5 rejects multi-
    // statement strings in Session.execute (same discipline as the
    // StorageBackend conformance harness).
    final endpoint = PostgresBackend.endpointFromUrl(url);
    final tmp = await Connection.open(
      endpoint,
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    await tmp.execute('DROP SCHEMA public CASCADE');
    await tmp.execute('CREATE SCHEMA public');
    await tmp.close();

    final pg = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
    // Close on test exit regardless of whether the test body cleaned
    // up. `close()` is idempotent (PostgresBackend `_closed` flag) so
    // a double-close from the test body and tearDown is safe.
    addTearDown(pg.close);
    return DemoBackends(
      backend: pg,
      idempotencyStore: PostgresIdempotencyStore.over(pg.pool),
    );
  }

  runBootstrapTests(factory, label: 'postgres');
}
