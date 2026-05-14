// Verifies: EVS-DEV-postgres-backend/D — demo routes (session/start,
//   dispatch, healthz, inspect) run against PostgresBackend, satisfying
//   the conformance harness alongside the sembast flavor in
//   demo_routes_test.dart.
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

import 'demo_routes_test.dart' show runDemoRoutesTests;
import 'support/demo_bootstrap.dart';

void main() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping postgres demo routes tests');
    });
    return;
  }

  Future<DemoBackends> factory() async {
    final endpoint = PostgresBackend.endpointFromUrl(url);
    final tmp = await Connection.open(
      endpoint,
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    await tmp.execute('DROP SCHEMA public CASCADE');
    await tmp.execute('CREATE SCHEMA public');
    await tmp.close();

    final pg = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
    addTearDown(pg.close);
    return DemoBackends(
      backend: pg,
      idempotencyStore: PostgresIdempotencyStore.over(pg.pool),
    );
  }

  runDemoRoutesTests(factory, label: 'postgres');
}
