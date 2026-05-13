// Verifies: EVS-DEV-postgres-backend/D — demo state projection
//   (matrix grants, directory, idempotency cache, events stream) runs
//   against PostgresBackend, satisfying the conformance harness
//   alongside the sembast flavor in demo_state_projection_test.dart.
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

import 'demo_state_projection_test.dart' show runDemoStateProjectionTests;
import 'support/demo_bootstrap.dart';

void main() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped(
        'PG_TEST_URL unset; skipping postgres demo projection tests',
      );
    });
    return;
  }

  Future<DemoBackends> factory() async {
    final endpoint = _endpointFromUrl(url);
    final tmp = await Connection.open(
      endpoint,
      settings: const ConnectionSettings(sslMode: SslMode.disable),
    );
    await tmp.execute('DROP SCHEMA public CASCADE');
    await tmp.execute('CREATE SCHEMA public');
    await tmp.close();

    final pg = await PostgresBackend.open(url: url);
    addTearDown(pg.close);
    return DemoBackends(
      backend: pg,
      idempotencyStore: PostgresIdempotencyStore.over(pg.pool),
    );
  }

  runDemoStateProjectionTests(factory, label: 'postgres');
}

Endpoint _endpointFromUrl(String url) {
  final uri = Uri.parse(url);
  final userInfoParts = uri.userInfo.isEmpty
      ? const <String>[]
      : uri.userInfo.split(':');
  return Endpoint(
    host: uri.host,
    port: uri.port == 0 ? 5432 : uri.port,
    database: uri.pathSegments.isEmpty ? '' : uri.pathSegments.first,
    username: userInfoParts.isEmpty ? null : userInfoParts.first,
    password: userInfoParts.length < 2
        ? null
        : userInfoParts.sublist(1).join(':'),
  );
}
