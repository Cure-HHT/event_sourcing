// Verifies: EVS-DEV-postgres-backend/D — PostgresBackend SHALL pass the
// backend-agnostic conformance harness. Same suite SembastBackend passes;
// run is gated on PG_TEST_URL.
// Verifies: EVS-PRD-portability/D — second concrete StorageBackend impl
// passes the same contract.

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
    final endpoint = _endpointFromUrl(url);
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

// TODO(CUR-1330): once @visibleForTesting'd, replace with
// PostgresBackend.endpointFromUrl + Connection.open(...).
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
