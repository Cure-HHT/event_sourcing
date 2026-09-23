// Runs the delivery-cycle scenarios on Postgres, each against a freshly
// reset schema; a second process is a second PostgresBackend on the same
// database, with its own pool and lock session, so standby and takeover
// run across two lock sessions. Gated on PG_TEST_URL. The scenarios'
// assertions are cited on their own tests in
// test_support/delivery_cycle_conformance.dart.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/delivery_cycle_conformance.dart';
import '../../test_support/queue_registry_conformance.dart';
import 'test_postgres_url.dart';

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

class _PostgresCycleDatabase implements QueueTestDatabase {
  _PostgresCycleDatabase(this._url);

  final String _url;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await PostgresBackend.open(
      url: _url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<Set<String>> backendStateKeys() async {
    final conn = await _connect(_url);
    try {
      final rows = await conn.execute('SELECT key FROM backend_state');
      return <String>{for (final r in rows) r[0]! as String};
    } finally {
      await conn.close();
    }
  }

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
  }
}

void main() {
  final url = testPostgresUrl();
  runDeliveryCycleScenarios(
    () async {
      if (url == null) return null;
      final tmp = await _connect(url);
      await tmp.execute('DROP SCHEMA public CASCADE');
      await tmp.execute('CREATE SCHEMA public');
      await tmp.close();
      return _PostgresCycleDatabase(url);
    },
    label: 'postgres',
    giveUpInjection: (fail) => (
      failAfterExclusionObtained: null,
      failEpochBumpWithSerializationFailure: fail,
    ),
  );
}
