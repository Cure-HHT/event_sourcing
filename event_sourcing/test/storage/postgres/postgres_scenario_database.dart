// A database for the backend-agnostic scenario suites, over a
// PostgresTestDatabase. This file declares no tests, so it carries no
// citation.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';

import '../../test_support/queue_registry_conformance.dart';
import '../../test_support/version_compatibility_conformance.dart';
import 'test_postgres_url.dart';

/// A database for the shared scenarios: its backends run as [db]'s runtime
/// role over the schema its owner provisioned.
class PostgresScenarioDatabase
    implements QueueTestDatabase, VersionTestDatabase {
  PostgresScenarioDatabase(this.db);

  final PostgresTestDatabase db;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  /// Recreates [db]'s schema, empty, and returns a database over it; null
  /// when [db] is (PG_TEST_URL is not set).
  static Future<PostgresScenarioDatabase?> fresh(
    PostgresTestDatabase? db,
  ) async {
    if (db == null) return null;
    await db.reset();
    return PostgresScenarioDatabase(db);
  }

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await db.open(provision: true);
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<Set<String>> backendStateKeys() async {
    final conn = await db.connectAdmin();
    try {
      final rows = await conn.execute('SELECT key FROM backend_state');
      return <String>{for (final r in rows) r[0]! as String};
    } finally {
      await conn.close();
    }
  }

  @override
  Future<void> stop(EventStore store) => store.close();

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
  }
}
