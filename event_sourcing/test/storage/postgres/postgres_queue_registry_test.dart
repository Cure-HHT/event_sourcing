// Runs the queue-registry scenarios on Postgres, each against a freshly
// reset schema, with a second PostgresBackend on the same database playing
// another process; gated on PG_TEST_URL. The scenarios' assertions are cited
// on their own tests in test_support/queue_registry_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/queue_registry_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runQueueRegistryScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
