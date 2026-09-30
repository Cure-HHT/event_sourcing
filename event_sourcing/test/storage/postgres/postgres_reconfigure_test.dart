// Runs the reconfigure scenarios on Postgres, each against a freshly reset
// schema; a second process is a second PostgresBackend on the same
// database, with its own pool and lock session, so each drainer of a
// scenario holds the lock on its own session. Gated on PG_TEST_URL. The
// scenarios' assertions are cited on their own tests in
// test_support/reconfigure_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/reconfigure_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runReconfigureScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
