// Runs the drain-wedge scenarios on Postgres, each against a freshly reset
// schema, with a second PostgresBackend on the same database playing a
// restarted process (the first closes before it opens); gated on
// PG_TEST_URL. On Postgres the two wedges of one cycle run under
// Future.wait on two pool connections and contend on the sequence counter,
// and the scenario asserts that one transaction is run again. The
// scenarios' assertions are cited on their own tests in
// test_support/drain_wedge_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/drain_wedge_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runDrainWedgeScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
    retriesConflictingTransactions: true,
    processesShareDatabaseHandle: false,
  );
}
