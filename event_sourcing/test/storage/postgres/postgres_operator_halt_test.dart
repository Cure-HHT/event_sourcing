// Runs the operator-halt scenarios on Postgres, each against a freshly
// reset schema; the scenarios that open a second process open a second
// PostgresBackend on the same database, so concurrent requests, a halt
// requested through another backend and the fence race run across two
// connection pools. Gated on PG_TEST_URL. The scenarios' assertions are
// cited on their own tests in test_support/operator_halt_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/operator_halt_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runOperatorHaltScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
