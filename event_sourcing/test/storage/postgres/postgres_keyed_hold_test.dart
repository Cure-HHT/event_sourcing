// Runs the keyed-hold scenarios on Postgres, each against a freshly
// reset schema; gated on PG_TEST_URL. The scenarios' assertions are cited on
// their own tests in test_support/keyed_hold_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/keyed_hold_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runKeyedHoldScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
    serializableConflicts: true,
  );
}
