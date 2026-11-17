// Runs the destination-holds view scenarios on Postgres, each against a
// freshly reset schema; gated on PG_TEST_URL. The scenarios' assertions are
// cited on their own tests in
// test_support/destination_holds_view_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/destination_holds_view_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runDestinationHoldsViewScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
