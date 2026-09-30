// Runs the scenarios of delivery_pull_conformance.dart on Postgres; gated
// on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/delivery_pull_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../test_support/delivery_pull_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'delpull');
  if (pg != null) tearDownAll(pg.drop);
  runDeliveryPullScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
