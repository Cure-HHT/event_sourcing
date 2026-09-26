// Runs the causal-stamping scenarios of causal_stamping_conformance.dart on
// Postgres; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// causal_stamping_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../event_store/causal_stamping_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'causal');
  final other = PostgresTestDatabase.fromEnvironment(tag: 'causal_other');
  if (pg != null) tearDownAll(pg.drop);
  if (other != null) tearDownAll(other.drop);
  runCausalStampingScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    openOtherDatabase: () => PostgresScenarioDatabase.fresh(other),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
