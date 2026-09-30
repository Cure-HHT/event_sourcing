// Runs the scenarios of outstanding_finding_mark_conformance.dart on
// Postgres; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/outstanding_finding_mark_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../test_support/outstanding_finding_mark_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'findmark');
  if (pg != null) tearDownAll(pg.drop);
  runOutstandingFindingMarkScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
