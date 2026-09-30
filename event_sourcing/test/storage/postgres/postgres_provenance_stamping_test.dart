// Runs the provenance-stamping scenarios of
// provenance_stamping_conformance.dart on Postgres; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// provenance_stamping_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../event_store/provenance_stamping_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'stamping');
  final other = PostgresTestDatabase.fromEnvironment(tag: 'stamping_other');
  if (pg != null) tearDownAll(pg.drop);
  if (other != null) tearDownAll(other.drop);
  runProvenanceStampingScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    openOtherDatabase: () => PostgresScenarioDatabase.fresh(other),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
