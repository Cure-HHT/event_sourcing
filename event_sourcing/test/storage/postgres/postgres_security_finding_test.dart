// Runs the security-finding scenarios of security_finding_conformance.dart
// on Postgres; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// security_finding_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../security/security_finding_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'finding');
  if (pg != null) tearDownAll(pg.drop);
  runSecurityFindingScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
