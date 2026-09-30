// Runs the scenarios of ingest_record_findings_conformance.dart on Postgres;
// gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/ingest_record_findings_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../test_support/ingest_record_findings_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'ingfind');
  if (pg != null) tearDownAll(pg.drop);
  runIngestRecordFindingScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
