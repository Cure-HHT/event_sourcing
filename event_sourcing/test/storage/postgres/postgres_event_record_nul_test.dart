// Runs the U+0000 append-refusal scenarios of event_record_nul_conformance.dart
// on Postgres; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// event_record_nul_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../event_store/event_record_nul_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'nulrec');
  if (pg != null) tearDownAll(pg.drop);
  runEventRecordNulScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );
}
