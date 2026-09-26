// Runs the ingest event-hash scenarios on Postgres, each against a freshly
// reset schema, with the peer and relay on in-memory Sembast databases;
// gated on PG_TEST_URL. Every record kept in a finding asserts a log
// unchanged beside the findings, so a partial write in the SERIALIZABLE
// ingest transaction would show. The scenarios' assertions are cited on their own tests in
// test_support/ingest_hash_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/ingest_hash_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runIngestHashScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
