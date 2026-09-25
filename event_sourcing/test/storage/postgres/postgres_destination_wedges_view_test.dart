// Runs the destination-wedges view scenarios on Postgres, each against a
// freshly reset schema, with peers on in-memory Sembast databases; gated
// on PG_TEST_URL. Every refusal is also asserted here, so a partial write
// in the SERIALIZABLE ingest transaction would show as a changed log, view
// or sequence counter. The scenarios' assertions are cited on their own
// tests in test_support/destination_wedges_view_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/destination_wedges_view_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runDestinationWedgesViewScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
