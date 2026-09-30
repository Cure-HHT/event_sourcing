// Runs the delivery-cycle scenarios on Postgres, each against a freshly
// reset schema; a second process is a second PostgresBackend on the same
// database, with its own pool and lock session, so standby and takeover
// run across two lock sessions. Gated on PG_TEST_URL. The scenarios'
// assertions are cited on their own tests in
// test_support/delivery_cycle_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/delivery_cycle_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  runDeliveryCycleScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
    giveUpInjection: (fail) => (
      failAfterExclusionObtained: null,
      failEpochBumpWithSerializationFailure: fail,
    ),
  );
}
