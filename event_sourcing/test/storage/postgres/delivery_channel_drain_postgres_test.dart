// Runs the delivery-channel drain scenarios on Postgres, each against a
// freshly reset schema opened through the owner, runtime and lock role
// fixture; gated on PG_TEST_URL. The scenarios' assertions are cited on
// their own tests in test_support/delivery_channel_drain_conformance.dart.

@TestOn('vm')
library;

import 'package:test/test.dart';

import '../../test_support/delivery_channel_drain_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'chdrain');
  if (db != null) tearDownAll(db.drop);
  runDeliveryChannelDrainScenarios(
    () => PostgresScenarioDatabase.fresh(db),
    label: 'postgres',
  );
}
