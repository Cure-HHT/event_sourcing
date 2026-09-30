// Runs the scenarios of succession_restore_conformance.dart on Postgres;
// gated on PG_TEST_URL. Each scenario opens two databases (a receiver and a
// successor), so this alternates between two schemas.
//
// The scenarios' assertions are cited on their own tests in
// test_support/succession_restore_conformance.dart.

@TestOn('vm')
library;

import 'package:flutter_test/flutter_test.dart';

import '../../test_support/succession_restore_conformance.dart';
import '../../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

void main() {
  final databases = <PostgresTestDatabase?>[
    PostgresTestDatabase.fromEnvironment(tag: 'restore'),
    PostgresTestDatabase.fromEnvironment(tag: 'restore_peer'),
  ];
  for (final db in databases) {
    if (db != null) tearDownAll(db.drop);
  }
  var next = 0;

  Future<VersionTestDatabase?> openDatabase() async {
    final db = databases[next++ % databases.length];
    return PostgresScenarioDatabase.fresh(db);
  }

  runSuccessionRestoreScenarios(
    openDatabase: openDatabase,
    backendLabel: 'postgres',
    skip: databases.first == null ? 'PG_TEST_URL is not set' : null,
  );
}
