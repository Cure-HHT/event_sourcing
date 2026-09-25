// Verifies: EVS-PRD-storage-barrier/B
// Verifies: EVS-PRD-storage-barrier/C
// Verifies: EVS-PRD-storage-barrier/D
// Verifies: EVS-PRD-storage-barrier/E
// Verifies: EVS-DEV-storage-capability/C
// Verifies: EVS-DEV-storage-capability/F
//
// Runs the run-time barrier scenarios of run_time_barrier_conformance.dart
// on Postgres databases the library opens from a description, under the
// declared runtime and lock roles: no handed-out object, the idempotency
// store the event store builds included, answers a writing,
// reserved-appending, publishing or handle-yielding member dynamically or
// downcasts to a writing type, and the public operations keep working.
//
// Gated on PG_TEST_URL; files that reset the schema run one at a time.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import '../../barrier/run_time_barrier_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final databases = <PostgresTestDatabase?>[
    PostgresTestDatabase.fromEnvironment(tag: 'barrier'),
    PostgresTestDatabase.fromEnvironment(tag: 'barrier_peer'),
  ];
  for (final db in databases) {
    if (db != null) tearDownAll(db.drop);
  }
  var next = 0;

  runRunTimeBarrierScenarios(
    freshStorage: () async {
      final db = databases[next++ % databases.length]!;
      await db.reset(provision: true);
      return PostgresStorage(
        url: db.runtimeUrl,
        schema: db.schema,
        lockUrl: db.lockUrl,
        sslMode: SslMode.disable,
      );
    },
    backendLabel: 'postgres',
    expectsIdempotencyStore: true,
    skip: databases.first == null ? 'PG_TEST_URL is not set' : null,
  );
}
