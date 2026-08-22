// Verifies: EVS-PRD-action-dispatch/D
// IdempotencyStore contract.
// Verifies: EVS-DEV-postgres-backend/F
// InMemoryIdempotencyStore passes
//   the conformance harness alongside PostgresIdempotencyStore.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

import '../storage/idempotency_store_conformance.dart';

void main() {
  runIdempotencyStoreConformanceTests(
    () async => InMemoryIdempotencyStore(),
    label: 'in-memory',
  );
}
