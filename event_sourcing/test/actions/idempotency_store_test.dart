// InMemoryIdempotencyStore runs the backend-agnostic IdempotencyStore
// conformance harness, alongside PostgresIdempotencyStore. The harness's
// assertions are cited on its own tests rather than here.

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
