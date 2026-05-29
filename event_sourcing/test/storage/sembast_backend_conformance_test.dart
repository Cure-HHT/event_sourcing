// Verifies: EVS-DEV-postgres-backend/D — SembastBackend SHALL pass the
//   backend-agnostic conformance harness. Both SembastBackend and
//   PostgresBackend call the same harness via their own factories.
// Verifies: EVS-PRD-portability/D — same contract realized by a second
//   concrete backend implementation. The harness is the source of truth
//   for the abstract StorageBackend contract; assertions are written
//   against the interface and exercised against this concrete impl.
@TestOn('vm')
library;

import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'storage_backend_conformance.dart';

void main() {
  runStorageBackendConformanceTests(() async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'conformance-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    return SembastBackend(database: db);
  }, backendLabel: 'sembast (memory)');
}
