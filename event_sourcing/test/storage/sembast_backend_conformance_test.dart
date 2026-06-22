// Verifies: EVS-DEV-postgres-backend/D — SembastBackend SHALL pass the
//   backend-agnostic conformance harness. Both SembastBackend and
//   PostgresBackend call the same harness via their own factories.
// Verifies: EVS-PRD-portability/D — same contract realized by a second
//   concrete backend implementation. The harness is the source of truth
//   for the abstract StorageBackend contract; assertions are written
//   against the interface and exercised against this concrete impl.
// Verifies: EVS-DEV-find-all-events-extended-filters/A/B/C/D — entryType +
//   clientTimestampStart/End range filters AND-composed with afterSequence/
//   limit/originator params (A/B/C), exercised against BOTH findAllEvents and
//   findAllEventsInTxn via the shared conformance harness — the single shared
//   _composeFindAllEventsFilter helper used by both paths (D).
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
