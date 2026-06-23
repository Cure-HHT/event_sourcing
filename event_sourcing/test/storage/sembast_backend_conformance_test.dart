// Verifies: EVS-DEV-postgres-backend/D — SembastBackend SHALL pass the
//   backend-agnostic conformance harness. Both SembastBackend and
//   PostgresBackend call the same harness via their own factories.
// Verifies: EVS-PRD-portability/D — same contract realized by a second
//   concrete backend implementation. The harness is the source of truth
//   for the abstract StorageBackend contract; assertions are written
//   against the interface and exercised against this concrete impl.
// Verifies: EVS-DEV-find-all-events-extended-filters/A, EVS-DEV-find-all-events-extended-filters/B, EVS-DEV-find-all-events-extended-filters/C
//   — entryType + client-timestamp filters AND-compose on findAllEvents and
//   findAllEventsInTxn; exercised by the conformance harness 'findAllEvents
//   extended filters' group. (Assertion D — the single shared
//   _composeFindAllEventsFilter helper — is a SembastBackend structural
//   property the backend-agnostic harness cannot observe; it is covered by
//   find_all_events_shared_filter_test.dart.)
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
