// SembastBackend runs the backend-agnostic conformance harness. Both
// SembastBackend and PostgresBackend call the same harness via their own
// factories, so the harness is the source of truth for the abstract
// StorageBackend contract; its assertions are cited on the harness's own
// tests rather than here.
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
