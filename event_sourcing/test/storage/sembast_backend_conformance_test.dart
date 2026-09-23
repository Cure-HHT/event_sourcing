// SembastBackend runs the backend-agnostic conformance harness. Both
// SembastBackend and PostgresBackend call the same harness via their own
// factories, so the harness is the source of truth for the abstract
// StorageBackend contract; its assertions are cited on the harness's own
// tests rather than here.
@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/isolate_drain_lock.dart'
    show isolateDrainLockHeld;
import 'package:event_sourcing/src/storage/sembast_backend.dart'
    show SembastBackendTestSupport;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'storage_backend_conformance.dart';

void main() {
  // The factory and path each backend's database was opened through, so a
  // closed backend's database can be opened again.
  final opened = Expando<(DatabaseFactory, String)>();
  runStorageBackendConformanceTests(
    () async {
      final factory = newDatabaseFactoryMemory();
      final path = 'conformance-${DateTime.now().microsecondsSinceEpoch}.db';
      final backend = SembastBackend(
        database: await factory.openDatabase(path),
      );
      opened[backend] = (factory, path);
      return backend;
    },
    reopen: (closed) async {
      // The closed handle's registry entry is gone.
      expect(
        isolateDrainLockHeld((closed as SembastBackend).databaseForTesting),
        isFalse,
      );
      final (factory, path) = opened[closed]!;
      return SembastBackend(database: await factory.openDatabase(path));
    },
    backendLabel: 'sembast (memory)',
    securityStoreOf: (backend) =>
        SembastSecurityContextStore(backend: backend as SembastBackend),
  );
}
