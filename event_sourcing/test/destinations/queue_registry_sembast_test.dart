// Runs the queue-registry scenarios on Sembast. The scenarios' assertions are
// cited on their own tests in test_support/queue_registry_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart'
    show SembastBackendTestSupport;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/queue_registry_conformance.dart';

class _SembastQueueDatabase implements QueueTestDatabase {
  _SembastQueueDatabase(this._db);

  final Database _db;
  final List<SembastBackend> _backends = <SembastBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = SembastBackend(database: _db);
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<Set<String>> backendStateKeys() async {
    final keys = await StoreRef<String, Object?>(
      'backend_state',
    ).findKeys(_backends.first.databaseForTesting);
    return keys.toSet();
  }

  @override
  Future<void> close() => _db.close();
}

void main() {
  var counter = 0;
  runQueueRegistryScenarios(() async {
    counter += 1;
    final db = await newDatabaseFactoryMemory().openDatabase(
      'queue-registry-$counter.db',
    );
    return _SembastQueueDatabase(db);
  }, label: 'sembast');
}
