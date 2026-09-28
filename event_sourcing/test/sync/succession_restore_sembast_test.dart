// Runs the scenarios of succession_restore_conformance.dart on Sembast (in
// memory).
//
// The scenarios' assertions are cited on their own tests in
// test_support/succession_restore_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart'
    show Database, newDatabaseFactoryMemory;

import '../test_support/succession_restore_conformance.dart';
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

class _SembastDatabase implements VersionTestDatabase {
  _SembastDatabase(this.db);

  final Database db;

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => db.close();
}

var _counter = 0;

Future<Database> _memoryDatabase() {
  _counter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'succession-restore-$_counter.db',
  );
}

void main() {
  runSuccessionRestoreScenarios(
    openDatabase: () async => _SembastDatabase(await _memoryDatabase()),
    backendLabel: 'sembast',
  );
}
