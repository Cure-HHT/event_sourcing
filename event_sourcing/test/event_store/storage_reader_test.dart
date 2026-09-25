// Runs the storage-reader scenarios of storage_reader_conformance.dart on
// Sembast (in memory).
//
// The scenarios' assertions are cited on their own tests in
// storage_reader_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'storage_reader_conformance.dart';

class _SembastReaderDatabase implements VersionTestDatabase {
  _SembastReaderDatabase(this._db);

  final Database _db;

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _dbCounter = 0;

Future<VersionTestDatabase> _memoryDatabase() async {
  _dbCounter += 1;
  return _SembastReaderDatabase(
    await newDatabaseFactoryMemory().openDatabase('reader-$_dbCounter.db'),
  );
}

void main() {
  runStorageReaderScenarios(
    openDatabase: _memoryDatabase,
    openOtherDatabase: _memoryDatabase,
    backendLabel: 'sembast (memory)',
  );
}
