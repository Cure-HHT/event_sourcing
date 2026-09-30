// Runs the U+0000 append-refusal scenarios of event_record_nul_conformance.dart
// on Sembast (in memory).
//
// The scenarios' assertions are cited on their own tests in
// event_record_nul_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'event_record_nul_conformance.dart';

class _SembastNulDatabase implements VersionTestDatabase {
  _SembastNulDatabase(this._db);

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
  return _SembastNulDatabase(
    await newDatabaseFactoryMemory().openDatabase('nul-$_dbCounter.db'),
  );
}

void main() {
  runEventRecordNulScenarios(
    openDatabase: _memoryDatabase,
    backendLabel: 'sembast (memory)',
  );
}
