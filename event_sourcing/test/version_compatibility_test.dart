// Runs the version-compatibility scenarios on Sembast.
// The scenarios' assertions are cited on their own tests in
// test_support/version_compatibility_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'test_support/version_compatibility_conformance.dart';

class _SembastVersionDatabase implements VersionTestDatabase {
  _SembastVersionDatabase(this._db);

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

void main() {
  runVersionCompatibilityScenarios(() async {
    _dbCounter += 1;
    final db = await newDatabaseFactoryMemory().openDatabase(
      'versions-$_dbCounter.db',
    );
    return _SembastVersionDatabase(db);
  }, backendLabel: 'sembast (memory)');
}
