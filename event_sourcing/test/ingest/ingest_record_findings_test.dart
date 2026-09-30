// Runs the scenarios of ingest_record_findings_conformance.dart on Sembast
// (in memory).
//
// The scenarios' assertions are cited on their own tests in
// test_support/ingest_record_findings_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/ingest_record_findings_conformance.dart';
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

class _SembastDatabase implements VersionTestDatabase {
  _SembastDatabase(this._db);

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

var _counter = 0;

void main() {
  runIngestRecordFindingScenarios(
    openDatabase: () async {
      _counter += 1;
      return _SembastDatabase(
        await newDatabaseFactoryMemory().openDatabase(
          'ingest-record-findings-$_counter.db',
        ),
      );
    },
    backendLabel: 'sembast',
  );
}
