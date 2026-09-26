// Runs the ingest event-hash scenarios on Sembast. The scenarios'
// assertions are cited on their own tests in
// test_support/ingest_hash_conformance.dart. The coverage of each
// provenance entry's library version by the event hash is checked here.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart'
    show SembastBackendTestSupport;
import 'package:event_sourcing/src/verification/chain_walk.dart'
    show hashMismatchEvidence;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/ingest_hash_conformance.dart';
import '../test_support/queue_registry_conformance.dart';

class _SembastHashDatabase implements QueueTestDatabase {
  _SembastHashDatabase(this._db);

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
  // Verifies: EVS-DEV-event-record/I
  test('changing the library version of a provenance entry changes the '
      'event hash, and a copy so changed does not verify', () async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'ingest-hash-library-version.db',
    );
    final backend = SembastBackend(database: db);
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: EntryTypeRegistry()
        ..register(
          const EntryTypeDefinition(
            id: 'hash_note',
            registeredVersion: EntryTypeVersion(1, 0),
            name: 'hash_note',
          ),
        ),
      source: const Source(
        hopId: 'mobile-device',
        identifier: 'hash-install',
        softwareVersion: 'test@1.0.0',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
    addTearDown(db.close);
    addTearDown(store.close);
    final event = (await store.append(
      entryType: 'hash_note',
      aggregateId: 'note-1',
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{'title': 'one'},
      initiator: const UserInitiator('u1'),
    ))!;
    final record = Map<String, Object?>.from(event.toMap());
    final metadata = Map<String, Object?>.from(record['metadata']! as Map);
    final entry = Map<String, Object?>.from(
      (metadata['provenance']! as List).single as Map,
    );
    expect(entry['library_version'], LibVersion.version);
    expect(canonicalEventHash(record), event.eventHash);

    entry['library_version'] = '${LibVersion.version}-changed';
    metadata['provenance'] = <Map<String, Object?>>[entry];
    record['metadata'] = metadata;

    expect(canonicalEventHash(record), isNot(event.eventHash));
    expect(hashMismatchEvidence(StoredEvent.fromMap(record, 0)), isNotEmpty);
  });

  var counter = 0;
  runIngestHashScenarios(() async {
    counter += 1;
    final db = await newDatabaseFactoryMemory().openDatabase(
      'ingest-hash-$counter.db',
    );
    return _SembastHashDatabase(db);
  }, label: 'sembast');
}
