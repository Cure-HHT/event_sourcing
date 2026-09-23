// Runs the version-compatibility scenarios on Sembast, plus the fold's
// stored-target lowering on a backend that re-runs every transaction body.
// The scenarios' assertions are cited on their own tests in
// test_support/version_compatibility_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'test_support/rerunning_sembast_backend.dart';
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

  group('stored-target lowering on a re-run transaction body', () {
    // Verifies: EVS-DEV-version-compatibility/E
    test(
      'the committed run lowers the target once and appends one event',
      () async {
        final backend = await RerunningSembastBackend.openInMemory('versions');
        backend.rerunEnabled = false;
        final registry = EntryTypeRegistry();
        for (final definition in kSystemEntryTypes) {
          registry.register(definition);
        }
        registry.register(
          const EntryTypeDefinition(
            id: 'versioned_note',
            registeredVersion: EntryTypeVersion(1, 0),
            name: 'versioned_note',
          ),
        );
        final store = await EventStore.open(
          storage: backend,
          entryTypes: registry,
          source: const Source(
            hopId: 'versions-hop',
            identifier: 'versions-install',
            softwareVersion: 'versions-test',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: ProjectionRegistry()
            ..register(
              const AggregateProjectionSpec(
                viewName: 'versioned_notes',
                interest: SubscriptionFilter(
                  entryTypes: <String>{'versioned_note'},
                ),
                tombstoneEventTypes: <String>{},
              ),
            ),
        );
        await backend.transaction(
          (txn) => backend.writeViewTargetVersionInTxn(
            txn,
            'versioned_notes',
            'versioned_note',
            const EntryTypeVersion(1, 3),
          ),
        );
        final eventsBefore = (await backend.findAllEvents()).length;

        backend.rerunEnabled = true;
        await store.append(
          entryType: 'versioned_note',
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'a': 1},
          initiator: const UserInitiator('versions-user'),
        );
        backend.rerunEnabled = false;

        expect(
          await backend.transaction(
            (txn) => backend.readViewTargetVersionInTxn(
              txn,
              'versioned_notes',
              'versioned_note',
            ),
          ),
          const EntryTypeVersion(1, 0),
        );
        expect((await backend.findAllEvents()).length, eventsBefore + 1);
      },
    );
  });
}
