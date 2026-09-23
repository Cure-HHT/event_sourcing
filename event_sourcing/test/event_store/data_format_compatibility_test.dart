// Runs the boot scenarios of EventStore.open on Sembast. The scenarios'
// assertions are cited on their own tests in
// test_support/boot_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/boot_conformance.dart';
import '../test_support/rerunning_sembast_backend.dart';

class _SembastBootDatabase implements BootTestDatabase {
  _SembastBootDatabase(this._db);

  final Database _db;

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> rewriteStoredDatabaseId(String? value) async {
    final record = StoreRef<String, Object?>(
      'backend_state',
    ).record('database_id');
    if (value == null) {
      await record.delete(_db);
    } else {
      await record.put(_db, value);
    }
  }

  @override
  Future<void> writeEarlierFormatShape() async {
    // An event record as an earlier data format stored it: its entry-type
    // and data-format versions are single integers.
    final record = StoredEvent.synthetic(
      eventId: 'earlier-format-event',
      aggregateId: '_lib',
      aggregateType: '_lib',
      entryType: 'lib_version_initialized',
      eventType: 'lib_version_initialized',
      sequenceNumber: 1,
      eventHash: 'earlier-format-hash',
      initiator: const AutomationInitiator(service: 'event_sourcing'),
      clientTimestamp: DateTime.utc(2026, 3),
      data: const <String, dynamic>{'version': '0.3.1'},
    ).toMap();
    record['entry_type_version'] = 1;
    record['lib_format_version'] = 1;
    await intMapStoreFactory.store('events').add(_db, record);
    await StoreRef<String, Object?>(
      'backend_state',
    ).record('sequence_counter').put(_db, 1);
  }

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _dbCounter = 0;

void main() {
  runBootScenarios(() async {
    _dbCounter += 1;
    final db = await newDatabaseFactoryMemory().openDatabase(
      'boot-$_dbCounter.db',
    );
    return _SembastBootDatabase(db);
  }, backendLabel: 'sembast (memory)');

  group('an earlier data format on sembast', () {
    // Verifies: EVS-DEV-event-store-open/F
    test('an integer view target with no events is refused with the reset '
        'error before any write', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'boot-integer-target.db',
      );
      addTearDown(db.close);
      await stringMapStoreFactory
          .store('view_target_versions')
          .record('boot_notes::boot_note')
          .put(db, <String, Object?>{
            'view_name': 'boot_notes',
            'entry_type': 'boot_note',
            'target_version': 1,
          });
      final backend = SembastBackend(database: db);
      await expectLater(
        openBootStoreForTest(_SembastBootDatabase(db), backend),
        throwsA(isA<DatabaseResetRequiredError>()),
      );
      final (id, check) = await backend.transaction(
        (txn) async => (
          await backend.readDatabaseIdTxn(txn),
          await backend.readBootCheckTxn(txn),
        ),
      );
      expect(id, isNull);
      expect(check, isNull);
      expect(await backend.findAllEvents(), isEmpty);
    });
  });

  group('the boot body on a backend that re-runs it', () {
    // Verifies: EVS-PRD-event-log/G
    // Verifies: EVS-DEV-event-store-open/E
    // Verifies: EVS-DEV-event-store-open/F
    test('a discarded run leaves nothing: one initialization, and the '
        'identity the open reports is the committed one', () async {
      final backend = await RerunningSembastBackend.openInMemory('boot');
      var bodyRuns = 0;
      final store = await runWithDeliveryTestHooks(
        DeliveryTestHooks(onBootBodyRun: () => bodyRuns++),
        () => EventStore.open(
          storage: backend,
          entryTypes: EntryTypeRegistry(),
          source: const Source(
            hopId: 'rerun',
            identifier: 'rerun',
            softwareVersion: 'rerun',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
        ),
      );
      expect(bodyRuns, 2, reason: 'the boot body ran twice');
      final initialized = await backend.findAllEvents(
        entryType: 'lib_version_initialized',
      );
      expect(initialized, hasLength(1));
      final stored = await backend.transaction(backend.readDatabaseIdTxn);
      expect(store.databaseId, stored);
      expect(initialized.single.data['database_id'], stored);
      await backend.close();
    });
  });
}
