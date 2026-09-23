// The drain lock in the browser: a Sembast database there grants no drain
// lock, because the tabs of an origin share the database and the isolate
// registry excludes none of them from another. A delivery cycle therefore
// refuses to start, and leaves nothing started. Two tab models share one
// IndexedDB database, each opening it through its own independently built
// sembast_web factory, as two browser tabs do.

// Verifies: EVS-DEV-destination-drain-lock/A
// a Sembast database in the browser grants no drain lock: the acquisition
//   is refused as a misconfiguration in every tab model, before any epoch is
//   raised.
// Verifies: EVS-DEV-destination-drain-lock/C
// a start refused as a misconfiguration throws and leaves nothing started:
//   the trigger slot is empty, the in-isolate registration is gone (a second
//   start is refused the same way, not as a second cycle), and an append
//   raises nothing.
@TestOn('browser')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
import 'package:sembast/sembast.dart' as sembast;
// ignore: implementation_imports, builds a second independent factory to model a second tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

const _kNote = 'web_drain_note';

Future<sembast.Database> _openDatabase(String dbName) =>
    DatabaseFactoryWeb(JdbFactoryWeb(idbFactoryWeb)).openDatabase(dbName);

Future<EventStore> _openTab(String dbName) async {
  final backend = SembastBackend(database: await _openDatabase(dbName));
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    const EntryTypeDefinition(
      id: _kNote,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kNote,
    ),
  );
  return EventStore.open(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'web-hop',
      identifier: 'web-install',
      softwareVersion: 'web-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

void main() {
  test('a delivery cycle refuses to start in the browser', () async {
    final dbName = 'drain-lock-web-${DateTime.now().microsecondsSinceEpoch}';
    final tab1 = await _openTab(dbName);
    final tab2 = await _openTab(dbName);
    try {
      for (final tab in <EventStore>[tab1, tab2]) {
        final registry = DestinationRegistry(eventStore: tab);
        for (var attempt = 0; attempt < 2; attempt++) {
          await expectLater(
            SyncCycle.start(
              registry: registry,
              cadence: const Duration(hours: 1),
            ),
            throwsA(
              isA<DrainLockConfigurationException>().having(
                (e) => e.message,
                'message',
                contains('browser'),
              ),
            ),
          );
        }
        expect(tab.deliveryTrigger, isNull);
        await expectLater(
          tab.backend.tryAcquireDrainLock(databaseId: tab.databaseId),
          throwsA(isA<DrainLockConfigurationException>()),
        );
        expect(
          await tab.backend.transaction(tab.backend.readDrainEpochTxn),
          isNull,
          reason: 'no epoch was raised',
        );
        await tab.append(
          entryType: _kNote,
          aggregateId: 'n',
          aggregateType: 'note',
          eventType: 'noted',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u'),
        );
      }
    } finally {
      await tab1.close();
      await tab2.close();
    }
  });
}
