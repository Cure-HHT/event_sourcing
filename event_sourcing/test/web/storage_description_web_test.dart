// Verifies: EVS-DEV-storage-capability/B
// in the browser a Sembast browser description is opened with the browser
//   (IndexedDB) factory the library selects: another tab's independent
//   IndexedDB factory reads what the event store wrote.
// Verifies: EVS-DEV-storage-capability/L
// the delete of a browser database refuses while an event store of this
//   isolate holds it open, and deletes it once the store closed.
// Verifies: EVS-PRD-storage-barrier/H
// closing the event store closes the IndexedDB database the library opened,
//   so its deletion is not blocked.

@TestOn('browser')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
// ignore: implementation_imports, builds an independent factory to model another tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

const _noteType = EntryTypeDefinition(
  id: 'description_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Description note',
);

Future<EventStore> _open(SembastStorage storage) => EventStore.open(
  storage: storage,
  entryTypes: EntryTypeRegistry()..register(_noteType),
  source: const Source(
    hopId: 'web-description',
    identifier: 'web-description-install',
    softwareVersion: 'web-description@1',
  ),
);

/// The note events another tab, through its own IndexedDB factory, reads
/// from the database [name].
Future<int> _notesSeenByAnotherTab(String name) async {
  final db = await DatabaseFactoryWeb(
    JdbFactoryWeb(idbFactoryWeb),
  ).openDatabase(name);
  final backend = SembastBackend(database: db);
  try {
    return (await backend.findAllEvents(entryType: _noteType.id)).length;
  } finally {
    await backend.close();
  }
}

void main() {
  test('a browser description opens an IndexedDB database the library '
      'closes and deletes', () async {
    final name = 'storage-description-${DateTime.now().microsecondsSinceEpoch}';
    final storage = SembastStorage.browser(name);

    final store = await _open(storage);
    await store.append(
      entryType: _noteType.id,
      aggregateId: 'n-1',
      aggregateType: 'DescriptionNote',
      eventType: 'written',
      data: const <String, Object?>{'text': 'hello'},
      initiator: const UserInitiator('web-user'),
    );
    await expectLater(deleteSembastDatabase(storage), throwsStateError);
    await store.close();

    expect(await _notesSeenByAnotherTab(name), 1);

    await deleteSembastDatabase(storage);
    expect(await _notesSeenByAnotherTab(name), 0);
    await deleteSembastDatabase(storage);
  });
}
