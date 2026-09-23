// Verifies: EVS-PRD-destinations/K
// an append publishes to live subscribers
//   only through the collector of the transaction run that commits it: a
//   collector whose run has ended is refused before any write, and nothing
//   is appended or published.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

Future<StoredEvent?> _appendNote(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
) => store.appendInTxn(
  txn,
  entryType: 'note',
  aggregateId: 'n1',
  aggregateType: 'Note',
  eventType: 'created',
  data: const <String, Object?>{'text': 'hello'},
  initiator: const UserInitiator('u1'),
  flowToken: null,
  metadata: null,
  security: null,
  checkpointReason: null,
  changeReason: null,
  dedupeByContent: false,
  collector: collector,
);

void main() {
  late SembastBackend backend;
  late EventStore store;
  late List<StoredEvent> delivered;

  setUp(() async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'collector-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    backend = SembastBackend(database: db);
    final registry = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: 1,
          name: 'note',
        ),
      );
    store = await EventStore.openForTest(
      storage: backend,
      entryTypes: registry,
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-00000000c011',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
    delivered = <StoredEvent>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value);
        });
    addTearDown(() async {
      await sub.cancel();
      await backend.close();
    });
  });

  test('an append through the collector of an ended run is refused; '
      'nothing is appended or published', () async {
    late PublishCollector leaked;
    await store.runTransaction<void>((txn, collector) async {
      leaked = collector;
    });
    final before = await backend.findAllEvents();
    final counterBefore = await backend.readSequenceCounter();

    await expectLater(
      store.runTransaction<StoredEvent?>(
        (txn, collector) => _appendNote(store, txn, leaked),
      ),
      throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('does not belong to this transaction run'),
        ),
      ),
    );
    await pumpEventQueue();

    expect(await backend.findAllEvents(), hasLength(before.length));
    expect(await backend.readSequenceCounter(), counterBefore);
    expect(delivered, isEmpty);
    expect(leaked.events, isEmpty);
  });

  test('an append through the collector the run received commits and is '
      'published once', () async {
    final appended = await store.runTransaction<StoredEvent?>(
      (txn, collector) => _appendNote(store, txn, collector),
    );
    await pumpEventQueue();

    final stored = await backend.findAllEvents();
    expect(stored.map((e) => e.eventId), contains(appended!.eventId));
    expect(delivered.map((e) => e.eventId).toList(), <String>[
      appended.eventId,
    ]);
  });
}
