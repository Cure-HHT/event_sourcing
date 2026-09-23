// Verifies: EVS-PRD-subscription/E
// a backend that starts a second run of a
//   transaction body while the first is still in progress breaks the
//   sequential-runs contract of StorageBackend.transaction; the event store
//   refuses it, so the transaction commits nothing and nothing is published.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

/// A [SembastBackend] that, once [overlap] is set, runs every body twice at
/// once inside one transaction.
class _OverlappingSembastBackend extends SembastBackend {
  _OverlappingSembastBackend({required super.database});

  bool overlap = false;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    if (!overlap) return super.transaction<T>(body);
    return super.transaction<T>((txn) async {
      final results = await Future.wait<T>([body(txn), body(txn)]);
      return results.last;
    });
  }
}

void main() {
  test('overlapping runs of one body are refused; nothing commits or '
      'is published', () async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'overlap-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    final backend = _OverlappingSembastBackend(database: db);
    addTearDown(backend.close);
    final registry = EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'note',
        ),
      );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: registry,
      source: const Source(
        hopId: 'test',
        identifier: 'aaaa0001-0000-4000-8000-000000000002',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
    final before = await backend.findAllEvents();

    final delivered = <StoredEvent>[];
    final sub = store
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value);
        });
    await pumpEventQueue();
    backend.overlap = true;

    await expectLater(
      store.append(
        entryType: 'note',
        aggregateId: 'n1',
        aggregateType: 'Note',
        eventType: 'created',
        data: const <String, Object?>{'text': 'hello'},
        initiator: const UserInitiator('u1'),
      ),
      throwsA(isA<StateError>()),
    );
    await pumpEventQueue();
    await sub.cancel();

    backend.overlap = false;
    final after = await backend.findAllEvents();
    expect(
      after.map((e) => e.eventId),
      before.map((e) => e.eventId),
      reason: 'the refused transaction committed nothing',
    );
    expect(delivered, isEmpty, reason: 'nothing was published');
  });
}
