// The Sembast backend serves the chain lookups from the log: it keeps one
// record of the latest local sequence the database authored, and scans the
// events for every other lookup. The lookups themselves are exercised on
// both backends by the conformance harness in
// storage_backend_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

Future<(EventStore, Database)> _openStore(String identifier) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'sembast-chain-lookups-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
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
    source: Source(
      hopId: 'test',
      identifier: identifier,
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
  addTearDown(backend.close);
  return (store, db);
}

Future<StoredEvent> _appendNote(EventStore store, String text) async =>
    (await store.append(
      entryType: 'note',
      aggregateId: 'n-$text',
      aggregateType: 'Note',
      eventType: 'created',
      data: <String, Object?>{'text': text},
      initiator: const UserInitiator('u1'),
    ))!;

void main() {
  // Verifies: EVS-PRD-destinations/L
  // Verifies: EVS-DEV-chain-verification/B
  test('after appends and ingests the database holds no chain index store and '
      'exactly one record of the latest sequence it authored', () async {
    final (sender, _) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000011',
    );
    final (holder, holderDb) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000012',
    );
    final sent1 = await _appendNote(sender, 's1');
    final sent2 = await _appendNote(sender, 's2');
    final own1 = await _appendNote(holder, 'h1');
    await holder.ingestEvent(sent1);
    await holder.ingestEvent(sent2);
    final own2 = await _appendNote(holder, 'h2');
    expect(own2.previousEventHash, own1.eventHash);

    final chainIndex = StoreRef<String, Object?>('chain_index');
    expect(await chainIndex.count(holderDb), 0);

    final state = await StoreRef<String, Object?>(
      'backend_state',
    ).find(holderDb);
    final latestAuthored = state
        .where((r) => r.key.startsWith('latest_authored'))
        .toList();
    expect(latestAuthored, hasLength(1));
    expect(latestAuthored.single.value, own2.sequenceNumber);
  });

  // Verifies: EVS-DEV-chain-verification/B
  test('every event is stored under its local sequence number, and each '
      'append links the latest event the database authored, read by that '
      'key', () async {
    final (sender, _) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000021',
    );
    final (holder, holderDb) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000022',
    );
    final own1 = await _appendNote(holder, 'h1');
    await holder.ingestEvent(await _appendNote(sender, 's1'));
    final own2 = await _appendNote(holder, 'h2');
    await holder.ingestEvent(await _appendNote(sender, 's2'));
    await holder.ingestEvent(await _appendNote(sender, 's3'));
    final own3 = await _appendNote(holder, 'h3');
    expect(own2.previousEventHash, own1.eventHash);
    expect(own3.previousEventHash, own2.eventHash);

    final events = intMapStoreFactory.store('events');
    for (final r in await events.find(holderDb)) {
      expect(r.key, r.value['sequence_number'], reason: 'keyed by sequence');
    }
  });

  // Verifies: EVS-DEV-chain-verification/B
  test('an append refuses when no event is stored under the recorded latest '
      'authored sequence', () async {
    final (holder, holderDb) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000031',
    );
    final own1 = await _appendNote(holder, 'h1');
    final events = intMapStoreFactory.store('events');
    await holderDb.transaction((txn) async {
      final value = await events.record(own1.sequenceNumber).get(txn);
      await events.record(own1.sequenceNumber).delete(txn);
      await events.record(own1.sequenceNumber + 1000).put(txn, value!);
    });
    await expectLater(_appendNote(holder, 'h2'), throwsStateError);
  });

  // Verifies: EVS-DEV-chain-verification/B
  test('an append refuses when the event stored under the recorded latest '
      'authored sequence carries another sequence', () async {
    final (holder, holderDb) = await _openStore(
      'aaaa0001-0000-4000-8000-000000000041',
    );
    final own1 = await _appendNote(holder, 'h1');
    final events = intMapStoreFactory.store('events');
    await holderDb.transaction((txn) async {
      final value = await events.record(own1.sequenceNumber).get(txn);
      await events.record(own1.sequenceNumber).put(txn, <String, Object?>{
        ...value!,
        'sequence_number': own1.sequenceNumber + 1000,
      });
    });
    await expectLater(_appendNote(holder, 'h2'), throwsStateError);
  });
}
