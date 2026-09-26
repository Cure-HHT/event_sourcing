// An append finds its predecessor in the chain index: it reads neither
// the aggregate's history nor the log to find the latest event its
// database holds as authored.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

/// A [SembastBackend] that, while [armed], records each read of an
/// aggregate's history or of the log, by method name.
class _ReadSpyBackend extends SembastBackend {
  _ReadSpyBackend({required super.database});

  bool armed = false;
  final List<String> logReads = <String>[];

  void _record(String name) {
    if (armed) logReads.add(name);
  }

  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) {
    _record('findEventsForAggregate');
    return super.findEventsForAggregate(aggregateId);
  }

  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) {
    _record('findEventsForAggregateInTxn');
    return super.findEventsForAggregateInTxn(txn, aggregateId);
  }

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) {
    _record('findAllEvents');
    return super.findAllEvents(
      afterSequence: afterSequence,
      limit: limit,
      originatorHopId: originatorHopId,
      originatorIdentifier: originatorIdentifier,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
  }

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) {
    _record('findAllEventsInTxn');
    return super.findAllEventsInTxn(
      txn,
      afterSequence: afterSequence,
      limit: limit,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
  }

  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) {
    _record('readEventsReverse');
    return super.readEventsReverse(eventTypes: eventTypes);
  }

  @override
  Stream<StoredEvent> readEventsReverseInTxn(
    Transaction txn, {
    Set<String>? eventTypes,
  }) {
    _record('readEventsReverseInTxn');
    return super.readEventsReverseInTxn(txn, eventTypes: eventTypes);
  }
}

void main() {
  // Verifies: EVS-DEV-chain-verification/N
  // Verifies: EVS-DEV-chain-verification/B
  test('an append reads its predecessor from the chain index, reading no '
      'aggregate history and no log', () async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'chain-index-reads-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    final backend = _ReadSpyBackend(database: db);
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
        identifier: 'aaaa0001-0000-4000-8000-000000000003',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );

    Future<StoredEvent?> appendNote(String text) => store.append(
      entryType: 'note',
      aggregateId: 'n1',
      aggregateType: 'Note',
      eventType: 'created',
      data: <String, Object?>{'text': text},
      initiator: const UserInitiator('u1'),
    );

    final first = await appendNote('one');
    backend.armed = true;
    final second = await appendNote('two');
    backend.armed = false;

    expect(backend.logReads, isEmpty);
    expect(second!.previousEventHash, first!.eventHash);
  });
}
