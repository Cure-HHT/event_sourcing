// Verifies: EVS-PRD-subscription/E
// when sembast_web re-runs a transaction
//   body because another tab committed first, live subscribers and FIFO
//   watchers see only the committed run's writes, once each, and the
//   delivered event carries its committed sequence number.
//
// Two tab models share one IndexedDB database. Each opens it through its own
// independently built sembast_web factory, so each holds its own sembast
// Database, transaction lock and revision, as two browser tabs do.

@TestOn('browser')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
import 'package:sembast/sembast.dart' show Database;
// ignore: implementation_imports, builds a second independent factory to model a second tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

import '../test_support/fifo_entry_helpers.dart';

EntryTypeDefinition _testEventDef() => const EntryTypeDefinition(
  id: 'test_event',
  registeredVersion: 1,
  name: 'test_event',
);

/// Opens [dbName] through a freshly built web factory, as one tab would.
Future<Database> _openAsTab(String dbName) =>
    DatabaseFactoryWeb(JdbFactoryWeb(idbFactoryWeb)).openDatabase(dbName);

Future<EventStore> _openStore(SembastBackend backend, String installId) {
  final registry = EntryTypeRegistry()..register(_testEventDef());
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: Source(
      hopId: 'test',
      identifier: installId,
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

/// Waits until [condition] holds, failing after a bounded timeout.
Future<void> _waitUntil(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('timed out waiting for the expected deliveries');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  test('a body re-run after another tab commits publishes only the '
      'committed run, once', () async {
    final dbName = 'rerun-${DateTime.now().microsecondsSinceEpoch}.db';
    final backend1 = SembastBackend(database: await _openAsTab(dbName));
    final backend2 = SembastBackend(database: await _openAsTab(dbName));
    addTearDown(() async {
      await backend1.close();
      await backend2.close();
    });
    final store1 = await _openStore(
      backend1,
      'aaaa0001-0000-4000-8000-00000000000a',
    );
    final store2 = await _openStore(
      backend2,
      'aaaa0001-0000-4000-8000-00000000000b',
    );

    final delivered = <StoredEvent>[];
    final eventSub = store2
        .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
        .listen((u) {
          if (u is Delta<StoredEvent>) delivered.add(u.value);
        });
    final fifoSnapshots = <List<FifoEntry>>[];
    final fifoSub = backend2.watchFifo('D').listen(fifoSnapshots.add);
    await pumpEventQueue();
    expect(fifoSnapshots, hasLength(1), reason: 'initial FIFO snapshot');

    var bodyRuns = 0;
    final reachedGate = Completer<void>();
    final gate = Completer<void>();
    final tab2Txn = store2.runTransaction<StoredEvent?>((txn, collector) async {
      bodyRuns += 1;
      final event = await store2.appendInTxn(
        txn,
        entryType: 'test_event',
        aggregateId: 'tab-2',
        aggregateType: 'Test',
        eventType: 'created',
        data: const <String, Object?>{'tab': 2},
        initiator: const UserInitiator('u2'),
        flowToken: null,
        metadata: null,
        security: null,
        checkpointReason: null,
        changeReason: null,
        dedupeByContent: false,
        collector: collector,
      );
      await backend2.enqueueFifoTxn(txn, 'D', [
        event!,
      ], wirePayload: wirePayloadJson(const <String, Object?>{'ok': true}));
      if (!reachedGate.isCompleted) reachedGate.complete();
      await gate.future;
      return event;
    });

    // Tab 1 commits while tab 2's transaction is open, so tab 2's commit
    // finds the stored revision changed and sembast_web re-runs its body.
    await reachedGate.future;
    await store1.append(
      entryType: 'test_event',
      aggregateId: 'tab-1',
      aggregateType: 'Test',
      eventType: 'created',
      data: const <String, Object?>{'tab': 1},
      initiator: const UserInitiator('u1'),
    );
    gate.complete();
    final committed = await tab2Txn;
    await _waitUntil(() => delivered.isNotEmpty && fifoSnapshots.length > 1);
    // A duplicate published late would arrive within this quiet period.
    await Future<void>.delayed(const Duration(milliseconds: 200));
    await eventSub.cancel();
    await fifoSub.cancel();

    expect(
      bodyRuns,
      2,
      reason: 'the test must exercise a sembast_web body re-run',
    );
    final stored = await backend2.findAllEvents();
    final storedTab2 = stored.where((e) => e.aggregateId == 'tab-2').single;
    expect(committed!.sequenceNumber, storedTab2.sequenceNumber);

    expect(
      delivered.map((e) => e.eventId).toList(),
      [storedTab2.eventId],
      reason: 'exactly one delivery, of the committed run',
    );
    expect(delivered.single.sequenceNumber, storedTab2.sequenceNumber);

    final liveSnapshots = fifoSnapshots.skip(1).toList();
    expect(
      liveSnapshots,
      hasLength(1),
      reason: 'the FIFO watcher is notified once for one committed txn',
    );
    expect(await backend2.listFifoEntries('D'), hasLength(1));
  });
}
