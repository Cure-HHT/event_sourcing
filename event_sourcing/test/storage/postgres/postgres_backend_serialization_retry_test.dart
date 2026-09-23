// Verifies: EVS-PRD-event-log/E
// concurrent appends against the Postgres
// backend make progress: the per-transaction reserve-and-increment of the
// global sequence counter under SERIALIZABLE isolation provokes SQLSTATE 40001
// (serialization_failure) on the losers of each race, and PostgresBackend
// .transaction's bounded retry re-runs each loser to completion so NO 40001
// escapes to the caller and every append is assigned a distinct, gapless
// sequence number. Gated on PG_TEST_URL; inert where no Postgres is available.
// Verifies: EVS-DEV-postgres-backend/C
// transaction<T> runs at SERIALIZABLE
//   isolation (conflicting concurrent txns retry/serialize); rollback on throw,
//   commit on return, handle invalidated after body.
// Verifies: EVS-PRD-subscription/E
// when the backend re-runs a transaction
//   body after a serialization conflict raised after the body appended, live
//   subscribers receive only the committed run's event, once, carrying its
//   committed sequence number.
// Verifies: EVS-PRD-event-log/G
// the re-run body's caller receives the
//   committed run's event, not the rolled-back run's.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }

  group('PostgresBackend serialization-conflict retry', () {
    late PostgresBackend backend;

    setUp(() async {
      // Clean slate so the counter starts at 0 and the assertions on the
      // final counter / contiguous sequence numbers are exact.
      final conn = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
      backend = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
    });

    tearDown(() => backend.close());

    test('concurrent appends all succeed (no 40001 escapes) and get '
        'distinct, gapless sequence numbers', () async {
      const concurrency = 12;

      // Each future opens its own SERIALIZABLE transaction that reserves a
      // sequence number and appends an event. Because every transaction
      // read-modify-writes the single global counter row, Postgres aborts the
      // losers with 40001 — which the backend's retry loop must absorb. A
      // micro-yield between the reserve and the append widens the conflict
      // window so the race is reliably exercised.
      Future<int> appendOne(int i) => backend.transaction<int>((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await Future<void>.delayed(Duration.zero);
        await backend.appendEvent(txn, _event('e$i-$seq', seq));
        return seq;
      });

      final seqs = await Future.wait([
        for (var i = 0; i < concurrency; i++) appendOne(i),
      ]);

      // No 40001 escaped (Future.wait would have rejected). Every reservation
      // is distinct and the set is exactly 1..concurrency — contiguous, no
      // gaps, no reuse — proving each retried transaction re-reserved cleanly.
      expect(
        seqs.toSet(),
        hasLength(concurrency),
        reason: 'sequence numbers must be distinct across concurrent appends',
      );
      expect(
        seqs.toSet(),
        equals({for (var n = 1; n <= concurrency; n++) n}),
        reason: 'reservations must be contiguous 1..N with no gaps',
      );

      // The durable counter and the persisted event count both reflect every
      // append having actually committed.
      expect(await backend.readSequenceCounter(), equals(concurrency));
      final all = await backend.findAllEvents();
      expect(all, hasLength(concurrency));
    });
  });

  group('EventStore publication across a retried transaction', () {
    late PostgresBackend backendA;
    late PostgresBackend backendB;
    late EventStore storeA;

    setUp(() async {
      final conn = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
      backendA = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
      backendB = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
      storeA = await _openStore(
        backendA,
        'aaaa0001-0000-4000-8000-00000000000a',
      );
    });

    tearDown(() async {
      await backendA.close();
      await backendB.close();
    });

    test(
      'a body that appends and then hits a serialization conflict is '
      're-run, and only the committed run is published and returned',
      () async {
        // A view row both transactions write. It exists before the test so
        // both writes are updates of one row.
        await backendA.transaction(
          (txn) => backendA.upsertViewRowInTxn(txn, _contentionView, 'x', {
            'writer': 'setup',
          }),
        );

        final received = <StoredEvent>[];
        final sub = storeA
            .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
            .listen((u) {
              if (u is Delta<StoredEvent>) received.add(u.value);
            });

        // The first run appends (so its collector holds an event), then waits
        // while a contender on another connection updates the shared row and
        // commits. The first run's own update of that row then fails with
        // 40001, after the append, and the backend re-runs the body.
        var bodyRuns = 0;
        final appendedInFirstRun = Completer<void>();
        final contenderCommitted = Completer<void>();
        final appendFuture = storeA.runTransaction<StoredEvent?>((
          txn,
          collector,
        ) async {
          bodyRuns += 1;
          final event = await storeA.appendInTxn(
            txn,
            entryType: 'test_event',
            aggregateId: 'contended',
            aggregateType: 'Test',
            eventType: 'created',
            data: const <String, Object?>{'k': 'v'},
            initiator: const UserInitiator('u1'),
            flowToken: null,
            metadata: null,
            security: null,
            checkpointReason: null,
            changeReason: null,
            dedupeByContent: false,
            collector: collector,
          );
          if (bodyRuns == 1) {
            appendedInFirstRun.complete();
            await contenderCommitted.future;
          }
          await backendA.upsertViewRowInTxn(txn, _contentionView, 'x', {
            'writer': 'store-a',
            'run': bodyRuns,
          });
          return event;
        });

        await appendedInFirstRun.future;
        await backendB.transaction(
          (txn) => backendB.upsertViewRowInTxn(txn, _contentionView, 'x', {
            'writer': 'contender',
          }),
        );
        contenderCommitted.complete();
        final returned = await appendFuture;

        expect(bodyRuns, 2, reason: 'the conflict must re-run the body');
        final stored = (await backendA.findAllEvents())
            .where((e) => e.aggregateId == 'contended')
            .toList();
        expect(stored, hasLength(1), reason: 'only the committed run appended');
        final committed = stored.single;
        expect(returned!.eventId, committed.eventId);
        expect(returned.sequenceNumber, committed.sequenceNumber);

        await _waitForCount(received, 1);
        // A duplicate published late would arrive within this quiet period.
        await Future<void>.delayed(const Duration(milliseconds: 200));
        await sub.cancel();
        final delivered = received
            .where((e) => e.aggregateId == 'contended')
            .toList();
        expect(
          delivered.map((e) => e.eventId).toList(),
          [committed.eventId],
          reason: 'exactly one delivery, of the committed run',
        );
        expect(delivered.single.sequenceNumber, committed.sequenceNumber);
      },
    );
  });
}

const _contentionView = 'contention';

/// Waits until [items] holds at least [count] entries, failing after a
/// bounded timeout.
Future<void> _waitForCount(List<Object?> items, int count) async {
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  while (items.length < count) {
    if (DateTime.now().isAfter(deadline)) {
      fail('expected $count deliveries, saw ${items.length}');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

StoredEvent _event(String eventId, int sequenceNumber) => StoredEvent(
  key: 0,
  eventId: eventId,
  aggregateId: 'agg-1',
  aggregateType: 'note',
  entryType: 'epistaxis_event',
  entryTypeVersion: 1,
  libFormatVersion: 1,
  eventType: 'Event',
  sequenceNumber: sequenceNumber,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026, 4, 22),
  eventHash: 'hash-$eventId',
);

EntryTypeDefinition _testEventDef() => const EntryTypeDefinition(
  id: 'test_event',
  registeredVersion: 1,
  name: 'test_event',
);

Future<EventStore> _openStore(PostgresBackend backend, String installId) {
  final registry = EntryTypeRegistry()..register(_testEventDef());
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: Source(
      hopId: 'test',
      identifier: installId,
      softwareVersion: '0.0.0-test',
    ),
    securityContexts: PostgresSecurityContextStore(backend: backend),
  );
}
