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
// Verifies: EVS-PRD-event-log/E
// a re-run of a body whose earlier run wrote the table holding the sequence
//   counter waits behind that table's writers (it holds the table lock); a
//   re-run of a body that wrote nothing there takes no such lock.
// Verifies: EVS-PRD-subscription/C
// a transaction re-run after appending publishes its event once, and a
//   later commit on the same store is published after it, not held back.
// Verifies: EVS-PRD-subscription/E
// when the backend re-runs a transaction
//   body after a serialization conflict raised after the body appended, live
//   subscribers receive only the committed run's event, once, carrying its
//   committed sequence number.
// Verifies: EVS-PRD-event-log/G
// the re-run body's caller receives the
//   committed run's event, not the rolled-back run's.
// Verifies: EVS-PRD-destinations/K
// an append inside a second, concurrent
//   transaction that is handed the collector of another run is refused
//   before any write, so a rolled-back append is never published and the
//   outer run publishes nothing it did not append.
// Verifies: EVS-DEV-event-store-open/E
// the boot transaction is re-run after a serialization failure on a table
//   it does not lock, as long as bootLockWait has not passed, and then
//   throws TransactionRetryExhaustedException; a re-run boot records one
//   initialization.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
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
      backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
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
      var runs = 0;
      Future<int> appendOne(int i) => backend.transaction<int>((txn) async {
        runs += 1;
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

      // The race did provoke conflicts, and the backend absorbed them by
      // re-running the losing bodies: a backend that ran the transactions
      // one at a time would run each body exactly once.
      expect(
        runs,
        greaterThan(concurrency),
        reason: 'at least one body must have lost a race and been re-run',
      );

      // The transactions run at SERIALIZABLE isolation.
      final isolation = await backend.transaction<Object?>((txn) async {
        final rows = await (txn as PostgresTxn).session.execute(
          'SHOW transaction_isolation',
        );
        return rows.first[0];
      });
      expect(isolation, 'serializable');
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
      backendA = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
      backendB = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
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

        // A later commit on the same store is not held back by the re-run
        // transaction's publication.
        final laterSeen = Completer<String>();
        final sub2 = storeA
            .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
            .listen((u) {
              if (u is Delta<StoredEvent> &&
                  u.value.aggregateId == 'later' &&
                  !laterSeen.isCompleted) {
                laterSeen.complete(u.value.eventId);
              }
            });
        final later = await storeA.append(
          entryType: 'test_event',
          aggregateId: 'later',
          aggregateType: 'Test',
          eventType: 'created',
          data: const <String, Object?>{'k': 'v'},
          initiator: const UserInitiator('u1'),
        );
        expect(
          await laterSeen.future.timeout(const Duration(seconds: 5)),
          later!.eventId,
        );
        await sub2.cancel();
      },
    );

    /// Whether this transaction holds the table lock a re-run takes.
    Future<bool> holdsStateTableLock(Transaction txn) async {
      final rows = await (txn as PostgresTxn).session.execute(
        'SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation '
        "WHERE c.relname = 'backend_state' AND l.pid = pg_backend_pid() "
        "AND l.mode = 'ShareRowExclusiveLock' AND l.granted",
      );
      return (rows.first[0]! as int) > 0;
    }

    /// Runs [body] on backendA with a first run that loses a race on a
    /// shared view row to a contender on backendB; returns, per run, whether
    /// it held the table lock.
    Future<List<bool>> rerunOnce(
      Future<void> Function(Transaction txn) body,
    ) async {
      await backendA.transaction(
        (txn) => backendA.upsertViewRowInTxn(txn, _contentionView, 'y', {
          'writer': 'setup',
        }),
      );
      final locks = <bool>[];
      final firstRunWrote = Completer<void>();
      final contenderCommitted = Completer<void>();
      final run = backendA.transaction((txn) async {
        locks.add(await holdsStateTableLock(txn));
        await body(txn);
        if (locks.length == 1) {
          firstRunWrote.complete();
          await contenderCommitted.future;
        }
        await backendA.upsertViewRowInTxn(txn, _contentionView, 'y', {
          'writer': 'a',
        });
      });
      await firstRunWrote.future;
      await backendB.transaction(
        (txn) => backendB.upsertViewRowInTxn(txn, _contentionView, 'y', {
          'writer': 'contender',
        }),
      );
      contenderCommitted.complete();
      await run;
      return locks;
    }

    test('a re-run takes the table lock only when its earlier run wrote the '
        "sequence counter's table", () async {
      expect(
        await rerunOnce((txn) async {
          await backendA.nextSequenceNumber(txn);
        }),
        <bool>[false, true],
        reason: 'a body that wrote backend_state re-runs behind its writers',
      );
      expect(
        await rerunOnce((txn) async {
          await backendA.readFillCursorTxn(txn, 'nobody');
        }),
        <bool>[false, false],
        reason: 'a body that wrote nothing there takes no table lock',
      );
    });
  });

  group('EventStore collector binding across two transactions', () {
    late PostgresBackend backend;
    late EventStore store;

    setUp(() async {
      final conn = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
      backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        provisionSchema: true,
      );
      store = await _openStore(backend, 'aaaa0001-0000-4000-8000-00000000000b');
    });

    tearDown(() => backend.close());

    test("an append in another transaction through the outer run's "
        'collector is refused; nothing is appended or published', () async {
      final received = <StoredEvent>[];
      final sub = store
          .subscribe<StoredEvent>(const SubscriptionFilter(), const Events())
          .listen((u) {
            if (u is Delta<StoredEvent>) received.add(u.value);
          });
      final before = await backend.findAllEvents();
      final counterBefore = await backend.readSequenceCounter();

      Object? refusal;
      await store.runTransaction<void>((txnA, collectorA) async {
        try {
          await backend.transaction<void>((txnB) async {
            await store.appendInTxn(
              txnB,
              entryType: 'test_event',
              aggregateId: 'cross',
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
              collector: collectorA,
            );
          });
        } on Object catch (e) {
          refusal = e;
        }
      });
      await Future<void>.delayed(const Duration(milliseconds: 200));
      await sub.cancel();

      expect(
        refusal,
        isA<StateError>().having(
          (e) => e.message,
          'message',
          contains('does not belong to this transaction run'),
        ),
      );
      expect(await backend.findAllEvents(), hasLength(before.length));
      expect(await backend.readSequenceCounter(), counterBefore);
      expect(received, isEmpty);
    });
  });

  group('PostgresBackend boot transaction retry', () {
    final backends = <PostgresBackend>[];
    late Connection contender;

    setUp(() async {
      final conn = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
      contender = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
    });

    tearDown(() async {
      await contender.close();
      for (final backend in backends) {
        await backend.close();
      }
      backends.clear();
    });

    Future<PostgresBackend> openBackend({
      Duration bootLockWait = const Duration(seconds: 60),
    }) async {
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
        bootLockWait: bootLockWait,
        provisionSchema: true,
      );
      backends.add(backend);
      return backend;
    }

    // The contender updates the row the boot body writes, without
    // committing, so the body's write waits for it; the contender commits
    // once the body is seen waiting, which aborts the body's run with a
    // serialization failure.
    Future<void> contendOnViewRow(Future<Object?> boot) async {
      await contender.execute('BEGIN');
      await contender.execute(
        "UPDATE view_rows SET row_data = '{\"writer\": \"contender\"}' "
        "WHERE view_name = '$_contentionView' AND row_key = 'x'",
      );
      await _commitOnceBlocked(contender, boot);
    }

    Future<Object?> settle(Future<Object?> future) =>
        future.then<Object?>((value) => value, onError: (Object e) => e);

    test('a serialization failure after bootLockWait has passed throws '
        'TransactionRetryExhaustedException', () async {
      final backend = await openBackend(bootLockWait: Duration.zero);
      await backend.transaction(
        (txn) => backend.upsertViewRowInTxn(txn, _contentionView, 'x', {
          'writer': 'setup',
        }),
      );
      var runs = 0;
      final boot = settle(
        backend.bootTransaction<int>((txn) async {
          runs++;
          await backend.upsertViewRowInTxn(txn, _contentionView, 'x', {
            'writer': 'boot',
          });
          return runs;
        }),
      );
      await contendOnViewRow(boot);
      final outcome = await boot;

      expect(
        outcome,
        isA<TransactionRetryExhaustedException>()
            .having((e) => e.attempts, 'attempts', 1)
            .having((e) => e.lastError.code, 'lastError.code', '40001'),
      );
      expect(runs, 1);
      final row = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _contentionView, 'x'),
      );
      expect(row!['writer'], 'contender', reason: 'the boot committed nothing');
    });

    test('a serialization failure within bootLockWait re-runs the body, '
        'and the second run commits', () async {
      final backend = await openBackend();
      await backend.transaction(
        (txn) => backend.upsertViewRowInTxn(txn, _contentionView, 'x', {
          'writer': 'setup',
        }),
      );
      var runs = 0;
      final boot = settle(
        backend.bootTransaction<int>((txn) async {
          runs++;
          await backend.upsertViewRowInTxn(txn, _contentionView, 'x', {
            'writer': 'boot',
            'run': runs,
          });
          return runs;
        }),
      );
      await contendOnViewRow(boot);

      expect(await boot, 2);
      expect(runs, 2);
      final row = await backend.transaction(
        (txn) => backend.readViewRowInTxn(txn, _contentionView, 'x'),
      );
      expect(row, <String, Object?>{'writer': 'boot', 'run': 2});
    });

    test('EventStore.open re-runs a boot a serialization failure aborted, '
        'and records one initialization', () async {
      final backend = await openBackend();
      // The contender seeds the target the boot is about to seed, without
      // committing, so the boot's insert waits for it and then fails.
      await contender.execute('BEGIN');
      await contender.execute(
        'INSERT INTO view_target_versions '
        '(view_name, entry_type, target_major, target_minor) '
        "VALUES ('$_contentionView', 'test_event', 1, 0)",
      );
      var bootRuns = 0;
      final open = settle(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
          () => EventStore.open(
            storage: backend,
            entryTypes: EntryTypeRegistry()..register(_testEventDef()),
            source: const Source(
              hopId: 'test',
              identifier: 'aaaa0001-0000-4000-8000-00000000000c',
              softwareVersion: '0.0.0-test',
            ),
            securityContexts: PostgresSecurityContextStore(backend: backend),
            projections: ProjectionRegistry()
              ..register(
                const AggregateProjectionSpec(
                  viewName: _contentionView,
                  interest: SubscriptionFilter(
                    entryTypes: <String>{'test_event'},
                  ),
                  tombstoneEventTypes: <String>{},
                ),
              ),
          ),
        ),
      );
      await _commitOnceBlocked(contender, open);
      final store = await open;

      expect(store, isA<EventStore>());
      expect(bootRuns, 2);
      final initializations = await backend.findAllEvents(
        entryType: 'lib_version_initialized',
      );
      expect(initializations, hasLength(1));
      expect(
        initializations.single.data['database_id'],
        (store! as EventStore).databaseId,
      );
    });
  });
}

/// Commits [contender]'s open transaction once another session of this
/// database waits for a lock [contender] holds, or once [waiter] has
/// completed.
Future<void> _commitOnceBlocked(
  Connection contender,
  Future<Object?> waiter,
) async {
  var done = false;
  unawaited(waiter.whenComplete(() => done = true));
  final deadline = DateTime.now().add(const Duration(seconds: 10));
  while (!done) {
    final waiting = await contender.execute(
      'SELECT count(*) FROM pg_stat_activity '
      'WHERE datname = current_database() '
      'AND pg_backend_pid() = ANY(pg_blocking_pids(pid))',
    );
    if ((waiting.first[0]! as int) > 0) break;
    if (DateTime.now().isAfter(deadline)) {
      fail('no session waited for the contender');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  await contender.execute('COMMIT');
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
  entryTypeVersion: const EntryTypeVersion(1, 0),
  libFormatVersion: const DataFormatVersion(2, 0),
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
  registeredVersion: EntryTypeVersion(1, 0),
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
