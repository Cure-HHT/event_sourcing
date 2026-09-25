// The drain lock on Postgres, across processes: a delivery cycle in this
// isolate (A) and one in a spawned isolate (B, test_support/
// drain_isolate_harness.dart), each with its own PostgresBackend, pool and
// lock session. Covers standby and takeover, fencing of a replaced holder's
// transactions, re-acquisition after a lost lock session, the lock-session
// requirements the drain lock rests on, and a fenced backend. Gated on
// PG_TEST_URL; files that reset the schema run one at a time. Each test
// spawns a second drainer isolate, which on a CI runner can take longer
// than the default 30-second limit, so the limit is raised.

@TestOn('vm')
@Timeout(Duration(minutes: 3))
library;

import 'dart:async';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart'
    show postgresDrainKey;
import 'package:event_sourcing/src/storage/postgres/postgres_lock_session.dart'
    show PostgresScope;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/delivery_cycle_conformance.dart'
    show Receiver, until;
import '../../test_support/drain_isolate_harness.dart';
import '../../test_support/hand_driven_cycle.dart';
import '../../test_support/manual_timers.dart';
import 'test_postgres_url.dart';

Future<T> _withConnection<T>(
  PostgresTestDatabase db,
  Future<T> Function(Connection c) body,
) async {
  final c = await db.connectAdmin();
  try {
    return await body(c);
  } finally {
    await c.close();
  }
}

/// Ends the server session [pid] and waits until it is gone.
Future<void> _terminate(PostgresTestDatabase db, int pid) =>
    _withConnection(db, (c) async {
      await c.execute(
        Sql.named('SELECT pg_terminate_backend(@p)'),
        parameters: <String, Object?>{'p': pid},
      );
      for (var i = 0; i < 200; i++) {
        final r = await c.execute(
          Sql.named('SELECT count(*) FROM pg_stat_activity WHERE pid = @p'),
          parameters: <String, Object?>{'p': pid},
        );
        if (r.first[0] == 0) return;
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
    });

/// The pids that hold the advisory lock [key] in the current database.
Future<Set<int>> _holdersOf(PostgresTestDatabase db, int key) =>
    _withConnection(db, (c) async {
      final r = await c.execute(
        Sql.named('''
          SELECT pid FROM pg_locks
          WHERE locktype = 'advisory' AND objsubid = 1 AND granted
            AND ((classid::bigint << 32) | objid::bigint) = @k
            AND database = (SELECT oid FROM pg_database
                            WHERE datname = current_database())
        '''),
        parameters: <String, Object?>{'k': key},
      );
      return <int>{for (final row in r) row[0]! as int};
    });

/// The number of sessions of the current database waiting on a lock while
/// running a statement that contains [fragment].
Future<int> _lockWaitersRunning(
  PostgresTestDatabase db,
  String fragment,
) => _withConnection(db, (c) async {
  final r = await c.execute(
    Sql.named(
      "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
      'AND datname = current_database() AND position(@f in query) > 0',
    ),
    parameters: <String, Object?>{'f': fragment},
  );
  return r.first[0]! as int;
});

Future<int?> _epoch(PostgresTestDatabase db) => _withConnection(db, (c) async {
  final r = await c.execute(
    "SELECT (value #>> '{}')::bigint FROM backend_state "
    "WHERE key = 'drain_epoch'",
  );
  return r.isEmpty ? null : r.first[0] as int?;
});

/// A TCP forwarder in front of the Postgres server, for the lock session:
/// it can stop relaying (freeze) without closing either socket, so the
/// client sees a black hole while the server session stays alive.
final class _Forwarder {
  _Forwarder(this._target);

  final Uri _target;
  late final ServerSocket _server;
  final List<_Pair> _pairs = <_Pair>[];

  /// New connections are accepted and never relayed.
  bool freezeNew = false;

  /// Connections accepted so far.
  int accepted = 0;

  int get port => _server.port;

  /// [url] with its host and port replaced by this forwarder's.
  String route(String url) => Uri.parse(
    url,
  ).replace(host: InternetAddress.loopbackIPv4.address, port: port).toString();

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((client) async {
      accepted += 1;
      final pair = _Pair(client);
      _pairs.add(pair);
      if (freezeNew) {
        pair.frozen = true;
        client.listen((_) {}, onError: (Object _) {});
        return;
      }
      final upstream = await Socket.connect(
        _target.host,
        _target.hasPort ? _target.port : 5432,
      );
      pair.upstream = upstream;
      client.listen(
        (data) {
          if (!pair.frozen) upstream.add(data);
        },
        onError: (Object _) {},
        onDone: () {
          if (!pair.frozen) upstream.destroy();
        },
      );
      upstream.listen(
        (data) {
          if (!pair.frozen) client.add(data);
        },
        onError: (Object _) {},
        onDone: () {
          if (!pair.frozen) client.destroy();
        },
      );
    });
  }

  /// Stops relaying on every connection accepted so far.
  void freeze() {
    for (final p in _pairs) {
      p.frozen = true;
    }
  }

  Future<void> close() async {
    await _server.close();
    for (final p in _pairs) {
      p.client.destroy();
      p.upstream?.destroy();
    }
  }
}

final class _Pair {
  _Pair(this.client);
  final Socket client;
  Socket? upstream;
  bool frozen = false;
}

/// One process in this isolate.
final class _Process {
  _Process(this.backend, this.store, this.registry);
  final PostgresBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }
  tearDownAll(db.drop);

  final backends = <PostgresBackend>[];
  final drainers = <SpawnedDrainer>[];
  final cycles = <SyncCycle>[];

  setUp(() async {
    await db.reset(provision: true);
  });

  tearDown(() async {
    for (final c in cycles) {
      await c.close(timeout: const Duration(seconds: 5));
    }
    cycles.clear();
    for (final d in drainers) {
      await d.close();
    }
    drainers.clear();
    for (final b in backends) {
      await b.close();
    }
    backends.clear();
  });

  /// Opens a backend in a zone with [hooks] installed (so timers it creates
  /// come from their factory), with a probe every [lockHeartbeat].
  Future<PostgresBackend> openBackend({
    DeliveryTestHooks? hooks,
    Duration lockHeartbeat = const Duration(hours: 1),
    Duration lockQueryTimeout = const Duration(seconds: 5),
    PostgresTestDatabase? at,
    String? lockUrl,
  }) async {
    final database = at ?? db;
    Future<PostgresBackend> open() => PostgresBackend.open(
      url: database.runtimeUrl,
      schema: database.schema,
      lockUrl: lockUrl,
      sslMode: SslMode.disable,
      lockHeartbeat: lockHeartbeat,
      lockQueryTimeout: lockQueryTimeout,
    );
    final backend = hooks == null
        ? await open()
        : await runWithDeliveryTestHooks(hooks, open);
    backends.add(backend);
    return backend;
  }

  Future<_Process> process({
    DeliveryTestHooks? hooks,
    Duration lockHeartbeat = const Duration(hours: 1),
    Duration lockQueryTimeout = const Duration(seconds: 5),
    List<Destination> destinations = const <Destination>[],
    PostgresTestDatabase? at,
    String? lockUrl,
  }) async {
    final backend = await openBackend(
      hooks: hooks,
      lockHeartbeat: lockHeartbeat,
      lockQueryTimeout: lockQueryTimeout,
      at: at,
      lockUrl: lockUrl,
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: harnessEntryTypes(),
      source: const Source(
        hopId: 'test',
        identifier: 'test-install',
        softwareVersion: 'test',
      ),
      securityContexts: PostgresSecurityContextStore(backend: backend),
    );
    final registry = DestinationRegistry(eventStore: store);
    for (final d in destinations) {
      await registry.addDestination(
        d,
        initiator: const AutomationInitiator(service: 'test'),
      );
      await registry.setStartDate(
        d.id,
        DateTime.utc(2000),
        initiator: const AutomationInitiator(service: 'test'),
      );
    }
    return _Process(backend, store, registry);
  }

  Future<SyncCycle> start(
    _Process p, {
    Duration cadence = const Duration(hours: 1),
    bool handDriven = true,
  }) async {
    final cycle = await startCycle(
      () => SyncCycle.start(registry: p.registry, cadence: cadence),
      handDriven: handDriven,
    );
    cycles.add(cycle);
    return cycle;
  }

  Future<String> note(EventStore store, String id) async => (await store.append(
    entryType: harnessNoteType,
    aggregateId: id,
    aggregateType: 'note',
    eventType: 'noted',
    data: const <String, Object?>{},
    initiator: const UserInitiator('u'),
  ))!.eventId;

  Future<SpawnedDrainer> spawn({
    Duration cadence = const Duration(milliseconds: 100),
    Set<String> hooks = const <String>{},
  }) async {
    final d = await SpawnedDrainer.spawn(
      db.runtimeUrl,
      schema: db.schema,
      cadence: cadence,
      hooks: hooks,
    );
    drainers.add(d);
    return d;
  }

  Future<int> drainKey(_Process p) async {
    final scope = await _withConnection(db, PostgresScope.read);
    return postgresDrainKey(scope, p.store.databaseId);
  }

  group('standby and takeover across processes', () {
    // Verifies: EVS-PRD-destinations/V
    // a drainer in another process stands by while this one drains, and
    //   takes over and delivers once this one closes.
    // Verifies: EVS-DEV-destination-drain-lock/A
    // the Postgres drain lock excludes drainers of one database across
    //   processes.
    test('a second process stands by and takes over', () async {
      final a = await process(
        destinations: <Destination>[
          Receiver(id: 'x', entryTypes: const <String>{harnessNoteType}),
        ],
      );
      final cycleA = await start(a);
      expect(cycleA.state, SyncCycleState.running);
      final b = await spawn();
      await b.reaches('standby');
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(b.state, 'standby');
      await cycleA.close();
      await b.reaches('running');
      await b.next('epoch');
      final id = await note(a.store, 'after-takeover');
      await until(() => b.sent.contains(id), reason: "B's delivery");
    });
  });

  group('fencing a replaced holder', () {
    // Verifies: EVS-DEV-destination-drain-lock/B
    // (a) a holder whose outcome transaction checks the lock after a new
    //   holder raised the epoch commits nothing: no attempt, no status, no
    //   event; the item stays pending and the new holder delivers it.
    // Verifies: EVS-PRD-destinations/J
    // the replaced holder's attempt is not recorded; the item is sent
    //   again.
    test('a check after the new holder raised the epoch', () async {
      final timers = ManualTimers();
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      )..outcome = (_) => const SendPermanent(error: 'no');
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: <Destination>[receiver],
      );
      final id = await note(a.store, 'r');
      final log = <LibraryLogRecord>[];
      late SpawnedDrainer b;
      String? headId;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          handDrivenCycle: true,
          timerFactory: timers.create,
          onLog: log.add,
          afterSendBeforeOutcome: (_) async {
            headId = (await a.backend.readFifoHead('x'))!.entryId;
            await _terminate(db, (await a.backend.lockSessionForTest()).pid);
            await b.next('epoch');
            expect(a.backend.generationStatus, GenerationStatus.registered);
          },
        ),
        () async {
          final cycleA = await start(a);
          b = await spawn();
          await b.reaches('standby');
          await cycleA();
        },
      );
      expect(
        log.any((r) => r.message.contains('stores drain epoch')),
        isTrue,
        reason: 'refused by the epoch, not a detected loss',
      );
      expect(
        await a.backend.findAllEvents(entryType: kDestinationWedgedEntryType),
        isEmpty,
      );
      await until(() => b.sent.contains(id), reason: "B's send");
      await until(
        () async =>
            (await a.backend.readFifoRow('x', headId!))?.finalStatus ==
            FinalStatus.sent,
        reason: "B's outcome",
      );
      final row = (await a.backend.readFifoRow('x', headId!))!;
      expect(row.eventIds, <String>[id]);
      expect(
        row.attempts.map((t) => t.outcome),
        <String>['ok'],
        reason: "only B's attempt; A's refusal was never recorded",
      );
      expect(
        await _holdersOf(db, await drainKey(a)),
        <int>{b.pid!},
        reason: "B's lock session is the one that took the lock",
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/B
    // (b) a holder whose outcome transaction checks the lock while the new
    //   holder's epoch raise is uncommitted waits for it, then fails and
    //   commits nothing.
    test('a check during an uncommitted epoch raise', () async {
      final timers = ManualTimers();
      final gate = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final receiver =
          Receiver(id: 'x', entryTypes: const <String>{harnessNoteType})
            ..gate = (() => gate.future)
            ..outcome = (_) => const SendPermanent(error: 'no');
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: <Destination>[receiver],
      );
      final id = await note(a.store, 'r');
      final log = <LibraryLogRecord>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          handDrivenCycle: true,
          timerFactory: timers.create,
          onLog: log.add,
        ),
        () async {
          final cycleA = await start(a);
          final b = await spawn(hooks: const <String>{'holdFirstBump'});
          await b.reaches('standby');
          final pass = cycleA();
          await until(() => receiver.started.isNotEmpty, reason: 'A sends');
          final headId = (await a.backend.readFifoHead('x'))!.entryId;
          await _terminate(db, (await a.backend.lockSessionForTest()).pid);
          await b.next('bumpPending');
          gate.complete();
          await until(
            () async => await _lockWaitersRunning(db, 'FOR SHARE') > 0,
            reason: "A's outcome check waits on the raise",
          );
          b.commitBump();
          await pass;
          expect(
            a.backend.generationStatus,
            GenerationStatus.registered,
            reason: 'A had not detected the loss of its session',
          );
          expect(
            log.any((r) => r.message.contains('stores drain epoch')),
            isTrue,
            reason: 'refused by the epoch, not a detected loss',
          );
          expect(
            log.any((r) => r.message.contains('lock session that held')),
            isFalse,
          );
          expect(
            await a.backend.findAllEvents(
              entryType: kDestinationWedgedEntryType,
            ),
            isEmpty,
          );
          await until(() => b.sent.contains(id), reason: "B's send");
          await until(
            () async =>
                (await a.backend.readFifoRow('x', headId))?.finalStatus ==
                FinalStatus.sent,
            reason: "B's outcome",
          );
          expect(
            (await a.backend.readFifoRow(
              'x',
              headId,
            ))!.attempts.map((t) => t.outcome),
            <String>['ok'],
            reason: "A's refusal was never recorded",
          );
          await b.next('epoch');
          expect(
            await _holdersOf(db, await drainKey(a)),
            <int>{b.pid!},
            reason: "B's lock session is the one that took the lock",
          );
        },
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/B
    // (c) a holder whose check took hold of the epoch before a new holder's
    //   raise commits its writes, and the raise orders after them: the new
    //   holder reads the wedged head and sends nothing for it.
    test('a check before the epoch raise', () async {
      final timers = ManualTimers();
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      )..outcome = (_) => const SendPermanent(error: 'no');
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: <Destination>[receiver],
      );
      final id = await note(a.store, 'r');
      late SpawnedDrainer b;
      var armed = false;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          handDrivenCycle: true,
          timerFactory: timers.create,
          afterSendBeforeOutcome: (_) async => armed = true,
          beforeQueueWrites: (_) async {
            if (!armed) return;
            armed = false;
            await _terminate(db, (await a.backend.lockSessionForTest()).pid);
            await until(
              () async =>
                  await _lockWaitersRunning(db, 'INSERT INTO backend_state') >
                  0,
              reason: "B's raise waits on A's check",
            );
            expect(a.backend.generationStatus, GenerationStatus.registered);
          },
        ),
        () async {
          final cycleA = await start(a);
          b = await spawn();
          await b.reaches('standby');
          await cycleA();
        },
      );
      final head = (await a.backend.readFifoHead('x'))!;
      expect(head.finalStatus, FinalStatus.wedged);
      expect(head.attempts.single.outcome, 'permanent');
      final wedge = (await a.backend.findAllEvents(
        entryType: kDestinationWedgedEntryType,
      )).single;
      await b.next('epoch');
      expect(b.epoch, greaterThan(wedge.data['drainer_epoch']! as int));
      await Future<void>.delayed(const Duration(milliseconds: 500));
      expect(b.sent, isNot(contains(id)), reason: 'B sends past no wedge');
      expect(
        await _holdersOf(db, await drainKey(a)),
        <int>{b.pid!},
        reason: "B's lock session is the one that took the lock",
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/E
    // each pass begins with a transaction that verifies the lock: a
    //   replaced holder that has not detected the loss writes no heartbeat
    //   and no declaration, and fills and sends nothing.
    test("a replaced holder's pass start writes nothing", () async {
      final timers = ManualTimers();
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: <Destination>[receiver],
      );
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(handDrivenCycle: true, timerFactory: timers.create),
        () async {
          final cycleA = await start(a);
          await cycleA();
          final epochA = (await _epoch(db))!;
          final b = await spawn(cadence: const Duration(hours: 1));
          await b.reaches('standby');
          await _terminate(db, (await a.backend.lockSessionForTest()).pid);
          // B's first attempt at its start found the key held; it acquires
          // at its next retry, which the test cannot fire in its isolate, so
          // it closes and a new drainer starts and takes the lock at once.
          await b.close();
          final c = await spawn(cadence: const Duration(hours: 1));
          await c.next('epoch');
          final heartbeatBefore = await a.backend.transaction(
            a.backend.readDrainHeartbeatTxn,
          );
          final declarationBefore = await a.backend.transaction(
            a.backend.readDrainerDeclarationTxn,
          );
          expect(heartbeatBefore?.epoch, epochA);
          final id = await note(a.store, 'late');
          await cycleA();
          expect(
            await a.backend.transaction(a.backend.readDrainHeartbeatTxn),
            heartbeatBefore,
            reason: 'no heartbeat from the replaced holder',
          );
          expect(
            await a.backend.transaction(a.backend.readDrainerDeclarationTxn),
            declarationBefore,
          );
          expect(receiver.sentIds, isNot(contains(id)));
          expect(await a.backend.listFifoEntries('x'), isEmpty);
        },
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/B
    // after a takeover the replaced holder's pass sends nothing on any of
    //   its destinations.
    // Verifies: EVS-PRD-destinations/J
    // its blocked sends' late outcomes write nothing.
    test('a replaced holder sends nothing more', () async {
      final timers = ManualTimers();
      final gate = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final receivers = <Receiver>[
        for (final id in <String>['w', 'x', 'y', 'z'])
          Receiver(id: id, entryTypes: const <String>{harnessNoteType})
            ..gate = (() => gate.future),
      ];
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: receivers,
      );
      // Two items per destination, one event each: the pass's fill queues
      // both, and every first send blocks, so each destination still has a
      // send pending in the pass in flight when the lock is taken over.
      await note(a.store, 'first');
      await note(a.store, 'first-b');
      final b = await spawn();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(handDrivenCycle: true, timerFactory: timers.create),
        () async {
          final cycleA = await start(a);
          await b.reaches('standby');
          final pass = cycleA();
          await until(
            () => receivers.every((r) => r.started.isNotEmpty),
            reason: "every destination's blocked first send",
          );
          for (final r in receivers) {
            expect(r.started, hasLength(1), reason: '${r.id} sends one item');
            expect(
              await a.backend.listFifoEntries(r.id),
              hasLength(2),
              reason: '${r.id} has a second item pending',
            );
          }
          await _terminate(db, (await a.backend.lockSessionForTest()).pid);
          await b.next('epoch');
          await note(a.store, 'second');
          gate.complete();
          await pass;
          expect(
            <int>[for (final r in receivers) r.started.length],
            <int>[1, 1, 1, 1],
            reason: 'the pass in flight sends nothing after the takeover',
          );
          await cycleA();
          expect(
            <int>[for (final r in receivers) r.started.length],
            <int>[1, 1, 1, 1],
            reason: 'the next pass sends nothing',
          );
        },
      );
      // B serves only x, so any attempt on w, y or z would be A's; on x at
      // most B's one attempt is recorded.
      for (final r in receivers) {
        final rows = await a.backend.listFifoEntries(r.id);
        expect(
          rows.first.attempts,
          r.id == 'x' ? hasLength(lessThanOrEqualTo(1)) : isEmpty,
          reason: "A's outcome on ${r.id} is lost",
        );
      }
    });
  });

  group('halt across a takeover', () {
    // Verifies: EVS-PRD-destinations/U
    // a halt requested while a replaced holder's send and the new holder's
    //   send are both on the wire: both complete at the receiver, the
    //   replaced holder records nothing, and the new holder honours the
    //   halt at its next head.
    test('the new holder honours a halt requested during both sends', () async {
      final timers = ManualTimers();
      final gate = Completer<void>();
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      )..gate = (() => gate.future);
      final a = await process(
        hooks: DeliveryTestHooks(timerFactory: timers.create),
        destinations: <Destination>[receiver],
      );
      final r = await note(a.store, 'r');
      final next = await note(a.store, 'next');
      final b = await spawn(hooks: const <String>{'gateSends'});
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(handDrivenCycle: true, timerFactory: timers.create),
        () async {
          final cycleA = await start(a);
          await b.reaches('standby');
          final pass = cycleA();
          await until(() => receiver.started.isNotEmpty, reason: 'A sends R');
          await _terminate(db, (await a.backend.lockSessionForTest()).pid);
          await until(() => b.sent.contains(r), reason: 'B sends R');
          final third = await process();
          await third.registry.requestHalt(
            'x',
            initiator: const UserInitiator('operator'),
            purpose: HaltPurpose.pause,
          );
          gate.complete();
          b.releaseSends();
          await pass;
        },
      );
      expect(receiver.received.single, <String>[r]);
      await until(() async {
        final rows = await a.backend.listFifoEntries('x');
        return rows.length == 2 && rows[1].finalStatus == FinalStatus.wedged;
      }, reason: 'the halt honoured at the next head');
      final rows = await a.backend.listFifoEntries('x');
      expect(rows[0].finalStatus, FinalStatus.sent);
      expect(rows[0].attempts, hasLength(1), reason: "only B's attempt");
      expect(rows[1].eventIds, <String>[next]);
      final wedge = (await a.backend.findAllEvents(
        entryType: kDestinationWedgedEntryType,
      )).single;
      expect(wedge.data['cause'], 'operator_halt');
      expect(b.sent.where((e) => e == next), isEmpty);
    });
  });

  group('loss and re-acquisition', () {
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a drainer whose lock session is ended detects the loss at its
    //   heartbeat, stands by until the backend registers again on a new
    //   session, and takes the lock again with a higher epoch, without a
    //   restart.
    test('a lost lock session: re-acquisition without a restart', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final a = await process(
        lockHeartbeat: const Duration(milliseconds: 200),
        destinations: <Destination>[receiver],
      );
      final cycle = await start(
        a,
        cadence: const Duration(milliseconds: 200),
        handDriven: false,
      );
      final first = (await _epoch(db))!;
      await _terminate(db, (await a.backend.lockSessionForTest()).pid);
      await until(
        () async =>
            cycle.state == SyncCycleState.running &&
            ((await _epoch(db)) ?? 0) > first,
        bound: const Duration(seconds: 20),
        reason: 're-acquisition',
      );
      final id = await note(a.store, 'after');
      await until(() => receiver.sentIds.contains(id), reason: 'delivery');
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a probe that outlasts the query timeout on a live session declares
    //   the loss; the replacement ends the old server session, which frees
    //   the drain key, and the drainer takes the lock again and delivers.
    // Verifies: EVS-DEV-postgres-backend/J
    // the drain key is not left held by the old server session.
    test('a stalled probe on a live session', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      var stall = false;
      final hooks = DeliveryTestHooks(
        handDrivenCycle: true,
        stallLockHeartbeatPastQueryTimeout: () {
          if (!stall) return false;
          stall = false;
          return true;
        },
      );
      final a = await process(
        hooks: hooks,
        lockHeartbeat: const Duration(milliseconds: 200),
        lockQueryTimeout: const Duration(seconds: 1),
        destinations: <Destination>[receiver],
      );
      final cycle = await runWithDeliveryTestHooks(
        hooks,
        () => start(a, cadence: const Duration(milliseconds: 200)),
      );
      final key = await drainKey(a);
      final oldPid = (await a.backend.lockSessionForTest()).pid;
      final first = (await _epoch(db))!;
      stall = true;
      await until(
        () async =>
            cycle.state == SyncCycleState.running &&
            ((await _epoch(db)) ?? 0) > first,
        bound: const Duration(seconds: 20),
        reason: 're-acquisition',
      );
      expect(await _holdersOf(db, key), isNot(contains(oldPid)));
      final id = await note(a.store, 'after');
      await runWithDeliveryTestHooks(hooks, cycle.call);
      expect(receiver.sentIds, contains(id));
      final watch = Stopwatch()..start();
      await openBackend();
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // the lock session runs one operation at a time: while an epoch raise
    //   is held, no probe is sent and the session is not declared lost.
    test('no probe while an epoch raise runs', () async {
      final timers = ManualTimers();
      var probes = 0;
      final raiseHeld = Completer<void>();
      final release = Completer<void>();
      addTearDown(() {
        if (!release.isCompleted) release.complete();
      });
      final hooks = DeliveryTestHooks(
        handDrivenCycle: true,
        timerFactory: timers.create,
        failNextLockHeartbeat: () {
          probes += 1;
          return false;
        },
        insideEpochBumpBeforeCommit: () async {
          if (!raiseHeld.isCompleted) raiseHeld.complete();
          await release.future;
        },
      );
      final a = await process(hooks: hooks);
      final starting = runWithDeliveryTestHooks(hooks, () => start(a));
      await raiseHeld.future;
      await timers.fire();
      await timers.fire();
      expect(probes, 0, reason: 'the session was busy');
      expect(a.backend.generationStatus, GenerationStatus.registered);
      release.complete();
      final cycle = await starting;
      expect(cycle.state, SyncCycleState.running);
      expect(await _epoch(db), isNotNull);
    });
  });

  group('a lock session behind a black hole', () {
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a lock session that stops answering is declared lost within the
    //   query timeout plus a second; the drainer stands by, and once new
    //   connections reach the server again the replacement ends the old
    //   server session, which still holds the drain key, and the drainer
    //   takes the lock again with a higher epoch and delivers.
    test('a frozen lock connection: loss, then re-acquisition', () async {
      final forwarder = _Forwarder(Uri.parse(db.adminUrl));
      await forwarder.start();
      addTearDown(forwarder.close);
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final a = await process(
        lockUrl: forwarder.route(db.runtimeUrl),
        lockHeartbeat: const Duration(milliseconds: 200),
        lockQueryTimeout: const Duration(seconds: 1),
        destinations: <Destination>[receiver],
      );
      final cycle = await start(
        a,
        cadence: const Duration(milliseconds: 200),
        handDriven: false,
      );
      final first = (await _epoch(db))!;
      final key = await drainKey(a);
      final oldHolders = await _holdersOf(db, key);
      expect(oldHolders, hasLength(1));
      forwarder
        ..freeze()
        ..freezeNew = true;
      final watch = Stopwatch()..start();
      await until(
        () => cycle.state == SyncCycleState.standby,
        bound: const Duration(seconds: 3),
        reason: 'the loss',
      );
      expect(
        watch.elapsed,
        lessThan(const Duration(seconds: 2, milliseconds: 500)),
      );
      // The old server session is alive behind the frozen connection and
      // still holds the key.
      expect(await _holdersOf(db, key), oldHolders);
      forwarder.freezeNew = false;
      await until(
        () async =>
            cycle.state == SyncCycleState.running &&
            ((await _epoch(db)) ?? 0) > first,
        bound: const Duration(seconds: 15),
        reason: 're-acquisition',
      );
      expect(await _holdersOf(db, key), isNot(oldHolders));
      final id = await note(a.store, 'after');
      await until(
        () => receiver.sentIds.contains(id),
        bound: const Duration(seconds: 10),
        reason: 'delivery',
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // closing a drainer while its lock session does not answer returns
    //   within the query timeout plus a second, and no epoch is raised
    //   afterwards.
    test('close while the lock connection is frozen', () async {
      final forwarder = _Forwarder(Uri.parse(db.adminUrl));
      await forwarder.start();
      addTearDown(forwarder.close);
      final a = await process(
        lockUrl: forwarder.route(db.runtimeUrl),
        lockHeartbeat: const Duration(milliseconds: 200),
        lockQueryTimeout: const Duration(seconds: 1),
      );
      final cycle = await start(
        a,
        cadence: const Duration(milliseconds: 200),
        handDriven: false,
      );
      forwarder
        ..freeze()
        ..freezeNew = true;
      await Future<void>.delayed(const Duration(milliseconds: 300));
      final watch = Stopwatch()..start();
      await cycle.close();
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
      final epoch = await _epoch(db);
      await Future<void>.delayed(const Duration(milliseconds: 600));
      expect(await _epoch(db), epoch);
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a standing-by drainer whose backend re-opens its lock session through
    //   a connection frozen before the handshake closes within the query
    //   timeout plus a second, and makes no connection attempt after its
    //   backend closed.
    test('close while the lock session reconnects into a black hole', () async {
      final forwarder = _Forwarder(Uri.parse(db.adminUrl));
      await forwarder.start();
      addTearDown(forwarder.close);
      final a = await process(
        lockUrl: forwarder.route(db.runtimeUrl),
        lockHeartbeat: const Duration(milliseconds: 200),
        lockQueryTimeout: const Duration(seconds: 1),
      );
      final cycle = await start(
        a,
        cadence: const Duration(milliseconds: 200),
        handDriven: false,
      );
      forwarder.freezeNew = true;
      await _terminate(db, (await a.backend.lockSessionForTest()).pid);
      await until(
        () => cycle.state == SyncCycleState.standby,
        reason: 'the loss',
      );
      final watch = Stopwatch()..start();
      await cycle.close();
      expect(watch.elapsed, lessThan(const Duration(seconds: 2)));
      await a.backend.close();
      backends.remove(a.backend);
      final attempts = forwarder.accepted;
      await Future<void>.delayed(const Duration(milliseconds: 800));
      expect(forwarder.accepted, attempts, reason: 'no connection attempt');
    });
  });

  group('acquisition on the lock session', () {
    // Verifies: EVS-DEV-destination-drain-lock/A
    // the acquisition verifies the database identity on the lock session:
    //   another identity is refused as a configuration error, the key is
    //   not held afterwards, and no epoch is raised.
    test('another database identity is refused', () async {
      final a = await process();
      final key = await drainKey(a);
      await expectLater(
        a.backend.tryAcquireDrainLock(databaseId: 'another-database'),
        throwsA(isA<DrainLockConfigurationException>()),
      );
      expect(
        await _holdersOf(
          db,
          postgresDrainKey(
            await _withConnection(db, PostgresScope.read),
            'another-database',
          ),
        ),
        isEmpty,
      );
      expect(await _holdersOf(db, key), isEmpty);
      expect(await _epoch(db), isNull);
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a drain key the lock session holds through something other than the
    //   library is refused as a configuration error, raising no epoch; a
    //   second try while the backend's own drain lock is live is refused as
    //   held.
    test('a key held outside the library, and a live lock', () async {
      final a = await process();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(holdDrainKeyOutsideLibrary: () => true),
        () => expectLater(
          a.backend.tryAcquireDrainLock(databaseId: a.store.databaseId),
          throwsA(isA<DrainLockConfigurationException>()),
        ),
      );
      expect(await _epoch(db), isNull);
      // Closing the backend ends its lock session and frees the key.
      await a.backend.close();
      final b = await process();
      final lock = await b.backend.tryAcquireDrainLock(
        databaseId: b.store.databaseId,
      );
      await expectLater(
        b.backend.tryAcquireDrainLock(databaseId: b.store.databaseId),
        throwsA(isA<DrainLockUnavailableException>()),
      );
      await lock.release();
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a start whose acquisition cannot verify the lock through the pool
    //   throws the configuration error and leaves nothing started: the
    //   trigger slot is empty, an append raises nothing and runs no pass,
    //   the key is free, and a later start succeeds.
    test('a verification failure at start throws and leaves nothing', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final a = await process(destinations: <Destination>[receiver]);
      final key = await drainKey(a);
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(failDrainLockVerification: () => true),
        () => expectLater(
          SyncCycle.start(registry: a.registry),
          throwsA(isA<DrainLockConfigurationException>()),
        ),
      );
      final errors = <Object>[];
      final wakes = <bool>[];
      await runZonedGuarded(
        () => runWithDeliveryTestHooks(
          DeliveryTestHooks(onDeliveryWake: wakes.add),
          () async {
            await note(a.store, 'n');
            await Future<void>.delayed(const Duration(milliseconds: 50));
          },
        ),
        (e, _) => errors.add(e),
      );
      expect(wakes, <bool>[false], reason: 'the trigger slot is empty');
      expect(errors, isEmpty);
      expect(receiver.started, isEmpty);
      expect(await _holdersOf(db, key), isEmpty);
      final cycle = await start(a);
      expect(cycle.state, SyncCycleState.running);
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // an epoch raise that outlasts the query timeout gives the key up (the
    //   stalled session is ended from its replacement): the cycle stands
    //   by, no epoch is raised, the key is free afterwards, and a retry
    //   acquires and delivers.
    test('an epoch raise past the query timeout gives the key up', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      var stall = true;
      final hooks = DeliveryTestHooks(
        stallEpochBumpPastQueryTimeout: () => stall,
      );
      final a = await process(
        hooks: hooks,
        lockHeartbeat: const Duration(milliseconds: 200),
        lockQueryTimeout: const Duration(seconds: 1),
        destinations: <Destination>[receiver],
      );
      final key = await drainKey(a);
      final cycle = await runWithDeliveryTestHooks(
        hooks,
        () => start(a, cadence: const Duration(seconds: 1), handDriven: false),
      );
      expect(cycle.state, SyncCycleState.standby);
      expect(await _epoch(db), isNull);
      await until(
        () async => (await _holdersOf(db, key)).isEmpty,
        bound: const Duration(seconds: 15),
        reason: 'the key is free',
      );
      stall = false;
      await until(
        () => cycle.state == SyncCycleState.running,
        bound: const Duration(seconds: 20),
        reason: 'the retry',
      );
      final id = await note(a.store, 'n');
      await until(() => receiver.sentIds.contains(id), reason: 'delivery');
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a lock session ended between taking the key and raising the epoch
    //   records no epoch; the acquisition is retried after the backend
    //   registers again.
    // Verifies: EVS-DEV-destination-drain-lock/B
    // a transaction body that throws DrainLockLostException is not re-run.
    test('a session ended before the epoch raise', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      late _Process a;
      var armed = true;
      final hooks = DeliveryTestHooks(
        handDrivenCycle: true,
        afterLockAcquireBeforeEpochBump: () async {
          if (!armed) return;
          armed = false;
          await _terminate(db, (await a.backend.lockSessionForTest()).pid);
        },
      );
      a = await process(
        hooks: hooks,
        lockHeartbeat: const Duration(milliseconds: 200),
        destinations: <Destination>[receiver],
      );
      final cycle = await runWithDeliveryTestHooks(
        hooks,
        () => start(a, cadence: const Duration(milliseconds: 200)),
      );
      expect(cycle.state, SyncCycleState.standby);
      expect(await _epoch(db), isNull, reason: 'no epoch recorded');
      await until(
        () => cycle.state == SyncCycleState.running,
        bound: const Duration(seconds: 20),
        reason: 'the retry after re-registration',
      );
      expect(await _epoch(db), 1);
      var runs = 0;
      await expectLater(
        a.backend.transaction<void>((txn) async {
          runs += 1;
          throw const DrainLockLostException(
            DrainLockLossReason.epochChanged,
            'test',
          );
        }),
        throwsA(isA<DrainLockLostException>()),
      );
      expect(runs, 1);
    });
  });

  group('scope', () {
    // Verifies: EVS-DEV-destination-drain-lock/A
    // two library databases in two schemas of one Postgres database each
    //   run a drainer, even when one schema's records, database identity
    //   included, are a copy of the other's: the key includes the scope.
    test('two schemas, one copied identity, two drainers', () async {
      final schemas = <PostgresTestDatabase>[
        for (final n in ['1', '2'])
          PostgresTestDatabase(db.adminUrl, tag: 'dl$n'),
      ];
      for (final schema in schemas) {
        addTearDown(schema.drop);
        await schema.reset(provision: true);
      }
      final r1 = Receiver(id: 'x', entryTypes: const <String>{harnessNoteType});
      final p1 = await process(at: schemas[0], destinations: <Destination>[r1]);
      // Schema s2 starts as a copy of s1's records, the identity included.
      await db.asAdmin(
        (admin) => admin.execute(
          'INSERT INTO ${quoteIdent(schemas[1].schema)}.backend_state '
          'SELECT * FROM ${quoteIdent(schemas[0].schema)}.backend_state '
          'ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value',
        ),
      );
      final r2 = Receiver(id: 'x', entryTypes: const <String>{harnessNoteType});
      final p2 = await process(at: schemas[1], destinations: <Destination>[r2]);
      expect(p2.store.databaseId, p1.store.databaseId);
      final c1 = await start(p1);
      final c2 = await start(p2);
      expect(c1.state, SyncCycleState.running);
      expect(c2.state, SyncCycleState.running);
      final id1 = await note(p1.store, 'one');
      final id2 = await note(p2.store, 'two');
      await c1();
      await c2();
      expect(r1.sentIds, contains(id1));
      expect(r2.sentIds, contains(id2));
    });
  });

  group('wakeups from another process', () {
    // Verifies: EVS-DEV-destination-drain-lock/E
    // a halt request and an append committed through another process's
    //   registry and store reach the drainer at its next pass: it honours
    //   the halt and delivers within about one cadence.
    test('a halt and an event from another backend', () async {
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final a = await process(destinations: <Destination>[receiver]);
      await start(a, cadence: const Duration(milliseconds: 200));
      final other = await process();
      final id = await note(other.store, 'from-other');
      await until(() => receiver.sentIds.contains(id), reason: 'delivery');
      await other.registry.requestHalt(
        'x',
        initiator: const UserInitiator('operator'),
        purpose: HaltPurpose.pause,
      );
      await note(other.store, 'halted');
      await until(
        () async =>
            (await a.backend.readFifoHead('x'))?.finalStatus ==
            FinalStatus.wedged,
        reason: 'the halt honoured',
      );
    });
  });

  group('a fenced backend', () {
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a drainer whose backend is fenced (a conflicting build registered
    //   while its lock session was lost) stops for good, with one error
    //   line, and sends nothing further.
    test('a fenced backend stops its drainer', () async {
      final timers = ManualTimers();
      final receiver = Receiver(
        id: 'x',
        entryTypes: const <String>{harnessNoteType},
      );
      final hooks = DeliveryTestHooks(timerFactory: timers.create);
      final a = await process(
        hooks: hooks,
        destinations: <Destination>[receiver],
      );
      final log = <LibraryLogRecord>[];
      final cycle = await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          handDrivenCycle: true,
          timerFactory: timers.create,
          onLog: log.add,
        ),
        () => start(a),
      );
      // An event the drainer would fill and send at its next pass.
      await note(a.store, 'pending');
      await _terminate(db, (await a.backend.lockSessionForTest()).pid);
      // A conflicting build (the note type at major 2) opens and closes.
      final conflicting = await openBackend();
      final registry = EntryTypeRegistry();
      for (final d in harnessEntryTypes().all()) {
        registry.register(
          d.id == harnessNoteType
              ? const EntryTypeDefinition(
                  id: harnessNoteType,
                  registeredVersion: EntryTypeVersion(2, 0),
                  name: harnessNoteType,
                )
              : d,
        );
      }
      final store = await EventStore.openForTest(
        storage: conflicting,
        entryTypes: registry,
        source: const Source(
          hopId: 'c',
          identifier: 'conflicting',
          softwareVersion: 'c',
        ),
        securityContexts: PostgresSecurityContextStore(backend: conflicting),
      );
      await store.close();
      backends.remove(conflicting);
      await until(() async {
        await timers.fire();
        return a.backend.generationStatus == GenerationStatus.fenced;
      }, reason: 'the fence');
      await until(() async {
        await timers.fire();
        return cycle.state == SyncCycleState.stopped;
      }, reason: 'the cycle stops');
      expect(
        log.where(
          (r) =>
              r.level == LibraryLogLevel.severe &&
              r.message.startsWith('the delivery cycle stops'),
        ),
        hasLength(1),
      );
      await cycle();
      await timers.fire();
      expect(
        receiver.started,
        isEmpty,
        reason: 'no send from a fenced backend',
      );
    });

    /// Opens and closes a build whose note type is at major 2, which fences
    /// a backend whose lock session is lost.
    Future<void> openConflictingBuild() async {
      final conflicting = await openBackend();
      final registry = EntryTypeRegistry();
      for (final d in harnessEntryTypes().all()) {
        registry.register(
          d.id == harnessNoteType
              ? const EntryTypeDefinition(
                  id: harnessNoteType,
                  registeredVersion: EntryTypeVersion(2, 0),
                  name: harnessNoteType,
                )
              : d,
        );
      }
      final store = await EventStore.openForTest(
        storage: conflicting,
        entryTypes: registry,
        source: const Source(
          hopId: 'c',
          identifier: 'conflicting',
          softwareVersion: 'c',
        ),
        securityContexts: PostgresSecurityContextStore(backend: conflicting),
      );
      await store.close();
      backends.remove(conflicting);
      await conflicting.close();
    }

    // Verifies: EVS-DEV-destination-drain-lock/B
    // a send in flight when the drainer's backend is fenced commits
    //   nothing afterwards (no attempt, no status, no event), and the
    //   drainer starts no further send: the queued row behind it is never
    //   sent.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // the fenced drainer stops for good and gives up its in-isolate
    //   registration: a later start over the same database is not refused
    //   as a second cycle, and stops too.
    test(
      'a send in flight when the backend is fenced records nothing',
      () async {
        final timers = ManualTimers();
        final receiver =
            Receiver(id: 'x', entryTypes: const <String>{harnessNoteType})
              ..outcome = (n) =>
                  n == 0 ? const SendTransient(error: 'busy') : const SendOk();
        final a = await process(
          hooks: DeliveryTestHooks(timerFactory: timers.create),
          destinations: <Destination>[receiver],
        );
        final cycle = await runWithDeliveryTestHooks(
          DeliveryTestHooks(handDrivenCycle: true, timerFactory: timers.create),
          () async {
            final c = await SyncCycle.start(
              registry: a.registry,
              cadence: const Duration(hours: 1),
              policy: const SyncPolicy(
                initialBackoff: Duration.zero,
                backoffMultiplier: 1.0,
                maxBackoff: Duration.zero,
                jitterFraction: 0.0,
                maxAttempts: 5,
              ),
            );
            cycles.add(c);
            return c;
          },
        );
        await note(a.store, 'r1');
        await note(a.store, 'r2');
        // Pass 1 queues r1 and records a transient attempt; pass 2 queues r2
        // and sends r1 again, held at the gate.
        await cycle();
        final gate = Completer<void>();
        addTearDown(() {
          if (!gate.isCompleted) gate.complete();
        });
        receiver.gate = () => gate.future;
        final pass = cycle();
        await until(() => receiver.started.length == 2, reason: 'the resend');
        final rows = await a.backend.listFifoEntries('x');
        expect(rows, hasLength(2));
        final destinationEventsBefore = (await a.backend.findAllEvents())
            .where((e) => e.aggregateType == 'system_destination')
            .length;

        await _terminate(db, (await a.backend.lockSessionForTest()).pid);
        await openConflictingBuild();
        await until(() async {
          await timers.fire();
          return a.backend.generationStatus == GenerationStatus.fenced;
        }, reason: 'the fence');
        receiver.gate = null;
        gate.complete();
        await pass;

        final after = await _withConnection(db, (c) async {
          final r = await c.execute(
            Sql.named(
              'SELECT entry_id, final_status, jsonb_array_length(attempts) '
              'FROM fifo_entries WHERE destination_id = @d '
              'ORDER BY sequence_in_queue',
            ),
            parameters: <String, Object?>{'d': 'x'},
          );
          return <List<Object?>>[for (final row in r) row.toList()];
        });
        expect(after, <List<Object?>>[
          <Object?>[rows[0].entryId, null, 1],
          <Object?>[rows[1].entryId, null, 0],
        ], reason: 'the outcome of the send in flight committed nothing');
        final destinationEvents = await _withConnection(db, (c) async {
          final r = await c.execute(
            'SELECT count(*) FROM events WHERE aggregate_type = '
            "'system_destination'",
          );
          return r.first[0]! as int;
        });
        expect(
          destinationEvents,
          destinationEventsBefore,
          reason: 'no wedge or other destination event',
        );
        await until(() async {
          await timers.fire();
          return cycle.state == SyncCycleState.stopped;
        }, reason: 'the cycle stops');
        await cycle();
        await timers.fire();
        expect(receiver.started, hasLength(2), reason: 'r2 is never sent');

        final again = await runWithDeliveryTestHooks(
          DeliveryTestHooks(timerFactory: timers.create),
          () => SyncCycle.start(
            registry: a.registry,
            cadence: const Duration(hours: 1),
          ),
        );
        cycles.add(again);
        await until(() async {
          await timers.fire();
          return again.state == SyncCycleState.stopped;
        }, reason: 'the second cycle stops on the fenced backend');
        expect(receiver.started, hasLength(2));
      },
    );
  });
}
