// Backend-agnostic scenarios of the delivery cycle and its drain lock: one
// drainer per database, standby and takeover, start failures, giving up a
// partial acquisition, close, outcomes that do not commit, the trigger
// that never raises, the cadence, the heartbeat and wakeups. Each test
// carries its own citations. Concrete backends run them from
// `test/sync/delivery_cycle_test.dart` (Sembast) and
// `test/storage/postgres/postgres_delivery_cycle_test.dart` (Postgres).
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kEntryTypeRegistryInitializedEventType,
        kRegistryAuditAggregateType,
        kSystemEntryTypes;
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'manual_timers.dart';
import 'queue_registry_conformance.dart' show QueueTestDatabase;
import 'queue_test_support.dart' show fillForTest, wedgeHeadForTest;

const Initiator _init = AutomationInitiator(service: 'cycle-scenarios');
const Initiator _operator = UserInitiator('operator-1');
const String _noteType = 'cycle_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'cycle-install',
  softwareVersion: 'test@1.0.0',
);

DateTime _fillNow() => DateTime.utc(2027, 1, 1);

/// No backoff, a budget of five attempts.
const SyncPolicy _policy = SyncPolicy(
  initialBackoff: Duration.zero,
  backoffMultiplier: 1.0,
  maxBackoff: Duration.zero,
  jitterFraction: 0.0,
  maxAttempts: 5,
);

/// A destination that records the event ids of every send it receives,
/// optionally waits on [gate] before answering, and answers with
/// [outcome] (SendOk by default).
class Receiver extends Destination {
  Receiver({
    required this.id,
    Set<String>? entryTypes,
    this.allowHardDelete = true,
    this.maxAccumulateTime = Duration.zero,
    this.tag = 'r',
  }) : filter = SubscriptionFilter(
         entryTypes: entryTypes ?? const <String>{_noteType},
       );

  @override
  final String id;

  @override
  final SubscriptionFilter filter;

  @override
  final bool allowHardDelete;

  @override
  final Duration maxAccumulateTime;

  /// Tagged into every payload, so a test tells configurations apart.
  final String tag;

  @override
  String get wireFormat => 'receiver-v1';

  /// The event ids of every send that started, in order.
  final List<List<String>> started = <List<String>>[];

  /// The event ids of every send that returned, in order.
  final List<List<String>> received = <List<String>>[];

  /// Awaited by every send after it is recorded as started.
  Future<void> Function()? gate;

  /// The outcome of the n-th send (from 0).
  SendResult Function(int n) outcome = (_) => const SendOk();

  /// Every event id sent, in order.
  List<String> get sentIds => <String>[for (final b in received) ...b];

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async => WirePayload(
    bytes: Uint8List.fromList(
      utf8.encode(
        jsonEncode(<String, Object?>{
          'tag': tag,
          'event_ids': <String>[for (final e in batch) e.eventId],
        }),
      ),
    ),
    contentType: 'application/json',
    transformVersion: 'receiver-v1',
  );

  @override
  Future<SendResult> send(WirePayload payload) async {
    final body = jsonDecode(utf8.decode(payload.bytes)) as Map<String, Object?>;
    final ids = (body['event_ids']! as List<Object?>).cast<String>();
    final n = started.length;
    started.add(ids);
    final g = gate;
    if (g != null) await g();
    received.add(ids);
    return outcome(n);
  }
}

/// One process: a backend, an event store over it and a registry.
class CycleProcess {
  CycleProcess(this.backend, this.store, this.registry);
  final StorageBackend backend;
  final EventStore store;
  final DestinationRegistry registry;
}

/// Polls [condition] every 5 ms until it holds, failing after [bound].
Future<void> until(
  FutureOr<bool> Function() condition, {
  Duration bound = const Duration(seconds: 10),
  String reason = 'the condition',
}) async {
  final deadline = DateTime.now().add(bound);
  while (!await condition()) {
    if (DateTime.now().isAfter(deadline)) {
      fail('$reason did not hold within $bound');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

/// The scenario world: a database, its first process [a], and the cycles a
/// test started (closed when the test ends).
class CycleWorld {
  CycleWorld(this.db);
  final QueueTestDatabase db;
  late CycleProcess a;
  final List<SyncCycle> cycles = <SyncCycle>[];

  StorageBackend get backend => a.backend;
  EventStore get store => a.store;
  DestinationRegistry get registry => a.registry;

  Future<CycleProcess> openProcess() async {
    final backend = await db.openBackend();
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes
      ..register(
        const EntryTypeDefinition(
          id: _noteType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: _noteType,
        ),
      )
      ..register(
        const EntryTypeDefinition(
          id: 'cycle_other',
          registeredVersion: EntryTypeVersion(1, 0),
          name: 'cycle_other',
        ),
      );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: db.securityFor(backend),
      clock: () => DateTime.utc(2026, 3, 1),
    );
    return CycleProcess(backend, store, DestinationRegistry(eventStore: store));
  }

  /// Registers [d] through [on] (default [registry]) and activates it.
  Future<void> activate(Destination d, {DestinationRegistry? on}) async {
    final r = on ?? registry;
    await r.addDestination(d, initiator: _init);
    await r.setStartDate(d.id, DateTime.utc(2026, 1, 1), initiator: _init);
  }

  /// Appends a note through [on] (default [store]); returns its event id.
  Future<String> note(String id, {EventStore? on}) async {
    final event = await (on ?? store).append(
      entryType: _noteType,
      aggregateId: id,
      aggregateType: 'note',
      eventType: 'noted',
      data: <String, Object?>{'id': id},
      initiator: const UserInitiator('u'),
    );
    return event!.eventId;
  }

  /// Starts a delivery cycle over [on] (default [registry]).
  Future<SyncCycle> start({
    DestinationRegistry? on,
    Duration cadence = const Duration(hours: 1),
    SyncPolicy? policy = _policy,
    SyncPolicy? Function()? policyResolver,
    String? configurationVersion,
  }) async {
    final cycle = await SyncCycle.start(
      registry: on ?? registry,
      clock: _fillNow,
      cadence: cadence,
      policy: policyResolver == null ? policy : null,
      policyResolver: policyResolver,
      configurationVersion: configurationVersion,
    );
    cycles.add(cycle);
    return cycle;
  }

  Future<int?> epoch() => backend.transaction(backend.readDrainEpochTxn);

  Future<FifoEntry?> row(String destId, String entryId) =>
      backend.readFifoRow(destId, entryId);

  Future<void> closeAll() async {
    for (final cycle in cycles) {
      await cycle.close(timeout: const Duration(seconds: 5));
    }
    cycles.clear();
    await db.close();
  }
}

/// Runs the delivery-cycle scenarios against the database [databaseFactory]
/// builds fresh for each test (a null database skips the test).
///
/// [giveUpInjection] makes an acquisition fail after the backend obtained
/// its exclusion primitive (Sembast: after the isolate registry entry is
/// set; Postgres: a serialization failure inside the epoch raise).
void runDeliveryCycleScenarios(
  Future<QueueTestDatabase?> Function() databaseFactory, {
  required String label,
  required GiveUpInjection Function(bool Function() fail) giveUpInjection,
}) {
  group('delivery cycle scenarios ($label)', () {
    late CycleWorld w;
    var available = false;

    setUp(() async {
      final db = await databaseFactory();
      if (db == null) {
        available = false;
        markTestSkipped('no database for $label');
        return;
      }
      available = true;
      w = CycleWorld(db);
      w.a = await w.openProcess();
    });

    tearDown(() async {
      if (!available) return;
      await w.closeAll();
    });

    group('one drainer', () {
      // Verifies: EVS-PRD-destinations/V
      // a second delivery cycle for one database in one isolate is refused,
      //   whether over the same registry, another registry over the same
      //   store, or another backend over the same database; the first
      //   cycle is unaffected and still woken by an append.
      // Verifies: EVS-DEV-destination-drain-lock/D
      // the trigger slot stays with the first cycle.
      test(
        'a second cycle over the database in one isolate is refused',
        () async {
          if (!available) return;
          final d = Receiver(id: 'x');
          await w.activate(d);
          final first = await w.start();
          final other = DestinationRegistry(eventStore: w.store);
          final b = await w.openProcess();
          for (final r in <DestinationRegistry>[
            w.registry,
            other,
            b.registry,
          ]) {
            await expectLater(
              SyncCycle.start(registry: r, cadence: const Duration(hours: 1)),
              throwsA(
                isA<StateError>().having(
                  (e) => e.message,
                  'message',
                  contains('EVS-PRD-destinations/V'),
                ),
              ),
            );
          }
          expect(first.state, SyncCycleState.running);
          final id = await w.note('n1');
          await until(() => d.sentIds.contains(id), reason: 'the wake');
        },
      );

      // Verifies: EVS-PRD-destinations/V
      // of two starts that race on one database, exactly one completes and
      //   the other is refused.
      test('two racing starts: exactly one completes', () async {
        if (!available) return;
        final outcomes = await Future.wait(<Future<Object>>[
          for (var i = 0; i < 2; i++)
            SyncCycle.start(
              registry: w.registry,
              cadence: const Duration(hours: 1),
            ).then<Object>((c) {
              w.cycles.add(c);
              return c;
            }, onError: (Object e) => e),
        ]);
        expect(outcomes.whereType<SyncCycle>(), hasLength(1));
        expect(outcomes.whereType<StateError>(), hasLength(1));
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // after close, and after close with a timeout, a new cycle over the
      //   database starts and takes the lock.
      test('a closed cycle gives way to a new one', () async {
        if (!available) return;
        final first = await w.start();
        await first.close();
        expect(first.state, SyncCycleState.stopped);
        await first.stopped;
        final second = await w.start();
        expect(second.state, SyncCycleState.running);
        await second.close(timeout: const Duration(milliseconds: 100));
        final third = await w.start();
        expect(third.state, SyncCycleState.running);
      });

      // Verifies: EVS-PRD-destinations/V
      // a cycle started while another holds the drain lock stands by, and
      //   takes over and delivers once it is released, without a restart.
      // Verifies: EVS-DEV-destination-drain-lock/C
      // standby, then running on the grant.
      // Verifies: EVS-DEV-destination-drain-lock/D
      // in standby the cycle holds the trigger slot and a trigger does no
      //   work.
      test('standby, then takeover when the holder releases', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final b = await w.openProcess();
        final holder = await b.backend.tryAcquireDrainLock(
          databaseId: b.store.databaseId,
        );
        final cycle = await w.start(cadence: const Duration(milliseconds: 100));
        expect(cycle.state, SyncCycleState.standby);
        expect(w.store.deliveryTrigger, isNotNull);
        final id = await w.note('n1');
        await cycle();
        await Future<void>.delayed(const Duration(milliseconds: 300));
        expect(d.started, isEmpty, reason: 'no work while standing by');
        await holder.release();
        await until(
          () => cycle.state == SyncCycleState.running,
          reason: 'the takeover',
        );
        await until(() => d.sentIds.contains(id), reason: 'delivery');
        expect(await w.epoch(), greaterThan(holder.epoch));
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // closing a cycle that stands by completes, makes no acquisition, and
      //   leaves the lock free.
      test('close during standby acquires nothing', () async {
        if (!available) return;
        final b = await w.openProcess();
        final holder = await b.backend.tryAcquireDrainLock(
          databaseId: b.store.databaseId,
        );
        final cycle = await w.start(cadence: const Duration(milliseconds: 50));
        expect(cycle.state, SyncCycleState.standby);
        await cycle.close();
        await holder.release();
        await Future<void>.delayed(const Duration(milliseconds: 300));
        expect(await w.epoch(), holder.epoch, reason: 'no acquisition');
        final later = await b.backend.tryAcquireDrainLock(
          databaseId: b.store.databaseId,
        );
        await later.release();
      });

      // Verifies: EVS-DEV-destination-drain/T
      // a process that does not hold the drain lock, and does not register
      //   a destination, does not honour its halt: its cycle stands by.
      test('a cycle that does not hold the lock honours no halt', () async {
        if (!available) return;
        final remote = Receiver(id: 'remote');
        final b = await w.openProcess();
        await w.activate(remote, on: b.registry);
        await w.note('r1');
        final holder = await b.backend.tryAcquireDrainLock(
          databaseId: b.store.databaseId,
        );
        // Seed a pending head as the registering process's fill would.
        final bLock = holder;
        await fillBatch(
          remote,
          backend: b.backend,
          source: _source,
          lock: bLock,
          clock: _fillNow,
        );
        expect(await b.backend.readFifoHead('remote'), isNotNull);
        final cycle = await w.start(cadence: const Duration(milliseconds: 50));
        expect(cycle.state, SyncCycleState.standby);
        await b.registry.requestHalt(
          'remote',
          initiator: _operator,
          purpose: HaltPurpose.pause,
        );
        await cycle();
        await Future<void>.delayed(const Duration(milliseconds: 200));
        expect(
          (await w.backend.readFifoHead('remote'))!.finalStatus,
          isNull,
          reason: 'the standing-by cycle wedged nothing',
        );
        await holder.release();
      });
    });

    group('start failures', () {
      // Verifies: EVS-DEV-destination-drain-lock/C
      // a start whose acquisition fails for a reason other than contention
      //   or misconfiguration stands by, logs one error line, and acquires
      //   at the next cadence tick once the failure is gone.
      // Verifies: EVS-DEV-destination-drain-lock/D
      // the standing-by cycle holds the trigger slot.
      test('an unreachable database at start: standby, then retry', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final id = await w.note('n1');
        final timers = ManualTimers();
        var failing = true;
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            failLockAcquisition: () => failing,
            timerFactory: timers.create,
            onLog: log.add,
          ),
          () async {
            final cycle = await w.start(
              cadence: const Duration(milliseconds: 20),
            );
            expect(cycle.state, SyncCycleState.standby);
            expect(w.store.deliveryTrigger, isNotNull);
            expect(
              log.where(
                (r) =>
                    r.level == LibraryLogLevel.severe &&
                    r.message.contains('acquiring the drain lock at start'),
              ),
              hasLength(1),
            );
            failing = false;
            await until(() async {
              await timers.fire();
              return cycle.state == SyncCycleState.running;
            }, reason: 'the retry');
            await cycle();
            expect(d.sentIds, contains(id));
          },
        );
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // an acquisition that fails after the backend obtained its exclusion
      //   primitive gives the primitive up before it reports the failure: the
      //   lock is free while the cycle stands by, the epoch is unchanged, and
      //   the next retry acquires.
      test('an acquisition that fails part-way gives the lock up', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final id = await w.note('n1');
        final epochBefore = await w.epoch();
        final timers = ManualTimers();
        var failing = true;
        final injection = giveUpInjection(() => failing);
        final b = await w.openProcess();
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            failAfterExclusionObtained: injection.failAfterExclusionObtained,
            failEpochBumpWithSerializationFailure:
                injection.failEpochBumpWithSerializationFailure,
            timerFactory: timers.create,
            onLog: log.add,
          ),
          () async {
            final cycle = await w.start(
              cadence: const Duration(milliseconds: 20),
            );
            expect(cycle.state, SyncCycleState.standby);
            // The background request's first attempt failed the same way
            // and waits for its retry timer.
            await until(
              () => log.any(
                (r) => r.message.startsWith('acquiring the drain lock failed'),
              ),
              reason: "the request's first attempt",
            );
            expect(await w.epoch(), epochBefore);
            failing = false;
            // The primitive is free: another process takes and releases it.
            final other = await b.backend.tryAcquireDrainLock(
              databaseId: b.store.databaseId,
            );
            await other.release();
            await until(() async {
              await timers.fire();
              return cycle.state == SyncCycleState.running;
            }, reason: 'the retry');
            await cycle();
            expect(d.sentIds, contains(id));
          },
        );
      });
    });

    group('close', () {
      // Verifies: EVS-DEV-destination-drain-lock/C
      // close waits for the send in flight, starts no other, and releases
      //   the lock; the outcome of the send it waited for commits.
      test('close waits for the send in flight only', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        for (var i = 0; i < 3; i++) {
          await w.note('n$i');
        }
        final gate = Completer<void>();
        d.gate = () => gate.future;
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        final pass = cycle();
        await until(() => d.started.isNotEmpty, reason: 'the first send');
        var closed = false;
        final closing = cycle.close().then((_) => closed = true);
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(closed, isFalse, reason: 'waits for the send in flight');
        gate.complete();
        await closing;
        await pass;
        expect(d.started, hasLength(1), reason: 'no other send started');
        final rows = await w.backend.listFifoEntries('x');
        expect(rows.first.finalStatus, FinalStatus.sent);
        expect(rows.skip(1).every((r) => r.finalStatus == null), isTrue);
        // The lock is free.
        final lock = await w.backend.tryAcquireDrainLock(
          databaseId: w.store.databaseId,
        );
        await lock.release();
      });

      // Verifies: EVS-DEV-destination-drain-lock/B
      // after a close that timed out, the late outcome of the send it left
      //   in flight commits nothing.
      // Verifies: EVS-PRD-destinations/J
      // an attempt whose outcome transaction does not commit (its drainer
      //   was closed) is not recorded; the item stays pending and is sent
      //   again.
      test('close with a timeout: the late outcome commits nothing', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final id = await w.note('n1');
        final gate = Completer<void>();
        d.gate = () => gate.future;
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        final pass = cycle();
        await until(() => d.started.isNotEmpty, reason: 'the send');
        final watch = Stopwatch()..start();
        await cycle.close(timeout: const Duration(milliseconds: 100));
        expect(watch.elapsed, lessThan(const Duration(seconds: 5)));
        final head = (await w.backend.readFifoHead('x'))!;
        gate.complete();
        await pass;
        expect(d.received.single, <String>[id], reason: 'the receiver has it');
        final after = (await w.row('x', head.entryId))!;
        expect(after.finalStatus, isNull);
        expect(after.attempts, isEmpty, reason: 'no attempt recorded');
        // The next drainer sends it again.
        d.gate = null;
        final next = await w.start();
        await next();
        expect(d.sentIds.where((e) => e == id), hasLength(2));
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // close returns while appends keep arriving.
      test('close returns while appends continue', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final cycle = await w.start();
        var appending = true;
        var n = 0;
        final loop = () async {
          while (appending) {
            await w.note('bg${n++}');
          }
        }();
        await until(() => n > 5, reason: 'the appends');
        await cycle.close().timeout(const Duration(seconds: 10));
        appending = false;
        await loop;
        expect(cycle.state, SyncCycleState.stopped);
      });

      // Verifies: EVS-DEV-destination-drain-lock/D
      // an append after close raises nothing and runs no pass.
      test('an append after close raises nothing', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final cycle = await w.start();
        await cycle.close();
        final errors = <Object>[];
        await runZonedGuarded(() async {
          await w.note('after');
          await Future<void>.delayed(const Duration(milliseconds: 50));
        }, (e, _) => errors.add(e));
        expect(errors, isEmpty);
        expect(d.started, isEmpty);
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // a close that arrives while the cycle handles the loss of its lock
      //   waits for that handling, and afterwards no acquisition runs.
      test(
        'close during loss handling leaves no acquisition running',
        () async {
          if (!available) return;
          final d = Receiver(id: 'x');
          await w.activate(d);
          await w.note('n1');
          final gate = Completer<void>();
          d.gate = () => gate.future;
          final timers = ManualTimers();
          var failHeartbeat = false;
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              timerFactory: timers.create,
              failNextHeartbeat: () {
                if (!failHeartbeat) return false;
                failHeartbeat = false;
                return true;
              },
            ),
            () async {
              final cycle = await w.start(
                cadence: const Duration(milliseconds: 20),
              );
              w.store.deliveryTrigger = null;
              final pass = cycle();
              await until(() => d.started.isNotEmpty, reason: 'the send');
              failHeartbeat = true;
              await timers.fire();
              final closing = cycle.close();
              gate.complete();
              await closing;
              await pass;
              final epochAfterClose = await w.epoch();
              for (var i = 0; i < 3; i++) {
                await timers.fire();
              }
              expect(
                await w.epoch(),
                epochAfterClose,
                reason: 'no acquisition',
              );
              expect(cycle.state, SyncCycleState.stopped);
            },
          );
          // Wait for the old lock session to be replaced before the next
          // acquisition (Postgres ends a lost session from its replacement).
          await until(() async {
            try {
              final lock = await w.backend.tryAcquireDrainLock(
                databaseId: w.store.databaseId,
              );
              await lock.release();
              return true;
            } on Object {
              return false;
            }
          }, reason: 'the lock is free');
        },
      );
    });

    group('outcomes that do not commit', () {
      // Verifies: EVS-PRD-destinations/J
      // a delivery whose outcome transaction does not commit leaves the item
      //   pending with no attempt; the next pass sends it again.
      test('a failed outcome transaction after SendOk', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final id = await w.note('n1');
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        var failing = true;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            failOutcomeTransaction: (_, outcome) => failing && outcome == 'ok',
          ),
          cycle.call,
        );
        final head = (await w.backend.readFifoHead('x'))!;
        expect(head.finalStatus, isNull);
        expect(head.attempts, isEmpty);
        expect(d.sentIds, <String>[id]);
        failing = false;
        await cycle();
        expect(d.sentIds, <String>[id, id], reason: 'sent again');
        expect((await w.row('x', head.entryId))!.finalStatus, FinalStatus.sent);
      });

      // Verifies: EVS-PRD-destinations/J
      // a refusal whose wedge and whose fallback both fail to commit leaves
      //   the item pending with no attempt; it is sent again.
      test('a refusal whose wedge and fallback both fail', () async {
        if (!available) return;
        final d = Receiver(id: 'x')
          ..outcome = (_) => const SendPermanent(error: 'no');
        await w.activate(d);
        final id = await w.note('n1');
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            failOutcomeTransaction: (_, outcome) => outcome == 'permanent',
          ),
          cycle.call,
        );
        final head = (await w.backend.readFifoHead('x'))!;
        expect(head.finalStatus, isNull);
        expect(head.attempts, isEmpty);
        await cycle();
        expect(d.sentIds, <String>[id, id], reason: 'sent again');
        expect(
          (await w.row('x', head.entryId))!.finalStatus,
          FinalStatus.wedged,
        );
      });
    });

    group('trigger never raises', () {
      // Verifies: EVS-DEV-destination-drain-lock/D
      // every operation that fires the event store's delivery-cycle trigger
      //   returns normally, with no uncaught error, when the cycle's policy
      //   resolver throws and after the cycle is closed. The table covers
      //   every call site of the trigger in lib/.
      test('every trigger call site', () async {
        if (!available) return;
        String? wedgedHead;
        final ops = <String, Future<void> Function()>{
          'append': () async {
            await w.note('t');
          },
          'appendReserved': () async {
            await w.store.appendReserved(
              entryType: kEntryTypeRegistryInitializedEntryType,
              aggregateId: 'reg',
              aggregateType: kRegistryAuditAggregateType,
              eventType: kEntryTypeRegistryInitializedEventType,
              data: <String, Object?>{
                'registry': <String, Object?>{'n': DateTime.now().toString()},
              },
              initiator: _init,
            );
          },
          'clearSecurityContext': () async {
            final event = await w.store.append(
              entryType: _noteType,
              aggregateId: 'sc',
              aggregateType: 'note',
              eventType: 'noted',
              data: const <String, Object?>{},
              initiator: const UserInitiator('u'),
              security: const SecurityDetails(ipAddress: '10.0.0.1'),
            );
            await w.store.clearSecurityContext(
              event!.eventId,
              reason: 'test',
              redactedBy: _operator,
            );
          },
          'applyRetentionPolicy': () async {
            await w.store.applyRetentionPolicy();
          },
          'addDestination': () =>
              w.registry.addDestination(Receiver(id: 'op'), initiator: _init),
          'setStartDate': () => w.registry.setStartDate(
            'op',
            DateTime.utc(2026, 1, 1),
            initiator: _init,
          ),
          'requestHalt': () async {
            await w.registry.requestHalt(
              'op',
              initiator: _operator,
              purpose: HaltPurpose.pause,
            );
          },
          'cancelHalt': () => w.registry.cancelHalt('op', initiator: _operator),
          'setEndDate': () async {
            await w.registry.setEndDate(
              'op',
              DateTime.utc(2030, 1, 1),
              initiator: _init,
            );
          },
          'deleteDestination': () =>
              w.registry.deleteDestination('op', initiator: _operator),
          'tombstoneAndRefill': () async {
            await w.registry.tombstoneAndRefill(
              'tr',
              wedgedHead!,
              initiator: _operator,
            );
          },
        };
        // Every call site of the trigger in lib/ is covered: the event
        // store's four append paths and the registry's one operation
        // runner, through which every registry operation commits.
        final sites = <String, int>{};
        for (final f in Directory('lib').listSync(recursive: true)) {
          if (f is! File || !f.path.endsWith('.dart')) continue;
          final n = RegExp(
            r'wakeDeliveryCycle\(\)',
          ).allMatches(f.readAsStringSync()).length;
          if (n > 0) sites[f.path.replaceAll(r'\', '/')] = n;
        }
        expect(sites, <String, int>{
          // append, appendReserved, clearSecurityContext, applyRetentionPolicy
          // and the method's own declaration.
          'lib/src/event_store.dart': 5,
          'lib/src/destinations/destination_registry.dart': 1,
          // action dispatch; its row is in test/actions/
          // action_dispatcher_test.dart.
          'lib/src/actions/action_dispatcher.dart': 1,
        });
        final tr = Receiver(id: 'tr');
        await w.activate(tr);
        for (final closedFirst in <bool>[false, true]) {
          // A wedged head for the recovery row, wedged before the cycle
          // takes the drain lock.
          await w.note('tr-$closedFirst');
          await fillForTest(tr, backend: w.backend, flushHeld: true);
          wedgedHead = await wedgeHeadForTest(w.registry, 'tr');
          final cycle = await w.start(
            policyResolver: () => throw StateError('resolver'),
          );
          if (closedFirst) await cycle.close();
          for (final entry in ops.entries) {
            final errors = <Object>[];
            final log = <LibraryLogRecord>[];
            await runZonedGuarded(
              () => runWithDeliveryTestHooks(
                DeliveryTestHooks(onLog: log.add),
                () async {
                  await entry.value();
                  await Future<void>.delayed(const Duration(milliseconds: 20));
                },
              ),
              (e, _) => errors.add(e),
            );
            expect(
              errors,
              isEmpty,
              reason: '${entry.key}, closed $closedFirst',
            );
            // The operation fired the trigger: the running cycle's throwing
            // resolver is reported, and a closed cycle reports nothing.
            expect(
              log.where(
                (r) => r.message == 'the delivery cycle trigger failed',
              ),
              closedFirst ? isEmpty : isNotEmpty,
              reason: '${entry.key} wakes the cycle, closed $closedFirst',
            );
          }
          await cycle.close();
        }
      });
    });

    group('cadence, heartbeat and wakeups', () {
      // Verifies: EVS-DEV-destination-drain-lock/E
      // a resolver that throws on a cadence tick raises nothing, logs one
      //   error line, and the next tick still runs a pass.
      // Verifies: EVS-DEV-destination-drain-lock/D
      // no error on the cadence path escapes the cycle.
      test('a throwing resolver on a cadence tick', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final id = await w.note('n1');
        final timers = ManualTimers();
        var throwing = true;
        final log = <LibraryLogRecord>[];
        final errors = <Object>[];
        await runZonedGuarded(
          () => runWithDeliveryTestHooks(
            DeliveryTestHooks(timerFactory: timers.create, onLog: log.add),
            () async {
              await w.start(
                policyResolver: () {
                  if (throwing) throw StateError('resolver');
                  return _policy;
                },
              );
              w.store.deliveryTrigger = null;
              await timers.fire();
              expect(d.started, isEmpty);
              expect(
                log.where((r) => r.message.contains('cadence failed')),
                hasLength(1),
              );
              throwing = false;
              await timers.fire();
              await until(() => d.sentIds.contains(id), reason: 'delivery');
            },
          ),
          (e, _) => errors.add(e),
        );
        expect(errors, isEmpty);
      });

      // Verifies: EVS-DEV-destination-drain-lock/C
      // a heartbeat failure on its timer raises nothing: the cycle stands by
      //   and takes the lock again with a higher epoch.
      test('a heartbeat failure: standby, then re-acquisition', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final timers = ManualTimers();
        var failNext = false;
        final errors = <Object>[];
        await runZonedGuarded(
          () => runWithDeliveryTestHooks(
            DeliveryTestHooks(
              timerFactory: timers.create,
              failNextHeartbeat: () {
                if (!failNext) return false;
                failNext = false;
                return true;
              },
            ),
            () async {
              final cycle = await w.start(
                cadence: const Duration(milliseconds: 20),
              );
              final first = (await w.epoch())!;
              failNext = true;
              await until(() async {
                await timers.fire();
                return cycle.state == SyncCycleState.running &&
                    (await w.epoch())! > first;
              }, reason: 're-acquisition');
              final id = await w.note('n1');
              await cycle();
              expect(d.sentIds, contains(id));
            },
          ),
          (e, _) => errors.add(e),
        );
        expect(errors, isEmpty);
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // the heartbeat runs on its own timer while a pass is blocked in a
      //   send.
      test('the heartbeat runs while a send is blocked', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        await w.note('n1');
        final gate = Completer<void>();
        d.gate = () => gate.future;
        final timers = ManualTimers();
        var heartbeats = 0;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            timerFactory: timers.create,
            failNextHeartbeat: () {
              heartbeats += 1;
              return false;
            },
          ),
          () async {
            final cycle = await w.start();
            w.store.deliveryTrigger = null;
            final pass = cycle();
            await until(() => d.started.isNotEmpty, reason: 'the send');
            await timers.fire();
            expect(heartbeats, greaterThan(0));
            gate.complete();
            await pass;
          },
        );
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // a trigger that arrives during a pass makes the cycle run one more
      //   pass.
      test('a trigger during a pass runs one more', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        await w.note('n1');
        final gate = Completer<void>();
        d.gate = () => gate.future;
        var passes = 0;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            timerFactory: neverFiringTimer,
            onInboundPoll: () => passes += 1,
          ),
          () async {
            final cycle = await w.start();
            w.store.deliveryTrigger = null;
            final pass = cycle();
            await until(() => d.started.isNotEmpty, reason: 'the send');
            final id = await w.note('n2');
            final second = cycle();
            d.gate = null;
            gate.complete();
            await pass;
            await second;
            expect(passes, 2);
            expect(d.sentIds, contains(id));
          },
        );
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // no trigger that arrives during a pass is lost: a call that arrives
      //   as the last pass finishes, at any point before the passes are
      //   done, still has a pass start after it, and returns once that pass
      //   has finished.
      test('a trigger as the last pass finishes is not lost', () async {
        if (!available) return;
        for (var depth = 0; depth < 12; depth++) {
          var passes = 0;
          Future<void>? lateCall;
          var lateDone = false;
          final cycle = await w.start();
          w.store.deliveryTrigger = null;
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              timerFactory: neverFiringTimer,
              onInboundPoll: () {
                passes += 1;
                if (passes != 1) return;
                void deep(int n) {
                  if (n == 0) {
                    lateCall = cycle().then((_) => lateDone = true);
                  } else {
                    scheduleMicrotask(() => deep(n - 1));
                  }
                }

                deep(depth);
              },
            ),
            () async {
              await cycle();
              while (lateCall == null) {
                await Future<void>.delayed(Duration.zero);
              }
              await lateCall;
            },
          );
          expect(lateDone, isTrue);
          expect(passes, 2, reason: 'a pass after the call at depth $depth');
          await cycle.close();
          w.cycles.remove(cycle);
        }
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // a halt request with no later append is honoured without a manual
      //   call and with no timer firing: the registry operation's trigger
      //   wakes the cycle after its commit.
      test('a halt request wakes the cycle', () async {
        if (!available) return;
        final d = Receiver(id: 'x')
          ..outcome = (_) => const SendTransient(error: 'busy');
        await w.activate(d);
        await w.note('n1');
        await runWithDeliveryTestHooks(
          const DeliveryTestHooks(timerFactory: neverFiringTimer),
          () async {
            final cycle = await w.start();
            await cycle();
            await w.registry.requestHalt(
              'x',
              initiator: _operator,
              purpose: HaltPurpose.pause,
            );
            await until(
              () async =>
                  (await w.backend.readFifoHead('x'))?.finalStatus ==
                  FinalStatus.wedged,
              reason: 'the halt honoured',
            );
          },
        );
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // the cadence alone delivers an event another process appended, with
      //   no trigger in the draining process.
      test('the cadence delivers what another process appends', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final b = await w.openProcess();
        final timers = ManualTimers();
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(timerFactory: timers.create),
          () async {
            await w.start();
            final id = await w.note('from-b', on: b.store);
            await Future<void>.delayed(const Duration(milliseconds: 50));
            expect(d.sentIds, isNot(contains(id)), reason: 'no trigger here');
            await timers.fire();
            await until(() => d.sentIds.contains(id), reason: 'the cadence');
          },
        );
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // each pass begins with a transaction that writes the heartbeat
      //   record under the lock's epoch.
      test('each pass writes the heartbeat record', () async {
        if (!available) return;
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        await cycle();
        final first = (await w.backend.transaction(
          w.backend.readDrainHeartbeatTxn,
        ))!;
        await cycle();
        final second = (await w.backend.transaction(
          w.backend.readDrainHeartbeatTxn,
        ))!;
        expect(first.epoch, await w.epoch());
        expect(second.epoch, first.epoch);
        expect(second.pass, first.pass + 1);
      });

      // Verifies: EVS-DEV-destination-drain-lock/E
      // a pass whose start transaction fails writes no heartbeat record,
      //   and still serves the destinations its registry holds.
      test('a failed pass start writes no heartbeat', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final cycle = await w.start();
        w.store.deliveryTrigger = null;
        await cycle();
        final before = await w.backend.transaction(
          w.backend.readDrainHeartbeatTxn,
        );
        final id = await w.note('n1');
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(failListSchedules: () => true),
          cycle.call,
        );
        expect(
          await w.backend.transaction(w.backend.readDrainHeartbeatTxn),
          before,
        );
        expect(d.sentIds, contains(id));
      });
    });

    group('lock loss and stopping', () {
      // Verifies: EVS-DEV-destination-drain-lock/B
      // after the drainer detects the loss of its lock it starts no further
      //   send: a loss detected while the pre-send fence transaction runs
      //   (its lock check already passed) starts no send once the fence
      //   commits.
      test(
        'a loss detected during the pre-send fence starts no send',
        () async {
          if (!available) return;
          final d = Receiver(id: 'x');
          await w.activate(d);
          await w.note('n1');
          final timers = ManualTimers();
          final first = <int?>[];
          var failNext = false;
          var inFence = false;
          final fenceHeld = Completer<void>();
          final releaseFence = Completer<void>();
          await runWithDeliveryTestHooks(
            DeliveryTestHooks(
              timerFactory: timers.create,
              failNextHeartbeat: () {
                if (!failNext) return false;
                failNext = false;
                return true;
              },
              onFenceBodyRun: (_) => inFence = true,
              beforeQueueWrites: (_) async {
                if (!inFence || fenceHeld.isCompleted) return;
                fenceHeld.complete();
                await releaseFence.future;
              },
            ),
            () async {
              final cycle = await w.start();
              w.store.deliveryTrigger = null;
              final epoch = await w.epoch();
              d.gate = () async => first.add(await w.epoch());
              final pass = cycle();
              await fenceHeld.future;
              failNext = true;
              await timers.fire();
              releaseFence.complete();
              await pass;
              expect(
                first.where((e) => e == epoch),
                isEmpty,
                reason: 'no send under the lost epoch',
              );
              await until(() async {
                await timers.fire();
                return d.started.isNotEmpty;
              }, reason: 'delivery after re-acquisition');
              expect(first.single, greaterThan(epoch!));
            },
          );
        },
      );

      // Verifies: EVS-DEV-destination-drain-lock/C
      // a cycle whose storage backend can no longer take the lock (it was
      //   closed under the cycle) stops, logging why, leaves no timer armed
      //   and gives up its in-isolate registration, so a later start over
      //   the database is not refused as a second cycle.
      test('closing the backend under a running cycle stops it', () async {
        if (!available) return;
        final d = Receiver(id: 'x');
        await w.activate(d);
        final timers = ManualTimers();
        final log = <LibraryLogRecord>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(timerFactory: timers.create, onLog: log.add),
          () async {
            final cycle = await w.start(
              cadence: const Duration(milliseconds: 20),
            );
            await cycle();
            await w.backend.close();
            await until(() async {
              await timers.fire();
              return cycle.state == SyncCycleState.stopped;
            }, reason: 'the cycle stops');
            expect(timers.active, isEmpty, reason: 'no timer left armed');
            expect(
              log.where((r) => r.message.contains('the delivery cycle stops')),
              hasLength(1),
            );
            await expectLater(
              SyncCycle.start(
                registry: w.registry,
                cadence: const Duration(hours: 1),
              ),
              throwsA(isA<DrainLockBackendClosedException>()),
              reason: 'refused for the closed backend, not as a second cycle',
            );
          },
        );
        if (label == 'postgres') {
          final b = await w.openProcess();
          final next = await w.start(on: b.registry);
          expect(next.state, SyncCycleState.running);
        }
      });
    });

    group('bootstrap', () {
      // Verifies: EVS-DEV-destination-drain-lock/D
      // an append through a store bootstrapEventStore opened drives the
      //   started cycle.
      test('an append through a bootstrapped store drives the cycle', () async {
        if (!available) return;
        final d = Receiver(id: 'boot');
        final bundle = await bootstrapEventStore(
          backend: await w.db.openBackend(),
          source: _source,
          entryTypes: const <EntryTypeDefinition>[
            EntryTypeDefinition(
              id: _noteType,
              registeredVersion: EntryTypeVersion(1, 0),
              name: _noteType,
            ),
          ],
          destinations: <Destination>[d],
        );
        await bundle.destinations.setStartDate(
          'boot',
          DateTime.utc(2000, 1, 1),
          initiator: _init,
        );
        final cycle = await SyncCycle.start(
          registry: bundle.destinations,
          cadence: const Duration(hours: 1),
        );
        w.cycles.add(cycle);
        final id = await w.note('b1', on: bundle.eventStore);
        await until(() => d.sentIds.contains(id), reason: 'the wake');
      });
    });
  });
}

/// The give-up injections of [runDeliveryCycleScenarios]: at most one is
/// set, per backend.
typedef GiveUpInjection = ({
  bool Function()? failAfterExclusionObtained,
  bool Function()? failEpochBumpWithSerializationFailure,
});
