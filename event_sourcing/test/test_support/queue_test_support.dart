// Test support for queue-level tests: persist a schedule the way a
// registration does, wedge a queue head the way the drainer does, and run
// the drainer's fill and drain directly under a drain lock the harness
// takes. This file declares no tests, so it carries no citation.
import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/sync/sync_cycle.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fake_destination.dart';

/// The `backend_state` keys of the drain lock's records: the drain epoch,
/// the heartbeat and the drainer's declaration.
const Set<String> drainLockRecordKeys = <String>{
  'drain_epoch',
  'drain_heartbeat',
  'drainer_declaration',
};

/// A delivery cycle a test drives by hand: [call] starts a `SyncCycle` over
/// [registry] (cadence and heartbeat one hour, so no timer fires inside a
/// test, and the event store's trigger slot emptied, so no append or
/// registry operation wakes it) when none is started, and runs its passes. Before a harness call
/// takes the drain lock of the same database, and before another test cycle
/// over it starts, the started cycle is closed ([pause]); the next [call]
/// starts a new one. Every started cycle is closed when the test ends.
final class TestCycle {
  TestCycle(
    this.registry, {
    this.clock,
    this.policy,
    this.policyResolver,
    this.configurationVersion,
  });

  final DestinationRegistry registry;
  final Clock? clock;
  final SyncPolicy? policy;
  final SyncPolicy? Function()? policyResolver;
  final String? configurationVersion;

  SyncCycle? _cycle;
  Map<String, UnservedReason> _lastUnserved = const <String, UnservedReason>{};

  Object get _key =>
      registry.backend.drainExclusionKey(registry.eventStore.databaseId);

  /// The started cycle, starting one when none is.
  Future<SyncCycle> started() async {
    final running = _cycle;
    if (running != null) return running;
    await pauseTestCycles(_key, except: this);
    final cycle = await SyncCycle.start(
      registry: registry,
      clock: clock,
      policy: policy,
      policyResolver: policyResolver,
      cadence: const Duration(hours: 1),
      configurationVersion: configurationVersion,
    );
    registry.eventStore.deliveryTrigger = null;
    _cycle = cycle;
    _testCycles.add(this);
    addTearDown(pause);
    return cycle;
  }

  /// Runs the started cycle's passes (see `SyncCycle.call`). Throws
  /// [StateError] when the cycle stands by (something else holds the drain
  /// lock), where a call would silently do nothing.
  Future<void> call({bool flushHeld = false}) async {
    final cycle = await started();
    if (cycle.state != SyncCycleState.running) {
      throw StateError(
        'TestCycle: the delivery cycle is ${cycle.state.name}; something '
        'else holds the drain lock of its database',
      );
    }
    await cycle(flushHeld: flushHeld);
    _lastUnserved = cycle.unserved;
  }

  /// The unserved destinations of the latest pass.
  Map<String, UnservedReason> get unserved => _cycle?.unserved ?? _lastUnserved;

  /// Closes the started cycle, if any.
  Future<void> pause() async {
    final cycle = _cycle;
    _cycle = null;
    _testCycles.remove(this);
    if (cycle == null) return;
    _lastUnserved = cycle.unserved;
    await cycle.close();
  }
}

final List<TestCycle> _testCycles = <TestCycle>[];

/// Closes every started [TestCycle] over the database whose exclusion key
/// is [key], but [except].
Future<void> pauseTestCycles(Object key, {TestCycle? except}) async {
  for (final cycle in List<TestCycle>.of(_testCycles)) {
    if (identical(cycle, except)) continue;
    if (cycle._key == key) await cycle.pause();
  }
}

/// Starts a delivery cycle over [registry], runs its passes once, and
/// closes it.
Future<void> cycleOnce(
  DestinationRegistry registry, {
  Clock? clock,
  SyncPolicy? policy,
  bool flushHeld = false,
}) async {
  final cycle = TestCycle(registry, clock: clock, policy: policy);
  try {
    await cycle(flushHeld: flushHeld);
  } finally {
    await cycle.pause();
  }
}

/// The drain locks the harness holds, by exclusion key, shared by the
/// harness calls that overlap on one database.
final Map<Object, _SharedLock> _sharedLocks = <Object, _SharedLock>{};

final class _SharedLock {
  _SharedLock(this.lock);
  final Future<DrainLock> lock;
  int users = 0;
}

/// The database identity a harness drain lock is taken for: the stored one,
/// or, on a backend no event store has opened, a fixed test identity (on
/// Postgres, minted and stored, since its drain lock verifies it).
Future<String> _harnessDatabaseId(StorageBackend backend) =>
    backend.transaction((txn) async {
      final stored = await backend.readDatabaseIdTxn(txn);
      if (stored != null) return stored;
      if (backend is SembastBackend) return 'test-database';
      return backend.readOrCreateDatabaseIdTxn(txn);
    });

/// Runs [body] holding the drain lock of [backend]'s database, taken
/// through the internal path the delivery cycle uses, as a direct caller of
/// the fill or the drain needs it. Overlapping calls on one database share
/// one lock; the last to finish releases it, so a delivery cycle started
/// later in the test can take it.
Future<T> withTestDrainLock<T>(
  StorageBackend backend,
  Future<T> Function(DrainLock lock) body, {
  String? databaseId,
}) async {
  final id = databaseId ?? await _harnessDatabaseId(backend);
  final key = backend.drainExclusionKey(id);
  await pauseTestCycles(key);
  final shared = _sharedLocks.putIfAbsent(
    key,
    () => _SharedLock(backend.tryAcquireDrainLock(databaseId: id)),
  )..users += 1;
  try {
    return await body(await shared.lock);
  } finally {
    shared.users -= 1;
    if (shared.users == 0 && identical(_sharedLocks[key], shared)) {
      _sharedLocks.remove(key);
      try {
        await (await shared.lock).release();
      } on Object catch (_) {
        // The acquisition failed; its error reached the callers.
      }
    }
  }
}

/// `drain` under a drain lock the harness takes (see [withTestDrainLock]).
Future<void> drainForTest(
  Destination destination, {
  required DestinationRegistry registry,
  Clock? clock,
  SyncPolicy? policy,
}) => withTestDrainLock(
  registry.backend,
  (lock) => drain(
    destination,
    registry: registry,
    lock: lock,
    clock: clock,
    policy: policy,
  ),
  databaseId: registry.eventStore.databaseId,
);

/// `honourHaltById` under a drain lock the harness takes.
Future<void> honourHaltForTest(
  String destinationId, {
  required DestinationRegistry registry,
}) => withTestDrainLock(
  registry.backend,
  (lock) => honourHaltById(destinationId, registry: registry, lock: lock),
  databaseId: registry.eventStore.databaseId,
);

/// `fillBatch` under a drain lock the harness takes. [source] defaults to
/// a fixed test source.
Future<void> fillForTest(
  Destination destination, {
  required StorageBackend backend,
  Source? source,
  Clock? clock,
  bool flushHeld = false,
  String? declaredFingerprint,
}) => withTestDrainLock(
  backend,
  (lock) => fillBatch(
    destination,
    backend: backend,
    source: source ?? testFillSource,
    lock: lock,
    clock: clock,
    flushHeld: flushHeld,
    declaredFingerprint: declaredFingerprint,
  ),
);

/// The source a direct fill stamps native envelopes with when a test gives
/// none.
const Source testFillSource = Source(
  hopId: 'mobile-device',
  identifier: 'test-fill-source',
  softwareVersion: 'test',
);

/// Persist [schedule] for [destinationId], stamped with a registration id
/// (the given one, or `test-registration-<destinationId>` when the schedule
/// carries none), as a registration writes it.
Future<DestinationSchedule> persistScheduleForTest(
  StorageBackend backend,
  String destinationId,
  DestinationSchedule schedule,
) async {
  final persisted = DestinationSchedule(
    startDate: schedule.startDate,
    endDate: schedule.endDate,
    registrationId:
        schedule.registrationId ?? 'test-registration-$destinationId',
    allowHardDelete: schedule.allowHardDelete,
  );
  await backend.transaction(
    (txn) => backend.writeScheduleTxn(txn, destinationId, persisted),
  );
  return persisted;
}

/// Wedge [destinationId]'s pending head the supported way: one drain pass,
/// through [registry], whose delivery reports a permanent failure (so the
/// wedge event and the wedge record are written with the wedge). Returns
/// the wedged item's `entry_id`. Throws when the head is not pending.
///
/// The pass runs at [at] (default far in the future), so a backoff left by
/// earlier attempts does not hold the send.
Future<String> wedgeHeadForTest(
  DestinationRegistry registry,
  String destinationId, {
  String error = 'refused by the test receiver',
  DateTime? at,
}) async {
  final backend = registry.backend;
  final head = await backend.readFifoHead(destinationId);
  if (head == null || head.finalStatus != null) {
    throw StateError(
      'wedgeHeadForTest($destinationId): the head is not pending '
      '(${head?.finalStatus})',
    );
  }
  await drainForTest(
    FakeDestination(
      id: destinationId,
      script: <SendResult>[SendPermanent(error: error)],
    ),
    registry: registry,
    clock: () => at ?? DateTime.utc(2100),
  );
  final after = await backend.readFifoRow(destinationId, head.entryId);
  if (after?.finalStatus != FinalStatus.wedged) {
    throw StateError(
      'wedgeHeadForTest($destinationId): the head did not wedge',
    );
  }
  return head.entryId;
}

/// Persist [schedule] (see [persistScheduleForTest]) and run one fill over
/// it: the fill reads the persisted schedule, so a test that describes the
/// window in memory persists it first.
Future<void> fillWithScheduleForTest(
  Destination destination, {
  required StorageBackend backend,
  required DestinationSchedule schedule,
  Source? source,
  Clock? clock,
  bool flushHeld = false,
}) async {
  await persistScheduleForTest(backend, destination.id, schedule);
  await fillForTest(
    destination,
    backend: backend,
    source: source,
    clock: clock,
    flushHeld: flushHeld,
  );
}
