// Implements: EVS-PRD-destinations/C
// (FIFO order — each cycle runs fillBatch
//   then drain per destination so events are promoted into the queue and
//   shipped in the order they were appended)
// Implements: EVS-PRD-destinations/E
// (pluggable delivery — SyncCycle drives
//   drain() which calls Destination.send, the application-supplied transport)
// Implements: EVS-PRD-destinations/F
// (dynamic registration — registry.all() is
//   called per cycle so destinations added or removed since the last cycle are
//   reflected in the current run without restart)
// Implements: EVS-DEV-destination-drain/J
// (the cycle refuses a retry budget below
//   one: a static policy when the cycle starts, a resolved one by filling
//   and draining nothing in that pass)
// Implements: EVS-DEV-destination-drain/T
// (each pass compares the persisted
//   schedules with the destinations its registry holds: it honours halt
//   requests on every persisted destination, fills and sends none that its
//   registry does not hold, that storage no longer knows, or that a refill
//   guard holds, and reports each of them; the pass start persists the
//   drainer's declared configuration and unserved destinations)
// Implements: EVS-DEV-destination-drain/E
// (delivery fills and sends only the
//   destinations registered in the draining process)
// Implements: EVS-PRD-destinations/V
// (at most one delivery cycle per database
//   drains at a time: a second cycle for one database within one isolate is
//   refused, and a cycle that cannot take the drain lock stands by and takes
//   over when it is released)
// Implements: EVS-DEV-destination-drain-lock/C+D+E
// (standby, takeover and re-acquisition
//   after a loss; close cancels a pending acquisition and leaves nothing
//   running; the trigger slot and a trigger that never raises; reruns on a
//   trigger that arrives during a pass, the cadence and the heartbeat; each
//   pass starts with a transaction that checks the lock and writes a
//   heartbeat record)
import 'dart:async';

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/transaction_rerun_limit.dart';
import 'package:event_sourcing/src/sync/clock.dart';
import 'package:event_sourcing/src/sync/declared_configuration.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/fill_batch.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

/// Where a delivery cycle is in its life.
enum SyncCycleState {
  /// Started, waiting for the drain lock (another drainer holds it, or it
  /// could not be acquired yet). The cycle does no delivery work.
  standby,

  /// Holding the drain lock and delivering.
  running,

  /// Closed, or stopped for good because its storage backend was fenced or
  /// closed, or its database handle cannot commit.
  stopped,
}

/// The delivery cycle of one database: fills each registered destination's
/// queue from the event log and drains it through the destination.
///
/// At most one delivery cycle drains a database at a time. [start] is the
/// only entry point; it refuses a second cycle for the same database in one
/// isolate (on Postgres, the same database and schema; on Sembast, the same
/// open database handle), throwing [StateError]. Across processes and
/// sessions the storage backend's drain lock decides: a cycle that cannot
/// take it starts in [SyncCycleState.standby], waits, and takes over when it
/// is released, without a restart.
///
/// A process that starts a cycle needs CPU while it has no requests to
/// serve: the cycle's cadence and heartbeat are timers in that process, and
/// the events and halt requests other processes commit reach the drainer at
/// its next pass. A deployment keeps at least one such process running at
/// all times. A process that does not drain keeps its event store and
/// registry and runs every registry operation; none of them needs the drain
/// lock.
///
/// Every process that may drain registers the same destinations, because
/// delivery uses the destinations registered in the process that drains.
/// Each pass records the configuration the drainer declares for each of
/// them ([declaredConfiguration]), so a recovery can tell whether a new
/// configuration is in effect. A process that receives no traffic (a
/// canary) either does not start a cycle, or accepts that it may become the
/// drainer and fill under its own declared configuration. Revisions with
/// the same data-format major and the same entry-type majors share the
/// database in any mix; a major step is deployed stop-then-start, and the
/// incompatible-generation guard refuses an overlap.
///
/// In the browser the tabs of an origin share the database, and the drain
/// lock is a Web Lock that follows the visible tab: a cycle whose page
/// becomes hidden finishes its sends in flight (waiting at most one
/// `cadence` for them), releases the lock and stands by without requesting
/// it; one whose page is visible requests it. While no tab of the origin is
/// visible nothing drains. Web Locks exist only in a secure context (HTTPS
/// or localhost): on a page without them [start] throws
/// [DrainLockConfigurationException].
///
/// A cycle whose storage backend can no longer take the lock -- the
/// backend was fenced (a conflicting build registered while its lock
/// session was lost) or closed, or its database handle cannot commit
/// ([TransactionRerunLimitException], a browser tab whose handle another
/// tab's open compacted past) -- stops for good, with one error line,
/// releases the lock so that another process or tab takes over, and gives
/// up its registration in this isolate, so a cycle over a reopened backend
/// may start. [stopped] completes and [stopCause] names the error.
///
/// Per pass: a transaction that checks the drain lock, writes the heartbeat
/// record and persists the drainer's declaration; then, per destination,
/// fillBatch and drain, run concurrently across destinations, each failure
/// caught so one destination cannot starve the others. A trigger (an
/// append, a committed registry operation, the cadence timer, a direct
/// [call]) that arrives during a pass makes the cycle run one more pass.
///
/// The retry policy is given statically (`policy:`) or resolved at the
/// start of each pass (`policyResolver:`). Its attempt budget must be at
/// least one: a static policy below one is refused with an
/// [ArgumentError], and a pass whose resolved policy is below one logs the
/// refusal and fills and drains nothing.
final class SyncCycle {
  SyncCycle._({
    required DestinationRegistry registry,
    required Object exclusionKey,
    required Clock? clock,
    required SyncPolicy? policy,
    required SyncPolicy? Function()? policyResolver,
    required Duration cadence,
    required String? configurationVersion,
  }) : _registry = registry,
       _exclusionKey = exclusionKey,
       _clock = clock,
       _policy = policy,
       _policyResolver = policyResolver,
       _cadence = cadence,
       _configurationVersion = configurationVersion;

  /// The longest `configurationVersion` [start] accepts.
  static const int maxConfigurationVersionLength = 128;

  static final RegExp _configurationVersionPattern = RegExp(
    r'^[A-Za-z0-9._\-+:@/]+$',
  );

  /// The started, not yet closed cycles of this isolate, by the exclusion
  /// key of their backend and database.
  static final Map<Object, SyncCycle> _started = <Object, SyncCycle>{};

  /// Start the delivery cycle of [registry]'s database.
  ///
  /// Throws [StateError], touching nothing, when a cycle over the same
  /// database is started and not yet closed in this isolate; throws
  /// [ArgumentError] when both [policy] and [policyResolver] are given, or
  /// when [policy]'s attempt budget is below one.
  ///
  /// Then tries the drain lock. Granted, the cycle is
  /// [SyncCycleState.running]. Held elsewhere, or not acquirable for any
  /// reason other than a misconfiguration (the database unreachable, a
  /// timeout), the cycle is [SyncCycleState.standby] and keeps requesting
  /// the lock every [cadence] in the background, so `start` never fails
  /// for contention or for a transient database error. A
  /// [DrainLockConfigurationException] (the drain lock cannot be granted
  /// with the backend's configuration, or the page has no lock manager),
  /// a [DrainLockBackendClosedException] and an [Error] are thrown, and
  /// nothing stays started.
  ///
  /// Once started, the cycle holds its event store's trigger slot, so every
  /// append and every committed registry operation wakes it, and it runs a
  /// pass at least every [cadence] and checks the drain lock (a heartbeat)
  /// every [cadence]. [clock] stamps the attempts and the fill's window.
  ///
  /// [configurationVersion] is part of each destination's declared
  /// configuration. Change it whenever something that shapes this
  /// process's queue items changes and is not visible in the declared
  /// fields: transform code, predicate code, batching code. A deployment
  /// may pass its build or revision identifier. It is recorded in every
  /// wedge and recovery event, which the log keeps for good, so it is an
  /// identifier, not free text: 1 to [maxConfigurationVersionLength]
  /// characters, each a letter, a digit or one of `. _ - + : @ /`; any other
  /// value is refused with an [ArgumentError].
  static Future<SyncCycle> start({
    required DestinationRegistry registry,
    Clock? clock,
    SyncPolicy? policy,
    SyncPolicy? Function()? policyResolver,
    Duration cadence = const Duration(seconds: 15),
    String? configurationVersion,
  }) async {
    // Everything up to the registration runs before the first await, so
    // two starts on one database in one isolate cannot both pass the check.
    if (policy != null && policyResolver != null) {
      throw ArgumentError(
        'SyncCycle.start: supply at most one of policy / policyResolver',
      );
    }
    if (policy != null) checkRetryBudget(policy);
    if (configurationVersion != null &&
        (configurationVersion.length > maxConfigurationVersionLength ||
            !_configurationVersionPattern.hasMatch(configurationVersion))) {
      throw ArgumentError.value(
        configurationVersion,
        'configurationVersion',
        'must be 1 to $maxConfigurationVersionLength characters, each a '
            'letter, a digit or one of . _ - + : @ /',
      );
    }
    final store = registry.eventStore;
    final key = registry.backend.drainExclusionKey(store.databaseId);
    if (_started.containsKey(key)) {
      throw StateError(
        'SyncCycle.start: a delivery cycle over this database is already '
        'started in this isolate; at most one delivery cycle drains a '
        'database (EVS-PRD-destinations/V). Close it before starting another.',
      );
    }
    final cycle = SyncCycle._(
      registry: registry,
      exclusionKey: key,
      clock: clock,
      policy: policy,
      policyResolver: policyResolver,
      cadence: cadence,
      configurationVersion: configurationVersion,
    );
    _started[key] = cycle;
    try {
      await cycle._begin();
      return cycle;
    } catch (_) {
      await cycle._undoStart();
      rethrow;
    }
  }

  final DestinationRegistry _registry;
  final Object _exclusionKey;
  final Clock? _clock;
  final SyncPolicy? _policy;
  final SyncPolicy? Function()? _policyResolver;
  final Duration _cadence;
  final String? _configurationVersion;

  SyncCycleState _state = SyncCycleState.standby;
  final Completer<void> _stopped = Completer<void>();

  /// The drain lock while the cycle holds it, wrapped so that a loss the
  /// cycle detected stops every sibling's next transaction.
  _CycleLock? _lock;

  /// The pending background request for the drain lock.
  DrainLockRequest? _request;

  /// Set by [close]; nothing new starts once it is.
  bool _stopping = false;
  bool _closeLogged = false;
  Future<void>? _closing;

  /// The running passes, while a [call] runs them.
  Future<void>? _running;

  /// A trigger arrived during a pass: run one more.
  bool _rerun = false;

  /// A forced trigger arrived during a pass, or while standing by.
  bool _rerunForce = false;

  /// The loss handling in progress, if any.
  Future<void>? _lossHandling;

  Timer? _cadenceTimer;
  Timer? _heartbeatTimer;

  /// Pass count under the current epoch.
  int _pass = 0;

  Map<String, UnservedReason> _unserved = const <String, UnservedReason>{};

  /// Where the cycle is in its life.
  SyncCycleState get state => _state;

  /// Completes when the cycle is stopped: closed, or stopped for good
  /// because its storage backend was fenced or closed, or its database
  /// handle cannot commit. A stopped cycle holds no registration in this
  /// isolate.
  Future<void> get stopped => _stopped.future;

  /// The error that stopped the cycle for good; null while it runs or
  /// stands by, and after [close]. An application that sees a
  /// [TransactionRerunLimitException] here closes its database and opens it
  /// again, then starts a new cycle.
  Object? get stopCause => _stopCause;
  Object? _stopCause;

  /// The destinations the latest pass did not serve, and why: those the
  /// database knows and this cycle's registry does not hold, those the
  /// registry holds and the database no longer knows, and those a refill
  /// guard holds for the configuration this cycle declares. Empty before
  /// the first pass, and after a pass that could not read the persisted
  /// schedules (that pass honours no halt on a destination its registry
  /// does not hold). The same set is persisted with the drainer's
  /// declaration, readable from any process through
  /// `DestinationRegistry.readDeliveryStatus`.
  Map<String, UnservedReason> get unserved =>
      Map<String, UnservedReason>.unmodifiable(_unserved);

  // ------------------------------------------------------------ lifecycle

  Future<void> _begin() async {
    final backend = _registry.backend;
    try {
      final lock = await backend.tryAcquireDrainLock(
        databaseId: _registry.eventStore.databaseId,
      );
      _hold(lock);
    } on DrainLockUnavailableException {
      _state = SyncCycleState.standby;
      _requestLock();
    } on Object catch (e, st) {
      // A misconfiguration, a closed backend, a handle that cannot commit
      // and a defect fail loudly at start; anything else is taken as
      // transient.
      if (e is DrainLockConfigurationException ||
          e is DrainLockBackendClosedException ||
          e is TransactionRerunLimitException ||
          e is Error) {
        rethrow;
      }
      libraryLog(
        'sync_cycle',
        'acquiring the drain lock at start failed; the cycle stands by and '
            'requests it every $_cadence',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
      _state = SyncCycleState.standby;
      _requestLock();
    }
    _registry.eventStore.deliveryTrigger = _trigger;
    _armCadence();
    _heartbeatTimer = libraryPeriodicTimer(_cadence, (_) => _beat());
  }

  /// Undoes a failed [start]: removes the registration, clears the trigger
  /// slot if it holds this cycle, cancels a request and releases a lock.
  Future<void> _undoStart() async {
    _stopping = true;
    _cadenceTimer?.cancel();
    _heartbeatTimer?.cancel();
    _clearSlot();
    if (identical(_started[_exclusionKey], this)) {
      _started.remove(_exclusionKey);
    }
    final request = _request;
    _request = null;
    await request?.cancel();
    final lock = _lock;
    _lock = null;
    await lock?.release();
    _state = SyncCycleState.stopped;
    if (!_stopped.isCompleted) _stopped.complete();
  }

  void _clearSlot() {
    final store = _registry.eventStore;
    if (identical(store.deliveryTrigger, _trigger)) {
      store.deliveryTrigger = null;
    }
  }

  /// The trigger the event store fires; bound once so the slot can be
  /// compared by identity.
  late final Future<void> Function() _trigger = call;

  void _hold(DrainLock lock) {
    final held = _CycleLock(lock);
    _lock = held;
    _pass = 0;
    _state = SyncCycleState.running;
    unawaited(
      lock.lost.then((_) {
        if (identical(_lock, held)) _lockLost(held, 'the backend detected it');
      }),
    );
    unawaited(
      lock.handOverRequested.then((_) {
        if (identical(_lock, held)) _handOver(held);
      }),
    );
  }

  void _requestLock() {
    if (_stopping || _request != null) return;
    final request = _registry.backend.requestDrainLock(
      databaseId: _registry.eventStore.databaseId,
      retryInterval: _cadence,
    );
    _request = request;
    unawaited(
      request.granted.then(
        (lock) async {
          final current = identical(_request, request);
          if (current) _request = null;
          if (lock == null) return;
          // A request the cycle withdrew (close, or a later request) holds
          // no lock the cycle keeps.
          if (_stopping || !current || _lock != null) {
            await lock.release();
            return;
          }
          _hold(lock);
          libraryLog(
            'sync_cycle',
            'the delivery cycle took the drain lock (epoch ${lock.epoch})',
          );
          _triggerSafely(force: _rerunForce);
        },
        onError: (Object e, StackTrace st) {
          if (identical(_request, request)) _request = null;
          if (_stopping) return;
          libraryLog(
            'sync_cycle',
            'the delivery cycle stops: its storage backend can no longer take '
                'the drain lock',
            level: LibraryLogLevel.severe,
            error: e,
            stackTrace: st,
          );
          _stopForGood(e);
        },
      ),
    );
  }

  /// Stops the cycle for good, because its storage backend can no longer
  /// take the drain lock: cancels its timers and any request, gives up the
  /// trigger slot and the in-isolate registration (so a new cycle may start
  /// over the database), and releases a lock it still holds. [cause] is
  /// kept as [stopCause].
  void _stopForGood(Object cause) {
    _stopCause ??= cause;
    _stopping = true;
    _cadenceTimer?.cancel();
    _heartbeatTimer?.cancel();
    _clearSlot();
    if (identical(_started[_exclusionKey], this)) {
      _started.remove(_exclusionKey);
    }
    final request = _request;
    _request = null;
    final lock = _lock;
    _lock = null;
    _state = SyncCycleState.stopped;
    if (!_stopped.isCompleted) _stopped.complete();
    unawaited(() async {
      try {
        await request?.cancel();
        await lock?.release();
      } on Object catch (e, st) {
        libraryLog(
          'sync_cycle',
          'releasing the drain lock of a stopped delivery cycle failed',
          level: LibraryLogLevel.warning,
          error: e,
          stackTrace: st,
        );
      }
    }());
  }

  /// Stops the cycle for good because a transaction found its database
  /// handle unable to commit: every later transaction on the handle fails
  /// the same way, so the cycle can neither drain nor keep the lock. The
  /// lock is released, so another process or tab takes over.
  void _handleCannotCommit(TransactionRerunLimitException e, StackTrace st) {
    if (_stopping) return;
    libraryLog(
      'sync_cycle',
      'the delivery cycle stops and releases the drain lock: its database '
          'handle cannot commit; the application closes the database and '
          'opens it again',
      level: LibraryLogLevel.severe,
      error: e,
      stackTrace: st,
    );
    _stopForGood(e);
  }

  /// Handles the loss of [held]: stops every further queue change through
  /// it, waits for the sends in flight to settle, releases it, and stands by
  /// requesting the lock again. Never stops the cycle for good.
  void _lockLost(_CycleLock held, String how) {
    if (held.lostByCycle) return;
    held.lostByCycle = true;
    // A hand-over in progress releases the lock and stands by itself.
    if (_stopping || held.handingOver) return;
    libraryLog(
      'sync_cycle',
      'the delivery cycle lost the drain lock (epoch ${held.epoch}; $how); '
          'it stands by and requests it again',
      level: LibraryLogLevel.warning,
    );
    final running = _running;
    _lossHandling = () async {
      if (_stopping) return;
      if (running != null) {
        try {
          await running;
        } on Object catch (_) {
          // The pass's own failure is logged where it happened.
        }
      }
      if (_stopping) return;
      if (identical(_lock, held)) _lock = null;
      await held.release();
      if (_stopping) return;
      _state = SyncCycleState.standby;
      _requestLock();
    }();
  }

  /// Hands [held] over because the runtime asked for it (in the browser, the
  /// page became hidden): starts no further send or fill, waits for the
  /// passes in flight (the outcomes of their sends commit, since the lock is
  /// still held), releases the lock, and stands by. The request it then
  /// makes waits until the runtime allows it (in the browser, until the page
  /// is visible). The release is the cycle's own, never a loss.
  void _handOver(_CycleLock held) {
    if (held.lostByCycle || held.handingOver || _stopping) return;
    held.handingOver = true;
    libraryLog(
      'sync_cycle',
      'the delivery cycle hands the drain lock over (epoch ${held.epoch}; '
          'the page is hidden): it finishes its sends in flight, releases '
          'the lock and stands by',
    );
    _lossHandling = () async {
      // A call that arrives meanwhile starts no work, but may briefly hold
      // the running slot.
      final settled = () async {
        for (var running = _running; running != null; running = _running) {
          try {
            await running;
          } on Object catch (_) {
            // The pass's own failure is logged where it happened.
          }
        }
      }();
      // A send that does not return within a cadence does not keep the lock
      // from the visible tab: once the lock is released its outcome commits
      // nothing, and the next drainer sends the item again.
      await settled.timeout(
        _cadence,
        onTimeout: () {
          libraryLog(
            'sync_cycle',
            'a send in flight did not return within $_cadence of the page '
                'becoming hidden; the delivery cycle releases the drain lock '
                'without its outcome, and the next drainer sends it again',
            level: LibraryLogLevel.warning,
          );
        },
      );
      if (_stopping) return;
      if (identical(_lock, held)) _lock = null;
      await held.release();
      if (_stopping) return;
      _state = SyncCycleState.standby;
      _requestLock();
    }();
  }

  void _armCadence() {
    if (_stopping) return;
    _cadenceTimer = libraryTimer(_cadence, () async {
      try {
        await call();
      } on Object catch (e, st) {
        libraryLog(
          'sync_cycle',
          'a pass started by the cadence failed',
          level: LibraryLogLevel.severe,
          error: e,
          stackTrace: st,
        );
      } finally {
        _armCadence();
      }
    });
  }

  Future<void> _beat() async {
    final held = _lock;
    if (held == null || _stopping) return;
    if (held.inner.isReleased) {
      // The cycle did not release it: its backend did (it was closed).
      _lockLost(held, 'its storage backend released it');
      return;
    }
    try {
      await held.inner.heartbeat();
    } on Object catch (e, st) {
      if (_stopping || held.inner.isReleased) return;
      libraryLog(
        'sync_cycle',
        'a heartbeat of the drain lock failed',
        level: LibraryLogLevel.warning,
        error: e,
        stackTrace: st,
      );
      _lockLost(held, 'a heartbeat failed');
    }
  }

  void _triggerSafely({bool force = false}) {
    unawaited(
      call(flushHeld: force).then(
        (_) {},
        onError: (Object e, StackTrace st) => libraryLog(
          'sync_cycle',
          'a pass failed',
          level: LibraryLogLevel.severe,
          error: e,
          stackTrace: st,
        ),
      ),
    );
  }

  /// Stop the cycle: cancel its timers and a pending lock request (a grant
  /// that races the cancellation is released), wait for the passes in
  /// flight (the sends already started) and any loss handling, give up the
  /// trigger slot, and release the drain lock. After it returns the cycle
  /// makes no acquisition and opens no lock connection, and a new cycle
  /// may start over the database.
  ///
  /// With [timeout], the lock is released once [timeout] has passed even if
  /// a send is still in flight; the outcome of that send then commits
  /// nothing, because every queue-changing transaction checks the lock
  /// first. Its receiver may still hold the delivery (delivery is
  /// at-least-once).
  Future<void> close({Duration? timeout}) =>
      _closing ??= _close(timeout: timeout);

  Future<void> _close({Duration? timeout}) async {
    _stopping = true;
    _cadenceTimer?.cancel();
    _heartbeatTimer?.cancel();
    final work = () async {
      final request = _request;
      _request = null;
      await request?.cancel();
      final running = _running;
      if (running != null) {
        try {
          await running;
        } on Object catch (_) {
          // Logged where it happened.
        }
      }
      final loss = _lossHandling;
      if (loss != null) await loss;
    }();
    if (timeout == null) {
      await work;
    } else {
      await work.timeout(timeout, onTimeout: () {});
    }
    _clearSlot();
    if (identical(_started[_exclusionKey], this)) {
      _started.remove(_exclusionKey);
    }
    final lock = _lock;
    _lock = null;
    await lock?.release();
    _state = SyncCycleState.stopped;
    if (!_stopped.isCompleted) _stopped.complete();
  }

  // ----------------------------------------------------------------- pass

  /// Run delivery passes until no trigger arrived during the last one, and
  /// return once they are done. A call that arrives while passes run starts
  /// none beside them: the running passes run one more, and the call returns
  /// when that pass is done, so a pass that started after the call has
  /// finished when it returns. Returns at once, doing nothing, while
  /// standing by (the first pass after the lock is granted runs promptly)
  /// and after [close].
  ///
  /// [flushHeld] makes the pass bypass fillBatch's single-event
  /// `maxAccumulateTime` hold, so a lone matching event ships now; a forced
  /// trigger that arrives during a pass makes the extra pass forced.
  ///
  /// A `policyResolver` that throws propagates to a direct caller; the
  /// event store's trigger and the cadence timer log it.
  Future<void> call({bool flushHeld = false}) async {
    if (_stopping || _state == SyncCycleState.stopped) {
      if (!_closeLogged) {
        _closeLogged = true;
        libraryLog(
          'sync_cycle',
          'a trigger reached a closed delivery cycle; it does nothing',
        );
      }
      return;
    }
    if (_state == SyncCycleState.standby || _lock == null) {
      if (flushHeld) _rerunForce = true;
      return;
    }
    final inFlight = _running;
    if (inFlight != null) {
      _rerun = true;
      if (flushHeld) _rerunForce = true;
      return inFlight;
    }
    final running = _runPasses(flushHeld);
    _running = running;
    await running;
  }

  /// Runs passes until no trigger arrived during the last one. The check
  /// for a trigger and the clearing of [_running] happen in one synchronous
  /// step, so a trigger either joins these passes or starts new ones.
  Future<void> _runPasses(bool flushHeld) async {
    try {
      var force = flushHeld;
      do {
        await _passes(force);
        force = false;
      } while (_rerun && !_stopping && !(_lock?.stopsWork ?? true));
    } finally {
      _running = null;
    }
  }

  Future<void> _passes(bool flushHeld) async {
    var force = flushHeld || _rerunForce;
    while (true) {
      _rerun = false;
      _rerunForce = false;
      final held = _lock;
      if (_stopping || held == null || held.stopsWork) return;
      if (held.inner.isReleased) {
        _lockLost(held, 'its storage backend released it');
        return;
      }
      // Resolved once per pass; the same value is used for every
      // destination of the pass.
      final passPolicy = _policyResolver != null ? _policyResolver() : _policy;
      if (passPolicy != null && passPolicy.maxAttempts < 1) {
        libraryLog(
          'sync_cycle',
          'the policy resolver returned a retry budget of '
              '${passPolicy.maxAttempts}; a budget must be at least one '
              'attempt, so this pass fills and drains nothing',
          level: LibraryLogLevel.severe,
        );
        return;
      }
      await _onePass(held, passPolicy, force);
      if (!_rerun || _stopping) return;
      force = _rerunForce;
    }
  }

  Future<void> _onePass(
    _CycleLock held,
    SyncPolicy? passPolicy,
    bool force,
  ) async {
    _PassPlan plan;
    try {
      plan = await _passStart(held);
    } on DrainLockLostException catch (e) {
      _lockLost(held, e.message);
      return;
    } on TransactionRerunLimitException catch (e, st) {
      _handleCannotCommit(e, st);
      return;
    } on Object catch (e, st) {
      if (held.inner.isReleased) {
        _lockLost(held, 'its storage backend released it');
        return;
      }
      // Without a reading of the persisted schedules the pass cannot tell
      // which destinations the database knows: it fills and sends the
      // registered destinations (each of their transactions checks the
      // drain lock and the persisted schedule itself) and honours no halt
      // on any other.
      libraryLog(
        'sync_cycle',
        'the pass-start transaction failed (reading the persisted '
            'destination schedules); this pass fills and sends the registered '
            'destinations and honours no halt on any other',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
      _unserved = const <String, UnservedReason>{};
      final registered = _registry.all();
      plan = _PassPlan(
        served: registered,
        haltOnly: const <String>[],
        declared: <String, _Declared>{
          for (final d in registered)
            d.id: _Declared.of(declaredConfiguration(d, _configurationVersion)),
        },
      );
    }
    // One destination's failure does not cancel the others. The lists come
    // from one reading of the schedules and are disjoint, so no destination
    // is drained and honoured at once.
    await Future.wait(<Future<void>>[
      for (final d in plan.served)
        _fillAndDrainOrSwallow(held, d, plan.declared[d.id], passPolicy, force),
      for (final id in plan.haltOnly) _honourHaltOrSwallow(held, id),
    ]);
    await pollInbound();
  }

  /// The pass-start transaction: checks the drain lock, writes the
  /// heartbeat record, reads the persisted schedules and refill guards, and
  /// writes the drainer's declaration when it changed.
  Future<_PassPlan> _passStart(_CycleLock held) async {
    final backend = _registry.backend;
    final registered = _registry.all();
    final declared = <String, _Declared>{
      for (final d in registered)
        d.id: _Declared.of(declaredConfiguration(d, _configurationVersion)),
    };
    final now = (_clock ?? () => DateTime.now().toUtc())();
    final pass = _pass + 1;
    final result = await backend.transaction((txn) async {
      await held.assertHeldInTxn(txn);
      await backend.writeDrainHeartbeatTxn(
        txn,
        DrainHeartbeat(epoch: held.epoch, pass: pass, at: now),
      );
      if (DeliveryTestHooks.current?.failListSchedules?.call() ?? false) {
        throw const InjectedFailure('the read of the persisted schedules');
      }
      final schedules = await backend.listSchedulesTxn(txn);
      final persisted = schedules.keys.toSet();
      final registeredIds = <String>{for (final d in registered) d.id};
      final unserved = <String, UnservedReason>{
        for (final id in persisted)
          if (!registeredIds.contains(id)) id: UnservedReason.notRegisteredHere,
        for (final id in registeredIds)
          if (!persisted.contains(id)) id: UnservedReason.deletedInStorage,
        for (final id in registeredIds)
          if (persisted.contains(id) &&
              schedules[id]!.registrationId !=
                  _registry.localRegistrationId(id))
            id: UnservedReason.registrationMismatch,
      };
      for (final id in registeredIds) {
        if (unserved.containsKey(id)) continue;
        final guard = await backend.readRefillGuardTxn(txn, id);
        if (guard != null && guard.fingerprint == declared[id]!.fingerprint) {
          unserved[id] = UnservedReason.refillAwaitsChangedConfiguration;
        }
      }
      final declaration = DrainerDeclaration(
        epoch: held.epoch,
        configurationVersion: _configurationVersion,
        // A destination held under a stale registration is not served, so
        // no configuration is declared for it.
        configurations: <String, Map<String, Object?>>{
          for (final e in declared.entries)
            if (unserved[e.key] != UnservedReason.registrationMismatch)
              e.key: e.value.configuration,
        },
        fingerprints: <String, String>{
          for (final e in declared.entries)
            if (unserved[e.key] != UnservedReason.registrationMismatch)
              e.key: e.value.fingerprint,
        },
        unserved: unserved,
        declaredAt: now,
      );
      final stored = await backend.readDrainerDeclarationTxn(txn);
      if (stored == null || !stored.declaresSameAs(declaration)) {
        await backend.writeDrainerDeclarationTxn(txn, declaration);
      }
      return (persisted: persisted, unserved: unserved);
    });
    _pass = pass;
    final unserved = result.unserved;
    for (final entry in unserved.entries) {
      if (_unserved[entry.key] == entry.value) continue;
      libraryLog('sync_cycle', switch (entry.value) {
        UnservedReason.notRegisteredHere =>
          'destination ${entry.key} is persisted but not registered in '
              "this cycle's registry; it is not filled or sent here, and a "
              'halt requested on it is still honoured',
        UnservedReason.deletedInStorage =>
          "destination ${entry.key} is registered in this cycle's "
              'registry but has no persisted schedule (deleted by another '
              'process); it is not filled or sent',
        UnservedReason.refillAwaitsChangedConfiguration =>
          'destination ${entry.key} is held by a refill guard: its '
              'recovery of a reconfigure halt awaits a drainer that '
              'declares another configuration; it is not refilled under the '
              'configuration this cycle declares',
        UnservedReason.registrationMismatch =>
          "destination ${entry.key} is registered in this cycle's "
              'registry under another registration than the database holds '
              '(another process deleted and registered it again); it is not '
              'filled or sent here until this process registers it again, '
              'and a halt requested on it is still honoured',
      }, level: LibraryLogLevel.warning);
    }
    _unserved = unserved;
    final registeredIds = <String>{for (final d in registered) d.id};
    bool mismatched(String id) =>
        unserved[id] == UnservedReason.registrationMismatch;
    return _PassPlan(
      served: <Destination>[
        for (final d in registered)
          if (result.persisted.contains(d.id) && !mismatched(d.id)) d,
      ],
      haltOnly: <String>[
        for (final id in result.persisted)
          if (!registeredIds.contains(id) || mismatched(id)) id,
      ],
      declared: declared,
    );
  }

  Future<void> _honourHaltOrSwallow(
    _CycleLock held,
    String destinationId,
  ) async {
    try {
      await honourHaltById(destinationId, registry: _registry, lock: held);
    } on DrainLockLostException catch (e) {
      _lockLost(held, e.message);
    } on TransactionRerunLimitException catch (e, st) {
      _handleCannotCommit(e, st);
    } on Object catch (e, st) {
      libraryLog(
        'sync_cycle',
        'honouring a halt request failed for destination $destinationId',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  Future<void> _fillAndDrainOrSwallow(
    _CycleLock held,
    Destination destination,
    _Declared? declared,
    SyncPolicy? passPolicy,
    bool flushHeld,
  ) async {
    // Step 1: promote events appended since the last pass into this
    // destination's queue, after performing any replay a registry operation
    // requested. The fill reads the persisted schedule and refill guard
    // itself.
    if (_stopping || held.stopsWork) return;
    try {
      await fillBatch(
        destination,
        backend: _registry.backend,
        source: _registry.eventStore.source,
        lock: held,
        clock: _clock,
        flushHeld: flushHeld,
        declaredFingerprint: declared?.fingerprint,
        registrationId: _registry.localRegistrationId(destination.id),
      );
    } on DrainLockLostException catch (e) {
      _lockLost(held, e.message);
      return;
    } on TransactionRerunLimitException catch (e, st) {
      _handleCannotCommit(e, st);
      return;
    } on Object catch (e, st) {
      // One destination's fill failure must not cancel another's drain. The
      // drain step still runs because items enqueued by an earlier pass are
      // still drainable. A failed fill wrote nothing (its transaction rolled
      // back) and has no per-attempt record; without this log, a
      // destination whose fill fails on every pass would silently stop
      // receiving new items.
      libraryLog(
        'sync_cycle',
        'fillBatch failed for destination ${destination.id}',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
    // Step 2: ship whatever sits at the queue head.
    try {
      await drain(
        destination,
        registry: _registry,
        lock: held,
        clock: _clock,
        policy: passPolicy,
        declared: declared == null
            ? null
            : DrainerConfiguration(
                configuration: declared.configuration,
                fingerprint: declared.fingerprint,
              ),
        stopRequested: () => _stopping || held.stopsWork,
      );
    } on DrainLockLostException catch (e) {
      _lockLost(held, e.message);
    } on TransactionRerunLimitException catch (e, st) {
      _handleCannotCommit(e, st);
    } on Object catch (e, st) {
      // A send's own failure never reaches here: drain records it as the
      // attempt's outcome, and a wedge that reports failure is followed by
      // a transaction that reads the head again and records the attempt
      // alone when the wedge did not commit. An exception that escapes
      // drain means an outcome transaction reported failure, so whether it
      // committed is not known here: the next pass reads the head again
      // (a wedged head ends the pass; a pending head's recorded attempts
      // decide its status) before any send. The log line below is the
      // only record of the failure.
      libraryLog(
        'sync_cycle',
        'drain failed for destination ${destination.id}',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  /// Inbound-tombstone hook, invoked once per pass after every outbound
  /// drain completes. The body does nothing: remote-authored tombstones
  /// (deletions initiated by another party) do not propagate inbound. The
  /// capability is recorded in `spec/roadmap/sync.md`.
  Future<void> pollInbound() async {
    final seam = DeliveryTestHooks.current?.onInboundPoll;
    if (seam == null) return;
    try {
      seam();
    } on Object catch (e, st) {
      libraryLog(
        'sync_cycle',
        'the onInboundPoll test seam threw',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }
}

/// A destination's declared configuration and its fingerprint.
final class _Declared {
  const _Declared(this.configuration, this.fingerprint);

  factory _Declared.of(Map<String, Object?> configuration) =>
      _Declared(configuration, configurationFingerprint(configuration));

  final Map<String, Object?> configuration;
  final String fingerprint;
}

/// What one pass does, decided from its pass-start transaction.
final class _PassPlan {
  const _PassPlan({
    required this.served,
    required this.haltOnly,
    required this.declared,
  });

  final List<Destination> served;
  final List<String> haltOnly;
  final Map<String, _Declared> declared;
}

/// The cycle's view of its drain lock: once the cycle detected the loss,
/// every later transaction of any destination of the pass refuses at its
/// lock check, before its writes.
final class _CycleLock implements DrainLock {
  _CycleLock(this.inner);

  final DrainLock inner;

  /// Set when the cycle detected the loss.
  bool lostByCycle = false;

  /// Set when the cycle hands the lock over. Unlike a loss it does not fail
  /// the lock check: the outcomes of the sends in flight still commit.
  bool handingOver = false;

  /// No further send, fill or pass starts through this lock.
  bool get stopsWork => lostByCycle || handingOver;

  @override
  int get epoch => inner.epoch;

  @override
  bool get isReleased => inner.isReleased;

  @override
  Future<void> assertHeldInTxn(Transaction txn) async {
    if (lostByCycle && !inner.isReleased) {
      throw const DrainLockLostException(
        DrainLockLossReason.lossDetected,
        'the delivery cycle detected the loss of its drain lock',
      );
    }
    await inner.assertHeldInTxn(txn);
  }

  @override
  Future<void> assertHeld() => inner.assertHeld();

  @override
  Future<void> heartbeat() => inner.heartbeat();

  @override
  Future<void> release() => inner.release();

  @override
  Future<void> get lost => inner.lost;

  @override
  Future<void> get handOverRequested => inner.handOverRequested;
}
