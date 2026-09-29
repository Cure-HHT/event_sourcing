// Implements: EVS-DEV-view-convergence/G
// the driver's start is scheduled through a runtime timer, so its first
//   transaction never runs until the microtasks of EventStore.open's own
//   return have drained.
// Implements: EVS-DEV-view-convergence/H
// stop() sets a flag a transaction attempt checks immediately before it
//   would begin, so a stop that races a due attempt wins.
// Implements: EVS-DEV-view-convergence/I
// stop() awaits the catch-up transaction in flight, if any, before it
//   returns.
// Implements: EVS-DEV-view-convergence/J
// the driver repeats catch-up transactions on a copy while it is behind --
//   for this instance's own registered views, forever: another build's
//   append can make a current copy converging again, and the next idle
//   pass notices it.
// Implements: EVS-DEV-view-convergence/L
// Implements: EVS-DEV-view-convergence/M
// a copy's lock, and on Postgres the sequence-counter table's SHARE lock
//   that orders a catch-up transaction against appends, are the backend's
//   own concern (StorageBackend.catchUpTransaction): this driver only
//   reacts to a null result by treating the attempt as lock-held.
// Implements: EVS-DEV-view-convergence/N
// a catch-up transaction begins no further step once the driver's clock
//   shows 200 ms have passed since the transaction began.
// Implements: EVS-DEV-view-convergence/O
// the step loop is a do-while: the first step always runs, whatever the
//   clock reads.
// Implements: EVS-DEV-view-convergence/Q
// a throw from a step is logged, recorded as the copy's last failure, and
//   backed off from 1 s, doubling to a 5-minute cap, without stopping the
//   driver or the catch-up of its other copies.
// Implements: EVS-DEV-view-convergence/S
// copies marked for deletion are found by the driver's discovery pass and
//   worked the same way: up to 500 rows per step, then the copy's own
//   record.
// Implements: EVS-DEV-view-convergence/T
// a registered view whose unmarked copy is missing gets a fresh one created
//   in the catch-up transaction that first notices it, before anything
//   folds into it.
import 'dart:async';

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/view_fingerprint.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart' show libraryTimer;
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/storage/view_copy_lock.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

// Implements: EVS-DEV-view-convergence/W
// a catch-up step holds the appends it orders against for at most this
//   bound, so a serving append commits within 1 second while a copy
//   converges.
// Implements: EVS-DEV-view-convergence/X
// steps repeat back to back while a copy is behind, so its watermark
//   reaches the log head within the measured window.
/// The step bound of one catch-up transaction (EVS-DEV-view-convergence/N):
/// no further step begins once a transaction has run this long.
@internal
const Duration kCatchUpStepBound = Duration(milliseconds: 200);

/// The row limit of one deletion step (EVS-DEV-view-convergence Terms).
@internal
const int kCatchUpDeleteBatchLimit = 500;

/// The initial retry delay after a catch-up transaction throws, doubling
/// with each consecutive failure (EVS-DEV-view-convergence/Q).
@internal
const Duration kCatchUpInitialBackoff = Duration(seconds: 1);

/// The retry delay cap (EVS-DEV-view-convergence/Q).
@internal
const Duration kCatchUpMaxBackoff = Duration(minutes: 5);

/// How long the driver idles between discovery passes when nothing is due:
/// no assertion binds this value, only that the driver keeps noticing new
/// work without spinning.
@internal
const Duration kCatchUpIdleInterval = Duration(seconds: 1);

/// Progress of one copy's catch-up, as the driver holds it in memory for
/// this instance's lifetime.
@internal
class ViewCopyProgress {
  ViewCopyProgress({this.lastFailure, this.lastFailureAt});

  /// The error the copy's last failed catch-up transaction threw, or null
  /// if its last attempt did not fail.
  Object? lastFailure;

  /// When [lastFailure] was recorded.
  DateTime? lastFailureAt;

  /// Consecutive failures since the last committed transaction, driving the
  /// backoff delay.
  int consecutiveFailures = 0;

  /// The time before which the driver will not retry this copy, or null
  /// when it is not backing off.
  DateTime? nextAttemptAt;
}

/// A registered view this driver keeps current: its name and the
/// fingerprint of its definition, from which the driver finds (or creates)
/// the instance's unmarked copy.
class _ViewTarget {
  _ViewTarget(this.viewName, this.fingerprint);
  final String viewName;
  final String fingerprint;
}

/// Test support: holds a copy's catch-up lock exactly as a Sembast
/// catch-up transaction attempt would (the isolate-local registry
/// `StorageBackend.catchUpTransaction` uses), so a test can force the
/// driver to find it held. Returns false when it is already held. Has no
/// effect on a `PostgresBackend`'s copy lock, which is a database-level
/// advisory lock, not isolate-local state.
@internal
bool debugTryHoldViewCopyLock(Object handle, String copyId) =>
    tryLockViewCopyIsolate(handle, copyId);

/// Test support: releases a lock [debugTryHoldViewCopyLock] took.
@internal
void debugReleaseViewCopyLock(Object handle, String copyId) =>
    unlockViewCopyIsolate(handle, copyId);

/// Drives the catch-up of an event store's copies after its open has
/// returned: creates a copy a registered fingerprint lacks, folds each
/// behind copy through bounded transactions, and deletes the rows and
/// record of every copy marked for deletion.
///
/// One catch-up transaction runs at a time for this driver. [start] must be
/// called only once open has returned to its caller (a timer task, not a
/// microtask, so it never races the caller's own continuation); [stop]
/// awaits the transaction in flight, if any, and prevents a further one
/// from beginning.
@internal
class ViewCatchUpDriver {
  ViewCatchUpDriver({
    required StorageBackend backend,
    required EntryTypeRegistry entryTypes,
    required ProjectionRegistry projections,
    required PromoterRegistry promoters,
    required Map<String, String> viewCopyIds,
    DateTime Function()? clock,
    this.onCaughtUp,
  }) : _backend = backend,
       _entryTypes = entryTypes,
       _projections = projections,
       _promoters = promoters,
       _viewCopyIds = viewCopyIds,
       _clock = clock ?? DateTime.now,
       _targets = [
         for (final spec in projections.all())
           _ViewTarget(
             spec.viewName,
             viewFingerprint(spec, entryTypes, promoters),
           ),
       ];

  final StorageBackend _backend;
  final EntryTypeRegistry _entryTypes;
  final ProjectionRegistry _projections;
  final PromoterRegistry _promoters;
  final Map<String, String> _viewCopyIds;
  final DateTime Function() _clock;
  final List<_ViewTarget> _targets;

  /// Called, after a catch-up transaction on a registered view's copy
  /// commits finding it at the log's tip, naming the view
  /// (EVS-DEV-converging-view-reads/G): the signal a live `AggregateMode`
  /// subscription on it waits for to redeliver and report the view current.
  /// Not a claim that the copy stayed current past the call. Fires on every
  /// pass that finds the copy at tip, not only a transition into that
  /// state: an idle pass over an already-current copy still commits and
  /// still confirms the tip, and a build sharing the copy with another
  /// instance -- one whose own catch-up never runs a step because the
  /// other instance's last commit already moved the watermark -- has no
  /// other way to learn the copy is current. Settable after construction:
  /// `EventStore.open` wires it once its own fields (which this driver is
  /// built from, in its constructor's initializer list) are all in place.
  void Function(String viewName)? onCaughtUp;

  final Map<String, ViewCopyProgress> _progressByCopyId = {};
  final Set<String> _deletionCopyIds = {};

  bool _stopping = false;
  Future<void>? _loopFuture;
  Future<void>? _currentTransaction;
  void Function()? _cancelWait;

  DateTime Function() get _effectiveClock =>
      DeliveryTestHooks.current?.catchUpClock ?? _clock;

  /// This instance's in-memory progress of [copyId] -- its last failure and
  /// the backoff it is retrying under, if it is behind -- or null when no
  /// catch-up transaction on it has failed.
  ViewCopyProgress? progressOf(String copyId) => _progressByCopyId[copyId];

  /// Schedules the driver's loop to begin on the event loop's task queue,
  /// after every microtask already pending -- in particular, after the
  /// continuation of the `await` that called `EventStore.open` -- has run.
  // Implements: EVS-DEV-view-convergence/G
  // no catch-up transaction begins before open has returned to its caller.
  void start() {
    Timer.run(() {
      if (_stopping) return;
      _loopFuture = _runLoop();
    });
  }

  /// Stops the driver: no further catch-up transaction begins, and this
  /// awaits the one in flight, if any, before returning.
  // Implements: EVS-DEV-view-convergence/H
  // Implements: EVS-DEV-view-convergence/I
  Future<void> stop() async {
    _stopping = true;
    _cancelWait?.call();
    final txn = _currentTransaction;
    if (txn != null) {
      // try/catch, not Future.catchError: the awaited future's reified
      // type argument is the backend's T from catchUpTransaction<T>, not
      // this field's declared Future<void>, so a null-returning onError
      // callback can fail catchError's own runtime type check. try/catch
      // has no such constraint. The failure is already recorded and
      // backed off by _attemptView/_attemptDeletion; stop() only waits for
      // the transaction to finish, not re-surfaces it.
      try {
        await txn;
      } on Object {
        // Swallowed: see above.
      }
    }
    final loop = _loopFuture;
    if (loop != null) await loop;
  }

  Future<void> _runLoop() async {
    while (!_stopping) {
      final List<ViewCopy> copies;
      try {
        copies = await _discover();
      } catch (e, st) {
        // The backend closed, or some other transient failure, outside a
        // copy's own catch-up transaction: logged like any other failure
        // (EVS-DEV-view-convergence/Q); the driver keeps retrying on the
        // idle cadence rather than leaving an unhandled error in flight.
        libraryLog(
          'view_catch_up',
          'the catch-up driver could not read its copies',
          level: LibraryLogLevel.warning,
          error: e,
          stackTrace: st,
        );
        if (_stopping) return;
        await _idleWait(kCatchUpIdleInterval);
        continue;
      }
      var progressed = false;
      for (final target in _targets) {
        if (_stopping) return;
        final outcome = await _attemptView(target, copies);
        // Implements: EVS-DEV-view-convergence/Q
        // a failure backs off; it is not progress, so it must not skip the
        //   idle wait below -- a copy that fails every attempt (a missing
        //   copy folding a poisoned event, among others) would otherwise
        //   spin the discovery loop with no delay at all.
        if (outcome == _Outcome.progressed) progressed = true;
      }
      for (final copyId in _deletionCopyIds.toList()) {
        if (_stopping) return;
        final outcome = await _attemptDeletion(copyId);
        // Implements: EVS-DEV-view-convergence/Q
        if (outcome == _Outcome.progressed) progressed = true;
        if (outcome == _Outcome.done) _deletionCopyIds.remove(copyId);
      }
      if (_stopping) return;
      // Implements: EVS-DEV-view-convergence/Q
      // the wait before the next pass is never longer than the idle
      //   cadence, so a copy backing off (as long as five minutes) never
      //   delays the discovery pass that notices another copy newly due,
      //   newly created or newly marked for deletion.
      if (!progressed) await _idleWait(_nextWaitDuration());
    }
  }

  /// The nearest `nextAttemptAt` among copies backing off, as a duration
  /// from now, capped at [kCatchUpIdleInterval] so a long backoff on one
  /// copy never delays the driver's next discovery pass.
  Duration _nextWaitDuration() {
    DateTime? earliest;
    for (final progress in _progressByCopyId.values) {
      final next = progress.nextAttemptAt;
      if (next == null) continue;
      if (earliest == null || next.isBefore(earliest)) earliest = next;
    }
    if (earliest == null) return kCatchUpIdleInterval;
    final remaining = earliest.difference(_effectiveClock());
    if (remaining <= Duration.zero) return Duration.zero;
    return remaining < kCatchUpIdleInterval ? remaining : kCatchUpIdleInterval;
  }

  Future<List<ViewCopy>> _discover() async {
    final copies = await _backend.transaction(_backend.readViewCopiesInTxn);
    for (final copy in copies) {
      if (copy.markedForDeletion) _deletionCopyIds.add(copy.copyId);
    }
    return copies;
  }

  bool _dueNow(String copyId) {
    final progress = _progressByCopyId[copyId];
    final next = progress?.nextAttemptAt;
    return next == null || !_effectiveClock().isBefore(next);
  }

  Future<_Outcome> _attemptView(
    _ViewTarget target,
    List<ViewCopy> discovered,
  ) async {
    final existing = discovered
        .where(
          (c) => !c.markedForDeletion && c.fingerprint == target.fingerprint,
        )
        .toList();
    final knownCopyId = existing.isEmpty ? null : existing.single.copyId;
    // Implements: EVS-DEV-view-convergence/Q
    // backoff applies whether or not an unmarked copy of the fingerprint
    //   exists yet: a missing copy's fold failing (a poisoned event, a
    //   transient storage error) leaves no copy behind for the next pass
    //   to key off, so this keys the check by the fingerprint instead.
    final lockKey = knownCopyId ?? target.fingerprint;
    if (!_dueNow(lockKey)) return _Outcome.idle;

    Future<_StepResult> body(Transaction txn) async {
      final existingCopy = await _backend.readUnmarkedViewCopyInTxn(
        txn,
        target.fingerprint,
      );
      final ViewCopy copy;
      if (existingCopy != null) {
        copy = existingCopy;
      } else {
        // Implements: EVS-DEV-view-convergence/T
        final copyId = await _backend.createViewCopyInTxn(
          txn,
          target.viewName,
          target.fingerprint,
          0,
        );
        copy = ViewCopy(
          copyId: copyId,
          viewName: target.viewName,
          fingerprint: target.fingerprint,
          watermark: 0,
          markedForDeletion: false,
        );
      }
      _observeTransactionBegin(copy.copyId);
      final spec = _projections.lookup(target.viewName);
      if (spec == null) return _StepResult(copy.copyId, 0, true);
      final start = _effectiveClock();
      var watermark = copy.watermark;
      var steps = 0;
      var reachedTip = false;
      // The page size the step loop reads events in: an implementation
      // detail of how it walks the log within one transaction, not a
      // spec-bound quantity like kCatchUpStepBound or
      // kCatchUpDeleteBatchLimit.
      const chunkSize = 200;
      var chunk = await _backend.findAllEventsInTxn(
        txn,
        afterSequence: watermark,
        limit: chunkSize,
      );
      var index = 0;
      // Implements: EVS-DEV-view-convergence/O
      // a do-while: the first step always runs, whatever the clock reads.
      do {
        if (index >= chunk.length) {
          if (chunk.length < chunkSize) {
            reachedTip = true;
            break;
          }
          chunk = await _backend.findAllEventsInTxn(
            txn,
            afterSequence: watermark,
            limit: chunkSize,
          );
          index = 0;
          if (chunk.isEmpty) {
            reachedTip = true;
            break;
          }
        }
        final event = chunk[index];
        index++;
        await DeliveryTestHooks.current?.onCatchUpStep?.call(
          copy.copyId,
          event.eventId,
        );
        final def = _entryTypes.byId(event.entryType);
        final registeredVersion =
            def?.registeredVersion ?? event.entryTypeVersion;
        await ProjectionInterpreter.foldStep(
          txn: txn,
          backend: _backend,
          spec: spec,
          promoters: _promoters,
          event: event,
          registeredVersion: registeredVersion,
          copyId: copy.copyId,
        );
        watermark = event.sequenceNumber;
        steps++;
      } while (_effectiveClock().difference(start) < kCatchUpStepBound);
      if (steps > 0) {
        await _backend.setViewCopyWatermarkInTxn(txn, copy.copyId, watermark);
      }
      return _StepResult(copy.copyId, steps, reachedTip);
    }

    try {
      final future = _backend.catchUpTransaction(lockKey, body);
      _currentTransaction = future;
      final result = await future;
      if (result == null) return _Outcome.lockHeld;
      // Recorded only once the transaction that created or found the copy
      // has committed: a rolled-back attempt must not leave this instance
      // pointing at a copy id no reader can find.
      _viewCopyIds[target.viewName] = result.copyId;
      // Implements: EVS-DEV-view-convergence/Q
      // Clears whichever key a prior failure on this attempt was recorded
      // under -- the fingerprint when no copy existed yet, the copy id
      // otherwise -- so a stale backoff entry never lingers once the
      // attempt it was keyed for succeeds: left in place, its
      // `nextAttemptAt` would sit in the past forever, making
      // `_nextWaitDuration` return zero on every future idle wait.
      _onSuccess(lockKey);
      if (result.reachedTip) onCaughtUp?.call(target.viewName);
      return result.steps == 0
          ? _Outcome.idle
          : (result.reachedTip ? _Outcome.done : _Outcome.progressed);
    } catch (e, st) {
      _onFailure(lockKey, e, st);
      return _Outcome.failed;
    } finally {
      _currentTransaction = null;
    }
  }

  Future<_Outcome> _attemptDeletion(String copyId) async {
    if (!_dueNow(copyId)) return _Outcome.idle;

    Future<_StepResult> body(Transaction txn) async {
      _observeTransactionBegin(copyId);
      final deleted = await _backend.deleteViewCopyRowsInTxn(
        txn,
        copyId,
        limit: kCatchUpDeleteBatchLimit,
      );
      var done = false;
      if (deleted < kCatchUpDeleteBatchLimit) {
        await _backend.deleteViewCopyRecordInTxn(txn, copyId);
        done = true;
      }
      return _StepResult(copyId, 1, done);
    }

    try {
      final future = _backend.catchUpTransaction(copyId, body);
      _currentTransaction = future;
      final result = await future;
      if (result == null) return _Outcome.lockHeld;
      _onSuccess(copyId);
      return result.reachedTip ? _Outcome.done : _Outcome.progressed;
    } catch (e, st) {
      _onFailure(copyId, e, st);
      return _Outcome.failed;
    } finally {
      _currentTransaction = null;
    }
  }

  /// Calls the purely-observing `onCatchUpTransactionBegin` seam, reporting
  /// and swallowing whatever it throws rather than letting it fail the
  /// transaction (`DeliveryTestHooks`'s contract for an observing seam).
  static void _observeTransactionBegin(String copyId) {
    final seam = DeliveryTestHooks.current?.onCatchUpTransactionBegin;
    if (seam == null) return;
    try {
      seam(copyId);
    } on Object catch (e, st) {
      libraryLog(
        'view_catch_up',
        'the onCatchUpTransactionBegin test seam threw',
        level: LibraryLogLevel.severe,
        error: e,
        stackTrace: st,
      );
    }
  }

  /// Clears a copy's retry state on a committed transaction, keeping
  /// [ViewCopyProgress.lastFailure] as the last failure a catch-up
  /// transaction on this copy recorded (EVS-DEV-view-convergence/Q), even
  /// once it has since caught up cleanly.
  void _onSuccess(String copyId) {
    final progress = _progressByCopyId[copyId];
    if (progress == null) return;
    progress
      ..consecutiveFailures = 0
      ..nextAttemptAt = null;
  }

  // Implements: EVS-DEV-view-convergence/Q
  void _onFailure(String copyId, Object error, StackTrace stackTrace) {
    libraryLog(
      'view_catch_up',
      'catch-up transaction for view copy "$copyId" failed',
      level: LibraryLogLevel.warning,
      error: error,
      stackTrace: stackTrace,
    );
    final progress = _progressByCopyId.putIfAbsent(
      copyId,
      ViewCopyProgress.new,
    );
    final now = _effectiveClock();
    progress
      ..lastFailure = error
      ..lastFailureAt = now
      ..consecutiveFailures += 1
      ..nextAttemptAt = now.add(_backoffFor(progress.consecutiveFailures));
  }

  static Duration _backoffFor(int consecutiveFailures) {
    var backoff = kCatchUpInitialBackoff;
    for (var i = 1; i < consecutiveFailures; i++) {
      backoff *= 2;
      if (backoff >= kCatchUpMaxBackoff) return kCatchUpMaxBackoff;
    }
    return backoff;
  }

  Future<void> _idleWait(Duration duration) {
    final completer = Completer<void>();
    final timer = libraryTimer(duration, () {
      if (!completer.isCompleted) completer.complete();
    });
    _cancelWait = () {
      timer.cancel();
      if (!completer.isCompleted) completer.complete();
    };
    return completer.future.whenComplete(() => _cancelWait = null);
  }
}

enum _Outcome { progressed, done, idle, lockHeld, failed }

class _StepResult {
  _StepResult(this.copyId, this.steps, this.reachedTip);
  final String copyId;
  final int steps;
  final bool reachedTip;
}
