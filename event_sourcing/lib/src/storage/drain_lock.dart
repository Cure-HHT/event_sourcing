// Implements: EVS-DEV-destination-drain-lock/A+B+C
// the drain lock a storage backend grants: one holder per database within
//   the backend's scope, an epoch that every acquisition raises, a check
//   that a queue-changing transaction runs against that epoch, and a
//   request that stands by until the lock is free, gives up whatever it
//   obtained when a later acquisition step fails, and leaves no lock held
//   when it is cancelled.
import 'dart:async';

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// Why a drain lock is no longer held.
enum DrainLockLossReason {
  /// The holder released it.
  released,

  /// The backend detected that the lock is gone (on Postgres, the lock
  /// session that held it was declared lost).
  lossDetected,

  /// The epoch the database stores is not the holder's: another drainer
  /// acquired the lock since.
  epochChanged,
}

/// The drain lock is held elsewhere: by another process, tab or delivery
/// cycle, or by a live lock granted through the same backend.
class DrainLockUnavailableException implements Exception {
  const DrainLockUnavailableException(this.message);

  final String message;

  @override
  String toString() => 'DrainLockUnavailableException: $message';
}

/// A drain lock's holder is no longer the drainer of its database. A
/// transaction that throws it commits nothing.
///
/// It is not a storage-engine error, so no storage retry loop re-runs a
/// transaction that throws it.
class DrainLockLostException implements Exception {
  const DrainLockLostException(this.reason, this.message);

  /// Why the lock is no longer held.
  final DrainLockLossReason reason;

  final String message;

  @override
  String toString() => 'DrainLockLostException(${reason.name}): $message';
}

/// The drain lock cannot be granted with the backend's configuration: on
/// Postgres, the lock session holds the drain key through something other
/// than the library, the database identity does not match, or the pool does
/// not see the lock the lock session took; in the browser, the page has no
/// lock manager (Web Locks exist only in a secure context). A deterministic
/// misconfiguration, not contention.
class DrainLockConfigurationException implements Exception {
  const DrainLockConfigurationException(this.message);

  final String message;

  @override
  String toString() => 'DrainLockConfigurationException: $message';
}

/// The storage backend was closed: it grants no drain lock. Terminal for a
/// delivery cycle over it, which stops.
class DrainLockBackendClosedException implements Exception {
  const DrainLockBackendClosedException();

  @override
  String toString() =>
      'DrainLockBackendClosedException: the storage backend was closed; it '
      'grants no drain lock';
}

/// The right to drain one database's destination queues, granted by its
/// storage backend.
///
/// Every acquisition raises the database's drain epoch in a transaction, and
/// the lock records the value it set. A holder calls [assertHeldInTxn] as
/// the first step of every transaction that changes a queue, so a holder
/// that another drainer has replaced commits nothing.
abstract class DrainLock {
  /// The drain epoch this acquisition stored.
  int get epoch;

  /// True once [release] started.
  bool get isReleased;

  /// Throws [DrainLockLostException] unless the lock is held and current
  /// inside [txn]: first when [isReleased] is set or a loss was detected,
  /// then when the epoch the database stores differs from [epoch]. The read
  /// holds the stored epoch against a concurrent change until [txn] ends.
  Future<void> assertHeldInTxn(Transaction txn);

  /// [assertHeldInTxn] in a transaction of its own.
  Future<void> assertHeld();

  /// Checks that whatever holds the lock is alive (on Postgres, a probe of
  /// the lock session). A failure is reported as a detected loss through
  /// [lost], never after [release] started.
  Future<void> heartbeat();

  /// Gives the lock up. Sets [isReleased] before anything else, so an error
  /// that the release causes is never reported as a loss, and [lost] never
  /// completes after it.
  Future<void> release();

  /// Completes when the backend detects that the lock is gone, unless the
  /// holder released it first.
  Future<void> get lost;

  /// Completes when the runtime asks the holder to hand the lock over: in
  /// the browser, when the page becomes hidden (the drain lock follows the
  /// visible tab). The lock is still held and current; the holder starts no
  /// further send, lets the outcomes of its sends in flight commit, and
  /// releases it. Never completes on a backend without such a signal.
  Future<void> get handOverRequested;
}

/// A pending acquisition of the drain lock.
abstract class DrainLockRequest {
  /// Completes with the lock once it is granted, or with null after
  /// [cancel].
  Future<DrainLock?> get granted;

  /// Stops the request. Awaits any acquisition step in flight; a grant that
  /// races the cancellation is released before [granted] completes with
  /// null, so a cancelled request never leaves the lock held.
  Future<void> cancel();
}

/// Creates a one-shot timer through the `timerFactory` test seam when one
/// is installed, and through [Timer] otherwise.
@internal
Timer libraryTimer(Duration duration, void Function() callback) {
  final factory = DeliveryTestHooks.current?.timerFactory;
  if (factory == null) return Timer(duration, callback);
  var fired = false;
  return factory(duration, (timer) {
    timer.cancel();
    if (fired) return;
    fired = true;
    callback();
  });
}

/// Creates a periodic timer through the `timerFactory` test seam when one is
/// installed, and through [Timer.periodic] otherwise.
@internal
Timer libraryPeriodicTimer(
  Duration period,
  void Function(Timer timer) callback,
) {
  final factory = DeliveryTestHooks.current?.timerFactory;
  return factory != null
      ? factory(period, callback)
      : Timer.periodic(period, callback);
}

/// A [DrainLockRequest] that repeats `attempt` until it grants the lock or
/// the request is cancelled.
///
/// After an attempt that finds the lock held elsewhere, the request waits
/// for `retryInterval`, or until `wake` (when given) completes, whichever is
/// first; after any other exception it logs the failure once per distinct
/// message and waits for `retryInterval`. A
/// [DrainLockBackendClosedException] or an [Error] (a defect, which a retry
/// would not cure) ends the request with that error. `ready`, when given, is
/// awaited before each attempt (a cancellation ends the wait); an error it
/// completes with ends the request with that error.
@internal
final class RetryingDrainLockRequest implements DrainLockRequest {
  RetryingDrainLockRequest({
    required Future<DrainLock> Function() attempt,
    required Duration retryInterval,
    Future<void> Function()? wake,
    Future<void> Function()? ready,
  }) : _attempt = attempt,
       _retryInterval = retryInterval,
       _wake = wake,
       _ready = ready {
    _loop = _run();
  }

  final Future<DrainLock> Function() _attempt;
  final Duration _retryInterval;
  final Future<void> Function()? _wake;
  final Future<void> Function()? _ready;
  final Completer<DrainLock?> _granted = Completer<DrainLock?>();
  late final Future<void> _loop;
  bool _cancelled = false;
  final Completer<void> _cancelSignal = Completer<void>();
  Completer<void>? _waiting;
  Timer? _timer;
  String? _lastError;

  @override
  Future<DrainLock?> get granted => _granted.future;

  Future<void> _run() async {
    try {
      while (!_cancelled) {
        final ready = _ready;
        if (ready != null) {
          await Future.any(<Future<void>>[ready(), _cancelSignal.future]);
        }
        if (_cancelled) break;
        DrainLock lock;
        try {
          lock = await _attempt();
        } on DrainLockUnavailableException {
          _lastError = null;
          await _pause(wakeEarly: true);
          continue;
        } on Object catch (e, st) {
          if (e is DrainLockBackendClosedException || e is Error) rethrow;
          final text = e.toString();
          if (text != _lastError) {
            _lastError = text;
            libraryLog(
              'drain_lock',
              'acquiring the drain lock failed; retried every '
                  '$_retryInterval',
              level: LibraryLogLevel.warning,
              error: e,
              stackTrace: st,
            );
          }
          await _pause(wakeEarly: false);
          continue;
        }
        await DeliveryTestHooks.current?.beforeGrantDelivered?.call();
        if (_cancelled) {
          await lock.release();
          break;
        }
        _granted.complete(lock);
        return;
      }
      if (!_granted.isCompleted) _granted.complete(null);
    } on Object catch (e, st) {
      if (!_granted.isCompleted) _granted.completeError(e, st);
    }
  }

  Future<void> _pause({required bool wakeEarly}) {
    final waiting = Completer<void>();
    _waiting = waiting;
    void done() {
      if (!waiting.isCompleted) waiting.complete();
    }

    _timer = libraryTimer(_retryInterval, done);
    final wake = _wake;
    if (wakeEarly && wake != null) {
      unawaited(wake().then((_) => done(), onError: (Object _) => done()));
    }
    return waiting.future.whenComplete(() {
      _timer?.cancel();
      _timer = null;
      _waiting = null;
    });
  }

  @override
  Future<void> cancel() async {
    _cancelled = true;
    if (!_cancelSignal.isCompleted) _cancelSignal.complete();
    _timer?.cancel();
    final waiting = _waiting;
    if (waiting != null && !waiting.isCompleted) waiting.complete();
    await _loop;
    if (!_granted.isCompleted) _granted.complete(null);
  }
}
