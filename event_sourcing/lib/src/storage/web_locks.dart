// The one web-only file under lib/: it is reached only through a conditional
// import (if dart.library.js_interop), so the library's core still loads on
// every runtime (EVS-PRD-portability/A).
//
// Implements: EVS-DEV-version-compatibility/F
// a tab refuses to open a database while another tab of the origin holds a
//   conflicting generation, before any write; otherwise it holds its
//   component locks until the registration is released.
// Implements: EVS-DEV-version-compatibility/G
// the exclusive boot lock of the database serializes the tabs' inspection,
//   registration and boot transaction.
// Implements: EVS-DEV-destination-drain-lock/A
// in the browser the drain lock is a Web Lock named for the IndexedDB
//   database and its identity, which excludes the tabs of an origin; it is
//   requested only while the page is visible, a hidden page hands it over,
//   and a page without a lock manager refuses it as a misconfiguration.
// Implements: EVS-DEV-destination-drain-lock/C
// a request for the Web Lock that is cancelled, or whose page becomes
//   hidden, is withdrawn, and a grant that races the withdrawal is released
//   at once; an acquisition that fails after the grant releases the Web
//   Lock before the failure surfaces.
// Implements: EVS-DEV-version-compatibility/H
// on the web the guard covers every tab of the origin through the browser's
//   lock manager, and a page without one is refused.
import 'dart:async';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';

import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/isolate_drain_lock.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/transaction_rerun_limit.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:web/web.dart' as web;

const String _bootPrefix = 'event_sourcing.boot:';
const String _generationPrefix = 'event_sourcing.generation:';
const String _writesPrefix = 'event_sourcing.writes:';
const String _drainerPrefix = 'event_sourcing.drainer:';

web.LockManager _locks() {
  final hooks = DeliveryTestHooks.current;
  final navigator = web.window.navigator;
  if ((hooks?.webLocksUnavailable ?? false) ||
      !(navigator as JSObject).has('locks')) {
    throw const GenerationGuardConfigurationException(
      'this page has no lock manager (navigator.locks). The browser exposes '
      'Web Locks only in a secure context (HTTPS or localhost); a page '
      'served over plain HTTP, an old browser or an embedded WebView that '
      'lacks them cannot open a shared database, because the library could '
      'not keep a tab of an incompatible build from opening it',
    );
  }
  return navigator.locks;
}

/// Requests [name] in [mode] and returns once it is granted, with a future
/// that completes once the browser has released the lock; the lock is held
/// until [release] completes. A [timeout] aborts a request not yet granted
/// and completes [release], so a grant the lock manager decided just
/// before the abort releases the lock at once instead of holding it for
/// the life of the page; the error thrown then is [timeoutError]'s, or a
/// [GenerationGuardConfigurationException] without one.
Future<({Future<void> released})> _hold(
  web.LockManager locks,
  String name,
  String mode,
  Completer<void> release, {
  Duration? timeout,
  Exception Function(String name, Duration timeout)? timeoutError,
}) async {
  final granted = Completer<void>();
  final controller = web.AbortController();
  JSPromise<JSAny?> onGrant(web.Lock? lock) {
    if (!granted.isCompleted) granted.complete();
    return release.future.toJS;
  }

  final released = locks
      .request(
        name,
        web.LockOptions(mode: mode, signal: controller.signal),
        onGrant.toJS,
      )
      .toDart
      .then<void>(
        (_) {},
        onError: (Object e) {
          if (!granted.isCompleted) granted.completeError(e);
        },
      );
  if (timeout == null) {
    await granted.future;
    return (released: released);
  }
  try {
    await granted.future.timeout(timeout);
  } on TimeoutException {
    if (!release.isCompleted) release.complete();
    controller.abort();
    if (timeoutError != null) throw timeoutError(name, timeout);
    throw GenerationGuardConfigurationException(
      'the lock $name was held for longer than $timeout by another tab '
      '(a boot, or the writes a boot waits for)',
    );
  }
  return (released: released);
}

/// Runs [body] holding the write lock of the database [path]: shared for
/// an ordinary transaction, [exclusive] for a boot transaction. Every tab's
/// transactions on one database take it, so a boot waits for the writes in
/// progress and every later write waits for the boot to commit, the way a
/// Postgres boot's table lock holds appends back; without it, a tab that
/// commits steadily makes another tab's boot re-run on every commit and
/// never finish. The lock manager grants requests in order, so a waiting
/// boot is not overtaken by later writes. An ordinary transaction that
/// keeps losing runs to other tabs' commits runs again holding it
/// exclusively. [timeout] bounds the wait of an exclusive request, and
/// [timeoutError] builds the error thrown when it passes.
// Implements: EVS-DEV-event-store-open/E
// on the web a boot holds every tab's writes to the database back for its
//   duration, so steady writes in another tab cannot starve it.
@internal
Future<T> runHoldingBrowserWriteLock<T>(
  String path, {
  required bool exclusive,
  required Future<T> Function() body,
  Duration? timeout,
  Exception Function(String name, Duration timeout)? timeoutError,
}) async {
  final release = Completer<void>();
  final (released: released) = await _hold(
    _locks(),
    '$_writesPrefix$path',
    exclusive ? 'exclusive' : 'shared',
    release,
    timeout: timeout,
    timeoutError: timeoutError,
  );
  try {
    return await body();
  } finally {
    release.complete();
    await released;
  }
}

/// The modes in which the origin holds the write lock of the database
/// [path] now.
@internal
Future<List<String>> heldBrowserWriteLockModes(String path) async {
  final snapshot = await _locks().query().toDart;
  return <String>[
    for (final info in snapshot.held.toDart)
      if (info.name == '$_writesPrefix$path') info.mode,
  ];
}

/// The guard's locks of the origin for the database [path]: its component
/// locks and its boot lock.
@internal
Future<List<({String name, String mode})>> heldBrowserLocks(String path) async {
  final snapshot = await _locks().query().toDart;
  final prefix = '$_generationPrefix$path:';
  final boot = '$_bootPrefix$path';
  return <({String name, String mode})>[
    for (final info in snapshot.held.toDart)
      if (info.name.startsWith(prefix) || info.name == boot)
        (name: info.name, mode: info.mode),
  ];
}

/// Registers [descriptor] for the IndexedDB database named [path] with the
/// browser's lock manager: takes the database's exclusive boot lock,
/// inspects the component locks every tab holds, refuses a conflict, and
/// otherwise holds one shared lock per component. Returns holding the boot
/// lock.
@internal
Future<GenerationRegistration> registerBrowserGeneration({
  required String path,
  required GenerationDescriptor descriptor,
  required Duration bootLockWait,
}) async {
  final locks = _locks();
  final bootRelease = Completer<void>();
  final (released: bootReleased) = await _hold(
    locks,
    '$_bootPrefix$path',
    'exclusive',
    bootRelease,
    timeout: bootLockWait,
  );
  final componentRelease = Completer<void>();
  final componentsReleased = <Future<void>>[];
  try {
    final prefix = '$_generationPrefix$path:';
    final conflicts = <String>{};
    final mine = descriptor.components.toSet();
    final snapshot = await locks.query().toDart;
    for (final info in snapshot.held.toDart) {
      if (!info.name.startsWith(prefix)) continue;
      final component = info.name.substring(prefix.length);
      if (mine.contains(component)) continue;
      if (component.startsWith('data_format:')) {
        conflicts.add(component);
      } else if (component.startsWith('entry_type:')) {
        final rest = component.substring('entry_type:'.length);
        final cut = rest.lastIndexOf(':');
        final id = rest.substring(0, cut);
        if (descriptor.entryTypes.containsKey(id)) conflicts.add(component);
      }
    }
    if (conflicts.isNotEmpty) {
      throw IncompatibleGenerationException(
        conflictingComponents: conflicts.toList()..sort(),
        descriptor: descriptor,
      );
    }
    final inside = DeliveryTestHooks.current?.insideBootLock;
    if (inside != null) await inside();
    final holds = await Future.wait(<Future<({Future<void> released})>>[
      for (final component in descriptor.components)
        _hold(locks, '$prefix$component', 'shared', componentRelease),
    ]);
    componentsReleased.addAll(holds.map((h) => h.released));
  } catch (_) {
    if (!componentRelease.isCompleted) componentRelease.complete();
    bootRelease.complete();
    await Future.wait(<Future<void>>[bootReleased, ...componentsReleased]);
    rethrow;
  }
  return _BrowserGenerationRegistration(
    bootRelease,
    bootReleased,
    componentRelease,
    componentsReleased,
  );
}

final class _BrowserGenerationRegistration extends GenerationRegistration {
  _BrowserGenerationRegistration(
    this._boot,
    this._bootReleased,
    this._components,
    this._componentsReleased,
  );

  final Completer<void> _boot;
  final Future<void> _bootReleased;
  final Completer<void> _components;
  final List<Future<void>> _componentsReleased;

  @override
  bool get isLost => false;

  @override
  @internal
  Future<void> recordInTxn(Transaction txn) async {}

  /// Releases the boot lock and returns once the browser released it.
  @override
  Future<void> completeBoot() async {
    if (!_boot.isCompleted) _boot.complete();
    await _bootReleased;
  }

  /// Releases every lock and returns once the browser released them.
  @override
  Future<void> release() async {
    if (!_boot.isCompleted) _boot.complete();
    if (!_components.isCompleted) _components.complete();
    await Future.wait(<Future<void>>[_bootReleased, ..._componentsReleased]);
  }
}

// ---------------------------------------------------------------- drain lock

/// The page's lock manager for the drain lock, or a
/// [DrainLockConfigurationException] naming the secure-context requirement
/// when the page has none. [hooks] are the seams of the caller's zone.
web.LockManager _drainLockManager(DeliveryTestHooks? hooks) {
  final navigator = web.window.navigator;
  if ((hooks?.webLocksUnavailable ?? false) ||
      !(navigator as JSObject).has('locks')) {
    throw const DrainLockConfigurationException(
      'this page has no lock manager (navigator.locks). The browser exposes '
      'Web Locks only in a secure context (HTTPS or localhost); on a page '
      'served over plain HTTP, an old browser or an embedded WebView that '
      'lacks them, the library could not keep the tabs of an origin from '
      'draining one database at once, so it grants no drain lock there',
    );
  }
  return navigator.locks;
}

/// The name of the Web Lock that is the drain lock of the database
/// [databaseId] stored under the IndexedDB name [path]. The name includes
/// the path, so a copy of the database under another name does not share
/// the lock.
@internal
String browserDrainLockName(String path, String databaseId) =>
    '$_drainerPrefix$path:$databaseId';

/// A Web Lock held by this page.
@internal
final class BrowserLockHold {
  BrowserLockHold._(this._release, this._released);

  final Completer<void> _release;
  final Future<void> _released;

  /// Releases the lock and returns once the browser released it. Calling it
  /// again does nothing more.
  Future<void> release() async {
    if (!_release.isCompleted) _release.complete();
    await _released;
  }
}

/// Takes the Web Lock [name] exclusively if no one holds it; null when it
/// is held. Throws [DrainLockConfigurationException] on a page without a
/// lock manager.
@internal
Future<BrowserLockHold?> tryBrowserLock(String name) async {
  final locks = _drainLockManager(DeliveryTestHooks.current);
  final release = Completer<void>();
  final outcome = Completer<bool>();
  JSPromise<JSAny?> onGrant(web.Lock? lock) {
    if (lock == null) {
      outcome.complete(false);
      return Future<void>.value().toJS;
    }
    outcome.complete(true);
    return release.future.toJS;
  }

  final released = locks
      .request(
        name,
        web.LockOptions(mode: 'exclusive', ifAvailable: true),
        onGrant.toJS,
      )
      .toDart
      .then<void>(
        (_) {},
        onError: (Object e) {
          if (!outcome.isCompleted) outcome.completeError(e);
        },
      );
  return await outcome.future ? BrowserLockHold._(release, released) : null;
}

/// Requests the Web Lock [name] exclusively and waits for it. Throws
/// [DrainLockConfigurationException] on a page without a lock manager.
@internal
BrowserLockRequest requestBrowserLock(String name) =>
    BrowserLockRequest._(name, DeliveryTestHooks.current);

/// A pending request for a Web Lock.
///
/// The request carries the signal of an abort controller; [cancel] aborts
/// it. A grant whose callback runs after [cancel] (a grant that raced the
/// abort) resolves the callback's promise at once, releasing the lock, and
/// [granted] completes with null. Otherwise the callback's promise stays
/// pending until the hold is released; the browser releases it when the
/// page closes and grants the lock to a waiting request.
@internal
final class BrowserLockRequest {
  BrowserLockRequest._(String name, DeliveryTestHooks? hooks) : _hooks = hooks {
    final locks = _drainLockManager(hooks);
    // The browser calls the callback outside this zone; run it in the
    // zone of the request, whose seams were captured above.
    final zone = Zone.current;
    JSPromise<JSAny?> onGrant(web.Lock? lock) =>
        zone.run<Future<void>>(_onGrant).toJS;
    _settled = locks
        .request(
          name,
          web.LockOptions(mode: 'exclusive', signal: _controller.signal),
          onGrant.toJS,
        )
        .toDart
        .then<void>(
          (_) {
            // A grant that raced the cancellation: its lock is released.
            if (!_granted.isCompleted) _granted.complete(null);
          },
          onError: (Object e, StackTrace st) {
            if (_granted.isCompleted) return;
            if (_cancelled) {
              _granted.complete(null);
            } else {
              _granted.completeError(e, st);
            }
          },
        );
  }

  final DeliveryTestHooks? _hooks;
  final web.AbortController _controller = web.AbortController();
  final Completer<BrowserLockHold?> _granted = Completer<BrowserLockHold?>();
  late final Future<void> _settled;
  bool _cancelled = false;

  Future<void> _onGrant() async {
    await _hooks?.beforeGrantDelivered?.call();
    // Returning resolves the callback's promise, which releases the lock;
    // [granted] completes with null once the browser released it.
    if (_cancelled) return;
    final release = Completer<void>();
    _granted.complete(BrowserLockHold._(release, _settled));
    await release.future;
  }

  /// Completes with the hold once the lock is granted, or with null after
  /// [cancel].
  Future<BrowserLockHold?> get granted => _granted.future;

  /// Withdraws the request and returns once it is settled. A grant that
  /// races the cancellation is released before [granted] completes with
  /// null; a hold [granted] already delivered stays its receiver's to
  /// release.
  Future<void> cancel() async {
    if (_cancelled) return;
    _cancelled = true;
    if (_granted.isCompleted) return;
    _controller.abort();
    await Future.any(<Future<void>>[
      _settled,
      _granted.future.then<void>((_) {}, onError: (Object _) {}),
    ]);
    if (!_granted.isCompleted) _granted.complete(null);
  }
}

/// The held and pending requests of the page's origin for the Web Lock
/// [name], from the lock manager's listing.
@internal
Future<({int held, int pending})> browserLockCounts(String name) async {
  final snapshot = await _drainLockManager(
    DeliveryTestHooks.current,
  ).query().toDart;
  int count(JSArray<web.LockInfo> infos) =>
      infos.toDart.where((i) => i.name == name).length;
  return (held: count(snapshot.held), pending: count(snapshot.pending));
}

/// The page's visibility, as the drain lock follows it.
abstract interface class _Visibility {
  /// Whether the page is visible: not hidden, not frozen and not being
  /// unloaded.
  bool get visible;

  /// An event after each change.
  Stream<void> get changes;
}

/// The document's visibility: `visibilitychange`, and `pagehide`/`freeze`
/// (with `pageshow`/`resume` undoing them).
final class _DocumentVisibility implements _Visibility {
  _DocumentVisibility() {
    web.document.addEventListener(
      'visibilitychange',
      ((web.Event _) => _changes.add(null)).toJS,
    );
    web.window.addEventListener(
      'pagehide',
      ((web.Event _) {
        _pageHidden = true;
        _changes.add(null);
      }).toJS,
    );
    web.window.addEventListener(
      'pageshow',
      ((web.Event _) {
        _pageHidden = false;
        _changes.add(null);
      }).toJS,
    );
    web.document.addEventListener(
      'freeze',
      ((web.Event _) {
        _frozen = true;
        _changes.add(null);
      }).toJS,
    );
    web.document.addEventListener(
      'resume',
      ((web.Event _) {
        _frozen = false;
        _changes.add(null);
      }).toJS,
    );
  }

  bool _pageHidden = false;
  bool _frozen = false;
  final StreamController<void> _changes = StreamController<void>.broadcast();

  @override
  bool get visible =>
      !_pageHidden && !_frozen && web.document.visibilityState == 'visible';

  @override
  Stream<void> get changes => _changes.stream;
}

/// The document's visibility, narrowed by the `pageVisibility` seam: the
/// seam can make a visible page count as hidden, never a hidden one as
/// visible.
final class _SeamVisibility implements _Visibility {
  _SeamVisibility(this._seam, this._document);

  final TestPageVisibility _seam;
  final _Visibility _document;

  @override
  bool get visible => _seam.visible && _document.visible;

  @override
  Stream<void> get changes => Stream<void>.multi((controller) {
    final seam = _seam.changes.listen(controller.add);
    final document = _document.changes.listen(controller.add);
    controller.onCancel = () async {
      await seam.cancel();
      await document.cancel();
    };
  }, isBroadcast: true);
}

final _DocumentVisibility _documentVisibility = _DocumentVisibility();

_Visibility _visibilityFor(DeliveryTestHooks? hooks) {
  final seam = hooks?.pageVisibility;
  return seam != null
      ? _SeamVisibility(seam, _documentVisibility)
      : _documentVisibility;
}

/// The drain lock's Web Lock, held while the page is visible: asks for a
/// hand-over as soon as the page is hidden.
final class _BrowserDrainExclusion implements DrainExclusionHold {
  _BrowserDrainExclusion(this._hold, _Visibility visibility) {
    void check() {
      if (!visibility.visible && !_handOver.isCompleted) _handOver.complete();
    }

    _subscription = visibility.changes.listen((_) => check());
    check();
  }

  final BrowserLockHold _hold;
  final Completer<void> _handOver = Completer<void>();
  late final StreamSubscription<void> _subscription;

  @override
  Future<void> get handOverRequested => _handOver.future;

  @override
  Future<void> release() async {
    await _subscription.cancel();
    await _hold.release();
  }
}

/// Takes the drain lock's Web Lock of the database [databaseId] stored
/// under [path] if it is free and the page is visible. Throws
/// [DrainLockConfigurationException] on a page without a lock manager, and
/// [DrainLockUnavailableException] when another tab holds the lock or the
/// page is hidden (the drain lock follows the visible tab).
@internal
Future<DrainExclusionHold?> tryBrowserDrainExclusion({
  required String path,
  required String databaseId,
}) async {
  final hooks = DeliveryTestHooks.current;
  _drainLockManager(hooks);
  final visibility = _visibilityFor(hooks);
  if (!visibility.visible) {
    throw const DrainLockUnavailableException(
      'the page is hidden: the drain lock follows the visible tab',
    );
  }
  final hold = await tryBrowserLock(browserDrainLockName(path, databaseId));
  if (hold == null) {
    throw const DrainLockUnavailableException(
      'another tab of the origin holds the drain lock of this database',
    );
  }
  return _BrowserDrainExclusion(hold, visibility);
}

/// A request for the drain lock of the database [databaseId] stored under
/// [path] that waits for the Web Lock while the page is visible.
/// [acquireHolding] completes the acquisition once the Web Lock is held
/// (the in-isolate registration and the epoch raise). [ended] completes
/// with the error that ends the request, whatever it is waiting for, when
/// the backend can no longer take the lock (it was closed, or its database
/// handle cannot commit).
@internal
DrainLockRequest? requestBrowserDrainLock({
  required String path,
  required String databaseId,
  required Future<DrainLock> Function(DrainExclusionHold exclusion)
  acquireHolding,
  required Duration retryInterval,
  required Future<void> Function() wake,
  required Future<Exception> ended,
}) => _BrowserDrainLockRequest(
  name: browserDrainLockName(path, databaseId),
  hooks: DeliveryTestHooks.current,
  acquireHolding: acquireHolding,
  retryInterval: retryInterval,
  wake: wake,
  ended: ended,
);

/// While the page is visible, a pending Web Lock request; while it is
/// hidden, none. A grant is completed through `acquireHolding`; a grant
/// that arrives after a cancellation or while the page is hidden is
/// released at once. After a failed completion (the Web Lock released
/// first) the request waits `retryInterval`, or for `wake` when the lock
/// was held elsewhere in this isolate, and requests again. A
/// [DrainLockConfigurationException] (no lock manager), a
/// [DrainLockBackendClosedException], a [TransactionRerunLimitException]
/// (a handle that cannot commit) or an [Error] ends it with that error, and
/// so does the backend's end signal, at once, whatever the request waits
/// for.
final class _BrowserDrainLockRequest implements DrainLockRequest {
  _BrowserDrainLockRequest({
    required String name,
    required DeliveryTestHooks? hooks,
    required Future<DrainLock> Function(DrainExclusionHold exclusion)
    acquireHolding,
    required Duration retryInterval,
    required Future<void> Function() wake,
    required Future<Exception> ended,
  }) : _name = name,
       _visibility = _visibilityFor(hooks),
       _acquireHolding = acquireHolding,
       _retryInterval = retryInterval,
       _wake = wake {
    _subscription = _visibility.changes.listen((_) => _poke());
    unawaited(
      ended.then((error) {
        _ended ??= error;
        if (!_endSignal.isCompleted) _endSignal.complete();
        _poke();
      }),
    );
    _loop = _run();
  }

  /// The error that ends the request, once the backend signalled it.
  Exception? _ended;
  final Completer<void> _endSignal = Completer<void>();

  /// Throws the backend's end signal, if it arrived.
  void _checkEnded() {
    final ended = _ended;
    if (ended != null) throw ended;
  }

  static bool _terminal(Object e) =>
      e is DrainLockConfigurationException ||
      e is DrainLockBackendClosedException ||
      e is TransactionRerunLimitException ||
      e is Error;

  final String _name;
  final _Visibility _visibility;
  final Future<DrainLock> Function(DrainExclusionHold exclusion)
  _acquireHolding;
  final Duration _retryInterval;
  final Future<void> Function() _wake;
  late final StreamSubscription<void> _subscription;
  late final Future<void> _loop;
  final Completer<DrainLock?> _granted = Completer<DrainLock?>();
  bool _cancelled = false;
  final Completer<void> _cancelSignal = Completer<void>();
  Completer<void> _poked = Completer<void>();
  String? _lastError;

  /// Wakes the loop: the page's visibility changed, or the request was
  /// cancelled.
  void _poke() {
    final poked = _poked;
    _poked = Completer<void>();
    if (!poked.isCompleted) poked.complete();
  }

  @override
  Future<DrainLock?> get granted => _granted.future;

  Future<void> _run() async {
    try {
      while (!_cancelled) {
        _checkEnded();
        if (!_visibility.visible) {
          await _poked.future;
          continue;
        }
        final BrowserLockRequest pending;
        try {
          pending = requestBrowserLock(_name);
        } on Object catch (e, st) {
          if (_terminal(e)) rethrow;
          _logOnce(e, st);
          await _pause(wakeEarly: false);
          continue;
        }
        BrowserLockHold? hold;
        // A change of visibility before the loop below waits is seen here.
        while (!_cancelled && _ended == null && _visibility.visible) {
          final poked = _poked.future;
          await Future.any(<Future<void>>[
            pending.granted.then<void>((_) {}, onError: (Object _) {}),
            poked,
          ]);
          if (_cancelled || _ended != null || !_visibility.visible) break;
          if (_isComplete(pending)) break;
        }
        if (_cancelled || _ended != null || !_visibility.visible) {
          await pending.cancel();
          await (await _heldOrNull(pending))?.release();
          continue;
        }
        try {
          hold = await pending.granted;
        } on Object catch (e, st) {
          if (e is Error) rethrow;
          _logOnce(e, st);
          await _pause(wakeEarly: false);
          continue;
        }
        if (hold == null) continue;
        final exclusion = _BrowserDrainExclusion(hold, _visibility);
        DrainLock lock;
        try {
          lock = await _acquireHolding(exclusion);
        } on DrainLockUnavailableException {
          await exclusion.release();
          _lastError = null;
          await _pause(wakeEarly: true);
          continue;
        } on Object catch (e, st) {
          await exclusion.release();
          if (_terminal(e)) rethrow;
          _logOnce(e, st);
          await _pause(wakeEarly: false);
          continue;
        }
        if (_cancelled || _ended != null || !_visibility.visible) {
          await lock.release();
          continue;
        }
        _granted.complete(lock);
        return;
      }
      if (!_granted.isCompleted) _granted.complete(null);
    } on Object catch (e, st) {
      if (!_granted.isCompleted) _granted.completeError(e, st);
    } finally {
      await _subscription.cancel();
    }
  }

  static bool _isComplete(BrowserLockRequest request) =>
      request._granted.isCompleted;

  static Future<BrowserLockHold?> _heldOrNull(BrowserLockRequest request) =>
      request.granted.then<BrowserLockHold?>(
        (hold) => hold,
        onError: (Object _) => null,
      );

  void _logOnce(Object e, StackTrace st) {
    final text = e.toString();
    if (text == _lastError) return;
    _lastError = text;
    libraryLog(
      'drain_lock',
      'acquiring the drain lock failed; retried every $_retryInterval',
      level: LibraryLogLevel.warning,
      error: e,
      stackTrace: st,
    );
  }

  /// Waits [_retryInterval], or until [_wake] completes when [wakeEarly];
  /// a cancellation ends the wait.
  Future<void> _pause({required bool wakeEarly}) {
    if (_cancelled) return Future<void>.value();
    final waiting = Completer<void>();
    void done() {
      if (!waiting.isCompleted) waiting.complete();
    }

    final timer = libraryTimer(_retryInterval, done);
    unawaited(_cancelSignal.future.then((_) => done()));
    unawaited(_endSignal.future.then((_) => done()));
    if (wakeEarly) {
      unawaited(_wake().then((_) => done(), onError: (Object _) => done()));
    }
    return waiting.future.whenComplete(timer.cancel);
  }

  @override
  Future<void> cancel() async {
    _cancelled = true;
    if (!_cancelSignal.isCompleted) _cancelSignal.complete();
    _poke();
    await _loop;
    if (!_granted.isCompleted) _granted.complete(null);
  }
}
