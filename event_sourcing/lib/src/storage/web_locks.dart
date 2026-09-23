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
// a browser database grants no drain lock: the isolate registry would not
//   exclude the drainers of several tabs of one origin, so a delivery cycle
//   in the browser is refused as a misconfiguration.
// Implements: EVS-DEV-version-compatibility/H
// on the web the guard covers every tab of the origin through the browser's
//   lock manager, and a page without one is refused.
import 'dart:async';
import 'dart:js_interop';
import 'dart:js_interop_unsafe';

import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:web/web.dart' as web;

const String _bootPrefix = 'event_sourcing.boot:';
const String _generationPrefix = 'event_sourcing.generation:';
const String _writesPrefix = 'event_sourcing.writes:';

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
/// the life of the page.
Future<({Future<void> released})> _hold(
  web.LockManager locks,
  String name,
  String mode,
  Completer<void> release, {
  Duration? timeout,
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
/// boot is not overtaken by later writes. [timeout] bounds the wait of an
/// exclusive request.
// Implements: EVS-DEV-event-store-open/E
// on the web a boot holds every tab's writes to the database back for its
//   duration, so steady writes in another tab cannot starve it.
@internal
Future<T> runHoldingBrowserWriteLock<T>(
  String path, {
  required bool exclusive,
  required Future<T> Function() body,
  Duration? timeout,
}) async {
  final release = Completer<void>();
  final (released: released) = await _hold(
    _locks(),
    '$_writesPrefix$path',
    exclusive ? 'exclusive' : 'shared',
    release,
    timeout: timeout,
  );
  try {
    return await body();
  } finally {
    release.complete();
    await released;
  }
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

/// A Sembast database in the browser grants no drain lock: several tabs of
/// one origin share the database, each in its own isolate, and the isolate
/// registry excludes none of them from another. Throws
/// [DrainLockConfigurationException], so `SyncCycle.start` fails loudly
/// rather than letting the tabs drain the database at once.
@internal
void refuseBrowserDrainLock() {
  throw const DrainLockConfigurationException(
    'a Sembast database in the browser grants no drain lock: the tabs of an '
    'origin share the database, and the library does not exclude their '
    'delivery cycles from one another, so a delivery cycle cannot start in '
    'the browser',
  );
}
