// Implements: EVS-PRD-reaction-widget-contract/H

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

/// Test-only [ReactionScope] implementation for widget tests.
///
/// Per `EVS-PRD-reaction-widget-contract`-H, this is a SHIPPED public
/// deliverable — every downstream consumer's widget tests use it
/// instead of writing their own fakes. Drives all observable
/// transitions deterministically (no timing in tests):
///
/// - [driveAuthStatus] — emits an [AuthStatus] on `authSession.stream`.
/// - [driveConnectionStatus] — emits a [ConnectionStatus] on
///   [connectionStatusStream].
/// - [queueDispatchResult] — queues the next [DispatchResult] for
///   `actionSubmitter.submit`.
/// - [emitViewUpdate] — pushes an [Update] to active `viewSource.watch`
///   subscribers of a given view.
/// - [drivePermission] — sets the current [EffectiveAuthorization] and
///   emits on `permissionSource.stream`.
class FakeReaction implements ReactionScope {
  FakeReaction({
    AuthStatus initialAuthStatus = const NotAuthenticated(),
    ConnectionStatus initialConnectionStatus = const Connected(),
    EffectiveAuthorization? initialPermission,
  }) : _authStatus = initialAuthStatus,
       _connectionStatus = initialConnectionStatus,
       _effectiveAuth = initialPermission {
    _authSession = _FakeAuthSession(this);
    _actionSubmitter = _FakeActionSubmitter(this);
    _viewSource = _FakeViewSource(this);
    _permissionSource = _FakePermissionSource(this);
  }

  AuthStatus _authStatus;
  ConnectionStatus _connectionStatus;
  EffectiveAuthorization? _effectiveAuth;

  final StreamController<AuthStatus> _authController =
      StreamController<AuthStatus>.broadcast();
  final StreamController<ConnectionStatus> _statusController =
      StreamController<ConnectionStatus>.broadcast();
  final StreamController<EffectiveAuthorization?> _permController =
      StreamController<EffectiveAuthorization?>.broadcast();
  final Map<String, StreamController<Update<dynamic>>> _viewControllers = {};
  final List<Future<DispatchResult<Object?>>> _queuedResults = [];
  final List<ActionSubmission> _submittedActions = [];

  late final _FakeAuthSession _authSession;
  late final _FakeActionSubmitter _actionSubmitter;
  late final _FakeViewSource _viewSource;
  late final _FakePermissionSource _permissionSource;

  bool _disposed = false;

  void _checkDisposed() {
    if (_disposed) {
      throw StateError('FakeReaction has been disposed.');
    }
  }

  @override
  AuthSession get authSession {
    _checkDisposed();
    return _authSession;
  }

  @override
  ActionSubmitter get actionSubmitter {
    _checkDisposed();
    return _actionSubmitter;
  }

  @override
  ViewSource get viewSource {
    _checkDisposed();
    return _viewSource;
  }

  @override
  PermissionSource get permissionSource {
    _checkDisposed();
    return _permissionSource;
  }

  @override
  ConnectionStatus get connectionStatus {
    _checkDisposed();
    return _connectionStatus;
  }

  @override
  Stream<ConnectionStatus> get connectionStatusStream {
    _checkDisposed();
    return _statusController.stream;
  }

  // ----- Driver API -----

  /// Set [AuthStatus] and emit to `authSession.stream`.
  void driveAuthStatus(AuthStatus s) {
    _checkDisposed();
    _authStatus = s;
    if (!_authController.isClosed) _authController.add(s);
  }

  /// Set [ConnectionStatus] and emit to [connectionStatusStream].
  void driveConnectionStatus(ConnectionStatus s) {
    _checkDisposed();
    _connectionStatus = s;
    if (!_statusController.isClosed) _statusController.add(s);
  }

  /// Queue a [DispatchResult] for the next [ActionSubmitter.submit].
  void queueDispatchResult(DispatchResult<Object?> r) {
    _checkDisposed();
    _queuedResults.add(Future.value(r));
  }

  /// Queue a pending [Future] for the next [ActionSubmitter.submit] call.
  ///
  /// Lets tests exercise paths that depend on a submission being IN-FLIGHT
  /// (e.g., dispose-during-Submitting): pass a [Completer.future] and
  /// complete it after the test has driven the desired widget-tree state.
  void queueDispatchResultFuture(Future<DispatchResult<Object?>> f) {
    _checkDisposed();
    _queuedResults.add(f);
  }

  /// All actions submitted via this fake (for test assertions).
  List<ActionSubmission> get submittedActions {
    _checkDisposed();
    return List.unmodifiable(_submittedActions);
  }

  /// Emit an [Update] to any active subscriber of [viewName].
  ///
  /// If no subscriber has called `viewSource.watch(viewName: ...)` yet,
  /// the update is dropped — `watch` uses a broadcast controller and
  /// has no replay buffer. Drive updates AFTER the test widget has
  /// subscribed.
  void emitViewUpdate<T>(String viewName, Update<T> update) {
    _checkDisposed();
    final c = _viewControllers[viewName];
    if (c != null && !c.isClosed) c.add(update);
  }

  /// Set the current [EffectiveAuthorization] and emit on
  /// `permissionSource.stream`.
  void drivePermission(EffectiveAuthorization? auth) {
    _checkDisposed();
    _effectiveAuth = auth;
    if (!_permController.isClosed) _permController.add(auth);
  }

  @override
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    await _authController.close();
    await _statusController.close();
    await _permController.close();
    for (final c in _viewControllers.values) {
      await c.close();
    }
  }
}

class _FakeAuthSession implements AuthSession {
  _FakeAuthSession(this._fake);
  final FakeReaction _fake;

  @override
  AuthStatus get current => _fake._authStatus;

  @override
  Stream<AuthStatus> get stream => _fake._authController.stream;

  @override
  void setCredential(String? credential) {
    // Test helper no-op: tests use [FakeReaction.driveAuthStatus] to
    // drive transitions deterministically. Production AuthSession
    // impls (Local/Remote) interpret the credential string differently;
    // the fake does not try to mimic either.
  }

  @override
  Principal? get principal {
    final c = _fake._authStatus;
    return c is Authenticated ? c.principal : null;
  }

  @override
  Future<void> dispose() async {
    // No-op; [FakeReaction] owns the controllers.
  }
}

class _FakeActionSubmitter implements ActionSubmitter {
  _FakeActionSubmitter(this._fake);
  final FakeReaction _fake;

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) {
    _fake._submittedActions.add(submission);
    if (_fake._queuedResults.isEmpty) {
      throw StateError(
        'FakeReaction: no DispatchResult queued for submit() call. '
        'Call queueDispatchResult() or queueDispatchResultFuture() '
        'before submitting.',
      );
    }
    return _fake._queuedResults.removeAt(0);
  }
}

class _FakeViewSource implements ViewSource {
  _FakeViewSource(this._fake);
  final FakeReaction _fake;

  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    final controller = _fake._viewControllers.putIfAbsent(
      viewName,
      () => StreamController<Update<dynamic>>.broadcast(),
    );
    return controller.stream.cast<Update<T>>();
  }
}

class _FakePermissionSource implements PermissionSource {
  _FakePermissionSource(this._fake);
  final FakeReaction _fake;

  @override
  EffectiveAuthorization? get current => _fake._effectiveAuth;

  @override
  Stream<EffectiveAuthorization?> get stream => _fake._permController.stream;

  @override
  Future<void> dispose() async {
    // No-op; [FakeReaction] owns the controllers.
  }
}

/// Pump a widget under test with [FakeReaction] threaded via
/// [ReActionScope] and wrapped in a [MaterialApp] for `Directionality`
/// / `Localizations` / navigator support.
Future<void> pumpReactionWidget(
  WidgetTester tester, {
  required FakeReaction fake,
  required Widget child,
}) {
  return tester.pumpWidget(
    ReActionScope(
      scope: fake,
      child: MaterialApp(home: child),
    ),
  );
}
