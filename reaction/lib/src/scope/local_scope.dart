// Implements: EVS-PRD-reaction-scope/A, /C, /E

import 'dart:async';

import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/scope/connection_status.dart';
import 'package:reaction/src/scope/reaction_scope.dart';

/// In-process [ReactionScope] composing the four `Local*` impls.
///
/// Reports [Connected] for the entire lifetime of the scope: in-process
/// composition has no transport to lose. Per `EVS-PRD-reaction-scope`-C,
/// this trivial always-connected report keeps consumer code (in
/// particular `ViewBuilder`/`ReActionErrorListener`) source-identical
/// across Local and Remote without nil-checking the in-process case.
///
/// Ownership: the four `Local*` impls are supplied by the caller. Their
/// own lifecycles (e.g. `LocalAuthSession.dispose`,
/// `LocalPermissionSource.dispose`) are NOT cascaded by
/// [LocalScope.dispose]; the caller composes and disposes them
/// independently. This keeps `LocalScope` a thin composition root with
/// no opinions about shared substrate handles (e.g. one `EventStore`
/// referenced by multiple impls).
class LocalScope implements ReactionScope {
  LocalScope({
    required AuthSession authSession,
    required ActionSubmitter actionSubmitter,
    required ViewSource viewSource,
    required PermissionSource permissionSource,
  }) : _authSession = authSession,
       _actionSubmitter = actionSubmitter,
       _viewSource = viewSource,
       _permissionSource = permissionSource;

  final AuthSession _authSession;
  final ActionSubmitter _actionSubmitter;
  final ViewSource _viewSource;
  final PermissionSource _permissionSource;
  bool _isDisposed = false;

  void _checkDisposed() {
    if (_isDisposed) {
      throw StateError('LocalScope has been disposed.');
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
    return const Connected();
  }

  @override
  Stream<ConnectionStatus> get connectionStatusStream {
    _checkDisposed();
    // Broadcast empty stream — no transitions ever occur.
    return const Stream<ConnectionStatus>.empty().asBroadcastStream();
  }

  @override
  Future<void> dispose() async {
    _isDisposed = true;
  }
}
