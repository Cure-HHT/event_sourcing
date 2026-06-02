// Implements: EVS-PRD-cross-process-event-transport — composition of the
//   four interface PRDs into one client-side bundle: RemoteScope assembles
//   RemoteAuthSession, RemoteActionSubmitter, RemoteViewSource, and
//   RemotePermissionSource over a shared RemoteConnection, and routes
//   auth-related WS close-frame codes (4001 / 4003) to the auth session.
// Implements: EVS-PRD-auth-session/G — the AuthSession's active Principal
//   is exposed via authSession to actionSubmitter, viewSource, and
//   permissionSource consumers; the composition guarantees a single
//   source of truth.
// Implements: EVS-PRD-action-submitter/E — composition mirrors the Local
//   side so source-identical consumer code works against either.
// Implements: EVS-PRD-view-subscriber/D — same.
// Implements: EVS-PRD-permission-source/D — Principal is sourced from the
//   co-mounted AuthSession (no direct setPrincipal on PermissionSource).
// Implements: EVS-PRD-reaction-scope/A — RemoteScope satisfies the
//   ReactionScope interface (four interface getters + connectionStatus
//   + connectionStatusStream + dispose).
// Implements: EVS-PRD-reaction-scope/D — drives ConnectionStatus
//   transitions from the underlying RemoteConnection's WS lifecycle by
//   wiring its onConnectionStatusChanged callback into a broadcast
//   StreamController.
// Implements: EVS-PRD-reaction-scope/E — post-dispose, the four
//   interface getters and the connection-status getters throw
//   StateError so consumer code is source-identical with LocalScope.

import 'dart:async';

import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/remote/remote_action_submitter.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';
import 'package:reaction/src/remote/remote_view_source.dart';
import 'package:reaction/src/scope/connection_status.dart';
import 'package:reaction/src/scope/reaction_scope.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class RemoteScope implements ReactionScope {
  RemoteScope({
    required Uri baseUrl,
    http.Client? httpClient,
    WebSocketChannel Function(Uri)? wsFactory,
    ExponentialBackoff? reconnectBackoff,
  }) : _connection = RemoteConnection(
         baseUrl: baseUrl,
         httpClient: httpClient ?? http.Client(),
         // Platform-neutral default: WebSocketChannel.connect uses
         // conditional imports to pick the io/html channel per platform,
         // so importing `package:reaction/reaction.dart` (which barrel-
         // exports RemoteScope) does not drag `dart:io` into a Flutter
         // web build. The reaction protocol authenticates via the first
         // WS message (not upgrade headers) precisely so web — which
         // cannot set WS headers — is supported.
         wsFactory: wsFactory ?? WebSocketChannel.connect,
         reconnectBackoff: reconnectBackoff ?? const ExponentialBackoff(),
       ) {
    // Wire 4001/4003 close-frame routing AFTER _connection is built
    // and BEFORE the first WS open: the lazy connect path is driven
    // by openSubscription(), and _auth is assigned on the next line,
    // so the callback will resolve a non-null _auth whenever the
    // server force-closes the WS with an auth-related code.
    _auth = RemoteAuthSession(connection: _connection);
    _connection.onAuthClose = () => _auth.handleAuthRejected();
    _submitter = RemoteActionSubmitter(
      connection: _connection,
      authSession: _auth,
    );
    _views = RemoteViewSource(connection: _connection);
    _perms = RemotePermissionSource(
      connection: _connection,
      authSession: _auth,
    );
    // Route the server-pushed stale_data envelope into a permission
    // re-fetch. The server emits this on security-EXPANDING changes
    // (role_assigned, permission_granted, containment update); the
    // re-fetch updates `_perms.current` so UI gating reacts live —
    // without it, the client would only learn about its widened
    // permissions on the next Authenticated transition.
    _connection.onStaleData = (_) => unawaited(_perms.refresh());
    // Surface every transport-status transition from the underlying
    // RemoteConnection onto the public broadcast stream. The callback
    // is already de-duped at the source (RemoteConnection._emitStatus
    // skips no-op transitions), so consumers see exactly the meaningful
    // changes.
    _connection.onConnectionStatusChanged = _statusController.add;
  }

  final RemoteConnection _connection;
  late final RemoteAuthSession _auth;
  late final RemoteActionSubmitter _submitter;
  late final RemoteViewSource _views;
  late final RemotePermissionSource _perms;

  /// Broadcast so multiple widget consumers can listen concurrently
  /// without re-subscribing the underlying RemoteConnection callback.
  /// Per `EVS-PRD-reaction-scope`-A and the `ReactionScope` interface
  /// doc, this stream emits ONLY subsequent transitions; consumers
  /// that need "current plus subsequent" seed from [connectionStatus]
  /// and listen to this stream.
  final StreamController<ConnectionStatus> _statusController =
      StreamController<ConnectionStatus>.broadcast();
  bool _isDisposed = false;

  void _checkDisposed() {
    if (_isDisposed) {
      throw StateError('RemoteScope has been disposed.');
    }
  }

  @override
  AuthSession get authSession {
    _checkDisposed();
    return _auth;
  }

  @override
  ActionSubmitter get actionSubmitter {
    _checkDisposed();
    return _submitter;
  }

  @override
  ViewSource get viewSource {
    _checkDisposed();
    return _views;
  }

  @override
  PermissionSource get permissionSource {
    _checkDisposed();
    return _perms;
  }

  @override
  ConnectionStatus get connectionStatus {
    _checkDisposed();
    return _connection.connectionStatus;
  }

  @override
  Stream<ConnectionStatus> get connectionStatusStream {
    _checkDisposed();
    return _statusController.stream;
  }

  // Implements: EVS-PRD-cross-process-event-transport/H — delegates to
  //   RemoteConnection.reconnect(), exposing the manual re-auth + re-issue
  //   path to consumers that hold a concrete RemoteScope.
  /// Force the underlying connection to reconnect (re-auth + re-issue live
  /// subscribes) with the current credential. Used after a change to the
  /// effective authorization context (e.g. active-role switch) so live view
  /// subscriptions re-open under the new context. AuthStatus is unaffected —
  /// this is a transport reconnect, not a credential change.
  Future<void> reconnect() async {
    _checkDisposed();
    await _connection.reconnect();
  }

  @override
  Future<void> dispose() async {
    _isDisposed = true;
    // Order matters: dispose the connection FIRST so its own
    // _isDisposed flag is set and its awaited _reconnectDone drains
    // any in-flight reconnect-loop iterations. Those iterations
    // call _emitStatus -> _statusController.add; if we closed the
    // controller first, a late emission would throw
    // 'Bad state: Cannot add event after closing' rather than
    // being silently dropped. Closing the controller AFTER the
    // connection has fully drained is safe.
    await _perms.dispose();
    await _auth.dispose();
    await _connection.dispose();
    await _statusController.close();
  }
}
