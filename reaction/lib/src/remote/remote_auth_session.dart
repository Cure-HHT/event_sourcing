// Implements: EVS-PRD-auth-session/A — Remote implementation of
//   AuthSession: current, stream, setCredential, principal.
// Implements: EVS-PRD-auth-session/E — handleAuthRejected (wired by
//   RemoteScope from WS close-frames 4001 / 4003) and HTTP 401 from
//   the GET /me round-trip both transition the session to Expired and
//   emit the new status on the stream.
// Implements: EVS-PRD-auth-session/G — exposes the validated Principal
//   as the source of truth for downstream interfaces.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/principal_codec.dart';

/// AuthSession over HTTP + WS-close-frame signals. The credential is
/// stored on the shared [RemoteConnection]; setCredential fires a
/// GET /me to validate and obtain the Principal.
class RemoteAuthSession implements AuthSession {
  RemoteAuthSession({required this.connection})
    : _current = const NotAuthenticated();

  final RemoteConnection connection;
  AuthStatus _current;
  final StreamController<AuthStatus> _controller =
      StreamController<AuthStatus>.broadcast();

  /// Monotonic generation counter, bumped on every [setCredential] call.
  /// In-flight [_validate] responses are discarded if a newer
  /// [setCredential] has fired in the meantime (last-writer-wins).
  int _credentialGen = 0;

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _controller.stream;

  @override
  Principal? get principal {
    final c = _current;
    return c is Authenticated ? c.principal : null;
  }

  @override
  void setCredential(String? credential) {
    _credentialGen++;
    connection.setCredential(credential);
    if (credential == null) {
      _transition(const NotAuthenticated());
      return;
    }
    unawaited(_validate());
  }

  /// Externally invoked by RemoteConnection when a WS close-frame
  /// 4001 auth_rejected arrives.
  void handleAuthRejected() => _transition(const Expired());

  /// Externally invoked by RemoteActionSubmitter / RemotePermissionSource
  /// when a 401 arrives on HTTP.
  void handleWireUnauthorized() => _transition(const Expired());

  Future<void> _validate() async {
    final gen = _credentialGen;
    final http.Response res;
    try {
      res = await connection.httpGet(connection.baseUrl.replace(path: '/me'));
    } catch (_) {
      // Transport error (server gone, connection severed mid-request,
      // DNS, etc.): treat the same as a non-200 / other-status branch
      // — leave current state untouched. This also covers the dispose-
      // race window where the underlying http client is closed while
      // a /me validate fired by setCredential is still in flight.
      return;
    }
    // Drop stale responses: a newer setCredential has superseded us.
    if (gen != _credentialGen) return;
    if (res.statusCode == 200) {
      final principal = PrincipalCodec.decode(
        jsonDecode(res.body) as Map<String, Object?>,
      );
      _transition(Authenticated(principal: principal));
    } else if (res.statusCode == 401) {
      _transition(const Expired());
    } else {
      // Other status: stay where we are. Server-side bug or transient.
    }
  }

  void _transition(AuthStatus next) {
    _current = next;
    if (!_controller.isClosed) _controller.add(next);
  }

  @override
  Future<void> dispose() async {
    if (!_controller.isClosed) await _controller.close();
  }
}
