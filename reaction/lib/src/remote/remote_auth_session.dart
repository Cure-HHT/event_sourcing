import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
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
  final StreamController<AuthStatus> _ctl =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _ctl.stream;

  @override
  Principal? get principal {
    final c = _current;
    return c is Authenticated ? c.principal : null;
  }

  @override
  void setCredential(String? credential) {
    connection.setCredential(credential);
    if (credential == null) {
      _transition(const NotAuthenticated());
      return;
    }
    unawaited(_validate());
  }

  /// Externally invoked by RemoteConnection when a WS close-frame
  /// 4001 auth_rejected arrives.
  void onAuthRejected() => _transition(const Expired());

  /// Externally invoked by RemoteActionSubmitter / RemotePermissionSource
  /// when a 401 arrives on HTTP.
  void onWireUnauthorized() => _transition(const Expired());

  Future<void> _validate() async {
    final res = await connection.httpGet(
      connection.baseUrl.replace(path: '/me'),
    );
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
    _ctl.add(next);
  }

  @override
  Future<void> dispose() async {
    await _ctl.close();
  }
}
