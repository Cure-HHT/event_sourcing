import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

/// PermissionSource over HTTP. Fetches an [EffectiveAuthorization]
/// from the server's `/permissions/snapshot` route on every Authenticated
/// transition of the co-mounted [AuthSession]; decodes it into the
/// substrate's [PermissionSnapshot] shape once at fetch time (so the
/// `current` getter returns a stable instance across reads — no
/// per-read `DateTime.now()` drift).
///
/// Clears to `null` when the auth session leaves Authenticated.
///
/// The WS-driven invalidation path (re-fetch when the server pushes a
/// permissions update) is intentionally deferred to Phase 4 e2e; this
/// impl is the HTTP-snapshot half only.
class RemotePermissionSource implements PermissionSource {
  RemotePermissionSource({
    required this.connection,
    required this.authSession,
  }) {
    _authSub = authSession.stream.listen(_onAuth);
    if (authSession.current is Authenticated) {
      unawaited(_fetchSnapshot());
    }
  }

  final RemoteConnection connection;
  final AuthSession authSession;

  PermissionSnapshot? _current;
  final StreamController<PermissionSnapshot?> _controller =
      StreamController<PermissionSnapshot?>.broadcast();
  late final StreamSubscription<AuthStatus> _authSub;
  bool _disposed = false;

  /// Monotonic generation counter, bumped on every auth-status change.
  /// In-flight `_fetchSnapshot` responses are discarded if a newer
  /// auth transition has fired in the meantime — same last-writer-wins
  /// pattern as `RemoteAuthSession._credentialGen`.
  int _fetchGen = 0;

  @override
  PermissionSnapshot? get current => _current;

  @override
  Stream<PermissionSnapshot?> get stream {
    // Per-listener wrapper: emits _current synchronously on subscribe
    // (snapshot-on-listen contract), then forwards all subsequent updates
    // from the broadcast controller. Mirrors LocalPermissionSource.stream.
    late StreamController<PermissionSnapshot?> per;
    StreamSubscription<PermissionSnapshot?>? forwardSub;
    per = StreamController<PermissionSnapshot?>(
      onListen: () {
        per.add(_current);
        forwardSub = _controller.stream.listen(per.add, onDone: per.close);
      },
      onCancel: () => forwardSub?.cancel(),
    );
    return per.stream;
  }

  void _onAuth(AuthStatus status) {
    _fetchGen++;
    if (status is Authenticated) {
      unawaited(_fetchSnapshot());
    } else {
      _current = null;
      if (!_controller.isClosed) _controller.add(null);
    }
  }

  Future<void> _fetchSnapshot() async {
    if (_disposed) return;
    final gen = _fetchGen;
    final url = connection.baseUrl.replace(path: '/permissions/snapshot');
    final res = await connection.httpGet(url);
    // Drop stale responses: a newer auth transition has superseded us,
    // or we've been disposed.
    if (gen != _fetchGen || _disposed) return;
    if (res.statusCode == 200) {
      final effective = EffectiveAuthorizationCodec.decode(
        jsonDecode(res.body) as Map<String, Object?>,
      );
      // Convert once at fetch time and retain the PermissionSnapshot
      // directly — successive `current` reads must return the same
      // instance (stable issuedAt). If a future task wants the richer
      // EffectiveAuthorization view (scopeAssignments etc.), that's a
      // wider PermissionSource interface change, out of scope here.
      final snapshot = PermissionSnapshot(
        role: effective.activeRole,
        grants: effective.rolePermissions,
        issuedAt: DateTime.now(),
      );
      _current = snapshot;
      if (!_controller.isClosed) _controller.add(snapshot);
    }
    // Non-200: leave current state untouched. Phase 4 e2e exercises the
    // 401 + WS-update paths.
  }

  @override
  Future<void> dispose() async {
    _disposed = true;
    await _authSub.cancel();
    if (!_controller.isClosed) await _controller.close();
  }
}
