// Implements: EVS-PRD-permission-source/A — Remote implementation of
//   PermissionSource: synchronous current getter + Stream of
//   EffectiveAuthorization? snapshot updates.
// Implements: EVS-PRD-permission-source/C — fetches the initial snapshot
//   via HTTP GET /permissions/snapshot and refreshes on the AuthzWatcher's
//   stale_data envelope (wired by RemoteScope).
// Implements: EVS-PRD-permission-source/D — Principal is sourced from
//   the co-mounted AuthSession; no direct mutator.
// Implements: EVS-PRD-permission-source/E — re-fetches and re-emits the
//   snapshot on every Authenticated transition of the AuthSession.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

/// PermissionSource over HTTP. Fetches an [EffectiveAuthorization]
/// from the server's `/permissions/snapshot` route on every Authenticated
/// transition of the co-mounted [AuthSession] and exposes it directly
/// to consumers (no intermediate type — clients see the substrate's
/// own permission/scope shape).
///
/// Clears to `null` when the auth session leaves Authenticated.
///
/// Mid-session refresh: [refresh] re-fetches the snapshot and is wired
/// by [RemoteScope] to the server's `stale_data` envelope (emitted by
/// the AuthzWatcher on security-EXPANDING changes — `role_assigned`,
/// `permission_granted`, containment changes — see
/// `spec/reaction-remote.md` "Mid-session permission changes"). UI
/// gating that wraps `stream` therefore updates live as the user's
/// permissions broaden, in addition to the initial fetch on
/// Authenticated.
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

  EffectiveAuthorization? _current;
  final StreamController<EffectiveAuthorization?> _controller =
      StreamController<EffectiveAuthorization?>.broadcast();
  late final StreamSubscription<AuthStatus> _authSub;
  bool _disposed = false;

  /// Monotonic generation counter, bumped on every auth-status change.
  /// In-flight `_fetchSnapshot` responses are discarded if a newer
  /// auth transition has fired in the meantime — same last-writer-wins
  /// pattern as `RemoteAuthSession._credentialGen`.
  int _fetchGen = 0;

  @override
  EffectiveAuthorization? get current => _current;

  @override
  Stream<EffectiveAuthorization?> get stream {
    // Per-listener wrapper: emits _current synchronously on subscribe
    // (snapshot-on-listen contract), then forwards all subsequent updates
    // from the broadcast controller. Mirrors LocalPermissionSource.stream.
    late StreamController<EffectiveAuthorization?> per;
    StreamSubscription<EffectiveAuthorization?>? forwardSub;
    per = StreamController<EffectiveAuthorization?>(
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

  /// Re-fetch the snapshot if currently authenticated. No-op otherwise
  /// (when the session is not Authenticated, [current] is already
  /// `null` and there is nothing on the server to read with our
  /// credential). Wired by [RemoteScope] to the WS `stale_data`
  /// envelope so security-EXPANDING permission changes propagate to
  /// the UI without waiting for the next Authenticated transition.
  ///
  /// Bumps the generation counter so any in-flight Authenticated-
  /// triggered fetch is superseded; the last writer wins. Quiet on
  /// transport errors (same shape as the Authenticated-transition
  /// path).
  Future<void> refresh() async {
    if (_disposed) return;
    if (authSession.current is! Authenticated) return;
    _fetchGen++;
    await _fetchSnapshot();
  }

  Future<void> _fetchSnapshot() async {
    if (_disposed) return;
    final gen = _fetchGen;
    final url = connection.baseUrl.replace(path: '/permissions/snapshot');
    final http.Response res;
    try {
      res = await connection.httpGet(url);
    } catch (_) {
      // Transport error (server gone, connection severed mid-request,
      // DNS, etc.): treat the same as a non-200 — leave current state
      // untouched. This also covers the dispose-race window where the
      // underlying http client is closed while a fetch triggered by a
      // just-observed Authenticated transition is in flight.
      return;
    }
    // Drop stale responses: a newer auth transition has superseded us,
    // or we've been disposed.
    if (gen != _fetchGen || _disposed) return;
    if (res.statusCode == 200) {
      final effective = EffectiveAuthorizationCodec.decode(
        jsonDecode(res.body) as Map<String, Object?>,
      );
      // Mirror LocalPermissionSource: the policy returns
      // EffectiveAuthorization.empty (activeRole == '') for principals
      // who don't actually hold their claimed activeRole. Map that to
      // `null` so `current` matches the in-process reference impl —
      // otherwise UI code treating non-null `current` as
      // "loaded/authorized" diverges between Local and Remote.
      if (effective.activeRole.isEmpty) {
        _current = null;
        if (!_controller.isClosed) _controller.add(null);
        return;
      }
      _current = effective;
      if (!_controller.isClosed) _controller.add(effective);
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
