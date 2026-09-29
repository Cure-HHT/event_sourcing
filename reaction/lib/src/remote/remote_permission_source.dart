// Implements: EVS-PRD-permission-source/A
// Remote implementation of
//   PermissionSource: synchronous current getter + Stream of
//   EffectiveAuthorization? snapshot updates.
// Implements: EVS-PRD-permission-source/C
// fetches the initial snapshot
//   via HTTP GET /permissions/snapshot and refreshes on the AuthorizationWatcher's
//   stale_data envelope (wired by RemoteScope).
// Implements: EVS-PRD-permission-source/D
// Principal is sourced from
//   the co-mounted AuthSession; no direct mutator.
// Implements: EVS-PRD-permission-source/E
// re-fetches and re-emits the
//   snapshot on every Authenticated transition of the AuthSession.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:reaction/src/wire/view_converging_codec.dart';

/// Schedules [callback] to run after [duration]. The seam
/// [RemotePermissionSource] uses for its 503-retry backoff, injectable
/// in tests (this package has no fake_async) so a test can fire the
/// callback immediately or capture it without waiting out a real
/// delay.
typedef RetryScheduler =
    Timer Function(Duration duration, void Function() callback);

/// PermissionSource over HTTP. Fetches an [EffectiveAuthorization]
/// from the server's `/permissions/snapshot` route on every Authenticated
/// transition of the co-mounted [AuthSession] and exposes it directly
/// to consumers (no intermediate type — clients see the substrate's
/// own permission/scope shape).
///
/// Clears to `null` when the auth session leaves Authenticated.
///
/// Mid-session refresh: [refresh] re-fetches the snapshot and is wired
/// by `RemoteScope` to the server's `stale_data` envelope (emitted by
/// the AuthorizationWatcher on security-EXPANDING changes — `role_assigned`,
/// `permission_granted`, containment changes — see
/// `spec/reaction-remote.md` "Mid-session permission changes"). UI
/// gating that wraps `stream` therefore updates live as the user's
/// permissions broaden, in addition to the initial fetch on
/// Authenticated.
///
/// A 503 `view_converging` response from the snapshot route — on the
/// Authenticated-transition fetch or a [refresh] call — schedules a
/// bounded-backoff retry (honouring a `Retry-After` header when the
/// server sends one) and surfaces the typed, transient refusal through
/// [converging] and [convergingStream] meanwhile, cleared on the next
/// `200`. The retry is cancelled by a newer auth transition, a
/// [refresh] call, or [dispose] (`EVS-DEV-converging-view-reads/H`).
class RemotePermissionSource implements PermissionSource {
  RemotePermissionSource({
    required this.connection,
    required this.authSession,
    RetryScheduler? scheduleRetry,
  }) : _scheduleRetry = scheduleRetry ?? Timer.new {
    _authSub = authSession.stream.listen(_onAuth);
    if (authSession.current is Authenticated) {
      unawaited(_fetchSnapshot());
    }
  }

  final RemoteConnection connection;
  final AuthSession authSession;
  final RetryScheduler _scheduleRetry;

  /// Bounded retry count for a 503 view_converging response: enough to
  /// ride out a copy's catch-up without retrying forever against a
  /// view that never converges.
  static const int _maxRetryAttempts = 5;

  /// Backoff base and cap used when the server sends no `Retry-After`
  /// header: doubles from [_retryBaseDelay] up to [_retryMaxDelay].
  static const Duration _retryBaseDelay = Duration(milliseconds: 200);
  static const Duration _retryMaxDelay = Duration(seconds: 5);

  /// The longest delay a server's `Retry-After` header can set, so a
  /// misconfigured or hostile server cannot park the retry indefinitely.
  static const Duration _retryAfterCeiling = Duration(seconds: 30);

  EffectiveAuthorization? _current;
  final StreamController<EffectiveAuthorization?> _controller =
      StreamController<EffectiveAuthorization?>.broadcast();
  late final StreamSubscription<AuthStatus> _authSub;
  bool _isDisposed = false;

  ViewConvergingRefusal? _converging;
  final StreamController<ViewConvergingRefusal?> _convergingController =
      StreamController<ViewConvergingRefusal?>.broadcast();
  Timer? _retryTimer;

  /// Monotonic generation counter, bumped on every auth-status change.
  /// In-flight `_fetchSnapshot` responses are discarded if a newer
  /// auth transition has fired in the meantime — same last-writer-wins
  /// pattern as `RemoteAuthSession._credentialGen`. Also the retry
  /// seam's cancellation key: a scheduled retry checks it stayed
  /// current before re-fetching.
  int _fetchGen = 0;

  @override
  EffectiveAuthorization? get current => _current;

  /// The typed, transient refusal from the most recent 503
  /// `view_converging` response still awaiting a retry, or `null` when
  /// the last fetch succeeded (or none has failed that way yet).
  ViewConvergingRefusal? get converging => _converging;

  /// Emits [converging] on every change: non-null when a 503
  /// `view_converging` response is retried in the background, `null`
  /// once a fetch succeeds.
  Stream<ViewConvergingRefusal?> get convergingStream =>
      _convergingController.stream;

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
    _cancelRetry();
    if (status is Authenticated) {
      unawaited(_fetchSnapshot());
    } else {
      _current = null;
      if (!_controller.isClosed) _controller.add(null);
      _setConverging(null);
    }
  }

  void _cancelRetry() {
    _retryTimer?.cancel();
    _retryTimer = null;
  }

  void _setConverging(ViewConvergingRefusal? refusal) {
    if (_converging == refusal) return;
    _converging = refusal;
    if (!_convergingController.isClosed) _convergingController.add(refusal);
  }

  /// Re-fetch the snapshot if currently authenticated. No-op otherwise
  /// (when the session is not Authenticated, [current] is already
  /// `null` and there is nothing on the server to read with our
  /// credential). Wired by `RemoteScope` to the WS `stale_data`
  /// envelope so security-EXPANDING permission changes propagate to
  /// the UI without waiting for the next Authenticated transition.
  ///
  /// Bumps the generation counter so any in-flight Authenticated-
  /// triggered fetch, and any retry pending from a prior 503, is
  /// superseded; the last writer wins. Quiet on a transport error, but
  /// propagates a typed [ViewConvergingRefusal]
  /// (EVS-DEV-converging-view-reads/H) from a 503 view_converging
  /// response: an explicit caller-awaited refresh is the one path
  /// where the refusal is worth surfacing to its caller rather than
  /// only backing off silently. The refusal also schedules its own
  /// bounded-backoff retry, same as the Authenticated-transition path,
  /// so a caller that does not await (or that ignores the throw) still
  /// converges on a snapshot once the view catches up.
  @override
  Future<void> refresh() async {
    if (_isDisposed) return;
    if (authSession.current is! Authenticated) return;
    _fetchGen++;
    _cancelRetry();
    await _fetchSnapshot(rethrowConverging: true);
  }

  // Implements: EVS-DEV-converging-view-reads/H
  // decodes a 503 view_converging response into a typed
  //   ViewConvergingRefusal; rethrowConverging distinguishes the
  //   caller-awaited refresh() path (propagates it) from the
  //   fire-and-forget Authenticated-transition fetch (stays quiet,
  //   since nothing awaits it to react). Either path schedules a
  //   bounded-backoff retry and surfaces the refusal via `converging`
  //   until a later fetch succeeds or the attempt bound is reached.
  Future<void> _fetchSnapshot({
    bool rethrowConverging = false,
    int attempt = 0,
  }) async {
    if (_isDisposed) return;
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
    if (gen != _fetchGen || _isDisposed) return;
    if (res.statusCode == 200) {
      _cancelRetry();
      _setConverging(null);
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
      return;
    }
    if (res.statusCode == 503) {
      final refusal = decodeViewConvergingBody(res.body);
      if (refusal != null) {
        _setConverging(refusal);
        _scheduleSnapshotRetry(gen: gen, response: res, attempt: attempt);
        if (rethrowConverging) throw refusal;
        return;
      }
    }
    // Non-200, or a 503 whose body isn't view_converging: leave current
    // state untouched.
  }

  /// Schedules a bounded-backoff retry of [_fetchSnapshot] after a 503
  /// view_converging response, honouring a `Retry-After` header when
  /// the server sends one. No-op once [attempt] reaches
  /// [_maxRetryAttempts], or once [gen] is superseded by the time the
  /// timer fires (a newer auth transition, an explicit [refresh], or
  /// [dispose] — each cancels the previous timer outright, so this is
  /// a second, belt-and-suspenders check).
  void _scheduleSnapshotRetry({
    required int gen,
    required http.Response response,
    required int attempt,
  }) {
    if (attempt >= _maxRetryAttempts) return;
    final delay = _retryAfterHeader(response) ?? _backoffDelay(attempt);
    _cancelRetry();
    _retryTimer = _scheduleRetry(delay, () {
      if (gen != _fetchGen || _isDisposed) return;
      unawaited(_fetchSnapshot(attempt: attempt + 1));
    });
  }

  /// Exponential backoff from [_retryBaseDelay], capped at
  /// [_retryMaxDelay], used when the server sends no `Retry-After`.
  static Duration _backoffDelay(int attempt) {
    final scaled = _retryBaseDelay * (1 << attempt);
    return scaled > _retryMaxDelay ? _retryMaxDelay : scaled;
  }

  /// Parses a `Retry-After` response header (seconds, per RFC 9110) if
  /// present and a non-negative integer, capped at [_retryAfterCeiling];
  /// `null` otherwise, so the caller falls back to [_backoffDelay].
  static Duration? _retryAfterHeader(http.Response response) {
    String? raw;
    for (final entry in response.headers.entries) {
      if (entry.key.toLowerCase() == 'retry-after') {
        raw = entry.value;
        break;
      }
    }
    if (raw == null) return null;
    final seconds = int.tryParse(raw.trim());
    if (seconds == null || seconds < 0) return null;
    final delay = Duration(seconds: seconds);
    return delay > _retryAfterCeiling ? _retryAfterCeiling : delay;
  }

  @override
  Future<void> dispose() async {
    _isDisposed = true;
    _cancelRetry();
    await _authSub.cancel();
    if (!_controller.isClosed) await _controller.close();
    if (!_convergingController.isClosed) await _convergingController.close();
  }
}
