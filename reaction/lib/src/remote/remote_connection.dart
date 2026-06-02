// Implements: EVS-PRD-cross-process-event-transport/A — drives codec
//   round-trip on the wire (Update<T> envelopes decoded via UpdateCodec
//   and routed to per-subscription StreamControllers).
// Implements: EVS-PRD-cross-process-event-transport/B — every routed
//   envelope carries sequence + subscriptionId end-to-end.
// Implements: EVS-PRD-cross-process-event-transport/D — multiplexes
//   multiple concurrent subscriptions over one WebSocket connection,
//   distinguishing them by client-chosen UUID v4 subscriptionId.
// Implements: EVS-PRD-cross-process-event-transport/F — injects the
//   bearer credential into every HTTP POST and into the first WS auth
//   message; httpPost adds Authorization: Bearer header when a
//   credential is set.
// Implements: EVS-PRD-cross-process-event-transport/H — auto-reconnect
//   with exponential backoff on non-auth WS drops; re-authenticates and
//   re-issues every active subscribe on success.
// Implements: EVS-PRD-cross-process-event-transport/I — observable
//   ConnectionStatus transitions driven by WS lifecycle events
//   (initial-open success -> Connected; non-auth close -> Reconnecting;
//   retry-exhausted -> Disconnected).

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/scope/connection_status.dart';
import 'package:reaction/src/wire/subscription_messages.dart';
import 'package:reaction/src/wire/update_codec.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Exponential-backoff policy for [RemoteConnection]'s auto-reconnect.
///
/// Default: starts at 250ms, doubles on each failure, capped at
/// [maxInterval] per attempt and [maxAttempts] retries before giving up.
/// Override per-deployment via [RemoteConnection]'s constructor (e.g.,
/// tests use ms-scale intervals so the suite runs in seconds, not
/// minutes).
///
/// The total wall-clock budget before [RemoteConnection] gives up and
/// transitions to [Disconnected] is the sum of [delayFor] over `0
/// .. maxAttempts - 1`. With defaults (250ms initial, 2x, 10 attempts,
/// 30s cap) that is roughly 0.25 + 0.5 + 1 + 2 + 4 + 8 + 16 + 30 + 30 +
/// 30 = ~122s end-to-end before [Disconnected] fires.
class ExponentialBackoff {
  const ExponentialBackoff({
    this.initial = const Duration(milliseconds: 250),
    this.multiplier = 2,
    this.maxAttempts = 10,
    this.maxInterval = const Duration(seconds: 30),
  });

  /// Delay before the FIRST reconnect attempt (after a WS drop). Each
  /// subsequent attempt scales by [multiplier], capped at [maxInterval].
  final Duration initial;

  /// Per-attempt multiplier; with the default of 2, delays double on
  /// each successive failure.
  final num multiplier;

  /// Number of reconnect attempts before transitioning to
  /// [Disconnected]. After [maxAttempts] consecutive failures, the
  /// reconnect loop gives up.
  final int maxAttempts;

  /// Upper bound on the per-attempt delay. Without this cap, an
  /// exponentially-growing delay would eventually exceed any
  /// reasonable wait between attempts.
  final Duration maxInterval;

  /// Delay before the (zero-indexed) reconnect attempt. `attempt == 0`
  /// returns [initial]; subsequent attempts multiply by [multiplier],
  /// capped at [maxInterval].
  Duration delayFor(int attempt) {
    var ms = initial.inMicroseconds.toDouble();
    for (var i = 0; i < attempt; i++) {
      ms *= multiplier;
      if (ms >= maxInterval.inMicroseconds) {
        return maxInterval;
      }
    }
    return Duration(microseconds: ms.round());
  }
}

/// Internal record: a live subscription's controller plus the
/// SubscribeMsg used to open it, so the auto-reconnect loop can
/// re-issue an identical subscribe after a WS drop.
class _SubscriptionEntry {
  _SubscriptionEntry({required this.controller, required this.subscribeMsg});
  final StreamController<Update<Map<String, Object?>>> controller;
  final SubscribeMsg subscribeMsg;
}

/// Shared wire-state across the four Remote* impls of one RemoteScope.
/// Owns:
///   - HTTP client + bearer header injection from current credential.
///   - WebSocket lifecycle (lazy connect; close on last unsub + grace
///     period).
///   - Subscription registry (keyed by client-chosen UUID v4).
///   - Auto-reconnect on non-auth WS drops: exponential backoff;
///     re-authenticates and re-issues every active subscribe on
///     successful reconnect; transitions through [Reconnecting] ->
///     [Connected] (or [Disconnected] on retry-exhausted).
class RemoteConnection {
  RemoteConnection({
    required this.baseUrl,
    required http.Client httpClient,
    required this.wsFactory,
    Duration idleGrace = const Duration(seconds: 30),
    ExponentialBackoff reconnectBackoff = const ExponentialBackoff(),
  }) : _httpClient = httpClient,
       _idleGrace = idleGrace,
       _backoff = reconnectBackoff;

  final Uri baseUrl;
  final http.Client _httpClient;
  final WebSocketChannel Function(Uri) wsFactory;
  final Duration _idleGrace;
  final ExponentialBackoff _backoff;

  /// Invoked when the WS channel closes with an auth-related close
  /// code (4001 auth_rejected, 4003 permissions_changed). Wired by
  /// [RemoteScope] to [RemoteAuthSession.handleAuthRejected], which flips
  /// the session to [Expired]. Other close codes (1000 normal, 1006
  /// abnormal, 1011 server-internal-error, etc.) are treated as wire
  /// drops, not auth changes, and do not invoke this callback.
  ///
  /// Implemented as a settable field rather than a constructor
  /// parameter because [RemoteAuthSession] takes a [RemoteConnection]
  /// at construction time, and Dart forbids referencing
  /// `this`-instance fields inside an initializer-list closure — so
  /// the wiring must happen post-construction in [RemoteScope]'s body.
  void Function()? onAuthClose;

  /// Invoked when the server pushes a `stale_data` envelope on the
  /// shared WS connection. The envelope is connection-scoped (not
  /// keyed to any one subscription): the server's `AuthorizationWatcher` emits
  /// it on security-EXPANDING changes (`role_assigned`,
  /// `permission_granted`) and on opt-in containment updates. Wired by
  /// [RemoteScope] to trigger a [RemotePermissionSource] re-fetch so
  /// UI gating updates without waiting for the next [Authenticated]
  /// transition.
  ///
  /// Settable field for the same `this`-in-initializer-closure reason
  /// as [onAuthClose].
  void Function(String? reason)? onStaleData;

  /// Invoked on every [ConnectionStatus] transition (de-duped: the
  /// callback is NOT fired when the new status equals the previous one,
  /// so a successful initial open emits `Connected` once, not twice).
  /// Wired by [RemoteScope] to drive its public
  /// `connectionStatusStream`. Same settable-field rationale as
  /// [onAuthClose] / [onStaleData].
  void Function(ConnectionStatus)? onConnectionStatusChanged;

  /// Synchronous accessor: the most recent [ConnectionStatus] emitted.
  /// Pre-first-WS-open this is [Disconnected]; per the spec
  /// (`spec/reaction-remote.md` Section 3, "ConnectionStatus
  /// transitions"), the initial state is `Disconnected` until the
  /// first WS open succeeds.
  ConnectionStatus get connectionStatus => _currentStatus;
  ConnectionStatus _currentStatus = const Disconnected();

  void _emitStatus(ConnectionStatus s) {
    if (s == _currentStatus) return;
    _currentStatus = s;
    onConnectionStatusChanged?.call(s);
  }

  String? _credential;
  bool _isDisposed = false;

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _wsStreamSub;
  Future<void>? _connecting;

  /// True while [_runReconnectLoop] is awaiting a backoff delay or a
  /// [_connect] retry. Guards against (a) two reconnect loops running
  /// in parallel if multiple `_onWsClosed` calls fire (defence in
  /// depth — only one channel exists at a time), and (b) `dispose()`
  /// returning before the in-flight loop finishes (otherwise the loop
  /// could outlive the connection and emit ghost status transitions).
  bool _isReconnecting = false;
  Completer<void>? _reconnectDone;

  /// Completes when the server acks the first-WS-message auth with
  /// `auth_ok`. `_ensureConnected` awaits this BEFORE returning so that
  /// queued `openSubscription` calls cannot flush a `SubscribeMsg` while
  /// the server is still running its (possibly slow, network-backed)
  /// validator — a subscribe arriving pre-auth is treated as an
  /// auth-protocol violation and the server closes with 4001. Completes
  /// with an error if the socket closes (4001/4003 or a wire drop)
  /// before `auth_ok` arrives, so awaiters don't hang forever. Reset on
  /// each (re)connect.
  Completer<void>? _authComplete;

  /// Registry of live subscriptions, keyed by client-chosen UUID v4
  /// subscriptionId. Each entry retains its [SubscribeMsg] so the
  /// auto-reconnect loop can re-issue an identical subscribe after
  /// a WS drop (per `EVS-PRD-cross-process-event-transport`-H).
  final Map<String, _SubscriptionEntry> _subs = {};
  Timer? _idleCloseTimer;

  /// Currently-stored credential. Null if not authenticated.
  String? get credential => _credential;

  /// Set or clear the credential. Future HTTP calls and the WS auth
  /// message will use the new value.
  void setCredential(String? credential) {
    _credential = credential;
  }

  // Implements: EVS-PRD-cross-process-event-transport/H — exposes the
  //   same re-auth + re-issue machinery as the auto-reconnect loop to
  //   callers that need to force a reconnect on a credential context
  //   change (e.g. active-role switch) rather than waiting for a wire drop.
  /// Force a WS reconnect that re-authenticates with the CURRENT credential
  /// and re-issues every live subscribe. Used when the effective
  /// authorization context changes (e.g. an active-role switch carried in
  /// the credential) so already-open subscriptions re-open under the new
  /// context. No-op when no socket is open (the next openSubscription will
  /// connect with the current credential anyway) or after dispose.
  Future<void> reconnect() async {
    final ch = _channel;
    if (ch == null || _isDisposed) return;
    // Cancel + null the old stream subscription FIRST so that the
    // channel's real close-handshake echo (which fires asynchronously
    // on a real WS, potentially AFTER the reconnect loop has already
    // opened a new _channel) can never reach _onWsClosed and clobber
    // the new generation's _authComplete. _isReconnecting remains a
    // belt-and-suspenders backstop.
    // The fake WS doesn't echo a close, so the direct _onWsClosed()
    // call below is what drives the reconnect loop in tests.
    await _wsStreamSub?.cancel();
    _wsStreamSub = null;
    _channel = null;
    unawaited(ch.sink.close(1000, 'reconnect'));
    _onWsClosed();
  }

  Map<String, String> _authHeaders() {
    final c = _credential;
    return c == null ? const {} : {'Authorization': 'Bearer $c'};
  }

  /// HTTP GET with auth header.
  Future<http.Response> httpGet(Uri url) =>
      _httpClient.get(url, headers: _authHeaders());

  /// HTTP POST with auth header + JSON body.
  Future<http.Response> httpPost(Uri url, {required String body}) =>
      _httpClient.post(
        url,
        headers: {..._authHeaders(), 'Content-Type': 'application/json'},
        body: body,
      );

  /// Derive the WS URL from baseUrl (http -> ws, https -> wss).
  Uri get wsUrl {
    final wsScheme = baseUrl.scheme == 'https' ? 'wss' : 'ws';
    return baseUrl.replace(scheme: wsScheme, path: '/subscriptions');
  }

  /// Open a subscription against the WS connection. Returns a stream
  /// that emits Update<Map<String, Object?>> envelopes for this
  /// subscriptionId. The mapper is applied client-side by
  /// RemoteViewSource (this connection method works in untyped maps).
  Stream<Update<Map<String, Object?>>> openSubscription({
    required String subscriptionId,
    required String viewName,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    final controller = StreamController<Update<Map<String, Object?>>>(
      onCancel: () => _closeSubscription(subscriptionId),
    );
    final subscribeMsg = SubscribeMsg(
      subscriptionId: subscriptionId,
      viewName: viewName,
      filter: filter,
      aggregates: aggregates,
    );
    _subs[subscriptionId] = _SubscriptionEntry(
      controller: controller,
      subscribeMsg: subscribeMsg,
    );
    _idleCloseTimer?.cancel();
    _idleCloseTimer = null;
    _ensureConnected().then(
      (_) {
        _sendClient(subscribeMsg);
      },
      // If the handshake failed (socket dropped before auth_ok), surface
      // it on this subscription's stream instead of leaving an unhandled
      // async error. _onWsClosed already errored any subs registered at
      // close time; this covers the open-after-close ordering.
      onError: (Object e) {
        final entry = _subs.remove(subscriptionId);
        if (entry != null && !entry.controller.isClosed) {
          entry.controller
            ..addError(e)
            ..close();
        }
      },
    );
    return controller.stream;
  }

  Future<void> _ensureConnected() async {
    if (_isDisposed) throw StateError('connection disposed');
    // A live channel that has already completed its auth handshake is
    // ready immediately. While auth is still in flight (`_authComplete`
    // not yet done) fall through to await it via `_connecting`.
    if (_channel != null && (_authComplete?.isCompleted ?? true)) return;
    _connecting ??= _connect();
    try {
      await _connecting;
    } finally {
      _connecting = null;
    }
  }

  Future<void> _connect() async {
    final channel = wsFactory(wsUrl);
    _channel = channel;
    final authComplete = Completer<void>();
    _authComplete = authComplete;
    _wsStreamSub = channel.stream.listen(
      _onMessage,
      onDone: _onWsClosed,
      onError: (_) => _onWsClosed(),
      cancelOnError: false,
    );
    _sendClient(AuthMessage(credential: _credential ?? ''));
    // Block connection-readiness on the server's auth ack so queued
    // subscriptions don't race ahead of authentication. Resolved in
    // [_onMessage] on `auth_ok`; failed in [_onWsClosed] if the socket
    // drops first.
    await authComplete.future;
  }

  void _sendClient(ClientMessage m) {
    _channel?.sink.add(jsonEncode(SubscriptionMessages.encodeClient(m)));
  }

  void _onMessage(dynamic raw) {
    final json = jsonDecode(raw as String) as Map<String, Object?>;
    final type = json['type'] as String?;
    if (type == 'auth_ok') {
      // Server acked auth: unblock _ensureConnected so queued
      // subscriptions can flush. Emit Connected on auth_ok rather
      // than raw WS open because the connection isn't usable until
      // auth completes (subscriptions queued pre-auth_ok don't flush);
      // tying the visible status to auth_ok matches the
      // "transport is up; subscriptions are live" semantics of
      // [Connected] (see connection_status.dart docstring).
      final ac = _authComplete;
      if (ac != null && !ac.isCompleted) ac.complete();
      _emitStatus(const Connected());
      return;
    }
    if (type == 'subscription_denied' || type == 'error') {
      final subId = json['subscriptionId'] as String?;
      if (subId != null) {
        final entry = _subs[subId];
        if (entry != null) {
          entry.controller.addError(
            'subscription_denied: ${json['reason'] ?? json['message']}',
          );
          _subs.remove(subId);
          entry.controller.close();
        }
      }
      return;
    }
    if (type == 'stale_data') {
      // Connection-scoped notification: the server's AuthorizationWatcher tells
      // us SOME of our cached authorization state may be stale (a
      // role_assigned / permission_granted / containment change
      // expanded what we can see). Not subscription-scoped — the
      // envelope carries `reason` only, no subscriptionId. Hand off to
      // the wired callback ([RemoteScope] routes this to
      // [RemotePermissionSource.refresh] so UI gating updates live).
      onStaleData?.call(json['reason'] as String?);
      return;
    }
    // Otherwise: Update<T> envelope routed by subscriptionId.
    final subId = UpdateCodec.subscriptionIdOf(json);
    final entry = _subs[subId];
    if (entry != null) {
      entry.controller.add(UpdateCodec.decode(json));
    }
  }

  void _onWsClosed() {
    // Capture the close code BEFORE nulling _channel: the server-side
    // AuthorizationWatcher signals mid-session auth-revocations via WS close
    // frames (4001 auth_rejected on credential rejection at connect-
    // time, 4003 permissions_changed on mid-session role_unassigned /
    // permission_revoked), and we must route those into
    // RemoteAuthSession.handleAuthRejected so the session flips to
    // Expired. Other close codes (1000 normal, 1006 abnormal, etc.)
    // are wire drops, not auth changes.
    final code = _channel?.closeCode;
    _channel = null;
    // If the socket dropped before `auth_ok`, fail the in-flight
    // handshake so any awaiting `_ensureConnected` (and the
    // `openSubscription` chained on it) errors out rather than hanging.
    final ac = _authComplete;
    if (ac != null && !ac.isCompleted) {
      ac.completeError(StateError('ws closed before auth_ok (code: $code)'));
    }
    _authComplete = null;
    if (code == 4001 || code == 4003) {
      // Auth-rejected / permissions-changed: route to AuthSession and
      // STOP. Per `EVS-PRD-cross-process-event-transport`-H these are
      // explicit carve-outs from auto-reconnect — the credential itself
      // is bad (4001) or the cached server-side authorization context
      // is stale (4003); reconnecting blindly would loop the same
      // failure. The application's auth handler decides what to do.
      onAuthClose?.call();
      // Tell open subs the wire dropped so awaiting consumers don't
      // hang silently; they'll see an explicit error rather than a
      // stalled stream.
      _failOpenSubsWithDisconnect();
      return;
    }
    // No active subscriptions means there's nothing to reconnect for:
    // either the connection was idle-closed (1000 from
    // _maybeScheduleIdleClose), or every subscription was already
    // cancelled. Skip the loop in either case — reconnecting an empty
    // connection would emit Reconnecting -> Connected just to sit
    // there with nothing to receive.
    if (_subs.isEmpty || _isDisposed) {
      return;
    }
    // Non-auth close with live subs: start the auto-reconnect cycle.
    _emitStatus(const Reconnecting());
    unawaited(_runReconnectLoop());
  }

  /// Errors every open subscription controller with 'wire_disconnected'.
  /// Used on the auth-rejected close paths where the client gives up on
  /// the existing subscriptions (the application must re-authenticate
  /// before opening new ones).
  void _failOpenSubsWithDisconnect() {
    for (final entry in _subs.values) {
      if (!entry.controller.isClosed) {
        entry.controller.addError('wire_disconnected');
      }
    }
  }

  /// Runs the exponential-backoff reconnect loop. On each attempt:
  ///   1. wait `_backoff.delayFor(attempt)`,
  ///   2. open a fresh WS via `_connect` (which re-sends the AuthMessage
  ///      and awaits `auth_ok`; emits Connected on success),
  ///   3. re-issue every active subscribe so the server replays a
  ///      fresh `Snapshot×N → EndOfReplay → live` per substrate
  ///      snapshot-then-deltas semantics.
  /// On `maxAttempts` consecutive failures, transitions to Disconnected.
  Future<void> _runReconnectLoop() async {
    if (_isReconnecting) return;
    _isReconnecting = true;
    _reconnectDone = Completer<void>();
    try {
      for (var attempt = 0; attempt < _backoff.maxAttempts; attempt++) {
        if (_isDisposed) return;
        await Future<void>.delayed(_backoff.delayFor(attempt));
        if (_isDisposed) return;
        try {
          // Re-use `_ensureConnected` so a concurrent `openSubscription`
          // racing in during the gap blocks on the SAME connect future
          // rather than starting a second one. `_connect` will be
          // invoked exactly once per attempt.
          await _ensureConnected();
          // auth_ok arrived -> _onMessage already emitted Connected.
          // Re-issue every live subscribe so the server replays a fresh
          // snapshot-then-deltas per the substrate's reactive semantics
          // (re-subscribed views see Snapshot×N → EndOfReplay → live).
          for (final entry in _subs.values) {
            _sendClient(entry.subscribeMsg);
          }
          return;
        } catch (_) {
          // Connect failed (handshake dropped, factory threw, etc.);
          // fall through to next attempt.
        }
      }
      // Exhausted all attempts.
      if (!_isDisposed) {
        _emitStatus(const Disconnected());
        _failOpenSubsWithDisconnect();
      }
    } finally {
      _isReconnecting = false;
      _reconnectDone?.complete();
      _reconnectDone = null;
    }
  }

  void _closeSubscription(String subscriptionId) {
    _subs.remove(subscriptionId);
    _sendClient(UnsubscribeMsg(subscriptionId: subscriptionId));
    _maybeScheduleIdleClose();
  }

  void _maybeScheduleIdleClose() {
    if (_subs.isEmpty) {
      _idleCloseTimer?.cancel();
      _idleCloseTimer = Timer(_idleGrace, () {
        // _subs.isEmpty here too (no openSubscription raced ahead),
        // so the resulting _onWsClosed will NOT auto-reconnect — the
        // empty-subs short-circuit handles deliberate idle teardown.
        _channel?.sink.close(1000, 'normal');
        _channel = null;
      });
    }
  }

  Future<void> dispose() async {
    _isDisposed = true;
    _idleCloseTimer?.cancel();
    // Fail any in-flight handshake so awaiters don't hang past dispose.
    final ac = _authComplete;
    if (ac != null && !ac.isCompleted) {
      ac.completeError(StateError('connection disposed'));
    }
    _authComplete = null;
    // Wait for any in-flight reconnect loop to notice `_isDisposed` and
    // bail out, so dispose() doesn't return while a background
    // reconnect attempt is still mutating state behind us.
    final reconnectDone = _reconnectDone;
    if (reconnectDone != null && !reconnectDone.isCompleted) {
      await reconnectDone.future;
    }
    // Snapshot controllers before close: onCancel callbacks mutate
    // _subs via _closeSubscription, so we cannot iterate _subs directly.
    final controllers = _subs.values.map((e) => e.controller).toList();
    _subs.clear();
    for (final ctrl in controllers) {
      await ctrl.close();
    }
    await _wsStreamSub?.cancel();
    _wsStreamSub = null;
    await _channel?.sink.close(1000, 'normal');
    _channel = null;
    _httpClient.close();
  }
}
