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

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/wire/subscription_messages.dart';
import 'package:reaction/src/wire/update_codec.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Shared wire-state across the four Remote* impls of one RemoteScope.
/// Owns:
///   - HTTP client + bearer header injection from current credential.
///   - WebSocket lifecycle (lazy connect; close on last unsub + grace
///     period).
///   - Subscription registry (keyed by client-chosen UUID v4).
///   - Baseline reconnect-on-drop (errors all subs; full
///     exponential-backoff reconnect deferred to e2e tests).
class RemoteConnection {
  RemoteConnection({
    required this.baseUrl,
    required http.Client httpClient,
    required this.wsFactory,
    Duration idleGrace = const Duration(seconds: 30),
  }) : _httpClient = httpClient,
       _idleGrace = idleGrace;

  final Uri baseUrl;
  final http.Client _httpClient;
  final WebSocketChannel Function(Uri) wsFactory;
  final Duration _idleGrace;

  /// Invoked when the WS channel closes with an auth-related close
  /// code (4001 auth_rejected, 4003 permissions_changed). Wired by
  /// [RemoteScope] to [RemoteAuthSession.onAuthRejected], which flips
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
  /// keyed to any one subscription): the server's `AuthzWatcher` emits
  /// it on security-EXPANDING changes (`role_assigned`,
  /// `permission_granted`) and on opt-in containment updates. Wired by
  /// [RemoteScope] to trigger a [RemotePermissionSource] re-fetch so
  /// UI gating updates without waiting for the next [Authenticated]
  /// transition.
  ///
  /// Settable field for the same `this`-in-initializer-closure reason
  /// as [onAuthClose].
  void Function(String? reason)? onStaleData;

  String? _credential;
  bool _disposed = false;

  WebSocketChannel? _channel;
  Future<void>? _connecting;

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

  final Map<String, StreamController<Update<Map<String, Object?>>>> _subs = {};
  Timer? _idleCloseTimer;

  /// Currently-stored credential. Null if not authenticated.
  String? get credential => _credential;

  /// Set or clear the credential. Future HTTP calls and the WS auth
  /// message will use the new value.
  void setCredential(String? credential) {
    _credential = credential;
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
    _subs[subscriptionId] = controller;
    _idleCloseTimer?.cancel();
    _idleCloseTimer = null;
    _ensureConnected().then(
      (_) {
        _sendClient(
          SubscribeMsg(
            subscriptionId: subscriptionId,
            viewName: viewName,
            filter: filter,
            aggregates: aggregates,
          ),
        );
      },
      // If the handshake failed (socket dropped before auth_ok), surface
      // it on this subscription's stream instead of leaving an unhandled
      // async error. _onWsClosed already errored any subs registered at
      // close time; this covers the open-after-close ordering.
      onError: (Object e) {
        final ctrl = _subs.remove(subscriptionId);
        if (ctrl != null && !ctrl.isClosed) {
          ctrl
            ..addError(e)
            ..close();
        }
      },
    );
    return controller.stream;
  }

  Future<void> _ensureConnected() async {
    if (_disposed) throw StateError('connection disposed');
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
    channel.stream.listen(
      _onMessage,
      onDone: _onWsClosed,
      onError: (_) => _onWsClosed(),
      cancelOnError: false,
    );
    _sendClient(AuthMsg(credential: _credential ?? ''));
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
      // subscriptions can flush.
      final ac = _authComplete;
      if (ac != null && !ac.isCompleted) ac.complete();
      return;
    }
    if (type == 'subscription_denied' || type == 'error') {
      final subId = json['subscriptionId'] as String?;
      if (subId != null) {
        _subs[subId]?.addError(
          'subscription_denied: ${json['reason'] ?? json['message']}',
        );
        _subs.remove(subId)?.close();
      }
      return;
    }
    if (type == 'stale_data') {
      // Connection-scoped notification: the server's AuthzWatcher tells
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
    final ctrl = _subs[subId];
    if (ctrl != null) {
      ctrl.add(UpdateCodec.decode(json));
    }
  }

  void _onWsClosed() {
    // Capture the close code BEFORE nulling _channel: the server-side
    // AuthzWatcher signals mid-session auth-revocations via WS close
    // frames (4001 auth_rejected on credential rejection at connect-
    // time, 4003 permissions_changed on mid-session role_unassigned /
    // permission_revoked), and we must route those into
    // RemoteAuthSession.onAuthRejected so the session flips to
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
      onAuthClose?.call();
    }
    for (final ctrl in _subs.values) {
      ctrl.addError('wire_disconnected');
    }
    // Reconnect on next openSubscription / explicit reconnect call.
    // Baseline behavior: subs error out; client decides whether to retry.
    // Full exponential-backoff reconnect deferred to e2e/reconnect_test.dart.
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
        _channel?.sink.close(1000, 'normal');
        _channel = null;
      });
    }
  }

  Future<void> dispose() async {
    _disposed = true;
    _idleCloseTimer?.cancel();
    // Fail any in-flight handshake so awaiters don't hang past dispose.
    final ac = _authComplete;
    if (ac != null && !ac.isCompleted) {
      ac.completeError(StateError('connection disposed'));
    }
    _authComplete = null;
    // Snapshot controllers before close: onCancel callbacks mutate
    // _subs via _closeSubscription, so we cannot iterate _subs directly.
    final controllers = _subs.values.toList();
    _subs.clear();
    for (final ctrl in controllers) {
      await ctrl.close();
    }
    await _channel?.sink.close(1000, 'normal');
    _channel = null;
    _httpClient.close();
  }
}
