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

  String? _credential;
  bool _disposed = false;

  WebSocketChannel? _channel;
  Future<void>? _connecting;
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
    _ensureConnected().then((_) {
      _sendClient(
        SubscribeMsg(
          subscriptionId: subscriptionId,
          viewName: viewName,
          filter: filter,
          aggregates: aggregates,
        ),
      );
    });
    return controller.stream;
  }

  Future<void> _ensureConnected() async {
    if (_disposed) throw StateError('connection disposed');
    if (_channel != null) return;
    _connecting ??= _connect();
    await _connecting;
    _connecting = null;
  }

  Future<void> _connect() async {
    final channel = wsFactory(wsUrl);
    _channel = channel;
    channel.stream.listen(
      _onMessage,
      onDone: _onWsClosed,
      onError: (_) => _onWsClosed(),
      cancelOnError: false,
    );
    _sendClient(AuthMsg(credential: _credential ?? ''));
  }

  void _sendClient(ClientMessage m) {
    _channel?.sink.add(jsonEncode(SubscriptionMessages.encodeClient(m)));
  }

  void _onMessage(dynamic raw) {
    final json = jsonDecode(raw as String) as Map<String, Object?>;
    final type = json['type'] as String?;
    if (type == 'auth_ok') return;
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
