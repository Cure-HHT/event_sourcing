import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

/// Shared wire-state across the four Remote* impls of one RemoteScope.
/// Owns:
///   - HTTP client + bearer header injection from current credential.
///   - WebSocket lifecycle (Task 20: lazy connect; close on last unsub
///     + grace period).
///   - Subscription registry (Task 20, keyed by client-chosen UUID v4).
///   - Reconnect loop (Task 20).
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
  // ignore: unused_field
  final Duration _idleGrace;

  String? _credential;
  // ignore: unused_field
  bool _disposed = false;

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

  Future<void> dispose() async {
    _disposed = true;
    _httpClient.close();
    // WS cleanup added in Task 20.
  }
}
