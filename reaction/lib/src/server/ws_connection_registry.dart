// Tracks open WS connections by userId so the AuthzWatcher can
// route close-frames and stale_data messages to affected clients.
//
// Package-private; not exported from reaction.dart.
//
// Implements: EVS-DEV-authz-watcher/A/B/C/E — the per-userId index the
//   AuthzWatcher consults when force-closing or sending stale_data;
//   carries the per-connection state separately from the watcher's
//   single substrate subscription.

import 'package:web_socket_channel/web_socket_channel.dart';

/// Tracks open WS connections by userId so the AuthzWatcher can
/// route close-frames and stale_data messages to affected clients.
class WsConnectionRegistry {
  final Map<String, Set<WebSocketChannel>> _byUser = {};

  void register(String userId, WebSocketChannel channel) {
    _byUser.putIfAbsent(userId, () => <WebSocketChannel>{}).add(channel);
  }

  void unregister(String userId, WebSocketChannel channel) {
    final set = _byUser[userId];
    if (set == null) return;
    set.remove(channel);
    if (set.isEmpty) _byUser.remove(userId);
  }

  Set<WebSocketChannel> channelsFor(String userId) =>
      _byUser[userId] ?? const <WebSocketChannel>{};

  Iterable<String> get connectedUserIds => _byUser.keys;
}
