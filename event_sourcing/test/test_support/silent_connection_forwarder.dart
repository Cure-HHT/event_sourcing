// Test support: a TCP forwarder in front of a Postgres server that can stop
// relaying without closing either socket, so a client sees a network black
// hole while the server session stays alive. This file declares no tests,
// so it carries no citation.
import 'dart:async';
import 'dart:io';

/// A TCP forwarder in front of the Postgres server: it can stop relaying
/// (freeze) without closing either socket, so the client sees a black hole
/// while the server session stays alive.
final class SilentConnectionForwarder {
  SilentConnectionForwarder(this._target);

  final Uri _target;
  late final ServerSocket _server;
  final List<_Pair> _pairs = <_Pair>[];

  /// New connections are accepted and never relayed.
  bool freezeNew = false;

  /// When set, the forwarder relays the server's answer to the next
  /// extended-protocol statement run inside a transaction and then freezes,
  /// so the client's next exchange on that connection (the driver's close of
  /// that statement's portal) meets a black hole. It freezes every
  /// connection and every new one, or, with [freezeOnlyAnsweringConnection],
  /// only the connection that carried the answer.
  bool freezeAfterStatementInTransaction = false;

  /// Makes [freezeAfterStatementInTransaction] freeze only the connection
  /// that carried the answer, and leave new connections relayed.
  bool freezeOnlyAnsweringConnection = false;

  /// Completes when [freezeAfterStatementInTransaction] has frozen.
  Future<void> get frozenAfterStatement => _frozenAfterStatement.future;
  final Completer<void> _frozenAfterStatement = Completer<void>();

  /// Connections accepted so far.
  int accepted = 0;

  int get port => _server.port;

  /// [url] with its host and port replaced by this forwarder's.
  String route(String url) => Uri.parse(
    url,
  ).replace(host: InternetAddress.loopbackIPv4.address, port: port).toString();

  Future<void> start() async {
    _server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    _server.listen((client) async {
      accepted += 1;
      final pair = _Pair(client);
      _pairs.add(pair);
      if (freezeNew) {
        pair.frozen = true;
        client.listen((_) {}, onError: (Object _) {});
        return;
      }
      final upstream = await Socket.connect(
        _target.host,
        _target.hasPort ? _target.port : 5432,
      );
      pair.upstream = upstream;
      client.listen(
        (data) {
          if (!pair.frozen) upstream.add(data);
        },
        onError: (Object _) {},
        onDone: () {
          if (!pair.frozen) upstream.destroy();
        },
      );
      upstream.listen(
        (data) {
          if (pair.frozen) return;
          client.add(data);
          if (freezeAfterStatementInTransaction &&
              _answersStatementInTransaction(data)) {
            freezeAfterStatementInTransaction = false;
            if (freezeOnlyAnsweringConnection) {
              pair.frozen = true;
            } else {
              freeze();
              freezeNew = true;
            }
            _frozenAfterStatement.complete();
          }
        },
        onError: (Object _) {},
        onDone: () {
          if (!pair.frozen) client.destroy();
        },
      );
    });
  }

  /// Stops relaying on every connection accepted so far.
  void freeze() {
    for (final p in _pairs) {
      p.frozen = true;
    }
  }

  /// True when [data] is a server answer to an extended-protocol statement,
  /// opening with ParseComplete (`1`) and ending with a ReadyForQuery that
  /// reports an open transaction: `Z`, length 5, `T`. The answer to a
  /// simple-protocol statement (a transaction's `BEGIN` that the driver
  /// sends) opens otherwise and is followed by no portal close, and a
  /// CloseComplete (`3`) answers a portal's close.
  static bool _answersStatementInTransaction(List<int> data) {
    const end = <int>[0x5A, 0, 0, 0, 5, 0x54];
    if (data.length <= end.length || data.first != 0x31) return false;
    for (var i = 0; i < end.length; i++) {
      if (data[data.length - end.length + i] != end[i]) return false;
    }
    return true;
  }

  Future<void> close() async {
    await _server.close();
    for (final p in _pairs) {
      p.client.destroy();
      p.upstream?.destroy();
    }
  }
}

final class _Pair {
  _Pair(this.client);
  final Socket client;
  Socket? upstream;
  bool frozen = false;
}
