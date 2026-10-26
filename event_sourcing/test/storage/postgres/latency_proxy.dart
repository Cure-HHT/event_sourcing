// A TCP relay that adds a fixed delay to every chunk it forwards, in each
// direction, so a test can open a Postgres database as a process in one
// cloud region opens a managed database: with milliseconds on every round
// trip rather than the microseconds of a database on the same host. This
// file declares no tests, so it carries no citation.

import 'dart:async';
import 'dart:io';

/// Listens on a port of its own and relays each connection to
/// [targetHost]:[targetPort], delaying every chunk by [latency] in each
/// direction, in order.
class LatencyProxy {
  LatencyProxy._(this._server, this.targetHost, this.targetPort, this.latency);

  final ServerSocket _server;
  final String targetHost;
  final int targetPort;
  final Duration latency;
  final List<Socket> _open = <Socket>[];

  /// The port the proxy listens on, on the loopback interface.
  int get port => _server.port;

  static Future<LatencyProxy> start({
    required String targetHost,
    required int targetPort,
    required Duration latency,
  }) async {
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final proxy = LatencyProxy._(server, targetHost, targetPort, latency);
    server.listen(proxy._accept);
    return proxy;
  }

  /// [url] with its host and port replaced by this proxy's.
  String rewrite(String url) => Uri.parse(
    url,
  ).replace(host: InternetAddress.loopbackIPv4.address, port: port).toString();

  Future<void> _accept(Socket client) async {
    final Socket upstream;
    try {
      upstream = await Socket.connect(targetHost, targetPort);
    } on Object {
      await client.close();
      return;
    }
    _open.addAll(<Socket>[client, upstream]);
    _relay(client, upstream);
    _relay(upstream, client);
  }

  void _relay(Socket from, Socket to) {
    var chain = Future<void>.value();
    from.listen(
      (data) {
        chain = chain
            .then((_) => Future<void>.delayed(latency))
            .then((_) => to.add(data));
      },
      onDone: () {
        chain = chain.then((_) => to.close()).catchError((_) {});
      },
      onError: (Object _) {
        chain = chain.then((_) => to.close()).catchError((_) {});
      },
      cancelOnError: true,
    );
  }

  Future<void> close() async {
    await _server.close();
    for (final socket in _open) {
      socket.destroy();
    }
    _open.clear();
  }
}
