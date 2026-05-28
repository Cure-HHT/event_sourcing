// Shared in-process WS / HTTP fakes for `reaction/test/remote/...` tests
// that need to drive the full client-side lifecycle (initial connect, auth
// handshake, server-initiated drops, reconnect generations) without
// touching a real socket or HTTP server.
//
// Extracted from `auto_reconnect_test.dart`'s self-contained fakes
// (Task 5 of the reaction_widgets implementation plan); Task 6's
// `remote_scope_test.dart` reuses them to drive `RemoteScope`'s
// `connectionStatusStream` end-to-end, and Task 7's cross-impl contract
// test reuses them again. Keep this file dependency-light so any
// `reaction/test/remote/*` file can import it.

import 'dart:async';
import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Stub HTTP client: returns an empty 200 to every request. Sufficient
/// for tests that only exercise the WS path.
class FakeHttpClient extends http.BaseClient {
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async =>
      http.StreamedResponse(Stream.value(<int>[]), 200);
}

/// Factory of in-process WS channel pairs, designed to model real WS
/// lifecycle. Each call to [build] returns a fresh client-side
/// [WebSocketChannel] and a controllable server-side handle
/// ([FakeWsPair]); the factory itself records every pair so the test
/// can interact with the current generation.
///
/// In `failConnects: true` mode, each newly-built pair immediately
/// closes the client side with code 1006 (abnormal closure) before any
/// auth handshake can complete, simulating an unreachable server.
/// Otherwise, the test drives the server-side `acceptAuth()` once the
/// client has flushed its initial auth message.
class FakeWsFactory {
  FakeWsFactory({this.failConnects = false});

  bool failConnects;
  final List<FakeWsPair> pairs = [];

  WebSocketChannel build(Uri _) {
    final pair = FakeWsPair();
    pairs.add(pair);
    if (failConnects) {
      // Simulate "server unreachable": close the client side immediately
      // with 1006, after a microtask so the listener gets wired first.
      scheduleMicrotask(() => pair.serverCloseClient(1006));
    }
    return pair.clientSide;
  }

  /// The most-recently-built pair (i.e. the connection the client is
  /// currently using). Throws if none yet built.
  FakeWsPair get latest => pairs.last;
}

/// In-process pair of [WebSocketChannel]s: anything the client's sink
/// emits arrives on `serverInbound`, and `serverSink.add` flows into
/// the client's stream. Modelled on `_Pair` in `remote_connection_test.dart`;
/// the difference is this version is reusable across multiple
/// generations (the factory builds a fresh pair on every reconnect).
class FakeWsPair {
  FakeWsPair() {
    serverSide = _MemChannel(
      stream: _clientToServer.stream,
      rawSink: _serverToClient.sink,
      closeCell: _serverCloseCell,
    );
    clientSide = _MemChannel(
      stream: _serverToClient.stream,
      rawSink: _clientToServer.sink,
      closeCell: _clientCloseCell,
    );
    // Buffer everything the client sends so tests can assert against
    // it after the fact (the production code may flush before the test
    // attaches a listener).
    _serverInboundSub = serverSide.stream.listen(
      (raw) => sentMessages.add(raw as String),
      onDone: _serverInboundDone.complete,
    );
  }

  final StreamController<Object?> _clientToServer = StreamController<Object?>();
  final StreamController<Object?> _serverToClient = StreamController<Object?>();
  final _CloseCodeCell _clientCloseCell = _CloseCodeCell();
  final _CloseCodeCell _serverCloseCell = _CloseCodeCell();
  late final _MemChannel serverSide;
  late final _MemChannel clientSide;
  late final StreamSubscription<dynamic> _serverInboundSub;
  final Completer<void> _serverInboundDone = Completer<void>();

  /// Every JSON message the client has sent on this connection.
  final List<String> sentMessages = [];

  /// Server-initiated close with a specific close code, surfacing on
  /// `clientSide.closeCode` (matching what
  /// `WebSocketChannel.closeCode` would report from a real WS close
  /// frame).
  Future<void> serverCloseClient(int code) async {
    _clientCloseCell.code = code;
    await _serverToClient.close();
  }

  /// Server-side accept: parse the first client message as an auth
  /// envelope and send back the `auth_ok` ack the production code
  /// awaits before flushing queued subscribes.
  Future<void> acceptAuth({String principalId = 'alice'}) async {
    // Wait until the client has actually sent its auth message.
    while (sentMessages.isEmpty) {
      await Future<void>.delayed(Duration.zero);
    }
    final first = jsonDecode(sentMessages.first) as Map<String, Object?>;
    expect(first['type'], 'auth', reason: 'first client message is auth');
    serverSide.sink.add(
      jsonEncode({'type': 'auth_ok', 'principalId': principalId}),
    );
  }

  Future<void> dispose() async {
    await _serverInboundSub.cancel();
    if (!_clientToServer.isClosed) await _clientToServer.close();
    if (!_serverToClient.isClosed) await _serverToClient.close();
  }
}

class _CloseCodeCell {
  int? code;
}

class _MemChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  _MemChannel({
    required Stream<Object?> stream,
    required StreamSink<Object?> rawSink,
    required _CloseCodeCell closeCell,
  }) : _stream = stream,
       _closeCell = closeCell,
       sink = _MemSink(rawSink);

  final Stream<Object?> _stream;
  final _CloseCodeCell _closeCell;

  @override
  Stream<dynamic> get stream => _stream;

  @override
  final WebSocketSink sink;

  @override
  int? get closeCode => _closeCell.code;

  @override
  String? get closeReason => null;

  @override
  String? get protocol => null;

  @override
  Future<void> get ready => Future<void>.value();
}

class _MemSink implements WebSocketSink {
  _MemSink(this._inner);

  final StreamSink<Object?> _inner;

  @override
  void add(Object? event) => _inner.add(event);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _inner.addError(error, stackTrace);

  @override
  Future<dynamic> addStream(Stream<Object?> stream) => _inner.addStream(stream);

  @override
  Future<dynamic> close([int? closeCode, String? closeReason]) =>
      _inner.close();

  @override
  Future<dynamic> get done => _inner.done;
}

/// Helper: settles every pending microtask + scheduled timer so a
/// reconnect loop's `Future.delayed` (with ms-scale intervals) drains
/// without burning test wall-clock. Repeats a handful of times because
/// each reconnect attempt yields control multiple times across the
/// loop -> _connect -> auth handshake chain.
Future<void> pumpEventLoop({int iterations = 20}) async {
  for (var i = 0; i < iterations; i++) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
