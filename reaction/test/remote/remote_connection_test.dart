import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class _FakeHttpClient extends http.BaseClient {
  http.BaseRequest? lastRequest;
  http.Response? response;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    lastRequest = request;
    return http.StreamedResponse(
      Stream.value((response?.bodyBytes) ?? <int>[]),
      response?.statusCode ?? 200,
    );
  }
}

/// In-process pair of [WebSocketChannel]s for tests: anything the
/// `clientSide` sink emits arrives on `serverSide.stream`, and vice
/// versa.
class _Pair {
  _Pair() {
    _clientToServer = StreamController<Object?>();
    _serverToClient = StreamController<Object?>();
    serverSide = _MemChannel(
      stream: _clientToServer.stream,
      rawSink: _serverToClient.sink,
    );
    clientSide = _MemChannel(
      stream: _serverToClient.stream,
      rawSink: _clientToServer.sink,
    );
  }

  late final StreamController<Object?> _clientToServer;
  late final StreamController<Object?> _serverToClient;
  late final WebSocketChannel serverSide;
  late final WebSocketChannel clientSide;

  Future<void> close() async {
    await _clientToServer.close();
    await _serverToClient.close();
  }
}

class _MemChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  _MemChannel({
    required Stream<Object?> stream,
    required StreamSink<Object?> rawSink,
  }) : _stream = stream,
       sink = _MemSink(rawSink);

  final Stream<Object?> _stream;

  @override
  Stream<dynamic> get stream => _stream;

  @override
  final WebSocketSink sink;

  @override
  int? get closeCode => null;

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

void main() {
  test('credential round-trips through setCredential/credential', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.credential, isNull);
    conn.setCredential('alice');
    expect(conn.credential, 'alice');
    conn.setCredential(null);
    expect(conn.credential, isNull);
  });

  test('HTTP requests include bearer header from credential', () async {
    final client = _FakeHttpClient()
      ..response = http.Response(
        '{"kind":"user","userId":"u","activeRole":"i"}',
        200,
      );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    conn.setCredential('alice');
    await conn.httpGet(Uri.parse('http://localhost:1234/me'));
    expect(client.lastRequest!.headers['Authorization'], 'Bearer alice');
  });

  test('HTTP requests omit auth header when credential is null', () async {
    final client = _FakeHttpClient()..response = http.Response('ok', 200);
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    await conn.httpGet(Uri.parse('http://localhost:1234/healthz'));
    expect(client.lastRequest!.headers.containsKey('Authorization'), isFalse);
  });

  test('wsUrl derives ws scheme from http baseUrl', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.wsUrl.scheme, 'ws');
    expect(conn.wsUrl.path, '/subscriptions');
  });

  test('wsUrl derives wss scheme from https baseUrl', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('https://api.example.com'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.wsUrl.scheme, 'wss');
    expect(conn.wsUrl.path, '/subscriptions');
  });

  test(
    'openSubscription returns a stream that receives routed envelopes',
    () async {
      final pair = _Pair();
      addTearDown(pair.close);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
      );
      conn.setCredential('alice');

      // Drain server-inbound traffic so the connection's dispose() can
      // close its sink without hanging on an undrained listener.
      pair.serverSide.stream.listen((_) {});

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );

      // Subscribe before the events arrive.
      final firstFuture = stream.first;

      // Wait a microtask so _ensureConnected/_connect/_sendClient flush.
      await Future<void>.delayed(Duration.zero);

      // Simulate server response: auth_ok then a snapshot envelope.
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'snapshot',
          'subscriptionId': 'sub-1',
          'sequence': 1,
          'value': {'aggregateId': 'a-1', 'k': 'v'},
        }),
      );

      final first = await firstFuture;
      expect(first, isA<Snapshot<Map<String, Object?>>>());
      await conn.dispose();
    },
  );
}
