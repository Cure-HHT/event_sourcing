import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/remote/remote_connection.dart';

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
}
