// reaction/example/test/server_smoke_test.dart
//
// Boots `bootstrap()` in-process onto a real shelf HTTP server bound
// to an ephemeral port, hits /me with and without auth, and asserts the
// expected 401 / 200 + Principal JSON shape. Keeps the demo from
// bit-rotting as the library evolves.

import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction_example/server/bootstrap.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;

void main() {
  test(
    'bootstrap() composes a server that gates /me with bearer auth',
    () async {
      final result = await bootstrap();
      final httpServer = await shelf_io.serve(
        result.router.call,
        '127.0.0.1',
        0,
      );
      addTearDown(() async {
        await httpServer.close(force: true);
        await result.dispose();
      });

      final baseUrl = Uri.parse(
        'http://${httpServer.address.host}:${httpServer.port}',
      );

      // No Authorization header → 401.
      final unauthRes = await http.get(baseUrl.replace(path: '/me'));
      expect(unauthRes.statusCode, 401);

      // Bearer token → 200 + Principal JSON.
      final authRes = await http.get(
        baseUrl.replace(path: '/me'),
        headers: <String, String>{'Authorization': 'Bearer alice'},
      );
      expect(authRes.statusCode, 200);
      final body = jsonDecode(authRes.body) as Map<String, Object?>;
      expect(body['userId'], 'alice');
      expect(body['activeRole'], 'editor');
    },
  );
}
