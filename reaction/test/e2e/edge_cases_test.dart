import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  test('malformed JSON to /actions returns 400', () async {
    final h = await ReactionRemoteTestHarness.open();
    final res = await http.post(
      Uri.parse('http://127.0.0.1:${h.httpServer.port}/actions'),
      headers: {
        'Authorization': 'Bearer alice',
        'Content-Type': 'application/json',
      },
      body: '{not json',
    );
    expect(res.statusCode, 400);
    await h.close();
  });

  test('missing Authorization on /actions returns 401', () async {
    final h = await ReactionRemoteTestHarness.open();
    final res = await http.post(
      Uri.parse('http://127.0.0.1:${h.httpServer.port}/actions'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode({'actionName': 'x', 'rawInput': <String, Object?>{}}),
    );
    expect(res.statusCode, 401);
    await h.close();
  });

  test('dispose mid-flight does not hang', () async {
    final h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await expectLater(h.close(), completes);
  });
}
