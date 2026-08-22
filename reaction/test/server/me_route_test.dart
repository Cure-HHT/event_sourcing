// Verifies: EVS-PRD-auth-session/A/G
// server returns the validated
//   Principal which the Remote AuthSession exposes as
//   AuthStatus.Authenticated and downstream interfaces consult.
// Verifies: EVS-PRD-cross-process-event-transport/A
// Principal codec
//   round-trip through the GET /me response body.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/me_route.dart';
import 'package:reaction/src/wire/principal_codec.dart';
import 'package:shelf/shelf.dart';

void main() {
  test('returns 200 + Principal JSON when Principal in context', () async {
    final handler = meHandler();
    final req = Request(
      'GET',
      Uri.parse('http://x/me'),
      context: {
        'reaction.principal': UserPrincipal(
          userId: 'u-1',
          roles: {'install'},
          activeRole: 'install',
        ),
      },
    );
    final res = await handler(req);
    expect(res.statusCode, 200);
    final body = jsonDecode(await res.readAsString()) as Map<String, Object?>;
    final decoded = PrincipalCodec.decode(body);
    expect((decoded as UserPrincipal).userId, 'u-1');
  });

  test('returns 500 when no Principal', () async {
    final handler = meHandler();
    final req = Request('GET', Uri.parse('http://x/me'));
    final res = await handler(req);
    expect(res.statusCode, 500);
  });
}
