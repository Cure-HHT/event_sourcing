// Verifies: EVS-PRD-auth-session/C — invokes
//   PrincipalAuthValidator.authenticate on the bearer credential and
//   attaches the resulting Principal to the request context.
// Verifies: EVS-PRD-auth-session/E — HTTP 401 on missing/bad credential
//   is the wire signal the Remote AuthSession maps to Expired.
// Verifies: EVS-PRD-cross-process-event-transport/F — bearer credential
//   is the required wire-level authentication carriage.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:shelf/shelf.dart';

void main() {
  late TrustingAuthValidator validator;

  setUp(() {
    validator = TrustingAuthValidator(defaultActiveRole: 'install');
  });

  Response inner(Request req) {
    final principal = principalFromContext(req);
    return Response.ok(
      principal == null ? 'no-principal' : (principal as UserPrincipal).userId,
    );
  }

  test('attaches Principal on valid Bearer header', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(
      Request(
        'GET',
        Uri.parse('http://x/y'),
        headers: {'Authorization': 'Bearer alice'},
      ),
    );
    expect(res.statusCode, 200);
    expect(await res.readAsString(), 'alice');
  });

  test('returns 401 on missing Authorization header', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(Request('GET', Uri.parse('http://x/y')));
    expect(res.statusCode, 401);
  });

  test('returns 401 on non-Bearer Authorization', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(
      Request(
        'GET',
        Uri.parse('http://x/y'),
        headers: {'Authorization': 'Basic xyz'},
      ),
    );
    expect(res.statusCode, 401);
  });

  test('returns 401 on AuthenticationDenied', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(
      Request(
        'GET',
        Uri.parse('http://x/y'),
        headers: {'Authorization': 'Bearer '}, // empty -> denied
      ),
    );
    expect(res.statusCode, 401);
  });
}
