// Verifies: EVS-PRD-permission-source/C — server side of
//   GET /permissions/snapshot that RemotePermissionSource fetches.
// Verifies: EVS-PRD-cross-process-event-transport/A — EffectiveAuthorization
//   codec round-trip through the route.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/permission_route.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:shelf/shelf.dart';

class _StubPolicy implements AuthorizationPolicy {
  _StubPolicy(this.snapshot);
  final EffectiveAuthorization snapshot;

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async => snapshot;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async => const Allow();
}

void main() {
  test('returns 200 + EffectiveAuthorization JSON', () async {
    final stub = _StubPolicy(
      EffectiveAuthorization(
        activeRole: 'install',
        rolePermissions: {Permission('greet.send')},
        scopeAssignments: [],
      ),
    );
    final handler = permissionRouteHandler(policy: stub);
    final req = Request(
      'GET',
      Uri.parse('http://x/permissions/snapshot'),
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
    final decoded = EffectiveAuthorizationCodec.decode(body);
    expect(decoded.activeRole, 'install');
    expect(decoded.rolePermissions.first.name, 'greet.send');
  });

  test('returns 500 when no Principal', () async {
    final stub = _StubPolicy(EffectiveAuthorization.empty);
    final handler = permissionRouteHandler(policy: stub);
    final req = Request('GET', Uri.parse('http://x/permissions/snapshot'));
    final res = await handler(req);
    expect(res.statusCode, 500);
  });
}
