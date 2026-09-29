// Verifies: EVS-PRD-permission-source/C
// server side of
//   GET /permissions/snapshot that RemotePermissionSource fetches.
// Verifies: EVS-PRD-cross-process-event-transport/A
// EffectiveAuthorization
//   codec round-trip through the route.
// Verifies: EVS-DEV-converging-view-reads/H
// a ViewConvergingRefusal
//   from the policy answers 503 + view_converging body + Retry-After,
//   not an untyped 500.

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
    Transaction? txn,
  }) async => snapshot;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async => const Allow();
}

void main() {
  test('returns 200 + EffectiveAuthorization JSON', () async {
    final stub = _StubPolicy(
      EffectiveAuthorization(
        activeRole: 'install',
        rolePermissions: {const Permission('greet.send')},
        scopeAssignments: const [],
      ),
    );
    final handler = permissionSnapshotHandler(policy: stub);
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
    final handler = permissionSnapshotHandler(policy: stub);
    final req = Request('GET', Uri.parse('http://x/permissions/snapshot'));
    final res = await handler(req);
    expect(res.statusCode, 500);
  });

  test('returns 503 + view_converging body + Retry-After when the policy '
      'throws ViewConvergingRefusal', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    final handler = permissionSnapshotHandler(
      policy: _ThrowingPolicy(const ViewConvergingRefusal('user_role_scopes')),
    );
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
    expect(res.statusCode, 503);
    expect(res.headers['retry-after'], isNotNull);
    final body = jsonDecode(await res.readAsString()) as Map<String, Object?>;
    expect(body['error'], 'view_converging');
    expect(body['view'], 'user_role_scopes');
  });
}

class _ThrowingPolicy implements AuthorizationPolicy {
  _ThrowingPolicy(this.error);
  final Exception error;

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async => throw error;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async => const Allow();
}
