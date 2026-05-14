// test/permissions/table_backed_authorization_policy_test.dart
// Verifies: EVS-PRD-permissions-as-events/B — TableBackedAuthorizationPolicy
// evaluates all authorization decisions from the event-derived role matrix
// (via RoleMatrixReader); role membership and anonymous principal handling
// are resolved from projection data alone, with no external authority
// consulted.
//
// NOTE: Scope-precondition assertions previously lived here. Task 5 removed
// the legacy ScopeClass{global,site,self} session-precondition switch from
// the policy; Task 17 will reintroduce scope-aware evaluation against a
// ScopeValue dispatched alongside each Permission. Until then this file
// covers only the matrix-lookup contract.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TableBackedAuthorizationPolicy', () {
    const reader = InMemoryRoleMatrixReader(<String, Map<String, Permission>>{
      'admin': <String, Permission>{
        'user.invite': Permission('user.invite'),
        'site.manage': Permission('site.manage', scopeClass: 'site'),
        'profile.read': Permission('profile.read'),
      },
    });
    const policy = TableBackedAuthorizationPolicy(reader);

    test('Allow when role holds permission', () async {
      final p = Principal.user(
        userId: 'u1',
        roles: const {'admin'},
        activeRole: 'admin',
      );
      final d = await policy.isPermitted(p, const Permission('user.invite'));
      expect(d, isA<Allow>());
    });

    test('Deny notGranted when role does not hold permission', () async {
      final p = Principal.user(
        userId: 'u1',
        roles: const {'patient'},
        activeRole: 'patient',
      );
      final d = await policy.isPermitted(p, const Permission('user.invite'));
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.notGranted);
    });

    test('Deny notGranted when anonymous principal (no role)', () async {
      const p = Principal.anonymous();
      final d = await policy.isPermitted(p, const Permission('user.invite'));
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.notGranted);
    });

    test('permissionsFor returns the role grants for a user', () async {
      final p = Principal.user(
        userId: 'u1',
        roles: const {'admin'},
        activeRole: 'admin',
      );
      final perms = await policy.permissionsFor(p);
      expect(perms.map((x) => x.name).toSet(), <String>{
        'user.invite',
        'site.manage',
        'profile.read',
      });
    });

    test('permissionsFor returns empty for anonymous principal', () async {
      const p = Principal.anonymous();
      expect(await policy.permissionsFor(p), isEmpty);
    });
  });
}
