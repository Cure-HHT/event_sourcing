// Verifies: EVS-PRD-action-dispatch/B (DenyAllAuthorizationPolicy satisfies the AuthorizationPolicy interface used in the authorize stage)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/deny_all_authorization_policy.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/permissions/effective_authorization.dart';
import 'package:test/test.dart';

// DenyAllAuthorizationPolicy is convenience scaffolding without a
// canonical REQ — these tests verify its contract as the deny-all
// fallback used by tests and early bootstrap.
void main() {
  group('DenyAllAuthorizationPolicy', () {
    test('returns Deny(notGranted) for an authenticated user', () async {
      const policy = DenyAllAuthorizationPolicy.forTests();
      final p = Principal.user(
        userId: 'u-1',
        roles: const {'Admin'},
        activeRole: 'Admin',
      );
      final result = await policy.isPermitted(
        p,
        const Permission('any.thing'),
        null,
      );
      expect(result, isA<Deny>());
      expect((result as Deny).reason, DenyReason.notGranted);
      expect(result.permission.name, 'any.thing');
    });

    test('returns Deny(notGranted) for anonymous', () async {
      const policy = DenyAllAuthorizationPolicy.forTests();
      const p = Principal.anonymous();
      final result = await policy.isPermitted(
        p,
        const Permission('any.thing'),
        null,
      );
      expect(result, isA<Deny>());
      expect((result as Deny).reason, DenyReason.notGranted);
    });

    test('effectivePermissionsFor returns empty', () async {
      const policy = DenyAllAuthorizationPolicy.forTests();
      final p = Principal.user(
        userId: 'u-1',
        roles: const {'Admin'},
        activeRole: 'Admin',
      );
      expect(
        await policy.effectivePermissionsFor(p),
        equals(EffectiveAuthorization.empty),
      );
    });
  });
}
