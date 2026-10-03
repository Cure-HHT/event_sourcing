// test/permissions/fail_safe_authorization_policy_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('FailSafeAuthorizationPolicy', () {
    test('isPermitted always denies', () async {
      const policy = FailSafeAuthorizationPolicy();
      final principal = Principal.user(
        userId: 'U',
        roles: const {'r'},
        activeRole: 'r',
      );
      final d = await policy.isPermitted(
        principal,
        const Permission('any'),
        null,
      );
      expect(d, isA<Deny>());
    });

    test(
      'effectivePermissionsFor returns empty EffectiveAuthorization',
      () async {
        const policy = FailSafeAuthorizationPolicy();
        final principal = Principal.user(
          userId: 'U',
          roles: const {'r'},
          activeRole: 'r',
        );
        final ea = await policy.effectivePermissionsFor(principal);
        expect(ea.rolePermissions, isEmpty);
        expect(ea.scopeAssignments, isEmpty);
      },
    );
  });
}
