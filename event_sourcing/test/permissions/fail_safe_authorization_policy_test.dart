// test/permissions/fail_safe_authorization_policy_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('FailSafeAuthorizationPolicy', () {
    const policy = FailSafeAuthorizationPolicy(<String>[
      'boot validation failed',
    ]);

    test('isPermitted returns Deny(bootstrapFailure) for any query', () async {
      const p = Principal.user(
        userId: 'u1',
        roles: {'admin'},
        activeRole: 'admin',
        activeSite: 's1',
      );
      final d = await policy.isPermitted(
        p,
        const Permission('user.invite', scope: ScopeClass.global),
      );
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.bootstrapFailure);
    });

    test('permissionsFor returns empty', () async {
      const p = Principal.user(
        userId: 'u1',
        roles: {'admin'},
        activeRole: 'admin',
        activeSite: 's1',
      );
      expect(await policy.permissionsFor(p), isEmpty);
    });

    test('bootstrapErrors are exposed', () {
      expect(policy.bootstrapErrors, <String>['boot validation failed']);
    });

    test('anonymous principal also denied with bootstrapFailure', () async {
      const p = Principal.anonymous();
      final d = await policy.isPermitted(
        p,
        const Permission('user.invite', scope: ScopeClass.global),
      );
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.bootstrapFailure);
    });
  });
}
