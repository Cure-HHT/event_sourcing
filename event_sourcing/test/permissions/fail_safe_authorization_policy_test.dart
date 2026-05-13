// test/permissions/fail_safe_authorization_policy_test.dart
// Verifies: EVS-PRD-permissions-as-events/B — FailSafeAuthorizationPolicy
// denies all requests with DenyReason.notGranted when the event-derived
// projection is unavailable, ensuring no authorization decision is made from
// a corrupt or incomplete projection.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('FailSafeAuthorizationPolicy', () {
    const policy = FailSafeAuthorizationPolicy(<String>[
      'boot validation failed',
    ]);

    test('isPermitted returns Deny(notGranted) for any query', () async {
      const p = Principal.user(
        userId: 'u1',
        roles: {'admin'},
        activeRole: 'admin',
      );
      final d = await policy.isPermitted(p, const Permission('user.invite'));
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.notGranted);
    });

    test('permissionsFor returns empty', () async {
      const p = Principal.user(
        userId: 'u1',
        roles: {'admin'},
        activeRole: 'admin',
      );
      expect(await policy.permissionsFor(p), isEmpty);
    });

    test('bootstrapErrors are exposed', () {
      expect(policy.bootstrapErrors, <String>['boot validation failed']);
    });

    test('anonymous principal also denied with notGranted', () async {
      const p = Principal.anonymous();
      final d = await policy.isPermitted(p, const Permission('user.invite'));
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.notGranted);
    });
  });
}
