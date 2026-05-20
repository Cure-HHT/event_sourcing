// Verifies: EVS-PRD-action-dispatch/B (AuthorizationDecision sealed type: Allow falls through; Deny short-circuits with permission + reason)
// Verifies: EVS-PRD-permissions-as-events/B (decision type is the output of isPermitted, which evaluates from event-derived projections)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:test/test.dart';

void main() {
  group('AuthorizationDecision', () {
    test('Allow is a const singleton-style variant', () {
      const a1 = Allow();
      const a2 = Allow();
      expect(a1, isA<Allow>());
      expect(
        identical(a1, a2),
        isTrue,
        reason: 'const Allow should canonicalize',
      );
    });

    test('Deny carries permission + reason', () {
      const d = Deny(
        permission: Permission('user.invite'),
        reason: DenyReason.notGranted,
      );
      expect(d.permission.name, 'user.invite');
      expect(d.reason, DenyReason.notGranted);
    });

    test('sealed switch is exhaustive across both variants', () {
      const AuthorizationDecision d = Allow();
      final desc = switch (d) {
        Allow() => 'allow',
        Deny() => 'deny',
      };
      expect(desc, 'allow');
    });

    test('DenyReason has two values', () {
      expect(DenyReason.values, hasLength(2));
      expect(DenyReason.values.toSet(), {
        DenyReason.notGranted,
        DenyReason.scopeUnresolvable,
      });
    });
  });
}
