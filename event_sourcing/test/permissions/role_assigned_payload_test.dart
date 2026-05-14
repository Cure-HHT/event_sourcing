// Verifies: EVS-PRD-permissions-as-events (role_assigned payload shape)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('RoleAssignedPayload', () {
    test('round-trips a bound scope', () {
      const p = RoleAssignedPayload(
        userId: 'U1',
        role: 'SC',
        scope: BoundScope(class_: 'site', value: 'A'),
      );
      final j = p.toJson();
      expect(j['user_id'], 'U1');
      expect(j['role'], 'SC');
      expect(j['scope'], {'class': 'site', 'value': 'A'});
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('round-trips a value-wildcard scope', () {
      const p = RoleAssignedPayload(
        userId: 'U1',
        role: 'SC',
        scope: ValueWildcardScope(class_: 'site'),
      );
      final j = p.toJson();
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('round-trips a total wildcard scope', () {
      const p = RoleAssignedPayload(
        userId: 'U2',
        role: 'ADMIN',
        scope: TotalWildcardScope(),
      );
      final j = p.toJson();
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('fromJson rejects missing user_id', () {
      expect(
        () => RoleAssignedPayload.fromJson({
          'role': 'r',
          'scope': {'wildcard_class': true},
        }),
        throwsA(anyOf(isA<TypeError>(), isA<FormatException>())),
      );
    });
  });
}
