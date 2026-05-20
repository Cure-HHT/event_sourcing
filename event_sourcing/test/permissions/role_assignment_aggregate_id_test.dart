// Verifies: EVS-PRD-permissions-as-events (aggregate-id encoding is collision-free)
// Verifies: EVS-PRD-scoped-permissions/C — aggregate id deterministically
//   derived from (user_id, role, scope) via canonical JSON.
// Verifies: EVS-DEV-role-assignment-aggregate-id/A/B/C — canonical-JSON
//   encoding shape; distinct tuples yield distinct ids; safe against
//   segment-encoding ambiguity.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('roleAssignmentAggregateId', () {
    test('encodes a bound scope as canonical JSON', () {
      final id = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      // Order is canonical (JCS); spaces normalized.
      expect(
        id,
        '{"role":"SC","scope":{"class":"site","value":"A"},"user_id":"U1"}',
      );
    });

    test('encodes a value-wildcard scope', () {
      final id = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const ValueWildcardScope(class_: 'site'),
      );
      expect(id, contains('"wildcard_value":true'));
      expect(id, contains('"class":"site"'));
    });

    test('encodes a total wildcard scope', () {
      final id = roleAssignmentAggregateId(
        userId: 'U2',
        role: 'ADMIN',
        scope: const TotalWildcardScope(),
      );
      expect(id, contains('"wildcard_class":true'));
    });

    test('distinct tuples produce distinct ids', () {
      final a = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A:B'),
      );
      final b = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site-X', value: 'A'),
      );
      expect(a, isNot(equals(b)));
    });

    test('same tuple produces identical id', () {
      final a = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      final b = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      expect(a, equals(b));
    });
  });
}
