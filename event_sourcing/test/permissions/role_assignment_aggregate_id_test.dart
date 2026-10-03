import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('computeRoleAssignmentAggregateId', () {
    // Verifies: EVS-PRD-scoped-permissions/C, EVS-DEV-role-assignment-aggregate-id/A
    test('encodes a bound scope as canonical JSON', () {
      final id = computeRoleAssignmentAggregateId(
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

    // Verifies: EVS-DEV-role-assignment-aggregate-id/A
    test('encodes a value-wildcard scope', () {
      final id = computeRoleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const ValueWildcardScope(class_: 'site'),
      );
      expect(id, contains('"wildcard_value":true'));
      expect(id, contains('"class":"site"'));
    });

    // Verifies: EVS-DEV-role-assignment-aggregate-id/A
    test('encodes a total wildcard scope', () {
      final id = computeRoleAssignmentAggregateId(
        userId: 'U2',
        role: 'ADMIN',
        scope: const TotalWildcardScope(),
      );
      expect(id, contains('"wildcard_class":true'));
    });

    // Verifies: EVS-DEV-role-assignment-aggregate-id/B+C
    test('distinct tuples produce distinct ids', () {
      final a = computeRoleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A:B'),
      );
      final b = computeRoleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site-X', value: 'A'),
      );
      expect(a, isNot(equals(b)));
    });

    // Verifies: EVS-PRD-scoped-permissions/C, EVS-DEV-role-assignment-aggregate-id/B
    test('same tuple produces identical id', () {
      final a = computeRoleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      final b = computeRoleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      expect(a, equals(b));
    });
  });
}
