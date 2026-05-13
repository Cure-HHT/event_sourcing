// test/permissions/in_memory_role_matrix_reader_test.dart
// Verifies: EVS-PRD-permissions-as-events/B — InMemoryRoleMatrixReader
// correctly answers isGranted and grantsForRole queries from a Map populated
// with event-derived data; unknown roles and absent grants return false/empty.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('InMemoryRoleMatrixReader', () {
    test('empty map answers false / empty set', () async {
      final reader = InMemoryRoleMatrixReader.empty();
      expect(await reader.isGranted('admin', 'user.invite'), isFalse);
      expect(await reader.grantsForRole('admin'), isEmpty);
    });

    test('returns true / non-empty when grant present', () async {
      const reader = InMemoryRoleMatrixReader(<String, Map<String, Permission>>{
        'admin': <String, Permission>{
          'user.invite': Permission('user.invite', scope: ScopeClass.global),
        },
      });
      expect(await reader.isGranted('admin', 'user.invite'), isTrue);
      expect(await reader.isGranted('admin', 'user.delete'), isFalse);
      final grants = await reader.grantsForRole('admin');
      expect(grants, hasLength(1));
      expect(grants.first.name, 'user.invite');
      expect(grants.first.scope, ScopeClass.global);
    });

    test('unknown role answers false / empty', () async {
      const reader = InMemoryRoleMatrixReader(<String, Map<String, Permission>>{
        'admin': <String, Permission>{
          'p': Permission('p', scope: ScopeClass.global),
        },
      });
      expect(await reader.isGranted('patient', 'p'), isFalse);
      expect(await reader.grantsForRole('patient'), isEmpty);
    });
  });
}
