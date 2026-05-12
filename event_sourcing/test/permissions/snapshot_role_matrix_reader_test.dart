// test/permissions/snapshot_role_matrix_reader_test.dart
// (snapshot is principal-scoped — answers false for any other role).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotRoleMatrixReader', () {
    test(
      'isGranted returns true for snapshot role + listed permission',
      () async {
        final snap = PermissionSnapshot(
          role: 'admin',
          grants: <Permission>{
            const Permission('user.invite', scope: ScopeClass.global),
          },
          issuedAt: DateTime(2026),
        );
        final reader = SnapshotRoleMatrixReader(snap);
        expect(await reader.isGranted('admin', 'user.invite'), isTrue);
      },
    );

    test(
      'isGranted returns false for any role other than snapshot.role',
      () async {
        final snap = PermissionSnapshot(
          role: 'admin',
          grants: <Permission>{
            const Permission('user.invite', scope: ScopeClass.global),
          },
          issuedAt: DateTime(2026),
        );
        final reader = SnapshotRoleMatrixReader(snap);
        expect(await reader.isGranted('patient', 'user.invite'), isFalse);
        expect(await reader.grantsForRole('patient'), isEmpty);
      },
    );
  });
}
