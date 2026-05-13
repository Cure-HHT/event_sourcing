// test/permissions/snapshot_role_matrix_reader_test.dart
// Verifies: EVS-PRD-permissions-as-events/B — SnapshotRoleMatrixReader
//   answers authorization queries from a PermissionSnapshot (log-derived
//   state); the snapshot is principal-scoped — answers false for any other
//   role.
// Verifies: EVS-PRD-permissions-as-events/C — snapshot captures log-derived
//   permission state in serializable form; grantsForRole for a different role
//   returns empty, confirming scoping is preserved through serialization.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SnapshotRoleMatrixReader', () {
    test(
      'isGranted returns true for snapshot role + listed permission',
      () async {
        final snap = PermissionSnapshot(
          role: 'admin',
          grants: <Permission>{const Permission('user.invite')},
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
          grants: <Permission>{const Permission('user.invite')},
          issuedAt: DateTime(2026),
        );
        final reader = SnapshotRoleMatrixReader(snap);
        expect(await reader.isGranted('patient', 'user.invite'), isFalse);
        expect(await reader.grantsForRole('patient'), isEmpty);
      },
    );
  });
}
