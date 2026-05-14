// lib/src/permissions/materialized_view_role_matrix_reader.dart
// Implements: EVS-PRD-permissions-as-events/B — reads authorization data
//   exclusively from the role_permission_grants view maintained by the
//   event-log projection interpreter; no external authority is consulted.
// Implements: EVS-PRD-permissions-as-events/C — queries the projection
//   via StorageBackend's view methods over the role_permission_grants view,
//   whose state is fully reconstructable from the event log alone.

import 'package:event_sourcing/event_sourcing.dart';

class MaterializedViewRoleMatrixReader implements RoleMatrixReader {
  const MaterializedViewRoleMatrixReader(this.backend);
  final StorageBackend backend;

  static const String _viewName = 'role_permission_grants';

  @override
  Future<bool> isGranted(String role, String permissionName) async {
    final rows = await backend.findViewRows(_viewName);
    return rows.any(
      (r) => r['role'] == role && r['permissionName'] == permissionName,
    );
  }

  @override
  Future<Set<Permission>> grantsForRole(String role) async {
    final rows = await backend.findViewRows(_viewName);
    return rows
        .where((r) => r['role'] == role)
        .map((r) => Permission(r['permissionName']! as String))
        .toSet();
  }
}
