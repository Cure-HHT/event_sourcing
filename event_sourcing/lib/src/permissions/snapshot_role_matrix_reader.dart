// lib/src/permissions/snapshot_role_matrix_reader.dart
// Implements: EVS-PRD-permissions-as-events/B — reads authorization data
//   from a PermissionSnapshot derived from the event-log projection;
//   decisions are made solely from event-derived state, not from any
//   external authority.
// Implements: EVS-PRD-permissions-as-events/C — the snapshot is itself a
//   serialized view of log-derived state; reconstructability is preserved
//   because any snapshot can be reproduced by replaying the event log.

import 'package:event_sourcing/event_sourcing.dart';

class SnapshotRoleMatrixReader implements RoleMatrixReader {
  const SnapshotRoleMatrixReader(this._snapshot);
  final PermissionSnapshot _snapshot;

  @override
  Future<bool> isGranted(String role, String permissionName) async {
    if (role != _snapshot.role) return false;
    return _snapshot.grants.any((p) => p.name == permissionName);
  }

  @override
  Future<Set<Permission>> grantsForRole(String role) async {
    return role == _snapshot.role ? _snapshot.grants : const <Permission>{};
  }
}
