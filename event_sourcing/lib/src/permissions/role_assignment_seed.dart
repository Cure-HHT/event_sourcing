// lib/src/permissions/role_assignment_seed.dart
// Implements: EVS-PRD-permissions-as-events — declarative seed shape for
//   user-role-scope assignments (parallels PermissionSeed for grants).
//   bootstrapRoleAssignments realises these as role_assigned events
//   written into the event log.

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:meta/meta.dart';

/// Declarative seed of (user, role, scope) assignments. Each entry maps
/// to one `role_assigned` event when applied by `bootstrapRoleAssignments`
/// against a log that does not yet contain that aggregate id.
@immutable
class RoleAssignmentSeed {
  const RoleAssignmentSeed({required this.entries});

  final List<RoleAssignmentSeedEntry> entries;
}

@immutable
class RoleAssignmentSeedEntry {
  const RoleAssignmentSeedEntry({
    required this.userId,
    required this.role,
    required this.scope,
  }) : assert(userId != '', 'userId must not be empty'),
       assert(role != '', 'role must not be empty');

  final String userId;
  final String role;
  final ScopeValue scope;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is RoleAssignmentSeedEntry &&
          userId == other.userId &&
          role == other.role &&
          scope == other.scope;

  @override
  int get hashCode => Object.hash(userId, role, scope);
}
