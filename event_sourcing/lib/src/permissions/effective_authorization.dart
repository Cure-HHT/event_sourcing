// Implements: EVS-PRD-permissions-as-events (effectivePermissionsFor surface for client-side UI gating)

import 'package:collection/collection.dart';
import 'package:meta/meta.dart';

import '../actions/permission.dart';
import 'scope_assignment.dart';

/// Materials a client needs to gate UI per scope: the active role's
/// permission set plus the user's scope assignments under that role.
/// Apps compose this with their own data projections to build
/// "items I can act on" lists; per-decision gating still goes through
/// `AuthorizationPolicy.isPermitted`.
@immutable
class EffectiveAuthorization {
  const EffectiveAuthorization({
    required this.activeRole,
    required this.rolePermissions,
    required this.scopeAssignments,
  });

  final String activeRole;
  final Set<Permission> rolePermissions;
  final List<ScopeAssignment> scopeAssignments;

  static const EffectiveAuthorization empty = EffectiveAuthorization(
    activeRole: '',
    rolePermissions: <Permission>{},
    scopeAssignments: <ScopeAssignment>[],
  );

  @override
  bool operator ==(Object other) {
    if (identical(this, other)) return true;
    return other is EffectiveAuthorization &&
        activeRole == other.activeRole &&
        const SetEquality<Permission>().equals(
          rolePermissions,
          other.rolePermissions,
        ) &&
        const ListEquality<ScopeAssignment>().equals(
          scopeAssignments,
          other.scopeAssignments,
        );
  }

  @override
  int get hashCode => Object.hash(
    activeRole,
    const SetEquality<Permission>().hash(rolePermissions),
    const ListEquality<ScopeAssignment>().hash(scopeAssignments),
  );
}
