// lib/src/permissions/table_backed_authorization_policy.dart
// Implements: EVS-PRD-permissions-as-events/B — evaluates all authorization
//   decisions solely from the event-derived role matrix exposed via
//   RoleMatrixReader (projection-based lookup); no external authority is
//   consulted at decision time.

import 'package:event_sourcing/event_sourcing.dart';

class TableBackedAuthorizationPolicy implements AuthorizationPolicy {
  const TableBackedAuthorizationPolicy(this._reader);
  final RoleMatrixReader _reader;

  // TODO(Task 17): rewrite this policy to consult ScopeClassRegistry and
  // evaluate scope membership against the ScopeValue carried on the action
  // dispatch. For now we only check role-membership in the matrix; the
  // legacy session-precondition switch (global/site/self) has been removed
  // along with Permission.scope, since the new Permission.scopeClass is a
  // free-form name validated by the registry rather than an enum the policy
  // can pattern-match on.
  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission perm,
  ) async {
    if (principal is! UserPrincipal) {
      // AnonymousPrincipal has no role — nothing in the matrix can be
      // granted to them.
      return Deny(permission: perm, reason: DenyReason.notGranted);
    }
    final granted = await _reader.isGranted(principal.activeRole, perm.name);
    return granted
        ? const Allow()
        : Deny(permission: perm, reason: DenyReason.notGranted);
  }

  @override
  Future<Set<Permission>> permissionsFor(Principal principal) async {
    if (principal is! UserPrincipal) {
      return const <Permission>{};
    }
    return _reader.grantsForRole(principal.activeRole);
  }
}
