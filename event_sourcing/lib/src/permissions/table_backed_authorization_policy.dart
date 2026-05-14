// lib/src/permissions/table_backed_authorization_policy.dart
// Implements: EVS-PRD-permissions-as-events/B — evaluates authorization
//   decisions solely from event-derived projections (role_permission_grants,
//   user_role_scopes, and containment projections via ContainmentResolver).
// Implements: EVS-PRD-permissions-as-events/A — reads grants and assignments
//   that are themselves recorded as events in the same log.
// Implements: EVS-PRD-action-dispatch/B — Allow/Deny decisions delivered to
//   the dispatcher's authorize stage.

import 'package:event_sourcing/event_sourcing.dart';

class TableBackedAuthorizationPolicy implements AuthorizationPolicy {
  TableBackedAuthorizationPolicy({
    required this.backend,
    required this.scopeClassRegistry,
    required this.txnProvider,
  }) : _resolver = ContainmentResolver(
         registry: scopeClassRegistry,
         // FindRowsInTxn declares its txn param as `Object` (so the
         // resolver doesn't have to leak the Txn type); the backend's
         // findViewRowsInTxn takes a concrete `Txn`. Wrap to bridge the
         // contravariance: every caller of FindRowsInTxn inside the
         // resolver passes a Txn anyway (the policy hands its own
         // transaction down), so the downcast is sound.
         findRowsInTxn:
             (
               Object txn,
               String viewName, {
               Map<String, Object?>? where,
               int? limit,
               int? offset,
             }) => backend.findViewRowsInTxn(
               txn as Txn,
               viewName,
               where: where,
               limit: limit,
               offset: offset,
             ),
       );

  final StorageBackend backend;
  final ScopeClassRegistry scopeClassRegistry;

  /// In production the dispatcher passes the active storage transaction
  /// (so authorize + execute run inside the same backend transaction).
  /// Tests can pass a one-shot supplier that opens a tx per call:
  /// `<T>(fn) => backend.transaction<T>(fn)`.
  final Future<T> Function<T>(Future<T> Function(Txn txn)) txnProvider;

  final ContainmentResolver _resolver;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async {
    // 1. Anonymous principals carry no role assignments.
    if (principal is! UserPrincipal) {
      return Deny(permission: permission, reason: DenyReason.notGranted);
    }

    // 2. Invariant: scopeValue non-null iff permission.scopeClass non-null.
    // Mismatch is a programmer bug (Action.scopeFor disagrees with
    // Permission.scopeClass) — surface it as scopeUnresolvable rather
    // than silently grant or deny on stale data.
    if ((permission.scopeClass == null) != (scopeValue == null)) {
      return Deny(permission: permission, reason: DenyReason.scopeUnresolvable);
    }

    return txnProvider((txn) async {
      // 3. Role-level grant: does the active role carry this permission name?
      final grants = await backend.findViewRowsInTxn(
        txn,
        'role_permission_grants',
        where: <String, Object?>{
          'role': principal.activeRole,
          'permissionName': permission.name,
        },
        limit: 1,
      );
      if (grants.isEmpty) {
        return Deny(permission: permission, reason: DenyReason.notGranted);
      }

      // 4. Unscoped permission: role grant is sufficient.
      if (permission.scopeClass == null) {
        return const Allow();
      }

      // 5. Scoped permission: enumerate user's assignments under active role.
      final assignments = await backend.findViewRowsInTxn(
        txn,
        'user_role_scopes',
        where: <String, Object?>{
          'user_id': principal.userId,
          'role': principal.activeRole,
        },
      );
      if (assignments.isEmpty) {
        return Deny(permission: permission, reason: DenyReason.notGranted);
      }

      // 6. Match (first-match-wins = union semantics over the assignments).
      final requested = scopeValue!;
      for (final row in assignments) {
        final assignedScope = ScopeValue.fromJson(
          (row['scope']! as Map).cast<String, Object?>(),
        );
        if (await _matches(
          txn,
          assigned: assignedScope,
          requested: requested,
        )) {
          return const Allow();
        }
      }
      return Deny(permission: permission, reason: DenyReason.notGranted);
    });
  }

  /// Variant switch for one assignment vs. the requested scope.
  ///
  /// - TotalWildcardScope assignment matches everything.
  /// - ValueWildcardScope(class=A) matches any requested BoundScope whose
  ///   class is A or whose class has A as an ancestor.
  /// - BoundScope(class=A, value=V) matches a requested BoundScope with
  ///   the same (class, value), OR matches a descendant class whose
  ///   containment resolves to V at class A.
  Future<bool> _matches(
    Txn txn, {
    required ScopeValue assigned,
    required ScopeValue requested,
  }) async {
    // The dispatcher contract is that requested scopes from action.scopeFor
    // are always BoundScope (concrete subjects). Defensive bail-out keeps
    // a future change here fail-closed rather than overgranting.
    if (requested is! BoundScope) return false;

    switch (assigned) {
      case TotalWildcardScope():
        return true;
      case ValueWildcardScope(class_: final ac):
        if (ac == requested.class_) return true;
        if (scopeClassRegistry.isAncestor(ac, requested.class_)) {
          // Any value of an ancestor class matches any descendant.
          return true;
        }
        return false;
      case BoundScope(class_: final ac, value: final av):
        if (ac == requested.class_ && av == requested.value) return true;
        if (scopeClassRegistry.isAncestor(ac, requested.class_)) {
          final resolved = await _resolver.resolve(
            txn: txn,
            from: requested,
            target: ac,
          );
          return resolved?.value == av;
        }
        return false;
    }
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal,
  ) async {
    if (principal is! UserPrincipal) {
      return EffectiveAuthorization.empty;
    }
    return txnProvider((txn) async {
      final grants = await backend.findViewRowsInTxn(
        txn,
        'role_permission_grants',
        where: <String, Object?>{'role': principal.activeRole},
      );
      final perms = <Permission>{
        // Permission grants don't store scopeClass (it's a code-registered
        // attribute on the Permission definition, not per-grant data). Apps
        // that need full scopeClass info look up against their own Permission
        // registry; this surface is for UI gating, where matching by name
        // is sufficient.
        for (final g in grants) Permission(g['permissionName']! as String),
      };
      final assignmentRows = await backend.findViewRowsInTxn(
        txn,
        'user_role_scopes',
        where: <String, Object?>{
          'user_id': principal.userId,
          'role': principal.activeRole,
        },
      );
      final assignments = <ScopeAssignment>[
        for (final r in assignmentRows)
          ScopeAssignment(
            scope: ScopeValue.fromJson(
              (r['scope']! as Map).cast<String, Object?>(),
            ),
          ),
      ];
      return EffectiveAuthorization(
        activeRole: principal.activeRole,
        rolePermissions: perms,
        scopeAssignments: assignments,
      );
    });
  }
}
