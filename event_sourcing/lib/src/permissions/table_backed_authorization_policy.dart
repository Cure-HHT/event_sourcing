// lib/src/permissions/table_backed_authorization_policy.dart
// Implements: EVS-PRD-permissions-as-events/B — evaluates authorization
//   decisions solely from event-derived projections (role_permission_grants,
//   user_role_scopes, and containment projections via ContainmentResolver).
// Implements: EVS-PRD-permissions-as-events/A — reads grants and assignments
//   that are themselves recorded as events in the same log.
// Implements: EVS-PRD-permissions-as-events/D — this policy IS the substrate's
//   policy code; v1 ships exactly one AuthorizationPolicy implementation
//   (this one, plus FailSafeAuthorizationPolicy for the boot-failure path)
//   and apps do not register alternatives. Alternative policy mechanisms
//   require lib extension under the Append-Only Primitives discipline.
// Implements: EVS-PRD-action-dispatch/B — Allow/Deny decisions delivered to
//   the dispatcher's authorize stage.
// Implements: EVS-PRD-scoped-permissions/D/F/G — evaluates solely from
//   event-derived projections; allows when any active-role assignment
//   matches via equality / wildcard / containment; fails closed on missing
//   containment rows.
// Implements: EVS-DEV-scoped-permissions-match-algorithm — first-match-wins
//   union of active-role assignments; bound, value-wildcard, and total-
//   wildcard variants handled per Section 2 of spec/scoped-permissions.md.
// Implements: EVS-DEV-effective-permissions-shape — effectivePermissionsFor
//   returns EffectiveAuthorization { activeRole, rolePermissions,
//   scopeAssignments }, and EffectiveAuthorization.empty for non-user
//   principals.

import 'package:event_sourcing/event_sourcing.dart';

class TableBackedAuthorizationPolicy implements AuthorizationPolicy {
  TableBackedAuthorizationPolicy({
    required this.backend,
    required this.scopeClassRegistry,
    required this.txnProvider,
  }) : _resolver = ContainmentResolver(
         registry: scopeClassRegistry,
         findRowsInTxn: backend.findViewRowsInTxn,
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
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async {
    // 1. Anonymous principals carry no role assignments.
    if (principal is! UserPrincipal) {
      return Deny(permission: permission, reason: DenyReason.notGranted);
    }

    // 2. Invariants on the (permission, scopeValue) pair. The dispatcher
    // already validates these before calling isPermitted; this is
    // defense-in-depth for callers that invoke the policy directly
    // (tests, app-side gating helpers, etc.). Mismatches surface as
    // scopeUnresolvable rather than silently grant or deny on stale
    // data.
    //
    // (a) presence — scopeValue non-null iff permission.scopeClass non-null.
    if ((permission.scopeClass == null) != (scopeValue == null)) {
      return Deny(permission: permission, reason: DenyReason.scopeUnresolvable);
    }
    // (b) shape — a non-null scopeValue for a scoped permission MUST be a
    // BoundScope whose class_ matches permission.scopeClass. TotalWildcardScope
    // carries no class to match against; ValueWildcardScope from the caller
    // side has no defined semantics in the v1 match algorithm. Either is
    // treated as a programmer bug.
    if (scopeValue != null) {
      if (scopeValue is! BoundScope) {
        return Deny(
          permission: permission,
          reason: DenyReason.scopeUnresolvable,
        );
      }
      if (scopeValue.class_ != permission.scopeClass) {
        return Deny(
          permission: permission,
          reason: DenyReason.scopeUnresolvable,
        );
      }
    }

    // Run the projection reads either in the caller-supplied txn (so
    // they share the read-snapshot with subsequent appends in that tx)
    // or open a fresh one via [txnProvider].
    if (txn != null) {
      return _evaluate(txn, principal, permission, scopeValue);
    }
    return txnProvider(
      (innerTxn) => _evaluate(innerTxn, principal, permission, scopeValue),
    );
  }

  /// Pure projection-read body shared by the txn-injected and
  /// txnProvider-opened paths.
  Future<AuthorizationDecision> _evaluate(
    Txn txn,
    UserPrincipal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async {
    // 3. Role-membership: verify the principal actually holds the claimed
    // activeRole via user_role_scopes. The Principal's `roles` and
    // `activeRole` fields are a user-supplied request ("which hat am I
    // wearing right now"), not a substrate-trusted assertion — the
    // substrate independently derives role-membership from the event log
    // before honouring any permission under that role. This read also
    // doubles as the scope-assignment enumeration consumed by step 6
    // for scoped permissions.
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

    // 4. Role-level grant: does the active role carry this permission name?
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

    // 5. Unscoped permission: verified role-membership + role grant is
    // sufficient.
    if (permission.scopeClass == null) {
      return const Allow();
    }

    // 6. Scoped permission: match the requested scope against the
    // already-fetched assignments (first-match-wins = union semantics).
    final requested = scopeValue!;
    for (final row in assignments) {
      final assignedScope = ScopeValue.fromJson(
        (row['scope']! as Map).cast<String, Object?>(),
      );
      if (await _matches(txn, assigned: assignedScope, requested: requested)) {
        return const Allow();
      }
    }
    return Deny(permission: permission, reason: DenyReason.notGranted);
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

    // The class gate (does the assigned scope's class equal-or-ancestor the
    // requested class?) is the single tested source of truth shared with the
    // read path; see scopeClassMatch in event_sourcing. This method then
    // does its own value-equality / containment resolution on top of it.
    final match = scopeClassMatch(
      assigned,
      requested.class_,
      scopeClassRegistry,
    );
    switch (match) {
      case ScopeClassMatch.doesNotApply:
        return false;
      case ScopeClassMatch.appliesExact:
        switch (assigned) {
          // Total/value wildcard: any value of the exact class matches.
          case TotalWildcardScope():
          case ValueWildcardScope():
            return true;
          case BoundScope(value: final av):
            return av == requested.value;
        }
      case ScopeClassMatch.appliesViaAncestor:
        switch (assigned) {
          // Not reachable for a total wildcard (always appliesExact); handle
          // defensively to keep the switch exhaustive and fail-open-safe.
          case TotalWildcardScope():
            return true;
          // Any value of an ancestor class matches any descendant.
          case ValueWildcardScope():
            return true;
          case BoundScope(class_: final ac, value: final av):
            final resolved = await _resolver.resolve(
              txn: txn,
              from: requested,
              target: ac,
            );
            return resolved?.value == av;
        }
    }
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async {
    if (principal is! UserPrincipal) {
      return EffectiveAuthorization.empty;
    }
    if (txn != null) {
      return _effectiveBody(txn, principal);
    }
    return txnProvider((innerTxn) => _effectiveBody(innerTxn, principal));
  }

  Future<EffectiveAuthorization> _effectiveBody(
    Txn txn,
    UserPrincipal principal,
  ) async {
    // Role-membership gate: if the substrate has no user_role_scopes
    // assignment for (userId, activeRole), the principal does not
    // effectively hold that role — return empty rather than leaking the
    // role's permissions based on the Principal's unverified claim.
    final assignmentRows = await backend.findViewRowsInTxn(
      txn,
      'user_role_scopes',
      where: <String, Object?>{
        'user_id': principal.userId,
        'role': principal.activeRole,
      },
    );
    if (assignmentRows.isEmpty) {
      return EffectiveAuthorization.empty;
    }
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
  }
}
