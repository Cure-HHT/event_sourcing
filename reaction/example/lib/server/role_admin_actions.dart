// reaction/example/lib/server/role_admin_actions.dart
//
// In-app admin actions for managing user-role-scope assignments. These
// are the production-shaped equivalents of the demo's /admin/revoke
// HTTP escape hatch: they go through the action pipeline, so the
// admin's intent is permission-gated (Permission('assign_role') /
// Permission('unassign_role')), idempotency-tracked, and audited via
// dispatch denial events the same as any other action.
//
// The actions emit `role_assigned` / `role_unassigned` events with the
// substrate's standard payloads — the `user_role_scopes` projection
// indexes them automatically and the AuthzWatcher reacts to them
// exactly like seed-time assignments (force-logout on unassign, push
// stale_data on assign).

import 'package:event_sourcing/event_sourcing.dart';

/// Input shape for [AssignRoleAction] / [UnassignRoleAction]:
/// `{user_id, role, scope}` where `scope` is a sealed-variant
/// ScopeValue JSON (`BoundScope`, `ValueWildcardScope`, or
/// `TotalWildcardScope`).
class RoleAssignmentInput {
  const RoleAssignmentInput({
    required this.userId,
    required this.role,
    required this.scope,
  });

  final String userId;
  final String role;
  final ScopeValue scope;

  static RoleAssignmentInput parse(Map<String, Object?> raw) {
    final user = raw['user_id'];
    final role = raw['role'];
    final scopeJson = raw['scope'];
    if (user is! String) {
      throw FormatException("'user_id' must be a string, got: $user");
    }
    if (role is! String) {
      throw FormatException("'role' must be a string, got: $role");
    }
    if (scopeJson is! Map) {
      throw FormatException("'scope' must be an object, got: $scopeJson");
    }
    return RoleAssignmentInput(
      userId: user,
      role: role,
      scope: ScopeValue.fromJson(scopeJson.cast<String, Object?>()),
    );
  }
}

/// Empty result body — clients care about the dispatch outcome
/// (DispatchSuccess vs DispatchAuthorizationDenied), not a returned
/// value. The substrate stamps the actual `role_assigned` event into
/// the log; the UI picks it up via `user_role_scopes` subscription.
class RoleAdminResult {
  const RoleAdminResult();
  Map<String, Object?> toJson() => const <String, Object?>{};
}

/// Grants a (userId, role, scope) assignment. Permission:
/// `assign_role` (unscoped — admins manage role-grants across all
/// workspaces).
class AssignRoleAction extends Action<RoleAssignmentInput, RoleAdminResult> {
  const AssignRoleAction();

  @override
  String get name => 'assign_role';

  @override
  String get description => 'Assign a (user, role, scope) — admin action.';

  @override
  Set<Permission> get permissions => {const Permission('assign_role')};

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  RoleAssignmentInput parseInput(Map<String, Object?> raw) =>
      RoleAssignmentInput.parse(raw);

  @override
  void validate(RoleAssignmentInput input) {
    if (input.userId.trim().isEmpty) {
      throw ArgumentError.value(input.userId, 'user_id', 'must not be empty');
    }
    if (input.role.trim().isEmpty) {
      throw ArgumentError.value(input.role, 'role', 'must not be empty');
    }
  }

  @override
  Future<ExecutionResult<RoleAdminResult>> execute(
    RoleAssignmentInput input,
    ActionContext ctx,
  ) async {
    return ExecutionResult<RoleAdminResult>(
      result: const RoleAdminResult(),
      events: [
        EventDraft(
          aggregateId: roleAssignmentAggregateId(
            userId: input.userId,
            role: input.role,
            scope: input.scope,
          ),
          aggregateType: 'user_role_scope',
          entryType: 'user_role_scope',
          eventType: 'role_assigned',
          data: RoleAssignedPayload(
            userId: input.userId,
            role: input.role,
            scope: input.scope,
          ).toJson(),
        ),
      ],
    );
  }
}

/// Revokes a (userId, role, scope) assignment. Permission:
/// `unassign_role` (unscoped).
class UnassignRoleAction extends Action<RoleAssignmentInput, RoleAdminResult> {
  const UnassignRoleAction();

  @override
  String get name => 'unassign_role';

  @override
  String get description => 'Unassign a (user, role, scope) — admin action.';

  @override
  Set<Permission> get permissions => {const Permission('unassign_role')};

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  RoleAssignmentInput parseInput(Map<String, Object?> raw) =>
      RoleAssignmentInput.parse(raw);

  @override
  void validate(RoleAssignmentInput input) {
    if (input.userId.trim().isEmpty) {
      throw ArgumentError.value(input.userId, 'user_id', 'must not be empty');
    }
    if (input.role.trim().isEmpty) {
      throw ArgumentError.value(input.role, 'role', 'must not be empty');
    }
  }

  @override
  Future<ExecutionResult<RoleAdminResult>> execute(
    RoleAssignmentInput input,
    ActionContext ctx,
  ) async {
    return ExecutionResult<RoleAdminResult>(
      result: const RoleAdminResult(),
      events: [
        EventDraft(
          aggregateId: roleAssignmentAggregateId(
            userId: input.userId,
            role: input.role,
            scope: input.scope,
          ),
          aggregateType: 'user_role_scope',
          entryType: 'user_role_scope',
          eventType: 'role_unassigned',
          data: RoleUnassignedPayload(
            userId: input.userId,
            role: input.role,
            scope: input.scope,
          ).toJson(),
        ),
      ],
    );
  }
}
