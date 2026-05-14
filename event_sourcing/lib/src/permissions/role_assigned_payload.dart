// Implements: EVS-PRD-permissions-as-events/A — payload for role_assigned events
// (user-to-role assignment with scope binding, append-only event in the log).
// Implements: EVS-PRD-scoped-permissions/C — role_assigned events bind a
//   user/role to a ScopeValue (sealed-variant JSON) recorded in the log.

import 'package:meta/meta.dart';

import '../actions/scope_value.dart';

@immutable
class RoleAssignedPayload {
  const RoleAssignedPayload({
    required this.userId,
    required this.role,
    required this.scope,
  }) : assert(userId != '', 'userId must not be empty'),
       assert(role != '', 'role must not be empty');

  factory RoleAssignedPayload.fromJson(Map<String, Object?> json) {
    return RoleAssignedPayload(
      userId: json['user_id']! as String,
      role: json['role']! as String,
      scope: ScopeValue.fromJson(
        (json['scope']! as Map).cast<String, Object?>(),
      ),
    );
  }

  final String userId;
  final String role;
  final ScopeValue scope;

  Map<String, Object?> toJson() => <String, Object?>{
    'user_id': userId,
    'role': role,
    'scope': scope.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      other is RoleAssignedPayload &&
      userId == other.userId &&
      role == other.role &&
      scope == other.scope;

  @override
  int get hashCode => Object.hash(userId, role, scope);
}
