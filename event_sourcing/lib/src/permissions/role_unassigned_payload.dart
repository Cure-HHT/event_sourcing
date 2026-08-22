// Implements: EVS-PRD-permissions-as-events/A
// payload for role_unassigned events
// (user-to-role unassignment with scope binding, append-only event in the log).
// Implements: EVS-PRD-scoped-permissions/C
// role_unassigned events remove a
//   user/role/scope assignment from the projection, recorded in the log.

import 'package:meta/meta.dart';

import '../actions/scope_value.dart';

@immutable
class RoleUnassignedPayload {
  const RoleUnassignedPayload({
    required this.userId,
    required this.role,
    required this.scope,
  }) : assert(userId != '', 'userId must not be empty'),
       assert(role != '', 'role must not be empty');

  factory RoleUnassignedPayload.fromJson(Map<String, Object?> json) {
    return RoleUnassignedPayload(
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
      other is RoleUnassignedPayload &&
      userId == other.userId &&
      role == other.role &&
      scope == other.scope;

  @override
  int get hashCode => Object.hash(userId, role, scope);
}
