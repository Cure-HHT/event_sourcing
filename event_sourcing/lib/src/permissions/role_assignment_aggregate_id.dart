// Implements: EVS-PRD-permissions-as-events (aggregate-id for role_assigned / role_unassigned events)

import 'package:canonical_json_jcs/canonical_json_jcs.dart';

import '../actions/scope_value.dart';

/// Canonical-JSON encoding of the (user_id, role, scope) tuple. Used as
/// the aggregate id for `role_assigned` and `role_unassigned` events so
/// that the projection's insert/remove discipline keys per-tuple
/// uniqueness without segment-encoding ambiguity.
String roleAssignmentAggregateId({
  required String userId,
  required String role,
  required ScopeValue scope,
}) {
  final m = <String, Object?>{
    'user_id': userId,
    'role': role,
    'scope': scope.toJson(),
  };
  return canonicalize(m);
}
