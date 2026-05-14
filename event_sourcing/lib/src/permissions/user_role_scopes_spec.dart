// lib/src/permissions/user_role_scopes_spec.dart
// Declares the TableProjectionSpec for the user_role_scopes view.
// Implements: EVS-PRD-permissions-as-events/A — user-role-scope assignments
//   are events in the same log as application state changes (role_assigned /
//   role_unassigned are appended alongside every other state transition).
// Implements: EVS-PRD-permissions-as-events/B — the substrate's authorize
//   stage reads user_role_scopes (via TableBackedAuthorizationPolicy) to
//   evaluate scope coverage; no external store participates in the decision.
// Implements: EVS-PRD-permissions-as-events/C — the projection is fully
//   reconstructable from the event log alone; replaying role_assigned /
//   role_unassigned events reproduces the view deterministically.
//
// Row-layout contract (consumed by TableBackedAuthorizationPolicy):
//   key: event.aggregateId — the canonical-JSON encoding of the
//     (user_id, role, scope) tuple produced by roleAssignmentAggregateId().
//   columns: {'user_id', 'role', 'scope'} — WholePayload of a
//     role_assigned event, whose payload is RoleAssignedPayload.
//
// The aggregateTypes filter mirrors role_permission_grants_spec.dart: the
// substrate only projects events whose aggregateType is 'user_role_scope',
// so unrelated events sharing the role_assigned/role_unassigned eventType
// (should any ever exist) cannot pollute the view.

import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';

final userRoleScopesSpec = TableProjectionSpec(
  viewName: 'user_role_scopes',
  interest: const SubscriptionFilter(
    eventTypes: {'role_assigned', 'role_unassigned'},
    aggregateTypes: {'user_role_scope'},
  ),
  insertEventTypes: const {'role_assigned'},
  removeEventTypes: const {'role_unassigned'},
  rowKey: const AggregateIdKey(),
  rowData: const WholePayload(),
);
