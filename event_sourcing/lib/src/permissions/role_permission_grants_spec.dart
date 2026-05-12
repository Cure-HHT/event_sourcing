// lib/src/permissions/role_permission_grants_spec.dart
// Declares the TableProjectionSpec for the role_permission_grants view.
// Implements: EVS-PRD-permissions-as-events (closed-under-events trust model) — the
// permission matrix is a first-class projection of the event log, not an
// external config store.
//
// Row-layout contract (consumed by MaterializedViewRoleMatrixReader):
//   key: event.aggregateId  (e.g. 'admin:user.invite')
//   columns: {'role', 'permissionName', 'scope'}  — WholePayload of a
//     permission_granted event, whose payload is PermissionGrantedPayload.
//
// Deviation from plan: AggregateIdKey() is used instead of the plan's
// CompositeKey(['data.role', 'data.permission', 'data.scope']) because:
//   1. The payload field is 'permissionName', not 'permission' (plan typo).
//   2. PermissionRevokedPayload carries no 'scope' field, so a three-segment
//      CompositeKey would throw on permission_revoked events.
//   3. The existing row key format ('admin:user.invite') is consumed by
//      EventSeedApplier; a key-format change would break that reader.
// AggregateIdKey() preserves the existing row key unchanged.
//
// aggregateTypes filter added to match the old materializer's appliesTo
// guard (aggregateType == 'role_permission_grant'), so unrelated events
// that happen to carry eventType 'permission_granted' are not processed.

import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';

final rolePermissionGrantsSpec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: const SubscriptionFilter(
    eventTypes: {'permission_granted', 'permission_revoked'},
    aggregateTypes: {'role_permission_grant'},
  ),
  insertEventTypes: const {'permission_granted'},
  removeEventTypes: const {'permission_revoked'},
  rowKey: const AggregateIdKey(),
  rowData: const WholePayload(),
);
