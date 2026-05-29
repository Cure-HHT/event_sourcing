// lib/src/permissions/role_permission_grants_spec.dart
// Declares the TableProjectionSpec for the role_permission_grants view.
// Implements: EVS-PRD-permissions-as-events/A — permission grant and revoke
//   events are recorded in the same event log as application state changes.
// Implements: EVS-PRD-permissions-as-events/B — the library evaluates
//   authorization decisions solely from the event-derived role_permission_grants
//   projection, not from any external authority.
// Implements: EVS-PRD-permissions-as-events/C — the projection is fully
//   reconstructable from the event log alone; replaying permission_granted /
//   permission_revoked events reproduces the view deterministically.
//
// Row-layout contract (consumed by TableBackedAuthorizationPolicy and
// EventSeedApplier):
//   key: event.aggregateId  (e.g. 'admin:user.invite')
//   columns: {'role', 'permissionName'}  — WholePayload of a
//     permission_granted event, whose payload is PermissionGrantedPayload —
//     plus the substrate-stamped 'aggregateId' (== key) and 'sequence'
//     fields that TableFold writes into every row, matching AggregateFold so
//     subscribe()/ViewBuilder consume table and aggregate views uniformly.
//
// AggregateIdKey() is used as the row key because:
//   1. The payload field is 'permissionName' (matching PermissionGrantedPayload).
//   2. The row key format ('admin:user.invite') is consumed by EventSeedApplier;
//      AggregateIdKey() keeps that format stable.
//
// The aggregateTypes filter restricts projection to events whose aggregateType
// is 'role_permission_grant', so unrelated events that happen to carry
// eventType 'permission_granted' are not processed.

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
