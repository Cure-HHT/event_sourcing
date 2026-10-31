// lib/src/permissions/role_permission_grants_spec.dart
// Declares the TableProjectionSpec for the role_permission_grants view.
// Implements: EVS-PRD-permissions-as-events/A
// permission grant and revoke
//   events are recorded in the same event log as application state changes.
// Implements: EVS-PRD-permissions-as-events/B
// the library evaluates
//   authorization decisions solely from the event-derived role_permission_grants
//   projection, not from any external authority.
// Implements: EVS-PRD-permissions-as-events/C
// the projection is fully
//   reconstructable from the event log alone; replaying permission_granted /
//   permission_revoked events reproduces the view deterministically.
//
// Row-layout contract (consumed by TableBackedAuthorizationPolicy and
// PermissionSeedApplier):
//   key: event.aggregateId  (e.g. 'admin:user.invite')
//   columns: {'role', 'permissionName'}  — WholePayload of a
//     permission_granted event, whose payload is PermissionGrantedPayload —
//     plus the substrate-stamped 'aggregateId' (== key) and 'sequence'
//     fields that TableFold writes into every row, matching AggregateFold so
//     subscribe()/ViewBuilder consume table and aggregate views uniformly.
//
// AggregateIdKey() is used as the row key because:
//   1. The payload field is 'permissionName' (matching PermissionGrantedPayload).
//   2. The row key format ('admin:user.invite') is consumed by PermissionSeedApplier;
//      AggregateIdKey() keeps that format stable.
//
// The aggregateTypes filter restricts projection to events whose aggregateType
// is 'role_permission_grant', so unrelated events that happen to carry
// eventType 'permission_granted' are not processed.

import 'package:event_sourcing/src/causal_record.dart';
import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';

/// The event type of a role-permission grant.
const String kPermissionGrantedEventType = 'permission_granted';

/// The event type of a role-permission revocation.
const String kPermissionRevokedEventType = 'permission_revoked';

/// The event types of the `role_permission_grant` entry type, which the
/// permission seed applier appends and [rolePermissionGrantsSpec] folds,
/// each an eligible version: the declarations an application's definition
/// of `role_permission_grant` carries.
const List<EventTypeDeclaration> kRolePermissionGrantDeclarations =
    <EventTypeDeclaration>[
      EventTypeDeclaration(
        eventType: kPermissionGrantedEventType,
        kind: CausalKind.version,
        eligible: true,
      ),
      EventTypeDeclaration(
        eventType: kPermissionRevokedEventType,
        kind: CausalKind.version,
        eligible: true,
      ),
    ];

const rolePermissionGrantsSpec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(
    eventTypes: {kPermissionGrantedEventType, kPermissionRevokedEventType},
    aggregateTypes: {'role_permission_grant'},
  ),
  insertEventTypes: {kPermissionGrantedEventType},
  removeEventTypes: {kPermissionRevokedEventType},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);
