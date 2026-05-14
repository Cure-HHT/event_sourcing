// test/permissions/user_role_scopes_spec_test.dart
// Verifies: EVS-PRD-permissions-as-events/A — role_assigned and
//   role_unassigned events are written into the same event log as all
//   other state changes, and the projection spec responds to them.
// Verifies: EVS-PRD-permissions-as-events/B — the user_role_scopes view
//   is the substrate-readable surface that TableBackedAuthorizationPolicy
//   queries to enumerate (user, role, scope) assignments.
// Verifies: EVS-PRD-permissions-as-events/C — insert and remove driven by
//   the event log alone confirms the view is fully reconstructable from
//   the log.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_support/sembast_event_store_harness.dart';

void main() {
  group('userRoleScopesSpec', () {
    test('has correct view name and interest filter', () {
      expect(userRoleScopesSpec.viewName, 'user_role_scopes');
      expect(userRoleScopesSpec.interest.eventTypes, {
        'role_assigned',
        'role_unassigned',
      });
      expect(userRoleScopesSpec.interest.aggregateTypes, {'user_role_scope'});
      expect(userRoleScopesSpec.insertEventTypes, {'role_assigned'});
      expect(userRoleScopesSpec.removeEventTypes, {'role_unassigned'});
    });

    test(
      'appending role_assigned upserts a row keyed by aggregate id',
      () async {
        final harness = await SembastEventStoreHarness.create(
          projectionSpecs: [userRoleScopesSpec],
        );
        addTearDown(harness.close);

        await harness.append(
          aggregateType: 'user_role_scope',
          aggregateId: roleAssignmentAggregateId(
            userId: 'U1',
            role: 'SC',
            scope: const BoundScope(class_: 'site', value: 'A'),
          ),
          eventType: 'role_assigned',
          payload: const RoleAssignedPayload(
            userId: 'U1',
            role: 'SC',
            scope: BoundScope(class_: 'site', value: 'A'),
          ).toJson(),
        );

        final rows = await harness.findRows('user_role_scopes');
        expect(rows, hasLength(1));
        expect(rows.single['user_id'], 'U1');
        expect(rows.single['role'], 'SC');
      },
    );

    test('appending role_unassigned removes the matching row', () async {
      final harness = await SembastEventStoreHarness.create(
        projectionSpecs: [userRoleScopesSpec],
      );
      addTearDown(harness.close);

      final aggId = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      await harness.append(
        aggregateType: 'user_role_scope',
        aggregateId: aggId,
        eventType: 'role_assigned',
        payload: const RoleAssignedPayload(
          userId: 'U1',
          role: 'SC',
          scope: BoundScope(class_: 'site', value: 'A'),
        ).toJson(),
      );
      await harness.append(
        aggregateType: 'user_role_scope',
        aggregateId: aggId,
        eventType: 'role_unassigned',
        payload: const RoleUnassignedPayload(
          userId: 'U1',
          role: 'SC',
          scope: BoundScope(class_: 'site', value: 'A'),
        ).toJson(),
      );

      expect(await harness.findRows('user_role_scopes'), isEmpty);
    });
  });
}
