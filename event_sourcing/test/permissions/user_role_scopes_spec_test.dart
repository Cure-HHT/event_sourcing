// test/permissions/user_role_scopes_spec_test.dart

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

    // Verifies: EVS-PRD-permissions-as-events/A
    test(
      'appending role_assigned upserts a row keyed by aggregate id',
      () async {
        final harness = await SembastEventStoreHarness.create(
          projectionSpecs: [userRoleScopesSpec],
        );
        addTearDown(harness.close);

        await harness.append(
          aggregateType: 'user_role_scope',
          aggregateId: computeRoleAssignmentAggregateId(
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

    // Verifies: EVS-PRD-permissions-as-events/A
    test('appending role_unassigned removes the matching row', () async {
      final harness = await SembastEventStoreHarness.create(
        projectionSpecs: [userRoleScopesSpec],
      );
      addTearDown(harness.close);

      final aggId = computeRoleAssignmentAggregateId(
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
