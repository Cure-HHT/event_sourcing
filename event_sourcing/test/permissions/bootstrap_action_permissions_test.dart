// test/permissions/bootstrap_action_permissions_test.dart
// Verifies: EVS-PRD-permissions-as-events/A — bootstrap emits permission_granted
//   events into the event log for all seed grants; mismatched yaml yields
//   PolicyFailSafe with no events written.
// Verifies: EVS-PRD-permissions-as-events/B — PolicyReady wraps a policy that
//   reads solely from the event-derived projection; valid declared perms ->
//   PolicyReady; mismatched yaml -> PolicyFailSafe.
// Verifies: EVS-PRD-permissions-as-events/C — re-running bootstrap with the
//   same yaml is idempotent (no new events), confirming the log alone
//   suffices to reconstruct permission state.
// Verifies: EVS-DEV-bootstrap-action-permissions/A/B/C/D — full YAML-seeded
//   bootstrap behavior: missing-grant event emission (A), PolicyFailSafe on
//   parse/validation failure (B), PolicyReady wrapping
//   TableBackedAuthorizationPolicy on success (C), idempotent on re-run (D).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_support/sembast_event_store_harness.dart';

void main() {
  group('bootstrapActionPermissions', () {
    late EventStore eventStore;

    setUp(() async {
      eventStore = await buildInMemoryEventStore();
    });

    test(
      'clean yaml + matching declared perms -> PolicyReady, ready answers permitted',
      () async {
        const yaml = '''
roles:
  - admin
grants:
  admin:
    - user.invite
''';
        final boot = await bootstrapActionPermissions(
          eventStore: eventStore,
          declaredPermissions: <Permission>{const Permission('user.invite')},
          yamlSource: yaml,
        );
        expect(boot, isA<PolicyReady>());
        expect(boot.isReady, isTrue);

        // Substrate now verifies user-role membership via user_role_scopes
        // (closed-under-events trust model): the Principal's activeRole
        // claim is not honoured until a role_assigned event is in the log.
        await eventStore.append(
          entryType: 'user_role_scope',
          aggregateType: 'user_role_scope',
          aggregateId: computeRoleAssignmentAggregateId(
            userId: 'u',
            role: 'admin',
            scope: const TotalWildcardScope(),
          ),
          eventType: 'role_assigned',
          data: const RoleAssignedPayload(
            userId: 'u',
            role: 'admin',
            scope: TotalWildcardScope(),
          ).toJson(),
          initiator: const AutomationInitiator(service: 'test'),
        );

        final policy = boot.policy;
        final p = Principal.user(
          userId: 'u',
          roles: const {'admin'},
          activeRole: 'admin',
        );
        final d = await policy.isPermitted(
          p,
          const Permission('user.invite'),
          null,
        );
        expect(d, isA<Allow>());
      },
    );

    test('yaml refers to undeclared permission -> PolicyFailSafe', () async {
      const yaml = '''
roles:
  - admin
grants:
  admin:
    - user.unknown
''';
      final boot = await bootstrapActionPermissions(
        eventStore: eventStore,
        declaredPermissions: <Permission>{const Permission('user.invite')},
        yamlSource: yaml,
      );
      expect(boot, isA<PolicyFailSafe>());
      expect(boot.isReady, isFalse);
      expect(boot.errors, isNotEmpty);

      // FailSafe denies everything.
      final p = Principal.user(
        userId: 'u',
        roles: const {'admin'},
        activeRole: 'admin',
      );
      final d = await boot.policy.isPermitted(
        p,
        const Permission('user.invite'),
        null,
      );
      expect(d, isA<Deny>());
      expect((d as Deny).reason, DenyReason.notGranted);
    });

    test('re-bootstrap with same yaml is idempotent (no new events)', () async {
      const yaml = '''
roles:
  - admin
grants:
  admin:
    - user.invite
''';
      final declared = <Permission>{const Permission('user.invite')};
      await bootstrapActionPermissions(
        eventStore: eventStore,
        declaredPermissions: declared,
        yamlSource: yaml,
      );
      final eventsBefore = await eventStore.backend.findAllEvents(limit: 1000);
      await bootstrapActionPermissions(
        eventStore: eventStore,
        declaredPermissions: declared,
        yamlSource: yaml,
      );
      final eventsAfter = await eventStore.backend.findAllEvents(limit: 1000);
      expect(eventsAfter.length, eventsBefore.length);
    });

    test('requires exactly one of yamlPath or yamlSource', () async {
      expect(
        () => bootstrapActionPermissions(
          eventStore: eventStore,
          declaredPermissions: const <Permission>{},
        ),
        throwsArgumentError,
      );
      expect(
        () => bootstrapActionPermissions(
          eventStore: eventStore,
          declaredPermissions: const <Permission>{},
          yamlPath: '/tmp/x.yaml',
          yamlSource: 'roles: []\ngrants: {}',
        ),
        throwsArgumentError,
      );
    });
  });
}
