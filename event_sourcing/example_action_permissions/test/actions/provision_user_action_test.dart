// Verifies: EVS-PRD-action-dispatch/A/B/C
// Verifies: EVS-PRD-permissions-as-events/A/B
// Verifies: EVS-PRD-action-dispatch/D
import 'package:action_permissions_demo/server/actions/provision_user_action.dart';
import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

ActionContext _adminCtx() => ActionContext(
  principal: Principal.user(
    userId: 'admin-user',
    roles: const <String>{'Admin'},
    activeRole: 'Admin',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.utc(2026, 5, 8, 12),
);

void main() {
  group('ProvisionUserAction', () {
    late UserDirectory directory;
    late ProvisionUserAction action;

    setUp(() {
      directory = UserDirectory();
      action = ProvisionUserAction(directory: directory);
    });

    test('declares global users.provision, idempotency required', () {
      expect(action.name, 'ProvisionUserAction');
      expect(action.permissions, contains(const Permission('users.provision')));
      expect(action.idempotency, Idempotency.required);
    });

    test('parseInput accepts {userId, role, activeSite?}', () {
      final input = action.parseInput(<String, Object?>{
        'userId': 'new-user',
        'role': 'GreenTeam',
        'activeSite': 'green-workspace',
      });
      expect(input.userId, 'new-user');
      expect(input.role, 'GreenTeam');
      expect(input.activeSite, 'green-workspace');
    });

    test('parseInput accepts null/missing activeSite', () {
      final fromNull = action.parseInput(<String, Object?>{
        'userId': 'admin-2',
        'role': 'Admin',
        'activeSite': null,
      });
      expect(fromNull.activeSite, isNull);
      final fromMissing = action.parseInput(<String, Object?>{
        'userId': 'admin-3',
        'role': 'Admin',
      });
      expect(fromMissing.activeSite, isNull);
    });

    test('parseInput throws FormatException on missing/wrong fields', () {
      expect(
        () => action.parseInput(<String, Object?>{'userId': 'x'}),
        throwsA(isA<FormatException>()),
      );
      expect(
        () =>
            action.parseInput(<String, Object?>{'userId': 42, 'role': 'Admin'}),
        throwsA(isA<FormatException>()),
      );
    });

    test('validate rejects empty userId or role with ArgumentError', () {
      expect(
        () => action.validate(
          const ProvisionUserInput(userId: '', role: 'Admin', activeSite: null),
        ),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => action.validate(
          const ProvisionUserInput(userId: 'x', role: '', activeSite: null),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('validate rejects userId already in directory', () {
      directory.upsert(userId: 'taken', role: 'GreenTeam', activeSite: null);
      expect(
        () => action.validate(
          const ProvisionUserInput(
            userId: 'taken',
            role: 'BlueTeam',
            activeSite: null,
          ),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('validate accepts a fresh userId', () {
      action.validate(
        const ProvisionUserInput(
          userId: 'fresh',
          role: 'GreenTeam',
          activeSite: 'green-workspace',
        ),
      );
    });

    test(
      'execute emits user_provisioned + role_assigned when activeSite is set',
      () async {
        final result = await action.execute(
          const ProvisionUserInput(
            userId: 'fresh',
            role: 'GreenTeam',
            activeSite: 'green-workspace',
          ),
          _adminCtx(),
        );
        expect(result.events, hasLength(2));

        final provisioned = result.events.singleWhere(
          (e) => e.eventType == 'user_provisioned',
        );
        expect(provisioned.aggregateType, 'user_directory');
        expect(provisioned.entryType, 'user_provisioned');
        expect(provisioned.aggregateId, 'fresh');
        expect(provisioned.data['userId'], 'fresh');
        expect(provisioned.data['role'], 'GreenTeam');
        expect(provisioned.data['activeSite'], 'green-workspace');

        // role_assigned event populates the user_role_scopes view in the
        // dispatch tx, replacing the post-commit subscriber-bridge that
        // previously had to re-emit role_assigned via eventStore.append.
        final assigned = result.events.singleWhere(
          (e) => e.eventType == 'role_assigned',
        );
        expect(assigned.aggregateType, 'user_role_scope');
        expect(assigned.entryType, 'user_role_scope');
        expect(assigned.data['user_id'], 'fresh');
        expect(assigned.data['role'], 'GreenTeam');
        expect((assigned.data['scope']! as Map)['value'], 'green-workspace');

        expect(result.result.userId, 'fresh');
      },
    );

    test(
      'execute emits only user_provisioned when activeSite is null',
      () async {
        final result = await action.execute(
          const ProvisionUserInput(
            userId: 'admin-2',
            role: 'Admin',
            activeSite: null,
          ),
          _adminCtx(),
        );
        expect(result.events, hasLength(1));
        expect(result.events.single.eventType, 'user_provisioned');
        expect(result.events.single.data['activeSite'], isNull);
      },
    );
  });
}
