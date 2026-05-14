// Verifies: EVS-PRD-permissions-as-events/B
//
// Site-scoped end-to-end test against the demo bootstrap. Walks the
// substrate's authorize path with the demo's role-permission matrix and
// the demo's user-role-scope assignments seeded from tool/users.yaml.
//
// What this exercises that walkthrough_03 (matrix-as-perimeter) does not:
// the role-grant check passes (BlueTeam carries buttons.press.blue), and
// only the scope match decides Allow vs. Deny. That isolates the scope
// binding wired in Task 25 (Action.scopeFor + RoleAssignmentSeed +
// ScopeClassRegistry) from the role-permission perimeter.

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

const String _permissionsYaml = '''
roles:
  - Admin
  - GreenTeam
  - BlueTeam
grants:
  Admin:
    - users.provision
  GreenTeam:
    - help.ask
    - notes.write.green
    - buttons.press.green
    - buttons.press.red
  BlueTeam:
    - help.ask
    - notes.write.blue
    - buttons.press.blue
    - buttons.press.red
''';

const String _usersYaml = '''
users:
  - userId: admin-user
    role: Admin
    activeSite: null
  - userId: green-user-1
    role: GreenTeam
    activeSite: green-workspace
  - userId: blue-user
    role: BlueTeam
    activeSite: blue-workspace
''';

void main() {
  group('Site-scoped end-to-end binding', () {
    late DemoServerComponents components;

    setUp(() async {
      components = await bootstrapDemoServer(
        dbPath: 'unused',
        ephemeral: true,
        permissionsYaml: _permissionsYaml,
        usersYaml: _usersYaml,
        installIdentifier: '00000000-0000-4000-8000-0000000d1331',
      );
      expect(components.policyErrors, isEmpty);
    });

    test(
      'blue-user pressing buttons.press.blue at blue-workspace is Allowed',
      () async {
        final principal = Principal.user(
          userId: 'blue-user',
          roles: const <String>{'BlueTeam'},
          activeRole: 'BlueTeam',
        );
        final decision = await components.policy.isPermitted(
          principal,
          const Permission('buttons.press.blue', scopeClass: 'site'),
          const BoundScope(class_: 'site', value: 'blue-workspace'),
        );
        expect(decision, isA<Allow>());
      },
    );

    test('blue-user pressing notes.write.blue at green-workspace is Denied '
        '(scope mismatch, not a role-grant gap)', () async {
      // BlueTeam carries notes.write.blue at the role level, so the
      // role-grant check passes. But the blue-user's only scope
      // assignment is at blue-workspace; asking the policy whether they
      // can act in green-workspace must Deny on the scope-coverage
      // step, not on missing grants.
      final principal = Principal.user(
        userId: 'blue-user',
        roles: const <String>{'BlueTeam'},
        activeRole: 'BlueTeam',
      );
      final decision = await components.policy.isPermitted(
        principal,
        const Permission('notes.write.blue', scopeClass: 'site'),
        const BoundScope(class_: 'site', value: 'green-workspace'),
      );
      expect(decision, isA<Deny>());
      final deny = decision as Deny;
      expect(deny.reason, DenyReason.notGranted);
      expect(deny.permission.name, 'notes.write.blue');
    });

    test(
      'green-user-1 pressing buttons.press.green at green-workspace is Allowed',
      () async {
        final principal = Principal.user(
          userId: 'green-user-1',
          roles: const <String>{'GreenTeam'},
          activeRole: 'GreenTeam',
        );
        final decision = await components.policy.isPermitted(
          principal,
          const Permission('buttons.press.green', scopeClass: 'site'),
          const BoundScope(class_: 'site', value: 'green-workspace'),
        );
        expect(decision, isA<Allow>());
      },
    );

    test('green-user-1 pressing notes.write.green at blue-workspace is Denied '
        '(scope mismatch)', () async {
      final principal = Principal.user(
        userId: 'green-user-1',
        roles: const <String>{'GreenTeam'},
        activeRole: 'GreenTeam',
      );
      final decision = await components.policy.isPermitted(
        principal,
        const Permission('notes.write.green', scopeClass: 'site'),
        const BoundScope(class_: 'site', value: 'blue-workspace'),
      );
      expect(decision, isA<Deny>());
      expect((decision as Deny).reason, DenyReason.notGranted);
    });

    test(
      'effectivePermissionsFor returns role permissions + site assignment',
      () async {
        final principal = Principal.user(
          userId: 'blue-user',
          roles: const <String>{'BlueTeam'},
          activeRole: 'BlueTeam',
        );
        final eff = await components.policy.effectivePermissionsFor(principal);
        expect(eff.activeRole, 'BlueTeam');
        expect(
          eff.rolePermissions.map((Permission p) => p.name).toSet(),
          <String>{
            'help.ask',
            'notes.write.blue',
            'buttons.press.blue',
            'buttons.press.red',
          },
        );
        expect(eff.scopeAssignments, hasLength(1));
        expect(
          eff.scopeAssignments.single.scope,
          const BoundScope(class_: 'site', value: 'blue-workspace'),
        );
      },
    );

    test(
      'admin-user (no activeSite) has no scope assignments seeded',
      () async {
        final principal = Principal.user(
          userId: 'admin-user',
          roles: const <String>{'Admin'},
          activeRole: 'Admin',
        );
        final eff = await components.policy.effectivePermissionsFor(principal);
        // Admin gets users.provision (unscoped) and no scope assignments
        // because admin-user has activeSite: null in users.yaml.
        expect(
          eff.rolePermissions.map((Permission p) => p.name).toSet(),
          <String>{'users.provision'},
        );
        expect(eff.scopeAssignments, isEmpty);
      },
    );
  });
}
