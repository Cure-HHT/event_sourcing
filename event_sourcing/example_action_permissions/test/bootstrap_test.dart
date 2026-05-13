// Verifies: EVS-PRD-action-dispatch/A
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<DemoServerComponents> _bootstrap({
  required String permissionsYaml,
  required String usersYaml,
  required String installIdentifier,
}) async {
  final db = await databaseFactoryMemory.openDatabase('demo');
  return bootstrapDemoServer(
    backend: SembastBackend(database: db),
    idempotencyStore: DemoIdempotencyStore(),
    permissionsYaml: permissionsYaml,
    usersYaml: usersYaml,
    installIdentifier: installIdentifier,
  );
}

const String _validPermissionsYaml = '''
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

const String _validUsersYaml = '''
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
  group('bootstrapDemoServer', () {
    test('composes dispatcher + EventStore + directory + policy', () async {
      final components = await _bootstrap(
        permissionsYaml: _validPermissionsYaml,
        usersYaml: _validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-000000000001',
      );
      expect(components.policyErrors, isEmpty);
      expect(components.policy, isA<TableBackedAuthorizationPolicy>());
      // Directory has the 3 seed users.
      expect(components.directory.listEntries(), hasLength(3));
      expect(components.directory.contains('admin-user'), isTrue);
      expect(components.directory.contains('green-user-1'), isTrue);
      expect(components.directory.contains('blue-user'), isTrue);
    });

    test('invalid seed produces FailSafe with errors', () async {
      const invalidYaml = '''
roles:
  - Admin
grants:
  Admin:
    - permission.does.not.exist
''';
      final components = await _bootstrap(
        permissionsYaml: invalidYaml,
        usersYaml: _validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-000000000002',
      );
      expect(components.policyErrors, isNotEmpty);
      expect(components.policy, isA<FailSafeAuthorizationPolicy>());
    });

    test('matrix readable: GreenTeam->help.ask granted', () async {
      final components = await _bootstrap(
        permissionsYaml: _validPermissionsYaml,
        usersYaml: _validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-000000000003',
      );
      const principal = Principal.user(
        userId: 'green-user-1',
        roles: <String>{'GreenTeam'},
        activeRole: 'GreenTeam',
        activeSite: 'green-workspace',
      );
      final decision = await components.policy.isPermitted(
        principal,
        const Permission('help.ask', scope: ScopeClass.global),
      );
      expect(decision, isA<Allow>());
    });
  });
}
