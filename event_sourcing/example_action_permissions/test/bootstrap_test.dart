// Verifies: EVS-PRD-action-dispatch/A
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/demo_idempotency_store.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'support/demo_bootstrap.dart';

/// Run the bootstrap-test suite against the [factory]-supplied backend
/// pair. The [label] disambiguates test names when multiple flavors run
/// in the same `flutter test` invocation.
void runBootstrapTests(DemoBackendFactory factory, {required String label}) {
  Future<DemoServerComponents> bootstrap({
    required String permissionsYaml,
    required String usersYaml,
    required String installIdentifier,
  }) async {
    final backends = await factory();
    return bootstrapDemoServer(
      backend: backends.backend,
      idempotencyStore: backends.idempotencyStore,
      permissionsYaml: permissionsYaml,
      usersYaml: usersYaml,
      installIdentifier: installIdentifier,
    );
  }

  group('bootstrapDemoServer ($label)', () {
    test('composes dispatcher + EventStore + directory + policy', () async {
      final components = await bootstrap(
        permissionsYaml: validPermissionsYaml,
        usersYaml: validUsersYaml,
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
      final components = await bootstrap(
        permissionsYaml: invalidYaml,
        usersYaml: validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-000000000002',
      );
      expect(components.policyErrors, isNotEmpty);
      expect(components.policy, isA<FailSafeAuthorizationPolicy>());
    });

    test('matrix readable: GreenTeam->help.ask granted', () async {
      final components = await bootstrap(
        permissionsYaml: validPermissionsYaml,
        usersYaml: validUsersYaml,
        installIdentifier: '00000000-0000-4000-8000-000000000003',
      );
      final principal = Principal.user(
        userId: 'green-user-1',
        roles: const <String>{'GreenTeam'},
        activeRole: 'GreenTeam',
      );
      final decision = await components.policy.isPermitted(
        principal,
        const Permission('help.ask'),
        null,
      );
      expect(decision, isA<Allow>());
    });
  });
}

Future<DemoBackends> _sembastFactory() async {
  final db = await databaseFactoryMemory.openDatabase('demo');
  return DemoBackends(
    backend: SembastBackend(database: db),
    idempotencyStore: DemoIdempotencyStore(),
  );
}

void main() {
  runBootstrapTests(_sembastFactory, label: 'sembast (memory)');
}
