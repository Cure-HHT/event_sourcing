// Verifies: EVS-DEV-postgres-backend/D — backend-agnostic harness shared
// across the demo's bootstrap, routes, and projection test surfaces. Each
// runner takes a [DemoBackendFactory] so the same test bodies can run
// against sembast in-memory, postgres, or any future StorageBackend.

import 'package:event_sourcing/event_sourcing.dart';

/// Backend pair handed to each runner per-test. The factory produces a
/// fresh pair for every `setUp` invocation so tests do not share state.
class DemoBackends {
  const DemoBackends({required this.backend, required this.idempotencyStore});

  final StorageBackend backend;
  final IdempotencyStore idempotencyStore;
}

/// Per-flavor factory. Each test file's `main()` provides a sembast
/// implementation; companion `*_postgres_test.dart` files supply a postgres
/// implementation that resets the schema before each call.
typedef DemoBackendFactory = Future<DemoBackends> Function();

/// Shared permissions YAML used by the demo test runners. Kept stable so
/// tests can assert against fixed userIds / role names without each file
/// duplicating its own copy.
const String validPermissionsYaml = '''
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

/// Shared users YAML — three seeded users (Admin, GreenTeam, BlueTeam)
/// against the matrix above.
const String validUsersYaml = '''
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
