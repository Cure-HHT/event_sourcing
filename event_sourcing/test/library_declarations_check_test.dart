// The stages that append to the library's three application entry types
// check at construction that the registered definition declares every event
// type they append.
//
// Uses flutter_test (not package:test) because EventStore depends on
// Sembast, which requires the Flutter test binding to run in this package.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<EventStore> _store(
  String entryTypeId,
  List<EventTypeDeclaration> declarations,
) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'declarations-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    EntryTypeDefinition(
      id: entryTypeId,
      registeredVersion: const EntryTypeVersion(1, 0),
      name: entryTypeId,
      declarations: declarations,
    ),
  );
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'test-server',
      identifier: 'test-instance-1',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

Matcher _namesEntryTypeAndMissing(String entryType, String missing) => throwsA(
  isA<ArgumentError>().having(
    (e) => e.toString(),
    'message',
    allOf(contains(entryType), contains(missing)),
  ),
);

ActionDispatcher _dispatcher(EventStore events) => ActionDispatcher(
  registry: ActionRegistry(),
  authorization: const DenyAllAuthorizationPolicy.forTests(),
  events: events,
  idempotency: InMemoryIdempotencyStore(),
);

void main() {
  // Verifies: EVS-DEV-causal-parents/T
  test('ActionDispatcher refuses an action_denial definition missing a '
      'denial declaration', () async {
    final store = await _store(
      'action_denial',
      kActionDenialDeclarations.skip(1).toList(),
    );
    expect(
      () => _dispatcher(store),
      _namesEntryTypeAndMissing(
        'action_denial',
        kActionDenialDeclarations.first.eventType,
      ),
    );
  });

  // Verifies: EVS-DEV-causal-parents/T
  test(
    'ActionDispatcher constructs over the full denial declarations',
    () async {
      final store = await _store('action_denial', kActionDenialDeclarations);
      expect(() => _dispatcher(store), returnsNormally);
    },
  );

  // Verifies: EVS-DEV-causal-parents/T
  test('PermissionSeedApplier refuses a role_permission_grant definition '
      'missing a grant declaration', () async {
    final store = await _store(
      'role_permission_grant',
      kRolePermissionGrantDeclarations.skip(1).toList(),
    );
    expect(
      () => PermissionSeedApplier(
        eventStore: store,
        seedInitiator: const AutomationInitiator(service: 'seed'),
      ),
      _namesEntryTypeAndMissing(
        'role_permission_grant',
        kRolePermissionGrantDeclarations.first.eventType,
      ),
    );
  });

  // Verifies: EVS-DEV-causal-parents/T
  test(
    'PermissionSeedApplier constructs over the full grant declarations',
    () async {
      final store = await _store(
        'role_permission_grant',
        kRolePermissionGrantDeclarations,
      );
      expect(
        () => PermissionSeedApplier(
          eventStore: store,
          seedInitiator: const AutomationInitiator(service: 'seed'),
        ),
        returnsNormally,
      );
    },
  );

  // Verifies: EVS-DEV-causal-parents/T
  test('bootstrapRoleAssignments refuses a user_role_scope definition '
      'missing a scope declaration', () async {
    final store = await _store(
      'user_role_scope',
      kUserRoleScopeDeclarations.skip(1).toList(),
    );
    await expectLater(
      bootstrapRoleAssignments(
        eventStore: store,
        seed: const RoleAssignmentSeed(entries: []),
      ),
      _namesEntryTypeAndMissing(
        'user_role_scope',
        kUserRoleScopeDeclarations.first.eventType,
      ),
    );
  });

  // Verifies: EVS-DEV-causal-parents/T
  test('bootstrapRoleAssignments passes the check over the full scope '
      'declarations', () async {
    final store = await _store('user_role_scope', kUserRoleScopeDeclarations);
    // The view is unregistered here, so the call fails past the check, not
    // with the declaration refusal.
    await expectLater(
      bootstrapRoleAssignments(
        eventStore: store,
        seed: const RoleAssignmentSeed(entries: []),
      ).then<Object?>((_) => null, onError: (Object e) => e),
      predicate<Object?>(
        (e) => e is! ArgumentError || !'$e'.contains('declare'),
      ),
    );
  });
}
