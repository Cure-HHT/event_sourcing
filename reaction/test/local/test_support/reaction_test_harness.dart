// In-memory substrate harness shared across reaction's Local* impl tests.
//
// Uses flutter_test (not package:test) because EventStore depends on
// Sembast, which requires the Flutter test binding. All tests that call
// ReactionTestHarness.open() must also use flutter_test.
//
// Wires up:
//   - An in-memory SembastBackend via sembast_memory.
//   - An EntryTypeRegistry with kSystemEntryTypes + 'note' + 'greeting'.
//   - A ProjectionRegistry with rolePermissionGrantsSpec + 'notes_today'
//     AggregateProjectionSpec.
//   - A SembastSecurityContextStore.
//   - EventStore (via openForTest — skips lib-version boot events so
//     sequence numbers are predictable).
//   - An ActionRegistry with SayHelloAction.
//   - A MaterializedViewRoleMatrixReader backed by the store's backend.
//   - A TableBackedAuthorizationPolicy backed by the reader.
//   - An ActionDispatcher with the registry, policy, store, and an
//     InMemoryIdempotencyStore.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:sembast/sembast_memory.dart';

/// Fully-wired in-memory substrate bundle for reaction Local* impl tests.
///
/// Call [open] in `setUp()` and [close] in `tearDown()`.
class ReactionTestHarness {
  ReactionTestHarness._({
    required this.eventStore,
    required this.dispatcher,
    required this.roleMatrixReader,
  });

  final EventStore eventStore;
  final ActionDispatcher dispatcher;
  final RoleMatrixReader roleMatrixReader;

  /// Build a fresh harness. Each call returns a fully-isolated instance.
  static Future<ReactionTestHarness> open() async {
    // --- Storage ---
    final db = await newDatabaseFactoryMemory().openDatabase(
      'reaction-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    final backend = SembastBackend(database: db);

    // --- Entry types ---
    final entryTypes = EntryTypeRegistry();
    for (final defn in kSystemEntryTypes) {
      entryTypes.register(defn);
    }
    entryTypes
      ..register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: 1,
          name: 'Note',
          materialize: true,
        ),
      )
      ..register(
        const EntryTypeDefinition(
          id: 'greeting',
          registeredVersion: 1,
          name: 'Greeting',
          materialize: false,
        ),
      )
      // Required by EventSeedApplier when seeding the role-permission matrix.
      ..register(
        const EntryTypeDefinition(
          id: 'role_permission_grant',
          registeredVersion: 1,
          name: 'Role-Permission Grant',
          materialize: false,
        ),
      )
      // Required by ActionDispatcher for denial-stage audit events.
      ..register(
        const EntryTypeDefinition(
          id: 'action_denial',
          registeredVersion: 1,
          name: 'Action Denial',
          materialize: false,
        ),
      );

    // --- Projections ---
    final projections = ProjectionRegistry()
      ..register(rolePermissionGrantsSpec)
      ..register(
        const AggregateProjectionSpec(
          viewName: 'notes_today',
          interest: SubscriptionFilter(aggregateTypes: {'note'}),
          tombstoneEventTypes: {'note_deleted'},
        ),
      );

    // --- Security context store ---
    final securityContexts = SembastSecurityContextStore(backend: backend);

    // --- EventStore ---
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'reaction-test',
        identifier: 'reaction-test-instance-1',
        softwareVersion: 'reaction_test@0.0.0',
      ),
      securityContexts: securityContexts,
      projections: projections,
      promoters: PromoterRegistry(),
    );

    // --- Permissions ---
    final roleMatrixReader = MaterializedViewRoleMatrixReader(backend);
    final policy = TableBackedAuthorizationPolicy(roleMatrixReader);

    // --- Action dispatcher ---
    final actionRegistry = ActionRegistry()..register(SayHelloAction());
    final idempotency = InMemoryIdempotencyStore();
    final dispatcher = ActionDispatcher(
      registry: actionRegistry,
      authorization: policy,
      events: store,
      idempotency: idempotency,
    );

    return ReactionTestHarness._(
      eventStore: store,
      dispatcher: dispatcher,
      roleMatrixReader: roleMatrixReader,
    );
  }

  /// Close the EventStore, releasing all resources.
  Future<void> close() async {
    await eventStore.close();
  }

  /// Convenience: append a `note` event to the store.
  ///
  /// The event is appended with an anonymous initiator. For custom
  /// initiators, call `eventStore.append(...)` directly. Returns the
  /// persisted [StoredEvent]; throws if the append returns null (which
  /// indicates a content-dedup skip — should not happen in tests).
  Future<StoredEvent> appendNote({
    required String aggregateId,
    required Map<String, Object?> payload,
  }) async {
    final stored = await eventStore.append(
      entryType: 'note',
      aggregateId: aggregateId,
      aggregateType: 'note',
      eventType: 'note_created',
      data: payload,
      initiator: const AnonymousInitiator(ipAddress: null),
    );
    if (stored == null) {
      throw StateError(
        'appendNote: EventStore.append returned null for aggregateId=$aggregateId. '
        'This should not happen in tests — check for accidental dedupeByContent.',
      );
    }
    return stored;
  }
}

// ---------------------------------------------------------------------------
// Sample action registered in the dispatcher.
// Used by LocalActionSubmitter tests in Task 10.
// ---------------------------------------------------------------------------

/// Input for [SayHelloAction].
class HelloSubmission {
  const HelloSubmission({required this.name});

  final String name;
}

/// Result for [SayHelloAction].
class HelloResult {
  const HelloResult({required this.greeting});

  final String greeting;
}

/// Accepts `{name: <str>}`, validates non-empty, emits one `greeting` event.
class SayHelloAction extends Action<HelloSubmission, HelloResult> {
  @override
  String get name => 'say_hello';

  @override
  String get description => 'Say hello — sample action for harness tests.';

  @override
  Set<Permission> get permissions => {
    const Permission('say_hello', scope: ScopeClass.global),
  };

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  HelloSubmission parseInput(Map<String, Object?> raw) {
    final nameVal = raw['name'];
    if (nameVal is! String) {
      throw FormatException(
        "'name' must be a string, got: ${nameVal.runtimeType}",
      );
    }
    return HelloSubmission(name: nameVal);
  }

  @override
  void validate(HelloSubmission input) {
    if (input.name.trim().isEmpty) {
      throw ArgumentError.value(input.name, 'name', 'must not be empty');
    }
  }

  @override
  Future<ExecutionResult<HelloResult>> execute(
    HelloSubmission input,
    ActionContext ctx,
  ) async {
    final greeting = 'Hello, ${input.name}!';
    return ExecutionResult<HelloResult>(
      result: HelloResult(greeting: greeting),
      events: [
        EventDraft(
          aggregateId: 'greeting-${input.name}',
          aggregateType: 'greeting',
          entryType: 'greeting',
          eventType: 'hello_said',
          data: <String, Object?>{'name': input.name, 'greeting': greeting},
        ),
      ],
    );
  }
}
