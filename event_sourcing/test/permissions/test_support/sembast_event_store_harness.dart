// test/permissions/test_support/sembast_event_store_harness.dart
// Test infrastructure — no direct assertion citations.
//
// Two surfaces:
//
// 1. The free function [buildInMemoryEventStore] builds an in-memory
//    Sembast-backed [EventStore] with the [rolePermissionGrantsSpec]
//    projection registered and the role_permission_grant entry type
//    registered. It is the long-standing helper used by the
//    rolePermissionGrantsSpec and adjacent permissions tests.
//
// 2. The [SembastEventStoreHarness] class wraps the same in-memory wiring
//    behind a small, projection-spec-parameterized API:
//      - `SembastEventStoreHarness.create(projectionSpecs: [...])`
//      - `harness.append(...)` — typed entry into EventStore.append.
//      - `harness.findRows(viewName)` — typed entry into
//        backend.findViewRows for assertion ergonomics.
//      - `harness.eventStore` — escape hatch for tests that need the
//        underlying store directly.
//      - `harness.close()` — release the backing in-memory database.
//
// Both surfaces share the same set of pre-registered entry types
// (role_permission_grant, user_role_scope) so callers can pick the
// helper that fits the test without juggling registry assembly. Adding
// more entry types here is fine as the permissions module grows; the
// harness deliberately does NOT accept caller-supplied entry-type
// definitions because Tasks 17/24 (the only near-term reusers) do not
// need that flexibility yet.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/permissions/role_permission_grants_spec.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart';

const String kRolePermissionGrantEntryType = 'role_permission_grant';
const String kRolePermissionGrantsView = 'role_permission_grants';
const String kUserRoleScopeEntryType = 'user_role_scope';

/// Entry-type definitions the permissions test harnesses pre-register.
/// Kept in one place so the free function and class surfaces stay in
/// sync as the module grows.
const List<EntryTypeDefinition> _kPermissionsEntryTypeDefinitions =
    <EntryTypeDefinition>[
      EntryTypeDefinition(
        id: kRolePermissionGrantEntryType,
        registeredVersion: 1,
        name: 'Role-permission grant',
        // materialize: false — projection is driven by the registered
        // ProjectionSpec (rolePermissionGrantsSpec), not by the legacy
        // materializer surface.
        materialize: false,
      ),
      EntryTypeDefinition(
        id: kUserRoleScopeEntryType,
        registeredVersion: 1,
        name: 'User-role-scope assignment',
        // materialize: false — projection is driven by the registered
        // ProjectionSpec (userRoleScopesSpec).
        materialize: false,
      ),
    ];

EntryTypeRegistry _buildEntryTypeRegistry() {
  final registry = EntryTypeRegistry();
  for (final def in _kPermissionsEntryTypeDefinitions) {
    registry.register(def);
  }
  return registry;
}

Future<sembast.Database> _openMemoryDatabase() => newDatabaseFactoryMemory()
    .openDatabase('permissions-${DateTime.now().microsecondsSinceEpoch}.db');

/// Build a fresh, isolated in-memory [EventStore] wired with the
/// permissions module's entry types and the [rolePermissionGrantsSpec]
/// TableProjectionSpec. Returns the [EventStore] (not the
/// [AppendOnlyDatastore] facade) for ergonomic use in tests.
Future<EventStore> buildInMemoryEventStore() async {
  final db = await _openMemoryDatabase();
  final backend = SembastBackend(database: db);

  final typeRegistry = _buildEntryTypeRegistry();
  final projections = ProjectionRegistry()..register(rolePermissionGrantsSpec);

  return EventStore.open(
    storage: backend,
    entryTypes: typeRegistry,
    source: const Source(
      hopId: 'test-server',
      identifier: 'test-instance-1',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: projections,
  );
}

/// Projection-spec-parameterized in-memory test harness.
///
/// Example:
/// ```dart
/// final harness = await SembastEventStoreHarness.create(
///   projectionSpecs: [userRoleScopesSpec],
/// );
/// await harness.append(
///   aggregateType: 'user_role_scope',
///   aggregateId: roleAssignmentAggregateId(...),
///   eventType: 'role_assigned',
///   payload: const RoleAssignedPayload(...).toJson(),
/// );
/// final rows = await harness.findRows('user_role_scopes');
/// ```
class SembastEventStoreHarness {
  SembastEventStoreHarness._(this._backend, this.eventStore);

  /// Build a fresh harness wired to a freshly-opened in-memory Sembast
  /// database with [projectionSpecs] registered. The permissions module's
  /// entry types (role_permission_grant, user_role_scope) are
  /// pre-registered so callers can append without further setup.
  static Future<SembastEventStoreHarness> create({
    required List<ProjectionSpec> projectionSpecs,
  }) async {
    final db = await _openMemoryDatabase();
    final backend = SembastBackend(database: db);

    final typeRegistry = _buildEntryTypeRegistry();
    final projections = ProjectionRegistry();
    for (final spec in projectionSpecs) {
      projections.register(spec);
    }

    final eventStore = await EventStore.open(
      storage: backend,
      entryTypes: typeRegistry,
      source: const Source(
        hopId: 'test-server',
        identifier: 'test-instance-1',
        softwareVersion: 'event_sourcing_test@0.0.0',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
      projections: projections,
    );

    return SembastEventStoreHarness._(backend, eventStore);
  }

  final SembastBackend _backend;

  /// The underlying [EventStore]. Tests that need primitives the harness
  /// does not surface (e.g., subscriptions, in-txn reads) can reach for
  /// this directly.
  final EventStore eventStore;

  /// Append an event keyed by [aggregateType] / [aggregateId] / [eventType]
  /// with [payload] as the JSON body. The entry type defaults to
  /// [aggregateType] because that matches the permissions module's
  /// convention (entry-type id == aggregate-type id); callers needing
  /// a different entry type can pass it explicitly.
  Future<void> append({
    required String aggregateType,
    required String aggregateId,
    required String eventType,
    required Map<String, Object?> payload,
    String? entryType,
    Initiator initiator = const AutomationInitiator(service: 'test'),
  }) async {
    await eventStore.append(
      entryType: entryType ?? aggregateType,
      aggregateType: aggregateType,
      aggregateId: aggregateId,
      eventType: eventType,
      data: payload,
      initiator: initiator,
    );
  }

  /// Return all rows currently materialized for [viewName].
  Future<List<Map<String, dynamic>>> findRows(String viewName) =>
      _backend.findViewRows(viewName);

  /// Close the underlying [EventStore] (and, transitively, the backing
  /// in-memory database).
  Future<void> close() => eventStore.close();
}
