// test/permissions/test_support/sembast_event_store_harness.dart
// Builds an in-memory Sembast-backed EventStore with the
// rolePermissionGrantsSpec registered in a ProjectionRegistry, the
// role_permission_grant entry type registered, and the initial view
// target version wired. Shared by every permissions-module test that
// needs a real EventStore + StorageBackend.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/permissions/role_permission_grants_spec.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:sembast/sembast_memory.dart';

const String kRolePermissionGrantEntryType = 'role_permission_grant';
const String kRolePermissionGrantsView = 'role_permission_grants';

/// Build a fresh, isolated in-memory [EventStore] wired with the
/// permissions module's entry type and the [rolePermissionGrantsSpec]
/// TableProjectionSpec. Returns the [EventStore] (not the
/// [AppendOnlyDatastore] facade) for ergonomic use in tests.
Future<EventStore> buildInMemoryEventStore() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'permissions-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);

  final typeRegistry = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: kRolePermissionGrantEntryType,
        registeredVersion: 1,
        name: 'Role-permission grant',
        widgetId: 'role_permission_grant_v1',
        widgetConfig: <String, Object?>{},
        // materialize: false — kept for compatibility; projection is driven
        // by the registered ProjectionSpec (rolePermissionGrantsSpec).
        materialize: false,
      ),
    );

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
