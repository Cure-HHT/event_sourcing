// Test support: shared in-memory EventStore factory used by action dispatcher tests.
// Verifies: EVS-PRD-action-dispatch/C (provides the EventStore into which denial and success events are recorded during dispatch tests)
// In-memory EventStore bootstrap helper shared across actions test files.
//
// Uses flutter_test (not package:test) because EventStore depends on
// Sembast, which requires the Flutter test binding to run in this package.
// All tests that call bootstrapTestEventStore() must also use flutter_test.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:sembast/sembast_memory.dart';

/// Builds a fully-wired in-memory [EventStore] suitable for actions tests.
///
/// Registers all reserved system entry types plus the two test-specific
/// entry types (`action_denial` and `greeting`) that `HelloAction` and
/// `MultiEventAction` emit. Returns a fresh instance on every call so
/// tests are isolated.
///
/// [projections] may be supplied to register projection specs that watch
/// the test-emitted events — used by the dispatch-materialization test
/// to confirm that views update inside the dispatch transaction.
Future<EventStore> bootstrapTestEventStore({
  ProjectionRegistry? projections,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'dispatcher-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();

  // Register every reserved system entry type (security-context lifecycle,
  // destination-mutation audits, retention sweep, registry-initialized audit).
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }

  // Register test-specific entry types. isMaterialized: false — these are
  // audit records and test fixtures, not diary entries.
  registry
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: 1,
        name: 'Action denial',
        isMaterialized: false,
      ),
    )
    // greeting is emitted by HelloAction and MultiEventAction.
    ..register(
      const EntryTypeDefinition(
        id: 'greeting',
        registeredVersion: 1,
        name: 'Greeting',
        isMaterialized: false,
      ),
    );

  final securityContexts = SembastSecurityContextStore(backend: backend);

  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'test-server',
      identifier: 'test-instance-1',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    securityContexts: securityContexts,
    projections: projections,
  );
}
