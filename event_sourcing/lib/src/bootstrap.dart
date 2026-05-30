// Implements: EVS-DEV-event-store-open/A — bootstrapEventStore is the
//   canonical production entry point that calls EventStore.open (the sole
//   public constructor) before returning an EventStoreBundle facade.
// Implements: EVS-DEV-event-store-open/E — the lib-version boot check and
//   snapshot-promotion pass both run inside EventStore.open's single
//   transaction; bootstrap wires this path via allowDowngrade forwarding.

import 'package:event_sourcing/src/destinations/destination.dart';
import 'package:event_sourcing/src/destinations/destination_registry.dart';
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/security/postgres_security_context_store.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';

/// Facade returned by `bootstrapEventStore`. Exposes the four
/// collaborators an app reads through after startup: the write API
/// (`eventStore`), the registries (`entryTypes`, `destinations`), and the
/// security-context sidecar surface (`securityContexts`). Also exposes
/// `setViewTargetVersion` for post-bootstrap registration of new entry
/// types into a materializer's `view_target_versions`.
class EventStoreBundle {
  const EventStoreBundle({
    required this.eventStore,
    required this.entryTypes,
    required this.destinations,
    required this.securityContexts,
    required StorageBackend backend,
  }) : _backend = backend;

  final EventStore eventStore;
  final EntryTypeRegistry entryTypes;
  final DestinationRegistry destinations;
  final SecurityContextStore securityContexts;
  final StorageBackend _backend;

  /// Register or update a (`viewName`, `entryType`) → `version` entry in
  /// the persisted `view_target_versions`. Used to add a new entry type
  /// to a materialized view after bootstrap (e.g., when a sponsor adds a
  /// new diary entry type at runtime).
  Future<void> setViewTargetVersion(
    String viewName,
    String entryType,
    int version,
  ) {
    return _backend.transaction((txn) async {
      await _backend.writeViewTargetVersionInTxn(
        txn,
        viewName,
        entryType,
        version,
      );
    });
  }
}

/// Wire the storage backend, the `EntryTypeRegistry`, the initial set of
/// `Destination`s, the security-context store, and the `EventStore`. Returns
/// an `EventStoreBundle` facade the rest of the app reads through.
///
/// Reserved system entry types (security-context audit events) are
/// auto-registered BEFORE the caller-supplied list. Id collision with a
/// reserved id throws `ArgumentError` with a "reserved" message.
///
/// Destinations are registered sequentially, preserving fail-fast on id
/// collision.
///
/// The [allowDowngrade] flag is forwarded to [EventStore.open] for the
/// lib-version boot check. Default `false` — production-correct behaviour
/// is to refuse a downgrade. Pass `true` only during development / testing.
Future<EventStoreBundle> bootstrapEventStore({
  required StorageBackend backend,
  required Source source,
  required List<EntryTypeDefinition> entryTypes,
  required List<Destination> destinations,
  ProjectionRegistry? projections,
  EventStoreSyncCycleTrigger? syncCycleTrigger,
  bool allowDowngrade = false,
}) async {
  final typeRegistry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    typeRegistry.register(definition);
  }
  for (final definition in entryTypes) {
    if (kReservedSystemEntryTypeIds.contains(definition.id)) {
      throw ArgumentError.value(
        definition.id,
        'definition.id',
        'entryType id "${definition.id}" is reserved for system events',
      );
    }
    typeRegistry.register(definition);
  }

  // The security-context sidecar is backend-specific. We pick the matching
  // concrete store by the runtime type of [backend]. Adding a new backend
  // means shipping a paired SecurityContextStore impl and extending this
  // dispatch — the substrate refuses to bootstrap on a StorageBackend it
  // does not know how to pair.
  final MutableSecurityContextStore securityContexts;
  if (backend is SembastBackend) {
    securityContexts = SembastSecurityContextStore(backend: backend);
  } else if (backend is PostgresBackend) {
    securityContexts = PostgresSecurityContextStore(backend: backend);
  } else {
    throw ArgumentError.value(
      backend,
      'backend',
      'bootstrapEventStore has no SecurityContextStore paired '
          'with ${backend.runtimeType}; supply a SembastBackend or '
          'PostgresBackend, or extend bootstrap to dispatch on a new '
          'concrete backend type.',
    );
  }
  final eventStore = await EventStore.open(
    storage: backend,
    entryTypes: typeRegistry,
    source: source,
    securityContexts: securityContexts,
    projections: projections,
    syncCycleTrigger: syncCycleTrigger,
    allowDowngrade: allowDowngrade,
  );

  final destinationRegistry = DestinationRegistry(
    backend: backend,
    eventStore: eventStore,
  );
  const bootstrapInitiator = AutomationInitiator(service: 'lib-bootstrap');

  // Emit an event recording the registry's full id->registered_version map
  // after EventStore construction and before destination registration.
  // dedupeByContent: same-state reboots no-op; a schema bump (added entry
  // type or registeredVersion bump) emits a new event. Each install uses
  // source.identifier as its aggregate, so there is a single per-installation
  // hash-chained system aggregate spanning bootstrap, destination registry,
  // and retention/redaction audits.
  final registryStateMap = <String, int>{};
  for (final definition in typeRegistry.all()) {
    registryStateMap[definition.id] = definition.registeredVersion;
  }
  await eventStore.append(
    entryType: kEntryTypeRegistryInitializedEntryType,
    aggregateId: source.identifier,
    aggregateType: 'system_registry',
    eventType: 'finalized',
    data: <String, Object?>{'registry': registryStateMap},
    initiator: bootstrapInitiator,
    dedupeByContent: true,
  );

  for (final destination in destinations) {
    await destinationRegistry.addDestination(
      destination,
      initiator: bootstrapInitiator,
    );
  }

  return EventStoreBundle(
    eventStore: eventStore,
    entryTypes: typeRegistry,
    destinations: destinationRegistry,
    securityContexts: securityContexts,
    backend: backend,
  );
}
