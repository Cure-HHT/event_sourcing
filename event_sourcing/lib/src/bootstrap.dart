// Implements: EVS-DEV-event-store-open/A
// bootstrapEventStore is the
//   canonical production entry point that calls EventStore.open (the sole
//   public constructor) before returning an EventStoreBundle facade.
// Implements: EVS-DEV-event-store-open/E
// bootstrapEventStore opens through EventStore.open, whose whole boot runs
//   in one storage transaction.

part of 'event_store.dart';

/// Facade returned by `bootstrapEventStore`. Exposes the four
/// collaborators an app reads through after startup: the write API
/// (`eventStore`), the registries (`entryTypes`, `destinations`), and the
/// security-context sidecar surface (`securityContexts`).
class EventStoreBundle {
  const EventStoreBundle({
    required this.eventStore,
    required this.entryTypes,
    required this.destinations,
    required this.securityContexts,
  });

  final EventStore eventStore;
  final EntryTypeRegistry entryTypes;
  final DestinationRegistry destinations;
  final SecurityContextStore securityContexts;
}

/// Open the `EventStore` over the storage [storage] describes, with the
/// `EntryTypeRegistry` and the initial set of `Destination`s. Returns an
/// `EventStoreBundle` facade the rest of the app reads through.
///
/// [storage] is opened as [EventStore.open] opens it, and the bundle's
/// security-context store is the event store's read-only one. When the
/// registry audit or a destination registration fails after the open, the
/// event store is closed, closing the storage the library opened, before
/// the error reaches the caller.
///
/// [entryTypes] lists the application's own entry types. `EventStore.open`
/// registers the reserved system entry types ([kSystemEntryTypes]) and the
/// default destination-wedges view beside them; a definition in
/// [entryTypes] under a reserved id throws `ArgumentError` with a
/// "reserved" message unless it is the library's own definition, and so
/// does a spec in [projections] under the default view's name unless it is
/// the library's own spec.
///
/// Destinations are registered sequentially, preserving fail-fast on id
/// collision. Delivery starts when the application starts a `SyncCycle`
/// over the bundle's destination registry; appends through the bundle's
/// event store then wake it.
///
/// The boot of [EventStore.open] runs as it documents: a build opens a
/// database written by a build of the same data-format major, older ones
/// included, and records that it did; a database of another data-format
/// major is refused before anything is written. Builds with the same
/// data-format major and entry-type majors share a database in any mix (a
/// canary, several instances, a rollback); a build of another data-format
/// major, or one that raises an entry-type major, is deployed
/// stop-then-start, and recovery after it is a restore from a backup taken
/// before the switch, or a roll-forward.
///
/// [onBootProgress] observes the boot of that open, as
/// [EventStore.open] documents: the progress of a long boot can be served
/// by a readiness endpoint while the open runs, and the observer must not
/// call back into an event store while the boot runs. Its
/// [BootPhase.complete] means the store opened, not that this function
/// finished: the registry audit and the destination registration run after
/// it and can still fail, so a readiness endpoint turns ready when this
/// function returns.
// Implements: EVS-DEV-event-store-open/G
// bootstrapEventStore passes its boot-progress observer to EventStore.open.
// Implements: EVS-PRD-storage-barrier/I
// a failure after the open (the registry audit, a destination registration)
//   closes the event store, and with it the storage the library opened,
//   before it reaches the caller.
Future<EventStoreBundle> bootstrapEventStore({
  required StorageDescription storage,
  required Source source,
  required List<EntryTypeDefinition> entryTypes,
  required List<Destination> destinations,
  ProjectionRegistry? projections,
  void Function(BootProgress progress)? onBootProgress,
}) async {
  final typeRegistry = EntryTypeRegistry();
  for (final definition in entryTypes) {
    typeRegistry.register(definition);
  }

  final eventStore = await EventStore.open(
    storage: storage,
    entryTypes: typeRegistry,
    source: source,
    projections: projections,
    onBootProgress: onBootProgress,
  );
  try {
    return await _completeBootstrap(
      eventStore,
      typeRegistry,
      source,
      destinations,
    );
  } catch (_) {
    await eventStore.close();
    rethrow;
  }
}

/// The part of [bootstrapEventStore] after the open: the registry audit and
/// the destination registration.
Future<EventStoreBundle> _completeBootstrap(
  EventStore eventStore,
  EntryTypeRegistry typeRegistry,
  Source source,
  List<Destination> destinations,
) async {
  final destinationRegistry = DestinationRegistry(eventStore: eventStore);
  const bootstrapInitiator = AutomationInitiator(service: 'lib-bootstrap');

  // Emit an event recording the registry's full id->registered_version map
  // after EventStore construction and before destination registration.
  // dedupeByContent: same-state reboots no-op; a schema change (an added
  // entry type, or a raised major or minor) emits a new event. Each install uses
  // source.identifier as its aggregate, so there is a single per-installation
  // hash-chained system aggregate spanning bootstrap, destination registry,
  // and retention/redaction audits. No delivery cycle can hold the store's
  // trigger slot yet (its registry is built here), so the audit wakes none.
  // Implements: EVS-DEV-version-compatibility/K
  // the registry audit records every registered entry type's major and minor
  //   as `M.m`; a changed set, major or minor changes the content, so a new
  //   audit is appended, and an unchanged registry dedupes to none.
  final registryStateMap = <String, String>{};
  for (final definition in typeRegistry.all()) {
    registryStateMap[definition.id] = definition.registeredVersion.toString();
  }
  await eventStore.runTransaction(
    (txn, collector) => eventStore._appendReservedInTxn(
      txn,
      collector,
      entryType: kEntryTypeRegistryInitializedEntryType,
      aggregateId: source.identifier,
      aggregateType: kRegistryAuditAggregateType,
      eventType: kEntryTypeRegistryInitializedEventType,
      data: <String, Object?>{'registry': registryStateMap},
      initiator: bootstrapInitiator,
      dedupeByContent: true,
    ),
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
    securityContexts: eventStore.securityContexts,
  );
}
