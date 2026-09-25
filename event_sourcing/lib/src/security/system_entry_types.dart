// Implements: EVS-PRD-event-log/A
// every reserved system id corresponds to
//   an audit event appended to the immutable log for each substrate mutation.
// Implements: EVS-PRD-event-log/F
// membership in
//   kReservedSystemEntryTypeIds is what a filter discriminates on, so a
//   consumer decides per filter whether these events are admitted.
// Implements: EVS-PRD-regulatory-alignment/A
// security-context and
//   retention audit events carry timestamps (via EventStore.append), satisfying
//   the ALCOA+ Contemporaneous obligation for substrate-emitted records.
// Implements: EVS-DEV-event-store-open/B+C
// kLibVersionInitializedEntryType
//   and kLibVersionChangedEntryType are the boot-version event types emitted
//   by EventStore.open on first boot and on version transitions respectively.
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

/// Reserved id for the per-event security-context redaction audit event.
const String kSecurityContextRedactedEntryType = 'security_context_redacted';

/// Reserved id for the bulk-truncation (retention compact) audit event.
const String kSecurityContextCompactedEntryType = 'security_context_compacted';

/// Reserved id for the bulk-delete (retention purge) audit event.
const String kSecurityContextPurgedEntryType = 'security_context_purged';

/// Aggregate type of every security-context audit event.
const String kSecurityContextAuditAggregateType = 'security_context';

/// Event type of the security-context redaction audit
/// ([kSecurityContextRedactedEntryType]).
const String kSecurityContextRedactedEventType = 'security_context_redacted';

/// Event type of the retention compact audit
/// ([kSecurityContextCompactedEntryType]).
const String kSecurityContextCompactedEventType = 'security_context_compacted';

/// Event type of the retention purge audit
/// ([kSecurityContextPurgedEntryType]).
const String kSecurityContextPurgedEventType = 'security_context_purged';

/// Reserved id for the destination-registration audit event.
const String kDestinationRegisteredEntryType = 'system.destination_registered';

/// Reserved id for the destination start-date set audit event.
const String kDestinationStartDateSetEntryType =
    'system.destination_start_date_set';

/// Reserved id for the destination end-date set audit event (covers
/// deactivate).
const String kDestinationEndDateSetEntryType =
    'system.destination_end_date_set';

/// Reserved id for the destination deletion audit event.
const String kDestinationDeletedEntryType = 'system.destination_deleted';

/// Reserved id for the wedge-recovery audit event emitted by
/// `DestinationRegistry.tombstoneAndRefill`.
const String kDestinationWedgeRecoveredEntryType =
    'system.destination_wedge_recovered';

/// Reserved id for the wedge event the drainer appends in the transaction
/// that marks a destination's queue head wedged.
const String kDestinationWedgedEntryType = 'system.destination_wedged';

/// Reserved id for the event recording an operator's request that the
/// drainer halt a destination's delivery (`DestinationRegistry.requestHalt`).
const String kDestinationHaltRequestedEntryType =
    'system.destination_halt_requested';

/// Reserved id for the event recording an operator's cancellation of an
/// open halt request (`DestinationRegistry.cancelHalt`).
const String kDestinationHaltCancelledEntryType =
    'system.destination_halt_cancelled';

// Implements: EVS-DEV-destination-drain/H
// each kind of destination audit event carries
//   an event type distinct from every other kind's, so a declarative filter
//   or projection tells the kinds apart by event type.

/// Aggregate type of every destination audit event. The aggregate id is the
/// appending install's `Source.identifier` and the destination id is
/// `data['id']`.
const String kDestinationAuditAggregateType = 'system_destination';

/// Event type of the destination-registration audit
/// ([kDestinationRegisteredEntryType]).
const String kDestinationRegisteredEventType = 'destination_registered';

/// Event type of the destination start-date set audit
/// ([kDestinationStartDateSetEntryType]).
const String kDestinationStartDateSetEventType = 'destination_start_date_set';

/// Event type of the destination end-date set audit
/// ([kDestinationEndDateSetEntryType]).
const String kDestinationEndDateSetEventType = 'destination_end_date_set';

/// Event type of the destination deletion audit
/// ([kDestinationDeletedEntryType]).
const String kDestinationDeletedEventType = 'destination_deleted';

/// Event type of the wedge-recovery audit
/// ([kDestinationWedgeRecoveredEntryType]).
const String kDestinationWedgeRecoveredEventType =
    'destination_wedge_recovered';

/// Event type of the wedge event ([kDestinationWedgedEntryType]).
const String kDestinationWedgedEventType = 'destination_wedged';

/// Event type of the halt request ([kDestinationHaltRequestedEntryType]).
const String kDestinationHaltRequestedEventType = 'destination_halt_requested';

/// Event type of the halt cancellation
/// ([kDestinationHaltCancelledEntryType]).
const String kDestinationHaltCancelledEventType = 'destination_halt_cancelled';

/// Every destination audit entry type: the registry's configuration,
/// recovery and halt audits and the drainer's wedge event. Each carries the
/// destination identifier in `data['id']` and the appending database's
/// identity in `data['database_id']`.
@internal
const List<String> kDestinationAuditEntryTypes = <String>[
  kDestinationRegisteredEntryType,
  kDestinationStartDateSetEntryType,
  kDestinationEndDateSetEntryType,
  kDestinationDeletedEntryType,
  kDestinationWedgeRecoveredEntryType,
  kDestinationWedgedEntryType,
  kDestinationHaltRequestedEntryType,
  kDestinationHaltCancelledEntryType,
];

/// Reserved id for the retention-policy-applied audit event emitted by
/// `EventStore.applyRetentionPolicy` once per sweep.
const String kRetentionPolicyAppliedEntryType =
    'system.retention_policy_applied';

/// Aggregate type of the retention-policy-applied audit
/// ([kRetentionPolicyAppliedEntryType]).
@internal
const String kRetentionAuditAggregateType = 'system_retention';

/// Event type of the retention-policy-applied audit
/// ([kRetentionPolicyAppliedEntryType]).
@internal
const String kRetentionPolicyAppliedEventType = 'finalized';

/// Reserved id for the bootstrap audit event recording the
/// `EntryTypeRegistry`'s id->registered_version map. Emitted once per
/// `bootstrapEventStore` call after `EventStore` construction
/// and before destination registration; deduped by content so a same-
/// version reboot no-ops while a schema bump emits a new event.
const String kEntryTypeRegistryInitializedEntryType =
    'system.entry_type_registry_initialized';

/// Aggregate type of the registry-initialized audit
/// ([kEntryTypeRegistryInitializedEntryType]).
@internal
const String kRegistryAuditAggregateType = 'system_registry';

/// Event type of the registry-initialized audit
/// ([kEntryTypeRegistryInitializedEntryType]).
@internal
const String kEntryTypeRegistryInitializedEventType = 'finalized';

/// Aggregate type (and aggregate id) of the library-version events and the
/// view-snapshot-promotion audit, which the boot appends.
@internal
const String kLibAggregateType = '_lib';

/// Reserved id for the substrate-level lib-version-initialized event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open`, through
/// `_appendLibVersionEventInTxn` inside the boot transaction, at the first
/// open of a database.
// Implements: EVS-DEV-event-store-open/B
// defines the entry-type id for the
//   first-boot lib-version event; boot-version events are substrate-internal
//   and must not be admitted to destinations as user events.
const String kLibVersionInitializedEntryType = 'lib_version_initialized';

/// Reserved id for the substrate-level lib-version-changed event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open`, through
/// `_appendLibVersionEventInTxn` inside the boot transaction, whenever the
/// opening build's package version or data format differs from the one
/// recorded last, an older one included.
// Implements: EVS-DEV-event-store-open/C
// defines the entry-type id for the
//   version-transition lib-version event; boot-version events are substrate-
//   internal and must not be admitted to destinations as user events.
const String kLibVersionChangedEntryType = 'lib_version_changed';

/// Reserved id for the raw-path ingest-audit event emitted by
/// `_emitDuplicateReceivedInTxn`. Registered here so the raw-path caller
/// can read `registeredVersion` from the registry instead of hardcoding.
const String kIngestAuditEntryType = 'ingest-audit';

/// Aggregate type of the ingest audits ([kIngestAuditEntryType]).
@internal
const String kIngestAuditAggregateType = 'ingest-audit';

/// Event type of the ingest audit recording a rejected batch. No operation
/// of this build appends it; it stays declared in [kReservedEventShapes],
/// whose shapes are fixed within a data-format major, so that ingest admits
/// one appended by another build of the same major.
@internal
const String kIngestBatchRejectedEventType = 'ingest.batch_rejected';

/// Event type of the ingest audit recording a duplicate received.
@internal
const String kIngestDuplicateReceivedEventType = 'ingest.duplicate_received';

/// Reserved id for the boot-time view-snapshot-promotion audit event
/// emitted by the snapshot-promotion pass.
const String kViewSnapshotPromotedEntryType = 'view_snapshot_promoted';

/// Event type of the view-snapshot-promotion audit
/// ([kViewSnapshotPromotedEntryType]).
@internal
const String kViewSnapshotPromotedEventType = 'finalized';

/// Reserved set of ids. `EventStore.open` registers every definition of
/// [kSystemEntryTypes] the caller's registry lacks, and refuses
/// (`ArgumentError` with an explicit "reserved" message) a caller registry
/// that holds one of these ids under any definition but the library's own.
/// The event store's public append operations refuse every one of them:
/// only the library appends reserved system events.
///
/// Also includes the substrate-internal lib-version boot events
/// (`lib_version_initialized`, `lib_version_changed`) so that
/// `SubscriptionFilter.matches` treats them as system events — requiring
/// `includeSystemEvents: true` to admit them — and so that tests that
/// filter on this set correctly exclude them from user-event assertions.
const Set<String> kReservedSystemEntryTypeIds = <String>{
  kSecurityContextRedactedEntryType,
  kSecurityContextCompactedEntryType,
  kSecurityContextPurgedEntryType,
  kDestinationRegisteredEntryType,
  kDestinationStartDateSetEntryType,
  kDestinationEndDateSetEntryType,
  kDestinationDeletedEntryType,
  kDestinationWedgeRecoveredEntryType,
  kDestinationWedgedEntryType,
  kDestinationHaltRequestedEntryType,
  kDestinationHaltCancelledEntryType,
  kRetentionPolicyAppliedEntryType,
  kEntryTypeRegistryInitializedEntryType,
  kLibVersionInitializedEntryType,
  kLibVersionChangedEntryType,
  kIngestAuditEntryType,
  kViewSnapshotPromotedEntryType,
};

/// The reserved system entry-type definitions covering security-
/// context lifecycle events (redacted / compacted / purged), config-
/// change audit events (destination registration / start_date / end_date /
/// deletion / wedge recovery / halt request / halt cancellation, plus
/// retention-policy-applied per-sweep), the drainer's destination wedge
/// event,
/// the bootstrap registry-initialized audit, the substrate-internal
/// lib-version boot events (initialized / changed), the raw-path
/// `ingest-audit` event (covering `_emitDuplicateReceivedInTxn`), and the `view_snapshot_promoted`
/// audit emitted by the boot-time snapshot-promotion pass. They exist to
/// stamp an immutable event_log row for every covered mutation.
///
/// Membership in this set is what `SubscriptionFilter` discriminates on: a
/// filter that does not opt in admits none of them, and one that opts in
/// admits them all. Nothing here makes an event unviewable — an audit view
/// opts in and receives them.
///
/// The lib-version entries (`lib_version_initialized`,
/// `lib_version_changed`) are appended raw by `_appendLibVersionEventInTxn`
/// (bypassing `EntryTypeRegistry`), but registering them here ensures:
///   1. `byId()` returns a non-null definition for tests that iterate the
///      full `kReservedSystemEntryTypeIds` set.
///   2. `SubscriptionFilter.matches` correctly gates them behind
///      `includeSystemEvents: true` via the `kReservedSystemEntryTypeIds`
///      membership check (which this list is the authoritative source for).
// Implements: EVS-DEV-event-store-open/B+C
// lib-version boot events
//   registered here so byId() returns non-null and SubscriptionFilter gates
//   them correctly, even though they are appended raw (bypassing the registry).
const List<EntryTypeDefinition> kSystemEntryTypes = <EntryTypeDefinition>[
  EntryTypeDefinition(
    id: kSecurityContextRedactedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Security Context Redacted',
  ),
  EntryTypeDefinition(
    id: kSecurityContextCompactedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Security Context Compacted',
  ),
  EntryTypeDefinition(
    id: kSecurityContextPurgedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Security Context Purged',
  ),
  EntryTypeDefinition(
    id: kDestinationRegisteredEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Registered',
  ),
  EntryTypeDefinition(
    id: kDestinationStartDateSetEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Start Date Set',
  ),
  EntryTypeDefinition(
    id: kDestinationEndDateSetEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination End Date Set',
  ),
  EntryTypeDefinition(
    id: kDestinationDeletedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Deleted',
  ),
  EntryTypeDefinition(
    id: kDestinationWedgeRecoveredEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Wedge Recovered',
  ),
  EntryTypeDefinition(
    id: kDestinationWedgedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Wedged',
  ),
  EntryTypeDefinition(
    id: kDestinationHaltRequestedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Halt Requested',
  ),
  EntryTypeDefinition(
    id: kDestinationHaltCancelledEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Destination Halt Cancelled',
  ),
  EntryTypeDefinition(
    id: kRetentionPolicyAppliedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Retention Policy Applied',
  ),
  EntryTypeDefinition(
    id: kEntryTypeRegistryInitializedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Entry Type Registry Initialized',
  ),
  EntryTypeDefinition(
    id: kLibVersionInitializedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Lib Version Initialized',
  ),
  EntryTypeDefinition(
    id: kLibVersionChangedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Lib Version Changed',
  ),
  EntryTypeDefinition(
    id: kIngestAuditEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'Ingest Audit',
  ),
  EntryTypeDefinition(
    id: kViewSnapshotPromotedEntryType,
    registeredVersion: EntryTypeVersion(1, 0),
    name: 'View Snapshot Promoted',
  ),
];

/// The one aggregate type and the event types the library appends a
/// reserved system entry type with.
@internal
class ReservedEventShape {
  const ReservedEventShape(this.aggregateType, this.eventTypes);

  /// The aggregate type of every event of the entry type.
  final String aggregateType;

  /// The event types the library appends the entry type with.
  final Set<String> eventTypes;

  /// Whether an event with [aggregateType] and [eventType] has this shape.
  bool admits(String aggregateType, String eventType) =>
      aggregateType == this.aggregateType && eventTypes.contains(eventType);
}

// Implements: EVS-DEV-destination-drain/L
// the library declares the aggregate type and event types of every
//   reserved entry type, changing them only with a data-format major step;
//   every library emitter appends in a declared shape, and ingest refuses a
//   reserved event in any other shape.
/// The declared shape of every reserved system entry type, keyed by entry
/// type id: the library appends each reserved entry type only with its
/// aggregate type and one of its event types, and no two reserved entry
/// types share an (aggregate type, event type) pair.
///
/// A declared shape is fixed within a data-format major
/// (`DataFormatVersion`): a build refuses at ingest a reserved event outside
/// the shapes it declares, so adding an event type to an entry here, or
/// changing its aggregate type, is a data-format major step. A new kind of
/// reserved event is a new reserved entry type.
@internal
const Map<String, ReservedEventShape> kReservedEventShapes =
    <String, ReservedEventShape>{
      kSecurityContextRedactedEntryType: ReservedEventShape(
        kSecurityContextAuditAggregateType,
        <String>{kSecurityContextRedactedEventType},
      ),
      kSecurityContextCompactedEntryType: ReservedEventShape(
        kSecurityContextAuditAggregateType,
        <String>{kSecurityContextCompactedEventType},
      ),
      kSecurityContextPurgedEntryType: ReservedEventShape(
        kSecurityContextAuditAggregateType,
        <String>{kSecurityContextPurgedEventType},
      ),
      kDestinationRegisteredEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationRegisteredEventType},
      ),
      kDestinationStartDateSetEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationStartDateSetEventType},
      ),
      kDestinationEndDateSetEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationEndDateSetEventType},
      ),
      kDestinationDeletedEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationDeletedEventType},
      ),
      kDestinationWedgeRecoveredEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationWedgeRecoveredEventType},
      ),
      kDestinationWedgedEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationWedgedEventType},
      ),
      kDestinationHaltRequestedEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationHaltRequestedEventType},
      ),
      kDestinationHaltCancelledEntryType: ReservedEventShape(
        kDestinationAuditAggregateType,
        <String>{kDestinationHaltCancelledEventType},
      ),
      kRetentionPolicyAppliedEntryType: ReservedEventShape(
        kRetentionAuditAggregateType,
        <String>{kRetentionPolicyAppliedEventType},
      ),
      kEntryTypeRegistryInitializedEntryType: ReservedEventShape(
        kRegistryAuditAggregateType,
        <String>{kEntryTypeRegistryInitializedEventType},
      ),
      // The library-version events carry their entry-type id as their
      // event type.
      kLibVersionInitializedEntryType: ReservedEventShape(
        kLibAggregateType,
        <String>{kLibVersionInitializedEntryType},
      ),
      kLibVersionChangedEntryType: ReservedEventShape(
        kLibAggregateType,
        <String>{kLibVersionChangedEntryType},
      ),
      kIngestAuditEntryType: ReservedEventShape(
        kIngestAuditAggregateType,
        <String>{
          kIngestBatchRejectedEventType,
          kIngestDuplicateReceivedEventType,
        },
      ),
      kViewSnapshotPromotedEntryType: ReservedEventShape(
        kLibAggregateType,
        <String>{kViewSnapshotPromotedEventType},
      ),
    };

/// Whether [data] has the shape the library appends every destination
/// audit event with: a destination identifier (`id`) and a database
/// identity (`database_id`), each a non-empty string without `|`. The
/// library's reserved appends and ingest apply this one predicate, so the
/// library never appends a destination audit that a receiver refuses.
@internal
bool isWellFormedDestinationAuditData(Map<String, Object?> data) {
  final id = data['id'];
  final databaseId = data['database_id'];
  return id is String &&
      id.isNotEmpty &&
      !id.contains('|') &&
      databaseId is String &&
      databaseId.isNotEmpty &&
      !databaseId.contains('|');
}

/// Throws [ArgumentError] unless [entryType] is a reserved system entry type
/// and [aggregateType] and [eventType] are a shape the library declares for
/// it in [kReservedEventShapes]. Every library emitter of a reserved event
/// passes through this check before it writes.
@internal
void checkReservedEventShape({
  required String entryType,
  required String aggregateType,
  required String eventType,
}) {
  final shape = kReservedEventShapes[entryType];
  if (shape == null) {
    throw ArgumentError.value(
      entryType,
      'entryType',
      'is not a reserved system entry type',
    );
  }
  if (!shape.admits(aggregateType, eventType)) {
    throw ArgumentError.value(
      '$aggregateType/$eventType',
      'aggregateType/eventType',
      'the library declares entry type $entryType only with aggregate type '
          '${shape.aggregateType} and event types '
          '${(shape.eventTypes.toList()..sort()).join(', ')}',
    );
  }
}

/// Throws [ArgumentError] unless [entryType] is a reserved system entry type
/// appended in a shape the library declares for it
/// ([checkReservedEventShape]) and, for a destination audit, with data
/// ingest admits ([isWellFormedDestinationAuditData]). The event store's
/// reserved appends run this check before they write.
// Implements: EVS-DEV-destination-drain/K
// every destination audit event the library appends carries a destination
//   identifier and the appending database's identity, each non-empty and
//   without '|'.
@internal
void checkReservedAppend({
  required String entryType,
  required String aggregateType,
  required String eventType,
  required Map<String, Object?> data,
}) {
  checkReservedEventShape(
    entryType: entryType,
    aggregateType: aggregateType,
    eventType: eventType,
  );
  if (kDestinationAuditEntryTypes.contains(entryType) &&
      !isWellFormedDestinationAuditData(data)) {
    throw ArgumentError.value(
      data,
      'data',
      'a destination audit event carries a destination identifier (id) '
          'and a database identity (database_id), each a non-empty string '
          "without '|'",
    );
  }
}
