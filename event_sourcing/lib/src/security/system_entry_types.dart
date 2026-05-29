// Implements: EVS-PRD-event-log/A — every reserved system id corresponds to
//   an audit event appended to the immutable log for each substrate mutation;
//   no reserved entry type has materialize:true, so they never corrupt views.
// Implements: EVS-PRD-regulatory-alignment/A — security-context and
//   retention audit events carry timestamps (via EventStore.append), satisfying
//   the ALCOA+ Contemporaneous obligation for substrate-emitted records.
// Implements: EVS-DEV-event-store-open/B,C — kLibVersionInitializedEntryType
//   and kLibVersionChangedEntryType are the boot-version event types emitted
//   by EventStore.open on first boot and on version transitions respectively.
import 'package:event_sourcing/src/entry_type_definition.dart';

/// Reserved id for the per-event security-context redaction audit event.
const String kSecurityContextRedactedEntryType = 'security_context_redacted';

/// Reserved id for the bulk-truncation (retention compact) audit event.
const String kSecurityContextCompactedEntryType = 'security_context_compacted';

/// Reserved id for the bulk-delete (retention purge) audit event.
const String kSecurityContextPurgedEntryType = 'security_context_purged';

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

/// Reserved id for the retention-policy-applied audit event emitted by
/// `EventStore.applyRetentionPolicy` once per sweep.
const String kRetentionPolicyAppliedEntryType =
    'system.retention_policy_applied';

/// Reserved id for the bootstrap audit event recording the
/// `EntryTypeRegistry`'s id->registered_version map. Emitted once per
/// `bootstrapAppendOnlyDatastore` call after `EventStore` construction
/// and before destination registration; deduped by content so a same-
/// version reboot no-ops while a schema bump emits a new event.
const String kEntryTypeRegistryInitializedEntryType =
    'system.entry_type_registry_initialized';

/// Reserved id for the substrate-level lib-version-initialized event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open` /
/// `_appendLibVersionEventToBackend` on first boot.
// Implements: EVS-DEV-event-store-open/B — defines the entry-type id for the
//   first-boot lib-version event; boot-version events are substrate-internal
//   and must not be admitted to destinations as user events.
const String kLibVersionInitializedEntryType = 'lib_version_initialized';

/// Reserved id for the substrate-level lib-version-changed event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open` /
/// `_appendLibVersionEventToBackend` on upgrade.
// Implements: EVS-DEV-event-store-open/C — defines the entry-type id for the
//   version-transition lib-version event; boot-version events are substrate-
//   internal and must not be admitted to destinations as user events.
const String kLibVersionChangedEntryType = 'lib_version_changed';

/// Reserved id for the raw-path ingest-audit event emitted by
/// `EventStore.logRejectedBatch` and `_emitDuplicateReceivedInTxn`.
/// Registered here so the raw-path callers can read
/// `registeredVersion` from the registry instead of hardcoding.
const String kIngestAuditEntryType = 'ingest-audit';

/// Reserved id for the boot-time view-snapshot-promotion audit event
/// emitted by the snapshot-promotion pass.
const String kViewSnapshotPromotedEntryType = 'view_snapshot_promoted';

/// Reserved set of ids. `bootstrapAppendOnlyDatastore` auto-registers
/// these BEFORE iterating the caller-supplied entry-type list. A
/// caller-supplied id colliding with one of these throws `ArgumentError`
/// with an explicit "reserved" message (-D revised).
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
/// deletion / wedge recovery, plus retention-policy-applied per-sweep),
/// the bootstrap registry-initialized audit, the substrate-internal
/// lib-version boot events (initialized / changed), the raw-path
/// `ingest-audit` event (covering `logRejectedBatch` and
/// `_emitDuplicateReceivedInTxn`), and the `view_snapshot_promoted`
/// audit emitted by the boot-time snapshot-promotion pass. All have
/// `materialize: false` so they never hit any view; they exist only to
/// stamp an immutable event_log row for every covered mutation.
///
/// The lib-version entries (`lib_version_initialized`,
/// `lib_version_changed`) are appended raw by `_appendLibVersionEventToBackend`
/// (bypassing `EntryTypeRegistry`), but registering them here ensures:
///   1. `byId()` returns a non-null definition for tests that iterate the
///      full `kReservedSystemEntryTypeIds` set.
///   2. `SubscriptionFilter.matches` correctly gates them behind
///      `includeSystemEvents: true` via the `kReservedSystemEntryTypeIds`
///      membership check (which this list is the authoritative source for).
// Implements: EVS-DEV-event-store-open/B,C — lib-version boot events
//   registered here so byId() returns non-null and SubscriptionFilter gates
//   them correctly, even though they are appended raw (bypassing the registry).
const List<EntryTypeDefinition> kSystemEntryTypes = <EntryTypeDefinition>[
  EntryTypeDefinition(
    id: kSecurityContextRedactedEntryType,
    registeredVersion: 1,
    name: 'Security Context Redacted',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kSecurityContextCompactedEntryType,
    registeredVersion: 1,
    name: 'Security Context Compacted',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kSecurityContextPurgedEntryType,
    registeredVersion: 1,
    name: 'Security Context Purged',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kDestinationRegisteredEntryType,
    registeredVersion: 1,
    name: 'Destination Registered',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kDestinationStartDateSetEntryType,
    registeredVersion: 1,
    name: 'Destination Start Date Set',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kDestinationEndDateSetEntryType,
    registeredVersion: 1,
    name: 'Destination End Date Set',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kDestinationDeletedEntryType,
    registeredVersion: 1,
    name: 'Destination Deleted',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kDestinationWedgeRecoveredEntryType,
    registeredVersion: 1,
    name: 'Destination Wedge Recovered',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kRetentionPolicyAppliedEntryType,
    registeredVersion: 1,
    name: 'Retention Policy Applied',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kEntryTypeRegistryInitializedEntryType,
    registeredVersion: 1,
    name: 'Entry Type Registry Initialized',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kLibVersionInitializedEntryType,
    registeredVersion: 1,
    name: 'Lib Version Initialized',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kLibVersionChangedEntryType,
    registeredVersion: 1,
    name: 'Lib Version Changed',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kIngestAuditEntryType,
    registeredVersion: 1,
    name: 'Ingest Audit',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kViewSnapshotPromotedEntryType,
    registeredVersion: 1,
    name: 'View Snapshot Promoted',
    materialize: false,
  ),
];
