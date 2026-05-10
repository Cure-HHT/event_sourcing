import 'package:event_sourcing/src/entry_type_definition.dart';

/// Reserved id for the per-event security-context redaction audit event.
const String kSecurityContextRedactedEntryType = 'security_context_redacted';

/// Reserved id for the bulk-truncation (retention compact) audit event.
const String kSecurityContextCompactedEntryType = 'security_context_compacted';

/// Reserved id for the bulk-delete (retention purge) audit event.
const String kSecurityContextPurgedEntryType = 'security_context_purged';

/// Reserved id for the destination-registration audit event.
// Implements: REQ-d00129-J — destination registration audit.
const String kDestinationRegisteredEntryType = 'system.destination_registered';

/// Reserved id for the destination start-date set audit event.
// Implements: REQ-d00129-K — destination start_date set audit.
const String kDestinationStartDateSetEntryType =
    'system.destination_start_date_set';

/// Reserved id for the destination end-date set audit event (covers
/// deactivate).
// Implements: REQ-d00129-L — destination end_date set audit.
const String kDestinationEndDateSetEntryType =
    'system.destination_end_date_set';

/// Reserved id for the destination deletion audit event.
// Implements: REQ-d00129-M — destination deletion audit.
const String kDestinationDeletedEntryType = 'system.destination_deleted';

/// Reserved id for the wedge-recovery audit event emitted by
/// `DestinationRegistry.tombstoneAndRefill`.
// Implements: REQ-d00144-G — wedge recovery audit.
const String kDestinationWedgeRecoveredEntryType =
    'system.destination_wedge_recovered';

/// Reserved id for the retention-policy-applied audit event emitted by
/// `EventStore.applyRetentionPolicy` once per sweep.
// Implements: REQ-d00138-H — retention policy applied audit (per-sweep).
const String kRetentionPolicyAppliedEntryType =
    'system.retention_policy_applied';

/// Reserved id for the bootstrap audit event recording the
/// `EntryTypeRegistry`'s id->registered_version map. Emitted once per
/// `bootstrapAppendOnlyDatastore` call after `EventStore` construction
/// and before destination registration; deduped by content so a same-
/// version reboot no-ops while a schema bump emits a new event.
// Implements: REQ-d00134-E+F — bootstrap registry-initialized audit.
const String kEntryTypeRegistryInitializedEntryType =
    'system.entry_type_registry_initialized';

/// Reserved id for the substrate-level lib-version-initialized event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open` /
/// `_appendLibVersionEventToBackend` on first boot.
// Implements: EVS-DEV-event-store-open — boot-version events are substrate-
//   internal and must not be admitted to destinations as user events.
const String kLibVersionInitializedEntryType = 'lib_version_initialized';

/// Reserved id for the substrate-level lib-version-changed event.
/// Appended raw (bypassing EntryTypeRegistry) by `EventStore.open` /
/// `_appendLibVersionEventToBackend` on upgrade.
// Implements: EVS-DEV-event-store-open — boot-version events are substrate-
//   internal and must not be admitted to destinations as user events.
const String kLibVersionChangedEntryType = 'lib_version_changed';

/// Reserved set of ids. `bootstrapAppendOnlyDatastore` auto-registers
/// these BEFORE iterating the caller-supplied entry-type list. A
/// caller-supplied id colliding with one of these throws `ArgumentError`
/// with an explicit "reserved" message (REQ-d00134-D revised).
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
};

/// The twelve reserved system entry-type definitions covering security-
/// context lifecycle events (redacted / compacted / purged), config-
/// change audit events (destination registration / start_date / end_date /
/// deletion / wedge recovery, plus retention-policy-applied per-sweep),
/// the bootstrap registry-initialized audit, and the substrate-internal
/// lib-version boot events (initialized / changed). All have
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
// Implements: REQ-d00138-D+E+F+G — system entry types for redaction /
// compact / purge audit events.
// Implements: REQ-d00129-J+K+L+M — destination mutation audit entry types.
// Implements: REQ-d00144-G — wedge recovery audit entry type.
// Implements: REQ-d00138-H — retention policy applied audit entry type.
// Implements: REQ-d00134-E+F+G — bootstrap registry-initialized audit
//   entry type; system audits read entryTypeVersion from the registry.
// Implements: EVS-DEV-event-store-open — lib-version boot events registered
//   here so byId() returns non-null and SubscriptionFilter gates them
//   correctly, even though they are appended raw (bypassing the registry).
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
];
