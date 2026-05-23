// Implements: EVS-PRD-library-charter — this barrel is the complete public
//   surface of the library; exporting it constitutes the library's contract
//   across all assertions (A–I) of the charter.

/// Reactive, append-only event-sourcing substrate.
///
/// Provides storage, sync, ingest, action dispatch, projections, and live
/// subscriptions. Cross-platform (iOS, Android, macOS, Windows, Linux, Web).
///
/// ## Key types
///
/// - `EventStore` — open, append, subscribe, close.
/// - `EventDraft` — input value returned by `Action.execute`.
/// - `StoredEvent` — immutable event record with hash-chain fields.
/// - `ProjectionRegistry` — declarative view specs registered before open.
/// - `PromoterRegistry` — entry-type promotion chains for schema migration.
/// - `AggregateProjectionSpec` / `TableProjectionSpec` — view spec shapes.
/// - `SubscriptionFilter` — filter by aggregate type, entry type, event type.
/// - `SubscriptionMode` — sealed: `Events` (raw) or `AggregateMode` (view).
/// - `Update` — sealed stream element: `Snapshot`, `EndOfReplay`, `Delta`, `Tombstone`.
/// - `DowngradeRefusedError` — thrown by `EventStore.open` on lib downgrade.
/// - `EntryTypeVersionDowngradeError` — thrown by `EventStore.open` when any
///   entry type's `registeredVersion` is below its stored target.
///
/// ## Quick start
///
/// ```dart
/// import 'package:event_sourcing/event_sourcing.dart';
/// import 'package:sembast/sembast_io.dart';
///
/// // Build a projection registry before opening the store.
/// final projections = ProjectionRegistry()
///   ..register(AggregateProjectionSpec(
///     viewName: 'invoices',
///     interest: SubscriptionFilter(aggregateTypes: {'Invoice'}),
///     tombstoneEventTypes: {'invoice_cancelled'},
///   ));
///
/// // Open the store (runs lib-version boot check).
/// final db = await databaseFactoryIo.openDatabase('data.db');
/// final store = await EventStore.open(
///   storage: SembastBackend(db),
///   projections: projections,
/// );
///
/// // Append an event. The substrate stamps entry_type_version from
/// // the registry's registeredVersion for 'invoice_created'.
/// await store.append(
///   aggregateId: 'inv-001',
///   aggregateType: 'Invoice',
///   entryType: 'invoice_created',
///   eventType: 'finalized',
///   data: {'amount': 100},
///   initiator: const UserInitiator('user-1'),
/// );
///
/// // Subscribe to live view updates.
/// final stream = store.subscribe<Map<String, Object?>>(
///   SubscriptionFilter(aggregateTypes: {'Invoice'}),
///   AggregateMode(
///     viewName: 'invoices',
///     mapper: (row) => row,
///   ),
/// );
/// await for (final update in stream) {
///   switch (update) {
///     case Snapshot(:final value):    print('snapshot: $value');
///     case EndOfReplay(:final sequence):
///       print('replay complete at sequence $sequence');
///     case Delta(:final value):       print('delta: $value');
///     case Tombstone(:final aggregateId):
///       print('deleted: $aggregateId');
///   }
/// }
///
/// await store.close();
/// ```
///
library;

// Provenance types — re-export from the provenance package for convenience
// (Phase 4.9, CUR-1154). ProvenanceEntry is exported so consumers of the
// returned type without depending on the provenance package directly.
export 'package:provenance/provenance.dart' show BatchContext, ProvenanceEntry;

// Actions module — trusted-boundary command/intent layer (formerly the
// audited_actions package; merged into event_sourcing via Phase C of the
// consolidation, CUR-1192).
export 'src/actions/action.dart' show Action;
export 'src/actions/action_context.dart' show ActionContext;
export 'src/actions/action_dispatcher.dart' show ActionDispatcher;
export 'src/actions/action_registry.dart' show ActionRegistry;
export 'src/actions/action_submission.dart' show ActionSubmission;
export 'src/actions/authorization_decision.dart'
    show AuthorizationDecision, Allow, Deny, DenyReason;
export 'src/actions/authorization_policy.dart' show AuthorizationPolicy;
export 'src/actions/bootstrap_audited_actions.dart'
    show bootstrapAuditedActions;
export 'src/actions/denial_events.dart'
    show
        denialUnknownAction,
        denialParseDenied,
        denialValidationDenied,
        denialAuthorizationDenied,
        denialExecutionFailed,
        sanitizeErrorMessage;
export 'src/actions/deny_all_authorization_policy.dart'
    show DenyAllAuthorizationPolicy;
export 'src/actions/dispatch_result.dart'
    show
        DispatchAuthorizationDenied,
        DispatchExecutionFailed,
        DispatchIdempotencyHit,
        DispatchParseDenied,
        DispatchResult,
        DispatchSuccess,
        DispatchUnknownAction,
        DispatchValidationDenied;
export 'src/actions/execution_result.dart' show ExecutionResult;
export 'src/actions/idempotency.dart'
    show defaultIdempotencyTtl, Idempotency, IdempotencyEntry;
export 'src/actions/idempotency_errors.dart' show MissingIdempotencyKeyError;
export 'src/actions/idempotency_store.dart'
    show IdempotencyStore, InMemoryIdempotencyStore;
export 'src/actions/permission.dart' show Permission;
export 'src/actions/principal.dart'
    show Principal, UserPrincipal, AnonymousPrincipal;
export 'src/actions/scope_value.dart'
    show BoundScope, ScopeValue, TotalWildcardScope, ValueWildcardScope;

// bootstrapAppendOnlyDatastore — single entry point for app main() to wire
// the storage backend, EntryTypeRegistry, destinations, security context
// store, and EventStore. Returns an AppendOnlyDatastore facade.
export 'src/bootstrap.dart'
    show AppendOnlyDatastore, bootstrapAppendOnlyDatastore;

// Core configuration
export 'src/core/config/datastore_config.dart';

// Exceptions
export 'src/core/errors/datastore_exception.dart';
export 'src/core/errors/sync_exception.dart';

// Destinations — per-destination routing contract (Phase 4, CUR-1154).
// FakeDestination lives in test/test_support/ and is intentionally NOT
// exported.
export 'src/destinations/batch_envelope_metadata.dart'
    show BatchEnvelopeMetadata;
export 'src/destinations/destination.dart' show Destination;
export 'src/destinations/destination_registry.dart' show DestinationRegistry;
export 'src/destinations/destination_schedule.dart'
    show DestinationSchedule, SetEndDateResult, TombstoneAndRefillResult;
export 'src/destinations/subscription_filter.dart'
    show SubscriptionFilter, SubscriptionPredicate;
export 'src/destinations/wire_payload.dart' show WirePayload;

// Entry Type Registry — maps entry_type ids to EntryTypeDefinition metadata
// consumed by the materializer and EventStore registry.
export 'src/entry_type_definition.dart' show EntryTypeDefinition;
export 'src/entry_type_registry.dart' show EntryTypeRegistry;

// EventDraft — input value type for Action.execute return value and
// appendWithSecurity call (Phase 5, CUR-1192).
export 'src/event_draft.dart' show EventDraft;
export 'src/event_store.dart'
    show
        DowngradeRefusedError,
        EntryTypeVersionDowngradeError,
        EventStore,
        EventStoreSyncCycleTrigger,
        RetentionResult;

// Ingest types — error types, result types, and chain verdict
// (Phase 4.9, CUR-1154).
export 'src/ingest/batch_envelope.dart' show BatchEnvelope;
export 'src/ingest/chain_verdict.dart'
    show ChainFailure, ChainFailureKind, ChainVerdict;
export 'src/ingest/ingest_errors.dart'
    show
        IngestChainBroken,
        IngestDecodeFailure,
        IngestEntryTypeVersionAhead,
        IngestIdentityMismatch,
        IngestLibFormatVersionAhead;
export 'src/ingest/ingest_result.dart'
    show IngestBatchResult, IngestOutcome, PerEventIngestOutcome;

// Projections — declarative view specs, the registry that holds them, and
// the parameterized rebuild helper. The legacy Materializer/EntryPromoter
// abstractions are removed (CUR-1317 Task 22); projections now use the
// declarative ProjectionSpec/PromoterRegistry model. MapEntryTypeDefinitionLookup
// is intentionally NOT exported — it lives under test/test_support/ so
// production code cannot depend on it.
export 'src/projections/rebuild.dart' show rebuildView;
// Callers create AggregateProjectionSpec / TableProjectionSpec values,
// register them in a ProjectionRegistry, and pass the registry to
// bootstrapAppendOnlyDatastore or directly to EventStore.
export 'src/projections/projection_spec.dart'
    show AggregateProjectionSpec, ProjectionSpec, TableProjectionSpec;
export 'src/projections/projection_registry.dart' show ProjectionRegistry;
// Projection primitives consumed by AggregateProjectionSpec / TableProjectionSpec
// when an app declares its own projections. Re-exported because they are the
// public API surface for defining projection shape, not internal implementation.
export 'src/projections/primitives/row_key.dart'
    show AggregateIdKey, CompositeKey, RowKeyExtractor;
export 'src/projections/primitives/row_data.dart'
    show PayloadField, RowDataExtractor, SelectedFields, WholePayload;
export 'src/projections/primitives/derived_field.dart'
    show
        ConstantValue,
        DerivedField,
        DerivedFieldComputation,
        DottedPathLookup,
        FallbackValue,
        FirstEventTimestamp;

// Promoters — entry-type version promotion chains for schema migration.
export 'src/promoters/promoter_registry.dart' show PromoterRegistry;
export 'src/promoters/promoter_spec.dart' show PromoterSpec;
export 'src/promoters/primitives/transform.dart'
    show
        DefaultField,
        DropField,
        RenameField,
        TransformChain,
        TransformPrimitive;

// Subscriptions — live-update stream primitives returned by
// EventStore.subscribe<T>().
export 'src/subscriptions/subscription_mode.dart'
    show AggregateMode, Events, SubscriptionMode;
export 'src/subscriptions/update.dart'
    show Delta, EndOfReplay, Snapshot, Tombstone, Update;

// Permissions module — role-permission matrix, materialized via the event
// log; YAML-seeded; failsafe bootstrap.
export 'src/permissions/authorization_policy_bootstrap.dart'
    show AuthorizationPolicyBootstrap, PolicyReady, PolicyFailSafe;
export 'src/permissions/bootstrap_action_permissions.dart'
    show bootstrapActionPermissions;
export 'src/permissions/bootstrap_role_assignments.dart'
    show RoleAssignmentSeedResult, bootstrapRoleAssignments;
export 'src/permissions/containment_resolver.dart'
    show ContainmentResolver, FindRowsInTxn;
export 'src/permissions/effective_authorization.dart'
    show EffectiveAuthorization;
export 'src/permissions/event_seed_applier.dart'
    show EventSeedApplier, SeedApplyResult;
export 'src/permissions/fail_safe_authorization_policy.dart'
    show FailSafeAuthorizationPolicy;
export 'src/permissions/permission_granted_payload.dart'
    show PermissionGrantedPayload;
export 'src/permissions/permission_revoked_payload.dart'
    show PermissionRevokedPayload;
export 'src/permissions/permission_seed.dart' show PermissionSeed;
export 'src/permissions/role_assigned_payload.dart' show RoleAssignedPayload;
export 'src/permissions/role_assignment_aggregate_id.dart'
    show roleAssignmentAggregateId;
export 'src/permissions/role_assignment_seed.dart'
    show RoleAssignmentSeed, RoleAssignmentSeedEntry;
export 'src/permissions/role_unassigned_payload.dart'
    show RoleUnassignedPayload;
export 'src/permissions/role_permission_grants_spec.dart'
    show rolePermissionGrantsSpec;
export 'src/permissions/user_role_scopes_spec.dart' show userRoleScopesSpec;
export 'src/permissions/scope_assignment.dart' show ScopeAssignment;
export 'src/permissions/scope_class_registry.dart'
    show ScopeClassRegistry, ScopeProjectionDescriptor;
export 'src/permissions/scope_class_spec.dart'
    show ContainmentRef, ScopeClassSpec;
export 'src/permissions/seed_validator.dart'
    show SeedInvalid, SeedValid, SeedValidationResult, SeedValidator;
export 'src/permissions/table_backed_authorization_policy.dart'
    show TableBackedAuthorizationPolicy;
export 'src/permissions/yaml_seed_loader.dart' show YamlSeedLoader;

// Security module — Phase 4.4 Tasks 11-15: EventSecurityContext value
// type, SecurityDetails caller input, SecurityRetentionPolicy sweeps,
// SecurityContextStore read-only surface, sembast concrete impl, reserved
// system entry types for redaction/compact/purge audit events.
export 'src/security/event_security_context.dart' show EventSecurityContext;
export 'src/security/security_context_store.dart'
    show AuditRow, PagedAudit, SecurityContextStore;
export 'src/security/security_details.dart' show SecurityDetails;
export 'src/security/security_retention_policy.dart'
    show SecurityRetentionPolicy;
export 'src/security/sembast_security_context_store.dart'
    show SembastSecurityContextStore;
export 'src/security/postgres_security_context_store.dart'
    show PostgresSecurityContextStore;
export 'src/security/system_entry_types.dart'
    show
        // Security-context lifecycle audits (Phase 4.4).
        kSecurityContextCompactedEntryType,
        kSecurityContextPurgedEntryType,
        kSecurityContextRedactedEntryType,
        // Destination-mutation audits (Phase 4.17).
        kDestinationDeletedEntryType,
        kDestinationEndDateSetEntryType,
        kDestinationRegisteredEntryType,
        kDestinationStartDateSetEntryType,
        kDestinationWedgeRecoveredEntryType,
        // Retention sweep audit (Phase 4.17).
        kRetentionPolicyAppliedEntryType,
        // Bootstrap registry-initialized audit (Phase 4.17 cross-phase
        // I-1 fix).
        kEntryTypeRegistryInitializedEntryType,
        // Substrate-internal lib-version boot events (Task 3 fix).
        kLibVersionChangedEntryType,
        kLibVersionInitializedEntryType,
        // Aggregates over all of the above.
        kReservedSystemEntryTypeIds,
        kSystemEntryTypes;

// Storage layer — StorageBackend contract, the SembastBackend +
// PostgresBackend concrete implementations, and the value types that
// flow through the contract (CUR-1154 for the contract; CUR-1330 for
// the Postgres backend). `ensurePostgresSchema` is intentionally
// library-private: only `PostgresBackend.open` calls it.
export 'src/storage/append_result.dart' show AppendResult;
export 'src/storage/attempt_result.dart' show AttemptResult;
export 'src/storage/fifo_entry.dart' show EventIdRange, FifoEntry;
export 'src/storage/final_status.dart' show FinalStatus;
export 'src/storage/initiator.dart'
    show Initiator, UserInitiator, AutomationInitiator, AnonymousInitiator;
export 'src/storage/postgres/postgres.dart';
export 'src/storage/sembast_backend.dart'
    show SembastBackend, SembastBackendTestSupport;
export 'src/storage/send_result.dart'
    show SendResult, SendOk, SendTransient, SendPermanent;
export 'src/storage/source.dart' show Source;
export 'src/storage/storage_backend.dart' show StorageBackend;
export 'src/storage/storage_exception.dart'
    show
        StorageCorruptException,
        StorageException,
        StoragePermanentException,
        StorageTransientException,
        classifyStorageException;
export 'src/storage/stored_event.dart' show StoredEvent;
export 'src/storage/txn.dart' show Txn;
export 'src/storage/wedged_fifo_summary.dart' show WedgedFifoSummary;

// Sync — backoff curve, drain loop, and top-level orchestrator (Phase 4,
// CUR-1154). Phase 5 wires triggers in clinical_diary that route into
// SyncCycle.call().
export 'src/sync/drain.dart' show ClockFn, drain;
export 'src/sync/fill_batch.dart' show fillBatch;
export 'src/sync/sync_cycle.dart' show SyncCycle;
export 'src/sync/sync_policy.dart' show SyncPolicy;
