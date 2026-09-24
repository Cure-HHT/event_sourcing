# Changelog

## 0.5.0

This release changes what the library stores and sends (data format 2.0,
and the `esd/batch@2` batch envelope). A database written by an earlier
release does not open: `EventStore.open` refuses it by name with
`DatabaseResetRequiredError`, and it must be reset. A Postgres schema
created by an earlier release is dropped and provisioned again with
`PostgresBackend.provision`.

### Delivery: one drainer per database

- Within what its storage backend's drain lock supports, at most one
  delivery cycle commits queue changes for a database at a time
  (`EVS-PRD-destinations/V`). The drain lock rests on inputs the library
  does not audit (see Trust below): the Postgres lock session, the browser's
  Web Locks, and a single opener of a Sembast file outside the browser.
  `drain` and `fillBatch` are no
  longer exported (and are internal): the delivery cycle is the only thing
  that fills and drains destination queues, and `DestinationRegistry` is
  the only thing that changes them otherwise. Replace a direct `drain` or
  `fillBatch` call with a started `SyncCycle`; `await cycle()` runs one
  fill-and-drain pass.
- `SyncCycle` is a `final class` started with `SyncCycle.start({registry,
  clock, policy, policyResolver, cadence = 15 s, configurationVersion})`
  and stopped with `close({timeout})`; the constructor and its `backend:`
  and `source:` parameters are gone (native envelopes use the event
  store's source). A second cycle over the same database in one isolate
  throws `StateError`. A cycle that cannot take the drain lock stands by
  (`SyncCycleState.standby`) and takes over when it is released, without
  a restart; `start` never fails for contention or a transient database
  error, and throws `DrainLockConfigurationException` for a misconfigured
  lock connection. `isInFlight` is replaced by `state` (`SyncCycleState`),
  `stopped` and `stopCause` (the error that stopped a cycle for good). A
  `call()` that arrives during a pass returns once a pass that started
  after it has finished.
- The drain lock: on Postgres a session advisory lock on the backend's
  lock session, keyed by the database, the schema and the database
  identity; on Sembast outside the browser one holder per open database
  handle in an isolate; in the browser a Web Lock that only a visible tab
  requests. Each acquisition raises a drain epoch that every
  queue-changing transaction of the drainer checks (fencing).
- New exports: `SyncCycleState`, `DrainLock`, `DrainLockRequest`,
  `DrainLockLossReason`, `DrainLockUnavailableException`,
  `DrainLockLostException`, `DrainLockConfigurationException`,
  `DrainLockBackendClosedException` (a closed backend grants no drain
  lock; `start` throws it, and a running cycle over a closed backend
  stops), `DrainerDeclaration`, `DrainHeartbeat`, `RefillGuard`
  (`refillThrough`), `DeliveryStatus`, `DestinationDeliveryStatus`,
  `declaredConfiguration`, `configurationFingerprint`, and
  `DestinationRegistry.readDeliveryStatus()`, which any process can read.
- `SyncCycle.start` takes `configurationVersion` (1 to 128 characters,
  `SyncCycle.maxConfigurationVersionLength`, each a letter, a digit or one
  of `.` `_` `-` `+` `:` `@` `/`; anything else is an `ArgumentError`): part of the configuration the
  drainer declares for each destination, and recorded in wedge and
  recovery events. Change it when transform, predicate or batching code
  changes.
- `SyncCycle.unserved` and `UnservedReason` (`notRegisteredHere`,
  `deletedInStorage`, `registrationMismatch`,
  `refillAwaitsChangedConfiguration`; now a storage record with wire
  strings): a cycle fills and sends only destinations that are both
  registered in its process and persisted, and honours halt requests on
  every persisted destination.
- `EventStore.open`, `openForTest` and `bootstrapEventStore` lose
  `syncCycleTrigger`; `EventStore.syncCycleTrigger` and
  `EventStoreSyncCycleTrigger` are gone. A started cycle holds its event
  store's trigger slot, so every append, every committed registry
  operation and every committed action dispatch wakes it; a trigger never
  raises into the operation that fired it.
- `SyncPolicy.periodicInterval` is removed; `maxAttempts` must be at least
  one (a static policy below one is refused at `start`, a resolved one is
  logged and the pass sends nothing).
- `DrainLock` gains `handOverRequested` (every `StorageBackend`'s
  `DrainLock` implements it). `SyncCycle.start` in the browser runs under
  a Web Lock that follows the visible tab: a tab whose page becomes hidden
  finishes its sends in flight, waiting at most one cadence, hands the lock
  over and stands by; a page without `navigator.locks` (not a secure
  context) throws `DrainLockConfigurationException`.
- Subscribers receive a store's events in sequence order across concurrent
  transactions.

### Destination audits, wedges and halts

- Destination audit events carry a per-kind event type instead of
  `finalized`: `destination_registered`, `destination_start_date_set`,
  `destination_end_date_set`, `destination_deleted`,
  `destination_wedge_recovered` (exported as `kDestination<Kind>EventType`,
  with `kDestinationAuditAggregateType`); a filter that matched them on
  `eventTypes: {'finalized'}` names the kinds it wants. Every destination
  audit carries `data.database_id`.
- Security-context audits append with event types
  `security_context_redacted`, `security_context_compacted` and
  `security_context_purged` (was `finalized`); `kSecurityContext*EventType`
  and `kSecurityContextAuditAggregateType` are exported.
- New reserved entry type `system.destination_wedged` (event type
  `destination_wedged`): the drainer appends it in the transaction that
  wedges a queue head, recording the destination, the item, the cause
  (`WedgeCause`: permanent refusal, retry budget exhausted, operator halt),
  the attempt count and budget, the halt request it consumed, the drain
  epoch, and the declared configuration beside its fingerprint; never the
  error text. New exports: `kDestinationWedgedEntryType`,
  `kDestinationWedgedEventType`, `WedgeCause`, `WedgeRecord`.
- A wedging attempt whose transaction does not commit is recorded alone;
  each drain pass first wedges a pending head whose recorded attempts call
  for it, without sending it again. Lowering `maxAttempts` below an item's
  attempt count wedges it at the next pass.
- Operator halt: `DestinationRegistry.requestHalt(id, initiator:,
  purpose:)` (returns the request event's id) and `cancelHalt(id,
  initiator:)`; `HaltPurpose` (`pause`, `reconfigure`), `HaltRequest`,
  `SendFence`; reserved entry types `system.destination_halt_requested` and
  `system.destination_halt_cancelled`. The drainer honours a request by
  wedging the head before its next send; a request on an empty queue waits
  for a head; any wedge consumes an open request.
- The default destination-wedges view, `defaultDestinationWedgesSpec`
  (view `default_destination_wedges`), is registered by `EventStore.open`:
  one row per wedged destination, keyed by database identity and
  destination id, removed by the recovery or deletion that ends the wedge.
- `tombstoneAndRefill` requires a wedged head (`StateError` on a pending
  head, naming the halt that must come first) and rewinds the fill
  position below every item it removes; a recovery of a halt for
  reconfiguration is refused until the drainer declares a changed
  configuration, and leaves a refill guard. Its audit records the
  tombstoned item as `row_id` (was `target_row_id`), and the drain epoch,
  the drainer's configuration and fingerprints; `TombstoneAndRefillResult
  .targetRowId` is renamed `rowId`.
- `deleteDestination` refuses a pending head, retains delivered, wedged
  and recovered items as the delivery record, acts on the persisted
  hard-delete opt-in (the latest registration's), closes an open halt
  request and removes the destination's other records; its audit records
  `tombstoned_row_id`, `deleted_pending_count`, `allow_hard_delete` and
  `closed_halt_request_event_id`. A destination deleted and registered
  again starts a new registration.
- `setStartDate` enqueues nothing: it records a replay request that the
  drainer's next fill performs. Moving a start date earlier is refused
  while the head is wedged.
- The registry's operations act on persisted state from any process, with
  no local `Destination` required; an unknown id throws `ArgumentError`,
  committing only the registry check record. `addDestination` refuses an
  empty id or one containing `|`.
- `DestinationRegistry({required EventStore eventStore})`: the `backend:`
  parameter is removed.
- Re-sends after a recovery, a re-registration or an outcome that did not
  commit are at-least-once.

### Reserved events and ingest

- `EventStore.append` and `appendInTxn` throw `ArgumentError` for every
  reserved system entry type: only the library appends them.
- `EventStore.open` and `openForTest` register the reserved system entry
  types and the default destination-wedges view; a registry holding a
  reserved id or the view's name under any other definition, or a sealed
  projection registry without the view, is refused. Remove manual
  `kSystemEntryTypes` registration loops.
- Ingest refuses reserved events outside their declared shapes, malformed
  destination audits, and destination audits naming the receiver's own
  database that it does not hold: `IngestReservedEventRefused` with
  `ReservedEventRefusal`.
- `IngestDataFormatIncompatible` replaces `IngestLibFormatVersionAhead`, and
  `ingestEvent` applies the version checks too. New
  `IngestEntryTypeVersionUnpromotable`: an event of a lower version that a
  view's promoter steps do not lead from is refused by name, before any
  write. A malformed event version is reported as `IngestDecodeFailure`.
- Ingest verifies every event's own hash, whatever the length of its
  provenance, and refuses the event before any write when it differs from
  its `event_hash` (`ingestBatch` refuses the whole batch). `ingestBatch`
  hashes each record exactly as the envelope carried it, not the parsed
  event;
  `ingestEvent` hashes `incoming.toMap()` of the event its caller parsed.
  An event with only its origin provenance entry is checked too, so a sender
  that builds events by hand seals each record with `canonicalEventHash`
  (now exported) after its last change; an invented `event_hash` is refused.
  `IngestChainBroken` carries the failing link's `kind`
  (`ChainFailureKind`), whose new `eventHashMismatch` names this refusal,
  and `verifyEventChain` reports the same failure. The hash is an unkeyed
  SHA-256 and does not cover `aggregate_type`; an incoming event's
  `previous_event_hash` is not checked against the upstream log.
- `StoredEvent.fromMap` keeps every field the event hash covers as the
  record spelled it: `toMap` writes back the `client_timestamp` string (a
  timestamp without a fraction, or with a `+00:00` offset, is not
  rewritten) and the `initiator` map, including keys `Initiator` does not
  model. Both backends store and return those spellings, so a received
  record, its stored copy and the copy a relay forwards hash alike. A
  version map with a key other than `major` and `minor` is malformed.
- The event store writes every time it stamps (an event's
  `client_timestamp`, a provenance entry's `received_at`) in UTC, whatever
  zone an injected `clock` returns.

### Versions and the boot

- Versions are major.minor: `EntryTypeVersion` for entry types
  (`registeredVersion`, `PromoterSpec` steps, `rebuildView` targets) and
  `DataFormatVersion` for the library's data format
  (`LibVersion.dataFormat`, `2.0`, stamped on every event). A minor step's
  promoters may only be `DefaultField` (or none); a rename or drop is a
  major step. The event hash covers both versions. A promoted
  `DefaultField` no longer overwrites a field the row already carries.
  `PromoterRegistry.chainGap` reports why no chain exists.
- `EventStore.open` runs its boot in one transaction and decides an older
  library by data-format compatibility: a build of the same data-format
  major opens and appends `lib_version_changed`; another major throws
  `DataFormatIncompatibleError` before any write. `allowDowngrade` and
  `DowngradeRefusedError` are removed. Library-version events record the
  data format and, on initialization, the database identity
  (`EventStore.databaseId`); a missing or changed stored identity throws
  `DatabaseIdentityMismatchError`. `LibVersion` is exported.
- Boot-time snapshot promotion re-derives the affected rows from the log,
  and views registered over events already in the log are caught up at
  the next open of a build that registers them.
- The incompatible-generation guard: `EventStore.open` registers the
  build's data generation (its data-format major and each entry type's
  major) and throws `IncompatibleGenerationException` while a conflicting
  build is live; the database's generation record refuses an older major
  afterwards. New exports: `GenerationDescriptor`, `GenerationRecord`,
  `GenerationRegistration`, `GenerationStatus`,
  `IncompatibleGenerationException`, `GenerationFencedException`,
  `GenerationGuardConfigurationException`.
- `EventStore.open`, `openForTest` and `bootstrapEventStore` take an
  optional `onBootProgress` observer receiving `BootProgress` (phase
  `BootPhase.checks`, `promotion`, `catchUp` or `complete`; done; total;
  elapsed). It is not awaited, what it throws is logged, and a call from it
  into an event store while the boot runs throws `StateError`.
- `EventStore.openForTest` is `@visibleForTesting`, runs the same refusals
  and appends no library-version event.
- `rebuildView` refuses a target version that differs from the registered
  one, or an unregistered entry type.

### Storage contract

- Every `StorageBackend` member that writes is `@internal` (queue, view,
  view-target, schema-version, fill-position and schedule writers,
  `appendEvent`, `nextSequenceNumber`, and the records this release adds);
  a consumer uses the reads, `transaction` and `close`. A third-party
  backend implements the new members and marks its overrides of internal
  ones `@internal`. The library's delivery guarantees, views and
  security-context records hold only while its persisted state changes
  through the library's operations.
- An application that kept its own state by writing view rows through
  backend writers, or by adding to the `PublishCollector` of a
  transaction, builds that state from `subscribe(filter, Events())` or
  `EventStore.read` instead, or registers a declarative `ProjectionSpec`
  whose view the library maintains.
- New abstract `StorageBackend` members, which a third-party backend
  implements with the same transactional, serializable semantics as the
  reference backends (the library's conformance suites under `test/`
  exercise them):
  - drain lock: `tryAcquireDrainLock`, `requestDrainLock`,
    `drainExclusionKey`, `readDrainEpochTxn`;
  - generation guard and boot: `registerGeneration`, `bootTransaction`,
    `readDataGenerationTxn`, `writeDataGenerationTxn`,
    `readOrCreateDatabaseIdTxn`, `readDatabaseIdTxn`, `readBootCheckTxn`,
    `writeBootCheckTxn`;
  - delivery records: `read`/`write`/`clear` of the halt request, wedge
    record, send fence, refill guard and replay request
    (`readHaltRequestTxn`, `writeWedgeRecordTxn`, `clearSendFenceTxn`, ...),
    `readRegistryCheckTxn`, `writeRegistryCheckTxn`,
    `readDrainerDeclarationTxn`, `writeDrainerDeclarationTxn`,
    `readDrainHeartbeatTxn`, `writeDrainHeartbeatTxn`;
  - queues and schedules: `appendAttemptTxn`, `retireQueueTxn`,
    `readFifoHeadTxn`, `readFillCursorTxn`, `readScheduleTxn`,
    `listSchedules`, `listSchedulesTxn`;
  - views and reads: `markViewTargetBehindInTxn`,
    `clearViewTargetBehindInTxn`, `readViewTargetBehindInTxn`,
    `readViewTargetsForEntryTypeInTxn`, `readEventsReverseInTxn`.
- Removed: `appendAttempt`, `markFinal`, `writeFillCursor`,
  `deleteFifoStoreTxn`, and the non-transactional `enqueueFifo` and
  `writeSchedule`. `setFinalStatusTxn` takes a non-null status and allows
  exactly pending to sent, pending to wedged and wedged to tombstoned.
  `deleteNullRowsAfterSequenceInQueueTxn` returns `TrailSweepResult`.
  Committed transactions are serializable; a backend may run a
  transaction body more than once.
- New exported value types: `TrailSweepResult`, `QueueRetirement`,
  `ReplayRequest`, `RegistryCheck`, `BootCheck`, `HaltRequest`,
  `SendFence`. `DestinationSchedule` gains `registrationId` and
  `allowHardDelete`. New public reads: `listSchedules`,
  `readViewTargetsForEntryTypeInTxn`, `readViewTargetBehindInTxn`.
- `findAllEvents` and `findAllEventsInTxn` treat `clientTimestampEnd` as an
  exclusive bound, on both reference backends: an event whose
  `client_timestamp` equals the end is not returned, so consecutive windows
  `[a, b)` and `[b, c)` never return the same event. `clientTimestampStart`
  stays inclusive. A caller that relied on an inclusive end passes an end
  one microsecond later. Both bounds compare instants, so on Sembast an
  event within the same millisecond as a bound falls on the correct side of
  it. So do the Sembast security-context store's retention cutoffs
  (`findOlderThanInTxn`, `findUnredactedOlderThanInTxn`) and `queryAudit`'s
  `from` and `to`.
- `EventStore.appendInTxn` requires the `PublishCollector` its
  `runTransaction` body received.
- `debugLogSink` is removed; library log lines go to `dart:developer` and
  to `package:logging` loggers named `event_sourcing.<component>`.
- Internal: `PostgresBackend.pool` (use
  `PostgresIdempotencyStore.forBackend`), `SembastBackend.unwrapSembastTxn`,
  `PublishCollector.add` and `addRowChanges`, `SembastBackendTestSupport`
  (no longer exported), the security-context store mutators,
  `EventStoreBundle.setViewTargetVersion`.

### Postgres

- `PostgresBackend.open` runs no DDL. Provision the schema once per
  deployment with `PostgresBackend.provision(url, ...)`, as the role that
  owns the schema (or `open(provisionSchema: true)` in development); `open`
  throws `PostgresSchemaIncompatibleException` for an unprovisioned or
  unsupported schema. `postgresBackendSchemaVersion` is replaced by
  `postgresSchemaVersion` and `postgresMinCompatibleSchemaVersion`.
- Instances run as a runtime role holding exactly the privileges of the
  new `postgresRuntimeRoleGrants`. The queue table carries a guard that,
  while it is in place (the schema owner can remove it), refuses every
  change outside the shapes of the library's own writes, in every session
  replication role, and any truncation.
- `PostgresBackend.open` gains `lockUrl`, `lockQueryTimeout`,
  `lockHeartbeat` and `provisionSchema`, and holds a dedicated lock
  connection (direct or session-mode proxy only; otherwise
  `LockSessionConfigurationException`); `bootLockWait` bounds the boot's
  waits. A backend whose generation is no longer admitted throws
  `GenerationFencedException` from every transaction
  (`PostgresBackend.generationStatus`).
- A transaction re-run after a serialization failure that wrote the
  sequence counter's table first takes that table's lock.
- The `events` table stores `client_timestamp_text`, the timestamp's
  string as the event hash covers it, beside the `client_timestamp`
  instant, and stores `initiator` as the event's record holds it;
  `queryAudit(initiator:)` matches the fields `Initiator` models. The
  column is part of schema version 1, so a database provisioned by an
  earlier 0.5.0 build is provisioned again.
- `PostgresIdempotencyStore.over` is `@visibleForTesting`; `forBackend` is
  fenced.

### Trust

- The delivery configuration is a trusted input, beside the storage
  backend and the destination's transport: each destination's filter
  (a predicate closure included) and transform, its send outcomes, the
  `SyncPolicy` (retry policy and attempt budget), the clock the delivery
  cycle computes its window from, and the `configurationVersion` the cycle
  is started with. The log records the outcome category and the budget of
  each wedge decision, and the declared configuration and its fingerprint
  in each wedge and recovery event. The clock's readings are not recorded,
  so its influence on which events are enqueued is unaudited, and
  `configurationVersion` is taken on faith to change whenever code the
  library cannot read changes.
- The generation guard and the drain lock rest on three deployment inputs
  the library does not audit: the Postgres lock-session path (one server
  session reaching the pool's server, database and schema, carrying
  keepalives; checked where possible when a backend opens), the browser's
  lock manager (Web Locks), and, outside the browser, a Sembast database
  file opened by one isolate of one process. `spec/roadmap/storage.md`
  records how each could be checked.

### Browser

- The tabs of an origin share one database under Web Locks: the
  generation guard, the boot and every transaction's write lock, and the
  drain lock. `SembastBackend` gains `bootLockWait`.
- New `TransactionRerunLimitException`: a sembast_web handle that cannot
  commit even with every other tab's writes held back; the application
  closes and reopens the database.
- `package:web` is a direct dependency.
