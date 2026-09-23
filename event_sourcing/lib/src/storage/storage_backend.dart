import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal;

/// Abstract persistence contract for the event-sourcing substrate.
///
/// Two concrete reference implementations ship in-tree:
///
/// - `SembastBackend` — mobile / Flutter deployments (sembast-on-disk).
/// - `PostgresBackend` — server-side deployments (managed Postgres).
///
/// Both pass the same backend-agnostic conformance harness in
/// `test/storage/storage_backend_conformance.dart`. Additional backends
/// (IndexedDB, alternative SQL stores) may be supplied by downstream
/// applications under the same contract.
///
/// The contract is deliberately Dart-pure: no sembast or postgres types
/// leak into the interface, so either backend can be swapped in without
/// changing callers. Writes are grouped into [transaction] bodies to
/// guarantee atomicity across the four logical stores (event log, generic
/// view store, per-destination FIFOs, backend_state KV).
///
/// Queue items whose `final_status` is terminal (`sent`, `wedged`,
/// `tombstoned`) are retained for the database's lifetime, including after
/// their destination is deleted: they are the delivery record. Only items
/// whose `final_status` is null are ever deleted (by an operator recovery's
/// trail sweep, or by a deletion).
///
/// Every member that writes is marked `@internal`: only the library's own
/// operations call them. A consumer uses the reads, [transaction] (to run
/// its own reads in one transaction; an event-store append runs only inside
/// `EventStore.runTransaction`, which refuses any other transaction) and
/// [close].
///
/// A third-party implementation overrides the internal members, and no
/// code outside the implementing package calls them. The analyzer reports a
/// call from another package only when the member it resolves to carries
/// `@internal`, so the guard covers the shipped backends, and a backend in
/// a separate package keeps it only by marking each of its overrides of an
/// internal member `@internal` (declared under its package's `lib/src/`:
/// the annotation on a declaration in a public library is itself a
/// diagnostic). A backend declared in the application's own package is
/// covered by the precondition below alone.
///
/// Precondition of this trust boundary: the library's delivery guarantees,
/// its views and its security-context records hold only while its
/// persisted state (destination queues, the views it materializes, the
/// records it keeps beside them, such as fill positions, schedules, replay
/// requests, wedge records and the registry check record, and the security
/// context it stores beside each event) changes only through
/// the library's operations. The internal marking is an analyzer guard,
/// not a barrier: the consumer holds the backend (and, on Sembast, the
/// database it opened), and a direct write is invisible to the library.
// Implements: EVS-PRD-destinations/K
// every member that writes a queue, a view,
//   the persisted delivery state, the event sequence or the schema version
//   is marked @internal on the contract and on each override.
// Implements: EVS-PRD-destinations/L
// the dartdoc above states the precondition
//   of the storage trust boundary.
// Implements: EVS-PRD-portability/D
// platform-divergent persistent storage
//   abstracted behind this Dart-side interface; the consuming application
//   supplies the concrete implementation per platform.
abstract class StorageBackend {
  const StorageBackend();

  /// Execute [body] inside a single atomic backend transaction. All
  /// `Transaction`-bound writes performed within [body] SHALL commit together or
  /// SHALL roll back together on any thrown exception. The returned future
  /// completes with [body]'s return value on commit, or rethrows on rollback.
  ///
  /// Concrete backends SHALL invalidate the [Transaction] handle when [body] returns
  /// or throws, so that a later out-of-scope use raises an error rather than
  /// silently writing against a closed transaction.
  ///
  /// A backend MAY run [body] more than once before one run commits, to
  /// recover from a transient conflict: a serializable database re-runs it
  /// after a serialization failure, and a browser database re-runs it when
  /// another tab committed first. When it does, the runs SHALL be
  /// sequential (a run starts only after the previous one returned or threw),
  /// each run SHALL get a fresh [Transaction] handle, every write of a run
  /// that does not commit SHALL be rolled back, and the run whose commit
  /// completes the returned future SHALL be the last run started. A backend
  /// that fires its own change notifications (for example a queue watcher)
  /// SHALL fire only those of the committed run, after the commit. Callers
  /// therefore keep any state that describes a run inside [body], or reset it
  /// at the start of each run, so a discarded run leaves nothing behind.
  ///
  /// Committed transactions SHALL be serializable: their combined effect
  /// SHALL equal that of running the committed runs one at a time in some
  /// order. The library's decisions that read inside a transaction and
  /// write on what they read (the fill's compare-and-set, the destination
  /// registry's refusals, the recovery's and the deletion's head checks)
  /// hold only under this isolation. `PostgresBackend` runs every
  /// transaction at `SERIALIZABLE` and `SembastBackend` runs one
  /// transaction at a time; a backend that allows a weaker isolation
  /// breaks the delivery guarantees.
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body);

  // -------- Events --------

  /// Append [event] to the event log inside [txn]. Returns an
  /// [AppendResult] carrying the sequence number that was stamped on the
  /// event and the event hash that was persisted.
  ///
  /// [appendEvent] SHALL NOT advance the per-device sequence counter —
  /// callers MUST have reserved the event's sequence number via
  /// [nextSequenceNumber] in the same transaction, and [appendEvent]
  /// simply persists the event under that reservation. See
  /// [nextSequenceNumber] for the reservation contract.
  // Implements: EVS-PRD-event-log/A
  // append to the append-only, immutable log.
  // Implements: EVS-PRD-event-log/B
  // stable total order via sequence counter.
  @internal
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event);

  /// Events for one aggregate, sorted by `sequence_number` ascending.
  // Implements: EVS-PRD-event-log/C
  // per-aggregate-per-authority order.
  // Implements: EVS-PRD-event-log/D
  // read events in order from any position.
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId);

  /// Events for one aggregate, read within [txn] so the result reflects
  /// writes already staged in the same transaction body. Sorted by
  /// `sequence_number` ascending. Used by callers that need hash-chain /
  /// no-op-detection reads to be coherent with the same-transaction append.
  // Implements: EVS-PRD-event-log/C
  // per-aggregate-per-authority order.
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  );

  /// All events, optionally sliced by `afterSequence` (exclusive) and
  /// `limit`, and optionally filtered by originator identity, entry type,
  /// and client-timestamp range. All supplied filters compose with AND.
  /// Returned in `sequence_number` order.
  ///
  /// [originatorHopId] matches `provenance[0].hopId` — the hop class of
  /// the originator (e.g. `'mobile-device'`, `'relay-server'`).
  /// [originatorIdentifier] matches `provenance[0].identifier` — the
  /// originator's install identity. When both are supplied results SHALL
  /// match both (AND semantics); when neither is supplied no originator
  /// filtering is applied.
  ///
  /// [entryType] matches the event's `entry_type` exactly.
  /// [clientTimestampStart] / [clientTimestampEnd] are inclusive bounds on
  /// `event.client_timestamp` (compared in UTC).
  ///
  /// Concrete backends are expected to translate these filters to whatever
  /// query mechanism they support (indexed predicate, WHERE clause, etc.).
  // Implements: EVS-PRD-event-log/D
  // read events in order from any position.
  // Implements: EVS-DEV-find-all-events-extended-filters/A — entryType,
  //   clientTimestampStart, clientTimestampEnd optional named parameters.
  // Implements: EVS-DEV-find-all-events-extended-filters/C
  // filters AND-compose.
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  });

  /// Event hash of the highest-sequence-number event currently in the log,
  /// or null when the event log is empty. Read inside [txn] so the value
  /// reflects writes already staged in the same transaction body.
  ///
  /// Provided so that callers computing the hash-chain input for the next
  /// event (i.e., `previous_event_hash`) can read the tail under the same
  /// transaction that will append the new event. Reading the tail outside
  /// the transaction would make the chain vulnerable to a concurrent writer
  /// stamping a different previous-hash between the read and the commit.
  Future<String?> readLatestEventHash(Transaction txn);

  /// Events in sequence_number order, read within [txn] so the result
  /// reflects writes already staged in the same transaction body. Optionally
  /// sliced by [afterSequence] (exclusive) and [limit] so callers can stream
  /// the log in fixed-size chunks instead of materializing the whole log in
  /// memory.
  ///
  /// Also optionally filtered by [entryType] (exact match on `entry_type`)
  /// and [clientTimestampStart] / [clientTimestampEnd] (inclusive bounds on
  /// `client_timestamp`, compared in UTC). All supplied filters compose with
  /// AND. Concrete backends translate these to whatever query mechanism they
  /// support.
  ///
  /// Used by `rebuildView` so the event snapshot folded into the
  /// cache is coherent with the clear+upsert done under the same transaction.
  // Implements: EVS-PRD-event-log/D
  // read events in order from any position
  //   (transactional variant; result reflects staged writes in same txn).
  // Implements: EVS-DEV-find-all-events-extended-filters/B
  // same three
  //   optional parameters with same semantics on the transactional variant.
  // Implements: EVS-DEV-find-all-events-extended-filters/C
  // filters AND-compose.
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  });

  /// Reserve-and-increment the per-device sequence counter within [txn] and
  /// return the reserved value.
  ///
  /// Implementations SHALL advance the counter as a side effect so that a
  /// second call in the same transaction returns `current + 2`. Callers
  /// MUST pair this with a single [appendEvent] carrying the reserved
  /// value; [appendEvent] SHALL NOT re-advance the counter. This makes
  /// hash-chain-construction and the append a single atomic step with a
  /// caller-visible reservation that cannot be silently double-consumed.
  ///
  /// Calling [appendEvent] without a prior [nextSequenceNumber] reservation
  /// in the same transaction is a caller bug; implementations SHALL reject
  /// it with a clear error rather than advancing the counter implicitly
  /// (Phase-2 Prereq B, Option 1).
  @internal
  Future<int> nextSequenceNumber(Transaction txn);

  /// Current value of the per-device sequence counter — i.e., the
  /// `sequence_number` of the most recently-persisted event. Returns 0
  /// when no event has been appended yet. Non-transactional, read-only.
  Future<int> readSequenceCounter();

  // -------- Generic view storage --------
  //
  // Projection fold interpreters read and write view rows via these
  // methods. The view namespace is flat — addressed by `(viewName,
  // rowKey)` from the caller's perspective; the on-disk layout is a
  // per-backend implementation detail (sembast uses one store per
  // viewName; postgres uses a single `view_rows` table keyed by
  // `(view_name, row_key)`). The backend does not own schema for the
  // row payload; the fold interpreter and its readers interpret the
  // row map. Reserved view name: `security_context` (reserved for the
  // sidecar store).

  /// Read one row from [viewName] by [key] inside [txn], or null when
  /// the row is absent.
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  );

  /// Whole-row upsert into [viewName] at [key] inside [txn].
  @internal
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  );

  /// Delete the row at [key] in [viewName] inside [txn].
  @internal
  Future<void> deleteViewRowInTxn(Transaction txn, String viewName, String key);

  /// Iterate rows in [viewName] with optional `limit` / `offset`.
  /// Non-transactional.
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  });

  /// Read the rows of [viewName] whose row key is in [keys], in a SINGLE
  /// bulk query, returned as a map from row key to row payload. Keys with no
  /// row are omitted from the result; an empty [keys] yields an empty map
  /// (no query). Non-transactional, mirroring [findViewRows].
  ///
  /// This is the batched counterpart to a loop of per-key [readViewRowInTxn]
  /// calls: a row-scoped `AggregateMode` snapshot materializes its allow-list
  /// of aggregate ids in one round-trip instead of one transaction per id
  /// (the former N+1 storm). The key is the storage row key (for an
  /// `AggregateProjectionSpec`, the aggregate id), which `findViewRows` does
  /// not surface — hence the map return so callers can re-associate each row
  /// with the id they asked for and emit absent-key signals.
  // Implements: EVS-PRD-subscription/A
  // a filtered (row-scoped) materialized-
  //   state snapshot reads its allow-list in one batched call, not per id.
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  );

  /// Iterate rows in [viewName] inside [txn] optionally filtered by
  /// column equality. `where` is interpreted as "every key/value pair
  /// must match the row's column of that name." A null or empty
  /// [where] applies no filtering. Returns rows in unspecified order;
  /// callers needing sort must sort the result.
  ///
  /// Used by the authorization policy to enumerate `user_role_scopes`
  /// for the current `(userId, role)` inside the dispatch transaction
  /// so that the authorize-stage read sees the same snapshot as the
  /// execute-stage append.
  // Implements: EVS-PRD-permissions-as-events
  // closed-under-events
  //   evaluation: the authorize stage reads scope state from the same
  //   transaction the execute stage appends into, so the decision is
  //   reproducible from (events, lib_version) under the same backend
  //   transaction.
  // Implements: EVS-PRD-action-dispatch
  // single-transaction authorize +
  //   execute path requires a transactional multi-row view-read primitive.
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  });

  /// Empty all rows in [viewName] inside [txn]. Other views are untouched.
  @internal
  Future<void> clearViewInTxn(Transaction txn, String viewName);

  // -------- View target versions --------

  /// Read the persisted target version for [viewName]/[entryType], or `null`
  /// if no entry has been registered. Used by `rebuildView`
  Future<EntryTypeVersion?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  );

  /// Persist [targetVersion] for the [viewName]/[entryType] pair.
  /// Idempotent on repeat writes of the same value.
  @internal
  Future<void> writeViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
    EntryTypeVersion targetVersion,
  );

  /// Read all entry-type → target-version entries for [viewName].
  /// Used by `rebuildView`'s strict-superset check.
  Future<Map<String, EntryTypeVersion>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  );

  /// Remove every target-version entry for [viewName]. Used by
  /// `rebuildView` before re-recording, and by view drop helpers.
  @internal
  Future<void> clearViewTargetVersionsInTxn(Transaction txn, String viewName);

  // -------- FIFO (per destination) --------

  /// Append a batch-shaped entry to destination [destinationId]'s FIFO
  /// inside [txn]. The batch covers every event in [batch], which MUST be
  /// non-empty. The returned `FifoEntry` carries the backend-assigned
  /// `sequence_in_queue`, a fresh v4-UUID `entry_id` and the constructed
  /// `event_ids` + `event_id_range` fields.
  ///
  /// The write participates in the surrounding transaction's atomicity,
  /// so the queue item and the fill's other writes (the fill position, a
  /// cleared replay request) commit or roll back together. Only the fill
  /// enqueues; the contract has no enqueue outside a transaction.
  ///
  /// Exactly one of [wirePayload] / [nativeEnvelope] SHALL be non-null.
  /// The two payload shapes are mutually exclusive:
  ///
  /// - [wirePayload] (3rd-party path) — destination owns the wire format
  ///   and produced opaque bytes via `Destination.transform`. The bytes
  ///   MUST encode a JSON object; the decoded map is persisted under
  ///   `wire_payload`, with `wire_format = wirePayload.contentType` and
  ///   `envelope_metadata = null`. Drain hands the bytes back to
  ///   `Destination.send` verbatim.
  /// - [nativeEnvelope] (native `esd/batch@2` path) — caller (typically
  ///   `fillBatch`) built the envelope identity from the local
  ///   `Source`. The metadata is persisted under `envelope_metadata`,
  ///   with `wire_payload = null` and `wire_format = "esd/batch@2"`.
  ///   Drain reconstructs wire bytes deterministically (RFC 8785 JCS)
  ///   from `envelope_metadata` + `event_ids`-resolved events on each
  ///   send attempt.
  ///
  /// Implementations SHALL extract `event_ids` from
  /// `batch.map((e) => e.eventId)` and `event_id_range` from
  /// `(firstSeq: batch.first.sequenceNumber, lastSeq: batch.last
  /// .sequenceNumber)` — callers are responsible for passing a batch
  /// whose elements are in ascending `sequence_number` order
  /// (contiguity is enforced by the fill-batch path, not this method).
  ///
  /// Implementations SHALL assign a monotonically-increasing
  /// `sequence_in_queue` per FIFO, SHALL reject an empty [batch] with
  /// `ArgumentError`, SHALL reject a non-XOR `(wirePayload,
  /// nativeEnvelope)` pair with `ArgumentError`, and SHALL register the
  /// destination on first use so `hasFifoWedged`/`wedgedFifos` can
  /// iterate all known FIFOs.
  @internal
  Future<FifoEntry> enqueueFifoTxn(
    Transaction txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  });

  /// Return the head row of [destinationId]'s FIFO — the first row in
  /// `sequence_in_queue` order whose `final_status` is either `null`
  /// (pre-terminal; drain may attempt) or [FinalStatus.wedged] (blocking
  /// terminal; drain halts). Rows whose `final_status` is
  /// [FinalStatus.sent] or [FinalStatus.tombstoned] SHALL be skipped.
  /// Returns `null` when no such row exists (the FIFO is empty, or
  /// every row is terminal-passable).
  ///
  /// Callers enforce the wedge: `drain` returns without calling
  /// `Destination.send` when the returned row's `final_status` is
  /// [FinalStatus.wedged]. Recovery from a wedged head is
  /// `tombstoneAndRefill`. Returning the wedged row here
  /// (rather than filtering it out) lets UI surfaces observe the
  /// wedge via this single entry point without a separate
  /// `wedgedFifos` probe.
  Future<FifoEntry?> readFifoHead(String destinationId);

  /// [readFifoHead] inside [txn], so the result reflects writes staged in
  /// the same transaction and the check that reads it runs in the
  /// transaction it governs.
  @internal
  Future<FifoEntry?> readFifoHeadTxn(Transaction txn, String destinationId);

  /// Enumerate FIFO entries for [destinationId], ordered by
  /// `sequence_in_queue` ascending. Optionally sliced by
  /// [afterSequenceInQueue] (exclusive lower bound) and [limit] (cap on
  /// returned size, taken from the start of the ordered range).
  ///
  /// Returns typed [FifoEntry] objects — never raw maps. When
  /// [destinationId] has no registered FIFO store, returns an empty list
  /// (consistent with [readFifoHead] returning `null` for the same case).
  ///
  /// Callers SHALL NOT reach past this method to read FIFO entries — the
  /// underlying per-destination storage layout is an implementation detail
  /// of each backend (sembast uses a `fifo_<destinationId>` store; postgres
  /// uses a single `fifo_entries` table keyed by `destination_id`) and is
  /// not part of the public storage contract; this method is the
  /// supported enumeration API.
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  });

  /// Append [attempt] to the `attempts[]` list of the entry identified by
  /// `(destinationId, entryId)` inside [txn]. Does not change
  /// `final_status`.
  ///
  /// The drainer records an attempt only on the pending head it sent, in
  /// the same transaction as the status the attempt produces. Implementations
  /// SHALL throw [StateError] when the destination has no queue, when the
  /// entry is absent, and when the entry is terminal: no library operation
  /// removes or finalizes an item while the drainer is attempting it, so each
  /// of these is a defect, not a race.
  @internal
  Future<void> appendAttemptTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    AttemptResult attempt,
  );

  /// True iff any registered destination's FIFO head is `wedged`.
  Future<bool> hasFifoWedged();

  /// Summarize every destination whose head row is wedged.
  Future<List<WedgedFifoSummary>> wedgedFifos();

  // -------- Backend state (KV bookkeeping) --------

  /// Read the current schema version from `backend_state`. Returns 0 when
  /// the backend has never been written to.
  Future<int> readSchemaVersion();

  /// Write [version] into `backend_state` inside [txn]. Used by the schema
  /// migration path at boot; typical production flow writes the version once
  /// and leaves it alone until a migration.
  @internal
  Future<void> writeSchemaVersion(Transaction txn, int version);

  /// Read the per-destination fill cursor — the highest `sequence_number`
  /// that has been promoted into any FIFO row (null, sent, wedged, or tombstoned)
  /// for [destinationId]. Returns `-1` when no cursor value has yet been
  /// written, i.e., no row has yet been enqueued for this destination.
  ///
  /// Note: `-1` is both the default-when-unset sentinel and the only
  /// legal pre-start rewind value (an operator recovery of a head that
  /// carries the first event). Callers that need
  /// to distinguish "never written" from "explicitly rewound to -1" MUST
  /// do so via other bookkeeping; this method treats them as equivalent.
  ///
  /// Persisted under `backend_state` key `fill_cursor_<destinationId>`.
  /// Non-transactional, read-only.
  Future<int> readFillCursor(String destinationId);

  /// [readFillCursor] inside [txn], so the result reflects a cursor write
  /// staged in the same transaction.
  @internal
  Future<int> readFillCursorTxn(Transaction txn, String destinationId);

  /// Write the per-destination fill cursor for [destinationId] to
  /// [sequenceNumber] inside [txn]. Participates in the surrounding
  /// transaction's atomicity: on rollback the cursor reverts to its
  /// pre-transaction value.
  @internal
  Future<void> writeFillCursorTxn(
    Transaction txn,
    String destinationId,
    int sequenceNumber,
  );

  /// Read a single event by `event_id` within [txn]. Returns `null` when no
  /// event with that id is present. Used by ingest's idempotency check.
  /// Reads the unified event log; origin-appended events and ingest-
  /// appended events occupy a single store keyed by `sequence_number`.
  Future<StoredEvent?> findEventByIdInTxn(Transaction txn, String eventId);

  /// Read a single event by `event_id` outside any transaction. Returns
  /// `null` when no event with that id is present. The abstract contract
  /// requires an indexed single-row lookup (the sembast and postgres
  /// reference impls both use a unique index on `event_id`); not a scan.
  ///
  /// Callers needing read-coherence with writes staged in the same
  /// transaction body SHALL use [findEventByIdInTxn] instead.
  Future<StoredEvent?> findEventById(String eventId);

  // -------- Destination schedules --------

  /// Read the persisted `DestinationSchedule` for [destinationId], or
  /// null when no schedule has ever been written. Non-transactional.
  ///
  /// Schedules are persisted under `backend_state` key
  /// `schedule_<destinationId>` as the JSON form produced by
  /// `DestinationSchedule.toJson`.
  Future<DestinationSchedule?> readSchedule(String destinationId);

  /// [readSchedule] inside [txn], so a registry operation or a fill decides
  /// from the persisted schedule in the transaction it writes in.
  @internal
  Future<DestinationSchedule?> readScheduleTxn(
    Transaction txn,
    String destinationId,
  );

  /// Persist [schedule] for [destinationId] inside [txn], so a schedule
  /// write commits or rolls back with the registry operation's other
  /// writes and its audit event. Only the destination registry writes a
  /// schedule; the contract has no schedule write outside a transaction.
  @internal
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  );

  /// Delete the `schedule_<destinationId>` record inside [txn]. Used by
  /// `deleteDestination` beside [retireQueueTxn], so the schedule is
  /// removed in the transaction that retires the queue.
  @internal
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId);

  /// Retire [destinationId]'s queue inside [txn] when the destination is
  /// deleted.
  ///
  /// Implementations SHALL throw [StateError] and change nothing when the
  /// queue head is pending (`final_status` null): the head may be in
  /// delivery. Otherwise they SHALL tombstone a wedged head
  /// (`wedged -> tombstoned`), delete every item whose `final_status` is
  /// null (all of which lie behind the head), delete the destination's
  /// fill cursor, and keep every terminal item and the destination's
  /// `sequence_in_queue` counter: retained items keep their keys, and items
  /// enqueued after the destination is registered again continue above them.
  /// The destination stays known to [hasFifoWedged] and [wedgedFifos].
  ///
  /// Returns the tombstoned head's `entry_id` (null for a queue with no
  /// head) and the number of pending items deleted.
  @internal
  Future<QueueRetirement> retireQueueTxn(Transaction txn, String destinationId);

  // -------- Replay requests --------

  /// Read [destinationId]'s pending replay request inside [txn], or null.
  ///
  /// Persisted under `backend_state` key `replay_request_<destinationId>`.
  @internal
  Future<ReplayRequest?> readReplayRequestTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [request] as [destinationId]'s pending replay request inside
  /// [txn], replacing any earlier one.
  @internal
  Future<void> writeReplayRequestTxn(
    Transaction txn,
    String destinationId,
    ReplayRequest request,
  );

  /// Delete [destinationId]'s pending replay request inside [txn]. No-op
  /// when none is pending.
  @internal
  Future<void> clearReplayRequestTxn(Transaction txn, String destinationId);

  // -------- Wedge records --------

  /// Read [destinationId]'s wedge record inside [txn], or null when the
  /// destination has no open wedge.
  ///
  /// Persisted under `backend_state` key `wedge_<destinationId>`.
  @internal
  Future<WedgeRecord?> readWedgeRecordTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [record] as [destinationId]'s wedge record inside [txn],
  /// replacing any earlier one. Only the drainer writes one, in the
  /// transaction that wedges the queue head.
  @internal
  Future<void> writeWedgeRecordTxn(
    Transaction txn,
    String destinationId,
    WedgeRecord record,
  );

  /// Delete [destinationId]'s wedge record inside [txn]. No-op when none
  /// exists. An operator recovery and a deletion delete it in the
  /// transaction that ends the wedge.
  @internal
  Future<void> clearWedgeRecordTxn(Transaction txn, String destinationId);

  // -------- Registry check record --------

  /// Write [check] as the database-wide registry check record inside [txn],
  /// replacing the previous one.
  ///
  /// Persisted under `backend_state` key `registry_check`.
  @internal
  Future<void> writeRegistryCheckTxn(Transaction txn, RegistryCheck check);

  /// Read the database-wide registry check record inside [txn], or null
  /// when none has been written.
  @internal
  Future<RegistryCheck?> readRegistryCheckTxn(Transaction txn);

  /// Read a single FIFO row identified by [entryId] on [destinationId],
  /// or `null` when no such row exists (either the FIFO store was never
  /// written to, or the row was deleted). Non-transactional.
  ///
  /// Exposed as an explicit row-read by `entry_id` (distinct from
  /// [readFifoHead], which always returns the head). Used by
  /// integration tests and tooling that needs to inspect a specific
  /// FIFO row by id.
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId);

  /// Set the row's `final_status` to [status] inside [txn]. The legal
  /// transitions are exactly:
  ///
  /// - `null -> sent` — the drainer delivered the pending head.
  /// - `null -> wedged` — the drainer wedged the pending head.
  /// - `wedged -> tombstoned` — an operator recovery or a deletion retired
  ///   a wedged head.
  ///
  /// Implementations SHALL throw [StateError] and change nothing on every
  /// other pair, on a repeated status, and when the target row is absent.
  ///
  /// On `null -> sent` the implementation SHALL stamp
  /// `sent_at = DateTime.now().toUtc()`. On every other transition
  /// `attempts[]` and `sent_at` SHALL be left untouched, so a tombstoned
  /// row keeps the attempts of the wedge it retired.
  @internal
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus status,
  );

  /// Delete every FIFO row on [destinationId] whose `sequence_in_queue`
  /// is strictly greater than [afterSequenceInQueue] AND whose
  /// `final_status IS null`. Returns the count of rows deleted and the
  /// lowest `event_id_range.first_seq` among them, both computed in [txn].
  ///
  /// Used by `tombstoneAndRefill` to sweep the trail behind a
  /// tombstoned target in one transaction; the recovery rewinds the fill
  /// cursor below the lowest event the sweep removed. Rows whose
  /// `final_status` is terminal (any of {sent, wedged, tombstoned})
  /// are left untouched regardless of their `sequence_in_queue`.
  @internal
  Future<TrailSweepResult> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  );

  // -------- Reverse event scan --------

  /// Reverse stream of stored events, optionally filtered to a set of
  /// event types. Emits events in descending `sequence_number` order.
  ///
  /// Used by lifecycle scans that need to terminate on the first match
  /// without paging through the entire log. Consumers that only need the
  /// single most-recent match SHOULD `await for` and `break` (or return)
  /// on the first event.
  ///
  /// When [eventTypes] is supplied only events whose `event_type` is
  /// contained in the set are emitted; when null no type filtering is
  /// applied.
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes});

  // -------- Audit query --------

  /// Cross-store audit query joining the event log with the security-
  /// context sidecar, filtered by the supplied predicates and paginated
  /// by an opaque [cursor]. Returned rows are sorted by
  /// `recordedAt DESC, eventId DESC` so a stable forward walk is
  /// possible without ties-induced reordering across pages.
  ///
  /// Filters (all optional, AND-combined):
  ///
  /// - [initiator] — match `event.initiator` exactly.
  /// - [flowToken] — match `event.flowToken` exactly.
  /// - [ipAddress] — match `securityContext.ipAddress` exactly.
  /// - [from] / [to] — bound `securityContext.recordedAt` inclusively.
  ///
  /// [limit] SHALL be in `[1, 1000]`; values outside the range throw
  /// `ArgumentError`. [cursor] SHALL be either null (first page) or a
  /// value previously returned in [PagedAudit.nextCursor]; corrupt
  /// cursors throw `ArgumentError`. Pagination is lower-bound on the
  /// `(recordedAt, eventId)` tuple from the previous page's tail, so
  /// concurrent inserts at the head of the result set do not skew
  /// page contents.
  ///
  /// Implementations SHALL perform the join inside the storage layer —
  /// consumers SHALL NOT reach past the abstraction to perform their
  /// own joins. `SembastSecurityContextStore.queryAudit` is a thin
  /// delegator that forwards to this method.
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  });

  // -------- Lifecycle --------

  /// Close the backend and release all resources (reactive streams, database
  /// connection). Not safe to call concurrently with an in-flight transaction.
  /// Callers MUST await all outstanding operations before calling close.
  Future<void> close();
}
