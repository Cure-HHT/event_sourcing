import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';

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
// Implements: EVS-PRD-portability/D — platform-divergent persistent storage
//   abstracted behind this Dart-side interface; the consuming application
//   supplies the concrete implementation per platform.
abstract class StorageBackend {
  const StorageBackend();

  /// Execute [body] inside a single atomic backend transaction. All
  /// `Txn`-bound writes performed within [body] SHALL commit together or
  /// SHALL roll back together on any thrown exception. The returned future
  /// completes with [body]'s return value on commit, or rethrows on rollback.
  ///
  /// Concrete backends SHALL invalidate the [Txn] handle when [body] returns
  /// or throws, so that a later out-of-scope use raises an error rather than
  /// silently writing against a closed transaction.
  Future<T> transaction<T>(Future<T> Function(Txn txn) body);

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
  // Implements: EVS-PRD-event-log/A — append to the append-only, immutable log.
  // Implements: EVS-PRD-event-log/B — stable total order via sequence counter.
  Future<AppendResult> appendEvent(Txn txn, StoredEvent event);

  /// Events for one aggregate, sorted by `sequence_number` ascending.
  // Implements: EVS-PRD-event-log/C — per-aggregate-per-authority order.
  // Implements: EVS-PRD-event-log/D — read events in order from any position.
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId);

  /// Events for one aggregate, read within [txn] so the result reflects
  /// writes already staged in the same transaction body. Sorted by
  /// `sequence_number` ascending. Used by callers that need hash-chain /
  /// no-op-detection reads to be coherent with the same-transaction append.
  // Implements: EVS-PRD-event-log/C — per-aggregate-per-authority order.
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Txn txn,
    String aggregateId,
  );

  /// All events, optionally sliced by `afterSequence` (exclusive) and
  /// `limit`, and optionally filtered by originator identity, entry type,
  /// and client-timestamp range. All supplied filters compose with AND.
  /// Returned in `sequence_number` order.
  ///
  /// [originatorHopId] matches `provenance[0].hopId` — the hop class of
  /// the originator (e.g. `'mobile-device'`, `'portal-server'`).
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
  // Implements: EVS-PRD-event-log/D — read events in order from any position.
  // Implements: EVS-DEV-find-all-events-extended-filters/A — entryType,
  //   clientTimestampStart, clientTimestampEnd optional named parameters.
  // Implements: EVS-DEV-find-all-events-extended-filters/C — filters AND-compose.
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
  Future<String?> readLatestEventHash(Txn txn);

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
  // Implements: EVS-PRD-event-log/D — read events in order from any position
  //   (transactional variant; result reflects staged writes in same txn).
  // Implements: EVS-DEV-find-all-events-extended-filters/B — same three
  //   optional parameters with same semantics on the transactional variant.
  // Implements: EVS-DEV-find-all-events-extended-filters/C — filters AND-compose.
  Future<List<StoredEvent>> findAllEventsInTxn(
    Txn txn, {
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
  Future<int> nextSequenceNumber(Txn txn);

  /// Current value of the per-device sequence counter — i.e., the
  /// `sequence_number` of the most recently-persisted event. Returns 0
  /// when no event has been appended yet. Non-transactional, read-only.
  Future<int> readSequenceCounter();

  // -------- Generic view storage (Phase 4.4) --------
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
    Txn txn,
    String viewName,
    String key,
  );

  /// Whole-row upsert into [viewName] at [key] inside [txn].
  Future<void> upsertViewRowInTxn(
    Txn txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  );

  /// Delete the row at [key] in [viewName] inside [txn].
  Future<void> deleteViewRowInTxn(Txn txn, String viewName, String key);

  /// Iterate rows in [viewName] with optional `limit` / `offset`.
  /// Non-transactional.
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  });

  /// Empty all rows in [viewName] inside [txn]. Other views are untouched.
  Future<void> clearViewInTxn(Txn txn, String viewName);

  // -------- View target versions (Phase 4.19) --------

  /// Read the persisted target version for [viewName]/[entryType], or `null`
  /// if no entry has been registered. Used by [rebuildView]
  Future<int?> readViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
  );

  /// Persist [targetVersion] for the [viewName]/[entryType] pair.
  /// Idempotent on repeat writes of the same value.
  Future<void> writeViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
    int targetVersion,
  );

  /// Read all entry-type → target-version entries for [viewName].
  /// Used by `rebuildView`'s strict-superset check.
  Future<Map<String, int>> readAllViewTargetVersionsInTxn(
    Txn txn,
    String viewName,
  );

  /// Remove every target-version entry for [viewName]. Used by
  /// `rebuildView` before re-recording, and by view drop helpers.
  Future<void> clearViewTargetVersionsInTxn(Txn txn, String viewName);

  // -------- FIFO (per destination) --------

  /// Append a batch-shaped entry to destination [destinationId]'s FIFO.
  /// The batch covers every event in [batch], which MUST be non-empty.
  /// The returned `FifoEntry` carries the backend-assigned
  /// `sequence_in_queue` and the constructed `event_ids` +
  /// `event_id_range` fields.
  ///
  /// The backend opens its own atomic transaction for the write so
  /// callers that are not already composing a larger transaction can
  /// enqueue in one call. Callers composing a larger transaction (e.g.,
  /// replay, fill_batch) use [enqueueFifoTxn] instead.
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
  /// - [nativeEnvelope] (native `esd/batch@1` path) — caller (typically
  ///   `fillBatch`) built the envelope identity from the local
  ///   `Source`. The metadata is persisted under `envelope_metadata`,
  ///   with `wire_payload = null` and `wire_format = "esd/batch@1"`.
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
  /// destination on first use so `anyFifoWedged`/`wedgedFifos` can
  /// iterate all known FIFOs.
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  });

  /// Transactional variant of [enqueueFifo]: participates in the
  /// surrounding transaction's atomicity so the FIFO-row write and the
  /// accompanying writes (e.g., fill_cursor advance in `fillBatch`) commit
  /// or roll back together. Same contract as [enqueueFifo] otherwise:
  /// rejects empty [batch], enforces the XOR `(wirePayload,
  /// nativeEnvelope)` precondition, mints a fresh v4-UUID `entry_id`,
  /// assigns monotonically-increasing `sequence_in_queue`, and registers
  /// the destination on first use.
  ///
  /// Implementations SHALL centralize row-construction logic here;
  /// [enqueueFifo] delegates to [enqueueFifoTxn] inside its own
  /// `transaction((txn) => ...)` wrapper.
  Future<FifoEntry> enqueueFifoTxn(
    Txn txn,
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
  /// `(destinationId, entryId)`. Does not change `final_status`.
  ///
  /// Implementations SHALL be a no-op (return without throwing) when the
  /// FIFO row identified by `entryId` does not exist in the destination's
  /// FIFO store, and SHALL be a no-op when the FIFO store for
  /// `destinationId` does not exist. This tolerates the
  /// drain/unjam + drain/delete race: drain `await send()`s outside a
  /// storage transaction, and a concurrent user operation may remove the
  /// target row before drain's subsequent `appendAttempt` transaction
  /// runs. Implementations SHALL emit a warning-level diagnostic when
  /// they no-op.
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  );

  /// Transition an entry to a terminal `final_status`. When [status] is
  /// [FinalStatus.sent] the entry's `sent_at` is also set. Entries
  /// transitioned to terminal status are retained forever as send-log
  /// records; they are never deleted.
  ///
  /// Implementations SHALL be a no-op (return without throwing) when the
  /// FIFO row identified by `entryId` does not exist in the destination's
  /// FIFO store, and SHALL be a no-op when the FIFO store for
  /// `destinationId` does not exist — see the matching
  /// note on [appendAttempt] for the race this closes. Implementations
  /// SHALL emit a warning-level diagnostic when they no-op.
  ///
  /// **Idempotent on matching already-final rows.** When the entry's
  /// current `final_status` equals [status] the call returns without
  /// throwing and without performing any additional write. This closes
  /// the at-least-once drain race: concurrent drainers may both reach
  /// `markFinal` after the first one succeeds; the second observes the
  /// already-correct terminal state and returns cleanly.
  ///
  /// **Throws `StateError` on a status mismatch.** When the entry is
  /// already terminal with a *different* status (e.g. already `sent`,
  /// asked to mark `wedged`) the implementations SHALL throw `StateError`
  /// with both the existing and requested statuses in the message —
  /// this signals real corruption and loud failure is correct.
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  );

  /// True iff any registered destination's FIFO head is `wedged`.
  Future<bool> anyFifoWedged();

  /// Summarize every destination whose head row is wedged.
  Future<List<WedgedFifoSummary>> wedgedFifos();

  // -------- Backend state (KV bookkeeping) --------

  /// Read the current schema version from `backend_state`. Returns 0 when
  /// the backend has never been written to.
  Future<int> readSchemaVersion();

  /// Write [version] into `backend_state` inside [txn]. Used by the schema
  /// migration path at boot; typical production flow writes the version once
  /// and leaves it alone until a migration.
  Future<void> writeSchemaVersion(Txn txn, int version);

  /// Read the per-destination fill cursor — the highest `sequence_number`
  /// that has been promoted into any FIFO row (null, sent, wedged, or tombstoned)
  /// for [destinationId]. Returns `-1` when no cursor value has yet been
  /// written, i.e., no row has yet been enqueued for this destination.
  ///
  /// Note: `-1` is both the default-when-unset sentinel and the only
  /// legal pre-start rewind value (e.g., `unjamDestination` rewinding a
  /// destination with no sent rows). Callers that need
  /// to distinguish "never written" from "explicitly rewound to -1" MUST
  /// do so via other bookkeeping; this method treats them as equivalent.
  ///
  /// Persisted under `backend_state` key `fill_cursor_<destinationId>`.
  /// Non-transactional, read-only.
  Future<int> readFillCursor(String destinationId);

  /// Write the per-destination fill cursor for [destinationId] to
  /// [sequenceNumber]. Opens its own atomic transaction. Callers that are
  /// already composing a larger transaction (e.g., fill_batch) SHALL use
  /// [writeFillCursorTxn] to keep the cursor advance co-atomic with the
  /// enqueue / sequence-counter writes it accompanies.
  Future<void> writeFillCursor(String destinationId, int sequenceNumber);

  /// Write the per-destination fill cursor for [destinationId] to
  /// [sequenceNumber] inside [txn]. Participates in the surrounding
  /// transaction's atomicity: on rollback the cursor reverts to its
  /// pre-transaction value.
  Future<void> writeFillCursorTxn(
    Txn txn,
    String destinationId,
    int sequenceNumber,
  );

  /// Read a single event by `event_id` within [txn]. Returns `null` when no
  /// event with that id is present. Used by ingest's idempotency check.
  /// Reads the unified event log; origin-appended events and ingest-
  /// appended events occupy a single store keyed by `sequence_number`.
  Future<StoredEvent?> findEventByIdInTxn(Txn txn, String eventId);

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

  /// Write [schedule] for [destinationId] inside its own atomic
  /// transaction. Callers already composing a transaction SHALL use
  /// [writeScheduleTxn] to keep the write co-atomic with adjacent
  /// schedule / FIFO mutations.
  Future<void> writeSchedule(
    String destinationId,
    DestinationSchedule schedule,
  );

  /// Transactional variant of [writeSchedule]: participates in the
  /// surrounding transaction's atomicity so a schedule write and the
  /// ops that accompany it (e.g. FIFO-store drop in
  /// `deleteDestination`) commit or roll back together.
  Future<void> writeScheduleTxn(
    Txn txn,
    String destinationId,
    DestinationSchedule schedule,
  );

  /// Delete the `schedule_<destinationId>` record inside [txn]. Used by
  /// `deleteDestination` to drop schedule state and the FIFO store in
  /// one atomic step.
  Future<void> deleteScheduleTxn(Txn txn, String destinationId);

  /// Drop the FIFO state for [destinationId] entirely inside [txn].
  /// Implementations SHALL remove every row associated with the
  /// destination (not just the currently-present records), so a
  /// subsequent `readFifoHead` on the same id returns null without
  /// seeing any trailing state. On backends that physically store
  /// FIFOs in per-destination containers (e.g., the sembast
  /// `fifo_<destinationId>` store) the container itself is dropped;
  /// on backends with a shared FIFO table (e.g., postgres
  /// `fifo_entries`) the matching rows are deleted.
  Future<void> deleteFifoStoreTxn(Txn txn, String destinationId);

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
  /// transitions are:
  ///
  /// - `null -> sent` — drain-terminal (SendOk).
  /// - `null -> wedged` — drain-terminal (SendPermanent, or
  ///   SendTransient at max attempts).
  /// - `null -> tombstoned` — `tombstoneAndRefill` on a still-pending
  ///   head.
  /// - `wedged -> tombstoned` — `tombstoneAndRefill` on a wedged head.
  ///
  /// Any other transition is illegal and SHALL throw `StateError`.
  /// `sent` and `tombstoned` are terminal end-states and cannot
  /// transition further. The one-way rule for `null -> terminal` owned
  /// by [markFinal] is subsumed here but the narrower contract on
  /// [markFinal] (null-targets only) remains in force for its callers.
  ///
  /// On `null -> sent` the implementation SHALL stamp
  /// `sent_at = DateTime.now().toUtc()`. On every other transition
  /// `attempts[]` and `sent_at` SHALL be left untouched —
  /// tombstoneAndRefill preserves the wedged row's attempts[] verbatim.
  ///
  /// Implementations SHALL throw [StateError] when the target row is
  /// absent — callers are expected to have verified existence (via
  /// [readFifoHead] for tombstoneAndRefill) before opening the
  /// transaction, so a missing row at this point indicates a
  /// concurrent delete race that these ops do not close.
  Future<void> setFinalStatusTxn(
    Txn txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
  );

  /// Delete every FIFO row on [destinationId] whose `sequence_in_queue`
  /// is strictly greater than [afterSequenceInQueue] AND whose
  /// `final_status IS null`. Returns the count of rows deleted.
  ///
  /// Used by `tombstoneAndRefill` to sweep the trail behind a
  /// tombstoned target in one transaction. Rows whose
  /// `final_status` is terminal (any of {sent, wedged, tombstoned})
  /// are left untouched regardless of their `sequence_in_queue` — per
  /// all non-null rows are retained forever.
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Txn txn,
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
