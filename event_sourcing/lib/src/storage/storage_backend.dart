import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kDestinationSenderSucceededEntryType;
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/boot_check.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
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
/// Precondition of this trust boundary: the library's delivery guarantees, its
/// views and its security-context records hold only while its persisted state
/// (destination queues, the views it materializes, the records it keeps beside
/// them, such as fill positions, schedules, replay requests, transform failure
/// records, wedge records, halt requests, send fences, refill guards, the
/// sender channel records of its delivery channels, on Sembast the record of
/// the latest sequence the database authored and the record of whether it holds
/// a security finding, the registry check record, the database identity, the
/// generation records, the declared library roles, the view copies' identities,
/// definition fingerprints, fold watermarks and deletion marks, the fencing
/// epoch and the declared configuration, and the security context it stores
/// beside each event) changes only through the library's operations, and
/// reserved system events are appended only by the library's own operations.
/// The internal marking, here and on the event store's reserved append
/// operations, is an analyzer guard, not a barrier: the consumer holds the
/// backend (and, on Sembast, the database it opened), and a direct write is
/// invisible to the library.
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

  /// Execute [body] inside a single transaction for reads only: the
  /// transaction the storage reader runs. It follows the [transaction]
  /// contract, re-runs included, and a backend whose engine can run a
  /// transaction read-only runs it so, making the engine refuse any write
  /// in it. This default runs [transaction]; `PostgresBackend` runs a
  /// `READ ONLY` transaction outside its generation fence.
  @internal
  Future<T> readOnlyTransaction<T>(Future<T> Function(Transaction txn) body) =>
      transaction(body);

  /// Runs [body] over reads of the log that hold nothing an append waits
  /// for, and returns its result: the reads of the chain verification.
  ///
  /// The handle [body] receives is accepted, while [body] runs, by
  /// [readDatabaseIdTxn], [findAllEventsInTxn], [readEventsReverseInTxn],
  /// [findEventByIdInTxn], [findEventsForAggregateInTxn] and the chain
  /// lookups ([findEventsBySealedHashInTxn], [findEventsByPredecessorInTxn],
  /// [findEventsByOriginPositionInTxn]); every write refuses it. The reads
  /// see only committed events. A backend whose engine offers a snapshot
  /// that writers do not wait for reads in one (`PostgresBackend` runs one
  /// `REPEATABLE READ READ ONLY` transaction on a pool session of its own);
  /// one whose transactions exclude each other reads outside any
  /// transaction (`SembastBackend` reads through the database), so a later
  /// read may see an event committed after an earlier one, and the caller
  /// bounds what it reads by the local sequence numbers it fixed first.
  /// [body] runs once.
  // Implements: EVS-DEV-chain-verification/S
  // the chain verification reads through a primitive that holds no
  //   transaction an append waits for.
  @internal
  Future<T> nonBlockingRead<T>(Future<T> Function(Transaction reads) body);

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
  ///
  /// [event] is stored as [StoredEvent.toMap] writes it, every key that
  /// record carries included, and reads back the same. An event whose
  /// client timestamp, or a provenance entry's `received_at`, is not one a
  /// record may carry ([StoredEvent.requireWellFormedRecord]) throws
  /// [FormatException] and nothing is written.
  ///
  /// Once [appendEvent] returns, the chain lookups
  /// ([readLatestHeldAsAuthoredInTxn], [findEventsBySealedHashInTxn],
  /// [findEventsByPredecessorInTxn], [findEventsByOriginPositionInTxn],
  /// [readLatestEligibleVersionInTxn]) read inside [txn] see [event], and a
  /// rolled-back append leaves them as they were.
  // Implements: EVS-PRD-event-log/A
  // append to the append-only, immutable log.
  // Implements: EVS-DEV-event-record/A+B+C
  // the append refuses a client timestamp or received_at a record may not
  //   carry, and stores every key of the record, returning it unchanged on read.
  // Implements: EVS-PRD-event-log/B
  // stable total order via sequence counter.
  @internal
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event);

  /// Runs [body] as an inner unit of work nested inside [txn], isolating a
  /// server-side error [body] raises from the rest of [txn].
  ///
  /// On Postgres this issues `SAVEPOINT` on the transaction's session
  /// before running [body]; on [body]'s normal return it issues `RELEASE
  /// SAVEPOINT`, and on any throw it issues `ROLLBACK TO SAVEPOINT` before
  /// rethrowing the original error unchanged, so a server-side error inside
  /// [body] (a constraint violation, for example) leaves [txn] usable for
  /// further reads and writes rather than aborting it. On Sembast, which
  /// runs one transaction with no partial-rollback primitive, [body] simply
  /// runs as given: the fold-failure ordering that always precedes a call
  /// here (compute before write) means [body] never writes ahead of a
  /// failure it then throws on that backend. [body] is a fold, or a catch-up
  /// transaction's batched flush of the rows it folded, whose rejected
  /// write ends that transaction unwritten.
  ///
  /// A value [body] returns commits with the rest of [txn]. A throw from
  /// [body] propagates to the caller, wrapped as [RowWriteRejected] when a
  /// backend recognizes the error as a rejection of a row write's value
  /// (`EVS-DEV-view-convergence` Terms): on Postgres, a `ServerException`
  /// whose SQLSTATE is class 22, 23 or 54. Every other backend, and every
  /// other error, is rethrown as [body] raised it.
  // Implements: EVS-DEV-view-convergence/E
  // on Postgres, an always-stored event's fold into a copy runs in a
  //   savepoint, so a fold failure's server-side error does not abort the
  //   storing transaction.
  @internal
  Future<T> runInSavepointInTxn<T>(Transaction txn, Future<T> Function() body);

  /// Whether [runInSavepointInTxn] undoes every write its body made when
  /// the body throws, so several folds may share one savepoint and a
  /// failure in any of them leaves none of their writes. False by default
  /// (a body that throws keeps what it wrote before the throw), so each
  /// fold runs in a savepoint of its own; `PostgresBackend` is true.
  @internal
  bool get savepointRollsBackWrites => false;

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
  /// [clientTimestampStart] (inclusive) and [clientTimestampEnd] (exclusive)
  /// bound `event.client_timestamp` (compared in UTC), so consecutive
  /// windows `[a, b)` and `[b, c)` never both return an event at `b`.
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
  /// Provided so that a caller recording the storage-chain link of the next
  /// stored event (its last provenance entry's `previous_ingest_hash`) can
  /// read the tail under the same transaction that will store the new
  /// event. Reading the tail outside the transaction would let a concurrent
  /// writer store another event between the read and the commit.
  Future<String?> readLatestEventHash(Transaction txn);

  /// Events in sequence_number order, read within [txn] so the result
  /// reflects writes already staged in the same transaction body. Optionally
  /// sliced by [afterSequence] (exclusive) and [limit] so callers can stream
  /// the log in fixed-size chunks instead of materializing the whole log in
  /// memory.
  ///
  /// Also optionally filtered by [entryType] (exact match on `entry_type`)
  /// and [clientTimestampStart] (inclusive) / [clientTimestampEnd]
  /// (exclusive) bounds on `client_timestamp`, compared in UTC. All supplied filters compose with
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
  // methods. The view namespace is flat — addressed by `(copyId,
  // rowKey)` from the caller's perspective; the on-disk layout is a
  // per-backend implementation detail (sembast uses one store per
  // copyId; postgres uses a single `view_rows` table keyed by
  // `(copy_id, row_key)`). The backend does not own schema for the
  // row payload; the fold interpreter and its readers interpret the
  // row map. Reserved view name: `security_context` (reserved for the
  // sidecar store).

  /// Read one row from [copyId] by [key] inside [txn], or null when
  /// the row is absent.
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
  );

  /// Whole-row upsert into [copyId] at [key] inside [txn].
  @internal
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
    Map<String, dynamic> row,
  );

  /// Delete the row at [key] in [copyId] inside [txn].
  @internal
  Future<void> deleteViewRowInTxn(Transaction txn, String copyId, String key);

  /// Iterate rows in [copyId] with optional `limit` / `offset`.
  /// Non-transactional.
  Future<List<Map<String, dynamic>>> findViewRows(
    String copyId, {
    int? limit,
    int? offset,
  });

  /// Read the rows of [copyId] whose row key is in [keys], in a SINGLE
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
    String copyId,
    Set<String> keys,
  );

  /// [readViewRowsByKeys] inside [txn]: the transactional counterpart used
  /// where the row fetch must share the same storage transaction as a
  /// preceding state read (EVS-DEV-converging-view-reads/A), so no commit
  /// that lands between the two is visible to the row fetch.
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeysInTxn(
    Transaction txn,
    String copyId,
    Set<String> keys,
  );

  /// Iterate rows in [copyId] inside [txn] optionally filtered by
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
    String copyId, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  });

  /// Empty all rows in [copyId] inside [txn]. Other views are untouched.
  @internal
  Future<void> clearViewInTxn(Transaction txn, String copyId);

  /// Whole-row upsert into a `TableProjectionSpec` view copy [copyId] at
  /// [key], as [upsertViewRowInTxn], additionally stamping [row]'s producer
  /// -- the aggregate id of the event whose insert wrote it -- into a
  /// backend-owned index from [sourceAggregateId] to the row keys it
  /// produced in [copyId]. [findTableRowsBySourceAggregateInTxn] serves the
  /// outstanding-finding refresh (`EVS-PRD-materializer/E`, `/G`) from this
  /// index, by key, rather than a scan of the whole copy; nothing else
  /// compares the index. A key already indexed under a different source
  /// aggregate (a table row whose key an aggregate other than
  /// [sourceAggregateId] produces on a later insert) moves to the new one.
  /// [deleteViewRowInTxn], [clearViewInTxn], [deleteViewCopyRowsInTxn] and
  /// [deleteViewCopyRecordInTxn] retire a key's entry from the index
  /// together with the row itself, so the index never names a key whose
  /// row is gone.
  // Implements: EVS-PRD-materializer/E
  // a TableProjectionSpec row's producer is stamped into a backend-owned
  //   index, keyed for lookup by source aggregate.
  @internal
  Future<void> upsertTableViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  });

  /// The rows of a `TableProjectionSpec` view copy [copyId] produced by
  /// [sourceAggregateId] -- those [upsertTableViewRowInTxn] last indexed
  /// under it and that are still present -- read by the backend's own
  /// index inside [txn], never a scan of the whole copy.
  // Implements: EVS-PRD-materializer/E
  // the outstanding-finding refresh reads a source aggregate's rows from
  //   this index rather than scanning the view copy.
  @internal
  Future<List<Map<String, dynamic>>> findTableRowsBySourceAggregateInTxn(
    Transaction txn,
    String copyId,
    String sourceAggregateId,
  );

  /// Writes every row of [rows] into [copyId] inside [txn], each exactly as
  /// [upsertViewRowInTxn] writes it at its key: the batched counterpart a
  /// catch-up transaction flushes its folded rows through. The default
  /// makes one [upsertViewRowInTxn] call per row; a backend may override it
  /// with fewer statements to the same effect.
  @internal
  Future<void> upsertViewRowsInTxn(
    Transaction txn,
    String copyId,
    Map<String, Map<String, dynamic>> rows,
  ) async {
    for (final MapEntry(:key, value: row) in rows.entries) {
      await upsertViewRowInTxn(txn, copyId, key, row);
    }
  }

  /// Writes every row of [rows] into the table view copy [copyId] inside
  /// [txn], each with the producer its entry names: a non-null `source`
  /// exactly as [upsertTableViewRowInTxn] writes it, a null one as a row
  /// no producer indexes (as [deleteViewRowInTxn] followed by
  /// [upsertViewRowInTxn] leaves it). The default makes those calls per
  /// row; a backend may override it with fewer statements to the same
  /// effect.
  @internal
  Future<void> upsertTableViewRowsInTxn(
    Transaction txn,
    String copyId,
    Map<String, ({Map<String, dynamic> row, String? source})> rows,
  ) async {
    for (final MapEntry(:key, value: entry) in rows.entries) {
      final source = entry.source;
      if (source == null) {
        await deleteViewRowInTxn(txn, copyId, key);
        await upsertViewRowInTxn(txn, copyId, key, entry.row);
      } else {
        await upsertTableViewRowInTxn(
          txn,
          copyId,
          key,
          entry.row,
          sourceAggregateId: source,
        );
      }
    }
  }

  /// Deletes the rows of [keys] from [copyId] inside [txn], each exactly as
  /// [deleteViewRowInTxn] deletes it. The default makes one
  /// [deleteViewRowInTxn] call per key; a backend may override it with
  /// fewer statements to the same effect.
  @internal
  Future<void> deleteViewRowsInTxn(
    Transaction txn,
    String copyId,
    List<String> keys,
  ) async {
    for (final key in keys) {
      await deleteViewRowInTxn(txn, copyId, key);
    }
  }

  // -------- View copies --------
  //
  // Records the stored copies of registered views: one row per copy,
  // identified by a fresh [ViewCopy.copyId] and keyed for lookup by its
  // [ViewCopy.fingerprint] — the digest of the view's definition
  // (EVS-DEV-view-convergence). At most one copy of a fingerprint is not
  // marked for deletion at a time. Rows of a copy live in the generic view
  // store above, addressed by the copy's id in place of a view name.

  /// Create a new copy of [viewName] under [fingerprint], with initial
  /// [watermark], and return its freshly assigned copy id. Implementations
  /// SHALL refuse a second unmarked copy of one [fingerprint]: a create
  /// while an unmarked copy of that fingerprint already exists throws
  /// [StateError] and creates nothing.
  // Implements: EVS-DEV-view-convergence/A
  // at most one copy of a fingerprint that is not marked for deletion.
  @internal
  Future<String> createViewCopyInTxn(
    Transaction txn,
    String viewName,
    String fingerprint,
    int watermark,
  );

  /// Read every stored copy, of every view and every fingerprint, inside
  /// [txn]. Used to decide which copies no live registration names.
  Future<List<ViewCopy>> readViewCopiesInTxn(Transaction txn);

  /// Read the copy of [fingerprint] that is not marked for deletion, or
  /// null when none is stored.
  // Implements: EVS-DEV-view-convergence/A
  // at most one copy of a fingerprint that is not marked for deletion.
  Future<ViewCopy?> readUnmarkedViewCopyInTxn(
    Transaction txn,
    String fingerprint,
  );

  /// Persist [watermark] as the log position [copyId] has folded through.
  /// No-op when [copyId] names no stored copy.
  @internal
  Future<void> setViewCopyWatermarkInTxn(
    Transaction txn,
    String copyId,
    int watermark,
  );

  /// Mark [copyId] for deletion. Idempotent: a repeat mark, or a mark of a
  /// copy id that names no stored copy, is a no-op.
  @internal
  Future<void> markViewCopyForDeletionInTxn(Transaction txn, String copyId);

  /// Delete up to [limit] rows of [copyId] from the generic view store,
  /// returning the number of rows deleted. A return below [limit] means
  /// the copy held no more rows to delete.
  @internal
  Future<int> deleteViewCopyRowsInTxn(
    Transaction txn,
    String copyId, {
    required int limit,
  });

  /// Delete [copyId]'s own record from the view-copies store. Idempotent:
  /// a copy id that names no stored copy is a no-op. Does not touch the
  /// copy's rows — callers delete them first via [deleteViewCopyRowsInTxn].
  @internal
  Future<void> deleteViewCopyRecordInTxn(Transaction txn, String copyId);

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
  /// - [nativeEnvelope] (native path) — caller (the fill, or a resume
  ///   copying a retained delivery's) built the envelope identity from the
  ///   local `Source`. The metadata is persisted under `envelope_metadata`,
  ///   with `wire_payload = null` and `wire_format` the metadata's
  ///   ([BatchEnvelopeMetadata.wireFormat]: `esd/batch@3` for an item of a
  ///   delivery channel). Drain reconstructs wire bytes deterministically
  ///   (RFC 8785 JCS) from `envelope_metadata` + `event_ids`-resolved
  ///   events on each send attempt.
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
  ///
  /// A transform-failed flavour, for the fill's enqueue of a batch whose
  /// transform kept failing until the destination's retry budget was
  /// exhausted (`EVS-PRD-destinations/X`): [transformFailed] true, both
  /// [wirePayload] and [nativeEnvelope] null, [transformFailures] the
  /// count of transform failures the fill recorded for the batch (a
  /// positive int), and [wireFormat] (with, optionally, [transformVersion])
  /// the destination's configured wire format (since no payload or
  /// envelope supplies one). The returned row carries `wire_payload =
  /// null`, `envelope_metadata = null`, `transform_failed = true` and
  /// `transform_failures` the given count; its `final_status` is `null`
  /// (pending), so the drainer wedges it on its next read, without a
  /// send. Implementations SHALL reject [transformFailed] true together
  /// with a non-null [wirePayload] or [nativeEnvelope], a null
  /// [wireFormat], or a [transformFailures] below one, with
  /// `ArgumentError`.
  /// [resendsDeliveryNumber], when given, marks the enqueued row as a
  /// resend item for a receiver-behind resume
  /// (`EVS-DEV-delivery-resume/M`): the delivery number it resends, held
  /// immutable thereafter. Null (the default) for an ordinary item the
  /// fill enqueues.
  @internal
  Future<FifoEntry> enqueueFifoTxn(
    Transaction txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
    bool transformFailed = false,
    int? transformFailures,
    String? wireFormat,
    String? transformVersion,
    int? resendsDeliveryNumber,
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

  /// [listFifoEntries] of every item of [destinationId]'s queue, in
  /// `sequence_in_queue` order, read inside [txn], so the result reflects
  /// writes staged in the same transaction. The drainer reads it when it
  /// reads a receiver record: to find the deliveries it attempted on the
  /// registration and the pending items a resume or a new generation
  /// retires.
  @internal
  Future<List<FifoEntry>> listFifoEntriesTxn(
    Transaction txn,
    String destinationId,
  );

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
  ///
  /// A read of this database's queues themselves. The library's default
  /// destination-wedges view (`defaultDestinationWedgesSpec`) is a
  /// convention folded from the log: its rows for this database name the
  /// same wedged heads, and it also holds rows for other databases whose
  /// wedge events a peer forwarded, which no read of the local queues shows.
  Future<bool> hasFifoWedged();

  /// Summarize every destination whose head row is wedged.
  ///
  /// A read of this database's queues themselves, as [hasFifoWedged] is;
  /// the default destination-wedges view's rows whose `database_id` is this
  /// database's identity name the same (destination, item) pairs.
  Future<List<WedgedFifoSummary>> wedgedFifos();

  // -------- Backend state (KV bookkeeping) --------

  /// Read the current schema version from `backend_state`. Returns 0 when
  /// the backend has never been written to.
  Future<int> readSchemaVersion();

  /// Write [version] into `backend_state` inside [txn].
  ///
  /// The library itself never calls it: on Postgres,
  /// `PostgresBackend.provision` records the schema version together with
  /// the minimum compatible version, and that pair gates `open`, the
  /// generation guard and every transaction, so a write here changes what
  /// they decide; on Sembast the value is kept and read back, and nothing
  /// else reads it. The member exists for the contract harness.
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

  /// Every persisted `DestinationSchedule`, keyed by destination id: the
  /// destinations the database knows, whichever process registered them.
  /// Non-transactional.
  Future<Map<String, DestinationSchedule>> listSchedules();

  /// [listSchedules] inside [txn].
  @internal
  Future<Map<String, DestinationSchedule>> listSchedulesTxn(Transaction txn);

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

  // -------- Transform failure records --------

  /// Read [destinationId]'s transform failure record inside [txn], or null
  /// when its transform is not currently failing.
  ///
  /// Persisted under `backend_state` key `transform_failure_<destinationId>`.
  @internal
  Future<TransformFailureRecord?> readTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [record] as [destinationId]'s transform failure record inside
  /// [txn], replacing any earlier one. Only the fill writes one, in a
  /// transaction of its own that changes no other queue state.
  @internal
  Future<void> writeTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
    TransformFailureRecord record,
  );

  /// Delete [destinationId]'s transform failure record inside [txn]. No-op
  /// when none exists. The fill deletes it when the failing batch enqueues
  /// as a transform-failed item; deletion, an operator recovery, a
  /// receiver-behind resume and a new channel generation each delete it too,
  /// since each rewinds the fill position below the batch it names.
  @internal
  Future<void> clearTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
  );

  // -------- Halt requests --------

  /// Read [destinationId]'s open halt request inside [txn], or null when
  /// none is open.
  ///
  /// Persisted under `backend_state` key `halt_request_<destinationId>`.
  @internal
  Future<HaltRequest?> readHaltRequestTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [request] as [destinationId]'s open halt request inside [txn],
  /// replacing any earlier one. Only the destination registry writes one,
  /// in the transaction that appends the request event.
  @internal
  Future<void> writeHaltRequestTxn(
    Transaction txn,
    String destinationId,
    HaltRequest request,
  );

  /// Delete [destinationId]'s halt request inside [txn]. No-op when none is
  /// open. The transaction that closes the request (a cancellation, a wedge
  /// or a deletion) deletes it.
  @internal
  Future<void> clearHaltRequestTxn(Transaction txn, String destinationId);

  // -------- Send fences --------

  /// Read [destinationId]'s send fence inside [txn]: the last send the
  /// drainer started, or null when it started none since the destination
  /// was registered.
  ///
  /// Persisted under `backend_state` key `send_fence_<destinationId>`.
  @internal
  Future<SendFence?> readSendFenceTxn(Transaction txn, String destinationId);

  /// Write [fence] as [destinationId]'s send fence inside [txn], replacing
  /// the previous one. Only the drainer writes one, in the transaction
  /// immediately before a send.
  @internal
  Future<void> writeSendFenceTxn(
    Transaction txn,
    String destinationId,
    SendFence fence,
  );

  /// Delete [destinationId]'s send fence inside [txn]. No-op when none
  /// exists. A deletion deletes it.
  @internal
  Future<void> clearSendFenceTxn(Transaction txn, String destinationId);

  // -------- Sender channel records --------

  /// Read [destinationId]'s sender channel record inside [txn], or null when
  /// the destination has none (it serializes natively and is registered
  /// exactly while it has one).
  ///
  /// Persisted under `backend_state` key `sender_channel_<destinationId>`.
  // Implements: EVS-PRD-destinations/L
  // the sender channel record is persisted state the storage precondition
  //   names: it changes only through the library's operations.
  @internal
  Future<SenderChannelRecord?> readSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [record] as [destinationId]'s sender channel record inside
  /// [txn], replacing the previous one. The registration writes
  /// [SenderChannelRecord.initial]; afterwards only the drainer writes one,
  /// in the transaction that commits a send outcome, a resume or a new
  /// generation.
  // Implements: EVS-DEV-delivery-channel/E
  // the sender channel record changes only with the registration that
  //   writes it and, afterwards, a send outcome, a resume or a new
  //   generation; no other registry operation writes it.
  @internal
  Future<void> writeSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
    SenderChannelRecord record,
  );

  /// Delete [destinationId]'s sender channel record inside [txn]. No-op
  /// when none exists. A deletion deletes it.
  @internal
  Future<void> clearSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
  );

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

  // -------- Database identity and boot record --------

  /// Read the database identity inside [txn], or null when none is stored.
  ///
  /// Persisted under `backend_state` key `database_id`.
  @internal
  Future<String?> readDatabaseIdTxn(Transaction txn);

  /// Read the database identity inside [txn]; when none is stored, mint a
  /// random (version 4) UUID, store it and return it.
  ///
  /// The identity is written at most once: a later call in the same or a
  /// later transaction returns the stored value, and a mint in a
  /// transaction that does not commit leaves no identity behind.
  // Implements: EVS-DEV-event-store-open/F
  // the identity is minted at most once, inside the boot transaction.
  @internal
  Future<String> readOrCreateDatabaseIdTxn(Transaction txn);

  /// Write [check] as the boot record inside [txn], replacing the previous
  /// one.
  ///
  /// Persisted under `backend_state` key `boot_check`.
  @internal
  Future<void> writeBootCheckTxn(Transaction txn, BootCheck check);

  /// Read the boot record inside [txn], or null when none has been
  /// written.
  @internal
  Future<BootCheck?> readBootCheckTxn(Transaction txn);

  /// Run the body of `EventStore.open`'s boot as one transaction, with the
  /// guarantees of [transaction].
  ///
  /// Every backend decides here how the boot is ordered against concurrent
  /// appends to the same database: a backend whose transactions can abort
  /// each other orders the boot so that appends committed while it runs
  /// cannot starve it, and one whose transactions run one at a time runs
  /// [body] through [transaction]. The shared conformance harness cannot
  /// observe that ordering; it is the backend's responsibility, under the
  /// storage trust boundary.
  @internal
  Future<T> bootTransaction<T>(Future<T> Function(Transaction txn) body);

  /// Runs [body] as a bounded catch-up transaction for the view copy keyed
  /// by [copyKey] (its copy id, or its fingerprint before a copy is first
  /// created), trying that copy's lock without waiting, and returns its
  /// result; returns null, writing nothing, when the lock is not granted.
  ///
  /// A backend shared by several processes or tabs admits at most one
  /// catch-up transaction per copy at a time across every instance: on
  /// Postgres a transaction-scoped advisory try-lock ordered after a
  /// `SHARE` lock on `backend_state` (so the transaction's snapshot, taken
  /// by its first query after both locks, includes every append in
  /// flight), on the web a Web Lock requested with `ifAvailable`. A backend
  /// used by one process (Sembast outside the browser) uses an
  /// isolate-local lock, since only one instance can ever reach the
  /// database.
  // Implements: EVS-DEV-view-convergence/L
  // Implements: EVS-DEV-view-convergence/M
  @internal
  Future<T?> catchUpTransaction<T>(
    String copyKey,
    Future<T> Function(Transaction txn) body,
  );

  // -------- Data generation --------

  /// Register [descriptor] with the backend's incompatible-generation
  /// guard, before `EventStore.open`'s boot transaction.
  ///
  /// A backend shared by several processes or tabs implements the guard:
  /// it takes an exclusive boot lock for the database, inspects the
  /// generations its live instances hold, and throws
  /// [IncompatibleGenerationException] (releasing the boot lock, writing
  /// nothing) when one conflicts with [descriptor]; otherwise it registers
  /// [descriptor]'s components and returns while still holding the boot
  /// lock, which [GenerationRegistration.completeBoot] releases once the
  /// boot transaction committed. A backend used by one process returns a
  /// registration that holds nothing.
  // Implements: EVS-DEV-version-compatibility/F+G+H
  // the live guard runs before any write of the open; the boot transaction
  //   runs under the boot lock the registration holds.
  @internal
  Future<GenerationRegistration> registerGeneration(
    GenerationDescriptor descriptor,
  );

  /// Read the database's generation record inside [txn], or null when no
  /// boot has recorded one.
  ///
  /// Persisted under `backend_state` key `data_generation`.
  @internal
  Future<GenerationRecord?> readDataGenerationTxn(Transaction txn);

  /// Write [record] as the database's generation record inside [txn],
  /// replacing the previous one.
  @internal
  Future<void> writeDataGenerationTxn(Transaction txn, GenerationRecord record);

  // -------- Drain lock and drain records --------

  /// The value on which the backend excludes drainers for the database
  /// whose identity is [databaseId]: two backends in one isolate that
  /// report equal values drain the same database. A backend shared by
  /// several processes reports the scope of its drain lock (on Postgres,
  /// the database, the schema and [databaseId]); a backend over one open
  /// database handle reports that handle.
  @internal
  Object drainExclusionKey(String databaseId);

  /// Acquire the drain lock of the database whose identity is [databaseId],
  /// or throw [DrainLockUnavailableException] when another holder has it
  /// (another process, tab or delivery cycle, or a live lock granted through
  /// this backend).
  ///
  /// An acquisition raises the database's drain epoch
  /// ([readDrainEpochTxn]) in a transaction and returns a lock that records
  /// the value it stored. When any step after the backend obtained its
  /// exclusion primitive fails, the backend gives the primitive up before
  /// the error surfaces, so the next attempt can obtain it. Throws
  /// [DrainLockConfigurationException] when the lock cannot be verified with
  /// the backend's configuration. A backend that verifies the database
  /// identity checks [databaseId] against the stored one.
  @internal
  Future<DrainLock> tryAcquireDrainLock({required String databaseId});

  /// Request the drain lock of the database whose identity is
  /// [databaseId]: the request's `granted` completes with the lock once an
  /// acquisition succeeds, retrying every [retryInterval] (and, where the
  /// backend can tell, as soon as the lock is released).
  @internal
  DrainLockRequest requestDrainLock({
    required String databaseId,
    required Duration retryInterval,
  });

  /// Read the database's drain epoch inside [txn], or null before the first
  /// acquisition.
  ///
  /// Persisted under `backend_state` key `drain_epoch`.
  @internal
  Future<int?> readDrainEpochTxn(Transaction txn);

  /// Read the current drainer's declaration inside [txn], or null when no
  /// drainer has written one.
  ///
  /// Persisted under `backend_state` key `drainer_declaration`.
  @internal
  Future<DrainerDeclaration?> readDrainerDeclarationTxn(Transaction txn);

  /// Write [declaration] as the drainer's declaration inside [txn],
  /// replacing the previous one.
  @internal
  Future<void> writeDrainerDeclarationTxn(
    Transaction txn,
    DrainerDeclaration declaration,
  );

  /// Read the drainer's heartbeat inside [txn], or null when no pass has
  /// started.
  ///
  /// Persisted under `backend_state` key `drain_heartbeat`.
  @internal
  Future<DrainHeartbeat?> readDrainHeartbeatTxn(Transaction txn);

  /// Write [heartbeat] as the drainer's heartbeat inside [txn], replacing
  /// the previous one.
  @internal
  Future<void> writeDrainHeartbeatTxn(
    Transaction txn,
    DrainHeartbeat heartbeat,
  );

  /// Read [destinationId]'s refill guard inside [txn], or null when none is
  /// set.
  ///
  /// Persisted under `backend_state` key `refill_guard_<destinationId>`.
  @internal
  Future<RefillGuard?> readRefillGuardTxn(
    Transaction txn,
    String destinationId,
  );

  /// Write [guard] as [destinationId]'s refill guard inside [txn],
  /// replacing the previous one.
  @internal
  Future<void> writeRefillGuardTxn(
    Transaction txn,
    String destinationId,
    RefillGuard guard,
  );

  /// Delete [destinationId]'s refill guard inside [txn]. No-op when none
  /// exists.
  @internal
  Future<void> clearRefillGuardTxn(Transaction txn, String destinationId);

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
  /// - `null -> sent` — the drainer delivered the pending head of a
  ///   destination that is no delivery channel ([markSentTxn] marks a
  ///   delivery sent).
  /// - `null -> wedged` — the drainer wedged the pending head.
  /// - `wedged -> tombstoned` — an operator recovery or a deletion retired
  ///   a wedged head.
  /// - `null -> tombstoned`, only for an item that carries attempts — a
  ///   resume or a new generation of the delivery channel retired it.
  ///
  /// Implementations SHALL throw [StateError] and change nothing on every
  /// other pair, on a repeated status, and when the target row is absent.
  ///
  /// On `null -> sent` the implementation SHALL stamp
  /// `sent_at = DateTime.now().toUtc()`. On every other transition
  /// `attempts[]` and `sent_at` SHALL be left untouched, so a tombstoned
  /// row keeps the attempts of the wedge or the sends it retired.
  @internal
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus status,
  );

  /// Mark the pending item [entryId] of [destinationId] sent inside [txn],
  /// recording the delivery it was acknowledged under: the channel's
  /// [generation], the [deliveryNumber] and the [deliveryHash]. Stamps
  /// `sent_at = DateTime.now().toUtc()` and leaves `attempts[]` untouched.
  ///
  /// This is the only change that writes the three delivery fields.
  /// Implementations SHALL throw [StateError] and change nothing when the
  /// item is absent or not pending.
  // Implements: EVS-DEV-delivery-channel/J
  // the change that marks a queue item sent records the generation,
  //   delivery number and delivery hash it was acknowledged under.
  @internal
  Future<void> markSentTxn(
    Transaction txn,
    String destinationId,
    String entryId, {
    required int generation,
    required int deliveryNumber,
    required String deliveryHash,
  });

  /// Delete the pending item [entryId] of [destinationId] inside [txn]: a
  /// resume or a new generation of the delivery channel retires a pending
  /// item that carries no attempt this way.
  ///
  /// Implementations SHALL throw [StateError] and change nothing when the
  /// item is absent, terminal, or carries attempts: an item that was sent
  /// is retired by tombstoning it, and a terminal item is the delivery
  /// record.
  @internal
  Future<void> deleteFifoEntryTxn(
    Transaction txn,
    String destinationId,
    String entryId,
  );

  /// Read inside [txn] the retained delivery of [destinationId] at
  /// [deliveryNumber] under [generation]: among the items marked sent with
  /// that generation and number, the one marked last (the highest
  /// `sequence_in_queue`), or null when there is none.
  ///
  /// An item sent at that number under another generation, and an item
  /// that is not sent, is never the retained delivery.
  // Implements: EVS-DEV-delivery-resume/W
  // the retained delivery at a number is the delivery the queue last
  //   marked sent at that number under the given generation; none where
  //   there is none.
  @internal
  Future<FifoEntry?> readRetainedDeliveryTxn(
    Transaction txn,
    String destinationId, {
    required int generation,
    required int deliveryNumber,
  });

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
  /// A public read for callers that look for the latest events of a kind
  /// without paging through the whole log. Consumers that only need the
  /// single most-recent match SHOULD `await for` and `break` (or return)
  /// on the first event.
  ///
  /// When [eventTypes] is supplied only events whose `event_type` is
  /// contained in the set are emitted; when null no type filtering is
  /// applied.
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes});

  /// [readEventsReverse] inside [txn]: the stream sees the events the
  /// transaction has appended so far, and is read to its end, or abandoned,
  /// before the transaction body returns. Each event is emitted once, in
  /// strictly descending `sequence_number` order, across however many
  /// pages the backend reads; the body does not append while the stream is
  /// open.
  @internal
  Stream<StoredEvent> readEventsReverseInTxn(
    Transaction txn, {
    Set<String>? eventTypes,
  });

  // -------- Chain lookups --------
  //
  // Reads over the log, inside a transaction, so each sees the events
  // stored earlier in it and none a rolled-back transaction stored. The
  // library keeps no index of its own for them: a backend serves them from
  // its storage's indexes or by scanning the log. No lookup refuses a
  // second match: the log holds forks and reused origin positions as
  // received. The coordinates they match on are those a stored copy yields
  // (the originating database of its first provenance entry; its sealed
  // hash and origin position, from the copy itself when its provenance
  // holds one entry and from the second entry otherwise).

  /// The event with the highest local sequence number among the events
  /// this database holds as authored (copies whose provenance holds exactly
  /// one entry, naming this database), or null when it holds none or
  /// [databaseId] is not this database's identity. Read inside [txn].
  // Implements: EVS-DEV-chain-verification/B
  // the latest event the appending database holds as authored, read inside
  //   the append's transaction.
  @internal
  Future<StoredEvent?> readLatestHeldAsAuthoredInTxn(
    Transaction txn,
    String databaseId,
  );

  /// The event of the aggregate [aggregateId] with the highest local
  /// sequence number among the events this database holds as authored
  /// (copies whose provenance holds exactly one entry, naming
  /// [databaseId], this database's identity), or null when it holds none.
  /// Read inside [txn], so it sees the events stored earlier in it.
  // Implements: EVS-DEV-delivery-receiver/I
  // the receiver's record of a channel is read from the latest
  //   accepted-delivery audit of the channel's audit aggregate that the
  //   receiver authored; audits another database authored never match.
  @internal
  Future<StoredEvent?> readLatestAuthoredOfAggregateInTxn(
    Transaction txn, {
    required String databaseId,
    required String aggregateId,
  });

  /// The `ingest.delivery_accepted` audits of the aggregate [aggregateId]
  /// that this database holds as authored (copies whose provenance holds
  /// exactly one entry, naming [databaseId]) and whose data's
  /// `delivery_number` is a number from [fromDeliveryNumber] to
  /// [toDeliveryNumber] inclusive, in ascending local sequence number.
  /// Read inside [txn].
  // Implements: EVS-DEV-delivery-receiver/O
  // the pull reads each delivery of a range from the accepted-delivery
  //   audit of the channel that the receiver authored for it.
  @internal
  Future<List<StoredEvent>> findAuthoredDeliveryAuditsInTxn(
    Transaction txn, {
    required String databaseId,
    required String aggregateId,
    required int fromDeliveryNumber,
    required int toDeliveryNumber,
  });

  /// For each aggregate holding an `ingest.delivery_accepted` audit that
  /// this database holds as authored and whose data's `channel` names a
  /// sending database in [senderDatabaseIds], the one such audit with the
  /// highest local sequence number. Read inside [txn]; in no particular
  /// order.
  // Implements: EVS-DEV-delivery-receiver/R
  // the channel listing reads, from the accepted-delivery audits the
  //   receiver authored, every channel of every generation it accepted a
  //   delivery on from the named senders.
  @internal
  Future<List<StoredEvent>> findLatestAuthoredDeliveryAuditsInTxn(
    Transaction txn, {
    required String databaseId,
    required Set<String> senderDatabaseIds,
  });

  /// The held events sealed under [sealedHash], in ascending local
  /// sequence number. Read inside [txn].
  // Implements: EVS-DEV-chain-verification/A
  // held events are found by sealed hash, never by a holder's re-stamped
  //   event_hash.
  @internal
  Future<List<StoredEvent>> findEventsBySealedHashInTxn(
    Transaction txn,
    String sealedHash,
  );

  /// The held events of [originatingDatabaseId] whose `previous_event_hash`
  /// is [previousEventHash] (null matching the events that name no
  /// predecessor), in ascending local sequence number. Read inside [txn].
  @internal
  Future<List<StoredEvent>> findEventsByPredecessorInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
  });

  /// The held events of [originatingDatabaseId] at origin position
  /// [originPosition], in ascending local sequence number. Read inside
  /// [txn].
  // Implements: EVS-DEV-chain-verification/A
  // held events are found by the origin position their copy records, never
  //   by the holder's local sequence number.
  @internal
  Future<List<StoredEvent>> findEventsByOriginPositionInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required int originPosition,
  });

  /// The held events the chain-structure checks of one stored event read,
  /// inside [txn], each list in ascending local sequence number:
  /// `predecessors` as [findEventsBySealedHashInTxn] returns for
  /// [previousEventHash] (empty when it is null), `atPosition` as
  /// [findEventsByOriginPositionInTxn] returns for [originatingDatabaseId]
  /// and [originPosition], and `successors` as
  /// [findEventsByPredecessorInTxn] returns for [originatingDatabaseId] and
  /// [previousEventHash]. An event may sit in more than one list. The
  /// default composes those three lookups; a backend may read the three in
  /// one statement.
  @internal
  Future<
    ({
      List<StoredEvent> predecessors,
      List<StoredEvent> atPosition,
      List<StoredEvent> successors,
    })
  >
  findChainNeighboursInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
    required int originPosition,
  }) async {
    final previous = previousEventHash;
    return (
      predecessors: previous == null
          ? const <StoredEvent>[]
          : await findEventsBySealedHashInTxn(txn, previous),
      atPosition: await findEventsByOriginPositionInTxn(
        txn,
        originatingDatabaseId: originatingDatabaseId,
        originPosition: originPosition,
      ),
      successors: await findEventsByPredecessorInTxn(
        txn,
        originatingDatabaseId: originatingDatabaseId,
        previousEventHash: previous,
      ),
    );
  }

  /// The held events of [originatingDatabaseId] at origin position
  /// [fromPosition] or above, in ascending local sequence number. Read
  /// inside [txn].
  // Implements: EVS-PRD-materializer/E
  // the events of a database at or above a reused or forked origin
  //   position, whose aggregates the default views mark.
  @internal
  Future<List<StoredEvent>> findEventsFromOriginPositionInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required int fromPosition,
  });

  /// The held security findings (events of the security-finding entry type
  /// and its one event type), authored and received, in ascending local
  /// sequence number. Read inside [txn], so it sees the findings stored
  /// earlier in it.
  // Implements: EVS-PRD-materializer/G
  // the findings every view folds into its outstanding-finding marks,
  //   whatever the view's interest.
  @internal
  Future<List<StoredEvent>> findSecurityFindingsInTxn(Transaction txn);

  /// Whether this database holds any security finding, authored or
  /// received: whether [findSecurityFindingsInTxn] would return an event.
  /// Read inside [txn], so it sees the findings stored earlier in it. The
  /// marks read it, through [readMarksHolderInTxn], on every transaction,
  /// so it reads no finding event.
  // Implements: EVS-PRD-materializer/G
  // every view's marks start from whether any finding is held, read inside
  //   the folding transaction.
  @internal
  Future<bool> holdsSecurityFindingInTxn(Transaction txn);

  /// The database identity ([readDatabaseIdTxn]) and whether this database
  /// holds any security finding ([holdsSecurityFindingInTxn]), both read
  /// inside [txn]: what the marks read once per transaction before they
  /// read any finding. The default composes those two reads; a backend may
  /// read both in one statement.
  @internal
  Future<({String? databaseId, bool holdsFinding})> readMarksHolderInTxn(
    Transaction txn,
  ) async => (
    databaseId: await readDatabaseIdTxn(txn),
    holdsFinding: await holdsSecurityFindingInTxn(txn),
  );

  /// Whether this database holds as authored a security finding whose
  /// `finding_id` is [findingId]: an event of the security-finding entry
  /// type carrying that identity whose provenance holds exactly one entry,
  /// naming [databaseId], this database's identity. A finding another
  /// database originated never matches, whatever detector it names. Read
  /// inside [txn], so it sees the findings stored earlier in it.
  // Implements: EVS-DEV-security-findings/E
  // the once-per-detector lookup reads, inside the appending transaction,
  //   only the findings the detecting database holds as authored.
  @internal
  Future<bool> holdsAuthoredSecurityFindingInTxn(
    Transaction txn, {
    required String databaseId,
    required String findingId,
  });

  /// The latest eligible version of the aggregate [aggregateId] this
  /// database holds: the held event of that aggregate with the highest
  /// local sequence number whose recorded `causal` says an eligible
  /// version and whose copy yields a sealed hash, or null when it holds
  /// none. Read inside [txn], so it sees the
  /// events stored earlier in it.
  // Implements: EVS-DEV-causal-parents/H
  // the latest eligible version of an aggregate is the held event of that
  //   aggregate with the highest local sequence number whose recorded causal
  //   says an eligible version, read from the log.
  @internal
  Future<StoredEvent?> readLatestEligibleVersionInTxn(
    Transaction txn,
    String aggregateId,
  );

  /// Who authored the held events of [aggregateId]: for each originating
  /// database with at least one held event of it, the highest origin
  /// position among those events, or null when none of them carries one.
  /// Read inside [txn], so it sees the events stored earlier in it.
  // Implements: EVS-PRD-materializer/E
  // the marks fold reads who authored an aggregate's held events from this
  //   lookup rather than the aggregate's events themselves, so an append
  //   evaluating a held finding does not read every held event of the
  //   aggregates it marks.
  @internal
  Future<Map<String, int?>> readAggregateAuthorshipInTxn(
    Transaction txn,
    String aggregateId,
  );

  /// The held `system.destination_sender_succeeded` events, authored and
  /// received, that the succession-lineage lookup a received chain finding's
  /// marks resolve from: served from a backend index keyed by this entry
  /// type, never a scan proportional to the whole event store. Read inside
  /// [txn], so it sees the events stored earlier in it. The default body
  /// serves a backend with no index of its own, at the cost of the scan the
  /// index exists to avoid.
  // Implements: EVS-PRD-materializer/E
  // the succession-lineage read a received chain finding's marks resolve
  //   from is served by a backend index over this entry type, not a scan
  //   proportional to the whole event store.
  @internal
  Future<List<StoredEvent>> findSenderSuccessionEventsInTxn(Transaction txn) =>
      findAllEventsInTxn(txn, entryType: kDestinationSenderSucceededEntryType);

  /// The lowest origin position among the held events of
  /// [originatingDatabaseId] whose `previous_event_hash` is
  /// [previousEventHash] (null included), or null when none carries one.
  /// Read inside [txn], so it sees the events stored earlier in it.
  // Implements: EVS-PRD-materializer/E
  // a fork finding's threshold is this lookup's answer rather than a scan
  //   of the database's events sharing its predecessor, so an append
  //   evaluating a held fork finding does not read every such event.
  @internal
  Future<int?> readLowestOriginPositionByPredecessorInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
  });

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

/// Thrown from [StorageBackend.runInSavepointInTxn]'s body, in place of the
/// original error, by a backend that classifies it as a rejection of a row
/// write's value rather than a storage failure (`EVS-DEV-view-convergence`
/// Terms: on Postgres, a `ServerException` whose SQLSTATE is class 22, 23
/// or 54). [cause] and [causeStackTrace] preserve the original error and
/// its stack trace for a catcher that logs or reports it; the fold_failed
/// finding the interpreter records carries only the failure reason
/// (`EVS-DEV-security-findings/R`), not the cause. No backend throws this
/// outside a savepoint's body: a `runInSavepointInTxn` caller is the only
/// intended catcher.
@internal
class RowWriteRejected implements Exception {
  @internal
  const RowWriteRejected(this.cause, this.causeStackTrace);

  /// The original error the backend classified.
  final Object cause;

  /// [cause]'s stack trace.
  final StackTrace causeStackTrace;

  @override
  String toString() => 'RowWriteRejected: $cause';
}
