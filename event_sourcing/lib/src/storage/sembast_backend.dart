import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/src/causal_record.dart';
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/lifecycle/boot_progress.dart';
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kIngestAuditEntryType,
        kIngestDeliveryAcceptedEventType,
        kSecurityFindingEntryType,
        kSecurityFindingRecordedEventType;
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/boot_check.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:event_sourcing/src/storage/drain_lock.dart';
import 'package:event_sourcing/src/storage/drain_records.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/generation.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/isolate_drain_lock.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/transaction_rerun_limit.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:event_sourcing/src/storage/view_copy_lock.dart';
import 'package:event_sourcing/src/storage/web_locks_stub.dart'
    if (dart.library.js_interop) 'package:event_sourcing/src/storage/web_locks.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:sembast/sembast.dart' as sembast show Transaction;
import 'package:sembast/sembast.dart' hide Transaction;
import 'package:uuid/uuid.dart';

part '../security/sembast_security_context_store.dart';

part 'sembast_test_support.dart';

/// Module-private v4 UUID generator used by [SembastBackend.enqueueFifoTxn]
/// to mint each FIFO row's [FifoEntry.entryId]. Held at file scope (const)
/// so every backend instance shares one generator; `Uuid.v4()` is
/// side-effect-free beyond its internal random state, so a shared
/// instance is correct.
const _uuidGen = Uuid();

/// Concrete Sembast-backed implementation of [StorageBackend].
///
/// The reference implementation for Dart VM / Flutter mobile / Flutter desktop
/// / Flutter web. Satisfies the portability contract by keeping all
/// Sembast-specific code behind this class; callers see only `StorageBackend`.
///
/// Opens a single Sembast database at `path` via `databaseFactory`. The
/// database hosts four logical stores:
// Implements: EVS-PRD-portability/D
// reference platform-divergent storage
//   implementation; app supplies databaseFactory per platform.
// Implements: EVS-PRD-event-log/A
// append-only event log in 'events' store.
// Implements: EVS-PRD-event-log/B
// stable total order via sequence counter in
//   backend_state; monotonic; never reset.
///
/// - `events` — append-only event log, keyed by Sembast auto-increment int.
/// - `notes` — materialized view, keyed by `note_id` (string).
/// - `fifo_<destinationId>` — one per registered destination.
/// - `backend_state` — key-value bookkeeping for the sequence counter and
///   the persisted schema version. Deliberately NOT named `metadata` — that
///   name is already used on every event record as the provenance / change-
///   reason carrier, so reusing it at the store level would make code
///   references ambiguous.
///
/// The database is opened lazily on first use so construction is cheap and
/// test setup can instantiate many backends without paying database-open
/// cost up front.
class SembastBackend extends StorageBackend {
  /// Construct a backend over an already-opened Sembast [Database]. The
  /// caller owns the database's lifecycle; this backend does not open or
  /// close it. Tests can get a self-contained in-memory instance by opening
  /// a database via `package:sembast/sembast_memory.dart`'s
  /// `newDatabaseFactoryMemory()` and passing it to this constructor, as
  /// the conformance test suite does.
  ///
  /// [bootLockWait] applies on the web, where every tab of the origin that
  /// opens the database takes the same exclusive boot lock: it bounds how
  /// long `EventStore.open` waits for another tab's boot, after which the
  /// open throws [GenerationGuardConfigurationException]. It must exceed
  /// the longest boot the deployment expects, since a boot that promotes a
  /// large view or re-derives a view over a long log holds the boot lock
  /// for its whole duration. Outside the browser it has no effect.
  SembastBackend({
    required Database database,
    Duration bootLockWait = const Duration(seconds: 60),
  }) : _db = database,
       _bootLockWait = bootLockWait;

  final Database _db;

  /// See the constructor's `bootLockWait`.
  final Duration _bootLockWait;

  static const _sequenceKey = 'sequence_counter';
  static const _schemaVersionKey = 'schema_version';
  static const _knownFifosKey = 'known_fifo_destinations';

  // Per-destination monotonic `sequence_in_queue` counter key, stored in
  // `backend_state` as `fifo_seq_counter_<destinationId>`. Used by
  // `enqueueFifoTxn` to assign a never-reused sequence_in_queue value:
  // the counter advances on every enqueue and is never reset, so a row
  // deleted by the trail sweep cannot have its slot re-used by a later enqueue.
  static String _fifoSeqCounterKey(String destinationId) =>
      'fifo_seq_counter_$destinationId';

  static const _eventStoreName = 'events';

  final StoreRef<int, Map<String, Object?>> _eventStore = intMapStoreFactory
      .store(_eventStoreName);
  final StoreRef<String, Object?> _backendStateStore =
      StoreRef<String, Object?>('backend_state');
  // Backend-private mirror of the `security_context` sembast store so
  // [queryAudit] can join against the event log without reaching into a
  // separate store object. The sembast `StoreRef` is just a typed name
  // handle — multiple refs to the same store name read/write the same
  // underlying records, so this cohabits cleanly with
  // `SembastSecurityContextStore`'s own ref.
  final StoreRef<String, Map<String, Object?>> _securityContextStore =
      stringMapStoreFactory.store('security_context');

  /// Backend-owned index, keyed by aggregate id, of the local sequence
  /// number of the aggregate's latest eligible version. Written in the same
  /// transaction that stores an eligible version (append, ingest, restore,
  /// finding — every path goes through [appendEvent]), so
  /// [readLatestEligibleVersionInTxn] reads this record and fetches the
  /// event at its key, rather than scanning the aggregate's events. Nothing
  /// compares this index with anything else; it is not part of the chain
  /// walk, only the backend's own read-path acceleration.
  final StoreRef<String, Object?> _latestEligibleVersionStore =
      StoreRef<String, Object?>('latest_eligible_version_by_aggregate');

  /// Backend-owned index, keyed by aggregate id, of who authored the
  /// aggregate's held events: a JSON map from originating database id to
  /// the highest origin position among its held events of that aggregate
  /// (or `null` when none of them carries one). Written in the same
  /// transaction that stores an event naming an originating database
  /// (append, ingest, restore), so the marks fold reads authorship by key
  /// rather than scanning the aggregate's events.
  final StoreRef<String, Object?> _aggregateAuthorshipStore =
      StoreRef<String, Object?>('aggregate_authorship');

  /// Backend-owned index, keyed by the JSON encoding of
  /// `[originatingDatabaseId, previousEventHash]`, of the lowest origin
  /// position among the held events of that database sharing that
  /// predecessor hash. Written in the same transaction that stores such an
  /// event, so a fork finding's threshold is read by key rather than by
  /// scanning the database's events for its predecessor.
  final StoreRef<String, Object?> _predecessorLowestPositionStore =
      StoreRef<String, Object?>('predecessor_lowest_position');

  /// Backend-owned index, keyed by aggregate id, of the local sequence
  /// numbers of the aggregate's held events, in the order stored. Written
  /// in the same transaction that stores each event (append, ingest,
  /// restore -- every path goes through [appendEvent]), so
  /// [findEventsForAggregateInTxn] fetches the aggregate's events by key in
  /// ascending sequence order rather than scanning the whole event store.
  /// Growing this per-aggregate list costs an append proportionally to the
  /// aggregate's own event count, not to the store's size; Sembast has no
  /// secondary-index primitive that would avoid it.
  final StoreRef<String, Object?> _aggregateEventSequencesStore =
      StoreRef<String, Object?>('aggregate_event_sequences');

  /// Backend-owned index, keyed by the JSON encoding of `[copyId, rowKey]`,
  /// of the source aggregate id `upsertTableViewRowInTxn` last indexed a
  /// `TableProjectionSpec` view copy's row key under: the reverse half of
  /// [_tableRowsBySourceStore], read to find which forward entry to retire
  /// when a row's key is deleted or reassigned.
  final StoreRef<String, Object?> _tableRowSourceStore =
      StoreRef<String, Object?>('table_row_source_aggregate');

  /// Backend-owned index, keyed by the JSON encoding of
  /// `[copyId, sourceAggregateId]`, of the row keys a `TableProjectionSpec`
  /// view copy's rows [upsertTableViewRowInTxn] indexed under that source
  /// aggregate. [findTableRowsBySourceAggregateInTxn] reads it by key
  /// instead of scanning the whole copy.
  final StoreRef<String, Object?> _tableRowsBySourceStore =
      StoreRef<String, Object?>('table_rows_by_source_aggregate');

  static String _rowSourceIndexKey(String copyId, String rowKey) =>
      jsonEncode(<Object?>[copyId, rowKey]);

  static String _bySourceIndexKey(String copyId, String sourceAggregateId) =>
      jsonEncode(<Object?>[copyId, sourceAggregateId]);

  /// The `copyId` a `_rowSourceIndexKey`/`_bySourceIndexKey` compound [key]
  /// was built for, or null when it does not decode to one.
  static String? _copyIdOfIndexKey(Object? key) {
    if (key is! String) return null;
    final decoded = jsonDecode(key);
    return decoded is List && decoded.isNotEmpty && decoded[0] is String
        ? decoded[0] as String
        : null;
  }

  StoreRef<int, Map<String, Object?>> _fifoStore(String destinationId) =>
      intMapStoreFactory.store('fifo_$destinationId');

  Database _database() => _db;

  // Broadcast controllers for reactive APIs.
  // _eventsController emits after each successful appendEvent commit;
  // origin and ingest paths both route through appendEvent
  // under the unified event store, so a single emission point covers both.
  // _fifoChangesController emits after each successful FIFO mutation;
  // payload is the destinationId. _viewChangesController emits
  // after each successful view-row mutation; payload is the viewName.
  final StreamController<StoredEvent> _eventsController =
      StreamController<StoredEvent>.broadcast();
  final StreamController<String> _fifoChangesController =
      StreamController<String>.broadcast();
  final StreamController<String> _viewChangesController =
      StreamController<String>.broadcast();

  /// Close the underlying sembast database AND the reactive broadcast
  /// controllers used by [watchEvents] / [watchFifo] / [watchView], after
  /// releasing a drain lock granted through this backend. After close,
  /// further calls to those reactive methods SHALL throw `StateError`.
  /// Active subscribers receive `done`.
  ///
  /// Not safe to call concurrently with an in-flight [transaction]. The
  /// caller is responsible for awaiting outstanding work before closing.
  @override
  Future<void> close() async {
    _closed = true;
    if (!_gone.isCompleted) {
      _gone.complete(const DrainLockBackendClosedException());
    }
    await _drainLock?.release();
    _drainLock = null;
    await _eventsController.close();
    await _fifoChangesController.close();
    await _viewChangesController.close();
    await _db.close();
  }

  // -------- transaction --------

  /// Runs the boot body as one transaction. The transactions of one
  /// Sembast database run one at a time in a process, so an append cannot
  /// abort the boot. On the web several tabs share the database, and a tab
  /// whose commit another tab preceded re-runs its body on fresh data; the
  /// boot therefore holds the database's write lock exclusively, which
  /// every tab's transactions take shared, so the other tabs' writes wait
  /// for the boot to commit instead of making it re-run without end. The
  /// wait for that lock is bounded by the constructor's `bootLockWait`.
  @override
  @internal
  Future<T> bootTransaction<T>(Future<T> Function(Transaction txn) body) {
    refuseCallFromBootProgressObserver('SembastBackend.bootTransaction');
    return runHoldingBrowserWriteLock(
      _database().path,
      exclusive: true,
      timeout: _bootLockWait,
      body: () async {
        _bootHoldsWriteLock = true;
        try {
          return await transaction(body);
        } finally {
          _bootHoldsWriteLock = false;
        }
      },
    );
  }

  /// True while a boot holds this backend's write lock exclusively. The
  /// boot's own transaction then runs without asking for the lock again,
  /// and so does any other transaction on this backend, which the
  /// database runs one at a time with the boot's.
  bool _bootHoldsWriteLock = false;

  /// Tries the view copy [copyKey]'s lock without waiting: the isolate-
  /// local registry keyed by this backend's open database handle, and, in
  /// the browser, also the database's Web Lock (tried second, so a lock
  /// this isolate lost to another catch-up in the same isolate never
  /// reaches the browser's lock manager). Not granted, returns null,
  /// taking no further step and writing nothing; granted, runs [body] as
  /// [transaction] does and releases both locks when it ends.
  // Implements: EVS-DEV-view-convergence/M (Sembast isolate-local lock and
  //   web Web Lock)
  @override
  @internal
  Future<T?> catchUpTransaction<T>(
    String copyKey,
    Future<T> Function(Transaction txn) body,
  ) {
    refuseCallFromBootProgressObserver('SembastBackend.catchUpTransaction');
    if (!tryLockViewCopyIsolate(_db, copyKey)) return Future<T?>.value();
    return runHoldingBrowserViewCopyLock<T>(
      path: _database().path,
      copyKey: copyKey,
      body: () => transaction(body),
    ).whenComplete(() => unlockViewCopyIsolate(_db, copyKey));
  }

  // Implements: EVS-PRD-subscription/E
  // Post-commit notifications are queued on the
  //   per-run transaction handle, not on the backend, so two transactions in
  //   flight at once never share a queue. sembast_web re-runs a body when
  //   another tab committed first; each run gets a fresh handle, and only the
  //   run that committed (the last one) has its queue fired. A body that
  //   throws commits nothing and fires nothing.
  // Implements: EVS-DEV-event-store-open/M
  // a transaction the boot progress observer, or work it started, asks for
  //   while the boot runs is refused.
  /// On the web a transaction takes the database's write lock shared, so
  /// the tabs' transactions run side by side and a body whose commit
  /// another tab preceded runs again. After [_sharedRuns] such runs the
  /// transaction runs again holding the write lock exclusively, where no
  /// other tab's write can come between, so contention between tabs delays
  /// a transaction but never fails it. A handle that cannot commit even
  /// then fails with [TransactionRerunLimitException], and so does every
  /// later transaction on it.
  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) async {
    refuseCallFromBootProgressObserver('SembastBackend.transaction');
    if (_bootHoldsWriteLock) return _transaction(body, exclusive: true);
    try {
      return await runHoldingBrowserWriteLock(
        _database().path,
        exclusive: false,
        body: () => _transaction(body, exclusive: false),
      );
    } on _RunsLostToOtherWriters {
      return runHoldingBrowserWriteLock(
        _database().path,
        exclusive: true,
        timeout: _bootLockWait,
        timeoutError: (name, wait) => TimeoutException(
          "a transaction that lost $_sharedRuns runs to other tabs' commits "
          'waited longer than $wait for the write lock $name, which another '
          'tab holds',
          wait,
        ),
        body: () => _transaction(body, exclusive: true),
      );
    }
  }

  /// The runs of a body, holding the write lock shared, after which the
  /// transaction runs again holding it exclusively.
  static const int _sharedRuns = 4;

  /// Set once a transaction found that this handle cannot commit; every
  /// later transaction fails with it at once.
  TransactionRerunLimitException? _handleFault;

  /// Completes with the error that ends every drain-lock request through
  /// this backend: its close, or a handle that cannot commit.
  final Completer<Exception> _gone = Completer<Exception>();

  // Implements: EVS-PRD-event-log/E
  // sembast re-runs a body whose commit found another tab's commit first;
  //   a body that keeps losing runs again with every other tab's writes held
  //   back, and a handle that fails even then is reported as unable to
  //   commit (another opener compacted the database past its revision).
  Future<T> _transaction<T>(
    Future<T> Function(Transaction txn) body, {
    required bool exclusive,
  }) async {
    final fault = _handleFault;
    if (fault != null) throw fault;
    final db = _database();
    late _SembastTxn committedRun;
    final bound = exclusive
        ? TransactionRerunLimitException.maxRuns
        : _sharedRuns;
    var runs = 0;
    final result = await db.transaction((sembastTxn) async {
      if (runs == bound) {
        if (!exclusive) throw const _RunsLostToOtherWriters();
        final fault = _handleFault ??= TransactionRerunLimitException(runs);
        if (!_gone.isCompleted) _gone.complete(fault);
        throw fault;
      }
      runs++;
      final txn = _SembastTxn._(sembastTxn, this);
      committedRun = txn;
      try {
        return await body(txn);
      } finally {
        txn._invalidate();
      }
    });
    // Each callback checks its controller is still open: close() is not
    // safe to race with an in-flight transaction, but a fast-cycle test may
    // still observe the closed state here.
    for (final cb in committedRun._postCommit) {
      cb();
    }
    // One queue notification per destination the committed run changed.
    for (final destinationId in committedRun._fifoChanged) {
      if (!_fifoChangesController.isClosed) {
        _fifoChangesController.add(destinationId);
      }
    }
    return result;
  }

  /// Runs [body] with a handle whose reads run through the database outside
  /// any transaction: sembast's reads take no lock, so they neither wait
  /// for a transaction nor hold one back, and they see only committed
  /// records. The handle is refused after [body] returns, and by every
  /// write.
  // Implements: EVS-DEV-chain-verification/S
  // on Sembast the chain verification reads through the database outside
  //   any transaction, so it takes no lock an append waits for.
  @override
  @internal
  Future<T> nonBlockingRead<T>(
    Future<T> Function(Transaction reads) body,
  ) async {
    refuseCallFromBootProgressObserver('SembastBackend.nonBlockingRead');
    final handle = _SembastTxn._nonBlocking(_database(), this);
    try {
      return await body(handle);
    } finally {
      handle._invalidate();
    }
  }

  // Implements: EVS-DEV-postgres-backend/L
  // a handle another backend instance produced is refused.
  _SembastTxn _requireValidTxn(Transaction txn) {
    if (txn is! _SembastTxn) {
      throw StateError('Transaction is not a SembastBackend Transaction');
    }
    if (!identical(txn._owner, this)) {
      throw StateError(
        'Transaction was produced by a different SembastBackend instance',
      );
    }
    if (!txn._isValid) {
      throw StateError('Transaction used outside its transaction() body');
    }
    return txn;
  }

  /// Return the underlying sembast [sembast.Transaction] for [txn]. Used by
  /// adjacent sembast-family stores (e.g. `SembastSecurityContextStore`)
  /// that need to commit writes atomically with this backend's
  /// transaction. NOT part of the abstract `StorageBackend` contract —
  /// only sembast-side code should reach for this.
  sembast.Transaction _unwrapSembastTxn(Transaction txn) =>
      _requireValidTxn(txn)._sembastTxn;

  // -------- Events --------

  /// Persist [event] inside [txn] and return its [AppendResult]. Under the
  /// reserve-and-increment contract, `event.sequenceNumber`
  /// MUST equal the value returned by a prior [nextSequenceNumber] call in
  /// the same transaction — i.e., it MUST equal the current persisted
  /// counter value. [appendEvent] does not advance the counter; the advance
  /// is owned by [nextSequenceNumber].
  ///
  /// A mismatch means the caller either skipped [nextSequenceNumber] or
  /// consumed the reservation with a wrong `sequenceNumber`; both are
  /// caller bugs, so `appendEvent` throws `StateError` rather than
  /// silently accepting an out-of-range value.
  // Implements: EVS-PRD-event-log/A
  // persists event to append-only log.
  // Implements: EVS-PRD-event-log/B
  // sequence number stamped by caller from
  //   nextSequenceNumber; persisted verbatim preserving total order.
  @override
  @internal
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event) async {
    final t = _requireValidTxn(txn);
    final currentRaw = await _backendStateStore
        .record(_sequenceKey)
        .get(t._sembastTxn);
    final current = (currentRaw as int?) ?? 0;
    if (event.sequenceNumber != current) {
      throw StateError(
        'appendEvent: event.sequenceNumber (${event.sequenceNumber}) '
        'must equal the reserved counter value ($current). '
        'Did the caller forget to call nextSequenceNumber in this '
        'transaction? appendEvent consumes a reservation; it does not '
        'create one.',
      );
    }
    event.requireWellFormedRecord();
    // Each event is stored under its local sequence number, so the chain
    // lookups read an event of a known sequence by its key.
    final stored = await _eventStore
        .record(event.sequenceNumber)
        .add(t._sembastTxn, event.toMap());
    if (stored == null) {
      throw StateError(
        'appendEvent: an event is already stored under sequence '
        '${event.sequenceNumber}',
      );
    }
    await _recordLatestAuthoredInTxn(t._sembastTxn, event);
    await _recordLatestEligibleVersionInTxn(t._sembastTxn, event);
    await _recordFindingHeldInTxn(t._sembastTxn, event);
    await _recordAggregateAuthorshipInTxn(t._sembastTxn, event);
    await _recordPredecessorLowestPositionInTxn(t._sembastTxn, event);
    await _recordAggregateEventSequenceInTxn(t._sembastTxn, event);
    // post-commit so live subscribers learn of the new event in
    // sequence_number order.
    t._postCommit.add(() {
      if (!_eventsController.isClosed) _eventsController.add(event);
    });
    return AppendResult(
      sequenceNumber: event.sequenceNumber,
      eventHash: event.eventHash,
    );
  }

  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) async {
    final db = _database();
    final finder = Finder(
      filter: Filter.equals('aggregate_id', aggregateId),
      sortOrders: [SortOrder('sequence_number')],
    );
    final records = await _eventStore.find(db, finder: finder);
    return records.map((r) => StoredEvent.fromMap(r.value, r.key)).toList();
  }

  /// Appends [event]'s sequence number to its aggregate's record in
  /// [_aggregateEventSequencesStore]. Called from [appendEvent] for every
  /// stored event, unconditionally: every event belongs to exactly one
  /// aggregate, and [findEventsForAggregateInTxn] fetches by this index for
  /// any aggregate id, held or not.
  Future<void> _recordAggregateEventSequenceInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    final record = _aggregateEventSequencesStore.record(event.aggregateId);
    final existing = await record.get(txn);
    final sequences = (existing is List ? List<int>.from(existing) : <int>[])
      ..add(event.sequenceNumber);
    await record.put(txn, sequences);
  }

  // Implements: EVS-PRD-event-log/C
  // an aggregate's held events, in per-aggregate order, are read from the
  //   backend's own index: the per-aggregate record of sequence numbers
  //   written alongside each event by _recordAggregateEventSequenceInTxn,
  //   fetched by key in ascending sequence order — no scan, no Finder over
  //   the event store.
  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _aggregateEventSequencesStore
        .record(aggregateId)
        .get(t._reads);
    if (value == null) return <StoredEvent>[];
    final sequences = List<int>.from(value as List);
    final events = <StoredEvent>[];
    for (final sequence in sequences) {
      final raw = await _eventStore.record(sequence).get(t._reads);
      if (raw == null || raw['sequence_number'] != sequence) {
        throw StateError(
          'findEventsForAggregateInTxn: aggregate $aggregateId records '
          'sequence $sequence in its event index, and no event of that '
          'sequence is stored under it',
        );
      }
      events.add(StoredEvent.fromMap(Map<String, Object?>.from(raw), sequence));
    }
    return events;
  }

  // provenance[0].hopId / provenance[0].identifier. The originator filter
  // runs on the already-loaded list rather than as a sembast query because
  // the provenance entries live inside the JSON-encoded `metadata` blob and
  // sembast finders do not project across nested array elements; for a
  // mobile-scale event log, in-memory filtering after the
  // `afterSequence` / `limit` / `entry_type` / `client_timestamp` slice is
  // well-bounded and matches the straightforward semantic.
  // Implements: EVS-PRD-event-log/D
  // read all events in order from any
  //   starting position, optionally sliced.
  // Implements: EVS-DEV-find-all-events-extended-filters/A
  // entryType +
  //   clientTimestampStart + clientTimestampEnd optional named parameters.
  // Implements: EVS-DEV-find-all-events-extended-filters/C
  // AND-composition;
  //   entry-type and client-timestamp filters land as sembast Filter predicates
  //   on the top-level `entry_type` / `client_timestamp` fields; the
  //   client-timestamp bounds compare parsed instants, start inclusive and
  //   end exclusive.
  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final db = _database();
    final finder = Finder(
      filter: _composeFindAllEventsFilter(
        afterSequence: afterSequence,
        entryType: entryType,
        clientTimestampStart: clientTimestampStart,
        clientTimestampEnd: clientTimestampEnd,
      ),
      sortOrders: [SortOrder('sequence_number')],
      limit: limit,
    );
    final records = await _eventStore.find(db, finder: finder);
    final all = records
        .map((r) => StoredEvent.fromMap(r.value, r.key))
        .toList();
    if (originatorHopId == null && originatorIdentifier == null) {
      return all;
    }
    return all.where((e) {
      final hop = e.originatorHop;
      if (originatorHopId != null && hop.hop != originatorHopId) {
        return false;
      }
      if (originatorIdentifier != null &&
          hop.identifier != originatorIdentifier) {
        return false;
      }
      return true;
    }).toList();
  }

  /// Build the sembast `Filter` for `findAllEvents` /
  /// `findAllEventsInTxn` from the supplied optional predicates. Returns
  /// `null` when no predicates are supplied (Finder treats `null` filter
  /// as "match all"). When exactly one predicate is supplied it is
  /// returned directly; multiple predicates compose via `Filter.and`.
  // Implements: EVS-DEV-find-all-events-extended-filters/D
  // single shared
  //   helper (_composeFindAllEventsFilter) used by both in-transaction and
  //   out-of-transaction code paths.
  Filter? _composeFindAllEventsFilter({
    required int? afterSequence,
    required String? entryType,
    required DateTime? clientTimestampStart,
    required DateTime? clientTimestampEnd,
  }) {
    final filters = <Filter>[];
    if (afterSequence != null) {
      filters.add(Filter.greaterThan('sequence_number', afterSequence));
    }
    if (entryType != null) {
      filters.add(Filter.equals('entry_type', entryType));
    }
    // The stored `client_timestamp` is an ISO 8601 string whose fraction
    // has three or six digits, so text order is not time order within a
    // millisecond; the bounds compare the parsed instants.
    if (clientTimestampStart != null) {
      filters.add(
        Filter.custom(
          (record) =>
              !_clientTimestampOf(record).isBefore(clientTimestampStart),
        ),
      );
    }
    if (clientTimestampEnd != null) {
      filters.add(
        Filter.custom(
          (record) => _clientTimestampOf(record).isBefore(clientTimestampEnd),
        ),
      );
    }
    if (filters.isEmpty) return null;
    if (filters.length == 1) return filters.single;
    return Filter.and(filters);
  }

  /// The instant a stored event record's `client_timestamp` names.
  static DateTime _clientTimestampOf(RecordSnapshot<Object?, Object?> record) =>
      DateTime.parse(record['client_timestamp']! as String);

  /// Reserve-and-increment the sequence counter within [txn]: the counter
  /// is advanced as a side effect so that a second call in the same
  /// transaction returns `current + 2`. A paired
  /// [appendEvent] consumes the reservation without advancing again. If
  /// the transaction rolls back, the counter rollback falls out of
  /// Sembast's transactional semantics.
  @override
  @internal
  Future<int> nextSequenceNumber(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final currentRaw = await _backendStateStore
        .record(_sequenceKey)
        .get(t._sembastTxn);
    final current = (currentRaw as int?) ?? 0;
    final reserved = current + 1;
    await _backendStateStore.record(_sequenceKey).put(t._sembastTxn, reserved);
    return reserved;
  }

  // Implements: EVS-DEV-chain-verification/C
  // events are keyed by their sequence number and the sequence counter
  //   names the latest reserved one, so the tip is read at that key, or,
  //   when the current transaction reserved it but has not yet stored an
  //   event there, at the key below.
  @override
  Future<String?> readLatestEventHash(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final counterRaw = await _backendStateStore
        .record(_sequenceKey)
        .get(t._sembastTxn);
    final counter = (counterRaw as int?) ?? 0;
    if (counter == 0) return null;
    final atCounter = await _eventStore.record(counter).get(t._sembastTxn);
    if (atCounter != null) {
      if (atCounter['sequence_number'] != counter) {
        throw StateError(
          'readLatestEventHash: the event stored at sequence $counter '
          'does not carry that sequence number',
        );
      }
      return atCounter['event_hash'] as String?;
    }
    // Nothing stored at the counter's key yet: the current transaction
    // reserved it (via nextSequenceNumber) but has not appended an event
    // under it. The tip is the event below, if any.
    final previous = counter - 1;
    if (previous == 0) return null;
    final atPrevious = await _eventStore.record(previous).get(t._sembastTxn);
    if (atPrevious == null || atPrevious['sequence_number'] != previous) {
      throw StateError(
        'readLatestEventHash: the reserved sequence counter is $counter '
        'with no event stored under it, and no event of sequence '
        '$previous is stored under the key below it either',
      );
    }
    return atPrevious['event_hash'] as String?;
  }

  // Implements: EVS-PRD-event-log/D
  // read events in order from any position
  //   (transactional variant; includes staged writes from same txn body).
  // Implements: EVS-DEV-find-all-events-extended-filters/B
  // same three
  //   optional parameters with same semantics; shared via
  //   _composeFindAllEventsFilter.
  // Implements: EVS-DEV-find-all-events-extended-filters/D
  // same shared
  //   helper reused here.
  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final t = _requireValidTxn(txn);
    final records = await _eventStore.find(
      t._reads,
      finder: Finder(
        filter: _composeFindAllEventsFilter(
          afterSequence: afterSequence,
          entryType: entryType,
          clientTimestampStart: clientTimestampStart,
          clientTimestampEnd: clientTimestampEnd,
        ),
        sortOrders: [SortOrder('sequence_number')],
        limit: limit,
      ),
    );
    return records.map((r) => StoredEvent.fromMap(r.value, r.key)).toList();
  }

  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) async* {
    final db = _database();
    final finder = Finder(
      filter: eventTypes != null
          ? Filter.inList('event_type', eventTypes.toList())
          : null,
      sortOrders: [SortOrder('sequence_number', false)],
    );
    final records = await _eventStore.find(db, finder: finder);
    for (final r in records) {
      yield StoredEvent.fromMap(r.value, r.key);
    }
  }

  // -------- Chain lookups --------
  //
  // One record, `latest_authored_sequence` in `backend_state`, holds the
  // local sequence number of the latest event this database holds as
  // authored; [appendEvent] writes it in the transaction that stores such
  // an event, and the event is read by its key, the sequence it is stored
  // under. Every other lookup scans the event store inside the caller's
  // transaction, so it sees the events stored earlier in it; only ingest,
  // the restore and the chain verification make them.

  static const _latestAuthoredSequenceKey = 'latest_authored_sequence';

  /// The `backend_state` record: the local sequence numbers of the held
  /// security-finding events, authored and received, in the order they were
  /// stored. So the marks fold reads the findings by key rather than
  /// scanning the log for them, and learns that none is held from an empty
  /// list without a scan. Findings are never removed, so entries are never
  /// cleared.
  static const _heldFindingSequencesKey = 'held_finding_sequences';

  /// The chain coordinates of the stored record [value].
  static ChainCoordinates _coordinatesOf(Map<String, Object?> value) {
    final metadata = value['metadata'];
    return ChainCoordinates.fromFields(
      sequenceNumber: value['sequence_number']! as int,
      eventId: value['event_id']! as String,
      eventHash: value['event_hash']! as String,
      previousEventHash: value['previous_event_hash'] as String?,
      provenance: metadata is Map ? metadata['provenance'] : null,
    );
  }

  /// Writes the latest-authored record when [event], just stored in [txn],
  /// is held as authored: its provenance holds one entry, naming this
  /// database.
  // Implements: EVS-DEV-chain-verification/B
  // the record of the latest event the database holds as authored is
  //   written in the transaction that stores such an event, so the append
  //   reads its predecessor inside its own transaction.
  // Implements: EVS-PRD-destinations/L
  // on Sembast the record of the latest sequence the database authored is
  //   the one chain record the backend persists beside the log.
  Future<void> _recordLatestAuthoredInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    final authoredBy = ChainCoordinates.of(event).heldAsAuthoredBy;
    if (authoredBy == null) return;
    final holder = await _backendStateStore.record(_databaseIdKey).get(txn);
    if (holder != authoredBy) return;
    await _backendStateStore
        .record(_latestAuthoredSequenceKey)
        .put(txn, event.sequenceNumber);
  }

  /// Whether [event] is an eligible version: one shared decision used by
  /// both the write side ([_recordLatestEligibleVersionInTxn]) and the read
  /// side ([readLatestEligibleVersionInTxn]), so the index and its lookup
  /// never disagree on what counts.
  // Implements: EVS-DEV-causal-parents/H
  // eligibility is causal.kind == version, causal.eligible == true and a
  //   non-null sealed hash, decided by one shared helper for the write that
  //   maintains the index and the read that serves it.
  static bool _isEligibleVersion(StoredEvent event) =>
      event.causal?.kind == CausalKind.version &&
      (event.causal?.eligible ?? false) &&
      ChainCoordinates.of(event).sealedHash != null;

  /// Writes the latest-eligible-version-by-aggregate record when [event],
  /// just stored in [txn], is an eligible version.
  // Implements: EVS-DEV-causal-parents/H
  // the record of the latest eligible version of an aggregate is written in
  //   the transaction that stores such a version, next to the
  //   latest-authored record.
  Future<void> _recordLatestEligibleVersionInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    if (!_isEligibleVersion(event)) return;
    await _latestEligibleVersionStore
        .record(event.aggregateId)
        .put(txn, event.sequenceNumber);
  }

  /// Appends [event]'s sequence number to the held-finding-sequences record
  /// when [event], just stored in [txn], is a security finding, authored or
  /// received.
  // Implements: EVS-PRD-destinations/L
  // on Sembast the record of the held security findings is written in the
  //   transaction that stores each one.
  Future<void> _recordFindingHeldInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    if (!_isSecurityFinding(event.entryType, event.eventType)) return;
    final record = _backendStateStore.record(_heldFindingSequencesKey);
    final existing = await record.get(txn);
    final sequences = (existing is List ? List<int>.from(existing) : <int>[])
      ..add(event.sequenceNumber);
    await record.put(txn, sequences);
  }

  static bool _isSecurityFinding(Object? entryType, Object? eventType) =>
      entryType == kSecurityFindingEntryType &&
      eventType == kSecurityFindingRecordedEventType;

  /// Merges [event]'s originating database and origin position into the
  /// aggregate-authorship record when [event], just stored in [txn], names
  /// an originating database — the same fold `_addAuthorship` applies in
  /// the marks module, kept in step with the backend's own index.
  // Implements: EVS-PRD-materializer/E
  // the authorship index is written in the transaction that stores each
  //   event naming an originating database.
  Future<void> _recordAggregateAuthorshipInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    final coordinates = ChainCoordinates.of(event);
    final origin = coordinates.originatingDatabaseId;
    if (origin == null) return;
    final record = _aggregateAuthorshipStore.record(event.aggregateId);
    final existing = await record.get(txn);
    final map = existing is Map
        ? Map<String, Object?>.from(existing)
        : <String, Object?>{};
    final position = coordinates.originPosition;
    final held = map[origin] as int?;
    map[origin] = position == null
        ? held
        : (held == null || position > held ? position : held);
    await record.put(txn, map);
  }

  /// The key under which [_predecessorLowestPositionStore] indexes events
  /// of [originatingDatabaseId] carrying [previousEventHash].
  static String _predecessorKey(
    String originatingDatabaseId,
    String? previousEventHash,
  ) => jsonEncode(<Object?>[originatingDatabaseId, previousEventHash]);

  /// Lowers the predecessor-lowest-position record when [event], just
  /// stored in [txn], names an originating database and an origin position,
  /// so it stays the lowest origin position among the held events sharing
  /// that database and predecessor hash.
  // Implements: EVS-PRD-materializer/E
  // the predecessor-position index is written in the transaction that
  //   stores each event naming an originating database and an origin
  //   position.
  Future<void> _recordPredecessorLowestPositionInTxn(
    sembast.Transaction txn,
    StoredEvent event,
  ) async {
    final coordinates = ChainCoordinates.of(event);
    final origin = coordinates.originatingDatabaseId;
    final position = coordinates.originPosition;
    if (origin == null || position == null) return;
    final record = _predecessorLowestPositionStore.record(
      _predecessorKey(origin, coordinates.previousEventHash),
    );
    final existing = await record.get(txn);
    if (existing == null || position < (existing as int)) {
      await record.put(txn, position);
    }
  }

  /// The stored events of [txn] whose record satisfies [matches], in
  /// ascending local sequence number.
  Future<List<StoredEvent>> _scanEventsInTxn(
    Transaction txn,
    bool Function(Map<String, Object?> value) matches,
  ) async {
    final t = _requireValidTxn(txn);
    final records = await _eventStore.find(
      t._reads,
      finder: Finder(
        filter: Filter.custom(
          (record) => matches(record.value! as Map<String, Object?>),
        ),
        sortOrders: [SortOrder('sequence_number')],
      ),
    );
    return <StoredEvent>[
      for (final r in records)
        StoredEvent.fromMap(Map<String, Object?>.from(r.value), r.key),
    ];
  }

  @override
  @internal
  Future<StoredEvent?> readLatestHeldAsAuthoredInTxn(
    Transaction txn,
    String databaseId,
  ) async {
    final t = _requireValidTxn(txn);
    final holder = await _backendStateStore
        .record(_databaseIdKey)
        .get(t._sembastTxn);
    if (holder != databaseId) return null;
    final sequence = await _backendStateStore
        .record(_latestAuthoredSequenceKey)
        .get(t._sembastTxn);
    if (sequence == null) return null;
    final value = await _eventStore.record(sequence as int).get(t._sembastTxn);
    if (value == null || value['sequence_number'] != sequence) {
      throw StateError(
        'the latest event this database authored is recorded at sequence '
        '$sequence, and no event of that sequence is stored under it',
      );
    }
    return StoredEvent.fromMap(Map<String, Object?>.from(value), sequence);
  }

  // Implements: EVS-DEV-delivery-receiver/I
  // on Sembast the latest authored event of an aggregate is found by a scan
  //   of the aggregate's events, newest first.
  @override
  @internal
  Future<StoredEvent?> readLatestAuthoredOfAggregateInTxn(
    Transaction txn, {
    required String databaseId,
    required String aggregateId,
  }) async {
    final t = _requireValidTxn(txn);
    final record = await _eventStore.findFirst(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and([
          Filter.equals('aggregate_id', aggregateId),
          Filter.custom(
            (record) =>
                _coordinatesOf(
                  record.value! as Map<String, Object?>,
                ).heldAsAuthoredBy ==
                databaseId,
          ),
        ]),
        sortOrders: [SortOrder('sequence_number', false)],
      ),
    );
    if (record == null) return null;
    return StoredEvent.fromMap(
      Map<String, Object?>.from(record.value),
      record.key,
    );
  }

  /// The filter matching the `ingest.delivery_accepted` audits the
  /// database [databaseId] holds as authored.
  static Filter _authoredDeliveryAudits(String databaseId) =>
      Filter.and(<Filter>[
        Filter.equals('event_type', kIngestDeliveryAcceptedEventType),
        Filter.equals('entry_type', kIngestAuditEntryType),
        Filter.custom(
          (record) =>
              _coordinatesOf(
                record.value! as Map<String, Object?>,
              ).heldAsAuthoredBy ==
              databaseId,
        ),
      ]);

  @override
  @internal
  Future<List<StoredEvent>> findAuthoredDeliveryAuditsInTxn(
    Transaction txn, {
    required String databaseId,
    required String aggregateId,
    required int fromDeliveryNumber,
    required int toDeliveryNumber,
  }) async {
    final t = _requireValidTxn(txn);
    final records = await _eventStore.find(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and(<Filter>[
          Filter.equals('aggregate_id', aggregateId),
          _authoredDeliveryAudits(databaseId),
          Filter.custom((record) {
            final data = (record.value! as Map<String, Object?>)['data'];
            final number = data is Map ? data['delivery_number'] : null;
            return number is num &&
                number >= fromDeliveryNumber &&
                number <= toDeliveryNumber;
          }),
        ]),
        sortOrders: [SortOrder('sequence_number')],
      ),
    );
    return <StoredEvent>[
      for (final r in records)
        StoredEvent.fromMap(Map<String, Object?>.from(r.value), r.key),
    ];
  }

  @override
  @internal
  Future<List<StoredEvent>> findLatestAuthoredDeliveryAuditsInTxn(
    Transaction txn, {
    required String databaseId,
    required Set<String> senderDatabaseIds,
  }) async {
    final t = _requireValidTxn(txn);
    final records = await _eventStore.find(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and(<Filter>[
          _authoredDeliveryAudits(databaseId),
          Filter.custom((record) {
            final data = (record.value! as Map<String, Object?>)['data'];
            final channel = data is Map ? data['channel'] : null;
            return channel is Map &&
                senderDatabaseIds.contains(channel['sender_database_id']);
          }),
        ]),
        sortOrders: [SortOrder('sequence_number')],
      ),
    );
    final latest = <String, StoredEvent>{};
    for (final r in records) {
      final event = StoredEvent.fromMap(
        Map<String, Object?>.from(r.value),
        r.key,
      );
      latest[event.aggregateId] = event;
    }
    return latest.values.toList(growable: false);
  }

  // Implements: EVS-DEV-causal-parents/H
  // the latest eligible version is read from the backend's own index: the
  //   per-aggregate record of the sequence written alongside the event by
  //   _recordLatestEligibleVersionInTxn, fetched by key — no scan, no
  //   Finder.
  @override
  @internal
  Future<StoredEvent?> readLatestEligibleVersionInTxn(
    Transaction txn,
    String aggregateId,
  ) async {
    final t = _requireValidTxn(txn);
    final sequence = await _latestEligibleVersionStore
        .record(aggregateId)
        .get(t._sembastTxn);
    if (sequence == null) return null;
    final value = await _eventStore.record(sequence as int).get(t._sembastTxn);
    if (value == null || value['sequence_number'] != sequence) {
      throw StateError(
        'the latest eligible version of aggregate $aggregateId is recorded '
        'at sequence $sequence, and no event of that sequence is stored '
        'under it',
      );
    }
    return StoredEvent.fromMap(Map<String, Object?>.from(value), sequence);
  }

  // Implements: EVS-PRD-materializer/E
  // the authorship of an aggregate's held events is read from the backend's
  //   own index: the per-aggregate record written alongside each event
  //   naming an originating database by _recordAggregateAuthorshipInTxn,
  //   fetched by key — no scan, no Finder.
  @override
  @internal
  Future<Map<String, int?>> readAggregateAuthorshipInTxn(
    Transaction txn,
    String aggregateId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _aggregateAuthorshipStore
        .record(aggregateId)
        .get(t._reads);
    if (value == null) return <String, int?>{};
    return Map<String, int?>.from(value as Map);
  }

  // Implements: EVS-PRD-materializer/E
  // a fork finding's threshold is read from the backend's own index: the
  //   per-predecessor record written alongside each event by
  //   _recordPredecessorLowestPositionInTxn, fetched by key — no scan, no
  //   Finder.
  @override
  @internal
  Future<int?> readLowestOriginPositionByPredecessorInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
  }) async {
    final t = _requireValidTxn(txn);
    final value = await _predecessorLowestPositionStore
        .record(_predecessorKey(originatingDatabaseId, previousEventHash))
        .get(t._reads);
    return value as int?;
  }

  // Implements: EVS-DEV-security-findings/E
  // on Sembast the lookup scans the finding events carrying the identity
  //   for one the database holds as authored.
  @override
  @internal
  Future<bool> holdsAuthoredSecurityFindingInTxn(
    Transaction txn, {
    required String databaseId,
    required String findingId,
  }) async {
    final t = _requireValidTxn(txn);
    final record = await _eventStore.findFirst(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and([
          Filter.equals('entry_type', kSecurityFindingEntryType),
          Filter.equals('data.finding_id', findingId),
          Filter.custom(
            (record) =>
                _coordinatesOf(
                  record.value! as Map<String, Object?>,
                ).heldAsAuthoredBy ==
                databaseId,
          ),
        ]),
      ),
    );
    return record != null;
  }

  @override
  @internal
  Future<List<StoredEvent>> findEventsBySealedHashInTxn(
    Transaction txn,
    String sealedHash,
  ) => _scanEventsInTxn(
    txn,
    (value) => _coordinatesOf(value).sealedHash == sealedHash,
  );

  @override
  @internal
  Future<List<StoredEvent>> findEventsByPredecessorInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
  }) => _scanEventsInTxn(txn, (value) {
    final coordinates = _coordinatesOf(value);
    return coordinates.originatingDatabaseId == originatingDatabaseId &&
        coordinates.previousEventHash == previousEventHash;
  });

  @override
  @internal
  Future<List<StoredEvent>> findEventsByOriginPositionInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required int originPosition,
  }) => _scanEventsInTxn(txn, (value) {
    final coordinates = _coordinatesOf(value);
    return coordinates.originatingDatabaseId == originatingDatabaseId &&
        coordinates.originPosition == originPosition;
  });

  @override
  @internal
  Future<List<StoredEvent>> findEventsFromOriginPositionInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required int fromPosition,
  }) => _scanEventsInTxn(txn, (value) {
    final coordinates = _coordinatesOf(value);
    final position = coordinates.originPosition;
    return coordinates.originatingDatabaseId == originatingDatabaseId &&
        position != null &&
        position >= fromPosition;
  });

  /// The held-finding sequence numbers recorded so far, read by key.
  Future<List<int>> _heldFindingSequencesInTxn(_SembastTxn t) async {
    final value = await _backendStateStore
        .record(_heldFindingSequencesKey)
        .get(t._reads);
    return value is List ? List<int>.from(value) : const <int>[];
  }

  // Implements: EVS-PRD-materializer/G
  // whether any finding is held is read from the backend's own index — the
  //   held-finding-sequences record's emptiness — not a scan of the log.
  @override
  @internal
  Future<bool> holdsSecurityFindingInTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    return (await _heldFindingSequencesInTxn(t)).isNotEmpty;
  }

  // Implements: EVS-PRD-materializer/G
  // the held findings are read from the backend's own index: the sequence
  //   numbers recorded alongside each one by _recordFindingHeldInTxn,
  //   fetched by key — no scan, no Finder.
  @override
  @internal
  Future<List<StoredEvent>> findSecurityFindingsInTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final sequences = await _heldFindingSequencesInTxn(t);
    final events = <StoredEvent>[];
    for (final sequence in sequences) {
      final value = await _eventStore.record(sequence).get(t._reads);
      if (value == null || value['sequence_number'] != sequence) {
        throw StateError(
          'a held security finding is recorded at sequence $sequence, and '
          'no event of that sequence is stored under it',
        );
      }
      events.add(
        StoredEvent.fromMap(Map<String, Object?>.from(value), sequence),
      );
    }
    return events;
  }

  /// Page size of [readEventsReverseInTxn].
  static const int _reverseScanInTxnPageSize = 256;

  /// Pages on `sequence_number` below the last event read, so an append
  /// in [txn] while the stream is open does not shift the pages.
  @override
  @internal
  Stream<StoredEvent> readEventsReverseInTxn(
    Transaction txn, {
    Set<String>? eventTypes,
  }) async* {
    int? lastSeenSequence;
    while (true) {
      final t = _requireValidTxn(txn);
      final filters = <Filter>[
        if (eventTypes != null)
          Filter.inList('event_type', eventTypes.toList()),
        if (lastSeenSequence != null)
          Filter.lessThan('sequence_number', lastSeenSequence),
      ];
      final records = await _eventStore.find(
        t._reads,
        finder: Finder(
          filter: filters.isEmpty
              ? null
              : (filters.length == 1 ? filters.single : Filter.and(filters)),
          sortOrders: [SortOrder('sequence_number', false)],
          limit: _reverseScanInTxnPageSize,
        ),
      );
      for (final r in records) {
        yield StoredEvent.fromMap(r.value, r.key);
      }
      if (records.length < _reverseScanInTxnPageSize) return;
      lastSeenSequence = records.last.value['sequence_number']! as int;
    }
  }

  // Replay-then-live event stream, broadcast and close-aware.
  //
  // The per-call controller is itself broadcast so a single `watchEvents()`
  // return value supports multiple `listen()` subscribers. On the first listen:
  //   1. `scheduleMicrotask(startReplay)` defers the replay so the caller's
  //      `listen()` returns before any emission, ensuring no replayed event
  //      is missed.
  //   2. The live listener on `_eventsController` attaches before the replay
  //      reads the log, and buffers what it receives until the replay has
  //      been forwarded, so an event that commits while the replay reads is
  //      not lost.
  //   3. Replay reads `findAllEvents(afterSequence: lowerBound)` and
  //      forwards each event, advancing `lastForwarded`; the buffer is then
  //      drained and the listener forwards directly. Every live event is
  //      forwarded only when its sequence number is above `lastForwarded`,
  //      so an event both read by the replay and notified live is delivered
  //      once.
  // Close on `_eventsController` propagates via `onDone`.
  // Not on the StorageBackend abstract surface — SembastBackend-specific.
  Stream<StoredEvent> watchEvents({int? afterSequence}) {
    if (_eventsController.isClosed) {
      throw StateError(
        'SembastBackend.close has been called; watchEvents unavailable',
      );
    }
    final lowerBound = afterSequence ?? 0;
    final controller = StreamController<StoredEvent>.broadcast();
    StreamSubscription<StoredEvent>? liveSub;
    var started = false;
    // Advanced by every first listen and every last cancel, so a pipeline
    // started for an earlier listen forwards nothing once it was cancelled.
    var pipeline = 0;

    Future<void> startReplay(int current) async {
      bool stale() => current != pipeline || controller.isClosed;
      if (stale()) return;
      var lastForwarded = lowerBound;
      var replayDone = false;
      final buffer = <StoredEvent>[];
      void forward(StoredEvent e) {
        if (stale() || e.sequenceNumber <= lastForwarded) return;
        controller.add(e);
        lastForwarded = e.sequenceNumber;
      }

      liveSub = _eventsController.stream.listen(
        (e) => replayDone ? forward(e) : buffer.add(e),
        onError: controller.addError,
        onDone: controller.close,
      );
      try {
        for (final e in await findAllEvents(afterSequence: lowerBound)) {
          forward(e);
        }
      } catch (err, st) {
        if (!stale()) controller.addError(err, st);
      }
      // The replay may have been cancelled while it read.
      if (stale()) return;
      for (final e in buffer) {
        forward(e);
      }
      buffer.clear();
      replayDone = true;
    }

    controller
      ..onListen = () {
        // Broadcast controllers fire `onListen` on every fresh listen
        // attach, but replay-then-live setup must run only once for the
        // lifetime of this watchEvents() call. The `started` guard
        // ensures multiple subscribers share a single replay + live
        // pipeline.
        if (started) return;
        started = true;
        final current = ++pipeline;
        scheduleMicrotask(() => startReplay(current));
      }
      ..onCancel = () async {
        // Broadcast `onCancel` fires when the LAST subscriber cancels;
        // tear down the upstream live subscription so the broadcast
        // controller does not leak after all subscribers detach. The
        // controller stays open so a later listener can re-attach
        // (broadcast semantics).
        pipeline++;
        started = false;
        final cancelled = liveSub?.cancel();
        liveSub = null;
        await cancelled;
      };
    return controller.stream;
  }

  @override
  Future<int> readSequenceCounter() async {
    final db = _database();
    final value = await _backendStateStore.record(_sequenceKey).get(db);
    return (value as int?) ?? 0;
  }

  // -------- Backend state KV --------

  @override
  Future<int> readSchemaVersion() async {
    final db = _database();
    final value = await _backendStateStore.record(_schemaVersionKey).get(db);
    return (value as int?) ?? 0;
  }

  @override
  @internal
  Future<void> writeSchemaVersion(Transaction txn, int version) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_schemaVersionKey)
        .put(t._sembastTxn, version);
  }

  static String _fillCursorKey(String destinationId) =>
      'fill_cursor_$destinationId';

  /// Read the per-destination fill cursor. Returns -1 when the key is absent
  /// (no row has yet been enqueued for this destination). Non-transactional.
  @override
  Future<int> readFillCursor(String destinationId) async {
    final db = _database();
    final value = await _backendStateStore
        .record(_fillCursorKey(destinationId))
        .get(db);
    return (value as int?) ?? -1;
  }

  /// Read the per-destination fill cursor inside [txn]. Returns -1 when the
  /// key is absent.
  @override
  @internal
  Future<int> readFillCursorTxn(Transaction txn, String destinationId) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_fillCursorKey(destinationId))
        .get(t._sembastTxn);
    return (value as int?) ?? -1;
  }

  /// Write the per-destination fill cursor inside [txn] so the advance is
  /// co-atomic with the surrounding transaction. Rolls back with the rest
  /// of the transaction body on a throw.
  @override
  @internal
  Future<void> writeFillCursorTxn(
    Transaction txn,
    String destinationId,
    int sequenceNumber,
  ) async {
    _validateFillCursorValue(sequenceNumber);
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_fillCursorKey(destinationId))
        .put(t._sembastTxn, sequenceNumber);
  }

  /// The fill cursor's legal domain is `[-1, ∞)`: `-1` is the "no row
  /// enqueued" / "rewound to pre-start" sentinel and
  /// all other values are `sequence_number`s drawn from the
  /// event log, which are non-negative ints. Reject anything smaller than
  /// `-1` at write time so a bogus caller value cannot land as a stored
  /// cursor and confuse the fill or a recovery rewind.
  void _validateFillCursorValue(int sequenceNumber) {
    if (sequenceNumber < -1) {
      throw ArgumentError.value(
        sequenceNumber,
        'sequenceNumber',
        'fill_cursor must be >= -1 (-1 = unset or rewound to pre-start; '
            'all other values are event sequence_numbers)',
      );
    }
  }

  // -------- Destination schedules --------

  static String _scheduleKey(String destinationId) => 'schedule_$destinationId';

  /// Read the persisted `DestinationSchedule` for [destinationId], or
  /// null when no schedule record exists. Non-transactional.
  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) async {
    final db = _database();
    final value = await _backendStateStore
        .record(_scheduleKey(destinationId))
        .get(db);
    if (value == null) return null;
    return DestinationSchedule.fromJson(
      Map<String, Object?>.from(value as Map),
    );
  }

  /// Read the persisted `DestinationSchedule` for [destinationId] inside
  /// [txn], or null when no schedule record exists.
  @override
  @internal
  Future<DestinationSchedule?> readScheduleTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_scheduleKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return DestinationSchedule.fromJson(
      Map<String, Object?>.from(value as Map),
    );
  }

  @override
  Future<Map<String, DestinationSchedule>> listSchedules() =>
      _listSchedules(_database());

  @override
  @internal
  Future<Map<String, DestinationSchedule>> listSchedulesTxn(Transaction txn) =>
      _listSchedules(_requireValidTxn(txn)._sembastTxn);

  Future<Map<String, DestinationSchedule>> _listSchedules(
    DatabaseClient client,
  ) async {
    const prefix = 'schedule_';
    final records = await _backendStateStore.find(
      client,
      finder: Finder(
        filter: Filter.custom(
          (record) => (record.key! as String).startsWith(prefix),
        ),
      ),
    );
    return <String, DestinationSchedule>{
      for (final record in records)
        record.key.substring(prefix.length): DestinationSchedule.fromJson(
          Map<String, Object?>.from(record.value! as Map),
        ),
    };
  }

  /// Persist [schedule] inside [txn] so the write participates in the
  /// surrounding transaction's atomicity.
  @override
  @internal
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_scheduleKey(destinationId))
        .put(t._sembastTxn, schedule.toJson());
  }

  /// Delete the persisted schedule record for [destinationId] inside
  /// [txn]. Used by `deleteDestination`.
  @override
  @internal
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_scheduleKey(destinationId))
        .delete(t._sembastTxn);
  }

  /// Retire [destinationId]'s queue inside [txn] on deletion: refuse a
  /// pending head, tombstone a wedged head, delete the null-status records
  /// (all behind the head) and the fill cursor, and keep every terminal
  /// record, the `sequence_in_queue` counter and the id in the known-FIFOs
  /// list. Pushes one post-commit `watchFifo` notification.
  // Implements: EVS-DEV-destination-drain/A
  // retire a deleted destination's queue:
  //   refuse a pending head; tombstone a wedged head; delete the pending
  //   records and the fill cursor; keep terminal records and the counter.
  @override
  @internal
  Future<QueueRetirement> retireQueueTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final head = await readFifoHeadTxn(txn, destinationId);
    if (head != null && head.finalStatus == null) {
      throw StateError(
        'retireQueueTxn($destinationId): the queue head ${head.entryId} is '
        'pending and may be in delivery; it is retired only once wedged.',
      );
    }
    String? tombstoned;
    if (head != null) {
      await setFinalStatusTxn(
        txn,
        destinationId,
        head.entryId,
        FinalStatus.tombstoned,
      );
      tombstoned = head.entryId;
    }
    final deleted = await _fifoStore(destinationId).delete(
      t._sembastTxn,
      finder: Finder(filter: Filter.isNull('final_status')),
    );
    await _backendStateStore
        .record(_fillCursorKey(destinationId))
        .delete(t._sembastTxn);
    t._fifoChanged.add(destinationId);
    return QueueRetirement(
      tombstonedRowId: tombstoned,
      deletedPendingCount: deleted,
    );
  }

  // -------- Replay requests --------

  static String _replayRequestKey(String destinationId) =>
      'replay_request_$destinationId';

  @override
  @internal
  Future<ReplayRequest?> readReplayRequestTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_replayRequestKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return ReplayRequest.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeReplayRequestTxn(
    Transaction txn,
    String destinationId,
    ReplayRequest request,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_replayRequestKey(destinationId))
        .put(t._sembastTxn, request.toJson());
  }

  @override
  @internal
  Future<void> clearReplayRequestTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_replayRequestKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Wedge records --------

  static String _wedgeRecordKey(String destinationId) => 'wedge_$destinationId';

  @override
  @internal
  Future<WedgeRecord?> readWedgeRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_wedgeRecordKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return WedgeRecord.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeWedgeRecordTxn(
    Transaction txn,
    String destinationId,
    WedgeRecord record,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_wedgeRecordKey(destinationId))
        .put(t._sembastTxn, record.toJson());
  }

  @override
  @internal
  Future<void> clearWedgeRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_wedgeRecordKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Transform failure records --------

  static String _transformFailureRecordKey(String destinationId) =>
      'transform_failure_$destinationId';

  @override
  @internal
  Future<TransformFailureRecord?> readTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_transformFailureRecordKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return TransformFailureRecord.fromJson(
      Map<String, Object?>.from(value as Map),
    );
  }

  @override
  @internal
  Future<void> writeTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
    TransformFailureRecord record,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_transformFailureRecordKey(destinationId))
        .put(t._sembastTxn, record.toJson());
  }

  @override
  @internal
  Future<void> clearTransformFailureRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_transformFailureRecordKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Halt requests --------

  static String _haltRequestKey(String destinationId) =>
      'halt_request_$destinationId';

  @override
  @internal
  Future<HaltRequest?> readHaltRequestTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_haltRequestKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return HaltRequest.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeHaltRequestTxn(
    Transaction txn,
    String destinationId,
    HaltRequest request,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_haltRequestKey(destinationId))
        .put(t._sembastTxn, request.toJson());
  }

  @override
  @internal
  Future<void> clearHaltRequestTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_haltRequestKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Send fences --------

  static String _sendFenceKey(String destinationId) =>
      'send_fence_$destinationId';

  @override
  @internal
  Future<SendFence?> readSendFenceTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_sendFenceKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return SendFence.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeSendFenceTxn(
    Transaction txn,
    String destinationId,
    SendFence fence,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_sendFenceKey(destinationId))
        .put(t._sembastTxn, fence.toJson());
  }

  @override
  @internal
  Future<void> clearSendFenceTxn(Transaction txn, String destinationId) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_sendFenceKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Sender channel records --------

  static String _senderChannelKey(String destinationId) =>
      'sender_channel_$destinationId';

  @override
  @internal
  Future<SenderChannelRecord?> readSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_senderChannelKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return SenderChannelRecord.fromJson(
      Map<String, Object?>.from(value as Map),
    );
  }

  @override
  @internal
  Future<void> writeSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
    SenderChannelRecord record,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_senderChannelKey(destinationId))
        .put(t._sembastTxn, record.toJson());
  }

  @override
  @internal
  Future<void> clearSenderChannelRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_senderChannelKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Registry check record --------

  static const _registryCheckKey = 'registry_check';

  @override
  @internal
  Future<void> writeRegistryCheckTxn(
    Transaction txn,
    RegistryCheck check,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_registryCheckKey)
        .put(t._sembastTxn, check.toJson());
  }

  @override
  @internal
  Future<RegistryCheck?> readRegistryCheckTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_registryCheckKey)
        .get(t._sembastTxn);
    if (value == null) return null;
    return RegistryCheck.fromJson(Map<String, Object?>.from(value as Map));
  }

  // -------- Data generation --------

  static const _dataGenerationKey = 'data_generation';

  /// On the web every `SembastBackend` registers with the browser's lock
  /// manager, under the name of its database (the IndexedDB name, unique
  /// per origin); the public sembast `Database` API cannot tell an
  /// IndexedDB database from an in-memory one, so an in-memory database on
  /// the web is guarded by its name too. Elsewhere a Sembast database is
  /// used by one process and the registration holds nothing.
  // Implements: EVS-DEV-version-compatibility/H
  // Web Locks on the web; nothing on io, where the database is used by one
  //   process.
  @override
  @internal
  Future<GenerationRegistration> registerGeneration(
    GenerationDescriptor descriptor,
  ) => registerBrowserGeneration(
    path: _database().path,
    descriptor: descriptor,
    bootLockWait: _bootLockWait,
  );

  @override
  @internal
  Future<GenerationRecord?> readDataGenerationTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_dataGenerationKey)
        .get(t._sembastTxn);
    if (value == null) return null;
    return GenerationRecord.fromJson(value);
  }

  @override
  @internal
  Future<void> writeDataGenerationTxn(
    Transaction txn,
    GenerationRecord record,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_dataGenerationKey)
        .put(t._sembastTxn, record.toJson());
  }

  // -------- Drain lock and drain records --------

  static const _drainEpochKey = 'drain_epoch';
  static const _drainerDeclarationKey = 'drainer_declaration';
  static const _drainHeartbeatKey = 'drain_heartbeat';
  static String _refillGuardKey(String destinationId) =>
      'refill_guard_$destinationId';

  /// The wrapped `Database` object: the drain lock excludes drainers per
  /// open database handle in this isolate.
  @override
  @internal
  Object drainExclusionKey(String databaseId) => _db;

  // Implements: EVS-DEV-destination-drain-lock/A
  // Sembast: an isolate-local registry keyed by the identity of the wrapped
  //   database handle, and in the browser the Web Lock of the database;
  //   every acquisition raises the drain epoch.
  /// Outside the browser the isolate registry is the whole lock. In the
  /// browser the acquisition also takes the database's Web Lock after the
  /// registry entry and before the epoch raise; it is refused
  /// ([DrainLockUnavailableException]) while another tab holds that lock or
  /// while the page is hidden, and throws [DrainLockConfigurationException]
  /// on a page without a lock manager.
  @override
  @internal
  Future<DrainLock> tryAcquireDrainLock({required String databaseId}) async {
    if (_closed) throw const DrainLockBackendClosedException();
    return _acquireDrainLock(
      obtainExclusion: () => tryBrowserDrainExclusion(
        path: _database().path,
        databaseId: databaseId,
      ),
    );
  }

  Future<DrainLock> _acquireDrainLock({
    required Future<DrainExclusionHold?> Function() obtainExclusion,
  }) async {
    final lock = await acquireIsolateDrainLock(
      backend: this,
      handle: _db,
      obtainExclusion: obtainExclusion,
      bumpEpoch: (insideBump) => transaction((txn) async {
        final t = _requireValidTxn(txn);
        final record = _backendStateStore.record(_drainEpochKey);
        final current = await record.get(t._sembastTxn);
        final next = (current is int ? current : 0) + 1;
        await record.put(t._sembastTxn, next);
        insideBump();
        return next;
      }),
    );
    // A close that ran while the acquisition was in flight released the
    // lock it knew of, not this one.
    if (_closed) {
      await lock.release();
      throw const DrainLockBackendClosedException();
    }
    _drainLock = lock;
    return lock;
  }

  /// The drain lock last granted through this backend; [close] releases
  /// it.
  DrainLock? _drainLock;

  /// Set by [close]: the backend grants no drain lock afterwards.
  bool _closed = false;

  /// In the browser the request waits for the database's Web Lock while the
  /// page is visible and withdraws while it is hidden, and ends at once when
  /// the backend is closed or its handle cannot commit; elsewhere it
  /// retries the isolate registry every [retryInterval] and when this
  /// handle's lock is released.
  @override
  @internal
  DrainLockRequest requestDrainLock({
    required String databaseId,
    required Duration retryInterval,
  }) =>
      requestBrowserDrainLock(
        path: _database().path,
        databaseId: databaseId,
        acquireHolding: (exclusion) async {
          if (_closed) throw const DrainLockBackendClosedException();
          return _acquireDrainLock(obtainExclusion: () async => exclusion);
        },
        retryInterval: retryInterval,
        wake: () => isolateDrainLockReleased(_db),
        ended: _gone.future,
      ) ??
      RetryingDrainLockRequest(
        attempt: () => tryAcquireDrainLock(databaseId: databaseId),
        retryInterval: retryInterval,
        wake: () => isolateDrainLockReleased(_db),
      );

  @override
  @internal
  Future<int?> readDrainEpochTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_drainEpochKey)
        .get(t._sembastTxn);
    return value as int?;
  }

  @override
  @internal
  Future<DrainerDeclaration?> readDrainerDeclarationTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_drainerDeclarationKey)
        .get(t._sembastTxn);
    if (value == null) return null;
    return DrainerDeclaration.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeDrainerDeclarationTxn(
    Transaction txn,
    DrainerDeclaration declaration,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_drainerDeclarationKey)
        .put(t._sembastTxn, declaration.toJson());
  }

  @override
  @internal
  Future<DrainHeartbeat?> readDrainHeartbeatTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_drainHeartbeatKey)
        .get(t._sembastTxn);
    if (value == null) return null;
    return DrainHeartbeat.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeDrainHeartbeatTxn(
    Transaction txn,
    DrainHeartbeat heartbeat,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_drainHeartbeatKey)
        .put(t._sembastTxn, heartbeat.toJson());
  }

  @override
  @internal
  Future<RefillGuard?> readRefillGuardTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_refillGuardKey(destinationId))
        .get(t._sembastTxn);
    if (value == null) return null;
    return RefillGuard.fromJson(Map<String, Object?>.from(value as Map));
  }

  @override
  @internal
  Future<void> writeRefillGuardTxn(
    Transaction txn,
    String destinationId,
    RefillGuard guard,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_refillGuardKey(destinationId))
        .put(t._sembastTxn, guard.toJson());
  }

  @override
  @internal
  Future<void> clearRefillGuardTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_refillGuardKey(destinationId))
        .delete(t._sembastTxn);
  }

  // -------- Database identity and boot record --------

  static const _databaseIdKey = 'database_id';
  static const _bootCheckKey = 'boot_check';

  @override
  @internal
  Future<String?> readDatabaseIdTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore.record(_databaseIdKey).get(t._reads);
    if (value == null) return null;
    if (value is! String || value.isEmpty) {
      throw StateError(
        'backend_state[$_databaseIdKey] is not a non-empty string; '
        'database corrupted',
      );
    }
    return value;
  }

  @override
  @internal
  Future<String> readOrCreateDatabaseIdTxn(Transaction txn) async {
    final existing = await readDatabaseIdTxn(txn);
    if (existing != null) return existing;
    final t = _requireValidTxn(txn);
    final minted = const Uuid().v4();
    await _backendStateStore.record(_databaseIdKey).put(t._sembastTxn, minted);
    return minted;
  }

  @override
  @internal
  Future<void> writeBootCheckTxn(Transaction txn, BootCheck check) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_bootCheckKey)
        .put(t._sembastTxn, check.toJson());
  }

  @override
  @internal
  Future<BootCheck?> readBootCheckTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final value = await _backendStateStore
        .record(_bootCheckKey)
        .get(t._sembastTxn);
    if (value == null) return null;
    return BootCheck.fromJson(Map<String, Object?>.from(value as Map));
  }

  // -------- Generic view storage --------

  final Map<String, StoreRef<String, Map<String, Object?>>> _viewStoreCache =
      <String, StoreRef<String, Map<String, Object?>>>{};

  StoreRef<String, Map<String, Object?>> _viewStore(String viewName) =>
      _viewStoreCache.putIfAbsent(
        viewName,
        () => stringMapStoreFactory.store(viewName),
      );

  @override
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final t = _requireValidTxn(txn);
    final raw = await _viewStore(viewName).record(key).get(t._sembastTxn);
    if (raw == null) return null;
    return Map<String, dynamic>.from(raw);
  }

  @override
  @internal
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  ) async {
    final t = _requireValidTxn(txn);
    await _viewStore(
      viewName,
    ).record(key).put(t._sembastTxn, Map<String, Object?>.from(row));
    t._postCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
  }

  @override
  @internal
  Future<void> deleteViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final t = _requireValidTxn(txn);
    await _retireTableRowIndexInTxn(t._sembastTxn, viewName, key);
    await _viewStore(viewName).record(key).delete(t._sembastTxn);
    t._postCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
  }

  /// Removes [key]'s entry, if any, from the `TableProjectionSpec` row
  /// index of [copyId]: the source aggregate it was last indexed under
  /// ([_tableRowSourceStore]) and its slot in that aggregate's row-key list
  /// ([_tableRowsBySourceStore]). A no-op for a key the index never
  /// recorded (an `AggregateProjectionSpec` row, or one never upserted
  /// through [upsertTableViewRowInTxn]).
  Future<void> _retireTableRowIndexInTxn(
    sembast.Transaction txn,
    String copyId,
    String key,
  ) async {
    final sourceRecord = _tableRowSourceStore.record(
      _rowSourceIndexKey(copyId, key),
    );
    final source = await sourceRecord.get(txn);
    if (source is! String) return;
    await _removeFromTableRowsBySourceInTxn(txn, copyId, source, key);
    await sourceRecord.delete(txn);
  }

  Future<void> _addToTableRowsBySourceInTxn(
    sembast.Transaction txn,
    String copyId,
    String sourceAggregateId,
    String key,
  ) async {
    final record = _tableRowsBySourceStore.record(
      _bySourceIndexKey(copyId, sourceAggregateId),
    );
    final existing = await record.get(txn);
    final keys = existing is List ? List<String>.from(existing) : <String>[];
    if (!keys.contains(key)) {
      keys.add(key);
      await record.put(txn, keys);
    }
  }

  Future<void> _removeFromTableRowsBySourceInTxn(
    sembast.Transaction txn,
    String copyId,
    String sourceAggregateId,
    String key,
  ) async {
    final record = _tableRowsBySourceStore.record(
      _bySourceIndexKey(copyId, sourceAggregateId),
    );
    final existing = await record.get(txn);
    if (existing is! List) return;
    final keys = List<String>.from(existing)..remove(key);
    if (keys.isEmpty) {
      await record.delete(txn);
    } else {
      await record.put(txn, keys);
    }
  }

  /// Removes every entry of [copyId] from both halves of the
  /// `TableProjectionSpec` row index: a full scan of the two index stores,
  /// filtered to [copyId]'s entries by decoding each compound key. Used only
  /// by [clearViewInTxn] and [deleteViewCopyRecordInTxn] -- teardown paths,
  /// not the append path this index exists to keep off a store-sized scan.
  Future<void> _clearTableRowIndexInTxn(
    sembast.Transaction txn,
    String copyId,
  ) async {
    final sourceKeys = await _tableRowSourceStore.findKeys(
      txn,
      finder: Finder(
        filter: Filter.custom(
          (snapshot) => _copyIdOfIndexKey(snapshot.key) == copyId,
        ),
      ),
    );
    if (sourceKeys.isNotEmpty) {
      await _tableRowSourceStore.records(sourceKeys).delete(txn);
    }
    final bySourceKeys = await _tableRowsBySourceStore.findKeys(
      txn,
      finder: Finder(
        filter: Filter.custom(
          (snapshot) => _copyIdOfIndexKey(snapshot.key) == copyId,
        ),
      ),
    );
    if (bySourceKeys.isNotEmpty) {
      await _tableRowsBySourceStore.records(bySourceKeys).delete(txn);
    }
  }

  // Implements: EVS-PRD-materializer/E
  // a TableProjectionSpec upsert stamps its row's source aggregate into the
  //   backend's own index, moving a key's entry when a later insert
  //   produces it under a different aggregate, so the outstanding-finding
  //   refresh of EVS-PRD-materializer/G can find the rows one aggregate
  //   produced without scanning the view.
  @override
  @internal
  Future<void> upsertTableViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  }) async {
    final t = _requireValidTxn(txn);
    final sembastTxn = t._sembastTxn;
    final sourceRecord = _tableRowSourceStore.record(
      _rowSourceIndexKey(copyId, key),
    );
    final priorSource = await sourceRecord.get(sembastTxn);
    if (priorSource != sourceAggregateId) {
      if (priorSource is String) {
        await _removeFromTableRowsBySourceInTxn(
          sembastTxn,
          copyId,
          priorSource,
          key,
        );
      }
      await _addToTableRowsBySourceInTxn(
        sembastTxn,
        copyId,
        sourceAggregateId,
        key,
      );
      await sourceRecord.put(sembastTxn, sourceAggregateId);
    }
    await upsertViewRowInTxn(txn, copyId, key, row);
  }

  // Implements: EVS-PRD-materializer/E
  // the rows a TableProjectionSpec view holds for one source aggregate are
  //   read from the backend's own index, fetched by key — no scan of the
  //   whole copy.
  @override
  @internal
  Future<List<Map<String, dynamic>>> findTableRowsBySourceAggregateInTxn(
    Transaction txn,
    String copyId,
    String sourceAggregateId,
  ) async {
    final t = _requireValidTxn(txn);
    final value = await _tableRowsBySourceStore
        .record(_bySourceIndexKey(copyId, sourceAggregateId))
        .get(t._reads);
    if (value == null) return <Map<String, dynamic>>[];
    final rows = <Map<String, dynamic>>[];
    for (final key in List<String>.from(value as List)) {
      final raw = await _viewStore(copyId).record(key).get(t._reads);
      if (raw != null) rows.add(Map<String, dynamic>.from(raw));
    }
    return rows;
  }

  @override
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) async {
    final db = _database();
    final records = await _viewStore(viewName).find(
      db,
      finder: Finder(limit: limit, offset: offset),
    );
    return records
        .map((r) => Map<String, dynamic>.from(r.value))
        .toList(growable: false);
  }

  // Implements: EVS-PRD-subscription/A
  // bulk key-set read backing the scoped
  //   (filtered materialized-state) AggregateMode snapshot. `records(keys).get`
  //   returns values aligned with the requested key list (null for absent),
  //   zipped back into a key->row map so the caller can re-associate and signal
  //   absent ids.
  @override
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  ) async {
    if (keys.isEmpty) return const <String, Map<String, dynamic>>{};
    final db = _database();
    final keyList = keys.toList(growable: false);
    final values = await _viewStore(viewName).records(keyList).get(db);
    final out = <String, Map<String, dynamic>>{};
    for (var i = 0; i < keyList.length; i++) {
      final v = values[i];
      if (v != null) out[keyList[i]] = Map<String, dynamic>.from(v);
    }
    return out;
  }

  // Implements: EVS-DEV-converging-view-reads/A
  // the transactional counterpart of
  //   readViewRowsByKeys, reading against the issued transaction's own
  //   handle so a by-key row fetch shares one storage transaction with a
  //   preceding state read.
  @override
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeysInTxn(
    Transaction txn,
    String viewName,
    Set<String> keys,
  ) async {
    if (keys.isEmpty) return const <String, Map<String, dynamic>>{};
    final t = _requireValidTxn(txn);
    final keyList = keys.toList(growable: false);
    final values = await _viewStore(
      viewName,
    ).records(keyList).get(t._sembastTxn);
    final out = <String, Map<String, dynamic>>{};
    for (var i = 0; i < keyList.length; i++) {
      final v = values[i];
      if (v != null) out[keyList[i]] = Map<String, dynamic>.from(v);
    }
    return out;
  }

  // Implements: EVS-PRD-permissions-as-events
  // transactional multi-row
  //   view-read primitive for the scoped-permissions authorize stage.
  // Implements: EVS-PRD-action-dispatch
  // same dispatch-transaction
  //   coherence requirement on the authorize side.
  @override
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final t = _requireValidTxn(txn);
    Filter? filter;
    if (where != null && where.isNotEmpty) {
      final clauses = where.entries
          .map((e) => Filter.equals(e.key, e.value))
          .toList(growable: false);
      filter = clauses.length == 1 ? clauses.single : Filter.and(clauses);
    }
    final records = await _viewStore(viewName).find(
      t._sembastTxn,
      finder: Finder(filter: filter, limit: limit, offset: offset),
    );
    return records
        .map((r) => Map<String, dynamic>.from(r.value))
        .toList(growable: false);
  }

  @override
  @internal
  Future<void> clearViewInTxn(Transaction txn, String viewName) async {
    final t = _requireValidTxn(txn);
    await _clearTableRowIndexInTxn(t._sembastTxn, viewName);
    await _viewStore(viewName).delete(t._sembastTxn);
    t._postCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
  }

  // -------- View copies --------
  //
  // One sembast store (`view_copies`), keyed by copy id, holding
  // `{copy_id, view_name, fingerprint, watermark, marked_for_deletion}`
  // rows. "At most one unmarked copy per fingerprint"
  // (EVS-DEV-view-convergence/A) is enforced in application code, since
  // sembast has no partial-unique-index primitive: a create checks for an
  // existing unmarked row of the fingerprint inside the same transaction
  // before inserting.

  static const _viewCopiesStore = 'view_copies';

  final StoreRef<String, Map<String, Object?>> _viewCopiesStoreRef =
      stringMapStoreFactory.store(_viewCopiesStore);

  ViewCopy _viewCopyOf(Map<String, Object?> row) => ViewCopy(
    copyId: row['copy_id'] as String,
    viewName: row['view_name'] as String,
    fingerprint: row['fingerprint'] as String,
    watermark: row['watermark'] as int,
    markedForDeletion: row['marked_for_deletion'] as bool,
  );

  // Implements: EVS-DEV-view-convergence/A
  // at most one copy of a fingerprint that is not marked for deletion;
  //   enforced by checking for an existing unmarked row of the fingerprint
  //   before inserting, inside the caller's transaction.
  @override
  @internal
  Future<String> createViewCopyInTxn(
    Transaction txn,
    String viewName,
    String fingerprint,
    int watermark,
  ) async {
    final t = _requireValidTxn(txn);
    final existing = await _viewCopiesStoreRef.find(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and([
          Filter.equals('fingerprint', fingerprint),
          Filter.equals('marked_for_deletion', false),
        ]),
      ),
    );
    if (existing.isNotEmpty) {
      throw StateError(
        'createViewCopyInTxn: an unmarked copy of fingerprint '
        '"$fingerprint" already exists (copy_id ${existing.first.key})',
      );
    }
    final copyId = _uuidGen.v4();
    await _viewCopiesStoreRef.record(copyId).put(t._sembastTxn, {
      'copy_id': copyId,
      'view_name': viewName,
      'fingerprint': fingerprint,
      'watermark': watermark,
      'marked_for_deletion': false,
    });
    return copyId;
  }

  @override
  Future<List<ViewCopy>> readViewCopiesInTxn(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final records = await _viewCopiesStoreRef.find(t._sembastTxn);
    return records.map((r) => _viewCopyOf(r.value)).toList(growable: false);
  }

  @override
  Future<ViewCopy?> readUnmarkedViewCopyInTxn(
    Transaction txn,
    String fingerprint,
  ) async {
    final t = _requireValidTxn(txn);
    final records = await _viewCopiesStoreRef.find(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and([
          Filter.equals('fingerprint', fingerprint),
          Filter.equals('marked_for_deletion', false),
        ]),
      ),
    );
    if (records.isEmpty) return null;
    return _viewCopyOf(records.single.value);
  }

  @override
  @internal
  Future<void> setViewCopyWatermarkInTxn(
    Transaction txn,
    String copyId,
    int watermark,
  ) async {
    final t = _requireValidTxn(txn);
    final record = _viewCopiesStoreRef.record(copyId);
    final existing = await record.get(t._sembastTxn);
    if (existing == null) return;
    await record.put(t._sembastTxn, {...existing, 'watermark': watermark});
  }

  @override
  @internal
  Future<void> markViewCopyForDeletionInTxn(
    Transaction txn,
    String copyId,
  ) async {
    final t = _requireValidTxn(txn);
    final record = _viewCopiesStoreRef.record(copyId);
    final existing = await record.get(t._sembastTxn);
    if (existing == null || existing['marked_for_deletion'] == true) return;
    await record.put(t._sembastTxn, {...existing, 'marked_for_deletion': true});
  }

  @override
  @internal
  Future<int> deleteViewCopyRowsInTxn(
    Transaction txn,
    String copyId, {
    required int limit,
  }) async {
    final t = _requireValidTxn(txn);
    final keys = await _viewStore(
      copyId,
    ).findKeys(t._sembastTxn, finder: Finder(limit: limit));
    if (keys.isEmpty) return 0;
    for (final key in keys) {
      await _retireTableRowIndexInTxn(t._sembastTxn, copyId, key);
    }
    await _viewStore(copyId).records(keys).delete(t._sembastTxn);
    return keys.length;
  }

  @override
  @internal
  Future<void> deleteViewCopyRecordInTxn(Transaction txn, String copyId) async {
    final t = _requireValidTxn(txn);
    // deleteViewCopyRowsInTxn retires each row's index entry as it deletes
    // the row, so [copyId]'s share of the index is normally already empty
    // by the time a caller deletes the copy's own record; this call is
    // defense in depth against a caller reaching this record without
    // draining rows first, so no entry is ever stranded under a deleted
    // copy id.
    await _clearTableRowIndexInTxn(t._sembastTxn, copyId);
    await _viewCopiesStoreRef.record(copyId).delete(t._sembastTxn);
  }

  // -------- FIFO --------

  /// Append a queue item to [destinationId]'s FIFO inside [txn], so the
  /// FIFO-row write and the caller's accompanying writes commit or roll
  /// back together. Used by
  /// `fillBatch` to keep the enqueue, the fill_cursor advance and a
  /// cleared replay request co-atomic.
  ///
  /// Exactly one of [wirePayload] / [nativeEnvelope] SHALL be non-null:
  ///
  /// - [wirePayload] (3rd-party): persists `wire_payload = decoded JSON
  ///   map`, `wire_format = wirePayload.contentType`,
  ///   `transform_version = wirePayload.transformVersion`,
  ///   `envelope_metadata = null`.
  /// - [nativeEnvelope] (native (`esd/batch@3`)): persists
  ///   `wire_payload = null`, `wire_format = "esd/batch@3"`,
  ///   `transform_version = null`, `envelope_metadata = nativeEnvelope`.
  ///
  /// Centralizes all row-construction logic: empty-batch rejection,
  /// XOR-shape enforcement, v4-UUID `entry_id` minting,
  /// `sequence_in_queue` assignment, and the known-FIFOs registry
  /// bookkeeping all live here.
  @override
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
  }) async {
    if (batch.isEmpty) {
      throw ArgumentError.value(
        batch,
        'batch',
        'enqueueFifoTxn requires a non-empty batch',
      );
    }
    // Resolve the row's wire-format / payload columns. A transform-failed
    // item carries no payload and no envelope; every other item is exactly
    // one of the two payload shapes (XOR enforced below), which supplies
    // wireFormat/transformVersion itself.
    Map<String, Object?>? payloadMap;
    String resolvedWireFormat;
    String? resolvedTransformVersion;
    if (transformFailed) {
      if (wirePayload != null || nativeEnvelope != null) {
        throw ArgumentError(
          'enqueueFifoTxn: a transform-failed item carries no payload and '
          'no envelope; got '
          'wirePayload=${wirePayload == null ? "null" : "set"}, '
          'nativeEnvelope=${nativeEnvelope == null ? "null" : "set"}',
        );
      }
      if (wireFormat == null) {
        throw ArgumentError(
          'enqueueFifoTxn: a transform-failed item needs wireFormat',
        );
      }
      if (transformFailures == null || transformFailures < 1) {
        throw ArgumentError.value(
          transformFailures,
          'transformFailures',
          'a transform-failed item needs at least one recorded failure',
        );
      }
      payloadMap = null;
      resolvedWireFormat = wireFormat;
      resolvedTransformVersion = transformVersion;
    } else {
      // XOR enforcement: exactly one payload shape is legal. Reject both
      // null and both non-null at the boundary so a downstream FIFO row
      // never carries an ambiguous (wire_payload, envelope_metadata) pair.
      if ((wirePayload == null) == (nativeEnvelope == null)) {
        throw ArgumentError(
          'enqueueFifoTxn requires exactly one of wirePayload or '
          'nativeEnvelope to be non-null; got '
          'wirePayload=${wirePayload == null ? "null" : "set"}, '
          'nativeEnvelope=${nativeEnvelope == null ? "null" : "set"}',
        );
      }
      if (nativeEnvelope != null) {
        payloadMap = null;
        resolvedWireFormat = nativeEnvelope.wireFormat;
        resolvedTransformVersion = null;
      } else {
        // 3rd-party: bytes MUST be valid JSON encoding a Map — destinations
        // that transform to bytes representing a top-level JSON object
        // conform; other shapes are rejected with ArgumentError rather
        // than corrupting the FIFO row.
        final wp = wirePayload!;
        try {
          final decoded = jsonDecode(utf8.decode(wp.bytes));
          if (decoded is! Map) {
            throw ArgumentError.value(
              wp,
              'wirePayload',
              'enqueueFifoTxn requires wirePayload.bytes to encode a JSON '
                  'object (Map); got ${decoded.runtimeType}',
            );
          }
          payloadMap = Map<String, Object?>.from(decoded);
        } on FormatException catch (e) {
          throw ArgumentError.value(
            wp,
            'wirePayload',
            'enqueueFifoTxn requires wirePayload.bytes to be UTF-8 JSON: '
                '${e.message}',
          );
        }
        resolvedWireFormat = wp.contentType;
        resolvedTransformVersion = wp.transformVersion;
      }
    }
    final t = _requireValidTxn(txn);
    final eventIds = batch.map((e) => e.eventId).toList(growable: false);
    final sequenceRange = (
      firstSeq: batch.first.sequenceNumber,
      lastSeq: batch.last.sequenceNumber,
    );
    // Mint a v4 UUID for this row's entry_id. The identifier is opaque
    // and has no relationship to the events the row carries — callers
    // that need event-level correlation use `eventIds` / `sequenceRange`.
    // UUID generation means two FIFO rows (of any final_status, including
    // tombstoned archive rows) never share an entry_id, so
    // `tombstoneAndRefill` can coexist with fresh rows re-promoting the
    // same underlying events.
    final entryId = _uuidGen.v4();
    final enqueuedAt = DateTime.now().toUtc();
    final store = _fifoStore(destinationId);
    // Assign the next sequence_in_queue from a persisted per-destination
    // counter (backend_state/fifo_seq_counter_<destinationId>). The
    // counter advances strictly monotonically and is NEVER reset: even
    // when a row is deleted (trail sweep), the deleted
    // slot is NOT re-used. The resulting invariant is
    // load-bearing for event-log cursor math and for the send-log's
    // auditability — two different rows with the same sequence_in_queue
    // would produce ambiguous "which row was deleted?" diagnostics.
    //
    // Storing the counter in backend_state rather than deriving from
    // max(existing key) + 1 at read time closes the reuse path: the
    // previous derivation would reassign slot N if row N was deleted
    // and row N was the current max.
    final counterRec = _backendStateStore.record(
      _fifoSeqCounterKey(destinationId),
    );
    final currentCounter = (await counterRec.get(t._sembastTxn) as int?) ?? 0;
    final assigned = currentCounter + 1;
    await counterRec.put(t._sembastTxn, assigned);
    final entry = FifoEntry(
      entryId: entryId,
      eventIds: eventIds,
      sequenceRange: sequenceRange,
      sequenceInQueue: assigned,
      wirePayload: payloadMap,
      wireFormat: resolvedWireFormat,
      transformVersion: resolvedTransformVersion,
      enqueuedAt: enqueuedAt,
      attempts: const <AttemptResult>[],
      finalStatus: null,
      sentAt: null,
      envelopeMetadata: nativeEnvelope,
      transformFailed: transformFailed,
      transformFailures: transformFailed ? transformFailures : null,
    );
    await store.record(assigned).put(t._sembastTxn, entry.toJson());
    await _registerFifoDestinationSembast(t._sembastTxn, destinationId);
    t._fifoChanged.add(destinationId);
    return entry;
  }

  Future<void> _registerFifoDestinationSembast(
    sembast.Transaction sembastTxn,
    String destinationId,
  ) async {
    final current =
        (await _backendStateStore.record(_knownFifosKey).get(sembastTxn)
                as List?)
            ?.cast<String>()
            .toList() ??
        <String>[];
    if (!current.contains(destinationId)) {
      current.add(destinationId);
      await _backendStateStore.record(_knownFifosKey).put(sembastTxn, current);
    }
  }

  Future<List<String>> _knownFifoDestinations() async {
    final db = _database();
    final value = await _backendStateStore.record(_knownFifosKey).get(db);
    return (value as List?)?.cast<String>().toList() ?? const <String>[];
  }

  /// Head row of [destinationId]'s FIFO: the first row in
  /// `sequence_in_queue` order whose `final_status` is either `null`
  /// (pre-terminal; drain may attempt) or [FinalStatus.wedged] (blocking
  /// terminal; drain halts on seeing this value). Rows whose
  /// `final_status` is [FinalStatus.sent] or [FinalStatus.tombstoned] are
  /// terminal-passable and are SKIPPED. Returns `null` when no row in
  /// `{null, wedged}` exists — i.e., the FIFO is empty or every row is
  /// terminal-passable.
  ///
  /// The wedge is enforced by the caller: on a wedged return value
  /// `drain` returns without calling `Destination.send`. Exposing the
  /// wedged row here (rather than filtering it out) lets UI surfaces
  /// observe the wedge via this single entry point without a separate
  /// `wedgedFifos` probe.
  @override
  Future<FifoEntry?> readFifoHead(String destinationId) =>
      _readFifoHead(_database(), destinationId);

  @override
  @internal
  Future<FifoEntry?> readFifoHeadTxn(Transaction txn, String destinationId) =>
      _readFifoHead(_requireValidTxn(txn)._sembastTxn, destinationId);

  Future<FifoEntry?> _readFifoHead(
    DatabaseClient client,
    String destinationId,
  ) async {
    final store = _fifoStore(destinationId);
    final records = await store.find(
      client,
      finder: Finder(
        filter: Filter.or([
          Filter.isNull('final_status'),
          Filter.equals('final_status', FinalStatus.wedged.toJson()),
        ]),
        sortOrders: [SortOrder('sequence_in_queue')],
        limit: 1,
      ),
    );
    if (records.isEmpty) return null;
    return FifoEntry.fromJson(Map<String, Object?>.from(records.single.value));
  }

  @override
  @internal
  Future<List<FifoEntry>> listFifoEntriesTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final records = await _fifoStore(destinationId).find(
      _requireValidTxn(txn)._sembastTxn,
      finder: Finder(sortOrders: [SortOrder('sequence_in_queue')]),
    );
    return records
        .map((r) => FifoEntry.fromJson(Map<String, Object?>.from(r.value)))
        .toList();
  }

  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) async {
    final db = _database();
    final store = _fifoStore(destinationId);
    final records = await store.find(
      db,
      finder: Finder(
        filter: afterSequenceInQueue != null
            ? Filter.greaterThan('sequence_in_queue', afterSequenceInQueue)
            : null,
        sortOrders: [SortOrder('sequence_in_queue')],
        limit: limit,
      ),
    );
    return records
        .map((r) => FifoEntry.fromJson(Map<String, Object?>.from(r.value)))
        .toList();
  }

  // Snapshot-on-subscribe + live-re-emission stream for a destination's FIFO.
  // The per-call controller is broadcast so a single `watchFifo()` return value
  // supports multiple `listen()` subscribers. On the first listen:
  //   1. `scheduleMicrotask(emitSnapshot)` defers the initial snapshot so
  //      the caller's `listen()` returns before any emission.
  //   2. A subscription to `_fifoChangesController` re-emits a fresh snapshot
  //      whenever the changed destinationId matches (cross-destination isolation).
  // Snapshot fetch goes through `listFifoEntries`, so an unknown destination
  // produces an empty list. Close on `_fifoChangesController` propagates via
  // `onDone`. Not on the StorageBackend abstract surface — SembastBackend-specific.
  Stream<List<FifoEntry>> watchFifo(String destinationId) {
    if (_fifoChangesController.isClosed) {
      throw StateError(
        'SembastBackend.close has been called; watchFifo unavailable',
      );
    }
    final controller = StreamController<List<FifoEntry>>.broadcast();
    StreamSubscription<String>? changesSub;
    var started = false;

    Future<void> emitSnapshot() async {
      try {
        final snap = await listFifoEntries(destinationId);
        if (!controller.isClosed) controller.add(snap);
      } catch (err, st) {
        if (!controller.isClosed) controller.addError(err, st);
      }
    }

    controller
      ..onListen = () {
        // Broadcast controllers fire `onListen` on every fresh listen
        // attach, but the snapshot + change-listener wiring must run
        // only once for the lifetime of this watchFifo() call. The
        // `started` guard ensures multiple subscribers share a single
        // upstream subscription.
        if (started) return;
        started = true;
        scheduleMicrotask(emitSnapshot);
        changesSub = _fifoChangesController.stream.listen(
          (changedDest) {
            if (changedDest == destinationId) {
              // Already inside a microtask delivered by the broadcast
              // controller; fire the snapshot fetch directly so the
              // re-emission lands one async tick sooner. Errors inside
              // the async body are forwarded to the per-call controller
              // via emitSnapshot's own try/catch.
              unawaited(emitSnapshot());
            }
          },
          onError: controller.addError,
          onDone: controller.close,
        );
      }
      ..onCancel = () async {
        // Broadcast `onCancel` fires when the LAST subscriber cancels;
        // tear down the upstream change subscription so the broadcast
        // controller does not leak after all subscribers detach.
        await changesSub?.cancel();
        changesSub = null;
        started = false;
      };
    return controller.stream;
  }

  // Snapshot-on-subscribe + live-re-emission stream for a named materialized
  // view. Mirrors watchFifo's shape: snapshot on subscribe + re-emit on every
  // mutation (upsert / delete / clear); cross-view isolation enforced by the
  // viewName filter; broadcast so multiple subscribers per view share a single
  // upstream subscription; close-aware via _viewChangesController's onDone
  // propagation. Not on the StorageBackend abstract surface — SembastBackend-specific.
  Stream<List<Map<String, Object?>>> watchView(String viewName) {
    if (_viewChangesController.isClosed) {
      throw StateError(
        'SembastBackend.close has been called; watchView unavailable',
      );
    }
    final controller = StreamController<List<Map<String, Object?>>>.broadcast();
    StreamSubscription<String>? changesSub;
    var started = false;

    Future<void> emitSnapshot() async {
      try {
        final snap = await findViewRows(viewName);
        if (!controller.isClosed) {
          controller.add(snap.cast<Map<String, Object?>>());
        }
      } catch (err, st) {
        if (!controller.isClosed) controller.addError(err, st);
      }
    }

    controller
      ..onListen = () {
        if (started) return;
        started = true;
        scheduleMicrotask(emitSnapshot);
        changesSub = _viewChangesController.stream.listen(
          (changedView) {
            if (changedView == viewName) {
              unawaited(emitSnapshot());
            }
          },
          onError: controller.addError,
          onDone: controller.close,
        );
      }
      ..onCancel = () async {
        await changesSub?.cancel();
        changesSub = null;
        started = false;
      };
    return controller.stream;
  }

  /// Append [attempt] to the entry's attempts[] inside [txn]. Does not
  /// change finalStatus. Throws [StateError] when the entry is absent (in
  /// Sembast a never-written store has no records, so this also covers an
  /// unknown destination) or terminal.
  @override
  @internal
  Future<void> appendAttemptTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) async {
    final t = _requireValidTxn(txn);
    final store = _fifoStore(destinationId);
    final records = await store.find(
      t._sembastTxn,
      finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
    );
    if (records.isEmpty) {
      throw StateError(
        'appendAttemptTxn($destinationId, $entryId): no such queue item; '
        'the drainer records an attempt only on the pending head it sent.',
      );
    }
    final record = records.single;
    final updated = Map<String, Object?>.from(record.value);
    final currentRaw = updated['final_status'];
    if (currentRaw != null) {
      throw StateError(
        'appendAttemptTxn($destinationId, $entryId): the item is '
        '$currentRaw; an attempt is recorded only on a pending item.',
      );
    }
    updated['attempts'] = <Map<String, Object?>>[
      ...(updated['attempts'] as List? ?? const <Object?>[])
          .cast<Map<String, Object?>>()
          .map(Map<String, Object?>.from),
      attempt.toJson(),
    ];
    await store.record(record.key).put(t._sembastTxn, updated);
    t._fifoChanged.add(destinationId);
  }

  @override
  Future<bool> hasFifoWedged() async {
    for (final dest in await _knownFifoDestinations()) {
      if (await _wedgedHead(dest) != null) return true;
    }
    return false;
  }

  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() async {
    final result = <WedgedFifoSummary>[];
    for (final dest in await _knownFifoDestinations()) {
      final head = await _wedgedHead(dest);
      if (head == null) continue;
      final hasAttempts = head.attempts.isNotEmpty;
      result.add(
        WedgedFifoSummary(
          destinationId: dest,
          headEntryId: head.entryId,
          // For batch rows, the summary reports the first event_id as a
          // stable single-string identifier for operators. Multi-event
          // batches' full id list is accessible via readFifoHead.
          headEventId: head.eventIds.first,
          wedgedAt: hasAttempts
              ? head.attempts.last.attemptedAt
              : head.enqueuedAt,
          lastError: hasAttempts
              ? (head.attempts.last.errorMessage ?? '<no error message>')
              : '<wedged with no attempts recorded>',
        ),
      );
    }
    return result;
  }

  /// Read a single FIFO row identified by [entryId] on [destinationId],
  /// or `null` when no such row exists. Non-transactional.
  ///
  /// In Sembast a never-written FIFO store simply has zero records,
  /// so the unknown-destination case and the unknown-row case both
  /// fall through to the `records.isEmpty` branch without needing a
  /// separate store-exists probe.
  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) async {
    final db = _database();
    final records = await _fifoStore(destinationId).find(
      db,
      finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
    );
    if (records.isEmpty) return null;
    return FifoEntry.fromJson(Map<String, Object?>.from(records.single.value));
  }

  /// Transition the target row's `final_status` to [status] inside
  /// [txn]. The legal transitions are exactly `null -> sent`,
  /// `null -> wedged`, `wedged -> tombstoned` and, for a row carrying
  /// attempts, `null -> tombstoned`; every other pair, a repeated status and
  /// a missing row throw [StateError] with nothing written.
  ///
  /// Preserves `attempts[]` verbatim on every transition. `sent_at` is set
  /// on `null -> sent` and untouched on every other transition.
  // Implements: EVS-DEV-destination-drain/B
  // exactly null -> sent, null -> wedged,
  //   wedged -> tombstoned and, for a row carrying attempts, null ->
  //   tombstoned; every other pair, a repeat and a missing row throw
  //   StateError with nothing written.
  @override
  @internal
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus status,
  ) async {
    final t = _requireValidTxn(txn);
    final store = _fifoStore(destinationId);
    final records = await store.find(
      t._sembastTxn,
      finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
    );
    if (records.isEmpty) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId, $status): no such '
        'queue item.',
      );
    }
    final record = records.single;
    final updated = Map<String, Object?>.from(record.value);
    final currentRaw = updated['final_status'];
    final current = currentRaw == null
        ? null
        : FinalStatus.fromJson(currentRaw as String);
    final hasAttempts = (updated['attempts'] as List? ?? const []).isNotEmpty;
    if (!isLegalFinalStatusTransition(
      current,
      status,
      hasAttempts: hasAttempts,
    )) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId): illegal transition '
        '${current?.name} -> ${status.name}. Legal transitions: '
        'null -> sent, null -> wedged, wedged -> tombstoned, and '
        'null -> tombstoned for an item carrying attempts.',
      );
    }
    updated['final_status'] = status.toJson();
    if (status == FinalStatus.sent) {
      updated['sent_at'] = DateTime.now().toUtc().toIso8601String();
    }
    await store.record(record.key).put(t._sembastTxn, updated);
    t._fifoChanged.add(destinationId);
  }

  /// The record of [entryId] on [destinationId] inside [t], or null.
  Future<RecordSnapshot<int, Map<String, Object?>>?> _fifoRecordTxn(
    _SembastTxn t,
    String destinationId,
    String entryId,
  ) => _fifoStore(destinationId).findFirst(
    t._sembastTxn,
    finder: Finder(filter: Filter.equals('entry_id', entryId)),
  );

  // Implements: EVS-DEV-delivery-channel/J
  // marking a pending item sent records the
  //   generation, delivery number and delivery hash it was acknowledged
  //   under.
  @override
  @internal
  Future<void> markSentTxn(
    Transaction txn,
    String destinationId,
    String entryId, {
    required int generation,
    required int deliveryNumber,
    required String deliveryHash,
  }) async {
    final t = _requireValidTxn(txn);
    final record = await _fifoRecordTxn(t, destinationId, entryId);
    if (record == null) {
      throw StateError('markSentTxn($destinationId, $entryId): no such item.');
    }
    final updated = Map<String, Object?>.from(record.value);
    final current = updated['final_status'];
    if (current != null) {
      throw StateError(
        'markSentTxn($destinationId, $entryId): the item is $current; only '
        'a pending item is marked sent.',
      );
    }
    updated['final_status'] = FinalStatus.sent.toJson();
    updated['sent_at'] = DateTime.now().toUtc().toIso8601String();
    updated['delivery_generation'] = generation;
    updated['delivery_number'] = deliveryNumber;
    updated['delivery_hash'] = deliveryHash;
    await _fifoStore(
      destinationId,
    ).record(record.key).put(t._sembastTxn, updated);
    t._fifoChanged.add(destinationId);
  }

  @override
  @internal
  Future<void> deleteFifoEntryTxn(
    Transaction txn,
    String destinationId,
    String entryId,
  ) async {
    final t = _requireValidTxn(txn);
    final record = await _fifoRecordTxn(t, destinationId, entryId);
    if (record == null) {
      throw StateError(
        'deleteFifoEntryTxn($destinationId, $entryId): no such item.',
      );
    }
    final status = record.value['final_status'];
    final attempts = record.value['attempts'] as List? ?? const <Object?>[];
    if (status != null || attempts.isNotEmpty) {
      throw StateError(
        'deleteFifoEntryTxn($destinationId, $entryId): the item is '
        '${status ?? 'pending with ${attempts.length} attempts'}; only a '
        'pending item that carries no attempt is deleted.',
      );
    }
    await _fifoStore(destinationId).record(record.key).delete(t._sembastTxn);
    t._fifoChanged.add(destinationId);
  }

  @override
  @internal
  Future<FifoEntry?> readRetainedDeliveryTxn(
    Transaction txn,
    String destinationId, {
    required int generation,
    required int deliveryNumber,
  }) async {
    final t = _requireValidTxn(txn);
    final record = await _fifoStore(destinationId).findFirst(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and(<Filter>[
          Filter.equals('final_status', FinalStatus.sent.toJson()),
          Filter.equals('delivery_generation', generation),
          Filter.equals('delivery_number', deliveryNumber),
        ]),
        sortOrders: <SortOrder>[SortOrder('sequence_in_queue', false)],
      ),
    );
    return record == null
        ? null
        : FifoEntry.fromJson(Map<String, Object?>.from(record.value));
  }

  /// Delete every FIFO row on [destinationId] whose `sequence_in_queue`
  /// is strictly greater than [afterSequenceInQueue] AND whose
  /// `final_status IS null`. Returns a [TrailSweepResult]: the count of
  /// rows deleted and the lowest first event sequence they carried.
  ///
  /// Used by `tombstoneAndRefill` to sweep the trail behind a
  /// tombstoned target in one transaction. Rows whose
  /// `final_status` is terminal (any of {sent, wedged, tombstoned})
  /// are left untouched regardless of their `sequence_in_queue` —
  /// all non-null rows are retained for the database's lifetime.
  @override
  @internal
  Future<TrailSweepResult> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  ) async {
    final t = _requireValidTxn(txn);
    final store = _fifoStore(destinationId);
    final finder = Finder(
      filter: Filter.and([
        Filter.isNull('final_status'),
        Filter.greaterThan('sequence_in_queue', afterSequenceInQueue),
      ]),
    );
    final matching = await store.find(t._sembastTxn, finder: finder);
    int? minFirstSeq;
    for (final record in matching) {
      final range = Map<String, Object?>.from(
        record.value['event_id_range']! as Map,
      );
      final firstSeq = range['first_seq']! as int;
      if (minFirstSeq == null || firstSeq < minFirstSeq) {
        minFirstSeq = firstSeq;
      }
    }
    await store.records(matching.map((r) => r.key)).delete(t._sembastTxn);
    t._fifoChanged.add(destinationId);
    return TrailSweepResult(
      deletedCount: matching.length,
      minFirstSeq: minFirstSeq,
    );
  }

  // -------- Event lookup by event_id --------

  /// Read a single event by `event_id` within [txn]. Returns `null` when no
  /// event with that id is present. Used by ingest's idempotency check
  /// against the unified event store (origin and ingest appends share
  /// `_eventStore`).
  @override
  Future<StoredEvent?> findEventByIdInTxn(
    Transaction txn,
    String eventId,
  ) async {
    final t = _requireValidTxn(txn);
    final finder = Finder(filter: Filter.equals('event_id', eventId), limit: 1);
    final record = await _eventStore.findFirst(t._reads, finder: finder);
    if (record == null) return null;
    return StoredEvent.fromMap(
      Map<String, Object?>.from(record.value),
      record.key,
    );
  }

  // Indexed lookup by event_id over the unified event store; returns null when absent.
  @override
  Future<StoredEvent?> findEventById(String eventId) async {
    final db = _database();
    final finder = Finder(filter: Filter.equals('event_id', eventId), limit: 1);
    final record = await _eventStore.findFirst(db, finder: finder);
    if (record == null) return null;
    return StoredEvent.fromMap(
      Map<String, Object?>.from(record.value),
      record.key,
    );
  }

  // -------- Audit query --------

  // Cross-store audit query; SembastSecurityContextStore.queryAudit is a thin
  // delegator that forwards here.
  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) async {
    if (limit < 1 || limit > 1000) {
      throw ArgumentError.value(
        limit,
        'limit',
        'queryAudit limit must be in [1, 1000]',
      );
    }

    _AuditCursorPoint? decodedCursor;
    if (cursor != null) {
      try {
        decodedCursor = _AuditCursorPoint.decode(cursor);
      } on Object catch (e) {
        throw ArgumentError.value(cursor, 'cursor', 'corrupt cursor: $e');
      }
    }

    final db = _database();

    // 1. Filter security rows by ipAddress + date range.
    final securityFilters = <Filter>[];
    if (ipAddress != null) {
      securityFilters.add(Filter.equals('ip_address', ipAddress));
    }
    if (from != null) {
      securityFilters.add(recordedAtNotBefore(from));
    }
    if (to != null) {
      securityFilters.add(recordedAtNotAfter(to));
    }
    // NOTE: we re-sort the join result in memory (see `rows.sort(...)`
    // below) so the in-memory order is authoritative for pagination; the
    // Sembast sort matches it only for clarity / debuggability.
    final securityFinder = Finder(
      filter: securityFilters.isEmpty
          ? null
          : (securityFilters.length == 1
                ? securityFilters.single
                : Filter.and(securityFilters)),
      sortOrders: [
        SortOrder('recorded_at', false),
        SortOrder(Field.key, false),
      ],
    );
    final securityRecords = await _securityContextStore.find(
      db,
      finder: securityFinder,
    );
    final securityByEventId = <String, EventSecurityContext>{
      for (final r in securityRecords)
        r.key: EventSecurityContext.fromJson(
          Map<String, Object?>.from(r.value),
        ),
    };
    if (securityByEventId.isEmpty) {
      return const PagedAudit(rows: <AuditRow>[]);
    }

    // 2. Fetch matching events.
    final eventFilters = <Filter>[
      Filter.inList('event_id', securityByEventId.keys.toList()),
    ];
    if (flowToken != null) {
      eventFilters.add(Filter.equals('flow_token', flowToken));
    }
    final eventFinder = Finder(
      filter: eventFilters.length == 1
          ? eventFilters.single
          : Filter.and(eventFilters),
    );
    final eventRecords = await _eventStore.find(db, finder: eventFinder);
    var events = eventRecords
        .map((r) => StoredEvent.fromMap(r.value, r.key))
        .toList();
    if (initiator != null) {
      events = events.where((e) => e.initiator == initiator).toList();
    }

    // 3. Inner join + sort by recordedAt desc.
    final rows = <AuditRow>[];
    for (final event in events) {
      final ctx = securityByEventId[event.eventId];
      if (ctx == null) continue;
      rows.add(AuditRow(event: event, securityContext: ctx));
    }
    rows.sort((a, b) {
      final cmp = b.securityContext.recordedAt.compareTo(
        a.securityContext.recordedAt,
      );
      if (cmp != 0) return cmp;
      return b.event.eventId.compareTo(a.event.eventId);
    });

    // 4. Apply cursor (lower bound) if provided.
    final filtered = decodedCursor == null
        ? rows
        : rows.where((r) {
            final cmp = r.securityContext.recordedAt.compareTo(
              decodedCursor!.recordedAt,
            );
            if (cmp < 0) return true;
            if (cmp == 0) {
              return r.event.eventId.compareTo(decodedCursor.eventId) < 0;
            }
            return false;
          }).toList();

    // 5. Paginate.
    final page = filtered.take(limit).toList();
    final nextCursor = filtered.length > limit
        ? _AuditCursorPoint(
            recordedAt: page.last.securityContext.recordedAt,
            eventId: page.last.event.eventId,
          ).encode()
        : null;
    return PagedAudit(rows: page, nextCursor: nextCursor);
  }

  /// The first non-sent entry in the FIFO when it is `wedged`. Returns
  /// null when either the FIFO has no entries, all entries are `sent`, or
  /// the earliest non-sent entry is pre-terminal (null final_status) or
  /// tombstoned (not wedged).
  Future<FifoEntry?> _wedgedHead(String destinationId) async {
    final db = _database();
    final records = await _fifoStore(
      destinationId,
    ).find(db, finder: Finder(sortOrders: [SortOrder('sequence_in_queue')]));
    for (final record in records) {
      final entry = FifoEntry.fromJson(Map<String, Object?>.from(record.value));
      final status = entry.finalStatus;
      if (status == null) {
        // Earliest non-sent row is pre-terminal: FIFO not wedged at head.
        return null;
      }
      switch (status) {
        case FinalStatus.sent:
          continue;
        case FinalStatus.wedged:
          return entry;
        case FinalStatus.tombstoned:
          // Tombstoned rows live in the audit trail but do not by
          // themselves wedge the FIFO; skip past and keep looking.
          continue;
      }
    }
    return null;
  }
}

class _SembastTxn extends Transaction {
  _SembastTxn._(sembast.Transaction txn, this._owner)
    : _txn = txn,
      _reads = txn;

  /// A handle whose reads run through [database] outside any transaction,
  /// and which every write refuses.
  _SembastTxn._nonBlocking(Database database, this._owner)
    : _txn = null,
      _reads = database;

  /// The sembast transaction of this run, or null for a non-blocking read
  /// handle.
  final sembast.Transaction? _txn;

  /// What the reads a non-blocking read handle accepts run through: the
  /// sembast transaction, or the database outside any transaction.
  final DatabaseClient _reads;

  /// The sembast transaction every other read and every write runs in.
  /// Throws [StateError] for a non-blocking read handle.
  sembast.Transaction get _sembastTxn {
    final txn = _txn;
    if (txn == null) {
      throw StateError(
        'a SembastBackend non-blocking read handle reads the event log only '
        'and writes nothing',
      );
    }
    return txn;
  }

  /// The backend whose `transaction()` produced this handle.
  final SembastBackend _owner;

  /// Notifications to fire if this run of the transaction body commits.
  /// Write paths push `() => controller.add(...)` here after their in-txn
  /// writes succeed; [SembastBackend.transaction] fires the list of the run
  /// that committed.
  final List<void Function()> _postCommit = <void Function()>[];

  /// Destinations whose queue this run changed; each is notified once if
  /// the run commits.
  final Set<String> _fifoChanged = <String>{};
  bool _isValid = true;
  void _invalidate() {
    _isValid = false;
  }
}

/// Opaque pagination cursor for [SembastBackend.queryAudit]. Encodes the
/// `(recorded_at, event_id)` tuple from the previous page's tail row;
/// the next page is a strict lower bound under the same sort order so
/// concurrent inserts at the head do not skew page contents.
// Used by queryAudit and (transitively) the SecurityContextStore delegator.
class _AuditCursorPoint {
  const _AuditCursorPoint({required this.recordedAt, required this.eventId});

  factory _AuditCursorPoint.decode(String encoded) {
    final raw = utf8.decode(base64Url.decode(encoded));
    final parts = raw.split('|');
    if (parts.length != 2) throw const FormatException('bad cursor shape');
    return _AuditCursorPoint(
      recordedAt: DateTime.parse(parts[0]),
      eventId: parts[1],
    );
  }

  final DateTime recordedAt;
  final String eventId;

  String encode() {
    final raw = '${recordedAt.toUtc().toIso8601String()}|$eventId';
    return base64Url.encode(utf8.encode(raw));
  }
}

/// A transaction body lost its runs holding the write lock shared; the
/// transaction runs again holding it exclusively.
final class _RunsLostToOtherWriters implements Exception {
  const _RunsLostToOtherWriters();
}

/// The instant a stored security-context record's `recorded_at` names.
DateTime _recordedAtOf(RecordSnapshot<Object?, Object?> record) =>
    DateTime.parse(record['recorded_at']! as String);

/// A filter admitting security-context records recorded at or before
/// [bound]. The stored `recorded_at` is an ISO 8601 string whose fraction
/// has three or six digits, so text order is not time order within a
/// millisecond; the filter compares the parsed instants.
@internal
Filter recordedAtNotAfter(DateTime bound) =>
    Filter.custom((record) => !_recordedAtOf(record).isAfter(bound));

/// A filter admitting security-context records recorded at or after
/// [bound], comparing parsed instants as [recordedAtNotAfter] does.
@internal
Filter recordedAtNotBefore(DateTime bound) =>
    Filter.custom((record) => !_recordedAtOf(record).isBefore(bound));
