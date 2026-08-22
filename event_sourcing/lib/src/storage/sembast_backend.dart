import 'dart:async';
import 'dart:convert';
import 'dart:developer' as developer;

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:meta/meta.dart' show visibleForTesting;
import 'package:sembast/sembast.dart' hide Transaction;
import 'package:sembast/sembast.dart' as sembast show Transaction;
import 'package:uuid/uuid.dart';

part 'sembast_test_support.dart';

/// Module-private v4 UUID generator used by [SembastBackend.enqueueFifoTxn]
/// to mint each FIFO row's [FifoEntry.entryId]. Held at file scope (const)
/// so every backend instance shares one generator; `Uuid.v4()` is
/// side-effect-free beyond its internal random state, so a shared
/// instance is correct.
const _uuidGen = Uuid();

/// Package-private default log sink used when a [SembastBackend] instance
/// has not overridden [SembastBackend.debugLogSink]. Routes through
/// `dart:developer` at the warning level (`level: 900`).
void _defaultLogSink(String message) {
  developer.log(message, name: 'SembastBackend', level: 900);
}

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
  SembastBackend({required Database database}) : _db = database;

  final Database _db;

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

  /// Per-transaction post-commit callback queue. The [transaction]
  /// wrapper swaps in a fresh inner list around each body, then runs
  /// the queued callbacks if and only if the body commits successfully.
  /// Write paths ([appendEvent], FIFO mutators) push
  /// `() => _eventsController.add(event)` /
  /// `() => _fifoChangesController.add(destinationId)` onto this list
  /// after their in-txn writes succeed; the wrapper drains them on
  /// commit. The field is mutable so the wrapper can preserve outer
  /// state across nested calls (sembast does not nest, but the swap is
  /// the cleanest race-safe pattern).
  List<void Function()> _pendingPostCommit = <void Function()>[];

  /// Close the underlying sembast database AND the reactive broadcast
  /// controllers used by [watchEvents] / [watchFifo] / [watchView]. After
  /// close, further calls to those reactive methods SHALL throw
  /// `StateError`. Active subscribers receive `done`.
  ///
  /// Not safe to call concurrently with an in-flight [transaction]. The
  /// caller is responsible for awaiting outstanding work before closing.
  Future<void> close() async {
    await _eventsController.close();
    await _fifoChangesController.close();
    await _viewChangesController.close();
    await _db.close();
  }

  /// Visible-for-testing sink for the warning-level diagnostic emitted by
  /// [markFinal] and [appendAttempt] when they no-op on a missing target.
  /// Defaults to the package-private [_defaultLogSink],
  /// which writes through `dart:developer` at `level: 900` (warning).
  /// Tests install a `List<String>.add` closure to capture emitted lines
  /// without depending on a global logger. Setting this to `null`
  /// suppresses diagnostics entirely.
  void Function(String)? debugLogSink = _defaultLogSink;

  // -------- transaction --------

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) async {
    final db = _database();
    final outerPending = _pendingPostCommit;
    final innerPending = <void Function()>[];
    _pendingPostCommit = innerPending;
    try {
      final result = await db.transaction((sembastTxn) async {
        final txn = _SembastTxn._(sembastTxn);
        try {
          return await body(txn);
        } finally {
          txn._invalidate();
        }
      });
      // Commit succeeded — fire post-commit callbacks. Skip emissions
      // when the corresponding controller has been closed (close() is
      // not safe to race with in-flight transactions, but a fast-cycle
      // test may still observe the closed state here).
      for (final cb in innerPending) {
        cb();
      }
      return result;
    } finally {
      _pendingPostCommit = outerPending;
    }
  }

  _SembastTxn _requireValidTxn(Transaction txn) {
    if (txn is! _SembastTxn) {
      throw StateError('Transaction is not a SembastBackend Transaction');
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
  // ignore: library_private_types_in_public_api
  sembast.Transaction unwrapSembastTxn(Transaction txn) =>
      _requireValidTxn(txn)._sembastTxn;

  // -------- Events --------

  /// Persist [event] inside [txn] and return its [AppendResult]. Under the
  /// Phase-2 Prereq B reserve-and-increment contract, `event.sequenceNumber`
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
        'transaction? (Phase-2 Prereq B, Option 1: reserve-and-increment; '
        'appendEvent consumes a reservation, it does not create one.)',
      );
    }
    await _eventStore.add(t._sembastTxn, event.toMap());
    // post-commit so live subscribers learn of the new event in
    // sequence_number order.
    _pendingPostCommit.add(() {
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

  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) async {
    final t = _requireValidTxn(txn);
    final finder = Finder(
      filter: Filter.equals('aggregate_id', aggregateId),
      sortOrders: [SortOrder('sequence_number')],
    );
    final records = await _eventStore.find(t._sembastTxn, finder: finder);
    return records.map((r) => StoredEvent.fromMap(r.value, r.key)).toList();
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
  //   on the top-level `entry_type` / `client_timestamp` fields (ISO 8601 UTC
  //   strings sort lexicographically in chronological order so
  //   greaterThanOrEquals / lessThanOrEquals reproduce the intended bounds).
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
    if (clientTimestampStart != null) {
      filters.add(
        Filter.greaterThanOrEquals(
          'client_timestamp',
          clientTimestampStart.toUtc().toIso8601String(),
        ),
      );
    }
    if (clientTimestampEnd != null) {
      filters.add(
        Filter.lessThanOrEquals(
          'client_timestamp',
          clientTimestampEnd.toUtc().toIso8601String(),
        ),
      );
    }
    if (filters.isEmpty) return null;
    if (filters.length == 1) return filters.single;
    return Filter.and(filters);
  }

  /// Reserve-and-increment the sequence counter within [txn]. Phase-2
  /// Prereq B, Option 1: the counter is advanced as a side effect so that
  /// a second call in the same transaction returns `current + 2`. A paired
  /// [appendEvent] consumes the reservation without advancing again. If
  /// the transaction rolls back, the counter rollback falls out of
  /// Sembast's transactional semantics.
  @override
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

  @override
  Future<String?> readLatestEventHash(Transaction txn) async {
    final t = _requireValidTxn(txn);
    final records = await _eventStore.find(
      t._sembastTxn,
      finder: Finder(
        sortOrders: [SortOrder('sequence_number', false)],
        limit: 1,
      ),
    );
    if (records.isEmpty) return null;
    return records.first.value['event_hash'] as String?;
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
      t._sembastTxn,
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

  // Replay-then-live event stream, broadcast and close-aware.
  //
  // The per-call controller is itself broadcast so a single `watchEvents()`
  // return value supports multiple `listen()` subscribers. On the first listen:
  //   1. `scheduleMicrotask(startReplay)` defers the replay so the caller's
  //      `listen()` returns before any emission, ensuring no replayed event
  //      is missed.
  //   2. Replay reads `findAllEvents(afterSequence: lowerBound)` and
  //      forwards each event, advancing `lastReplayed`.
  //   3. After replay completes, attach to `_eventsController` and filter
  //      `e.sequenceNumber > lastReplayed` to close the race where an event
  //      commits between the replay snapshot read and the live attach.
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
    var lastReplayed = lowerBound;
    StreamSubscription<StoredEvent>? liveSub;
    var started = false;

    Future<void> startReplay() async {
      try {
        final replay = await findAllEvents(afterSequence: lowerBound);
        for (final e in replay) {
          if (controller.isClosed) return;
          controller.add(e);
          lastReplayed = e.sequenceNumber;
        }
      } catch (err, st) {
        if (!controller.isClosed) controller.addError(err, st);
      }
      if (controller.isClosed) return;
      liveSub = _eventsController.stream.listen(
        (e) {
          if (e.sequenceNumber > lastReplayed) controller.add(e);
        },
        onError: controller.addError,
        onDone: controller.close,
      );
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
        scheduleMicrotask(startReplay);
      }
      ..onCancel = () async {
        // Broadcast `onCancel` fires when the LAST subscriber cancels;
        // tear down the upstream live subscription so the broadcast
        // controller does not leak after all subscribers detach. The
        // controller stays open so a later listener can re-attach
        // (broadcast semantics).
        await liveSub?.cancel();
        liveSub = null;
        started = false;
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

  /// Write the per-destination fill cursor inside its own atomic
  /// transaction.
  @override
  Future<void> writeFillCursor(String destinationId, int sequenceNumber) async {
    _validateFillCursorValue(sequenceNumber);
    await _database().transaction((sembastTxn) async {
      await _backendStateStore
          .record(_fillCursorKey(destinationId))
          .put(sembastTxn, sequenceNumber);
    });
  }

  /// Write the per-destination fill cursor inside [txn] so the advance is
  /// co-atomic with the surrounding transaction. Rolls back with the rest
  /// of the transaction body on a throw.
  @override
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
  /// cursor and confuse downstream fillBatch / unjam logic.
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

  /// Persist [schedule] for [destinationId] inside its own atomic
  /// transaction (standalone variant).
  @override
  Future<void> writeSchedule(
    String destinationId,
    DestinationSchedule schedule,
  ) async {
    await _database().transaction((sembastTxn) async {
      await _backendStateStore
          .record(_scheduleKey(destinationId))
          .put(sembastTxn, schedule.toJson());
    });
  }

  /// Persist [schedule] inside [txn] so the write participates in the
  /// surrounding transaction's atomicity.
  @override
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
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) async {
    final t = _requireValidTxn(txn);
    await _backendStateStore
        .record(_scheduleKey(destinationId))
        .delete(t._sembastTxn);
  }

  /// Drop the entire `fifo_<destinationId>` Sembast store inside [txn]
  /// and remove [destinationId] from the known-FIFOs registry so
  /// `hasFifoWedged` / `wedgedFifos` no longer iterate it.
  @override
  Future<void> deleteFifoStoreTxn(Transaction txn, String destinationId) async {
    final t = _requireValidTxn(txn);
    await _fifoStore(destinationId).drop(t._sembastTxn);
    // Also drop the fill-cursor record so a later addDestination of the
    // same id starts from a clean slate rather than inheriting a stale
    // cursor.
    await _backendStateStore
        .record(_fillCursorKey(destinationId))
        .delete(t._sembastTxn);
    // Drop the per-destination sequence_in_queue counter so a later
    // addDestination of the same id starts at 1 rather than inheriting
    // the old counter. The "never reused" invariant is scoped to a
    // destination's lifetime; a fresh addDestination begins a new lifetime.
    await _backendStateStore
        .record(_fifoSeqCounterKey(destinationId))
        .delete(t._sembastTxn);
    // Remove the id from the known-FIFOs registry so wedged-FIFO
    // iteration does not hit a dropped store.
    final current =
        (await _backendStateStore.record(_knownFifosKey).get(t._sembastTxn)
                as List?)
            ?.cast<String>()
            .toList() ??
        <String>[];
    if (current.remove(destinationId)) {
      await _backendStateStore
          .record(_knownFifosKey)
          .put(t._sembastTxn, current);
    }
    // post-commit so live `watchFifo(destinationId)` subscribers see
    // the FIFO-store drop (subsequent listFifoEntries will be empty).
    _pendingPostCommit.add(() {
      if (!_fifoChangesController.isClosed) {
        _fifoChangesController.add(destinationId);
      }
    });
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
    _pendingPostCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
  }

  @override
  Future<void> deleteViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final t = _requireValidTxn(txn);
    await _viewStore(viewName).record(key).delete(t._sembastTxn);
    _pendingPostCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
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
  Future<void> clearViewInTxn(Transaction txn, String viewName) async {
    final t = _requireValidTxn(txn);
    await _viewStore(viewName).delete(t._sembastTxn);
    _pendingPostCommit.add(() {
      if (!_viewChangesController.isClosed) {
        _viewChangesController.add(viewName);
      }
    });
  }

  // -------- View target versions --------
  //
  // Persists the per-(viewName, entryType) target schema version that the
  // promoter pipeline reads on every materialization. One sembast store
  // (`view_target_versions`) keyed on `'<viewName>::<entryType>'`; rows
  // carry `view_name` / `entry_type` / `target_version` so `find` /
  // `delete` can scope by `view_name`.

  static const _viewTargetVersionsStore = 'view_target_versions';

  final StoreRef<String, Map<String, Object?>> _viewTargetVersionsStoreRef =
      stringMapStoreFactory.store(_viewTargetVersionsStore);

  String _viewTargetVersionsKey(String viewName, String entryType) =>
      '$viewName::$entryType';

  @override
  Future<int?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) async {
    final t = _requireValidTxn(txn);
    final raw = await _viewTargetVersionsStoreRef
        .record(_viewTargetVersionsKey(viewName, entryType))
        .get(t._sembastTxn);
    if (raw == null) return null;
    final v = raw['target_version'];
    if (v is! int) {
      throw StateError(
        'view_target_versions[$viewName::$entryType]: target_version not int '
        '(got ${v.runtimeType}); database corrupted',
      );
    }
    return v;
  }

  @override
  Future<void> writeViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
    int targetVersion,
  ) async {
    final t = _requireValidTxn(txn);
    await _viewTargetVersionsStoreRef
        .record(_viewTargetVersionsKey(viewName, entryType))
        .put(t._sembastTxn, <String, Object?>{
          'view_name': viewName,
          'entry_type': entryType,
          'target_version': targetVersion,
        });
  }

  @override
  Future<Map<String, int>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) async {
    final t = _requireValidTxn(txn);
    final records = await _viewTargetVersionsStoreRef.find(
      t._sembastTxn,
      finder: Finder(filter: Filter.equals('view_name', viewName)),
    );
    return <String, int>{
      for (final r in records)
        (r.value['entry_type'] as String): (r.value['target_version'] as int),
    };
  }

  @override
  Future<void> clearViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) async {
    final t = _requireValidTxn(txn);
    await _viewTargetVersionsStoreRef.delete(
      t._sembastTxn,
      finder: Finder(filter: Filter.equals('view_name', viewName)),
    );
  }

  // -------- FIFO --------

  /// Append a batch-shaped row to destination [destinationId]'s FIFO. The
  /// row covers every event in [batch].
  ///
  /// Opens its own atomic transaction and delegates the actual row
  /// construction to [enqueueFifoTxn]. Callers already composing a larger
  /// transaction (replay, fill_batch) SHALL use [enqueueFifoTxn] so the
  /// enqueue and any accompanying writes (e.g., fill_cursor advance)
  /// commit co-atomically.
  ///
  /// Exactly one of [wirePayload] / [nativeEnvelope] SHALL be non-null
  ///. See [StorageBackend.enqueueFifo] for the contract
  /// distinguishing the two payload shapes.
  ///
  /// The backend owns `sequence_in_queue` via the persisted
  /// `fifo_seq_counter_<destinationId>` record:
  /// monotonic, never reused.
  ///
  /// The returned `FifoEntry` is the persisted record. Callers that
  /// need to advance a per-destination cursor use
  /// `result.sequenceRange.lastSeq` as the inclusive upper bound of the
  /// batch on the event log.
  ///
  /// The row's `entry_id` is a freshly-minted v4 UUID and has no
  /// relationship to the events the row carries — callers that need
  /// to correlate against events use `eventIds` / `sequenceRange`.
  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) async {
    // Route through this backend's `transaction()` (rather than the
    // raw `_database().transaction(...)`) so the post-commit callback
    // appended inside `enqueueFifoTxn` is drained by the wrapper on
    // commit; otherwise FIFO-change emissions on the
    // standalone enqueue path would silently drop.
    return transaction(
      (txn) => enqueueFifoTxn(
        txn,
        destinationId,
        batch,
        wirePayload: wirePayload,
        nativeEnvelope: nativeEnvelope,
      ),
    );
  }

  /// Transactional variant of [enqueueFifo]: participates in the
  /// surrounding [txn] so the FIFO-row write and the caller's
  /// accompanying writes commit or roll back together. Used by
  /// `fillBatch` to keep the enqueue + fill_cursor advance co-atomic,
  /// and by `runHistoricalReplay` to compose a larger walk of the event
  /// log into a single transaction.
  ///
  /// Exactly one of [wirePayload] / [nativeEnvelope] SHALL be non-null:
  ///
  /// - [wirePayload] (3rd-party): persists `wire_payload = decoded JSON
  ///   map`, `wire_format = wirePayload.contentType`,
  ///   `transform_version = wirePayload.transformVersion`,
  ///   `envelope_metadata = null`.
  /// - [nativeEnvelope] (native `esd/batch@1`): persists
  ///   `wire_payload = null`, `wire_format = "esd/batch@1"`,
  ///   `transform_version = null`, `envelope_metadata = nativeEnvelope`.
  ///
  /// Centralizes all row-construction logic: empty-batch rejection,
  /// XOR-shape enforcement, v4-UUID `entry_id` minting,
  /// `sequence_in_queue` assignment, and the known-FIFOs registry
  /// bookkeeping all live here; [enqueueFifo] is a thin
  /// `transaction(...)` wrapper.
  @override
  Future<FifoEntry> enqueueFifoTxn(
    Transaction txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) async {
    if (batch.isEmpty) {
      throw ArgumentError.value(
        batch,
        'batch',
        'enqueueFifo requires a non-empty batch',
      );
    }
    // XOR enforcement: exactly one payload shape is legal. Reject both
    // null and both non-null at the boundary so a downstream FIFO row
    // never carries an ambiguous (wire_payload, envelope_metadata) pair.
    if ((wirePayload == null) == (nativeEnvelope == null)) {
      throw ArgumentError(
        'enqueueFifo requires exactly one of wirePayload or nativeEnvelope '
        'to be non-null; got '
        'wirePayload=${wirePayload == null ? "null" : "set"}, '
        'nativeEnvelope=${nativeEnvelope == null ? "null" : "set"}',
      );
    }
    final t = _requireValidTxn(txn);
    final eventIds = batch.map((e) => e.eventId).toList(growable: false);
    final sequenceRange = (
      firstSeq: batch.first.sequenceNumber,
      lastSeq: batch.last.sequenceNumber,
    );
    // Resolve the row's wire-format / payload columns from the chosen
    // payload shape. Native rows carry envelope_metadata; 3rd-party rows
    // decode the bytes once and persist the resulting JSON map.
    Map<String, Object?>? payloadMap;
    String wireFormat;
    String? transformVersion;
    if (nativeEnvelope != null) {
      payloadMap = null;
      wireFormat = BatchEnvelope.wireFormat;
      transformVersion = null;
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
            'enqueueFifo requires wirePayload.bytes to encode a JSON object '
                '(Map); got ${decoded.runtimeType}',
          );
        }
        payloadMap = Map<String, Object?>.from(decoded);
      } on FormatException catch (e) {
        throw ArgumentError.value(
          wp,
          'wirePayload',
          'enqueueFifo requires wirePayload.bytes to be UTF-8 JSON: '
              '${e.message}',
        );
      }
      wireFormat = wp.contentType;
      transformVersion = wp.transformVersion;
    }
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
      wireFormat: wireFormat,
      transformVersion: transformVersion,
      enqueuedAt: enqueuedAt,
      attempts: const <AttemptResult>[],
      finalStatus: null,
      sentAt: null,
      envelopeMetadata: nativeEnvelope,
    );
    await store.record(assigned).put(t._sembastTxn, entry.toJson());
    await _registerFifoDestinationSembast(t._sembastTxn, destinationId);
    // post-commit so live `watchFifo(destinationId)` subscribers learn
    // of the new row. Pushed onto _pendingPostCommit so the emission is
    // co-atomic with the surrounding `transaction()` commit; fires only
    // if the transaction succeeds. `enqueueFifo` (the standalone
    // wrapper) routes through `transaction()` for the same reason.
    _pendingPostCommit.add(() {
      if (!_fifoChangesController.isClosed) {
        _fifoChangesController.add(destinationId);
      }
    });
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
  Future<FifoEntry?> readFifoHead(String destinationId) async {
    final db = _database();
    final store = _fifoStore(destinationId);
    final records = await store.find(
      db,
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

  /// Append [attempt] to the entry's attempts[]. Does not change
  /// finalStatus. Runs in its own transaction.
  ///
  /// Tolerates a missing target row or a never-registered FIFO store:
  /// in both cases this method returns without throwing and emits a
  /// warning-level diagnostic via [debugLogSink]. This closes
  /// the drain/unjam + drain/delete race documented in design §6.6 —
  /// drain `await send()`s outside any storage transaction, and a
  /// concurrent user operation (unjamDestination, deleteDestination) may
  /// remove the row before drain's subsequent `appendAttempt` runs.
  ///
  /// In Sembast, stores are lazily-created namespaces: a store that was
  /// never written to simply has zero records, so the `records.isEmpty`
  /// branch covers both "unknown destination" and "row deleted from a
  /// known destination". No separate "store exists?" probe is needed.
  @override
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) async {
    // Route through this backend's `transaction()` so post-commit FIFO
    // emissions appended below are drained on commit.
    await transaction((txn) async {
      final t = _requireValidTxn(txn);
      final store = _fifoStore(destinationId);
      final records = await store.find(
        t._sembastTxn,
        finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
      );
      if (records.isEmpty) {
        debugLogSink?.call(
          'appendAttempt: entry $entryId absent from FIFO '
          '$destinationId; skipping (expected during drain/unjam or '
          'drain/delete race)',
        );
        // No row mutated -> no FIFO-change emission.
        return;
      }
      final record = records.single;
      final updated = Map<String, Object?>.from(record.value);
      final attemptsRaw = <Map<String, Object?>>[
        ...(updated['attempts'] as List? ?? const <Object?>[])
            .cast<Map<String, Object?>>()
            .map(Map<String, Object?>.from),
        attempt.toJson(),
      ];
      updated['attempts'] = attemptsRaw;
      await store.record(record.key).put(t._sembastTxn, updated);
      // post-commit so live `watchFifo(destinationId)` subscribers see
      // the appended attempt.
      _pendingPostCommit.add(() {
        if (!_fifoChangesController.isClosed) {
          _fifoChangesController.add(destinationId);
        }
      });
    });
  }

  /// Transition entry's finalStatus to [status]. For `sent`, also stamps
  /// `sent_at = DateTime.now().toUtc()`. The entry is RETAINED: no delete
  /// ever happens through this path.
  ///
  /// Tolerates a missing target row or a never-registered FIFO store:
  /// in both cases this method returns without throwing and emits a
  /// warning-level diagnostic via [debugLogSink]. This closes
  /// the drain/unjam + drain/delete race documented in design §6.6 —
  /// drain `await send()`s outside any storage transaction, and a
  /// concurrent user operation (unjamDestination, deleteDestination) may
  /// remove the row before drain's subsequent `markFinal` runs.
  ///
  /// In Sembast, stores are lazily-created namespaces: a store that was
  /// never written to simply has zero records, so the `records.isEmpty`
  /// branch covers both "unknown destination" and "row deleted from a
  /// known destination". No separate "store exists?" probe is needed.
  ///
  /// The one-way transition rule is preserved with idempotency: when the
  /// entry is already terminal with the SAME status as [status], the call
  /// returns cleanly (no-op, no re-stamp of `sent_at`). When the entry is
  /// already terminal with a DIFFERENT status, `StateError` is thrown —
  /// this is real corruption and loud failure is correct.
  @override
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  ) async {
    // markFinal transitions a pre-terminal row (final_status == null)
    // into one of the three non-null terminal states. `null` is not a
    // legal target — it is the INITIAL state and is set only by
    // enqueueFifo. The non-null target is enforced by the parameter
    // type `FinalStatus` (non-nullable); the type system makes a
    // runtime null-check unnecessary here.
    //
    // Routed through this backend's `transaction()` so post-commit
    // FIFO emissions appended below are drained on commit.
    await transaction((txn) async {
      final t = _requireValidTxn(txn);
      final store = _fifoStore(destinationId);
      final records = await store.find(
        t._sembastTxn,
        finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
      );
      if (records.isEmpty) {
        debugLogSink?.call(
          'markFinal: entry $entryId absent from FIFO $destinationId; '
          'skipping (expected during drain/unjam or drain/delete race)',
        );
        // No row mutated -> no FIFO-change emission.
        return;
      }
      final record = records.single;
      final updated = Map<String, Object?>.from(record.value);
      final currentRaw = updated['final_status'];
      final currentStatus = currentRaw == null
          ? null
          : FinalStatus.fromJson(currentRaw as String);
      // final_status is one-way: null -> sent|wedged|tombstoned.
      // A duplicate call with the SAME status is a no-op — drain() is
      // documented at-least-once and concurrent drainers can both reach
      // markFinal after the first completes. Matching status: return
      // cleanly. Mismatched status: real corruption; loud failure.
      if (currentStatus != null) {
        if (currentStatus == status) {
          // Idempotent duplicate — first call already wrote the correct
          // terminal state. No additional write or sent_at re-stamp needed.
          return;
        }
        throw StateError(
          'markFinal($destinationId, $entryId, $status): entry is already '
          '$currentStatus; final_status transitions are one-way.',
        );
      }
      updated['final_status'] = status.toJson();
      if (status == FinalStatus.sent) {
        updated['sent_at'] = DateTime.now().toUtc().toIso8601String();
      }
      await store.record(record.key).put(t._sembastTxn, updated);
      // post-commit so live `watchFifo(destinationId)` subscribers see
      // the terminal-status transition.
      _pendingPostCommit.add(() {
        if (!_fifoChangesController.isClosed) {
          _fifoChangesController.add(destinationId);
        }
      });
    });
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
  /// [txn]. The legal transitions, enforced by a guard below, are:
  ///
  /// - `null -> sent` — drain-terminal SendOk; stamps
  ///   `sent_at = DateTime.now().toUtc()`.
  /// - `null -> wedged` — drain-terminal SendPermanent / SendTransient
  ///   at max attempts.
  /// - `null -> tombstoned` — tombstoneAndRefill on a null head
  ///  .
  /// - `wedged -> tombstoned` — tombstoneAndRefill on a wedged head
  ///  .
  ///
  /// Any other transition throws [StateError]. In particular `sent`
  /// and `tombstoned` are terminal end-states; they cannot transition
  /// to anything else.
  ///
  /// Preserves `attempts[]` verbatim on every transition (
  /// tombstoneAndRefill requires it). `sent_at` is set on `null -> sent`
  /// and untouched on every other transition.
  ///
  /// Throws [StateError] on a missing target row: callers verify
  /// existence before opening the transaction, so a missing row here
  /// indicates a concurrent delete race that these ops do not close.
  @override
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
  ) async {
    final t = _requireValidTxn(txn);
    final store = _fifoStore(destinationId);
    final records = await store.find(
      t._sembastTxn,
      finder: Finder(filter: Filter.equals('entry_id', entryId), limit: 1),
    );
    if (records.isEmpty) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId, $status): target '
        'row not found. Callers must verify existence (readFifoHead) '
        'before opening the transaction; a missing row here indicates '
        'a concurrent delete race.',
      );
    }
    final record = records.single;
    final updated = Map<String, Object?>.from(record.value);
    final currentRaw = updated['final_status'];
    final current = currentRaw == null
        ? null
        : FinalStatus.fromJson(currentRaw as String);
    // Legal transitions:
    //  - null  -> sent          (drain SendOk)
    //  - null  -> wedged        (drain SendPermanent / max-attempts)
    //  - null  -> tombstoned    (tombstoneAndRefill on null head)
    //  - wedged -> tombstoned   (tombstoneAndRefill on wedged head)
    final valid =
        (current == null &&
            (status == FinalStatus.sent ||
                status == FinalStatus.wedged ||
                status == FinalStatus.tombstoned)) ||
        (current == FinalStatus.wedged && status == FinalStatus.tombstoned);
    if (!valid) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId): illegal transition '
        '$current -> $status. Legal transitions: null -> {sent, wedged, '
        'tombstoned}; wedged -> {tombstoned}. (one-way '
        'rule.)',
      );
    }
    updated['final_status'] = status?.toJson();
    if (status == FinalStatus.sent) {
      // Drain-terminal SendOk stamps sent_at.
      updated['sent_at'] = DateTime.now().toUtc().toIso8601String();
    }
    // attempts[] is deliberately NOT touched
    // tombstoneAndRefill requires verbatim preservation; the
    // drain-terminal null->{sent,wedged} path has already appended its
    // attempts via appendAttempt before calling markFinal /
    // setFinalStatusTxn.
    await store.record(record.key).put(t._sembastTxn, updated);
    // post-commit so live `watchFifo(destinationId)` subscribers see
    // the final-status transition (tombstone / drain-terminal). The
    // surrounding `transaction()` drains _pendingPostCommit on commit.
    _pendingPostCommit.add(() {
      if (!_fifoChangesController.isClosed) {
        _fifoChangesController.add(destinationId);
      }
    });
  }

  /// Delete every FIFO row on [destinationId] whose `sequence_in_queue`
  /// is strictly greater than [afterSequenceInQueue] AND whose
  /// `final_status IS null`. Returns the count of rows deleted.
  ///
  /// Used by `tombstoneAndRefill` to sweep the trail behind a
  /// tombstoned target in one transaction. Rows whose
  /// `final_status` is terminal (any of {sent, wedged, tombstoned})
  /// are left untouched regardless of their `sequence_in_queue` —
  /// all non-null rows are retained forever.
  @override
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  ) async {
    final t = _requireValidTxn(txn);
    final store = _fifoStore(destinationId);
    final deleted = await store.delete(
      t._sembastTxn,
      finder: Finder(
        filter: Filter.and([
          Filter.isNull('final_status'),
          Filter.greaterThan('sequence_in_queue', afterSequenceInQueue),
        ]),
      ),
    );
    if (deleted > 0) {
      // post-commit so live `watchFifo(destinationId)` subscribers see
      // the trail-sweep deletion. Skip when no rows were actually
      // removed to avoid spurious wakeups.
      _pendingPostCommit.add(() {
        if (!_fifoChangesController.isClosed) {
          _fifoChangesController.add(destinationId);
        }
      });
    }
    return deleted;
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
    final record = await _eventStore.findFirst(t._sembastTxn, finder: finder);
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
      securityFilters.add(
        Filter.greaterThanOrEquals(
          'recorded_at',
          from.toUtc().toIso8601String(),
        ),
      );
    }
    if (to != null) {
      securityFilters.add(
        Filter.lessThanOrEquals('recorded_at', to.toUtc().toIso8601String()),
      );
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
  _SembastTxn._(this._sembastTxn);
  final sembast.Transaction _sembastTxn;
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
