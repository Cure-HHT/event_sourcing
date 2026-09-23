// Implements: EVS-PRD-portability/D
// second concrete StorageBackend impl
//   alongside SembastBackend; selectable per deployment with no caller
//   changes (the contract is Dart-pure).
// Implements: EVS-DEV-postgres-backend/A
// `PostgresBackend.open` connects
//   and emits `CREATE TABLE IF NOT EXISTS` DDL for every table the backend
//   uses; re-open against a provisioned database is a no-op on the schema.
// Implements: EVS-PRD-event-log/A+B+C+D
// event-log surface: append-only;
//   stable total order via sequence counter (reserve-and-increment);
//   per-aggregate order isolated by aggregate_id; read events in order
//   from any starting position; findEventById/-InTxn lookups.
// Implements: EVS-DEV-find-all-events-extended-filters/A+B+C+D — entryType,
//   clientTimestampStart, clientTimestampEnd filters AND-compose with
//   afterSequence/limit/originator filters; both in-txn and out-of-txn
//   variants share a single composition helper (_findAllEventsComposed).
// Implements: EVS-DEV-postgres-backend/B
// view rows persisted as JSONB
//   blobs in a single view_rows(view_name, row_key, row_data JSONB,
//   updated_at) table with primary key (view_name, row_key).
// Implements: EVS-PRD-destinations
// FIFO-queue surface: per-destination
//   monotone sequence_in_queue, head behavior, attempt log, final-status
//   transitions, wedged-FIFO summary, trail-sweep delete, full FIFO
//   delete on destination teardown.
// Implements: EVS-PRD-event-log/B
// sequence counter durability (via
//   backend_state schema_version), per-destination fill cursors,
//   destination schedules.
// Implements: EVS-PRD-regulatory-alignment
// queryAudit joins events and
//   security_context for ALCOA+-aligned audit access; the join lives in
//   the storage layer so callers cannot reach past the abstraction.

import 'dart:async';
import 'dart:convert';
import 'dart:math' show Random, min;

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/lifecycle/boot_errors.dart';
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/boot_check.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/storage/queue_records.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:postgres/postgres.dart';
import 'package:uuid/uuid.dart';

/// Module-private v4 UUID generator used by [PostgresBackend.enqueueFifoTxn]
/// to mint each FIFO row's [FifoEntry.entryId]. Held at file scope so every
/// backend instance shares one generator; `Uuid.v4()` is side-effect-free
/// beyond its internal random state. Parallels the sembast backend's
/// module-private `_uuidGen` to keep the two impls structurally aligned.
const _uuidGen = Uuid();

/// Thrown by every public [PostgresBackend] I/O method after
/// `PostgresBackend.close` has run. Implements [Exception] (not [Error])
/// so callers can match on `isA<Exception>()` in the conformance harness
/// — `StateError` from the underlying `package:pool` `Pool` would
/// otherwise leak through as an [Error], which the contract test
/// rejects.
///
/// The instance has no fields; the closed state is binary. Callers
/// inspecting the exception receive a stable `toString()` for logs.
// Implements: EVS-DEV-postgres-backend/D
// post-close I/O throws an
//   Exception subtype (not Error), matching the storage-backend conformance
//   harness' `throwsA(isA<Exception>())` expectation on the close subgroup.
class PostgresBackendClosedException implements Exception {
  const PostgresBackendClosedException();
  @override
  String toString() =>
      'PostgresBackendClosedException: backend has been closed; '
      're-open via PostgresBackend.open() to perform further I/O.';
}

/// Thrown by [PostgresBackend.transaction] when a transaction body keeps
/// failing with a transient serialization/deadlock conflict (SQLSTATE 40001
/// `serialization_failure` / 40P01 `deadlock_detected`) and the bounded
/// in-request retry is exhausted without the body ever committing.
///
/// This is **retryable**, not terminal. A serialization conflict is a
/// probabilistic outcome of write contention — losing the race
/// [attempts] times in a row signals *sustained* contention, not that the work
/// is impossible. A later attempt can still commit. The library bounds its own
/// tight fast-path retry and hands control back precisely so the caller can
/// apply a recovery a tight loop must not: a longer/jittered backoff,
/// load-shedding/backpressure, queue-and-re-drive, or a "system busy, retry"
/// response. Accordingly `classifyStorageException` maps this to a
/// `StorageTransientException`.
///
/// It is surfaced as its own type (rather than a bare driver exception) so that
/// callers can *also* recognize the "fast-path exhausted under sustained
/// contention" condition specifically — e.g. to emit an ops metric/alert — and
/// so they need not sniff the driver's SQLSTATE. The library stays neutral
/// about what happens next: alerting/Slack/metrics are the caller's concern.
///
/// The data is intact: the final attempt rolled back, so nothing partial was
/// written. [lastError] preserves the final [ServerException] (carrying its
/// SQLSTATE on `code`) for forensics.
// Implements: EVS-PRD-event-log/E
// once the bounded retry is exhausted the
//   transient conflict is surfaced as a distinguishable, typed (and
//   transient-classified) failure rather than a bare driver exception, so
//   callers can re-drive it without coupling to the storage driver's taxonomy.
class TransactionRetryExhaustedException implements Exception {
  const TransactionRetryExhaustedException({
    required this.attempts,
    required this.lastError,
  });

  /// Number of attempts made before giving up: the configured bound,
  /// [PostgresBackend._maxTransactionAttempts], for a transaction, and the
  /// runs made within `bootLockWait` for the boot transaction.
  final int attempts;

  /// The serialization/deadlock failure from the final attempt; its `code`
  /// holds the SQLSTATE (`40001` or `40P01`).
  final ServerException lastError;

  @override
  String toString() =>
      'TransactionRetryExhaustedException: transaction aborted by a transient '
      'serialization/deadlock conflict (SQLSTATE ${lastError.code}) on all '
      '$attempts attempts; data is intact (final attempt rolled back). '
      'Investigate write contention before re-driving the operation.';
}

/// Concrete Postgres-backed implementation of [StorageBackend].
class PostgresBackend extends StorageBackend {
  PostgresBackend._(this._pool, {required Duration bootLockWait})
    : _bootLockWait = bootLockWait;

  final Pool<void> _pool;

  /// How long [bootTransaction] keeps re-running a boot that a concurrent
  /// commit aborted before it gives up.
  final Duration _bootLockWait;

  /// Latches true on the first call to [close]. Subsequent I/O on this
  /// backend instance throws [PostgresBackendClosedException]; the flag
  /// also makes [close] itself idempotent (a second call is a no-op).
  // Implements: EVS-DEV-postgres-backend/D
  // closed-state guard for the
  //   conformance harness' close subgroup.
  bool _closed = false;

  /// Open against [url] using the supplied [sslMode]. Connects, emits
  /// the schema DDL (idempotent on re-open), and returns a ready
  /// backend. Callers MUST call [close] to release the connection
  /// pool.
  ///
  /// Example: `postgres://user:pass@host:5432/db`.
  ///
  /// The default `SslMode.require` matches Cloud SQL's default
  /// "Require SSL" posture, so a managed-Postgres deployment Just
  /// Works without code changes. Local development against an
  /// unencrypted Postgres (e.g., the docker-compose Postgres in
  /// `example_action_permissions/`) should pass `SslMode.disable`.
  /// Production deployments against a managed Postgres over the
  /// public internet should consider `SslMode.verifyFull` to validate
  /// the server certificate.
  // Implements: EVS-DEV-postgres-backend/A
  // connects and emits the schema
  //   DDL on every open; idempotent on re-open against a provisioned db.
  ///
  /// The schema statements run in their own transaction on every open.
  /// Each `CREATE INDEX IF NOT EXISTS` locks its table in `SHARE` mode
  /// even when the index exists, and the `idempotency` column statement
  /// locks that table exclusively, so an open waits for the appends in
  /// flight on another instance and holds new ones back until it commits;
  /// it can also end in a deadlock failure against another instance's
  /// transactions, which this method does not retry. They also run before
  /// `EventStore.open` refuses a database an earlier data format wrote, so
  /// opening such a database may create a table or index it lacks before
  /// the boot refuses it.
  ///
  /// [bootLockWait] bounds only how long `EventStore.open`'s boot
  /// transaction keeps re-running after a serialization or deadlock
  /// failure: a failure once it has passed since the first run started
  /// throws [TransactionRetryExhaustedException]. It bounds neither the
  /// boot's wait for its table lock nor the boot's run time (see
  /// [bootTransaction]).
  static Future<PostgresBackend> open({
    required String url,
    SslMode sslMode = SslMode.require,
    Duration bootLockWait = const Duration(seconds: 60),
  }) async {
    final endpoint = endpointFromUrl(url);
    final pool = Pool<void>.withEndpoints([
      endpoint,
    ], settings: PoolSettings(maxConnectionCount: 4, sslMode: sslMode));
    final backend = PostgresBackend._(pool, bootLockWait: bootLockWait);
    await pool.runTx(ensurePostgresSchema);
    return backend;
  }

  @visibleForTesting
  static Endpoint endpointFromUrl(String url) {
    final uri = Uri.parse(url);
    final userInfoParts = uri.userInfo.isEmpty
        ? const <String>[]
        : uri.userInfo.split(':');
    return Endpoint(
      host: uri.host,
      port: uri.port == 0 ? 5432 : uri.port,
      database: uri.pathSegments.isEmpty ? '' : uri.pathSegments.first,
      username: userInfoParts.isEmpty ? null : userInfoParts.first,
      password: userInfoParts.length < 2
          ? null
          : userInfoParts.sublist(1).join(':'),
    );
  }

  /// The underlying connection pool, shared with the library's own
  /// Postgres-side stores (the idempotency store). Internal: a consumer
  /// holding it could write the library's tables directly.
  @internal
  Pool<void> get pool => _pool;

  /// Close the underlying connection pool. Idempotent: a second call is
  /// a no-op. After close, every public I/O method on this instance
  /// throws [PostgresBackendClosedException].
  // Implements: EVS-DEV-postgres-backend/D
  // close is idempotent and
  //   subsequent I/O surfaces a typed Exception (not the underlying
  //   `package:pool` StateError, which `isA<Exception>()` would reject).
  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    await _pool.close();
  }

  /// Throws [PostgresBackendClosedException] when [close] has already
  /// run. Called at the top of every public I/O method so the caller
  /// receives a typed [Exception] rather than the `package:pool`
  /// `StateError` that would otherwise leak through. Cheap (single
  /// field read); inlined trivially.
  // Implements: EVS-DEV-postgres-backend/D
  // closed-state guard.
  void _checkOpen() {
    if (_closed) throw const PostgresBackendClosedException();
  }

  // ------------------------------------------------------------------
  // StorageBackend method implementations follow. The closed-state
  // guard (`_checkOpen()`) runs at the top of every public I/O method
  // so a post-close call throws PostgresBackendClosedException rather
  // than the underlying `package:pool` StateError (which is an Error,
  // not an Exception, and would not satisfy the conformance harness'
  // `throwsA(isA<Exception>())` expectation).
  // ------------------------------------------------------------------

  // -------- Task 5: transactions --------

  /// Maximum number of times [transaction] re-runs its body when Postgres
  /// aborts the transaction with a transient serialization/deadlock failure.
  /// Bounded so pathological contention eventually surfaces as a rethrown
  /// error (after [_maxTransactionAttempts] tries) rather than spinning
  /// forever; the rethrow is then the caller's to handle (e.g., a reactor's
  /// catchError backstop logs it instead of crashing).
  static const int _maxTransactionAttempts = 8;

  // Implements: EVS-PRD-event-log/A
  // successful body commits atomically;
  //   thrown exception rolls back. Postgres SERIALIZABLE isolation prevents
  //   the per-device sequence counter from being read+written by concurrent
  //   transactions.
  // Implements: EVS-PRD-event-log/E
  // under SERIALIZABLE, concurrent
  //   transactions that touch the global sequence counter (or the hash-chain
  //   tip, or overlapping view rows) make Postgres abort one with SQLSTATE
  //   40001 (serialization_failure) / 40P01 (deadlock_detected); the standard,
  //   safe remedy is to re-run the whole transaction body. Retrying here is
  //   transparent to every caller (EventStore.append and every reactor get
  //   conflict-retry for free) and correct because the body is idempotent: the
  //   sequence reservation, hash read, event insert, and projection writes all
  //   live inside the rolled-back transaction, so a retry re-derives them
  //   cleanly from the latest committed state.
  // Implements: EVS-DEV-postgres-backend/C
  // Transaction handle invalidated after
  //   body returns or throws.
  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) async {
    _checkOpen();
    for (var attempt = 1; ; attempt++) {
      try {
        return await _pool.runTx<T>(
          (tx) async {
            final wrapper = PostgresTxn(tx);
            try {
              return await body(wrapper);
            } finally {
              wrapper.invalidate();
            }
          },
          settings: TransactionSettings(
            isolationLevel: IsolationLevel.serializable,
          ),
        );
      } on ServerException catch (e, st) {
        // 40001 serialization_failure and 40P01 deadlock_detected are
        // transient under SERIALIZABLE: the loser of a race is aborted and
        // must simply re-run. The SQLSTATE is exposed on `e.code` by the
        // `postgres` v3 package (ServerException.code). Any other ServerException
        // (constraint violation, syntax error, …) is a real failure — rethrow.
        final retryable = e.code == '40001' || e.code == '40P01';
        if (!retryable) rethrow;
        if (attempt >= _maxTransactionAttempts) {
          // Bound exhausted: surface a distinguishable, typed failure (not the
          // bare driver exception) so callers can alert/quarantine without
          // coupling to the SQLSTATE. Preserve the original stack for forensics.
          Error.throwWithStackTrace(
            TransactionRetryExhaustedException(attempts: attempt, lastError: e),
            st,
          );
        }
        // Small linear backoff so the winning transaction can commit before we
        // re-contend; keeps a thundering herd of retries from livelocking.
        await Future<void>.delayed(Duration(milliseconds: 5 * attempt));
      }
    }
  }

  /// Runs `EventStore.open`'s boot as one `SERIALIZABLE` transaction whose
  /// first statement locks the `backend_state` table in `SHARE ROW
  /// EXCLUSIVE` mode.
  ///
  /// Every append updates the sequence counter row in `backend_state`, so
  /// the lock waits for the appends that hold it to commit and then keeps
  /// every later append out until the boot commits. `LOCK TABLE` takes no
  /// snapshot: the transaction's snapshot is taken by the statement after
  /// it, so it already includes every append the lock waited for, and those
  /// appends cannot abort the boot. An append that started before the boot
  /// committed fails once with a serialization failure afterwards, which
  /// its own retry absorbs. So the appends of a revision serving the same
  /// database, and every other write to `backend_state` (the drainer's fill
  /// cursor among them), pause for the boot's whole duration: its reads of
  /// the log's library-version events, its checks, and any seeding,
  /// promotion and re-derivation it performs. Two boots on one database
  /// run one after the other.
  ///
  /// The boot can still fail with a serialization or deadlock failure on a
  /// table it does not lock. It is then re-run, with a jittered 5 to 50 ms
  /// backoff, as long as the `bootLockWait` given to [open] has not passed
  /// since the first run started; a failure after that throws
  /// [TransactionRetryExhaustedException]. `bootLockWait` bounds only this
  /// re-running: neither the wait for the table lock (behind a long
  /// transaction such as another instance's boot) nor the run time of the
  /// body is bounded, and a body that runs longer than `bootLockWait` gets
  /// one run.
  ///
  /// Before the body runs, a database whose tables carry an earlier data
  /// format's single integer version columns is refused with
  /// [DatabaseResetRequiredError].
  // Implements: EVS-DEV-event-store-open/E
  // the boot transaction's first statement locks the table holding the
  //   sequence counter, so a serving revision's appends queue behind the
  //   boot instead of aborting it.
  @override
  @internal
  Future<T> bootTransaction<T>(Future<T> Function(Transaction txn) body) async {
    _checkOpen();
    final giveUpAt = DateTime.now().add(_bootLockWait);
    final random = Random();
    for (var attempt = 1; ; attempt++) {
      try {
        return await _pool.runTx<T>(
          (tx) async {
            final wrapper = PostgresTxn(tx);
            try {
              await tx.execute(
                'LOCK TABLE backend_state IN SHARE ROW EXCLUSIVE MODE',
              );
              await _refuseEarlierFormatColumns(tx);
              return await body(wrapper);
            } finally {
              wrapper.invalidate();
            }
          },
          settings: TransactionSettings(
            isolationLevel: IsolationLevel.serializable,
          ),
        );
      } on ServerException catch (e, st) {
        final retryable = e.code == '40001' || e.code == '40P01';
        if (!retryable) rethrow;
        if (!DateTime.now().isBefore(giveUpAt)) {
          Error.throwWithStackTrace(
            TransactionRetryExhaustedException(attempts: attempt, lastError: e),
            st,
          );
        }
        await Future<void>.delayed(
          Duration(milliseconds: 5 + random.nextInt(46)),
        );
      }
    }
  }

  /// Throws [DatabaseResetRequiredError] when the `events` or
  /// `view_target_versions` table carries an earlier data format's single
  /// integer version column, before any statement reads or writes the
  /// split major and minor columns.
  static Future<void> _refuseEarlierFormatColumns(Session session) async {
    final result = await session.execute(
      Sql.named('''
        SELECT table_name, column_name FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND ((table_name = 'events'
                AND column_name IN ('entry_type_version', 'lib_format_version'))
            OR (table_name = 'view_target_versions'
                AND column_name = 'target_version'))
        ORDER BY table_name, column_name
      '''),
    );
    if (result.isEmpty) return;
    final columns = [for (final row in result) '${row[0]}.${row[1]}'];
    throw DatabaseResetRequiredError(
      'its tables carry the single integer version columns of an earlier '
      'data format: ${columns.join(', ')}',
    );
  }

  // -------- Task 6: event log --------

  /// Persist [event] inside [txn] and return its [AppendResult]. Under the
  /// reserve-and-increment contract, `event.sequenceNumber` MUST equal the
  /// value returned by a prior [nextSequenceNumber] call in the same
  /// transaction. [appendEvent] does not advance the counter; the advance
  /// is owned by [nextSequenceNumber].
  ///
  /// A mismatch surfaces as `StateError` rather than a silent skipped
  /// sequence number — both branches indicate a caller bug.
  // Implements: EVS-PRD-event-log/A
  // persists event to append-only log
  //   atomically inside the supplied transaction.
  // Implements: EVS-PRD-event-log/B
  // sequence number stamped by caller
  //   from nextSequenceNumber; persisted verbatim preserving total order;
  //   advance owned by nextSequenceNumber, not appendEvent.
  @override
  @internal
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event) async {
    final session = _asPgTxn(txn).session;
    // Validate the reservation: the persisted counter must equal the seq
    // the caller is consuming. Reading the counter inside the same txn
    // sees the value staged by nextSequenceNumber.
    final reservedResult = await session.execute(
      Sql.named('''
        SELECT value::numeric::int FROM backend_state
        WHERE key = @k
      '''),
      parameters: {'k': _sequenceCounterKey},
    );
    final reserved = reservedResult.isEmpty
        ? 0
        : reservedResult.first[0] as int;
    if (event.sequenceNumber != reserved) {
      throw StateError(
        'appendEvent: event.sequenceNumber (${event.sequenceNumber}) '
        'must equal the reserved counter value ($reserved). '
        'Did the caller forget to call nextSequenceNumber in this '
        'transaction? appendEvent consumes a reservation, it does not '
        'create one.',
      );
    }
    await session.execute(
      Sql.named('''
        INSERT INTO events (
          sequence_number, event_id, aggregate_id, aggregate_type, entry_type,
          entry_type_version_major, entry_type_version_minor,
          lib_format_version_major, lib_format_version_minor, event_type,
          data, metadata, initiator,
          client_timestamp, event_hash, flow_token, previous_event_hash
        ) VALUES (
          @seq, @eventId, @aggId, @aggType, @entryType,
          @entryTypeMajor, @entryTypeMinor,
          @libFmtMajor, @libFmtMinor, @eventType,
          @data:jsonb, @metadata:jsonb, @initiator:jsonb,
          @clientTs:timestamptz, @eventHash, @flowToken, @prevHash
        )
      '''),
      parameters: {
        'seq': event.sequenceNumber,
        'eventId': event.eventId,
        'aggId': event.aggregateId,
        'aggType': event.aggregateType,
        'entryType': event.entryType,
        'entryTypeMajor': event.entryTypeVersion.major,
        'entryTypeMinor': event.entryTypeVersion.minor,
        'libFmtMajor': event.libFormatVersion.major,
        'libFmtMinor': event.libFormatVersion.minor,
        'eventType': event.eventType,
        'data': event.data,
        'metadata': event.metadata,
        'initiator': event.initiator.toJson(),
        'clientTs': event.clientTimestamp.toUtc(),
        'eventHash': event.eventHash,
        'flowToken': event.flowToken,
        'prevHash': event.previousEventHash,
      },
    );
    return AppendResult(
      sequenceNumber: event.sequenceNumber,
      eventHash: event.eventHash,
    );
  }

  // Implements: EVS-PRD-event-log/C
  // events for a single aggregate are
  //   returned in sequence_number order; the aggregate_id index keeps the
  //   lookup O(log n + k).
  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named(
        'SELECT * FROM events WHERE aggregate_id = @aggId '
        'ORDER BY sequence_number ASC',
      ),
      parameters: {'aggId': aggregateId},
    );
    return result.map(_storedEventFromRow).toList(growable: false);
  }

  // Implements: EVS-PRD-event-log/C
  // transactional variant; reads inside
  //   the same txn see writes staged in the same body (read-your-writes).
  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named(
        'SELECT * FROM events WHERE aggregate_id = @aggId '
        'ORDER BY sequence_number ASC',
      ),
      parameters: {'aggId': aggregateId},
    );
    return result.map(_storedEventFromRow).toList(growable: false);
  }

  // Implements: EVS-PRD-event-log/D
  // read all events in sequence order
  //   from any starting position (afterSequence + limit).
  // Implements: EVS-DEV-find-all-events-extended-filters/A+C — entryType,
  //   clientTimestampStart, clientTimestampEnd filters AND-compose with
  //   afterSequence, limit, originatorHopId, originatorIdentifier.
  // Implements: EVS-DEV-find-all-events-extended-filters/D
  // single shared
  //   composition helper (_findAllEventsComposed) used by both this method
  //   and findAllEventsInTxn.
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
    // `async` (not arrow / sync-return) so that a synchronous
    // `_checkOpen` throw on a closed backend lands as a rejected Future
    // rather than a synchronous exception at call-site evaluation. The
    // conformance harness uses `expectLater(..., throwsA(...))` which
    // matches on the awaited Future's error; a synchronous throw would
    // short-circuit before the matcher could observe it.
    _checkOpen();
    return _findAllEventsComposed(
      afterSequence: afterSequence,
      limit: limit,
      originatorHopId: originatorHopId,
      originatorIdentifier: originatorIdentifier,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
  }

  // Implements: EVS-PRD-event-log/D
  // transactional variant; reads see
  //   writes staged in the same txn body.
  // Implements: EVS-DEV-find-all-events-extended-filters/B+C+D
  // same
  //   three filters with same AND-composition semantics; shared helper.
  //
  // `async` (not arrow) so that the synchronous `_asPgTxn(txn).session`
  // check — which throws StateError on a post-body escape or a foreign
  // Transaction — completes the returned Future with the error rather than
  // throwing synchronously past the caller's `await`. The conformance
  // harness' `throwsStateError` matcher awaits the Future, so a
  // synchronous throw at call-site evaluation would short-circuit it.
  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final session = _asPgTxn(txn).session;
    return _findAllEventsComposed(
      session: session,
      afterSequence: afterSequence,
      limit: limit,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );
  }

  /// Single composition helper for [findAllEvents] and [findAllEventsInTxn]:
  /// builds the WHERE clause from supplied predicates, orders by
  /// `sequence_number ASC`, and applies the limit. Caller can pass an
  /// in-transaction [session] (a `TxSession`) or leave it null to query
  /// the pool directly. Both `Pool` and `TxSession` implement [Session].
  ///
  /// Originator predicates project into the JSONB `metadata` column at
  /// `metadata->'provenance'->0`. The originator-hop convention pins the
  /// first provenance entry as the originating hop (this is Layer 2; see
  /// CLAUDE.md). Reading the JSONB sub-field as text via `->>` keeps the
  /// comparison string-typed and matches the way `ProvenanceEntry.fromJson`
  /// reads the same keys on the Dart side.
  // Implements: EVS-DEV-find-all-events-extended-filters/D
  // single shared
  //   helper reused by both variants.
  Future<List<StoredEvent>> _findAllEventsComposed({
    Session? session,
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async {
    final wheres = <String>[];
    final params = <String, dynamic>{};
    if (afterSequence != null) {
      wheres.add('sequence_number > @afterSeq');
      params['afterSeq'] = afterSequence;
    }
    if (entryType != null) {
      wheres.add('entry_type = @entryType');
      params['entryType'] = entryType;
    }
    if (clientTimestampStart != null) {
      wheres.add('client_timestamp >= @ctsStart:timestamptz');
      params['ctsStart'] = clientTimestampStart.toUtc();
    }
    if (clientTimestampEnd != null) {
      wheres.add('client_timestamp <= @ctsEnd:timestamptz');
      params['ctsEnd'] = clientTimestampEnd.toUtc();
    }
    if (originatorHopId != null) {
      // First provenance entry's "hop" field as text. The double-arrow
      // (`->>`) returns text, matching the String comparison.
      wheres.add("metadata->'provenance'->0->>'hop' = @origHop");
      params['origHop'] = originatorHopId;
    }
    if (originatorIdentifier != null) {
      wheres.add("metadata->'provenance'->0->>'identifier' = @origId");
      params['origId'] = originatorIdentifier;
    }
    final whereClause = _composeWhere(wheres);
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final sql =
        'SELECT * FROM events $whereClause '
        'ORDER BY sequence_number ASC $limitClause';
    final exec = session ?? _pool;
    final result = await exec.execute(Sql.named(sql), parameters: params);
    return result.map(_storedEventFromRow).toList(growable: false);
  }

  // Implements: EVS-PRD-event-log/A
  // readLatestEventHash is transactional;
  //   value reflects writes staged in the same txn so a caller can build
  //   the next event's previous_event_hash atomically with the append that
  //   uses it.
  @override
  Future<String?> readLatestEventHash(Transaction txn) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      'SELECT event_hash FROM events '
      'ORDER BY sequence_number DESC LIMIT 1',
    );
    return result.isEmpty ? null : result.first[0] as String;
  }

  /// Reserve-and-increment the sequence counter inside [txn]. A second
  /// call in the same transaction returns `current + 2`; a paired
  /// [appendEvent] consumes the reservation without advancing again. If
  /// the surrounding transaction rolls back, the counter advance falls
  /// out of Postgres's transactional semantics.
  ///
  /// The row in `backend_state` is materialized lazily: a first-time
  /// caller sees the `INSERT ... ON CONFLICT DO NOTHING` initialize it
  /// to `0`, and the subsequent `UPDATE ... SET value = value + 1`
  /// reserves `1`. The counter is stored as a JSONB number; the
  /// `::text::int` round-trip keeps both the read and the increment
  /// explicit (JSONB doesn't have a direct arithmetic operator).
  // Implements: EVS-PRD-event-log/B
  // monotonic per-transaction reserve.
  @override
  @internal
  Future<int> nextSequenceNumber(Transaction txn) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, '0'::jsonb)
        ON CONFLICT (key) DO NOTHING
      '''),
      parameters: {'k': _sequenceCounterKey},
    );
    final result = await session.execute(
      Sql.named('''
        UPDATE backend_state
        SET value = ((value::numeric::int) + 1)::text::jsonb
        WHERE key = @k
        RETURNING value::numeric::int
      '''),
      parameters: {'k': _sequenceCounterKey},
    );
    return result.first[0] as int;
  }

  // Implements: EVS-PRD-event-log/B
  // counter is readable outside any txn
  //   for diagnostics; returns 0 when the row has never been materialized.
  @override
  Future<int> readSequenceCounter() async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named('SELECT value::numeric::int FROM backend_state WHERE key = @k'),
      parameters: {'k': _sequenceCounterKey},
    );
    return result.isEmpty ? 0 : result.first[0] as int;
  }

  // Implements: EVS-PRD-event-log/D
  // single-event lookup by event_id
  //   inside the supplied transaction; returns null when absent.
  @override
  Future<StoredEvent?> findEventByIdInTxn(
    Transaction txn,
    String eventId,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
      parameters: {'id': eventId},
    );
    return result.isEmpty ? null : _storedEventFromRow(result.first);
  }

  // Implements: EVS-PRD-event-log/D
  // single-event lookup by event_id
  //   outside any transaction; returns null when absent.
  @override
  Future<StoredEvent?> findEventById(String eventId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
      parameters: {'id': eventId},
    );
    return result.isEmpty ? null : _storedEventFromRow(result.first);
  }

  // -------- Task 7: view rows --------

  // Implements: EVS-DEV-postgres-backend/B
  // read a single JSONB blob from
  //   view_rows; returns null when the (view_name, row_key) pair is absent.
  @override
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT row_data FROM view_rows
        WHERE view_name = @v AND row_key = @k
        LIMIT 1
      '''),
      parameters: {'v': viewName, 'k': key},
    );
    if (result.isEmpty) return null;
    return _asJsonMap(result.first[0]);
  }

  // Implements: EVS-DEV-postgres-backend/B
  // whole-row upsert via
  //   INSERT … ON CONFLICT (view_name, row_key) DO UPDATE.
  @override
  @internal
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO view_rows (view_name, row_key, row_data, updated_at)
        VALUES (@v, @k, @row:jsonb, NOW())
        ON CONFLICT (view_name, row_key)
        DO UPDATE SET row_data = EXCLUDED.row_data, updated_at = NOW()
      '''),
      parameters: {'v': viewName, 'k': key, 'row': row},
    );
  }

  // Implements: EVS-DEV-postgres-backend/B
  // delete a single row from
  //   view_rows by (view_name, row_key); no-op when absent.
  @override
  @internal
  Future<void> deleteViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_rows WHERE view_name = @v AND row_key = @k'),
      parameters: {'v': viewName, 'k': key},
    );
  }

  // Implements: EVS-DEV-postgres-backend/B
  // list all rows for a view in
  //   deterministic row_key ASC order; optional LIMIT/OFFSET for paging.
  @override
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) async {
    _checkOpen();
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final offsetClause = offset == null ? '' : 'OFFSET $offset';
    final result = await _pool.execute(
      Sql.named('''
        SELECT row_data FROM view_rows
        WHERE view_name = @v
        ORDER BY row_key ASC
        $limitClause $offsetClause
      '''),
      parameters: {'v': viewName},
    );
    return result.map((r) => _asJsonMap(r[0])).toList();
  }

  // Implements: EVS-DEV-postgres-backend/B
  // bulk view_rows key-set read for the
  //   scoped AggregateMode snapshot: one `row_key = ANY(@keys)` query for the
  //   whole allow-list instead of a BEGIN/SELECT/COMMIT per id. The
  //   Dart `List<String>` binds to a Postgres text[] (same as the event_type
  //   `ANY(@types)` filter); selecting row_key lets the caller re-key the map.
  @override
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  ) async {
    _checkOpen();
    if (keys.isEmpty) return const <String, Map<String, dynamic>>{};
    final result = await _pool.execute(
      Sql.named('''
        SELECT row_key, row_data FROM view_rows
        WHERE view_name = @v AND row_key = ANY(@keys)
      '''),
      parameters: {'v': viewName, 'keys': keys.toList()},
    );
    return <String, Map<String, dynamic>>{
      for (final r in result) r[0] as String: _asJsonMap(r[1]),
    };
  }

  // Implements: EVS-DEV-postgres-backend/B
  // in-txn multi-row read with
  //   optional column-equality filter; required by the scoped-permissions
  //   authorize stage so its policy reads and the dispatch's event-append
  //   share one read-consistent snapshot. The `where` map is compiled into
  //   `row_data ->> $key_param = $value_param` predicates AND-composed; both
  //   key and value are passed as parameters (Postgres accepts a text-type
  //   parameter as the right operand of ->>), so caller-supplied keys
  //   cannot be interpolated into SQL. Null `where` or an empty map
  //   returns every row in the view (paging via limit/offset).
  // Implements: EVS-PRD-permissions-as-events
  // Implements: EVS-PRD-action-dispatch
  @override
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final session = _asPgTxn(txn).session;
    final params = <String, Object?>{'v': viewName};
    final whereClauses = <String>['view_name = @v'];
    if (where != null) {
      var i = 0;
      for (final entry in where.entries) {
        final keyParam = 'wk$i';
        final valParam = 'wv$i';
        whereClauses.add('row_data ->> @$keyParam = @$valParam');
        params[keyParam] = entry.key;
        params[valParam] = entry.value?.toString();
        i++;
      }
    }
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final offsetClause = offset == null ? '' : 'OFFSET $offset';
    final result = await session.execute(
      Sql.named('''
        SELECT row_data FROM view_rows
        WHERE ${whereClauses.join(' AND ')}
        ORDER BY row_key ASC
        $limitClause $offsetClause
      '''),
      parameters: params,
    );
    return result.map((r) => _asJsonMap(r[0])).toList();
  }

  // Implements: EVS-DEV-postgres-backend/B
  // delete all rows for a view
  //   without touching other views (WHERE view_name = @v).
  @override
  @internal
  Future<void> clearViewInTxn(Transaction txn, String viewName) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_rows WHERE view_name = @v'),
      parameters: {'v': viewName},
    );
  }

  // -------- Task 8: view target versions --------

  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; readViewTargetVersionInTxn reads a single row from the
  //   view_target_versions(view_name, entry_type, target_major,
  //   target_minor) table and returns null when the (view_name, entry_type)
  //   pair is absent.
  @override
  Future<EntryTypeVersion?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT target_major, target_minor FROM view_target_versions
        WHERE view_name = @v AND entry_type = @et
        LIMIT 1
      '''),
      parameters: {'v': viewName, 'et': entryType},
    );
    return result.isEmpty
        ? null
        : _entryTypeVersionOf(result.first[0], result.first[1]);
  }

  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; writeViewTargetVersionInTxn upserts via INSERT … ON CONFLICT
  //   DO UPDATE so repeated writes for the same (view_name, entry_type) pair
  //   reflect the latest target major and minor.
  @override
  @internal
  Future<void> writeViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
    EntryTypeVersion targetVersion,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO view_target_versions
          (view_name, entry_type, target_major, target_minor)
        VALUES (@v, @et, @major, @minor)
        ON CONFLICT (view_name, entry_type)
        DO UPDATE SET target_major = EXCLUDED.target_major,
                      target_minor = EXCLUDED.target_minor
      '''),
      parameters: {
        'v': viewName,
        'et': entryType,
        'major': targetVersion.major,
        'minor': targetVersion.minor,
      },
    );
  }

  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; readAllViewTargetVersionsInTxn returns all (entry_type →
  //   target version) pairs for the given view_name.
  @override
  Future<Map<String, EntryTypeVersion>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT entry_type, target_major, target_minor
        FROM view_target_versions
        WHERE view_name = @v
      '''),
      parameters: {'v': viewName},
    );
    return <String, EntryTypeVersion>{
      for (final row in result)
        row[0]! as String: _entryTypeVersionOf(row[1], row[2]),
    };
  }

  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; clearViewTargetVersionsInTxn deletes all rows for the given
  //   view_name without touching rows belonging to other views.
  @override
  @internal
  Future<void> clearViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_target_versions WHERE view_name = @v'),
      parameters: {'v': viewName},
    );
  }

  @override
  Future<Map<String, EntryTypeVersion>> readViewTargetsForEntryTypeInTxn(
    Transaction txn,
    String entryType,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT view_name, target_major, target_minor
        FROM view_target_versions
        WHERE entry_type = @et
      '''),
      parameters: {'et': entryType},
    );
    return <String, EntryTypeVersion>{
      for (final row in result)
        row[0]! as String: _entryTypeVersionOf(row[1], row[2]),
    };
  }

  @override
  @internal
  Future<void> markViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => _setViewTargetBehind(txn, viewName, entryType, behind: true);

  @override
  Future<bool> readViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT behind FROM view_target_versions
        WHERE view_name = @v AND entry_type = @et
      '''),
      parameters: {'v': viewName, 'et': entryType},
    );
    return result.isNotEmpty && result.first[0] == true;
  }

  @override
  @internal
  Future<void> clearViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => _setViewTargetBehind(txn, viewName, entryType, behind: false);

  Future<void> _setViewTargetBehind(
    Transaction txn,
    String viewName,
    String entryType, {
    required bool behind,
  }) async {
    final session = _asPgTxn(txn).session;
    // Only a row whose mark differs is updated, so a repeated mark writes
    // nothing.
    await session.execute(
      Sql.named('''
        UPDATE view_target_versions SET behind = @b
        WHERE view_name = @v AND entry_type = @et AND behind <> @b
      '''),
      parameters: {'v': viewName, 'et': entryType, 'b': behind},
    );
  }

  // -------- Task 9: FIFO --------
  //
  // Storage shape: a single `fifo_entries` table with PRIMARY KEY
  // (destination_id, sequence_in_queue). Where the sembast backend uses
  // a separate store-per-destination plus a `known_fifo_destinations`
  // registry to enumerate FIFOs, Postgres relies on the table-wide
  // scan: any row with a given destination_id IS the registration. This
  // collapses the "register on first use" step into the INSERT itself.

  /// Centralized row construction for the FIFO enqueue path, inside the
  /// caller's transaction (the fill and the replays it performs), so all
  /// the contract enforcement — empty-batch
  /// rejection, XOR shape, UUID minting, monotone sequence_in_queue
  /// assignment — lives in exactly one place.
  ///
  /// Per the contract:
  /// - 3rd-party (`wirePayload`): `wire_payload` stores the decoded JSON
  ///   map (one decode at enqueue time; drain hands the original bytes
  ///   back to `Destination.send` via re-encoding from `wire_payload`);
  ///   `wire_format = wirePayload.contentType`;
  ///   `transform_version = wirePayload.transformVersion`;
  ///   `envelope_metadata = null`.
  /// - Native (`nativeEnvelope`): `envelope_metadata` stores the
  ///   `BatchEnvelopeMetadata` map; `wire_payload = null`;
  ///   `wire_format = 'esd/batch@2'`; `transform_version = null`.
  // Implements: EVS-PRD-destinations
  // empty batch rejected with
  //   ArgumentError; XOR(wirePayload, nativeEnvelope) enforced; v4 UUID
  //   entry_id minted; sequence_in_queue assigned monotone via
  //   per-destination counter in backend_state; row persisted with all
  //   contract fields.
  @override
  @internal
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
        'enqueueFifoTxn requires a non-empty batch',
      );
    }
    // XOR: exactly one payload shape is legal. Reject both null and both
    // non-null at the boundary so a downstream FIFO row never carries an
    // ambiguous (wire_payload, envelope_metadata) pair.
    if ((wirePayload == null) == (nativeEnvelope == null)) {
      throw ArgumentError(
        'enqueueFifoTxn requires exactly one of wirePayload or nativeEnvelope '
        'to be non-null; got '
        'wirePayload=${wirePayload == null ? "null" : "set"}, '
        'nativeEnvelope=${nativeEnvelope == null ? "null" : "set"}',
      );
    }
    final session = _asPgTxn(txn).session;

    // Resolve payload columns from the chosen shape. Native rows carry
    // envelope_metadata + null wire_payload; 3rd-party rows decode the
    // bytes once (and reject non-Map JSON) and persist the resulting
    // map under wire_payload.
    Map<String, Object?>? payloadMap;
    String wireFormat;
    String? transformVersion;
    if (nativeEnvelope != null) {
      payloadMap = null;
      wireFormat = BatchEnvelope.wireFormat;
      transformVersion = null;
    } else {
      final wp = wirePayload!;
      try {
        final decoded = jsonDecode(utf8.decode(wp.bytes));
        if (decoded is! Map) {
          throw ArgumentError.value(
            wp,
            'wirePayload',
            'enqueueFifoTxn requires wirePayload.bytes to encode a JSON object '
                '(Map); got ${decoded.runtimeType}',
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
      wireFormat = wp.contentType;
      transformVersion = wp.transformVersion;
    }

    // Reserve the next sequence_in_queue from the per-destination
    // counter at backend_state/fifo_seq_counter_<dest>. Mirrors
    // `nextSequenceNumber`'s reserve-and-increment pattern: lazy
    // materialization on first use, monotone advance via UPDATE
    // RETURNING. The counter is NEVER reset — even when rows are
    // deleted by trail sweep, the vacated slot is not reused.
    final counterKey = _fifoSeqCounterKey(destinationId);
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, '0'::jsonb)
        ON CONFLICT (key) DO NOTHING
      '''),
      parameters: {'k': counterKey},
    );
    final counterResult = await session.execute(
      Sql.named('''
        UPDATE backend_state
        SET value = ((value::numeric::int) + 1)::text::jsonb
        WHERE key = @k
        RETURNING value::numeric::int
      '''),
      parameters: {'k': counterKey},
    );
    final sequenceInQueue = counterResult.first[0] as int;

    final entryId = _uuidGen.v4();
    final enqueuedAt = DateTime.now().toUtc();
    final eventIds = batch.map((e) => e.eventId).toList(growable: false);
    final firstSeq = batch.first.sequenceNumber;
    final lastSeq = batch.last.sequenceNumber;

    await session.execute(
      Sql.named('''
        INSERT INTO fifo_entries (
          destination_id, sequence_in_queue, entry_id,
          event_ids, event_id_first_seq, event_id_last_seq,
          wire_format, transform_version, enqueued_at,
          attempts, final_status, sent_at,
          wire_payload, envelope_metadata
        ) VALUES (
          @dest, @seq, @entryId,
          @eventIds:jsonb, @firstSeq, @lastSeq,
          @wireFmt, @transformV, @enqueuedAt:timestamptz,
          '[]'::jsonb, NULL, NULL,
          @wirePayload:jsonb, @envelope:jsonb
        )
      '''),
      parameters: {
        'dest': destinationId,
        'seq': sequenceInQueue,
        'entryId': entryId,
        'eventIds': eventIds,
        'firstSeq': firstSeq,
        'lastSeq': lastSeq,
        'wireFmt': wireFormat,
        'transformV': transformVersion,
        'enqueuedAt': enqueuedAt,
        'wirePayload': payloadMap,
        'envelope': nativeEnvelope?.toMap(),
      },
    );

    return FifoEntry(
      entryId: entryId,
      eventIds: List<String>.unmodifiable(eventIds),
      sequenceRange: (firstSeq: firstSeq, lastSeq: lastSeq),
      sequenceInQueue: sequenceInQueue,
      wirePayload: payloadMap == null
          ? null
          : Map<String, Object?>.unmodifiable(payloadMap),
      wireFormat: wireFormat,
      transformVersion: transformVersion,
      enqueuedAt: enqueuedAt,
      attempts: const <AttemptResult>[],
      finalStatus: null,
      sentAt: null,
      envelopeMetadata: nativeEnvelope,
    );
  }

  // Implements: EVS-PRD-destinations
  // readFifoHead returns the first row
  //   in sequence_in_queue order whose final_status is null OR 'wedged';
  //   sent and tombstoned rows are skipped. Returns null on empty FIFO.
  //   Uses the partial `fifo_entries_head_idx` for an index-only scan.
  @override
  Future<FifoEntry?> readFifoHead(String destinationId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named(_fifoHeadSql),
      parameters: {'dest': destinationId},
    );
    return result.isEmpty ? null : _fifoEntryFromRow(result.first);
  }

  @override
  @internal
  Future<FifoEntry?> readFifoHeadTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named(_fifoHeadSql),
      parameters: {'dest': destinationId},
    );
    return result.isEmpty ? null : _fifoEntryFromRow(result.first);
  }

  static const String _fifoHeadSql = '''
    SELECT * FROM fifo_entries
    WHERE destination_id = @dest
      AND (final_status IS NULL OR final_status = 'wedged')
    ORDER BY sequence_in_queue ASC
    LIMIT 1
  ''';

  // Implements: EVS-PRD-destinations
  // listFifoEntries enumerates rows
  //   in sequence_in_queue ASC; afterSequenceInQueue is exclusive; limit
  //   caps from the start of the ordered range. Empty list on unknown
  //   destination (no rows match the WHERE clause).
  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) async {
    _checkOpen();
    final wheres = <String>['destination_id = @dest'];
    final params = <String, Object?>{'dest': destinationId};
    if (afterSequenceInQueue != null) {
      wheres.add('sequence_in_queue > @afterSeq');
      params['afterSeq'] = afterSequenceInQueue;
    }
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final sql =
        'SELECT * FROM fifo_entries ${_composeWhere(wheres)} '
        'ORDER BY sequence_in_queue ASC $limitClause';
    final result = await _pool.execute(Sql.named(sql), parameters: params);
    return result.map(_fifoEntryFromRow).toList(growable: false);
  }

  /// Append [attempt] to the entry's `attempts[]` JSONB array inside
  /// [txn]. Throws [StateError] when the entry is absent or terminal.
  @override
  @internal
  Future<void> appendAttemptTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) async {
    final session = _asPgTxn(txn).session;
    final existing = await session.execute(
      Sql.named('''
        SELECT final_status FROM fifo_entries
        WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'dest': destinationId, 'e': entryId},
    );
    if (existing.isEmpty) {
      throw StateError(
        'appendAttemptTxn($destinationId, $entryId): no such queue item; '
        'the drainer records an attempt only on the pending head it sent.',
      );
    }
    final currentRaw = existing.first[0] as String?;
    if (currentRaw != null) {
      throw StateError(
        'appendAttemptTxn($destinationId, $entryId): the item is '
        '$currentRaw; an attempt is recorded only on a pending item.',
      );
    }
    await session.execute(
      Sql.named('''
        UPDATE fifo_entries
        SET attempts = attempts || @attempt:jsonb
        WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {
        'dest': destinationId,
        'e': entryId,
        'attempt': <Object?>[attempt.toJson()],
      },
    );
  }

  // Implements: EVS-PRD-destinations
  // hasFifoWedged true iff any
  //   destination's head row (first sequence_in_queue with
  //   final_status IN {null, wedged}) is wedged. Single SQL pass via
  //   DISTINCT ON (destination_id) so we visit each FIFO's head row in
  //   one scan, then filter to wedged.
  @override
  Future<bool> hasFifoWedged() async {
    _checkOpen();
    final result = await _pool.execute('''
      SELECT EXISTS (
        SELECT 1 FROM (
          SELECT DISTINCT ON (destination_id) destination_id, final_status
          FROM fifo_entries
          WHERE final_status IS NULL OR final_status = 'wedged'
          ORDER BY destination_id, sequence_in_queue ASC
        ) heads
        WHERE heads.final_status = 'wedged'
      )
    ''');
    return result.first[0] as bool;
  }

  // Implements: EVS-PRD-destinations
  // wedgedFifos returns one summary
  //   per wedged FIFO. headEventId is the first event_id on the wedged
  //   head row; wedgedAt = last attempt's attemptedAt (or enqueued_at
  //   when no attempts recorded); lastError = last attempt's
  //   error_message (or fallback string when none).
  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() async {
    _checkOpen();
    final result = await _pool.execute('''
      SELECT destination_id, entry_id, event_ids,
             enqueued_at, attempts, final_status
      FROM (
        SELECT DISTINCT ON (destination_id)
          destination_id, entry_id, event_ids, enqueued_at, attempts,
          final_status, sequence_in_queue
        FROM fifo_entries
        WHERE final_status IS NULL OR final_status = 'wedged'
        ORDER BY destination_id, sequence_in_queue ASC
      ) heads
      WHERE heads.final_status = 'wedged'
      ORDER BY destination_id
    ''');
    return result
        .map((row) {
          final destinationId = row[0] as String;
          final entryId = row[1] as String;
          final eventIds = List<String>.from(row[2] as List);
          final enqueuedAt = (row[3] as DateTime).toUtc();
          final attemptsRaw = row[4] as List;
          final hasAttempts = attemptsRaw.isNotEmpty;
          // A wedged row with no attempts is not produced by the drain,
          // but the summary still surfaces enqueued_at + a placeholder
          // error string so operators can identify such a row.
          final DateTime wedgedAt;
          final String lastError;
          if (hasAttempts) {
            final lastAttempt = _asJsonMap(attemptsRaw.last);
            wedgedAt = DateTime.parse(
              lastAttempt['attempted_at']! as String,
            ).toUtc();
            lastError =
                (lastAttempt['error_message'] as String?) ??
                '<no error message>';
          } else {
            wedgedAt = enqueuedAt;
            lastError = '<wedged with no attempts recorded>';
          }
          return WedgedFifoSummary(
            destinationId: destinationId,
            headEntryId: entryId,
            headEventId: eventIds.first,
            wedgedAt: wedgedAt,
            lastError: lastError,
          );
        })
        .toList(growable: false);
  }

  // Implements: EVS-PRD-destinations
  // readFifoRow looks up a single row
  //   by (destination_id, entry_id); returns null when absent. Used by
  //   tooling/tests to inspect a specific row.
  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named('''
        SELECT * FROM fifo_entries
        WHERE destination_id = @dest AND entry_id = @e
        LIMIT 1
      '''),
      parameters: {'dest': destinationId, 'e': entryId},
    );
    return result.isEmpty ? null : _fifoEntryFromRow(result.first);
  }

  // Implements: EVS-DEV-destination-drain/B
  // setFinalStatusTxn allows exactly
  //   null -> sent, null -> wedged and wedged -> tombstoned; every other pair,
  //   a repeated status and a missing row throw StateError with nothing
  //   written. null -> sent stamps sent_at; attempts[] is never touched.
  @override
  @internal
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus status,
  ) async {
    final session = _asPgTxn(txn).session;
    final existing = await session.execute(
      Sql.named('''
        SELECT final_status FROM fifo_entries
        WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'dest': destinationId, 'e': entryId},
    );
    if (existing.isEmpty) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId, $status): no such '
        'queue item.',
      );
    }
    final currentRaw = existing.first[0] as String?;
    final current = currentRaw == null
        ? null
        : FinalStatus.fromJson(currentRaw);
    if (!isLegalFinalStatusTransition(current, status)) {
      throw StateError(
        'setFinalStatusTxn($destinationId, $entryId): illegal transition '
        '${current?.name} -> ${status.name}. Legal transitions: '
        'null -> sent, null -> wedged, wedged -> tombstoned.',
      );
    }
    if (status == FinalStatus.sent) {
      await session.execute(
        Sql.named('''
          UPDATE fifo_entries
          SET final_status = @s, sent_at = @t:timestamptz
          WHERE destination_id = @dest AND entry_id = @e
        '''),
        parameters: {
          's': status.name,
          't': DateTime.now().toUtc(),
          'dest': destinationId,
          'e': entryId,
        },
      );
    } else {
      await session.execute(
        Sql.named('''
          UPDATE fifo_entries
          SET final_status = @s
          WHERE destination_id = @dest AND entry_id = @e
        '''),
        parameters: {'s': status.name, 'dest': destinationId, 'e': entryId},
      );
    }
  }

  // Implements: EVS-DEV-destination-drain/F
  // trail-sweep DELETE used by
  //   tombstoneAndRefill: removes rows whose sequence_in_queue is
  //   strictly greater than [afterSequenceInQueue] AND whose
  //   final_status IS null. Terminal rows are retained for the database's
  //   lifetime as audit records and never touched here. Returns a
  //   TrailSweepResult (the deleted count and lowest first sequence, read
  //   through DELETE ... RETURNING).
  @override
  @internal
  Future<TrailSweepResult> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        DELETE FROM fifo_entries
        WHERE destination_id = @dest
          AND sequence_in_queue > @afterSeq
          AND final_status IS NULL
        RETURNING event_id_first_seq
      '''),
      parameters: {'dest': destinationId, 'afterSeq': afterSequenceInQueue},
    );
    int? minFirstSeq;
    for (final row in result) {
      final firstSeq = row[0]! as int;
      if (minFirstSeq == null || firstSeq < minFirstSeq) minFirstSeq = firstSeq;
    }
    return TrailSweepResult(
      deletedCount: result.length,
      minFirstSeq: minFirstSeq,
    );
  }

  // Implements: EVS-DEV-destination-drain/A
  // retire a deleted destination's queue:
  //   refuse a pending head; tombstone a wedged head; delete the pending rows
  //   and the fill cursor; keep every terminal row and the sequence_in_queue
  //   counter so a re-added destination's rows continue above the retained
  //   ones (the primary key is (destination_id, sequence_in_queue)).
  @override
  @internal
  Future<QueueRetirement> retireQueueTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final session = _asPgTxn(txn).session;
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
    final deleted = await session.execute(
      Sql.named('''
        DELETE FROM fifo_entries
        WHERE destination_id = @dest AND final_status IS NULL
      '''),
      parameters: {'dest': destinationId},
    );
    await session.execute(
      Sql.named('DELETE FROM backend_state WHERE key = @k'),
      parameters: {'k': 'fill_cursor_$destinationId'},
    );
    return QueueRetirement(
      tombstonedRowId: tombstoned,
      deletedPendingCount: deleted.affectedRows,
    );
  }

  // -------- Replay requests --------

  @override
  @internal
  Future<ReplayRequest?> readReplayRequestTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final value = await _readStateTxn(txn, 'replay_request_$destinationId');
    return value == null ? null : ReplayRequest.fromJson(_asJsonMap(value));
  }

  @override
  @internal
  Future<void> writeReplayRequestTxn(
    Transaction txn,
    String destinationId,
    ReplayRequest request,
  ) => _writeStateTxn(txn, 'replay_request_$destinationId', request.toJson());

  @override
  @internal
  Future<void> clearReplayRequestTxn(Transaction txn, String destinationId) =>
      _deleteStateTxn(txn, 'replay_request_$destinationId');

  // -------- Wedge records --------

  @override
  @internal
  Future<WedgeRecord?> readWedgeRecordTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final value = await _readStateTxn(txn, 'wedge_$destinationId');
    return value == null ? null : WedgeRecord.fromJson(_asJsonMap(value));
  }

  @override
  @internal
  Future<void> writeWedgeRecordTxn(
    Transaction txn,
    String destinationId,
    WedgeRecord record,
  ) => _writeStateTxn(txn, 'wedge_$destinationId', record.toJson());

  @override
  @internal
  Future<void> clearWedgeRecordTxn(Transaction txn, String destinationId) =>
      _deleteStateTxn(txn, 'wedge_$destinationId');

  // -------- Registry check record --------

  @override
  @internal
  Future<void> writeRegistryCheckTxn(Transaction txn, RegistryCheck check) =>
      _writeStateTxn(txn, 'registry_check', check.toJson());

  @override
  @internal
  Future<RegistryCheck?> readRegistryCheckTxn(Transaction txn) async {
    final value = await _readStateTxn(txn, 'registry_check');
    return value == null ? null : RegistryCheck.fromJson(_asJsonMap(value));
  }

  // -------- Database identity and boot record --------

  @override
  @internal
  Future<String?> readDatabaseIdTxn(Transaction txn) async {
    final value = await _readStateTxn(txn, _databaseIdKey);
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
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, to_jsonb(@id::text))
        ON CONFLICT (key) DO NOTHING
      '''),
      parameters: {'k': _databaseIdKey, 'id': const Uuid().v4()},
    );
    final stored = await readDatabaseIdTxn(txn);
    if (stored == null) {
      throw StateError(
        'backend_state[$_databaseIdKey] is absent after it was written',
      );
    }
    return stored;
  }

  @override
  @internal
  Future<void> writeBootCheckTxn(Transaction txn, BootCheck check) =>
      _writeStateTxn(txn, 'boot_check', check.toJson());

  @override
  @internal
  Future<BootCheck?> readBootCheckTxn(Transaction txn) async {
    final value = await _readStateTxn(txn, 'boot_check');
    return value == null ? null : BootCheck.fromJson(_asJsonMap(value));
  }

  static const String _databaseIdKey = 'database_id';

  Future<Object?> _readStateTxn(Transaction txn, String key) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('SELECT value FROM backend_state WHERE key = @k'),
      parameters: {'k': key},
    );
    return result.isEmpty ? null : result.first[0];
  }

  Future<void> _writeStateTxn(
    Transaction txn,
    String key,
    Map<String, Object?> value,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, @v:jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
      '''),
      parameters: {'k': key, 'v': value},
    );
  }

  Future<void> _deleteStateTxn(Transaction txn, String key) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM backend_state WHERE key = @k'),
      parameters: {'k': key},
    );
  }

  // -------- Task 10: backend_state (schema version, fill cursor, schedule) --------

  // Implements: EVS-DEV-postgres-backend/D
  // backend-state KV (schema
  //   version, fill cursor, schedules) persisted in the backend_state
  //   table keyed by stable string keys.

  // Key: 'schema_version'; value: integer.
  // Returns 0 when the row has never been written.
  @override
  Future<int> readSchemaVersion() async {
    _checkOpen();
    final result = await _pool.execute(
      "SELECT value::numeric::int FROM backend_state WHERE key = 'schema_version'",
    );
    return result.isEmpty ? 0 : result.first[0] as int;
  }

  // Key: 'schema_version'; value: integer. INSERT … ON CONFLICT DO UPDATE.
  @override
  @internal
  Future<void> writeSchemaVersion(Transaction txn, int version) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES ('schema_version', @v:jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
      '''),
      parameters: {'v': version},
    );
  }

  // Key: 'fill_cursor_<destinationId>'; value: integer.
  // Returns -1 when the row has never been written.
  @override
  Future<int> readFillCursor(String destinationId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named('SELECT value::numeric::int FROM backend_state WHERE key = @k'),
      parameters: {'k': 'fill_cursor_$destinationId'},
    );
    return result.isEmpty ? -1 : result.first[0] as int;
  }

  // In-txn read of fill_cursor; -1 when unset.
  @override
  @internal
  Future<int> readFillCursorTxn(Transaction txn, String destinationId) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('SELECT value::numeric::int FROM backend_state WHERE key = @k'),
      parameters: {'k': 'fill_cursor_$destinationId'},
    );
    return result.isEmpty ? -1 : result.first[0] as int;
  }

  // In-txn write for fill_cursor. INSERT … ON CONFLICT DO UPDATE.
  // `async` so validation errors land as Future completions.
  @override
  @internal
  Future<void> writeFillCursorTxn(
    Transaction txn,
    String destinationId,
    int sequenceNumber,
  ) async {
    _validateFillCursorValue(sequenceNumber);
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, @v:jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
      '''),
      parameters: {'k': 'fill_cursor_$destinationId', 'v': sequenceNumber},
    );
  }

  // Key: 'schedule_<destinationId>'; value: DestinationSchedule JSON map.
  // Returns null when the row has never been written.
  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) async {
    _checkOpen();
    final result = await _pool.execute(
      Sql.named('SELECT value FROM backend_state WHERE key = @k'),
      parameters: {'k': 'schedule_$destinationId'},
    );
    if (result.isEmpty) return null;
    return DestinationSchedule.fromJson(_asJsonMap(result.first[0]));
  }

  // In-txn read of the schedule; null when absent.
  @override
  @internal
  Future<DestinationSchedule?> readScheduleTxn(
    Transaction txn,
    String destinationId,
  ) async {
    final value = await _readStateTxn(txn, 'schedule_$destinationId');
    return value == null
        ? null
        : DestinationSchedule.fromJson(_asJsonMap(value));
  }

  // In-txn write for schedule. INSERT … ON CONFLICT DO UPDATE.
  @override
  @internal
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO backend_state (key, value)
        VALUES (@k, @v:jsonb)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
      '''),
      parameters: {'k': 'schedule_$destinationId', 'v': schedule.toJson()},
    );
  }

  // Delete the schedule row for [destinationId]. No-op when absent.
  @override
  @internal
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM backend_state WHERE key = @k'),
      parameters: {'k': 'schedule_$destinationId'},
    );
  }

  // -------- Task 11: reverse scan + audit query --------

  /// Server-side-paged reverse scan over the event log. Emits events in
  /// descending `sequence_number` order, optionally filtered to a set of
  /// event types. The implementation pages on `sequence_number < @last`
  /// in batches of [_reverseScanPageSize] so a `await for ... break` on
  /// the first match terminates after at most one page worth of network
  /// traffic. When [eventTypes] is null no type filter is applied;
  /// otherwise the filter binds as a Postgres `event_type = ANY(@types)`
  /// against a `List<String>` parameter.
  // Implements: EVS-PRD-event-log/D
  // read events in (reverse) order from
  //   any starting position; a consumer may stop at its first match.
  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; the reverse read behaves as SembastBackend's does.
  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) async* {
    _checkOpen();
    int? lastSeenSequence;
    while (true) {
      final wheres = <String>[];
      final params = <String, dynamic>{};
      if (lastSeenSequence != null) {
        wheres.add('sequence_number < @last');
        params['last'] = lastSeenSequence;
      }
      if (eventTypes != null) {
        // ANY(@types) binds a Dart `List<String>` to a Postgres text[] in
        // postgres v3.5 without further coercion; verified at task 11
        // landing.
        wheres.add('event_type = ANY(@types)');
        params['types'] = eventTypes.toList();
      }
      final whereClause = _composeWhere(wheres);
      final result = await _pool.execute(
        Sql.named(
          'SELECT * FROM events $whereClause '
          'ORDER BY sequence_number DESC '
          'LIMIT $_reverseScanPageSize',
        ),
        parameters: params,
      );
      if (result.isEmpty) return;
      for (final row in result) {
        yield _storedEventFromRow(row);
      }
      // Use the last row's sequence_number as the upper-exclusive bound
      // for the next page. Once a result page is shorter than the page
      // size we know there are no more rows; bail without an extra
      // round-trip.
      lastSeenSequence = result.last.toColumnMap()['sequence_number'] as int;
      if (result.length < _reverseScanPageSize) return;
    }
  }

  /// Pages on `sequence_number < @last`. The first page holds 16 events
  /// and each later page twice as many, up to [_reverseScanPageSize], so a
  /// caller that stops after the first few events reads little.
  @override
  @internal
  Stream<StoredEvent> readEventsReverseInTxn(
    Transaction txn, {
    Set<String>? eventTypes,
  }) async* {
    int? lastSeenSequence;
    var pageSize = 16;
    while (true) {
      final session = _asPgTxn(txn).session;
      final wheres = <String>[];
      final params = <String, dynamic>{};
      if (lastSeenSequence != null) {
        wheres.add('sequence_number < @last');
        params['last'] = lastSeenSequence;
      }
      if (eventTypes != null) {
        wheres.add('event_type = ANY(@types)');
        params['types'] = eventTypes.toList();
      }
      final whereClause = _composeWhere(wheres);
      final result = await session.execute(
        Sql.named(
          'SELECT * FROM events $whereClause '
          'ORDER BY sequence_number DESC '
          'LIMIT $pageSize',
        ),
        parameters: params,
      );
      if (result.isEmpty) return;
      for (final row in result) {
        yield _storedEventFromRow(row);
      }
      lastSeenSequence = result.last.toColumnMap()['sequence_number'] as int;
      if (result.length < pageSize) return;
      pageSize = min(pageSize * 2, _reverseScanPageSize);
    }
  }

  /// Cross-store audit query joining `events` and `security_context` on
  /// `event_id`. Filters AND-compose; pagination is a strict lower bound
  /// on the `(recorded_at, event_id)` tuple under DESC ordering so a
  /// stable forward walk does not skew under concurrent head-inserts.
  ///
  /// Cursor encoding: base64-url of `"<isoRecordedAt>|<eventId>"`. Pipe
  /// is illegal in both ISO 8601 timestamps and UUIDs, so the split is
  /// unambiguous. Corrupt cursors (wrong shape, unparseable timestamp,
  /// non-base64) surface as `ArgumentError`. The encoding mirrors the
  /// sembast `_AuditCursorPoint` shape so a cursor minted under one
  /// backend would be readable under the other if/when shared — though
  /// the contract does not require this and tests do not exercise it.
  // Implements: EVS-PRD-regulatory-alignment
  // queryAudit provides the
  //   ALCOA+-Available retrieval path; join lives in the storage layer
  //   so consumers cannot reach past the abstraction.
  // Implements: EVS-DEV-postgres-backend/D
  // backend passes the conformance
  //   harness; queryAudit materialized so the security-context store's
  //   delegator round-trips identically to sembast.
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
    _checkOpen();
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

    final wheres = <String>[];
    final params = <String, dynamic>{};

    if (initiator != null) {
      // events.initiator is JSONB; the `@v:jsonb = jsonb` comparison is
      // structural (independent of key order) so the comparison matches
      // SembastBackend's `events.where((e) => e.initiator == initiator)`.
      wheres.add('events.initiator = @initJson:jsonb');
      params['initJson'] = initiator.toJson();
    }
    if (flowToken != null) {
      wheres.add('events.flow_token = @flowTok');
      params['flowTok'] = flowToken;
    }
    if (ipAddress != null) {
      wheres.add('security_context.ip_address = @ip');
      params['ip'] = ipAddress;
    }
    if (from != null) {
      wheres.add('security_context.recorded_at >= @from:timestamptz');
      params['from'] = from.toUtc();
    }
    if (to != null) {
      wheres.add('security_context.recorded_at <= @to:timestamptz');
      params['to'] = to.toUtc();
    }
    if (decodedCursor != null) {
      // Postgres supports row-value tuple comparison directly: the
      // strict-less-than form below evaluates to TRUE iff recorded_at is
      // less than the cursor's recorded_at, OR they are equal and
      // event_id is less than the cursor's event_id — i.e. the strict
      // lower bound under the DESC ordering used below. (Verified
      // working under Postgres 16 by task 11's conformance run.)
      wheres.add(
        '(security_context.recorded_at, events.event_id) '
        '< (@curAt:timestamptz, @curEv)',
      );
      params['curAt'] = decodedCursor.recordedAt.toUtc();
      params['curEv'] = decodedCursor.eventId;
    }

    final whereClause = _composeWhere(wheres);
    // Fetch limit + 1 to detect "more pages" without a separate
    // COUNT(*); the extra row, if present, is dropped from the page and
    // its predecessor's (recorded_at, event_id) tuple is encoded into
    // nextCursor.
    final result = await _pool.execute(
      Sql.named('''
        SELECT
          events.sequence_number, events.event_id, events.aggregate_id,
          events.aggregate_type, events.entry_type,
          events.entry_type_version_major, events.entry_type_version_minor,
          events.lib_format_version_major, events.lib_format_version_minor,
          events.event_type,
          events.data, events.metadata, events.initiator,
          events.client_timestamp, events.event_hash, events.flow_token,
          events.previous_event_hash,
          security_context.recorded_at, security_context.ip_address,
          security_context.payload
        FROM events
        INNER JOIN security_context
          ON events.event_id = security_context.event_id
        $whereClause
        ORDER BY security_context.recorded_at DESC, events.event_id DESC
        LIMIT ${limit + 1}
      '''),
      parameters: params,
    );

    final rows = <AuditRow>[];
    final returnedCount = result.length > limit ? limit : result.length;
    for (var i = 0; i < returnedCount; i++) {
      final row = result[i];
      final event = StoredEvent(
        key: row[0] as int,
        sequenceNumber: row[0] as int,
        eventId: row[1] as String,
        aggregateId: row[2] as String,
        aggregateType: row[3] as String,
        entryType: row[4] as String,
        entryTypeVersion: _entryTypeVersionOf(row[5], row[6]),
        libFormatVersion: _dataFormatVersionOf(row[7], row[8]),
        eventType: row[9] as String,
        data: _asJsonMap(row[10]),
        metadata: _asJsonMap(row[11]),
        initiator: Initiator.fromJson(_asJsonMap(row[12])),
        clientTimestamp: (row[13] as DateTime).toUtc(),
        eventHash: row[14] as String,
        flowToken: row[15] as String?,
        previousEventHash: row[16] as String?,
      );
      // The security row is reified from the JSONB `payload` column so
      // every field on EventSecurityContext (user_agent, session_id,
      // geo_*, redacted_at, redaction_reason) lands populated — the
      // top-level `recorded_at` / `ip_address` columns exist only for
      // server-side filtering and ORDER BY.
      final context = EventSecurityContext.fromJson(_asJsonMap(row[19]));
      rows.add(AuditRow(event: event, securityContext: context));
    }
    String? nextCursor;
    if (result.length > limit) {
      final tail = rows.last;
      nextCursor = _AuditCursorPoint(
        recordedAt: tail.securityContext.recordedAt,
        eventId: tail.event.eventId,
      ).encode();
    }
    return PagedAudit(rows: rows, nextCursor: nextCursor);
  }

  /// Page size for [readEventsReverse]'s server-side pagination. 1024 is
  /// a soft choice — large enough that the typical first-match-and-break
  /// terminates in one round-trip, small enough that a full reverse
  /// walk over a multi-million-row log doesn't blow per-batch memory.
  static const int _reverseScanPageSize = 1024;

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  /// Validates that [sequenceNumber] is a legal fill-cursor value.
  /// The legal domain is `[-1, ∞)`: -1 is "unset or rewound to pre-start";
  /// all other values are event sequence_numbers (non-negative). Mirrors
  /// `SembastBackend._validateFillCursorValue`.
  static void _validateFillCursorValue(int sequenceNumber) {
    if (sequenceNumber < -1) {
      throw ArgumentError.value(
        sequenceNumber,
        'sequenceNumber',
        'fill_cursor must be >= -1 (-1 = unset or rewound to pre-start; '
            'all other values are event sequence_numbers)',
      );
    }
  }

  /// Key under which the per-install sequence counter is persisted in
  /// the `backend_state` KV row. Sembast uses the same string for the
  /// same purpose; keeping the key name aligned makes a cross-backend
  /// audit easier (`select * from backend_state where key = 'sequence_counter'`
  /// works on Postgres and parallels the sembast record key).
  static const String _sequenceCounterKey = 'sequence_counter';

  /// Key under which a per-destination FIFO sequence_in_queue counter is
  /// persisted in `backend_state`. Sembast uses the same key shape; the
  /// aligned naming makes a cross-backend audit trivial.
  static String _fifoSeqCounterKey(String destinationId) =>
      'fifo_seq_counter_$destinationId';

  /// Downcast a [Transaction] handed to this backend's StorageBackend methods
  /// into the concrete [PostgresTxn]. Any other concrete subtype indicates
  /// the caller mixed two different backends' Transaction handles — that's a bug,
  /// not a recoverable state, so we surface it as `StateError`.
  ///
  /// The `session` getter on a valid [PostgresTxn] in turn throws
  /// `StateError` when the surrounding transaction body has already
  /// returned (the handle was invalidated). Either failure mode produces
  /// the same outward shape, which matches the conformance harness'
  /// `throwsStateError` expectations for both "foreign Transaction" and
  /// "post-body escape" cases.
  PostgresTxn _asPgTxn(Transaction txn) {
    if (txn is! PostgresTxn) {
      throw StateError(
        'PostgresBackend: Transaction was produced by a different StorageBackend '
        'implementation; refusing to apply it. Got ${txn.runtimeType}.',
      );
    }
    return txn;
  }

  /// Reify a Postgres event row into a [StoredEvent]. The driver returns
  /// JSONB columns as already-decoded Dart maps/lists and TIMESTAMPTZ as
  /// `DateTime` in UTC, so most fields land verbatim. The map adapters
  /// (`_asJsonMap`) coerce a less-typed `Map` (e.g. `Map<Object?, Object?>`
  /// from nested JSONB decoding) into the `Map<String, dynamic>` shape
  /// [StoredEvent] expects on its public surface.
  ///
  /// Skips `StoredEvent.fromMap` because that factory expects
  /// `client_timestamp` to be an ISO 8601 *string*; the binary protocol
  /// has already produced a `DateTime` for us, so we construct the value
  /// type directly to avoid a stringify-then-parse round-trip.
  StoredEvent _storedEventFromRow(ResultRow row) {
    final m = row.toColumnMap();
    return StoredEvent(
      // `key` mirrors `sequence_number` for the Postgres backend: the
      // sembast backend's `key` is the auto-assigned record key, which
      // happens to track sequence_number for the events store. On
      // Postgres there's no separate key surface — sequence_number IS
      // the primary key.
      key: m['sequence_number'] as int,
      eventId: m['event_id'] as String,
      aggregateId: m['aggregate_id'] as String,
      aggregateType: m['aggregate_type'] as String,
      entryType: m['entry_type'] as String,
      entryTypeVersion: _entryTypeVersionOf(
        m['entry_type_version_major'],
        m['entry_type_version_minor'],
      ),
      libFormatVersion: _dataFormatVersionOf(
        m['lib_format_version_major'],
        m['lib_format_version_minor'],
      ),
      eventType: m['event_type'] as String,
      sequenceNumber: m['sequence_number'] as int,
      data: _asJsonMap(m['data']),
      metadata: _asJsonMap(m['metadata']),
      initiator: Initiator.fromJson(_asJsonMap(m['initiator'])),
      flowToken: m['flow_token'] as String?,
      clientTimestamp: (m['client_timestamp'] as DateTime).toUtc(),
      eventHash: m['event_hash'] as String,
      previousEventHash: m['previous_event_hash'] as String?,
    );
  }

  /// Reify a Postgres `fifo_entries` row into a [FifoEntry]. JSONB
  /// columns are returned by the driver as already-decoded Dart
  /// maps/lists; TIMESTAMPTZ columns as `DateTime` in UTC. The shape
  /// matches the contract enforced by [FifoEntry]'s constructor:
  /// `eventIds` non-empty and `sequenceRange.firstSeq <= lastSeq`. The
  /// driver's already-typed `int`/`String?` columns are passed through
  /// without re-encoding so the comparison surface stays explicit.
  FifoEntry _fifoEntryFromRow(ResultRow row) {
    final m = row.toColumnMap();
    final eventIds = List<String>.from(m['event_ids'] as List);
    final attemptsRaw = m['attempts'] as List;
    final attempts = List<AttemptResult>.unmodifiable(
      attemptsRaw.map((j) => AttemptResult.fromJson(_asJsonMap(j))),
    );
    final wirePayloadRaw = m['wire_payload'];
    final envelopeRaw = m['envelope_metadata'];
    final finalStatusRaw = m['final_status'] as String?;
    return FifoEntry(
      entryId: m['entry_id'] as String,
      eventIds: List<String>.unmodifiable(eventIds),
      sequenceRange: (
        firstSeq: m['event_id_first_seq'] as int,
        lastSeq: m['event_id_last_seq'] as int,
      ),
      sequenceInQueue: m['sequence_in_queue'] as int,
      wirePayload: wirePayloadRaw == null
          ? null
          : Map<String, Object?>.unmodifiable(_asJsonMap(wirePayloadRaw)),
      wireFormat: m['wire_format'] as String,
      transformVersion: m['transform_version'] as String?,
      enqueuedAt: (m['enqueued_at'] as DateTime).toUtc(),
      attempts: attempts,
      finalStatus: finalStatusRaw == null
          ? null
          : FinalStatus.fromJson(finalStatusRaw),
      sentAt: (m['sent_at'] as DateTime?)?.toUtc(),
      envelopeMetadata: envelopeRaw == null
          ? null
          : BatchEnvelopeMetadata.fromMap(_asJsonMap(envelopeRaw)),
    );
  }

  /// Coerce a JSONB column value (returned by the postgres v3.5 driver
  /// as a decoded Dart map) into the `Map<String, dynamic>` shape used
  /// across the substrate's storage surface. Returns an empty map when
  /// the column is null — this matches the substrate convention that
  /// "no metadata" is `{}` rather than `null` (`StoredEvent.fromMap`
  /// applies the same convention on the sembast path).
  static Map<String, dynamic> _asJsonMap(Object? raw) {
    if (raw == null) return <String, dynamic>{};
    if (raw is Map<String, dynamic>) return raw;
    if (raw is Map) return Map<String, dynamic>.from(raw);
    throw StateError(
      'PostgresBackend: expected JSON map for JSONB column; '
      'got ${raw.runtimeType}',
    );
  }
}

/// Compose a `WHERE` clause from a list of predicate fragments.
///
/// Each fragment is an SQL predicate (e.g., `sequence_number > @afterSeq`)
/// whose placeholders are bound by the caller through the `parameters:`
/// map. Returns an empty string when [fragments] is empty.
String _composeWhere(List<String> fragments) {
  if (fragments.isEmpty) return '';
  return 'WHERE ${fragments.join(' AND ')}';
}

/// Opaque pagination cursor for [PostgresBackend.queryAudit]. Encodes
/// the `(recorded_at, event_id)` tuple from the previous page's tail
/// row; the next page is a strict lower bound under the
/// `recorded_at DESC, event_id DESC` ordering so concurrent inserts at
/// the head do not skew page contents. Encoding is base64-url of
/// `"<isoTimestamp>|<eventId>"`. Pipe is illegal in both ISO 8601
/// timestamps and UUIDs so the split is unambiguous. The shape parallels
/// `_AuditCursorPoint` in `sembast_backend.dart` so a cursor minted on
/// one backend is recognizable on the other; the contract does not
/// require this round-trip but tests do not exercise inter-backend
/// cursor portability either.
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

/// An entry-type version read from its two columns, through the strict
/// parser, so a stored value out of range is refused on read as the
/// Sembast backend refuses it.
EntryTypeVersion _entryTypeVersionOf(Object? major, Object? minor) =>
    EntryTypeVersion.fromJson(<String, Object?>{
      'major': major,
      'minor': minor,
    });

/// A data-format version read from its two columns, through the strict
/// parser.
DataFormatVersion _dataFormatVersionOf(Object? major, Object? minor) =>
    DataFormatVersion.fromJson(<String, Object?>{
      'major': major,
      'minor': minor,
    });
