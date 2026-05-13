// Implements: EVS-PRD-portability/D — second concrete StorageBackend impl
//   alongside SembastBackend; selectable per deployment with no caller
//   changes (the contract is Dart-pure).
// Implements: EVS-DEV-postgres-backend/A — `PostgresBackend.open` connects
//   and emits `CREATE TABLE IF NOT EXISTS` DDL for every table the backend
//   uses; re-open against a provisioned database is a no-op on the schema.
// Implements: EVS-PRD-event-log/A,B,C,D — event-log surface: append-only;
//   stable total order via sequence counter (reserve-and-increment);
//   per-aggregate order isolated by aggregate_id; read events in order
//   from any starting position; findEventById/-InTxn lookups.
// Implements: EVS-DEV-find-all-events-extended-filters/A,B,C,D — entryType,
//   clientTimestampStart, clientTimestampEnd filters AND-compose with
//   afterSequence/limit/originator filters; both in-txn and out-of-txn
//   variants share a single composition helper (_findAllEventsComposed).

import 'dart:async';

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:postgres/postgres.dart';

/// Concrete Postgres-backed implementation of [StorageBackend].
///
/// Task 4 (schema skeleton): only `open()`, `pool`, and `close()` are
/// functional. Every other method throws [UnimplementedError] with a
/// pointer to the task that lands it (Tasks 5–11). Subsequent tasks
/// fill in transactions (Task 5), event log (Task 6), view rows
/// (Task 7), view target versions (Task 8), FIFO (Task 9), backend
/// state (Task 10), and reverse scan / audit query (Task 11).
class PostgresBackend extends StorageBackend {
  PostgresBackend._(this._pool);

  final Pool<void> _pool;

  /// Open against [url]. Connects, emits the schema DDL (idempotent on
  /// re-open), and returns a ready backend. Callers MUST call [close]
  /// to release the connection pool.
  ///
  /// Example: `postgres://user:pass@host:5432/db`.
  ///
  /// `SslMode.disable` is hard-wired in this skeleton because the first
  /// deployment target is local-only (docker-compose Postgres on the
  /// developer's machine and the future CI runner). Productionizing
  /// against a remote Postgres will need to plumb [SslMode] through
  /// here; that's deliberately out of scope until a deployment requires
  /// it.
  // Implements: EVS-DEV-postgres-backend/A — connects and emits the schema
  //   DDL on every open; idempotent on re-open against a provisioned db.
  static Future<PostgresBackend> open({required String url}) async {
    final endpoint = _endpointFromUrl(url);
    final pool = Pool<void>.withEndpoints(
      [endpoint],
      settings: const PoolSettings(
        maxConnectionCount: 4,
        sslMode: SslMode.disable,
      ),
    );
    final backend = PostgresBackend._(pool);
    await pool.runTx(ensurePostgresSchema);
    return backend;
  }

  static Endpoint _endpointFromUrl(String url) {
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

  // Expose the underlying [Pool] so future subsystems (e.g.,
  // PostgresIdempotencyStore) can share connections without
  // re-parsing the URL. Internal; not part of the public API surface.
  Pool<void> get pool => _pool;

  @override
  Future<void> close() => _pool.close();

  // ------------------------------------------------------------------
  // Stubs. Implementations land in Tasks 5–11. Each body throws
  // UnimplementedError with the task number that owns it so failing
  // tests in the meantime point at the next piece of work.
  // ------------------------------------------------------------------

  // -------- Task 5: transactions --------

  // Implements: EVS-PRD-event-log/A — successful body commits atomically;
  //   thrown exception rolls back. Postgres SERIALIZABLE isolation prevents
  //   the per-device sequence counter from being read+written by concurrent
  //   transactions.
  // Implements: EVS-DEV-postgres-backend/C — Txn handle invalidated after
  //   body returns or throws.
  @override
  Future<T> transaction<T>(Future<T> Function(Txn txn) body) async {
    return _pool.runTx<T>(
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
  // Implements: EVS-PRD-event-log/A — persists event to append-only log
  //   atomically inside the supplied transaction.
  // Implements: EVS-PRD-event-log/B — sequence number stamped by caller
  //   from nextSequenceNumber; persisted verbatim preserving total order;
  //   advance owned by nextSequenceNumber, not appendEvent.
  @override
  Future<AppendResult> appendEvent(Txn txn, StoredEvent event) async {
    final session = _asPgTxn(txn).session;
    // Validate the reservation: the persisted counter must equal the seq
    // the caller is consuming. Reading the counter inside the same txn
    // sees the value staged by nextSequenceNumber.
    final reservedResult = await session.execute(
      Sql.named('''
        SELECT value::text::int FROM backend_state
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
          entry_type_version, lib_format_version, event_type,
          data, metadata, initiator,
          client_timestamp, event_hash, flow_token, previous_event_hash
        ) VALUES (
          @seq, @eventId, @aggId, @aggType, @entryType,
          @entryTypeV, @libFmtV, @eventType,
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
        'entryTypeV': event.entryTypeVersion,
        'libFmtV': event.libFormatVersion,
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

  // Implements: EVS-PRD-event-log/C — events for a single aggregate are
  //   returned in sequence_number order; the aggregate_id index keeps the
  //   lookup O(log n + k).
  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) async {
    final result = await _pool.execute(
      Sql.named(
        'SELECT * FROM events WHERE aggregate_id = @aggId '
        'ORDER BY sequence_number ASC',
      ),
      parameters: {'aggId': aggregateId},
    );
    return result.map(_storedEventFromRow).toList(growable: false);
  }

  // Implements: EVS-PRD-event-log/C — transactional variant; reads inside
  //   the same txn see writes staged in the same body (read-your-writes).
  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Txn txn,
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

  // Implements: EVS-PRD-event-log/D — read all events in sequence order
  //   from any starting position (afterSequence + limit).
  // Implements: EVS-DEV-find-all-events-extended-filters/A,C — entryType,
  //   clientTimestampStart, clientTimestampEnd filters AND-compose with
  //   afterSequence, limit, originatorHopId, originatorIdentifier.
  // Implements: EVS-DEV-find-all-events-extended-filters/D — single shared
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
  }) => _findAllEventsComposed(
    afterSequence: afterSequence,
    limit: limit,
    originatorHopId: originatorHopId,
    originatorIdentifier: originatorIdentifier,
    entryType: entryType,
    clientTimestampStart: clientTimestampStart,
    clientTimestampEnd: clientTimestampEnd,
  );

  // Implements: EVS-PRD-event-log/D — transactional variant; reads see
  //   writes staged in the same txn body.
  // Implements: EVS-DEV-find-all-events-extended-filters/B,C,D — same
  //   three filters with same AND-composition semantics; shared helper.
  //
  // `async` (not arrow) so that the synchronous `_asPgTxn(txn).session`
  // check — which throws StateError on a post-body escape or a foreign
  // Txn — completes the returned Future with the error rather than
  // throwing synchronously past the caller's `await`. The conformance
  // harness' `throwsStateError` matcher awaits the Future, so a
  // synchronous throw at call-site evaluation would short-circuit it.
  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Txn txn, {
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
  // Implements: EVS-DEV-find-all-events-extended-filters/D — single shared
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
    final whereClause = wheres.isEmpty ? '' : 'WHERE ${wheres.join(' AND ')}';
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final sql =
        'SELECT * FROM events $whereClause '
        'ORDER BY sequence_number ASC $limitClause';
    final exec = session ?? _pool;
    final result = await exec.execute(Sql.named(sql), parameters: params);
    return result.map(_storedEventFromRow).toList(growable: false);
  }

  // Implements: EVS-PRD-event-log/A — readLatestEventHash is transactional;
  //   value reflects writes staged in the same txn so a caller can build
  //   the next event's previous_event_hash atomically with the append that
  //   uses it.
  @override
  Future<String?> readLatestEventHash(Txn txn) async {
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
  // Implements: EVS-PRD-event-log/B — monotonic per-transaction reserve.
  @override
  Future<int> nextSequenceNumber(Txn txn) async {
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
        SET value = ((value::text::int) + 1)::text::jsonb
        WHERE key = @k
        RETURNING value::text::int
      '''),
      parameters: {'k': _sequenceCounterKey},
    );
    return result.first[0] as int;
  }

  // Implements: EVS-PRD-event-log/B — counter is readable outside any txn
  //   for diagnostics; returns 0 when the row has never been materialized.
  @override
  Future<int> readSequenceCounter() async {
    final result = await _pool.execute(
      Sql.named('SELECT value::text::int FROM backend_state WHERE key = @k'),
      parameters: {'k': _sequenceCounterKey},
    );
    return result.isEmpty ? 0 : result.first[0] as int;
  }

  // Implements: EVS-PRD-event-log/D — single-event lookup by event_id
  //   inside the supplied transaction; returns null when absent.
  @override
  Future<StoredEvent?> findEventByIdInTxn(Txn txn, String eventId) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
      parameters: {'id': eventId},
    );
    return result.isEmpty ? null : _storedEventFromRow(result.first);
  }

  // Implements: EVS-PRD-event-log/D — single-event lookup by event_id
  //   outside any transaction; returns null when absent.
  @override
  Future<StoredEvent?> findEventById(String eventId) async {
    final result = await _pool.execute(
      Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
      parameters: {'id': eventId},
    );
    return result.isEmpty ? null : _storedEventFromRow(result.first);
  }

  // -------- Task 7: view rows --------

  @override
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Txn txn,
    String viewName,
    String key,
  ) => throw UnimplementedError('PostgresBackend.readViewRowInTxn — Task 7');

  @override
  Future<void> upsertViewRowInTxn(
    Txn txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  ) => throw UnimplementedError('PostgresBackend.upsertViewRowInTxn — Task 7');

  @override
  Future<void> deleteViewRowInTxn(Txn txn, String viewName, String key) =>
      throw UnimplementedError('PostgresBackend.deleteViewRowInTxn — Task 7');

  @override
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) => throw UnimplementedError('PostgresBackend.findViewRows — Task 7');

  @override
  Future<void> clearViewInTxn(Txn txn, String viewName) =>
      throw UnimplementedError('PostgresBackend.clearViewInTxn — Task 7');

  // -------- Task 8: view target versions --------

  @override
  Future<int?> readViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
  ) => throw UnimplementedError(
    'PostgresBackend.readViewTargetVersionInTxn — Task 8',
  );

  @override
  Future<void> writeViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
    int targetVersion,
  ) => throw UnimplementedError(
    'PostgresBackend.writeViewTargetVersionInTxn — Task 8',
  );

  @override
  Future<Map<String, int>> readAllViewTargetVersionsInTxn(
    Txn txn,
    String viewName,
  ) => throw UnimplementedError(
    'PostgresBackend.readAllViewTargetVersionsInTxn — Task 8',
  );

  @override
  Future<void> clearViewTargetVersionsInTxn(Txn txn, String viewName) =>
      throw UnimplementedError(
        'PostgresBackend.clearViewTargetVersionsInTxn — Task 8',
      );

  // -------- Task 9: FIFO --------

  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => throw UnimplementedError('PostgresBackend.enqueueFifo — Task 9');

  @override
  Future<FifoEntry> enqueueFifoTxn(
    Txn txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => throw UnimplementedError('PostgresBackend.enqueueFifoTxn — Task 9');

  @override
  Future<FifoEntry?> readFifoHead(String destinationId) =>
      throw UnimplementedError('PostgresBackend.readFifoHead — Task 9');

  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) => throw UnimplementedError('PostgresBackend.listFifoEntries — Task 9');

  @override
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) => throw UnimplementedError('PostgresBackend.appendAttempt — Task 9');

  @override
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  ) => throw UnimplementedError('PostgresBackend.markFinal — Task 9');

  @override
  Future<bool> anyFifoWedged() =>
      throw UnimplementedError('PostgresBackend.anyFifoWedged — Task 9');

  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() =>
      throw UnimplementedError('PostgresBackend.wedgedFifos — Task 9');

  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) =>
      throw UnimplementedError('PostgresBackend.readFifoRow — Task 9');

  @override
  Future<void> setFinalStatusTxn(
    Txn txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
  ) => throw UnimplementedError('PostgresBackend.setFinalStatusTxn — Task 9');

  @override
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Txn txn,
    String destinationId,
    int afterSequenceInQueue,
  ) => throw UnimplementedError(
    'PostgresBackend.deleteNullRowsAfterSequenceInQueueTxn — Task 9',
  );

  @override
  Future<void> deleteFifoStoreTxn(Txn txn, String destinationId) =>
      throw UnimplementedError('PostgresBackend.deleteFifoStoreTxn — Task 9');

  // -------- Task 10: backend_state (schema version, fill cursor, schedule) --------

  @override
  Future<int> readSchemaVersion() =>
      throw UnimplementedError('PostgresBackend.readSchemaVersion — Task 10');

  @override
  Future<void> writeSchemaVersion(Txn txn, int version) =>
      throw UnimplementedError('PostgresBackend.writeSchemaVersion — Task 10');

  @override
  Future<int> readFillCursor(String destinationId) =>
      throw UnimplementedError('PostgresBackend.readFillCursor — Task 10');

  @override
  Future<void> writeFillCursor(String destinationId, int sequenceNumber) =>
      throw UnimplementedError('PostgresBackend.writeFillCursor — Task 10');

  @override
  Future<void> writeFillCursorTxn(
    Txn txn,
    String destinationId,
    int sequenceNumber,
  ) => throw UnimplementedError('PostgresBackend.writeFillCursorTxn — Task 10');

  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) =>
      throw UnimplementedError('PostgresBackend.readSchedule — Task 10');

  @override
  Future<void> writeSchedule(
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError('PostgresBackend.writeSchedule — Task 10');

  @override
  Future<void> writeScheduleTxn(
    Txn txn,
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError('PostgresBackend.writeScheduleTxn — Task 10');

  @override
  Future<void> deleteScheduleTxn(Txn txn, String destinationId) =>
      throw UnimplementedError('PostgresBackend.deleteScheduleTxn — Task 10');

  // -------- Task 11: reverse scan + audit query --------

  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) =>
      throw UnimplementedError('PostgresBackend.readEventsReverse — Task 11');

  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => throw UnimplementedError('PostgresBackend.queryAudit — Task 11');

  // ------------------------------------------------------------------
  // Internal helpers
  // ------------------------------------------------------------------

  /// Key under which the per-install sequence counter is persisted in
  /// the `backend_state` KV row. Sembast uses the same string for the
  /// same purpose; keeping the key name aligned makes a cross-backend
  /// audit easier (`select * from backend_state where key = 'sequence_counter'`
  /// works on Postgres and parallels the sembast record key).
  static const String _sequenceCounterKey = 'sequence_counter';

  /// Downcast a [Txn] handed to this backend's StorageBackend methods
  /// into the concrete [PostgresTxn]. Any other concrete subtype indicates
  /// the caller mixed two different backends' Txn handles — that's a bug,
  /// not a recoverable state, so we surface it as `StateError`.
  ///
  /// The `session` getter on a valid [PostgresTxn] in turn throws
  /// `StateError` when the surrounding transaction body has already
  /// returned (the handle was invalidated). Either failure mode produces
  /// the same outward shape, which matches the conformance harness'
  /// `throwsStateError` expectations for both "foreign Txn" and
  /// "post-body escape" cases.
  PostgresTxn _asPgTxn(Txn txn) {
    if (txn is! PostgresTxn) {
      throw StateError(
        'PostgresBackend: Txn was produced by a different StorageBackend '
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
      entryTypeVersion: m['entry_type_version'] as int,
      libFormatVersion: m['lib_format_version'] as int,
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
