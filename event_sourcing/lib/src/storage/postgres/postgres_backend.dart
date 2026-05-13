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
// Implements: EVS-DEV-postgres-backend/B — view rows persisted as JSONB
//   blobs in a single view_rows(view_name, row_key, row_data JSONB,
//   updated_at) table with primary key (view_name, row_key).
// Implements: EVS-PRD-destinations — FIFO-queue surface: per-destination
//   monotone sequence_in_queue, head behavior, attempt log, final-status
//   transitions, wedged-FIFO summary, trail-sweep delete, full FIFO
//   delete on destination teardown.

import 'dart:async';
import 'dart:convert';
import 'dart:developer' as developer;

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
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
import 'package:uuid/uuid.dart';

/// Module-private v4 UUID generator used by [PostgresBackend.enqueueFifoTxn]
/// to mint each FIFO row's [FifoEntry.entryId]. Held at file scope so every
/// backend instance shares one generator; `Uuid.v4()` is side-effect-free
/// beyond its internal random state. Parallels the sembast backend's
/// module-private `_uuidGen` to keep the two impls structurally aligned.
const _uuidGen = Uuid();

/// Default warning-level diagnostic sink used by [PostgresBackend] when a
/// FIFO mutation no-ops on a missing row. Routes through `dart:developer`
/// at level 900 (matches sembast's `_defaultLogSink`).
void _defaultLogSink(String message) {
  developer.log(message, name: 'PostgresBackend', level: 900);
}

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

  /// Override the default warning-level diagnostic sink used by FIFO
  /// methods that no-op on a missing target row ([appendAttempt],
  /// [markFinal]). Defaults to a `dart:developer` log call at level 900;
  /// tests may swap this for a buffer or null-sink to assert on the
  /// no-op-warning behavior. Parallels `SembastBackend.debugLogSink`.
  // Implements: EVS-PRD-destinations — appendAttempt/markFinal emit a
  //   warning-level diagnostic when the target row is absent (drain/unjam
  //   or drain/delete race tolerance).
  void Function(String) logSink = _defaultLogSink;

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

  // Implements: EVS-DEV-postgres-backend/B — read a single JSONB blob from
  //   view_rows; returns null when the (view_name, row_key) pair is absent.
  @override
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Txn txn,
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

  // Implements: EVS-DEV-postgres-backend/B — whole-row upsert via
  //   INSERT … ON CONFLICT (view_name, row_key) DO UPDATE.
  @override
  Future<void> upsertViewRowInTxn(
    Txn txn,
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

  // Implements: EVS-DEV-postgres-backend/B — delete a single row from
  //   view_rows by (view_name, row_key); no-op when absent.
  @override
  Future<void> deleteViewRowInTxn(Txn txn, String viewName, String key) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_rows WHERE view_name = @v AND row_key = @k'),
      parameters: {'v': viewName, 'k': key},
    );
  }

  // Implements: EVS-DEV-postgres-backend/B — list all rows for a view in
  //   deterministic row_key ASC order; optional LIMIT/OFFSET for paging.
  @override
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) async {
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

  // Implements: EVS-DEV-postgres-backend/B — delete all rows for a view
  //   without touching other views (WHERE view_name = @v).
  @override
  Future<void> clearViewInTxn(Txn txn, String viewName) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_rows WHERE view_name = @v'),
      parameters: {'v': viewName},
    );
  }

  // -------- Task 8: view target versions --------

  // Implements: EVS-DEV-postgres-backend/D — backend passes the conformance
  //   harness; readViewTargetVersionInTxn reads a single row from the
  //   view_target_versions(view_name, entry_type, target_version) table and
  //   returns null when the (view_name, entry_type) pair is absent.
  @override
  Future<int?> readViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT target_version FROM view_target_versions
        WHERE view_name = @v AND entry_type = @et
        LIMIT 1
      '''),
      parameters: {'v': viewName, 'et': entryType},
    );
    return result.isEmpty ? null : result.first[0] as int;
  }

  // Implements: EVS-DEV-postgres-backend/D — backend passes the conformance
  //   harness; writeViewTargetVersionInTxn upserts via INSERT … ON CONFLICT
  //   DO UPDATE so repeated writes for the same (view_name, entry_type) pair
  //   reflect the latest target_version value.
  @override
  Future<void> writeViewTargetVersionInTxn(
    Txn txn,
    String viewName,
    String entryType,
    int targetVersion,
  ) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('''
        INSERT INTO view_target_versions (view_name, entry_type, target_version)
        VALUES (@v, @et, @tv)
        ON CONFLICT (view_name, entry_type)
        DO UPDATE SET target_version = EXCLUDED.target_version
      '''),
      parameters: {'v': viewName, 'et': entryType, 'tv': targetVersion},
    );
  }

  // Implements: EVS-DEV-postgres-backend/D — backend passes the conformance
  //   harness; readAllViewTargetVersionsInTxn returns all (entry_type →
  //   target_version) pairs for the given view_name as a Map<String, int>.
  @override
  Future<Map<String, int>> readAllViewTargetVersionsInTxn(
    Txn txn,
    String viewName,
  ) async {
    final session = _asPgTxn(txn).session;
    final result = await session.execute(
      Sql.named('''
        SELECT entry_type, target_version FROM view_target_versions
        WHERE view_name = @v
      '''),
      parameters: {'v': viewName},
    );
    return {for (final row in result) row[0] as String: row[1] as int};
  }

  // Implements: EVS-DEV-postgres-backend/D — backend passes the conformance
  //   harness; clearViewTargetVersionsInTxn deletes all rows for the given
  //   view_name without touching rows belonging to other views.
  @override
  Future<void> clearViewTargetVersionsInTxn(Txn txn, String viewName) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM view_target_versions WHERE view_name = @v'),
      parameters: {'v': viewName},
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

  /// Standalone enqueue: opens this backend's own atomic transaction and
  /// delegates row construction to [enqueueFifoTxn]. Callers composing a
  /// larger transaction (e.g., `fillBatch` advancing fill_cursor) SHALL use
  /// [enqueueFifoTxn] directly.
  // Implements: EVS-PRD-destinations — standalone enqueue path; opens
  //   its own transaction so the row write is atomic for callers that
  //   aren't already inside one.
  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) {
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

  /// Centralized row construction for the FIFO enqueue path. Both
  /// [enqueueFifo] (which wraps this in its own transaction) and callers
  /// already composing a transaction (fillBatch, runHistoricalReplay)
  /// route through here so all the contract enforcement — empty-batch
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
  ///   `wire_format = 'esd/batch@1'`; `transform_version = null`.
  // Implements: EVS-PRD-destinations — empty batch rejected with
  //   ArgumentError; XOR(wirePayload, nativeEnvelope) enforced; v4 UUID
  //   entry_id minted; sequence_in_queue assigned monotone via
  //   per-destination counter in backend_state; row persisted with all
  //   contract fields.
  @override
  Future<FifoEntry> enqueueFifoTxn(
    Txn txn,
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
    // XOR: exactly one payload shape is legal. Reject both null and both
    // non-null at the boundary so a downstream FIFO row never carries an
    // ambiguous (wire_payload, envelope_metadata) pair.
    if ((wirePayload == null) == (nativeEnvelope == null)) {
      throw ArgumentError(
        'enqueueFifo requires exactly one of wirePayload or nativeEnvelope '
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
        SET value = ((value::text::int) + 1)::text::jsonb
        WHERE key = @k
        RETURNING value::text::int
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
      eventIdRange: (firstSeq: firstSeq, lastSeq: lastSeq),
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

  // Implements: EVS-PRD-destinations — readFifoHead returns the first row
  //   in sequence_in_queue order whose final_status is null OR 'wedged';
  //   sent and tombstoned rows are skipped. Returns null on empty FIFO.
  //   Uses the partial `fifo_entries_head_idx` for an index-only scan.
  @override
  Future<FifoEntry?> readFifoHead(String destinationId) async {
    final result = await _pool.execute(
      Sql.named('''
        SELECT * FROM fifo_entries
        WHERE destination_id = @dest
          AND (final_status IS NULL OR final_status = 'wedged')
        ORDER BY sequence_in_queue ASC
        LIMIT 1
      '''),
      parameters: {'dest': destinationId},
    );
    return result.isEmpty ? null : _fifoEntryFromRow(result.first);
  }

  // Implements: EVS-PRD-destinations — listFifoEntries enumerates rows
  //   in sequence_in_queue ASC; afterSequenceInQueue is exclusive; limit
  //   caps from the start of the ordered range. Empty list on unknown
  //   destination (no rows match the WHERE clause).
  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) async {
    final wheres = <String>['destination_id = @dest'];
    final params = <String, Object?>{'dest': destinationId};
    if (afterSequenceInQueue != null) {
      wheres.add('sequence_in_queue > @afterSeq');
      params['afterSeq'] = afterSequenceInQueue;
    }
    final limitClause = limit == null ? '' : 'LIMIT $limit';
    final sql =
        'SELECT * FROM fifo_entries WHERE ${wheres.join(' AND ')} '
        'ORDER BY sequence_in_queue ASC $limitClause';
    final result = await _pool.execute(Sql.named(sql), parameters: params);
    return result.map(_fifoEntryFromRow).toList(growable: false);
  }

  /// Append [attempt] to the entry's `attempts[]` JSONB array. JSONB
  /// concatenation via `||` requires the right operand to be a JSONB
  /// array; we pass `[attempt.toJson()]` so the driver encodes a
  /// single-element array which gets concatenated onto the existing
  /// attempts list.
  // Implements: EVS-PRD-destinations — appendAttempt no-ops with a
  //   warning when the target row is absent (drain/unjam race
  //   tolerance). Postgres collapses the sembast distinction between
  //   "missing row" and "missing FIFO store" — both surface as zero
  //   affected rows on the same WHERE clause.
  @override
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) async {
    final result = await _pool.execute(
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
    if (result.affectedRows == 0) {
      logSink(
        'appendAttempt: entry $entryId absent from FIFO $destinationId; '
        'skipping (expected during drain/unjam or drain/delete race)',
      );
    }
  }

  // Implements: EVS-PRD-destinations — markFinal:
  //   - no-op + warning when target row absent;
  //   - idempotent return when row already final with matching status;
  //   - StateError naming both statuses on mismatched already-final;
  //   - null -> sent stamps sent_at = NOW().toUtc().
  @override
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  ) async {
    await transaction<void>((txn) async {
      final session = _asPgTxn(txn).session;
      final existing = await session.execute(
        Sql.named('''
          SELECT final_status FROM fifo_entries
          WHERE destination_id = @dest AND entry_id = @e
        '''),
        parameters: {'dest': destinationId, 'e': entryId},
      );
      if (existing.isEmpty) {
        logSink(
          'markFinal: entry $entryId absent from FIFO $destinationId; '
          'skipping (expected during drain/unjam or drain/delete race)',
        );
        return;
      }
      final currentRaw = existing.first[0] as String?;
      // final_status transitions are one-way. Duplicate call with the
      // SAME status is a no-op (at-least-once drain race-closer);
      // mismatched status is real corruption — loud failure.
      if (currentRaw != null) {
        if (currentRaw == status.name) return;
        throw StateError(
          'markFinal($destinationId, $entryId, ${status.name}): entry is '
          'already $currentRaw; final_status transitions are one-way.',
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
    });
  }

  // Implements: EVS-PRD-destinations — anyFifoWedged true iff any
  //   destination's head row (first sequence_in_queue with
  //   final_status IN {null, wedged}) is wedged. Single SQL pass via
  //   DISTINCT ON (destination_id) so we visit each FIFO's head row in
  //   one scan, then filter to wedged.
  @override
  Future<bool> anyFifoWedged() async {
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

  // Implements: EVS-PRD-destinations — wedgedFifos returns one summary
  //   per wedged FIFO. headEventId is the first event_id on the wedged
  //   head row; wedgedAt = last attempt's attemptedAt (or enqueued_at
  //   when no attempts recorded); lastError = last attempt's
  //   error_message (or fallback string when none).
  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() async {
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
          // Wedged-with-no-attempts is rare but legal (e.g., manual
          // setFinalStatusTxn(wedged) bypassing drain); the summary
          // surfaces enqueued_at + a placeholder error string so
          // operators can identify the row without a separate code path.
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

  // Implements: EVS-PRD-destinations — readFifoRow looks up a single row
  //   by (destination_id, entry_id); returns null when absent. Used by
  //   tooling/tests to inspect a specific row.
  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) async {
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

  // Implements: EVS-PRD-destinations — setFinalStatusTxn enforces legal
  //   transitions: {null -> sent | wedged | tombstoned, wedged ->
  //   tombstoned}. Throws StateError on illegal transitions and on
  //   missing rows (caller is expected to have verified existence via
  //   readFifoHead before opening the txn). On null -> sent stamps
  //   sent_at; on every other legal transition attempts[] and sent_at
  //   are left untouched (tombstoneAndRefill preserves attempts[]
  //   verbatim).
  @override
  Future<void> setFinalStatusTxn(
    Txn txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
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
        'setFinalStatusTxn($destinationId, $entryId, $status): target '
        'row not found. Callers must verify existence (readFifoHead) '
        'before opening the transaction; a missing row here indicates '
        'a concurrent delete race.',
      );
    }
    final currentRaw = existing.first[0] as String?;
    final current = currentRaw == null
        ? null
        : FinalStatus.fromJson(currentRaw);
    // Legal transitions:
    //  - null   -> sent          (drain SendOk)
    //  - null   -> wedged        (drain SendPermanent / max-attempts)
    //  - null   -> tombstoned    (tombstoneAndRefill on null head)
    //  - wedged -> tombstoned    (tombstoneAndRefill on wedged head)
    // sent/tombstoned are terminal end-states; status=null target is
    // not legal via this method.
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
        'tombstoned}; wedged -> {tombstoned}. (one-way rule.)',
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
          's': status!.name,
          't': DateTime.now().toUtc(),
          'dest': destinationId,
          'e': entryId,
        },
      );
    } else {
      // wedged / tombstoned: stamp final_status, leave sent_at and
      // attempts[] untouched. attempts[] preservation is load-bearing
      // for tombstoneAndRefill.
      await session.execute(
        Sql.named('''
          UPDATE fifo_entries
          SET final_status = @s
          WHERE destination_id = @dest AND entry_id = @e
        '''),
        parameters: {'s': status!.name, 'dest': destinationId, 'e': entryId},
      );
    }
  }

  // Implements: EVS-PRD-destinations — trail-sweep DELETE used by
  //   tombstoneAndRefill: removes rows whose sequence_in_queue is
  //   strictly greater than [afterSequenceInQueue] AND whose
  //   final_status IS null. Terminal rows are retained forever as
  //   audit records and never touched here. Returns the count of rows
  //   deleted (via the postgres driver's affectedRows).
  @override
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Txn txn,
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
      '''),
      parameters: {'dest': destinationId, 'afterSeq': afterSequenceInQueue},
    );
    return result.affectedRows;
  }

  // Implements: EVS-PRD-destinations — drop the FIFO store for
  //   [destinationId] entirely (used by `deleteDestination`).
  //   Postgres has no per-destination store, so we delete every row
  //   under the destination_id discriminator AND the persisted
  //   sequence_in_queue counter so a subsequent re-use of the same
  //   id starts at 1 again. (Sembast achieves the same by dropping the
  //   per-destination store; the counter on sembast is wiped via
  //   `backend_state` deletion.)
  @override
  Future<void> deleteFifoStoreTxn(Txn txn, String destinationId) async {
    final session = _asPgTxn(txn).session;
    await session.execute(
      Sql.named('DELETE FROM fifo_entries WHERE destination_id = @dest'),
      parameters: {'dest': destinationId},
    );
    await session.execute(
      Sql.named('DELETE FROM backend_state WHERE key = @k'),
      parameters: {'k': _fifoSeqCounterKey(destinationId)},
    );
  }

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

  /// Key under which a per-destination FIFO sequence_in_queue counter is
  /// persisted in `backend_state`. Sembast uses the same key shape; the
  /// aligned naming makes a cross-backend audit trivial.
  static String _fifoSeqCounterKey(String destinationId) =>
      'fifo_seq_counter_$destinationId';

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

  /// Reify a Postgres `fifo_entries` row into a [FifoEntry]. JSONB
  /// columns are returned by the driver as already-decoded Dart
  /// maps/lists; TIMESTAMPTZ columns as `DateTime` in UTC. The shape
  /// matches the contract enforced by [FifoEntry]'s constructor:
  /// `eventIds` non-empty and `eventIdRange.firstSeq <= lastSeq`. The
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
      eventIdRange: (
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
