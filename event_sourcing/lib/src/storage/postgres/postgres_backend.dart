// Implements: EVS-PRD-portability/D — second concrete StorageBackend impl
//   alongside SembastBackend; selectable per deployment with no caller
//   changes (the contract is Dart-pure).
// Implements: EVS-DEV-postgres-backend/A — `PostgresBackend.open` connects
//   and emits `CREATE TABLE IF NOT EXISTS` DDL for every table the backend
//   uses; re-open against a provisioned database is a no-op on the schema.

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

  @override
  Future<T> transaction<T>(Future<T> Function(Txn txn) body) =>
      throw UnimplementedError('PostgresBackend.transaction — Task 5');

  // -------- Task 6: event log --------

  @override
  Future<AppendResult> appendEvent(Txn txn, StoredEvent event) =>
      throw UnimplementedError('PostgresBackend.appendEvent — Task 6');

  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) =>
      throw UnimplementedError(
        'PostgresBackend.findEventsForAggregate — Task 6',
      );

  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Txn txn,
    String aggregateId,
  ) => throw UnimplementedError(
    'PostgresBackend.findEventsForAggregateInTxn — Task 6',
  );

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) => throw UnimplementedError('PostgresBackend.findAllEvents — Task 6');

  @override
  Future<String?> readLatestEventHash(Txn txn) =>
      throw UnimplementedError('PostgresBackend.readLatestEventHash — Task 6');

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Txn txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) => throw UnimplementedError('PostgresBackend.findAllEventsInTxn — Task 6');

  @override
  Future<int> nextSequenceNumber(Txn txn) =>
      throw UnimplementedError('PostgresBackend.nextSequenceNumber — Task 6');

  @override
  Future<int> readSequenceCounter() =>
      throw UnimplementedError('PostgresBackend.readSequenceCounter — Task 6');

  @override
  Future<StoredEvent?> findEventByIdInTxn(Txn txn, String eventId) =>
      throw UnimplementedError('PostgresBackend.findEventByIdInTxn — Task 6');

  @override
  Future<StoredEvent?> findEventById(String eventId) =>
      throw UnimplementedError('PostgresBackend.findEventById — Task 6');

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
}
