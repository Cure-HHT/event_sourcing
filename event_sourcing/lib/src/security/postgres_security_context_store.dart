// Implements: EVS-PRD-event-log/A
// all mutations accept a caller-supplied
//   `Transaction` so they commit atomically with the event-log row they describe.
//   Postgres-side, the [Transaction] passed in is a [_PostgresTxn] holding the
//   `TxSession` opened by [PostgresBackend.transaction]; writes routed
//   through `txn._session` therefore live inside the same transaction as
//   the `events` row produced by [PostgresBackend.appendEvent].
// Implements: EVS-PRD-regulatory-alignment
// `findUnredactedOlderThanInTxn`
//   and `findOlderThanInTxn` drive the retention compact/purge sweeps that
//   satisfy ALCOA+ Enduring / §11.10(c) protection-of-records obligations.
// Implements: EVS-DEV-postgres-backend/D
// fills the second SecurityContext
//   sidecar implementation needed for the Postgres backend to support
//   the substrate's full action-dispatch path. `PostgresBackend.provision`
//   creates the `security_context` table with the rest of the schema.

part of '../storage/postgres/postgres_backend.dart';

/// Postgres-backed `SecurityContextStore`. Persists one row per event in
/// the `security_context` table; `payload` is the full
/// [EventSecurityContext] JSON shape, with `recorded_at` and `ip_address`
/// promoted to dedicated columns for filtering and ORDER BY support.
///
/// Cross-store reads (the security_context + events join) live on the
/// backend via [PostgresBackend.queryAudit]; this store's [queryAudit]
/// is a thin delegator. Mutations live here so they can run in the same
/// transaction as the matching [PostgresBackend.appendEvent].
class PostgresSecurityContextStore extends MutableSecurityContextStore {
  /// A store over [backend]: the event store builds one over the storage
  /// it opens, and an application builds one for a backend it constructed
  /// and names as application-supplied storage.
  PostgresSecurityContextStore({required PostgresBackend backend})
    : _backend = backend;

  final PostgresBackend _backend;

  @override
  Future<EventSecurityContext?> read(String eventId) {
    return _backend.transaction((txn) => readInTxn(txn, eventId));
  }

  @override
  Future<EventSecurityContext?> readInTxn(
    Transaction txn,
    String eventId,
  ) async {
    final session = _session(txn);
    final result = await session.execute(
      Sql.named('SELECT payload FROM security_context WHERE event_id = @id'),
      parameters: {'id': eventId},
    );
    if (result.isEmpty) return null;
    return EventSecurityContext.fromJson(_asJsonMap(result.first[0]));
  }

  @internal
  @override
  Future<void> writeInTxn(Transaction txn, EventSecurityContext row) async {
    final session = _session(txn);
    await session.execute(
      Sql.named('''
        INSERT INTO security_context (event_id, recorded_at, ip_address, payload)
        VALUES (@id, @recAt:timestamptz, @ip, @payload:jsonb)
      '''),
      parameters: {
        'id': row.eventId,
        'recAt': row.recordedAt.toUtc(),
        'ip': row.ipAddress,
        'payload': row.toJson(),
      },
    );
  }

  @internal
  @override
  Future<void> upsertInTxn(Transaction txn, EventSecurityContext row) async {
    final session = _session(txn);
    await session.execute(
      Sql.named('''
        INSERT INTO security_context (event_id, recorded_at, ip_address, payload)
        VALUES (@id, @recAt:timestamptz, @ip, @payload:jsonb)
        ON CONFLICT (event_id) DO UPDATE SET
          recorded_at = EXCLUDED.recorded_at,
          ip_address  = EXCLUDED.ip_address,
          payload     = EXCLUDED.payload
      '''),
      parameters: {
        'id': row.eventId,
        'recAt': row.recordedAt.toUtc(),
        'ip': row.ipAddress,
        'payload': row.toJson(),
      },
    );
  }

  @internal
  @override
  Future<void> deleteInTxn(Transaction txn, String eventId) async {
    final session = _session(txn);
    await session.execute(
      Sql.named('DELETE FROM security_context WHERE event_id = @id'),
      parameters: {'id': eventId},
    );
  }

  @override
  Future<List<EventSecurityContext>> findUnredactedOlderThanInTxn(
    Transaction txn,
    DateTime cutoff,
  ) async {
    final session = _session(txn);
    final result = await session.execute(
      Sql.named('''
        SELECT payload FROM security_context
        WHERE recorded_at <= @cutoff:timestamptz
          AND (payload->>'redacted_at') IS NULL
      '''),
      parameters: {'cutoff': cutoff.toUtc()},
    );
    return result
        .map((r) => EventSecurityContext.fromJson(_asJsonMap(r[0])))
        .toList(growable: false);
  }

  @override
  Future<List<EventSecurityContext>> findOlderThanInTxn(
    Transaction txn,
    DateTime cutoff,
  ) async {
    final session = _session(txn);
    final result = await session.execute(
      Sql.named('''
        SELECT payload FROM security_context
        WHERE recorded_at <= @cutoff:timestamptz
      '''),
      parameters: {'cutoff': cutoff.toUtc()},
    );
    return result
        .map((r) => EventSecurityContext.fromJson(_asJsonMap(r[0])))
        .toList(growable: false);
  }

  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => _backend.queryAudit(
    initiator: initiator,
    flowToken: flowToken,
    ipAddress: ipAddress,
    from: from,
    to: to,
    limit: limit,
    cursor: cursor,
  );

  TxSession _session(Transaction txn) {
    if (txn is! _PostgresTxn) {
      throw ArgumentError.value(
        txn,
        'txn',
        'PostgresSecurityContextStore requires a _PostgresTxn produced by '
            'PostgresBackend.transaction(); received ${txn.runtimeType}',
      );
    }
    // Implements: EVS-DEV-postgres-backend/L
    // a transaction handle is honoured only by the backend that minted it.
    if (!identical(txn._owner, _backend)) {
      throw StateError(
        'PostgresSecurityContextStore: Transaction was produced by a '
        'different PostgresBackend instance; refusing to apply it.',
      );
    }
    return txn._session;
  }

  Map<String, Object?> _asJsonMap(Object? value) {
    if (value is Map<String, Object?>) return value;
    if (value is Map) return Map<String, Object?>.from(value);
    throw FormatException(
      'PostgresSecurityContextStore: expected JSON object, got '
      '${value?.runtimeType}',
    );
  }
}
