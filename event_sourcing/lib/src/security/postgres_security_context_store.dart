// Implements: EVS-PRD-event-log/A — all mutations accept a caller-supplied
//   `Txn` so they commit atomically with the event-log row they describe.
//   Postgres-side, the [Txn] passed in is a [PostgresTxn] holding the
//   `TxSession` opened by [PostgresBackend.transaction]; writes routed
//   through `txn.session` therefore live inside the same transaction as
//   the `events` row produced by [PostgresBackend.appendEvent].
// Implements: EVS-PRD-regulatory-alignment — `findUnredactedOlderThanInTxn`
//   and `findOlderThanInTxn` drive the retention compact/purge sweeps that
//   satisfy ALCOA+ Enduring / §11.10(c) protection-of-records obligations.
// Implements: EVS-DEV-postgres-backend/D — fills the second SecurityContext
//   sidecar implementation needed for the Postgres backend to support
//   the substrate's full action-dispatch path. The `security_context`
//   DDL is emitted by `ensurePostgresSchema` alongside the events table.

import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_txn.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:postgres/postgres.dart';

/// Postgres-backed `SecurityContextStore`. Persists one row per event in
/// the `security_context` table; `payload` is the full
/// [EventSecurityContext] JSON shape, with `recorded_at` and `ip_address`
/// promoted to dedicated columns for filtering and ORDER BY support.
///
/// Cross-store reads (the security_context + events join) live on the
/// backend via [PostgresBackend.queryAudit]; this store's [queryAudit]
/// is a thin delegator. Mutations live here so they can share a
/// [PostgresTxn] with the matching [PostgresBackend.appendEvent].
class PostgresSecurityContextStore extends InternalSecurityContextStore {
  PostgresSecurityContextStore({required this.backend});

  final PostgresBackend backend;

  @override
  Future<EventSecurityContext?> read(String eventId) {
    return backend.transaction((txn) => readInTxn(txn, eventId));
  }

  @override
  Future<EventSecurityContext?> readInTxn(Txn txn, String eventId) async {
    final session = _session(txn);
    final result = await session.execute(
      Sql.named('SELECT payload FROM security_context WHERE event_id = @id'),
      parameters: {'id': eventId},
    );
    if (result.isEmpty) return null;
    return EventSecurityContext.fromJson(_asJsonMap(result.first[0]));
  }

  @override
  Future<void> writeInTxn(Txn txn, EventSecurityContext row) async {
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

  @override
  Future<void> upsertInTxn(Txn txn, EventSecurityContext row) async {
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

  @override
  Future<void> deleteInTxn(Txn txn, String eventId) async {
    final session = _session(txn);
    await session.execute(
      Sql.named('DELETE FROM security_context WHERE event_id = @id'),
      parameters: {'id': eventId},
    );
  }

  @override
  Future<List<EventSecurityContext>> findUnredactedOlderThanInTxn(
    Txn txn,
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
    Txn txn,
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
  }) => backend.queryAudit(
    initiator: initiator,
    flowToken: flowToken,
    ipAddress: ipAddress,
    from: from,
    to: to,
    limit: limit,
    cursor: cursor,
  );

  TxSession _session(Txn txn) {
    if (txn is! PostgresTxn) {
      throw ArgumentError.value(
        txn,
        'txn',
        'PostgresSecurityContextStore requires a PostgresTxn produced by '
            'PostgresBackend.transaction(); received ${txn.runtimeType}',
      );
    }
    return txn.session;
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
