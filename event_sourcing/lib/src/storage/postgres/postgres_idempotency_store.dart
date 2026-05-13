// Implements: EVS-PRD-action-dispatch/D — IdempotencyStore contract:
//   record stores the dispatch outcome; lookup hit returns the cached
//   IdempotencyEntry verbatim; expired entries miss on lookup;
//   sweepExpired physically removes them.
// Implements: EVS-DEV-postgres-backend/E — persist entries in the
//   `idempotency` table keyed by (action_name, principal_id,
//   idempotency_key). The DDL is emitted by `ensurePostgresSchema`
//   (Task 1 of the Postgres backend plan) and shared with the rest of
//   the backend.
// Implements: EVS-DEV-postgres-backend/F — passes the
//   `runIdempotencyStoreConformanceTests` harness alongside
//   InMemoryIdempotencyStore.

import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:event_sourcing/src/actions/idempotency_store.dart';
import 'package:postgres/postgres.dart';

/// Postgres-backed [IdempotencyStore]. Persists each dispatch outcome
/// as one row in the `idempotency` table; primary key is
/// `(action_name, principal_id, idempotency_key)` so the three-tuple
/// determines uniqueness and the in-memory and Postgres impls share
/// the same key semantics.
///
/// The store assumes the `idempotency` table already exists; the
/// substrate's `PostgresBackend.open` emits the DDL alongside the
/// events tables via `ensurePostgresSchema`. Standalone callers
/// (test harnesses, scripts) must call `ensurePostgresSchema` against
/// the underlying database first.
class PostgresIdempotencyStore implements IdempotencyStore {
  /// Build a [PostgresIdempotencyStore] over an already-opened [Pool].
  /// Pool lifecycle (open/close, connection limits) is the caller's
  /// concern — `PostgresBackend` owns its own pool, and standalone
  /// callers (e.g., the conformance harness) own theirs.
  PostgresIdempotencyStore.over(this._pool);

  final Pool<void> _pool;

  // Implements: EVS-PRD-action-dispatch/D — entries past their
  //   `expires_at` MUST NOT be returned by lookup, even before
  //   sweepExpired physically removes them. The `expires_at > @cutoff`
  //   predicate enforces this at read time.
  @override
  Future<IdempotencyEntry?> lookup(
    String actionName,
    String principalId,
    String key, {
    DateTime? now,
  }) async {
    final cutoff = (now ?? DateTime.now()).toUtc();
    final result = await _pool.execute(
      Sql.named('''
        SELECT result_json, emitted_event_ids, recorded_at, expires_at
        FROM idempotency
        WHERE action_name = @a
          AND principal_id = @p
          AND idempotency_key = @k
          AND expires_at > @cutoff
        LIMIT 1
      '''),
      parameters: {
        'a': actionName,
        'p': principalId,
        'k': key,
        'cutoff': cutoff,
      },
    );
    if (result.isEmpty) return null;
    final row = result.first;
    return IdempotencyEntry(
      resultJson: Map<String, Object?>.unmodifiable(
        Map<String, dynamic>.from(row[0] as Map),
      ),
      emittedEventIds: List<String>.unmodifiable(
        (row[1] as List).cast<String>(),
      ),
      recordedAt: (row[2] as DateTime).toUtc(),
      expiresAt: (row[3] as DateTime).toUtc(),
    );
  }

  // Implements: EVS-DEV-postgres-backend/E — upsert via
  //   `INSERT ... ON CONFLICT (action_name, principal_id,
  //   idempotency_key) DO UPDATE`. Repeat records for the same tuple
  //   overwrite, matching the in-memory store's map-overwrite
  //   semantics; the action-dispatch path is responsible for the
  //   "same key, different content" denial (EVS-PRD-action-dispatch/E)
  //   before this store is called.
  @override
  Future<void> record({
    required String actionName,
    required String principalId,
    required String key,
    required Map<String, Object?> resultJson,
    required List<String> emittedEventIds,
    required DateTime expiresAt,
  }) async {
    await _pool.execute(
      Sql.named('''
        INSERT INTO idempotency (
          action_name, principal_id, idempotency_key,
          result_json, emitted_event_ids, recorded_at, expires_at
        ) VALUES (
          @a, @p, @k,
          @res:jsonb, @ids:jsonb, @recAt, @expAt
        )
        ON CONFLICT (action_name, principal_id, idempotency_key)
        DO UPDATE SET
          result_json = EXCLUDED.result_json,
          emitted_event_ids = EXCLUDED.emitted_event_ids,
          recorded_at = EXCLUDED.recorded_at,
          expires_at = EXCLUDED.expires_at
      '''),
      parameters: {
        'a': actionName,
        'p': principalId,
        'k': key,
        'res': resultJson,
        'ids': emittedEventIds,
        'recAt': DateTime.now().toUtc(),
        'expAt': expiresAt.toUtc(),
      },
    );
  }

  // Implements: EVS-PRD-action-dispatch/D — sweepExpired physically
  //   removes entries whose expires_at <= cutoff and returns the
  //   count deleted (via the postgres driver's `affectedRows`).
  @override
  Future<int> sweepExpired({DateTime? before}) async {
    final cutoff = (before ?? DateTime.now()).toUtc();
    final result = await _pool.execute(
      Sql.named('DELETE FROM idempotency WHERE expires_at <= @c'),
      parameters: {'c': cutoff},
    );
    return result.affectedRows;
  }
}
