// Implements: EVS-PRD-action-dispatch/D
// IdempotencyStore contract:
//   record stores the dispatch outcome; lookup hit returns the cached
//   IdempotencyEntry verbatim; expired entries miss on lookup;
//   sweepExpired physically removes them.
// Implements: EVS-DEV-postgres-backend/E
// persist entries in the
//   `idempotency` table keyed by (action_name, principal_id,
//   idempotency_key). The table is created by `PostgresBackend.provision`
//   with the rest of the backend's schema.
// Implements: EVS-DEV-postgres-backend/F
// passes the
//   `runIdempotencyStoreConformanceTests` harness alongside
//   InMemoryIdempotencyStore.

part of 'postgres_backend.dart';

/// Postgres-backed [IdempotencyStore]. Persists each dispatch outcome
/// as one row in the `idempotency` table; primary key is
/// `(action_name, principal_id, idempotency_key)` so the three-tuple
/// determines uniqueness and the in-memory and Postgres impls share
/// the same key semantics.
///
/// The store assumes the `idempotency` table exists: `PostgresBackend.
/// provision` creates it with the rest of the backend's schema.

class PostgresIdempotencyStore implements IdempotencyStore {
  /// Build a [PostgresIdempotencyStore] over [backend], so dispatch
  /// outcomes persist in the backend's database. Every read and write runs
  /// through `PostgresBackend.transaction`, so it is checked against the
  /// database's generation record like every other library write, and a
  /// fenced backend refuses it. The backend owns the connections; closing
  /// the backend closes the store's. The event store builds it for its own
  /// storage (`EventStore.idempotencyStore`).
  // Implements: EVS-DEV-version-compatibility/I
  // the idempotency store's lookups, records and sweeps run through the
  //   backend's fenced transactions.
  // Implements: EVS-DEV-storage-capability/I
  // the constructor over a backend is private to the backend's Dart
  //   library: the event store builds the idempotency store over the
  //   storage it opened, through the backend.
  PostgresIdempotencyStore._forBackend(PostgresBackend backend)
    : _run = _throughBackend(backend);

  /// Build a [PostgresIdempotencyStore] over a [Pool] the application
  /// opened, under its own role, whose connections find the `idempotency`
  /// table (the application provisions it in its own schema). The
  /// application owns the pool: its lifecycle and connection limits are
  /// the caller's concern.
  PostgresIdempotencyStore.over(Pool<void> pool)
    : _run = (<R>(Future<R> Function(Session session) op) => pool.run(op));

  final Future<R> Function<R>(Future<R> Function(Session session) op) _run;

  static Future<R> Function<R>(Future<R> Function(Session session) op)
  _throughBackend(PostgresBackend backend) =>
      <R>(Future<R> Function(Session session) op) =>
          backend.transaction((txn) => op((txn as _PostgresTxn)._session));

  // Implements: EVS-PRD-action-dispatch/D
  // entries past their
  //   `expires_at` MUST NOT be returned by lookup, even before
  //   sweepExpired physically removes them. The `expires_at > @cutoff`
  //   predicate enforces this at read time.
  // Implements: EVS-PRD-action-dispatch/E
  // returns
  //   `rawInputCanonicalJson` so the dispatcher can compare the
  //   submitted rawInput's canonical form against the cached one.
  //   NULL passes through as null; the dispatcher treats null as
  //   "no mismatch detection available" and returns the cache hit as
  //   before, never raising a false `idempotency_mismatch`.
  @override
  Future<IdempotencyEntry?> lookup(
    String actionName,
    String principalId,
    String key, {
    DateTime? now,
  }) async {
    final cutoff = (now ?? DateTime.now()).toUtc();
    final result = await _run(
      (session) => session.execute(
        Sql.named('''
        SELECT result_json, emitted_event_ids, recorded_at, expires_at,
               raw_input_canonical_json
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
      ),
    );
    if (result.isEmpty) return null;
    final row = result.first;
    return IdempotencyEntry(
      actionName: actionName,
      principalId: principalId,
      idempotencyKey: key,
      resultJson: Map<String, Object?>.unmodifiable(
        Map<String, dynamic>.from(row[0] as Map),
      ),
      emittedEventIds: List<String>.unmodifiable(
        (row[1] as List).cast<String>(),
      ),
      recordedAt: (row[2] as DateTime).toUtc(),
      expiresAt: (row[3] as DateTime).toUtc(),
      rawInputCanonicalJson: row[4] as String?,
    );
  }

  // Implements: EVS-DEV-postgres-backend/E
  // upsert via
  //   `INSERT ... ON CONFLICT (action_name, principal_id,
  //   idempotency_key) DO UPDATE`. Repeat records for the same tuple
  //   overwrite, matching the in-memory store's map-overwrite
  //   semantics. The dispatcher detects the "same key, different
  //   content" mismatch (EVS-PRD-action-dispatch/E) BEFORE calling
  //   `record` on the success path; this UPSERT only ever fires when
  //   the dispatcher has decided the submission is a fresh-or-matching
  //   write, never to overwrite a conflicting entry.
  // Implements: EVS-PRD-action-dispatch/E
  // persists
  //   `raw_input_canonical_json` alongside the cached outcome so a
  //   subsequent lookup can drive the dispatcher's content-mismatch
  //   check.
  @override
  Future<void> record({
    required String actionName,
    required String principalId,
    required String key,
    required Map<String, Object?> resultJson,
    required List<String> emittedEventIds,
    required DateTime expiresAt,
    String? rawInputCanonicalJson,
  }) async {
    await _run(
      (session) => session.execute(
        Sql.named('''
        INSERT INTO idempotency (
          action_name, principal_id, idempotency_key,
          result_json, emitted_event_ids, recorded_at, expires_at,
          raw_input_canonical_json
        ) VALUES (
          @a, @p, @k,
          @res:jsonb, @ids:jsonb, @recAt, @expAt,
          @rawJson
        )
        ON CONFLICT (action_name, principal_id, idempotency_key)
        DO UPDATE SET
          result_json = EXCLUDED.result_json,
          emitted_event_ids = EXCLUDED.emitted_event_ids,
          recorded_at = EXCLUDED.recorded_at,
          expires_at = EXCLUDED.expires_at,
          raw_input_canonical_json = EXCLUDED.raw_input_canonical_json
      '''),
        parameters: {
          'a': actionName,
          'p': principalId,
          'k': key,
          'res': resultJson,
          'ids': emittedEventIds,
          'recAt': DateTime.now().toUtc(),
          'expAt': expiresAt.toUtc(),
          'rawJson': rawInputCanonicalJson,
        },
      ),
    );
  }

  // Implements: EVS-PRD-action-dispatch/D
  // sweepExpired physically
  //   removes entries whose expires_at <= cutoff and returns the
  //   count deleted (via the postgres driver's `affectedRows`).
  @override
  Future<int> sweepExpired({DateTime? before}) async {
    final cutoff = (before ?? DateTime.now()).toUtc();
    final result = await _run(
      (session) => session.execute(
        Sql.named('DELETE FROM idempotency WHERE expires_at <= @c'),
        parameters: {'c': cutoff},
      ),
    );
    return result.affectedRows;
  }

  // Implements: EVS-PRD-action-dispatch/D
  // listEntries enumerates every
  //   currently-cached entry; not filtered by expiry (callers decide).
  @override
  Future<List<IdempotencyEntry>> listEntries() async {
    final result = await _run(
      (session) => session.execute(
        Sql.named('''
        SELECT action_name, principal_id, idempotency_key,
               result_json, emitted_event_ids, recorded_at, expires_at,
               raw_input_canonical_json
        FROM idempotency
        ORDER BY action_name ASC, principal_id ASC, idempotency_key ASC
      '''),
      ),
    );
    return List<IdempotencyEntry>.unmodifiable(
      result.map(
        (row) => IdempotencyEntry(
          actionName: row[0] as String,
          principalId: row[1] as String,
          idempotencyKey: row[2] as String,
          resultJson: Map<String, Object?>.unmodifiable(
            Map<String, dynamic>.from(row[3] as Map),
          ),
          emittedEventIds: List<String>.unmodifiable(
            (row[4] as List).cast<String>(),
          ),
          recordedAt: (row[5] as DateTime).toUtc(),
          expiresAt: (row[6] as DateTime).toUtc(),
          rawInputCanonicalJson: row[7] as String?,
        ),
      ),
    );
  }
}
