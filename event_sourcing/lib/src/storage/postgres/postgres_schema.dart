// Implements: EVS-DEV-postgres-backend/A — idempotent CREATE TABLE IF NOT
// EXISTS DDL for every table the PostgresBackend reads or writes. Running
// this against an already-provisioned database is a no-op on the schema.

import 'package:postgres/postgres.dart';

/// The schema version this build of the postgres backend emits. Bumped
/// when backwards-incompatible DDL changes ship; subsequent boots check
/// this against the value stored in `backend_state` (Task 10).
const int postgresBackendSchemaVersion = 1;

/// Emit `CREATE TABLE IF NOT EXISTS` for every table the backend uses.
///
/// Each statement is its own private `const` String at the bottom of
/// this file (one DDL per constant) so they stay greppable; the body of
/// this function is just a sequence of `await session.execute(_xxx)`
/// calls. Idempotent: running this against an already-provisioned
/// database raises no errors.
///
/// Accepts a [Session] (rather than a [Connection] or [TxSession])
/// because both raw sessions returned by `Pool.run` and the
/// transactional sessions returned by `Pool.runTx` satisfy this
/// interface. `PostgresBackend.open` invokes this inside `runTx` so
/// the schema emission is atomic.
Future<void> ensurePostgresSchema(Session session) async {
  await session.execute(_eventsTable);
  // No explicit index on event_id: the UNIQUE constraint above creates a
  // B-tree index automatically; a second non-unique index would be redundant.
  await session.execute(_eventsAggregateIdx);
  await session.execute(_eventsClientTsIdx);

  await session.execute(_viewRowsTable);
  await session.execute(_viewTargetVersionsTable);

  await session.execute(_fifoEntriesTable);
  await session.execute(_fifoEntriesHeadIdx);

  await session.execute(_backendStateTable);
  await session.execute(_securityContextTable);
  await session.execute(_idempotencyTable);
  // `raw_input_canonical_json` is declared in the CREATE TABLE above, but
  // CREATE TABLE is a no-op on a pre-existing table, so an older database
  // may lack the column. `PostgresIdempotencyStore.lookup` SELECTs it;
  // a missing COLUMN would raise "column does not exist" rather than
  // returning null (null covers a missing VALUE, not a missing COLUMN).
  // ADD COLUMN IF NOT EXISTS is a metadata-only, idempotent operation, so
  // run it unconditionally on every boot.
  await session.execute(_idempotencyAddRawInputColumn);
}

// --- Events ---------------------------------------------------------------

const String _eventsTable = '''
CREATE TABLE IF NOT EXISTS events (
  sequence_number      BIGINT       PRIMARY KEY,
  event_id             TEXT         NOT NULL UNIQUE,
  aggregate_id         TEXT         NOT NULL,
  aggregate_type       TEXT         NOT NULL,
  entry_type           TEXT         NOT NULL,
  entry_type_version   INTEGER      NOT NULL,
  lib_format_version   INTEGER      NOT NULL,
  event_type           TEXT         NOT NULL,
  data                 JSONB        NOT NULL,
  metadata             JSONB        NOT NULL,
  initiator            JSONB        NOT NULL,
  client_timestamp     TIMESTAMPTZ  NOT NULL,
  event_hash           TEXT         NOT NULL,
  flow_token           TEXT,
  previous_event_hash  TEXT
)
''';

const String _eventsAggregateIdx = '''
CREATE INDEX IF NOT EXISTS events_aggregate_idx
  ON events (aggregate_id, sequence_number)
''';

const String _eventsClientTsIdx = '''
CREATE INDEX IF NOT EXISTS events_client_ts_idx
  ON events (client_timestamp)
''';

// --- View rows ------------------------------------------------------------

const String _viewRowsTable = '''
CREATE TABLE IF NOT EXISTS view_rows (
  view_name   TEXT         NOT NULL,
  row_key     TEXT         NOT NULL,
  row_data    JSONB        NOT NULL,
  updated_at  TIMESTAMPTZ  NOT NULL,
  PRIMARY KEY (view_name, row_key)
)
''';

// --- View target versions -------------------------------------------------

const String _viewTargetVersionsTable = '''
CREATE TABLE IF NOT EXISTS view_target_versions (
  view_name       TEXT     NOT NULL,
  entry_type      TEXT     NOT NULL,
  target_version  INTEGER  NOT NULL,
  PRIMARY KEY (view_name, entry_type)
)
''';

// --- FIFO entries ---------------------------------------------------------

const String _fifoEntriesTable = '''
CREATE TABLE IF NOT EXISTS fifo_entries (
  destination_id      TEXT          NOT NULL,
  sequence_in_queue   BIGINT        NOT NULL,
  entry_id            TEXT          NOT NULL UNIQUE,
  event_ids           JSONB         NOT NULL,
  event_id_first_seq  BIGINT        NOT NULL,
  event_id_last_seq   BIGINT        NOT NULL,
  wire_format         TEXT          NOT NULL,
  transform_version   TEXT,
  enqueued_at         TIMESTAMPTZ   NOT NULL,
  attempts            JSONB         NOT NULL,
  final_status        TEXT,
  sent_at             TIMESTAMPTZ,
  wire_payload        JSONB,
  envelope_metadata   JSONB,
  PRIMARY KEY (destination_id, sequence_in_queue)
)
''';

const String _fifoEntriesHeadIdx = '''
CREATE INDEX IF NOT EXISTS fifo_entries_head_idx
  ON fifo_entries (destination_id, sequence_in_queue)
  WHERE final_status IS NULL OR final_status = 'wedged'
''';

// --- Backend state KV -----------------------------------------------------

const String _backendStateTable = '''
CREATE TABLE IF NOT EXISTS backend_state (
  key    TEXT   PRIMARY KEY,
  value  JSONB  NOT NULL
)
''';

// --- Security context sidecar --------------------------------------------

const String _securityContextTable = '''
CREATE TABLE IF NOT EXISTS security_context (
  event_id     TEXT         PRIMARY KEY,
  recorded_at  TIMESTAMPTZ  NOT NULL,
  ip_address   TEXT,
  payload      JSONB        NOT NULL
)
''';

// --- Idempotency ----------------------------------------------------------

// `raw_input_canonical_json` (TEXT, nullable) carries the RFC-8785
// canonicalization of the original submission's `rawInput`, recorded so
// the dispatcher can detect same-key, different-content collisions
// (EVS-PRD-action-dispatch/E). A NULL VALUE in this column means no
// canonical form was captured; the dispatcher treats null as
// "no mismatch detection available" and returns the cache hit as-is,
// never raising a false `idempotency_mismatch`. (Note: that null-value
// fallback does NOT cover a missing COLUMN; the idempotent ALTER in
// `ensurePostgresSchema` guarantees the column itself exists.)
const String _idempotencyTable = '''
CREATE TABLE IF NOT EXISTS idempotency (
  action_name               TEXT         NOT NULL,
  principal_id              TEXT         NOT NULL,
  idempotency_key           TEXT         NOT NULL,
  result_json               JSONB        NOT NULL,
  emitted_event_ids         JSONB        NOT NULL,
  recorded_at               TIMESTAMPTZ  NOT NULL,
  expires_at                TIMESTAMPTZ  NOT NULL,
  raw_input_canonical_json  TEXT,
  PRIMARY KEY (action_name, principal_id, idempotency_key)
)
''';

// Idempotent migration: the CREATE TABLE above is a no-op on a pre-existing
// table, so the `raw_input_canonical_json` column may be absent on databases
// provisioned before it was added. ADD COLUMN IF NOT EXISTS backfills the
// column (nullable add = metadata-only) so `lookup`'s SELECT of it never
// hits "column does not exist".
const String _idempotencyAddRawInputColumn = '''
ALTER TABLE idempotency
  ADD COLUMN IF NOT EXISTS raw_input_canonical_json TEXT
''';
