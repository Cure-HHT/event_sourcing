// Implements: EVS-DEV-postgres-backend/G
// the ordered migration list whose steps provisioning applies; this build
//   ships one step, version 1, holding the whole DDL.

import 'package:event_sourcing/src/storage/postgres/postgres_migration.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;

/// The Postgres schema version this build requires and provisions: the
/// last migration step's `toVersion`.
///
/// The schema version versions the backend's DDL, not the data it stores
/// (the data format, `LibVersion.dataFormat`, versions that). A data-format
/// minor that adds DDL adds a migration step that raises the schema version
/// and keeps [postgresMinCompatibleSchemaVersion]; a data-format major is
/// provisioned only after every instance of the old major has stopped,
/// which the incompatible-generation guard enforces.
const int postgresSchemaVersion = 1;

/// The minimum compatible schema version this build records when it
/// provisions: the last migration step's `minCompatibleVersion`. A build
/// whose [postgresSchemaVersion] is below the minimum stored in a database
/// refuses to open it.
const int postgresMinCompatibleSchemaVersion = 1;

/// The ordered migration steps of this build. Step `n` brings a schema at
/// the previous step's version to its `toVersion`.
@internal
const List<PostgresMigrationStep> postgresMigrations = <PostgresMigrationStep>[
  PostgresMigrationStep(
    toVersion: 1,
    minCompatibleVersion: 1,
    ddl: <String>[
      _eventsTable,
      // No explicit index on event_id: the UNIQUE constraint creates one.
      _eventsAggregateIdx,
      _eventsClientTsIdx,
      // The boot reads the library-version events inside the transaction
      // that holds every append back; this index keeps that read
      // proportional to the number of those events, not to the length of
      // the log.
      _eventsTypeSeqIdx,
      _viewRowsTable,
      _viewTargetVersionsTable,
      _fifoEntriesTable,
      _fifoEntriesHeadIdx,
      _backendStateTable,
      _securityContextTable,
      _idempotencyTable,
    ],
  ),
];

/// The tables the library creates. A schema that holds any of them but
/// records no schema version was not created by provisioning, and
/// provisioning refuses to certify it.
@internal
const List<String> postgresLibraryTables = <String>[
  'events',
  'view_rows',
  'view_target_versions',
  'fifo_entries',
  'backend_state',
  'security_context',
  'idempotency',
];

/// The migration steps in effect: [postgresMigrations], or the list a test
/// installed through the `schemaDeclaration` test seam.
@internal
List<PostgresMigrationStep> effectivePostgresMigrations() =>
    DeliveryTestHooks.current?.schemaDeclaration ?? postgresMigrations;

// --- Events ---------------------------------------------------------------

const String _eventsTable = '''
CREATE TABLE IF NOT EXISTS events (
  sequence_number      BIGINT       PRIMARY KEY,
  event_id             TEXT         NOT NULL UNIQUE,
  aggregate_id         TEXT         NOT NULL,
  aggregate_type       TEXT         NOT NULL,
  entry_type           TEXT         NOT NULL,
  entry_type_version_major  INTEGER  NOT NULL
    CHECK (entry_type_version_major >= 1),
  entry_type_version_minor  INTEGER  NOT NULL
    CHECK (entry_type_version_minor >= 0),
  lib_format_version_major  INTEGER  NOT NULL
    CHECK (lib_format_version_major >= 1),
  lib_format_version_minor  INTEGER  NOT NULL
    CHECK (lib_format_version_minor >= 0),
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

const String _eventsTypeSeqIdx = '''
CREATE INDEX IF NOT EXISTS events_type_seq_idx
  ON events (event_type, sequence_number)
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
  target_major    INTEGER  NOT NULL  CHECK (target_major >= 1),
  target_minor    INTEGER  NOT NULL  CHECK (target_minor >= 0),
  behind          BOOLEAN  NOT NULL  DEFAULT false,
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
// never raising a false `idempotency_mismatch`.
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
