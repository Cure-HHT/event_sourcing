// Implements: EVS-DEV-postgres-backend/G
// the ordered migration list whose steps provisioning applies: version 1
//   holds the tables of the log, the views, the queues and the sidecars;
//   version 2 adds the declared library roles and keeps the minimum.
// Implements: EVS-DEV-postgres-backend/P
// the `library_roles` table in the library's schema, created by the owner,
//   in which provisioning records the declared runtime and lock roles.
// Implements: EVS-DEV-destination-drain/S
// the queue table's status check and the `fifo_entries_guard` triggers,
//   created by provisioning with the table they guard.

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
const int postgresSchemaVersion = 2;

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
      _fifoEntriesGuardFunction,
      _fifoEntriesGuardTriggerDrop,
      _fifoEntriesGuardTrigger,
      _fifoEntriesTruncateGuardFunction,
      _fifoEntriesTruncateGuardTriggerDrop,
      _fifoEntriesTruncateGuardTrigger,
      _fifoEntriesGuardEnableAlways,
      _fifoEntriesTruncateGuardEnableAlways,
      _backendStateTable,
      _securityContextTable,
      _idempotencyTable,
    ],
  ),
  PostgresMigrationStep(
    toVersion: 2,
    minCompatibleVersion: 1,
    ddl: <String>[_libraryRolesTable],
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
  'library_roles',
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
  entry_type_version_json  JSONB    NOT NULL,
  lib_format_version_json  JSONB    NOT NULL,
  event_type           TEXT         NOT NULL,
  data                 JSONB        NOT NULL,
  metadata             JSONB        NOT NULL,
  initiator            JSONB        NOT NULL,
  client_timestamp     TIMESTAMPTZ  NOT NULL,
  client_timestamp_text  TEXT       NOT NULL,
  event_hash           TEXT         NOT NULL,
  flow_token           TEXT,
  previous_event_hash  TEXT,
  unknown_fields       JSONB        NOT NULL
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
  final_status        TEXT
    CONSTRAINT fifo_entries_final_status_check
    CHECK (final_status IS NULL
           OR final_status IN ('sent', 'wedged', 'tombstoned')),
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

// The queue table's guard. The library changes a queue item only in these
// shapes: it inserts an item pending, with no attempts and no delivery time;
// it appends one attempt to a pending item; it marks a pending item sent
// (stamping the delivery time) or wedged, appending at most the attempt
// that decided it; it tombstones a wedged item; and it deletes pending
// items. The guard refuses every change outside those shapes, whatever role
// makes it: no item is inserted terminal, rewrites what it was enqueued
// with, or loses a terminal item. It checks the shape of a change, not who
// makes it, so a hand-written change of a legal shape passes (wedging or
// marking sent a pending item, tombstoning a wedged one, deleting a pending
// one, inserting a pending one); those rest on the storage precondition.
// The triggers fire in every session replication role, and the role that
// owns the table can drop them; the runtime role cannot.
//
// The functions pin their search path, so a role that can create objects on
// a schema earlier on its path cannot substitute an operator or function
// they call.
const String _fifoEntriesGuardFunction = r'''
CREATE OR REPLACE FUNCTION fifo_entries_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
  old_status text;
  new_status text;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF NEW.final_status IS NOT NULL THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is inserted with status %; an item is inserted pending',
        NEW.entry_id, NEW.final_status USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.attempts IS DISTINCT FROM '[]'::jsonb THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is inserted with attempts; an item is inserted with none',
        NEW.entry_id USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.sent_at IS NOT NULL THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is inserted with a delivery time; an item is inserted undelivered',
        NEW.entry_id USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;

  IF TG_OP = 'DELETE' THEN
    IF OLD.final_status IS NOT NULL THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is %; a terminal item is never deleted',
        OLD.entry_id, OLD.final_status USING ERRCODE = 'check_violation';
    END IF;
    RETURN OLD;
  END IF;

  -- UPDATE. The statuses are compared as text with pending spelled out, so
  -- no comparison below is null.
  old_status := coalesce(OLD.final_status, 'pending');
  new_status := coalesce(NEW.final_status, 'pending');
  IF (old_status, new_status) NOT IN (
    ('pending', 'pending'),
    ('pending', 'sent'),
    ('pending', 'wedged'),
    ('wedged', 'tombstoned')
  ) THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % cannot change status from % to %; the legal changes are pending to sent, pending to wedged and wedged to tombstoned',
      OLD.entry_id, old_status, new_status USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.destination_id IS DISTINCT FROM OLD.destination_id
     OR NEW.entry_id IS DISTINCT FROM OLD.entry_id
     OR NEW.event_ids IS DISTINCT FROM OLD.event_ids
     OR NEW.event_id_first_seq IS DISTINCT FROM OLD.event_id_first_seq
     OR NEW.event_id_last_seq IS DISTINCT FROM OLD.event_id_last_seq
     OR NEW.sequence_in_queue IS DISTINCT FROM OLD.sequence_in_queue
     OR NEW.wire_format IS DISTINCT FROM OLD.wire_format
     OR NEW.transform_version IS DISTINCT FROM OLD.transform_version
     OR NEW.wire_payload IS DISTINCT FROM OLD.wire_payload
     OR NEW.envelope_metadata IS DISTINCT FROM OLD.envelope_metadata
     OR NEW.enqueued_at IS DISTINCT FROM OLD.enqueued_at THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % changes a column it was enqueued with',
      OLD.entry_id USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.sent_at IS DISTINCT FROM OLD.sent_at
     AND (old_status, new_status) <> ('pending', 'sent') THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % changes its delivery time outside the change that marks it sent',
      OLD.entry_id USING ERRCODE = 'check_violation';
  END IF;
  -- The array checks come first and on their own, since the length
  -- functions raise on anything else.
  IF NEW.attempts IS DISTINCT FROM OLD.attempts THEN
    IF old_status <> 'pending'
       OR jsonb_typeof(NEW.attempts) IS DISTINCT FROM 'array'
       OR jsonb_typeof(OLD.attempts) IS DISTINCT FROM 'array' THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % changes its attempts other than by appending one attempt while pending',
        OLD.entry_id USING ERRCODE = 'check_violation';
    END IF;
    IF jsonb_array_length(NEW.attempts)
         <> jsonb_array_length(OLD.attempts) + 1
       OR (NEW.attempts - (jsonb_array_length(NEW.attempts) - 1))
         IS DISTINCT FROM OLD.attempts THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % changes its attempts other than by appending one attempt while pending',
        OLD.entry_id USING ERRCODE = 'check_violation';
    END IF;
  END IF;
  RETURN NEW;
END
$guard$
''';

const String _fifoEntriesGuardTriggerDrop =
    'DROP TRIGGER IF EXISTS fifo_entries_guard ON fifo_entries';

const String _fifoEntriesGuardTrigger = '''
CREATE TRIGGER fifo_entries_guard
  BEFORE INSERT OR UPDATE OR DELETE ON fifo_entries
  FOR EACH ROW EXECUTE FUNCTION fifo_entries_guard()
''';

// A truncation fires no row trigger, so a statement trigger refuses every
// truncation of the queue table. The library never truncates it, and a
// conditional refusal (only while a terminal item exists) would read under
// the truncating transaction's snapshot and miss a terminal item another
// transaction committed after it.
const String _fifoEntriesTruncateGuardFunction = r'''
CREATE OR REPLACE FUNCTION fifo_entries_truncate_guard() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $guard$
BEGIN
  RAISE EXCEPTION 'fifo_entries_guard: the queue table is never truncated; its terminal items are the delivery record'
    USING ERRCODE = 'check_violation';
END
$guard$
''';

const String _fifoEntriesTruncateGuardTriggerDrop =
    'DROP TRIGGER IF EXISTS fifo_entries_truncate_guard ON fifo_entries';

const String _fifoEntriesTruncateGuardTrigger = '''
CREATE TRIGGER fifo_entries_truncate_guard
  BEFORE TRUNCATE ON fifo_entries
  FOR EACH STATEMENT EXECUTE FUNCTION fifo_entries_truncate_guard()
''';

// A trigger created plainly does not fire in a session whose replication
// role is `replica`; these fire in every session.
const String _fifoEntriesGuardEnableAlways =
    'ALTER TABLE fifo_entries ENABLE ALWAYS TRIGGER fifo_entries_guard';

const String _fifoEntriesTruncateGuardEnableAlways =
    'ALTER TABLE fifo_entries ENABLE ALWAYS TRIGGER fifo_entries_truncate_guard';

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

// --- Declared library roles ----------------------------------------------

// The runtime and lock roles the deployment declared when it provisioned the
// database, one row per role and kind. The owner creates the table and
// provisioning, run as the owner, rewrites its rows; the runtime role is
// granted `SELECT` alone, so no library role changes which roles `open`
// admits.
const String _libraryRolesTable = '''
CREATE TABLE IF NOT EXISTS library_roles (
  role_name  TEXT  NOT NULL  CHECK (role_name <> ''),
  kind       TEXT  NOT NULL  CHECK (kind IN ('runtime', 'lock')),
  PRIMARY KEY (role_name, kind)
)
''';
