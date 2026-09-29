// Implements: EVS-DEV-postgres-backend/G
// the ordered migration list whose steps provisioning applies: version 1
//   holds the tables of the log, the views, the queues and the sidecars;
//   version 2 adds the declared library roles and keeps the minimum;
//   version 3 adds the chain lookup columns and indexes, the causal column
//   and the latest-eligible-version index to the events table and raises
//   the minimum to itself; version 4 adds the delivery columns and the
//   retained-delivery index to the queue table, rewrites its guard, and
//   raises the minimum to itself; version 5 adds the transform-failed
//   columns to the queue table and rewrites its guard, and raises the
//   minimum to itself; version 6 adds the `view_copies` table with its
//   partial unique index on an unmarked fingerprint, renames `view_rows`'
//   `view_name` column to `copy_id`, and raises the minimum to itself;
//   version 7 adds `view_rows.source_aggregate_id` with its partial index
//   and raises the minimum to itself.
// Implements: EVS-DEV-chain-verification/A
// the chain lookups on Postgres: columns of the events table holding the
//   originating database, sealed hash and origin position each stored copy
//   yields, written by the insert that stores it, and the non-unique
//   indexes the lookups read.
// Implements: EVS-DEV-postgres-backend/P
// the `library_roles` table in the library's schema, created by the owner,
//   in which provisioning records the declared runtime and lock roles.
// Implements: EVS-DEV-destination-drain/S
// the queue table's status check and the `fifo_entries_guard` triggers,
//   created by provisioning with the table they guard.

import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kSecurityFindingEntryType;
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
const int postgresSchemaVersion = 7;

/// The minimum compatible schema version this build records when it
/// provisions: the last migration step's `minCompatibleVersion`. A build
/// whose [postgresSchemaVersion] is below the minimum stored in a database
/// refuses to open it.
const int postgresMinCompatibleSchemaVersion = 7;

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
  // A build before this step inserts events without their chain lookup and
  // causal columns, so the step raises the minimum: such a build refuses the
  // database rather than store events the lookups cannot find.
  PostgresMigrationStep(
    toVersion: 3,
    minCompatibleVersion: 3,
    ddl: <String>[
      _eventsChainLookupColumns,
      _eventsSealedHashIdx,
      _eventsPredecessorIdx,
      _eventsOriginPositionIdx,
      _eventsHeldAsAuthoredIdx,
      _eventsSecurityFindingIdx,
      _eventsCausalColumn,
      _eventsLatestEligibleIdx,
    ],
  ),
  // A build before this step marks a delivery sent without the delivery it
  // was acknowledged under, and its guard refuses a resume's retirement of
  // an attempted item, so the step raises the minimum.
  PostgresMigrationStep(
    toVersion: 4,
    minCompatibleVersion: 4,
    ddl: <String>[
      _fifoEntriesDeliveryColumns,
      _fifoEntriesDeliveryIdx,
      _fifoEntriesGuardFunction,
    ],
  ),
  // A build before this step has no transform_failed/transform_failures
  // columns, and its guard does not hold them immutable, so the step
  // raises the minimum.
  PostgresMigrationStep(
    toVersion: 5,
    minCompatibleVersion: 5,
    ddl: <String>[
      _fifoEntriesTransformFailedColumns,
      _fifoEntriesGuardFunction,
    ],
  ),
  // A build before this step has no view_copies table and stores view rows
  // keyed by view_name rather than copy_id, so the step raises the
  // minimum: such a build cannot address a fingerprinted copy's rows.
  PostgresMigrationStep(
    toVersion: 6,
    minCompatibleVersion: 6,
    ddl: <String>[
      _viewCopiesTable,
      _viewCopiesFingerprintIdx,
      _viewRowsRenameColumn,
    ],
  ),
  // A build before this step upserts a TableProjectionSpec row without
  // recording its source aggregate, so the outstanding-finding refresh's
  // per-source-aggregate index cannot find it; the step raises the minimum.
  PostgresMigrationStep(
    toVersion: 7,
    minCompatibleVersion: 7,
    ddl: <String>[_viewRowsSourceAggregateColumn, _viewRowsSourceAggregateIdx],
  ),
];

/// The tables the library creates. A schema that holds any of them but
/// records no schema version was not created by provisioning, and
/// provisioning refuses to certify it.
@internal
const List<String> postgresLibraryTables = <String>[
  'events',
  'view_rows',
  'view_copies',
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

// --- Chain lookups --------------------------------------------------------

// The chain lookup columns: for each stored event its originating database,
// sealed hash and origin position (read from the stored copy's provenance)
// beside its `previous_event_hash` column, and the holding database when it
// holds the copy as authored (the copy's provenance holds exactly one entry,
// naming the holding database), null for every other copy. The insert that
// stores the event writes them; the runtime role holds no UPDATE on the
// table, so nothing changes them afterwards. A column the stored copy does
// not yield is null. No index is unique: the log holds forks and reused
// origin positions as received. The library keeps no index table of its
// own.
const String _eventsChainLookupColumns = '''
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS origin_database_id   TEXT,
  ADD COLUMN IF NOT EXISTS sealed_hash          TEXT,
  ADD COLUMN IF NOT EXISTS origin_position      BIGINT,
  ADD COLUMN IF NOT EXISTS held_as_authored_by  TEXT
''';

const String _eventsSealedHashIdx = '''
CREATE INDEX IF NOT EXISTS events_sealed_hash_idx
  ON events (sealed_hash, sequence_number)
''';

const String _eventsPredecessorIdx = '''
CREATE INDEX IF NOT EXISTS events_predecessor_idx
  ON events (origin_database_id, previous_event_hash, sequence_number)
''';

const String _eventsOriginPositionIdx = '''
CREATE INDEX IF NOT EXISTS events_origin_position_idx
  ON events (origin_database_id, origin_position, sequence_number)
''';

const String _eventsHeldAsAuthoredIdx = '''
CREATE INDEX IF NOT EXISTS events_held_as_authored_idx
  ON events (held_as_authored_by, sequence_number)
  WHERE held_as_authored_by IS NOT NULL
''';

// The security findings each database holds as authored, by identity: the
// partial index serves the once-per-detector lookup, which the library
// runs inside the transaction that would append a finding. Not unique: a
// uniqueness violation would fail the detection point's transaction.
const String _eventsSecurityFindingIdx =
    '''
CREATE INDEX IF NOT EXISTS events_security_finding_idx
  ON events (held_as_authored_by, (data ->> 'finding_id'))
  WHERE entry_type = '$kSecurityFindingEntryType'
''';

// --- Causal record and the latest eligible version ----------------------

// The event's `causal` object as the record carries it; null for a record
// that carries none.
const String _eventsCausalColumn = '''
ALTER TABLE events
  ADD COLUMN IF NOT EXISTS causal JSONB
''';

// The latest eligible version of each aggregate: a partial index over the
// events whose recorded `causal` says an eligible version, so the event of
// an aggregate with the highest local sequence number among them is one
// index probe. The predicate is the one
// `PostgresBackend.readLatestEligibleVersionInTxn` queries with.
const String _eventsLatestEligibleIdx = '''
CREATE INDEX IF NOT EXISTS events_latest_eligible_idx
  ON events (aggregate_id, sequence_number DESC)
  WHERE (causal ->> 'kind') = 'version'
    AND (causal -> 'eligible') = 'true'::jsonb
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

// --- View copies ------------------------------------------------------------
//
// One row per stored copy of a registered view (EVS-DEV-view-convergence).
// The partial unique index on fingerprint, scoped to unmarked copies,
// enforces "at most one copy of a fingerprint that is not marked for
// deletion" (assertion A) as a database constraint rather than an
// application-level check.

const String _viewCopiesTable = '''
CREATE TABLE IF NOT EXISTS view_copies (
  copy_id              TEXT         PRIMARY KEY,
  view_name            TEXT         NOT NULL,
  fingerprint          TEXT         NOT NULL,
  watermark            BIGINT       NOT NULL,
  marked_for_deletion  BOOLEAN      NOT NULL  DEFAULT false,
  created_at           TIMESTAMPTZ  NOT NULL  DEFAULT NOW()
)
''';

const String _viewCopiesFingerprintIdx = '''
CREATE UNIQUE INDEX IF NOT EXISTS view_copies_unmarked_fingerprint_idx
  ON view_copies (fingerprint)
  WHERE NOT marked_for_deletion
''';

// The generic view store keys rows by copy id, not by view name, once a
// view is stored per fingerprinted copy.
const String _viewRowsRenameColumn = '''
ALTER TABLE view_rows RENAME COLUMN view_name TO copy_id
''';

// The source aggregate id a TableProjectionSpec row's producing insert
// event named, null for a row no such insert wrote (an AggregateProjectionSpec
// row, or a TableProjectionSpec row from a build before this column exists).
// `PostgresBackend.upsertTableViewRowInTxn` is the only write; the generic
// `upsertViewRowInTxn`'s ON CONFLICT clause never touches it, so the
// outstanding-finding refresh's rewrite (which goes through the generic
// upsert to change only `$integrity`) leaves a row's producer intact. The
// partial index over the non-null column serves
// `PostgresBackend.findTableRowsBySourceAggregateInTxn`'s lookup
// (`EVS-PRD-materializer/E`).
const String _viewRowsSourceAggregateColumn = '''
ALTER TABLE view_rows
  ADD COLUMN IF NOT EXISTS source_aggregate_id TEXT
''';

const String _viewRowsSourceAggregateIdx = '''
CREATE INDEX IF NOT EXISTS view_rows_source_aggregate_idx
  ON view_rows (copy_id, source_aggregate_id)
  WHERE source_aggregate_id IS NOT NULL
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

// The delivery a queue item was acknowledged under: the channel's
// generation, the delivery number and the delivery hash. Null until the
// change that marks the item sent under a delivery.
const String _fifoEntriesDeliveryColumns = '''
ALTER TABLE fifo_entries
  ADD COLUMN IF NOT EXISTS delivery_generation  BIGINT,
  ADD COLUMN IF NOT EXISTS delivery_number      BIGINT,
  ADD COLUMN IF NOT EXISTS delivery_hash        TEXT
''';

// A transform-failed item's flag and the transform failures the fill
// recorded on it: set only by the insert, held immutable by the guard like
// every other column the item is enqueued with.
const String _fifoEntriesTransformFailedColumns = '''
ALTER TABLE fifo_entries
  ADD COLUMN IF NOT EXISTS transform_failed     BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS transform_failures    INTEGER
''';

// The retained-delivery read: the sent item at a number under a
// generation, the latest first.
const String _fifoEntriesDeliveryIdx = '''
CREATE INDEX IF NOT EXISTS fifo_entries_delivery_idx
  ON fifo_entries (destination_id, delivery_generation, delivery_number,
                   sequence_in_queue)
  WHERE final_status = 'sent'
''';

const String _fifoEntriesHeadIdx = '''
CREATE INDEX IF NOT EXISTS fifo_entries_head_idx
  ON fifo_entries (destination_id, sequence_in_queue)
  WHERE final_status IS NULL OR final_status = 'wedged'
''';

// The queue table's guard. The library changes a queue item only in these
// shapes: it inserts an item pending, with no attempts, no delivery time and
// no delivery; it appends one attempt to a pending item; it marks a pending
// item sent (stamping the delivery time and, on a delivery channel, the
// delivery it was acknowledged under) or wedged, appending at most the
// attempt that decided it; it tombstones a wedged item, and a pending item
// that carries attempts (a resume or a new generation of its channel); and
// it deletes pending items that carry no attempt. The guard refuses every
// change outside those shapes, whatever role makes it: no item is inserted
// terminal or delivered, rewrites what it was enqueued with or the delivery
// it was acknowledged under, or loses a terminal or attempted item. It
// checks the shape of a change, not who makes it, so a hand-written change
// of a legal shape passes (wedging or marking sent a pending item,
// tombstoning a wedged one, deleting a pending one that carries no attempt,
// inserting a pending one); those rest on the storage precondition. The
// triggers fire in every session replication role, and the role that owns
// the table can drop them; the runtime role cannot.
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
    IF NEW.delivery_generation IS NOT NULL
       OR NEW.delivery_number IS NOT NULL
       OR NEW.delivery_hash IS NOT NULL THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is inserted with a delivery; an item is inserted with none',
        NEW.entry_id USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;

  IF TG_OP = 'DELETE' THEN
    IF OLD.final_status IS NOT NULL THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % is %; a terminal item is never deleted',
        OLD.entry_id, OLD.final_status USING ERRCODE = 'check_violation';
    END IF;
    IF OLD.attempts IS DISTINCT FROM '[]'::jsonb THEN
      RAISE EXCEPTION 'fifo_entries_guard: queue item % carries attempts; an item carrying attempts is never deleted',
        OLD.entry_id USING ERRCODE = 'check_violation';
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
    ('pending', 'tombstoned'),
    ('wedged', 'tombstoned')
  ) THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % cannot change status from % to %; the legal changes are pending to sent, pending to wedged, wedged to tombstoned and, for an item carrying attempts, pending to tombstoned',
      OLD.entry_id, old_status, new_status USING ERRCODE = 'check_violation';
  END IF;
  IF (old_status, new_status) = ('pending', 'tombstoned')
     AND OLD.attempts IS NOT DISTINCT FROM '[]'::jsonb THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % carries no attempt; only an item carrying attempts changes from pending to tombstoned',
      OLD.entry_id USING ERRCODE = 'check_violation';
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
     OR NEW.transform_failed IS DISTINCT FROM OLD.transform_failed
     OR NEW.transform_failures IS DISTINCT FROM OLD.transform_failures
     OR NEW.enqueued_at IS DISTINCT FROM OLD.enqueued_at THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % changes a column it was enqueued with',
      OLD.entry_id USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.sent_at IS DISTINCT FROM OLD.sent_at
     AND (old_status, new_status) <> ('pending', 'sent') THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % changes its delivery time outside the change that marks it sent',
      OLD.entry_id USING ERRCODE = 'check_violation';
  END IF;
  IF (NEW.delivery_generation IS DISTINCT FROM OLD.delivery_generation
      OR NEW.delivery_number IS DISTINCT FROM OLD.delivery_number
      OR NEW.delivery_hash IS DISTINCT FROM OLD.delivery_hash)
     AND (old_status, new_status) <> ('pending', 'sent') THEN
    RAISE EXCEPTION 'fifo_entries_guard: queue item % changes its delivery outside the change that marks it sent',
      OLD.entry_id USING ERRCODE = 'check_violation';
  END IF;
  -- The array checks come first and on their own, since the length
  -- functions raise on anything else.
  IF NEW.attempts IS DISTINCT FROM OLD.attempts THEN
    IF old_status <> 'pending'
       OR new_status NOT IN ('pending', 'sent', 'wedged')
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
