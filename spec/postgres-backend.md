# Postgres Backend

**Status**: Stable.

The substrate's persistence contract is exposed behind the abstract
`StorageBackend` interface. Two reference implementations ship in-tree:

- `SembastBackend` — mobile / Flutter deployments (sembast-on-disk).
- `PostgresBackend` — server-side deployments (self-managed or managed Postgres).

Both pass the same backend-agnostic conformance harness. This document is
the cross-system narrative for the `PostgresBackend` design: the choices
that made it ship, the alternatives that didn't, and the follow-up work
that remains. The normative DEV-level obligations live in
[`dev-postgres-backend.md`](dev-postgres-backend.md).

## Why

Server-side deployments need a server-side `StorageBackend`.
The substrate is backend-agnostic via the abstract `StorageBackend`
interface. Shipping a second concrete impl alongside `SembastBackend`
serves three purposes:

- **Activates the backend-portability commitment.** The
  `EVS-PRD-portability` PRD asserts that the substrate runs on every
  Dart-supported runtime. Postgres is the first concrete server-side
  backend to prove this for server deployments.
- **Unblocks server-side deployment of the full substrate.** A
  server-side deployment needs a backend it can actually deploy on a
  managed Postgres service. Sembast on a server is technically possible but
  operationally awkward.
- **Hardens the `StorageBackend` contract.** Having two impls that both
  pass the conformance harness verifies that the abstraction boundary
  holds and that no sembast-specific behavior leaks through. The
  conformance harness covers both backends with the same assertions.

## Architectural decision: view-row representation

Three candidates were considered for materialized-view row storage:

1. **JSONB blob per row** — single table `view_rows(view_name, row_key,
   row_data JSONB, updated_at)`. Closest fit to sembast's semantics.
2. **Per-spec typed columns** — emit a dedicated table per
   `TableProjectionSpec` with typed columns matching the spec's field
   shape.
3. **Hybrid** — typed columns for index-friendly fields plus a JSONB
   spillover for the rest.

**Chosen: JSONB blob per row** (option 1).

The tradeoff: SQL-native queries on view contents go through JSONB
operators (`row_data->>'field'`) rather than typed columns. Consumers
of view rows query through the substrate's
`findViewRows` API today; they do not reach past the abstraction to
query the table directly. Choosing JSONB now commits to that pattern.

The migration path to per-spec typed cols (or hybrid) is open: if a
downstream deployment needs SQL-native view queries, add a new
`SqlNativeTableProjectionSpec` primitive under the Append-Only
Primitives discipline. The JSONB-blob layout stays as the default for
`TableProjectionSpec`.

## Layer 1 vs Layer 2 framing

Per CLAUDE.md's "Epistemic layers" section, the substrate makes two
kinds of claims and the distinction is load-bearing here.

The **JSONB-blob view-row decision is Layer 2** — the library's chosen
interpretation of how to materialize `TableProjectionSpec` outputs on
Postgres. Applications needing different materializations build them on
top of Layer 1 facts via `subscribe<T>(_, Events())` or
`EventStore.read(...)`, or via future substrate primitives.

The **transactional atomicity assertion is Layer 1** — a hard
cryptographic / structural guarantee about the backend. SERIALIZABLE
BEGIN/COMMIT around each `transaction<T>` body ensures concurrent
`nextSequenceNumber` callers cannot stamp duplicate sequence numbers,
and that row writes inside a `transaction<T>` body either all commit
together with the event append or none do. This is the same atomicity
guarantee sembast provides via its in-process transaction handle, just
realized through a different mechanism.

## Schema overview

The Postgres schema is a small, fixed set of tables that
`PostgresBackend.provision` creates, as the ordered migration steps of
`postgres_schema.dart`, in a provisioning step of its own. Each table maps one-for-one to a
sembast store the reference impl uses today; the contents are the same
`StoredEvent` / view-row / FIFO-entry / KV shapes the substrate already
operates on. The tables are:

- **`events`** — the append-only event log. Columns: `sequence_number`
  (BIGINT PRIMARY KEY), `event_id` (TEXT UNIQUE), `aggregate_id`,
  `aggregate_type`, `entry_type`, `event_type` (TEXT), the entry-type
  version and the data-format version as major and minor `INTEGER` columns
  (`entry_type_version_major`, `entry_type_version_minor`,
  `lib_format_version_major`, `lib_format_version_minor`) beside the two
  version maps as the event hash covers them (`entry_type_version_json`,
  `lib_format_version_json`, JSONB), `data`, `metadata` and `initiator`
  (JSONB), `client_timestamp` (TIMESTAMPTZ) with `client_timestamp_text`
  (TEXT, the timestamp's string as the event hash covers it),
  `event_hash` and `previous_event_hash` (TEXT), `flow_token` (TEXT), and
  `unknown_fields` (JSONB, the record's top-level keys the library does
  not read, as they arrived).
  Secondary indexes on `(aggregate_id, sequence_number)`,
  `client_timestamp` and `(event_type, sequence_number)` support the
  filter combinations enumerated in
  `EVS-DEV-find-all-events-extended-filters` and the boot's read of the
  library-version events.
- **`view_rows`** — single table for every materialized view, keyed by
  `(view_name TEXT, row_key TEXT)` with `row_data JSONB` payload and an
  `updated_at TIMESTAMPTZ` audit column. `findViewRows` walks
  `view_name = ?` ordered by `row_key`.
- **`view_target_versions`** — the per-view target-version map. One row per (view, entry type) the view's interest names, and one whole-view row, with entry type `*` and no target, per view whose interest names no entry type. Columns: `view_name TEXT`, `entry_type TEXT`, `target_major INTEGER` and `target_minor INTEGER` (null on a whole-view row); keyed by `(view_name, entry_type)`.
- **`view_convergence_gaps`** — each pair's convergence gaps, keyed by `(view_name, entry_type, kind)` with `kind TEXT` (`catch_up` or `promotion`). Columns: `token BIGINT`, `from_seq BIGINT`, `whole_log BOOLEAN`, and, for a promotion gap, `version_limited BOOLEAN`, `round_major INTEGER`, `round_minor INTEGER`, `prior_major INTEGER` and `prior_minor INTEGER`.
- **`view_convergence_aggregates`** — the aggregates a gap or a round names, has planned or holds as re-derived, keyed by `(view_name, entry_type, kind, holder, row_key)` with `holder TEXT` (`gap` or `round`) and a `state TEXT` column (`named`, `planned` or `rederived`).
- **`view_convergence_rounds`** — one row per view with a round in progress: `view_name TEXT` (the key), `stamp BIGINT`, the gaps and tokens it took (`taken JSONB`), `round_from_seq BIGINT`, `planned_through_seq BIGINT` (null once planning has finished), `table_deleted BOOLEAN`, `refold_through_seq BIGINT`, `units_done BIGINT`, `units_counted BIGINT`, `began_at TIMESTAMPTZ`, `longest_txn_ms INTEGER`, `failures INTEGER` and `last_error TEXT`.
- **`library_roles`** — the runtime and lock roles the deployment declared at provisioning, one row per role with its kind (`runtime` or `lock`); only the owner writes it, and opening a backend reads it (EVS-DEV-postgres-backend/P).
- **`fifo_entries`** — single table for every outbound FIFO queue,
  keyed by `(destination_id TEXT, sequence_in_queue BIGINT)`. Each row is
  one queue item: `entry_id` (TEXT UNIQUE), the events it carries
  (`event_ids` JSONB, `event_id_first_seq`, `event_id_last_seq`), how it
  was built (`wire_format`, `transform_version`, `wire_payload`,
  `envelope_metadata`), `enqueued_at`, and its delivery bookkeeping:
  `attempts` (a JSONB array of recorded attempts), `final_status` (null
  while pending, then `sent`, `wedged` or `tombstoned`) and `sent_at`.
  The table is guarded (EVS-DEV-destination-drain/S): a CHECK
  (`fifo_entries_final_status_check`) constrains `final_status`, and the
  trigger `fifo_entries_guard` refuses every change outside the shapes of
  the library's own writes: any insert of an item that is not pending with
  no attempts and no `sent_at`; any status change but pending to sent,
  pending to wedged and wedged to tombstoned; any change to a column the
  item was enqueued with; any change to `attempts` but appending one
  attempt while pending (in the change that keeps it pending or marks it
  sent or wedged); any change to `sent_at` outside the change that marks
  the item sent; and the deletion of a terminal item. A statement trigger,
  `fifo_entries_truncate_guard`, refuses every truncation. Both triggers
  are enabled `ALWAYS`, so they fire in every session replication role.
  The guard checks the shape of a change, not who makes it: a hand-written
  change of a legal shape (wedging, marking sent or deleting a pending
  item, tombstoning a wedged one, inserting a pending one) passes, and
  rests on the storage precondition. The guard catches defects and
  hand-written SQL of any other shape; the role that owns the table can
  drop it, which the runtime role cannot (see "Roles and privileges").
- **`backend_state`** — the substrate's general-purpose KV bookkeeping
  area (library-version watermark, current sequence counter, last-hash
  cache, originator identity, the provisioned schema version pair, the
  database's generation record and the records that map the generation
  guard's lock keys back to their components, the drain epoch
  (`drain_epoch`), the drainer's declaration (`drainer_declaration`) and
  heartbeat (`drain_heartbeat`), and each destination's refill guard
  (`refill_guard_<destination>`)). Columns
  `key TEXT PRIMARY KEY`, `value JSONB`.
- **`security_context`** — the persisted role/permission/scope snapshot
  the substrate maintains for closed-under-events authorization
  evaluation. Schema mirrors the sembast layout; one logical row per
  (principal, role) pair stored as JSONB for symmetry with `view_rows`.
- **`idempotency`** — action-dispatch idempotency entries keyed by
  `(action_name TEXT, principal_id TEXT, idempotency_key TEXT)` with
  the recorded outcome payload (`outcome JSONB`) and audit timestamps.
  TTL policy is enforced by the substrate's action-dispatch path; the
  table simply records entries.

Full DDL — column types, NOT NULL constraints, indexes, foreign keys —
lives in `postgres_schema.dart`. This section is the narrative
orientation; the DDL file is the source of truth.

## Transactional model

- `StorageBackend.transaction<T>` maps to Postgres `BEGIN ... COMMIT` at
  SERIALIZABLE isolation.
- The sequence counter is a single row in `backend_state` (key
  `current_sequence`); SERIALIZABLE isolation makes concurrent
  `nextSequenceNumber` calls serialize as expected. The substrate is
  single-writer-per-source by design; this just prevents accidental
  concurrent writers from silently corrupting the chain.
- A transaction that wrote `backend_state` (every append does, through
  the sequence counter) and lost a serialization race is re-run after
  `LOCK TABLE backend_state IN SHARE ROW EXCLUSIVE MODE`, taken before
  its snapshot, so the re-run waits for the writes it lost to and cannot
  lose the same race again. The runtime role therefore needs a privilege
  that `SHARE ROW EXCLUSIVE` requires on `backend_state` (it writes the
  table anyway: `INSERT`, `UPDATE` and `DELETE`). A re-run of a
  transaction that wrote nothing to `backend_state` takes no lock, so a
  read-only role needs only `SELECT`. While the lock is held every other
  write to `backend_state`, and so every append, waits.

## Roles and privileges

The library assumes three kinds of database role, and the deployment
creates them (EVS-DEV-postgres-backend/K):

- **Owner.** Owns the schema and the tables. Provisioning
  (`PostgresBackend.provision`, which also records the declared runtime and
  lock roles) runs as this role, in its own deployment step; it is the
  one library operation the runtime role cannot perform. No process serves
  traffic as the owner: the owner can disable or drop the queue table's
  guard and rewrite the log.
- **Runtime.** The role an application's `PostgresBackend` opens its pool
  and, unless `lockUrl` names another role, its lock session as. It holds
  `USAGE` on the schema and exactly the table privileges below (exported
  as `postgresRuntimeRoleGrants`), and every library operation other than
  provisioning works under them. The log is append-only for it. For a
  lock session opened as another role, `USAGE` on the schema and the
  runtime role's privileges on `backend_state` suffice; every lock role
  must be allowed to end its own sessions, as the role that owns them is.
- **Read-only.** Reporting and inspection hold `SELECT` only. No person
  holds write access.

The split holds only if neither the runtime role nor any lock role can
become the owner or create objects in the schema. Each of them:

- does not own the schema or any table in it, and is not a member of the
  owning role;
- holds neither `SUPERUSER` nor `CREATEROLE`, and is not a member of any
  role that carries them (a hosting platform's administrative role
  included), so it cannot grant itself the owner's membership. Create it
  with plain SQL or as a platform identity that carries no such
  membership, and check its attributes and memberships on the platform's
  server;
- holds no `CREATE` on the schema: the schema grants `CREATE` to no role
  but the owner. A server's default `public` schema grants `CREATE` to
  every role on Postgres majors before 15, so a deployment on `public`
  runs `REVOKE CREATE ON SCHEMA public FROM PUBLIC`; a schema the owner
  creates grants nothing to `PUBLIC`.

The order of a deployment is: create the schema for the owner and grant
the runtime and lock roles `USAGE` on it; provision as the owner; grant
the runtime role the privileges below (and a lock role its
`backend_state` privileges); then start the new build's instances.
Provisioning commits its DDL on its own, before the grants, so an instance
of the new build started before the grants fails with a permission error
on a table the provisioning added.

The library is built and tested against PostgreSQL 16; that is the
supported server major.

Grants cannot separate the library from the application that embeds it, because the application supplies the credentials of the library's roles in the storage description. They separate the process from the schema, and every other role from the library's tables.

Opening a backend checks the grants and memberships the server records. It refuses a role not declared at provisioning, a runtime or lock role that could change the schema, and a database on which a role outside the owner and the declared roles may write a library table (through a grant, `pg_write_all_data`, or membership in the owner or a declared role) (EVS-DEV-postgres-backend/M+N+P).

What remains is the storage precondition (EVS-PRD-destinations/L): code that connects with the library's credentials, and the database's administrators, meaning the owner, superusers, and roles holding `CREATEROLE` or the admin option over a library role.

The queue table carries a database guard (above) that refuses changes outside the shapes of the library's writes. `backend_state` has no such guard. It holds the fill positions, schedules, replay requests, wedge records, halt requests, send fences, refill guards, the drain epoch, the drainer's declaration and heartbeat, the generation records and the database identity.

### An application's own tables

An application that keeps tables of its own in the same database (EVS-DEV-postgres-backend/O):

- creates a schema of its own, which the library does not provision, and keeps its tables there;
- connects under an application role of its own, through a pool it opens itself, never through the library's;
- grants the application role no privilege on the library's tables beyond `SELECT`, and no membership that lets it inherit or set a library role, the owner or `pg_write_all_data`;
- on a server before Postgres 15, revokes `CREATE` on the library's schema from `PUBLIC`.

The database then refuses the application role every insert, update, delete and truncation of a library table. A grant that would allow one makes the library refuse to open the database.

### Runtime role privileges

| Table | Privileges |
| --- | --- |
| `events` | SELECT, INSERT |
| `view_rows` | SELECT, INSERT, UPDATE, DELETE |
| `view_target_versions` | SELECT, INSERT, UPDATE, DELETE |
| `fifo_entries` | SELECT, INSERT, UPDATE, DELETE |
| `backend_state` | SELECT, INSERT, UPDATE, DELETE |
| `security_context` | SELECT, INSERT, UPDATE, DELETE |
| `idempotency` | SELECT, INSERT, UPDATE, DELETE |
| `view_convergence_gaps` | SELECT, INSERT, UPDATE, DELETE |
| `view_convergence_aggregates` | SELECT, INSERT, UPDATE, DELETE |
| `view_convergence_rounds` | SELECT, INSERT, UPDATE, DELETE |
| `library_roles` | SELECT |

Besides these, the runtime role holds `USAGE` on the schema. `UPDATE` on
`backend_state` also covers the table lock a re-run transaction takes and
the share lock the drain-epoch read takes (see "Transactional model").

## What's the same as sembast

The `PostgresBackend` implements exactly the abstract `StorageBackend`
interface that `SembastBackend` does today — same method signatures,
same `StoredEvent` shape on the wire between substrate and backend,
same `Transaction` handle lifetime rules, same return-value contracts. The
substrate calls into the backend identically regardless of which impl
is wired in at composition time; tests written against the conformance
harness exercise both with the same assertions.

FIFO delivery semantics (per-destination ordering, at-least-once on
retry) are preserved verbatim; only the on-disk layout changes. The
`backend_state` KV bookkeeping (library-version watermark,
current-sequence cache, last-hash cache, originator identity) keeps its
existing keys and value shapes, just relocated from sembast's main
store to the dedicated `backend_state` table.

## What's different from sembast

- View rows are JSONB blobs in a single `view_rows` table, not one
  sembast store per view name. The substrate iterates a view's rows
  through `findViewRows(viewName)`, which is now a `SELECT ... WHERE
  view_name = ?` instead of a per-store walk.
- FIFO is one `fifo_entries` table keyed by `(destination_id,
  sequence_in_queue)`, not per-destination sembast stores. Adding a new
  destination is a no-op at the DDL level; sembast's lazy-store
  creation is replaced by row inserts into the shared table.
- Schema DDL runs in a provisioning step (`PostgresBackend.provision`)
  that a deployment runs once, before its instances open the database;
  `open` runs no DDL and verifies the stored schema version and minimum
  compatible version (EVS-DEV-postgres-backend/G, H). Sembast creates
  stores lazily on first write. The upfront DDL makes Postgres
  deployments observable (a freshly provisioned database has the tables
  present even before any events are appended), which matters for ops
  tooling.
- Each backend holds a dedicated lock session besides its pool, on which
  it holds the incompatible-generation guard's advisory locks
  (EVS-DEV-postgres-backend/J, EVS-DEV-version-compatibility/F to I). The
  lock connection must be one real server session: a direct connection
  to the database, or a session-mode proxy that resets sessions on
  release, never a transaction-mode pooler. `open` checks that three
  separate statements reach one server session carrying a setting the
  first made and that the session reaches the pool's database and schema,
  but the check can miss a pooler that happens to hand back the same
  server connection every time. The library sets server-side keepalives
  and no idle-session timeout on it; a proxy between the process and the
  database has client-side timeouts of its own, which the deployment
  configures. The lock role must be allowed to end its own sessions.
- The drain lock lives on the same lock session: a session advisory lock
  whose key derives from the database, the schema and the database
  identity (EVS-DEV-destination-drain-lock/A). Every acquisition raises
  `drain_epoch` in a transaction on the lock session after confirming the
  key and the identity, then checks through the pool that the lock
  session holds the key; every queue-changing transaction of the drainer
  reads `drain_epoch` under a share lock as its first step
  (EVS-DEV-destination-drain-lock/B). When the lock session is declared
  lost, its replacement ends the old server session if it still holds
  the drain key, as for the generation locks.
- JSONB payloads accept native Postgres JSON operators on the
  underlying column, but the substrate's API surface does not expose
  them; all reads go through the abstract `StorageBackend` methods.

## Decisions rejected

- **Per-spec typed columns for view rows.** Rejected for 0.x; deferred
  to a future `SqlNativeTableProjectionSpec` primitive shipped under
  the Append-Only Primitives discipline.
- **Per-destination FIFO tables.** Rejected; cleaner DDL surface with a
  single table; observable behavior unchanged.
- **READ COMMITTED transactions.** Rejected; risk of phantom-read
  corrupting the sequence counter under concurrent writers.

## Future work

Deferred work for this area is recorded in `spec/roadmap/storage.md`.
Two clarifications about the shipped baseline, to head off common
misreadings:

- **Same-process reactive `subscribe<T>` works on Postgres.** Live
  updates flow through the `EventStore`-level post-commit publish bus
  (`SubscriptionEngine`), which is backend-agnostic — nothing about
  reactive subscribe is bound to sembast's change notifications. The
  open item is only cross-process notification (another process's
  writes against the same database are invisible without polling).
- **`PostgresBackend` already runs over a connection pool.** It uses a
  `Pool` (`maxConnectionCount: 4`) with SERIALIZABLE transactions and
  serialization-failure retry; the only open item is exposing the pool
  size as an `open()` parameter.
