# Postgres Backend

**Status**: Stable.

The substrate's persistence contract is exposed behind the abstract
`StorageBackend` interface. Two reference implementations ship in-tree:

- `SembastBackend` — mobile / Flutter deployments (sembast-on-disk).
- `PostgresBackend` — server-side deployments (Cloud SQL / managed Postgres).

Both pass the same backend-agnostic conformance harness. This document is
the cross-system narrative for the `PostgresBackend` design: the choices
that made it ship, the alternatives that didn't, and the follow-up work
that remains. The normative DEV-level obligations live in
[`dev-postgres-backend.md`](dev-postgres-backend.md).

## Why

Phase IV (portal+server cutover) needs a server-side `StorageBackend`.
The substrate is backend-agnostic via the abstract `StorageBackend`
interface. Shipping a second concrete impl alongside `SembastBackend`
serves three purposes:

- **Activates the deferred backend-portability commitment.** The
  `EVS-PRD-portability` PRD asserts that the substrate runs on every
  Dart-supported runtime. Postgres is the first concrete server-side
  backend to prove this for server deployments.
- **Unblocks Phase IV.** The portal-server / diary-server / portal-UI
  cutover needs a backend it can actually deploy on Cloud SQL. Sembast
  on a server is technically possible but operationally awkward.
- **Hardens the `StorageBackend` contract.** Having two impls that both
  pass the conformance harness verifies that the abstraction boundary
  holds and that no sembast-specific behavior leaks through. The
  conformance harness covers both backends with the same assertions.

## Open architectural decision: view-row representation

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
operators (`row_data->>'field'`) rather than typed columns. The
portal-side consumers of view rows query through the substrate's
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

The Postgres schema is a small, fixed set of tables emitted at `open()`
time via `CREATE TABLE IF NOT EXISTS`. Each table maps one-for-one to a
sembast store the reference impl uses today; the contents are the same
`StoredEvent` / view-row / FIFO-entry / KV shapes the substrate already
operates on. The tables are:

- **`events`** — the append-only event log. Columns include `sequence`
  (BIGINT PRIMARY KEY), `entry_type` (TEXT), `entry_type_version`
  (INTEGER), `aggregate_id` (TEXT), `event_id` (TEXT UNIQUE),
  `payload` (JSONB), `prev_hash` (TEXT), `hash` (TEXT),
  `client_timestamp` (TIMESTAMPTZ), `originator_hop_id` (TEXT),
  `originator_identifier` (TEXT), and a `metadata` JSONB column for the
  remainder of `StoredEvent`'s fields. Secondary indexes on
  `aggregate_id`, `entry_type`, and `client_timestamp` support the
  filter combinations enumerated in
  `EVS-DEV-find-all-events-extended-filters`.
- **`view_rows`** — single table for every materialized view, keyed by
  `(view_name TEXT, row_key TEXT)` with `row_data JSONB` payload and an
  `updated_at TIMESTAMPTZ` audit column. `findViewRows` walks
  `view_name = ?` ordered by `row_key`.
- **`view_target_versions`** — the per-view target-version map
  maintained by `EventStore.open`'s snapshot-promotion pass.
  Single-row-per-view KV; columns `view_name TEXT PRIMARY KEY`,
  `target_version INTEGER`.
- **`fifo_entries`** — single table for every outbound FIFO queue,
  keyed by `(destination_id TEXT, sequence_in_queue BIGINT)` with the
  queued event reference and delivery bookkeeping columns
  (`event_sequence BIGINT`, `enqueued_at TIMESTAMPTZ`,
  `last_attempt_at TIMESTAMPTZ NULL`, `attempt_count INTEGER`,
  `state TEXT`).
- **`backend_state`** — the substrate's general-purpose KV bookkeeping
  area (library-version watermark, current sequence counter, last-hash
  cache, originator identity). Columns `key TEXT PRIMARY KEY`,
  `value JSONB`.
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
- Schema DDL is emitted at backend `open()` time as `CREATE TABLE IF
  NOT EXISTS` statements; sembast creates stores lazily on first write.
  The upfront DDL makes Postgres deployments observable (a freshly-
  opened DB has the tables present even before any events are
  appended), which matters for ops tooling.
- JSONB payloads accept native Postgres JSON operators on the
  underlying column, but the substrate's API surface does not expose
  them; all reads go through the abstract `StorageBackend` methods.

## Decisions rejected

- **Per-spec typed columns for view rows.** Rejected for v1; deferred
  to a future `SqlNativeTableProjectionSpec` primitive shipped under
  the Append-Only Primitives discipline.
- **Per-destination FIFO tables.** Rejected; cleaner DDL surface with a
  single table; observable behavior unchanged.
- **READ COMMITTED transactions.** Rejected; risk of phantom-read
  corrupting the sequence counter under concurrent writers.

## Open questions

- None blocking. Reactive change streams and connection pool sizing are
  scoped follow-ups tracked in "Future work" below, not design gaps.

## Future work

- **Reactive `subscribe<T>` over Postgres.** The sembast backend emits
  `StoredEvent` on a `StreamController.broadcast` after each commit;
  `subscribe<T>` consumes that stream. The Postgres backend does not
  implement an equivalent stream — `subscribe<T>` over Postgres will
  need polling or `LISTEN`/`NOTIFY` plumbing, and the choice between
  them depends on the portal-server load profile. Follow-up ticket.
- **Connection pool sizing for the portal load profile.** The current
  `PostgresBackend` opens a single connection; portal-server deployment
  will need a pool. Sizing depends on concurrent action submissions and
  the read load from `findViewRows` calls, neither of which is
  characterised yet. Follow-up alongside Phase IV portal-server
  deployment.
