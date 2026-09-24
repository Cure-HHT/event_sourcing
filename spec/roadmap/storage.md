# Roadmap — storage backends

Deferred work on the `StorageBackend` layer and its two reference
implementations (`SembastBackend`, `PostgresBackend`). Each item states
what already exists in code and what remains.

## Cross-process change notification for Postgres

**Baseline.** Same-process reactive `subscribe<T>` works fully on
`PostgresBackend`. Live updates flow through the `EventStore`-level
post-commit publish bus (`SubscriptionEngine`), which is backend-agnostic
— the `EventStore` publishes each committed event and row change to its
subscribers regardless of which backend persisted it. Nothing about
reactive subscribe is bound to sembast's change notifications.

The delivery cycle of the process that drains a database is woken at once
by the appends and registry operations of its own process, and by its
cadence timer (`SyncCycle.start`'s `cadence`) for everything other
processes commit: another process's appends and halt requests reach the
drainer at its next pass, at most one cadence after its current pass ends
(the timer is armed again when a pass ends, so a long pass or a hanging
send holds them off). The cadence alone guarantees that the drainer picks
every change up.

**Remaining.** The gap is cross-process only. Writes made by another
process against the same database produce no emissions in this process,
because the publish bus is in-memory and per-`EventStore`. A multi-process
Postgres deployment therefore polls `findViewRows` to observe another
process's writes. Closing this needs a cross-process bridge — Postgres
`LISTEN`/`NOTIFY`, or a polling loop — feeding the same subscription bus.

The same bridge wakes the drainer. The transaction that appends an event,
or that commits a registry operation, issues a `NOTIFY` on the database's
channel; Postgres delivers a notification only when that transaction
commits, and coalesces identical notifications within it, so a burst of
appends in one transaction wakes the drainer once. The draining process
listens on its dedicated lock session, which it already holds for its
lifetime, and runs a pass when a notification arrives. The cadence timer
stays as the fallback: notifications are not stored, so one sent while the
listener's session was being replaced, or while no process drained, is
lost, and the next pass picks the change up. This is a latency
optimization only; correctness rests on the cadence.

## Configurable connection-pool sizing

**Baseline.** `PostgresBackend` already runs over a `package:pool` `Pool`
(`maxConnectionCount: 4`) with SERIALIZABLE transactions and
serialization-failure retry (SQLSTATE 40001 / 40P01, up to 8 attempts);
the pool is shared with the idempotency store.

**Remaining.** Expose the pool size as an `open()` parameter so a
deployment can tune it to its concurrent-write and read-load profile.
The pooling machinery itself is in place.

## SQL-native view rows (`SqlNativeTableProjectionSpec`)

**Baseline.** `view_rows` is stored as opaque JSONB blob rows — one
`(view_name, row_key, row_data JSONB, updated_at)` table for every view —
and the `StorageBackend` contract traffics in opaque row maps. SQL-native
queries on view contents go through JSONB operators rather than typed
columns.

**Remaining.** A `SqlNativeTableProjectionSpec` primitive that emits
typed columns per spec, for deployments that need SQL-native view
queries. It ships under the Append-Only Primitives discipline as a new
spec shape; the JSONB-blob layout stays the default for
`TableProjectionSpec`.

## Additional backends

**Baseline.** Exactly two `StorageBackend` implementations exist. The
in-memory test configuration is `SembastBackend` over sembast's memory
factory — not a separate backend — and both implementations pass the
same conformance harness at
`event_sourcing/test/storage/storage_backend_conformance.dart`.

**Remaining.** Any third backend (SQLite, IndexedDB, a first-class
in-memory backend, etc.). Per the trust-boundary model these are
app-supplied: each deployment's backend is the trusted persistence layer
for that deployment, and any new backend earns trust by passing the
conformance harness.

## Horizontal scaling beyond a single backend instance

**Baseline.** The reference `PostgresBackend` (and the abstract
`StorageBackend` contract) target a single backing store per substrate
instance. High-volume workloads — the sensor-network sketch
(`docs/scenarios/iot-sensor-network.md`) reaches millions of events per
day per fleet — can exceed a single-Postgres deployment's headroom.
One horizontal path works today: **substrate-per-shard**, deploying one
substrate per logical shard (per-tenant, per-region) with an app-layer
aggregator subscribing across shards via separate `RemoteScope`
connections; the audit story is "per-shard log" rather than "one global
log," which is the right answer for tenant-isolated deployments.

**Remaining.** **Backend-side partitioning** — a `ShardedPostgresBackend`
(or similar) that partitions the event table across multiple Postgres
instances by `(originatorId, aggregateType)` or by sequence range while
presenting the substrate with a single logical `StorageBackend`. It
requires a non-trivial cross-shard sequence-number coordination strategy,
and the hash chain's per-installation linearity is what makes integrity
verifiable in the first place — so any partitioning impl is a downstream
extension under the same trust-boundary discipline, not a free lunch.

## Closing consumer access to internal storage members

**Baseline.** Every `StorageBackend` member that writes, every raw
handle to the database or to an engine transaction, and every event-store
operation that appends a reserved system event, is marked `@internal`, so
a consumer's call to one is an analyzer error (`EVS-PRD-destinations/K`). The guarantee is stated as a precondition of
the storage trust boundary (`EVS-PRD-destinations/L`): the consumer
constructs the backend and holds it, on Sembast it also holds the
`Database` it opened, and a direct write to the library's persisted state
is invisible to the library.

**Remaining.** A run-time barrier: for example, a capability-scoped
backend handle that the library keeps and the consumer never receives,
exposing to the consumer only the reads, `transaction` and `close`, and a
Sembast construction path in which the library opens the database itself.

## A browser tab whose database handle cannot commit

**Baseline (what exists).** sembast_web compacts a database when a
handle that may write opens it while a deleted record is on file. A
handle in another tab that has not seen the commits up to the compaction
then fails every commit: its reload after a failed commit keeps its
stale revision, and sembast re-runs the transaction body without end.
A transaction that keeps losing its commit to other tabs runs again
holding the database's write lock exclusively, so contention alone never
fails it; a handle that still cannot commit there throws
`TransactionRerunLimitException`, which names closing and reopening the
database as the recovery (`EVS-PRD-event-log/E`), and every later
transaction on the handle fails at once. A delivery cycle over that
handle stops, releases the drain lock so another tab drains, and reports
the exception as `SyncCycle.stopCause`; the application reopens.

**Remaining.** Recover without the application. The defect lies in
sembast's reload, which updates a handle's revision after a delta reload
but not after a full one; once it is fixed upstream the bound only
guards. The library cannot reopen the database itself, because the
application opens it and hands the library the handle.

## Verifying the inputs the generation guard and the drain lock trust

**Baseline.** Three inputs of the incompatible-generation guard and the
drain lock are trusted without a pluggable interface (`CLAUDE.md`, "Trust
boundaries", and `EVS-PRD-library-charter` assertion H). The Postgres
lock-session path (`lockUrl`, or the pool's URL) is trusted to be one
server session reaching the pool's server, database and schema, with
keepalives and a role that may end its own sessions; `PostgresBackend.open`
checks what it can (a session setting read back in separate statements
with the server process id, the database and schema, an advisory lock a
pool connection takes), and the check can miss a pooler that returns the
same server connection every time. The browser's lock manager is trusted
to grant, report and release locks as the Web Locks API specifies. Outside
the browser, a Sembast database file is trusted to be opened by one
isolate of one process.

**Remaining.** Reduce each to a checked property. For the lock session:
detect a pooler by observing a session-scoped state the pooler cannot
carry across server connections over the session's lifetime, not only at
open. For the browser's lock manager: nothing in the page can audit it;
the remaining step is documenting the browsers the library is tested
against. For the Sembast file: a lock file or an operating-system file
lock taken when the database opens, refusing a second opener, where the
platform offers one.
