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

**Remaining.** The gap is cross-process only. Writes made by another
process against the same database produce no emissions in this process,
because the publish bus is in-memory and per-`EventStore`. A multi-process
Postgres deployment therefore polls `findViewRows` to observe another
process's writes. Closing this needs a cross-process bridge — Postgres
`LISTEN`/`NOTIFY`, or a polling loop — feeding the same subscription bus.

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
