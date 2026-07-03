# EVS-DEV-postgres-backend: Postgres backend reference impl

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-portability

## Purpose

A second `StorageBackend` implementation alongside `SembastBackend`, targeting
server-side deployments (Cloud SQL / managed Postgres). Demonstrates that the
substrate's persistence contract is backend-agnostic, and provides the
storage layer for server-side deployments.

## Assertions

A. `PostgresBackend.open` SHALL emit `CREATE TABLE IF NOT EXISTS` DDL for
   every table the backend reads or writes, idempotently. Re-opening an
   already-provisioned database SHALL be a no-op on the schema.

B. The backend SHALL store view rows as JSONB blobs in a single
   `view_rows(view_name TEXT, row_key TEXT, row_data JSONB, updated_at
   TIMESTAMPTZ)` table, with `PRIMARY KEY (view_name, row_key)`.

C. `PostgresBackend.transaction<T>(body)` SHALL execute `body` inside a
   single Postgres transaction at SERIALIZABLE isolation. On any thrown
   exception the transaction SHALL be rolled back; on normal return it
   SHALL be committed. The `Transaction` handle passed to `body` SHALL be
   invalidated after `body` returns or throws.

D. Both `PostgresBackend` and `SembastBackend` SHALL pass the conformance
   harness in `event_sourcing/test/storage/storage_backend_conformance.dart`.

E. `PostgresIdempotencyStore` SHALL persist entries in an `idempotency` table
   keyed by `(action_name, principal_id, idempotency_key)`, with the
   policy semantics (`none / optional / required`) enforced by the
   substrate's action-dispatch path, not by the store.

F. `PostgresIdempotencyStore` SHALL pass the conformance harness in
   `event_sourcing/test/storage/idempotency_store_conformance.dart` and the
   `InMemoryIdempotencyStore` SHALL pass it too.

## Rationale

**Why JSONB-blob for view rows?** Closest fit to sembast semantics;
minimal DDL evolution machinery; consumers query view rows through the
substrate's `findViewRows` API, not directly against the table.

**Why a single `fifo_entries` table?** Cleaner DDL surface than the
sembast `fifo_<destinationId>` store-per-destination layout; observable
behavior is identical because the substrate iterates FIFOs through
`StorageBackend` methods only.

**Why SERIALIZABLE?** The per-device sequence counter is a single row;
SERIALIZABLE isolation guarantees that concurrent `nextSequenceNumber`
calls cannot both read the same value and stamp two events with the same
sequence number. The substrate is single-writer-per-source by design
but the storage layer should not assume the caller has external
synchronization.

## Changelog

- 2026-07-02 | e69b5a15 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Postgres backend reference impl* | **Hash**: e69b5a15
