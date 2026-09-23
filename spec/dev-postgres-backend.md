# EVS-DEV-postgres-backend: Postgres backend reference impl

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-portability

## Purpose

A second `StorageBackend` implementation alongside `SembastBackend`, targeting
server-side deployments (managed Postgres). Demonstrates that the substrate's
persistence contract is backend-agnostic, and provides the storage layer for
server-side deployments. Its schema is provisioned in a separate step, which
serializes with booting instances; opening a backend verifies the provisioned
schema, and each backend holds the incompatible-generation guard's locks on a
dedicated lock session.

## Assertions

A. <RETIRED> Provisioning (G) and opening (H) state the schema obligations.

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

G. `PostgresBackend.provision` SHALL bring a database's schema to the
   build's schema version by applying, in one transaction, every migration
   step above the stored version, SHALL record the resulting schema version
   and minimum compatible schema version, SHALL leave a database at or above
   the build's version untouched, and SHALL run while holding the generation
   guard's boot lock, so that provisioners and booting instances of one
   database serialize.

H. `PostgresBackend.open` SHALL perform no DDL, and SHALL refuse, naming
   provisioning, a database with no schema, one whose schema version is
   below the build's, or one whose minimum compatible schema version is
   above the build's; a newer schema whose minimum the build meets SHALL
   open.

I. Provisioning SHALL refuse, writing nothing, when a live instance
   registered on the database requires a schema version below the minimum
   compatible version the provisioning would record.

J. `PostgresBackend` SHALL hold its generation locks on one dedicated lock
   session, verified at open, and again for every replacement, to be a
   single server session reaching the pool's server, database and schema,
   configured with keepalives, no idle-session timeout and bounded connect
   and query timeouts, and used by one library operation at a time; when
   it declares that session lost it SHALL close it and, before registering
   or acquiring anything again, end the old server session if that session
   still holds a library lock.

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

**Why is provisioning a separate step (assertions G and H)?** Opening a
database is what every instance does, many at once and while others serve;
changing its schema is what a deployment does once. DDL run by `open`
would take table locks at every instance's open, could deadlock against
another instance, and would mutate the database before the
incompatible-generation guard had run. Provisioning is therefore its own call, run once per deployment,
and `open` only verifies the result. Provisioning applies an ordered list of
migration steps, so a later schema version is reached from any earlier one,
in one transaction that leaves the database as it was if any step fails.
It takes the boot lock the guard takes, so two provisionings never run DDL
at once (the second finds the schema at its version), and no instance boots
while the schema changes under it. The deployment creates the schema itself
(the first schema on the role's search path) and its grants; the library
creates the tables in it. The schema version pair versions the DDL, not the
data: a data-format minor that adds DDL adds a step that raises the schema
version and keeps the minimum, and a data-format major is provisioned only
after every instance of the old major has stopped.

**Why does a minimum let a serving revision keep opening (assertions H and
I)?** During a canary the new revision provisions ahead of the serving one.
A step that only adds (a table, a nullable column, an index) keeps the
minimum compatible version, so the serving revision, whose schema version is
below the stored one, still opens the database -- a restart or a scale-out
of the serving revision keeps working. A step an older build cannot run
against raises the minimum; provisioning refuses to raise it above the
version a live instance requires, so a canary's provisioning never locks
the serving revision out of a database it is using, and such a change is
deployed stop-then-start.

**Why one dedicated lock session, one operation at a time (assertion J)?**
Session-level advisory locks belong to the server session that took them.
The pool hands out connections per transaction, and a transaction-mode
pooler routes separate statements to different server sessions, so a lock
taken there lands on a session the library cannot address again. The lock
session is one connection held for the backend's lifetime, checked at open:
three separate statements must reach one server session carrying a setting
the first made, and it must reach the pool's database and schema. Names
alone do not identify a server: a lock URL to another Postgres instance
whose database and schema have the same names, or to a standby, passes a
comparison of names, and the locks it takes are invisible to every
instance on the pool's server. The check therefore has a pool connection
take a transaction-level advisory lock and requires the lock session to
see that lock, held by that connection's server process; a replacement
session is checked the same way. The check can miss a pooler that happens
to hand back the same server connection every time, so the requirement is
also documented. The library's
statements on the lock session run one at a time because the driver starts
a statement's timeout clock before the statement reaches the connection,
and a timed-out statement's cancel request cancels whatever the session is
running: a statement queued behind another would spend its timeout waiting,
and its cancel would land on another operation. The session carries server
keepalives and no idle-session timeout, so an idle lock session is neither
dropped by the server nor left open after its client vanished; a proxy
between the process and the database has client-side timeouts of its own,
which the deployment configures.

**Why is a lost session ended rather than assumed gone (assertion J)?** A
probe or an operation that times out does not mean the server session
ended: a cancelled statement leaves the session alive and holding its
locks, and a connection the client could not close cleanly may leave the
server session running until its keepalives expire. If the replacement
registered while the old session still held the library's locks, one
instance would hold them twice, and a lock the old session holds would
outlive the instance's view of it. The replacement therefore looks for
advisory locks held by the old session (by process id and start time, since
a process id can be reused) and ends that session before it registers
anything; when it cannot (the lock role may not end its own sessions), it
registers nothing, reports the requirement, and retries. The lock role must
be allowed to end its own sessions, which the role that owns them is.

## Changelog

- 2026-09-23 | 546da053 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Retire A; add G-J: provisioning is a separate, serialized step; open verifies the provisioned schema; provisioning keeps a live instance's schema version supported; the dedicated lock session, checked to reach the pool's server
- 2026-08-10 | 4e78d64b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | e69b5a15 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Postgres backend reference impl* | **Hash**: 546da053
