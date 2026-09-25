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
dedicated lock session. Outside provisioning, the backend runs as a role that
neither owns nor can create its tables, holding only the documented table
privileges. Provisioning records which runtime and lock roles the deployment
declares. Opening refuses an undeclared role, a runtime or lock role that
could change the schema, and a database on which a role outside the owner
and the declared roles may write the library's tables or act as one of those
roles. An application keeps tables of its own in a schema of its own, under
a role of its own that holds no write privilege on the library's tables.

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

J. `PostgresBackend` SHALL hold its generation locks, the drain lock and its view convergence leases
   on one dedicated lock session, verified at open, and again for every replacement, to be a
   single server session reaching the pool's server, database and schema,
   configured with keepalives, no idle-session timeout and bounded connect
   and query timeouts, and used by one library operation at a time; when
   it declares that session lost it SHALL close it and, before registering
   or acquiring anything again, end the old server session if that session
   still holds a library lock.

K. The library SHALL document the privileges its Postgres runtime role needs on each table, and every library operation other than schema provisioning, which the role that owns the schema runs, SHALL work for a role that holds exactly those privileges and usage of the schema, and neither owns nor can create the tables.

L. `PostgresBackend` and `SembastBackend` SHALL refuse with `StateError` a `Transaction` handle that a different backend instance produced, including one still live within its own `transaction()` body.

M. `PostgresBackend.open` SHALL refuse the database, before it registers a generation and naming the role and the privilege, in any of these cases:

- a role other than the owner of the library's tables and the declared library roles holds a grant of `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `TRIGGER` or `REFERENCES` on a library table or on a column of one, or of `USAGE` or `UPDATE` on a sequence in the library's schema;
- such a role can inherit the privileges of, or set its role to, `pg_write_all_data`, the owner of the library's tables or a declared library role;
- `PUBLIC` holds any privilege on a library table, or `CREATE` on the library's schema.

N. `PostgresBackend.open` SHALL refuse, before it registers a generation and naming the role and the attribute, when the role its pool or its lock session connects as owns the library's schema or one of its tables, can inherit the privileges of or set its role to the owner of the library's tables, holds `SUPERUSER` or `CREATEROLE` directly or through a role it can inherit or set, or holds `CREATE` on the library's schema.

O. The library SHALL document, beside the runtime role's privileges, a setup for an application's own tables in the library's database that grants the application role no privilege on a library table beyond `SELECT` and no membership through which it can inherit or set a declared library role, the owner of the library's tables or `pg_write_all_data`, and that revokes `CREATE` on the library's schema from `PUBLIC`.

P. `PostgresBackend.provision` SHALL record, in a table of the library's schema that only the owner can write, the runtime and lock roles the deployment declares, and `PostgresBackend.open` SHALL refuse, before it registers a generation, when the role its pool or its lock session connects as is not a declared role of the matching kind.

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

**Why refuse another backend's live handle (assertion L)?** A
`Transaction` handle carries the underlying database's own transaction.
Applied to a different backend, a live handle would read or write another
database, or another connection to the same database, outside the
transaction the caller holds on this one: its writes would escape this
backend's commit, rollback and post-commit publication. A type check and a
validity check do not catch it, since the foreign handle has the right type
and is still valid; each backend therefore records which instance produced
a handle and refuses any other.

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

**Why a runtime role that owns nothing (assertion K)?** Postgres grants cannot separate the library from the application that embeds it: they share one process and one connection. What grants can separate is the process from the schema. The role that owns the tables can do anything to them, including disabling or dropping the queue table's guard (`EVS-DEV-destination-drain/S`) and rewriting the log, so the process runs as a role that neither owns nor can create them and holds only the table privileges the library's operations use: `SELECT` and `INSERT` on the log, which is therefore append-only for it, and `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the other tables. Provisioning creates the tables and so runs as the owning role, in its own deployment step (assertion G); it is the one library operation the runtime role cannot perform. The separation holds only if the runtime role cannot become the owner or create objects in the schema: it does not own the schema, is not a member of the owning role, holds neither `SUPERUSER` nor `CREATEROLE` nor membership in any role that carries them, and the schema grants `CREATE` to no role but the owner (a server's default `public` schema grants it to every role on some Postgres majors, so the deployment revokes it). The grants are a separate step from provisioning, which commits its DDL on its own, so a deployment provisions, then grants, and only then starts the instances of the new build. Reporting uses a read-only role holding `SELECT`, and no person holds write access. This split is a deployment concern: the library states the privileges once, as `postgresRuntimeRoleGrants` and in `spec/postgres-backend.md`, and every library operation other than provisioning is exercised under exactly those privileges, in a schema set up as the documentation describes. The lock session runs as the runtime role unless the deployment gives it another; for another role, `USAGE` on the schema and the runtime role's privileges on `backend_state` suffice, with the right to end its own sessions (assertion J), which it has as their owner.

**Why refuse a foreign write grant at open (assertion M)?** The application process holds the credentials of the roles it gives the library, so grants cannot keep that process out of the library's tables, but they can keep every other role out, and the refusal makes the deployment's grants a checked property instead of a documented one.

The check reads what the server records: explicit table, column and sequence privileges, and memberships. It also reads membership in `pg_write_all_data`, the predefined role that writes every table without a table grant, and membership in the owner of the library's tables, which carries every privilege ownership does. `SELECT` is not a write and is admitted, so reporting keeps its read-only role. `REFERENCES` and `TRIGGER` are refused because a foreign key onto a library table can block the library's own deletes and updates, and a trigger can change or refuse its writes. `USAGE` or `UPDATE` on a sequence lets a role move it.

A membership held only with the admin option grants neither inheritance nor set. On Postgres 16 a role that creates another receives that membership, and it is an administrative right like `CREATEROLE`. Neither can be refused without refusing every hosted database, because a hosting platform's administrative role holds them. Their holders are therefore named, beside the owner and superusers, as the database's administrators that the storage precondition excludes (EVS-PRD-destinations/L).

The check runs at open. A grant made while an instance runs is seen at the next open of any instance, which is why deployments grant only in their deployment step.

**Why refuse a runtime role that can change the schema (assertion N)?** The separation of assertion K holds only while the runtime and lock roles cannot become the owner or create objects in the schema. The refusal checks:

- ownership of the schema and its tables;
- membership in the owning role;
- the `SUPERUSER` and `CREATEROLE` attributes, held directly or through an inheritable or settable membership (this includes a hosting platform's administrative role, so a role created as a platform identity carrying such a membership is refused);
- `CREATE` on the schema.

Before Postgres 15, `PUBLIC` holds `CREATE` on the `public` schema, so a deployment on such a server revokes it (assertion O) or provisions the library into a schema of its own. Development and tests provision as the owner and open as a declared runtime role, as a deployment does.

**Why declared roles (assertion P)?** Instances of one deployment may connect under different roles: a canary or a blue-green deployment beside the serving instances, a credential rotated by swapping roles, or a separate delivery process. If the admitted set were the opener's own roles, each such instance would find the other's grants foreign and refuse. Provisioning, run as the owner, records the roles the deployment declares; every instance admits them all and refuses a role that is not declared. A role rotation declares the new role, deploys it, and retires the old role with a second provisioning once no instance uses it.

**Why a documented setup for an application's own tables (assertion O)?** An application often keeps state of its own beside the library's, an idempotency store or a job table for instance. Opening its own connection under a role of its own, in a schema of its own, gives it that without any access to the library's tables: the database refuses the application role's writes to them whatever the application's code does, and assertion M refuses a database whose grants would let it write them. The application role reads the library's tables only if the deployment grants it `SELECT`. The library's idempotency table stays in the library's schema. Only the idempotency store the library builds over its own storage (EVS-DEV-storage-capability) writes it.

## Changelog

- 2026-09-25 | c87576f9 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend J: the lock session also carries the view convergence leases. Add M-P: open refuses a database on which a role outside the owner and the declared library roles may write the library's tables or act as one of those roles, and a runtime or lock role that can change the schema; the documented setup for an application's own tables; provisioning records the declared library roles and open refuses an undeclared role
- 2026-09-24 | 793c6039 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add L: each backend refuses a Transaction handle another backend instance produced
- 2026-09-23 | 98f15f7c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add K: the documented runtime-role privileges suffice for every library operation other than provisioning, which the owning role runs
- 2026-09-23 | 1f8d49d6 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | J: the drain lock is held on the lock session beside the generation locks
- 2026-09-23 | 546da053 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Retire A; add G-J: provisioning is a separate, serialized step; open verifies the provisioned schema; provisioning keeps a live instance's schema version supported; the dedicated lock session, checked to reach the pool's server
- 2026-08-10 | 4e78d64b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | e69b5a15 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Postgres backend reference impl* | **Hash**: c87576f9
