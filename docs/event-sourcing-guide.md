# A Guide to the `event_sourcing` Library

This is a hands-on, plain-English introduction to the `event_sourcing`
library. It is meant for developers who have never used it before and who
want to know what it does, what it expects of them, and how to wire up an
application that uses it.

The first sections cover the everyday surface area: events, projections,
actions, and permissions. The final section adds the layer of complexity
that's load-bearing but easy to ignore on first read: how the library
handles event metadata, cross-installation sync, and the cryptographic
trail.

## What this library is, and isn't

`event_sourcing` is a pure-Dart substrate for building **append-only,
auditable applications**. Every state change is recorded as an immutable
event in a single ordered log. The library doesn't store rows you mutate;
it stores the *history* of what happened, and it computes the rows you
read from that history. Every dispatch — successful or denied — produces
an event, so the question "what happened, when, and why?" is always
answerable from the log alone.

It is intentionally narrow. It supplies:

- An append-only event log with strong ordering and integrity guarantees.
- A reactive subscription primitive for reading the log and the views
  derived from it.
- A declarative projection mechanism: you describe the shape of a view,
  the library computes and maintains it.
- An action dispatcher with a parse → validate → resolve-scopes →
  authorize → execute → persist pipeline.
- A role/permission/scope authorization model where every grant and
  assignment is itself an event in the same log.
- A pluggable `StorageBackend` abstraction. Two reference backends ship:
  `SembastBackend` for client-side, `PostgresBackend` for server-side.
  Both pass the same conformance harness.

It does **not** ship domain types (no `Patient`, no `Invoice`), domain
materializers, transport protocols, or any opinion about what your
application is *about*. Your app brings the vocabulary; the substrate
brings the bookkeeping.

## The mental model

Five concepts carry most of the weight. The rest of the library
elaborates on these.

### Event

An event is an immutable record of something that happened. Once
written, it never changes. Each event carries:

- A position in the log (its `sequence_number`).
- An identity (aggregate id + aggregate type + event type + entry type).
- A payload of domain data (your fields, as JSON).
- An initiator (who or what caused it: a user, an automation, anonymous).
- A timestamp and a cryptographic hash that chains to the previous
  event's hash.

You don't usually construct `StoredEvent` directly. You produce an
`EventDraft` from an action's `execute` method, or you call
`eventStore.append(...)` for system-driven events (seed loaders, etc.),
and the library stamps everything else.

### Aggregate

An aggregate is the unit of consistency. Events for the same aggregate
are ordered with respect to each other. An aggregate id is a string you
choose — typically a UUID, sometimes a composite key like
`"admin:user.invite"` when the natural identity is a tuple. Aggregate
type is the kind: `"Patient"`, `"role_permission_grant"`,
`"demo_note"`.

### Projection

A projection is a declarative recipe for turning the event log into a
table you can query. You don't write a fold function — you supply a
`ProjectionSpec` describing which events to listen to, how to derive a
row key from each event, and what to put in the row data. The library
maintains the materialized view for you, deterministically.

Two shapes ship:

- **`TableProjectionSpec`** — a flat table where rows are inserted on
  one set of event types and removed on another. Good for lookup tables
  like "which permissions does this role have?" or for index-like views.
- **`AggregateProjectionSpec`** — one row per aggregate, built by
  merging successive events' payloads. Good for "the current state of
  invoice X" where many events contribute fields.

### View

A view is the materialized output of a projection. It lives in the
storage backend (a Sembast store, a Postgres table) and you read it via
`eventStore.subscribe<T>(...)` for live updates, or directly via
`eventStore.reader.findViewRows(...)` for one-off queries inside tests and admin
tools.

### Action

An action is the substrate's write API. You subclass
`Action<TInput, TResult>`, declare which permissions it requires,
implement `parseInput`, `validate`, and `execute`, and the dispatcher
runs the pipeline in order — parsing, validation, and scope-resolution
first, then the authorize, execute, and persist steps inside a single
storage transaction.
Your `execute` returns an `ExecutionResult` carrying the events to
persist; the library handles the appending, the audit trail, and the
authorization check.

## Two layers of trust

This is worth internalizing once: the library makes two kinds of claims,
and they're not the same.

**Layer 1 — Facts.** These are cryptographic or structural and
absolute. The library guarantees them:

- The event at sequence N has hash H.
- The hash chain from genesis to N is intact (tamper-evident).
- The provenance entries record which database authored the event and
  which databases stored it after (a receiver, or a successor that
  restores it), with
  attribution to initiators and times.
- The events of one aggregate that one database wrote on one branch of its
  origin chain are stored in the order it wrote them, whenever they reach
  the log through one path.
- Each delivery a receiver accepted on a channel follows the one before it,
  by number and hash link.
- The append of an event was atomic with its view-row updates inside the
  same transaction.

**Layer 2 — Conventions.** These are choices the library makes that
*most* apps want, but they're not unique truths:

- A "tombstone" event type deletes the row (it could equally mark the
  row deleted but keep it).
- Missing keys in a delta payload preserve the prior value; explicit
  null clears.
- Whoever appends the first event for an aggregate is the canonical
  authority for that aggregate (the library could equally require
  out-of-band canonicalization assignment).
- One row per aggregate, materialized by deep-merging successive
  payloads.

One more piece worth knowing: the substrate's job ends at "the event
log says X". For action authorization, that means the substrate
verifies `(userId, activeRole)` against `user_role_scopes` — the auth
layer's responsibility is to identify the user; everything else (which
roles, which scopes, which permissions) is derived from the log.
`Principal.userId` is trusted on faith from auth; `Principal.activeRole`
is a user-chosen "which hat" that the substrate independently verifies.

Most of what you'll write deals with Layer 2: declaring projections,
defining actions, configuring permissions. Layer 1 is what you fall back
to when an app needs an interpretation different from the library's
default — at that point, you subscribe to raw events with `Events()`
mode and compute your own state. The library expects this.

## The substrate's standing commitments

A few non-negotiable choices the library makes. Worth knowing because
they shape what's possible:

- **The library is domain-neutral.** It ships no `Patient`, no
  `Invoice`, no `Site`. Your app registers its own aggregate types,
  projections, scope classes, and permissions at composition time.
- **Projections are declarative, not coded.** You give the library a
  `ProjectionSpec`; the library computes the view. There is no callback
  you supply to fold an event into a row. This is what makes the views
  deterministic and reconstructable.
- **The authorization policy is library code, not app code.** Apps
  declare permissions and seed assignments; the library decides whether
  a dispatch is allowed. The decision logic lives in the substrate
  because every Allow/Deny outcome must be reproducible from the event
  log + the library version. App-supplied policy callbacks would break
  that.
- **The substrate records its own version in the log.** The first open
  of a database appends `lib_version_initialized`, and every open by a
  build of another package or data-format version appends
  `lib_version_changed`, older builds included. Builds of one
  data-format major share a database; another major is refused before
  anything is written. "Which library opened the database, and in what
  order?" is answerable from the log alone.
- **Single-source-per-aggregate-type, today.** The multi-source
  machinery exists in design but is dormant in 0.x. In practice this
  means: in the 0.x model, each kind of aggregate is produced by one
  deployment.

## Dispatching an action — what happens

When the dispatcher receives an `ActionSubmission` (a request to run
some action with some input), it walks through six author-visible steps
(the full dispatcher pipeline wraps these in additional internal stages —
idempotency lookup, hit/mismatch handling, and outcome recording). Parse,
validate, and scope-resolution are pure-or-near-pure and run outside
any storage transaction; the authorize, execute, and persist steps
share a single transaction:

1. **Parse.** The dispatcher calls `Action.parseInput` to turn the raw
   JSON-like map into a typed input value. Any `FormatException` or
   `ArgumentError` at this stage produces a parse-denial event and
   the dispatch terminates.
2. **Validate.** It calls `Action.validate` on the typed input. Throw
   to reject: produces a validation-denial event.
3. **Resolve scopes.** For each scoped `Permission` the action declares,
   the dispatcher calls `Action.scopeFor(permission, input)` and
   validates the returned `ScopeValue` (no null, no
   `TotalWildcardScope`, scope class matches the permission's
   `scopeClass`). A failure here produces a `scopeUnresolvable`
   authorization-denial event via a standalone append — the dispatch
   transaction has not been opened yet.
4. **Authorize.** Inside a fresh storage transaction, for
   each `Permission` in turn, the dispatcher asks the policy whether
   the principal has the grant. The policy first verifies the
   principal actually holds the claimed `activeRole` by reading
   `user_role_scopes` — the substrate doesn't trust the Principal's
   role claim, only its `userId`, and the same membership check runs
   for scoped and unscoped permissions. It then checks the
   role-permission grant, and for scoped permissions also matches the
   resolved scope value from step 3 against the user's assignments.
   The first Deny stops the pipeline and produces an
   authorization-denial event committed inside the same transaction
   as the policy's projection reads — the audit log records the
   precise snapshot the decision saw.
5. **Execute.** Still inside the same transaction, the dispatcher
   calls `Action.execute(input, ctx)`. Your code returns an
   `ExecutionResult` listing the events to append. An uncaught
   exception rolls the transaction back, and the dispatcher emits the
   execution-failed denial in a separate append after the rollback.
6. **Persist.** Each `EventDraft` in the result is written via
   `appendInTxn`, which both records the event and runs the projection
   interpreter so view rows update in the same transaction. The
   transaction commits.

Because the authorize stage's reads and the execute-then-persist
stage's writes share one transaction, a revocation committed
concurrently does not affect an in-flight dispatch.

## Permissions in plain English

The permission model has four moving parts. They are deliberately
boring; the substrate gets out of the way once they're configured.

### Roles and permissions

A **role** is a name (`"Admin"`, `"StudyCoordinator"`). A
**permission** is a named capability (`"users.provision"`,
`"patient.edit"`). The `permissions.yaml` seed wires which roles
carry which permissions:

```yaml
roles:
  - Admin
  - GreenTeam
  - BlueTeam
grants:
  Admin:
    - users.provision
  GreenTeam:
    - help.ask
    - notes.write.green
  BlueTeam:
    - help.ask
    - notes.write.blue
```

When the app boots, `bootstrapActionPermissions` parses this YAML,
validates that every permission referenced is actually declared by some
registered `Action`, and emits a `permission_granted` event for each
grant that doesn't already exist in the log. The
`role_permission_grants` projection (a `TableProjectionSpec` the
substrate ships built-in) materializes those grants into a queryable
view.

### Scope classes

Some permissions are inherently scoped: a coordinator at site A should
be able to edit patients at site A but not at site B. Apps declare the
scope dimensions they need by registering `ScopeClassSpec`s:

```dart
final scopeClassRegistry = ScopeClassRegistry(
  classes: const <ScopeClassSpec>[
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'patient',
      containedIn: ContainmentRef(
        parentClass: 'site',
        projection: 'patient_site_index',
        keyColumn: 'patient_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: ..., // wires to ProjectionRegistry
);
```

The substrate ships no built-in scope classes. The optional
`containedIn` says "patient is contained in site, and the
`patient_site_index` projection tells you which site a patient lives
at." When a user is assigned a permission at the site level and an
action requests the same permission at the patient level, the substrate
uses the index to check whether the patient lives at the assigned site.

A `Permission` then declares its scope class:

```dart
const Permission('patient.edit', scopeClass: 'patient')
```

### Scope values

When an action's permission is scoped, the action must tell the
dispatcher *which* scope value applies for this particular dispatch.
That's `Action.scopeFor`:

```dart
@override
ScopeValue? scopeFor(Permission perm, EditPatientInput input) =>
    perm.scopeClass == 'patient'
        ? BoundScope(class_: 'patient', value: input.patientId)
        : null;
```

`ScopeValue` is sealed with three variants:

- **`BoundScope(class_: ..., value: ...)`** — a specific scope value.
  The everyday case.
- **`ValueWildcardScope(class_: ...)`** — "any value of this class."
  Used in assignments for super-users at a given level.
- **`TotalWildcardScope()`** — "any class, any value." Used in
  assignments for full-admin roles. Actions must never return this from
  `scopeFor` — there's no permission scope class to match it against.

Mismatches (null returned for a scoped permission, `TotalWildcardScope`
returned, class disagreement) all become `Deny(scopeUnresolvable)`.

### Role assignments

A user being a "BlueTeam member at blue-workspace" is itself an event
in the log: a `role_assigned` event with payload
`(userId, role, scope)`. The `user_role_scopes` projection materializes
these into a queryable view, keyed by a canonical-JSON encoding of the
tuple so duplicates are impossible.

You seed initial assignments through `bootstrapRoleAssignments`. After
boot, assignments evolve through your app's own actions
(`ProvisionUserAction`, `ChangeUserRoleAction`, etc.) — whatever your
app needs.

### The match algorithm

Authorization happens in two phases. The dispatcher does some
pre-checks before calling into the policy; the policy then runs the
real match against the event-derived projections.

**Dispatcher pre-checks** (these never reach the policy):

- If the action's permission is scoped (`scopeClass` is non-null) and
  `Action.scopeFor` returns `null`, or returns `TotalWildcardScope`,
  or returns a `ScopeValue` whose `class_` does not equal the
  permission's `scopeClass` — the dispatcher denies with
  `scopeUnresolvable` and never calls the policy.
- If the permission is unscoped and `scopeFor` returns a non-null
  value, that's also a class mismatch and denies with
  `scopeUnresolvable`.

**Policy match** — when the dispatcher does call
`policy.isPermitted(principal, permission, scopeValue)`:

1. If `principal` is not a `UserPrincipal` (i.e., anonymous): return
   `Deny(notGranted)`. The substrate has no notion of anonymous
   assignments.
2. Look up `user_role_scopes` for
   `(principal.userId, principal.activeRole)` to get all assignments
   under the claimed active role. If empty: `Deny(notGranted)` — the
   principal claims to be acting as a role they don't actually hold
   according to the log. The substrate verifies this independently of
   whatever the auth layer told it; auth's only contract is to identify
   the `userId`. (See "Two layers of trust" — this is the substrate's
   response to the question "is this user actually wearing the hat they
   say they are?")
3. Look up `role_permission_grants` for
   `(principal.activeRole, permission.name)`. If no grant exists for
   the active role: `Deny(notGranted)`.
4. If the permission is unscoped: `Allow`.
5. The semantics are "at least one assignment must cover the
   requested scope," and the substrate implements this by iterating
   the assignments from step 2 and returning `Allow` on the first
   match:
   - `TotalWildcardScope` covers anything.
   - `ValueWildcardScope(class: C)` covers any scope of class C, and
     any class that is a descendant of C in the containment graph.
   - `BoundScope(class: C, value: V)` covers the same class+value
     directly, and any descendant class+value if the containment
     resolver can walk from the requested scope up to C and finds V
     there.
6. No assignment covers the request: `Deny(notGranted)`.

The containment resolver is fail-closed: if any hop in the chain is
missing a row, that assignment doesn't match. Step 5 keeps walking;
step 6 fires if nothing matches.

## Wiring up an implementation

Bringing the substrate online consists of registering everything your
app contributes, then handing those registries to the bootstrap
helpers. The canonical example lives at
`event_sourcing/example_action_permissions/lib/server/bootstrap.dart`.
Here is what it does, with the parts that always look the same.

### 1. Describe the storage

The library opens the storage itself from a description, holds it, and
closes it when the event store closes (and when an open fails after it
opened it). A Sembast description names where the database lives:

```dart
const storage = SembastStorage.file('/path/to/events.db'); // native runtimes
// SembastStorage.browser('events')  -- an IndexedDB database on the web
// SembastStorage.memory('events')   -- an in-memory database of the isolate
```

The library selects the Sembast factory for the description itself.
`deleteSembastDatabase(storage)` deletes the database a description names,
once no event store of the isolate holds it open: this is how an
application resets a database an earlier data format wrote.

Or, for server-side, provision the schema once per deployment (a step
of its own, before any instance starts), then describe the database the
instances open:

```dart
// As the role that owns the schema, declaring the roles instances connect
// as (a lock session opened without lockUrl runs as the runtime role).
await PostgresBackend.provision(
  'postgres://evs:evs@localhost:5432/evs_demo',
  schema: 'demo',
  runtimeRoles: {'evs_runtime'},
  lockRoles: {'evs_runtime'},
  sslMode: SslMode.disable,
);

// As the runtime role, after the grants.
const storage = PostgresStorage(
  url: 'postgres://evs_runtime:evs@localhost:5432/evs_demo',
  schema: 'demo',
  sslMode: SslMode.disable,
);
```

The library opens a `PostgresStorage` with `PostgresBackend.open`. A
backend the application constructs itself (its own `StorageBackend`
implementation, say) enters only as
`ApplicationSuppliedStorage(backend, securityContexts)`; the application
holds that backend and closes it, and the event store over it does not.

`open` runs no DDL: it verifies the provisioned schema version and
refuses, naming `provision`, a database that was never provisioned or
whose schema this build does not support. A schema a newer build
provisioned ahead of it still opens when the newer build kept the
minimum compatible version, so a serving revision keeps restarting while
a canary runs. Provisioning takes the same boot lock as the instances'
boots, and refuses to raise the minimum above what a live instance needs.
The deployment creates the schema itself (the one the storage description
names) and its grants; `provision` creates the tables. Every transaction the
library runs sets its search path, as its first statement and for that
transaction only, to that schema, `pg_catalog` and `pg_temp`, and `open`
refuses when the current schema is another, for example a schema the
description names that does not exist. Because the setting travels with
each transaction, the pool's connection may run through a
transaction-mode pooler; the lock session must still be one server
session.

Run provisioning as the role that owns the schema, and the instances as a
separate runtime role that neither owns nor can create the tables: grant
it `USAGE` on the schema and, after each provisioning, exactly the table
privileges `postgresRuntimeRoleGrants` lists (the table in
`spec/postgres-backend.md`, "Roles and privileges"; `SELECT` and `INSERT`
only on `events`). Every library operation other than provisioning works
under those privileges (`EVS-DEV-postgres-backend/K`). Provision, then
grant, then start the new build's instances: provisioning commits its DDL
before the grants exist. The runtime role, and any role `lockUrl` names,
must not be able to become the owner: it owns nothing in the schema, is not
a member of the owning role, holds neither `SUPERUSER` nor `CREATEROLE`
nor membership in a role that carries them (a hosting platform's
administrative role included), and has no `CREATE` on the schema (revoke
it from `PUBLIC` on a `public` schema of a Postgres major before 15). The
library is tested against PostgreSQL 16.

Provisioning records the runtime and lock roles the deployment declares,
in a table of the schema only the owner writes, and each provisioning
replaces the set. `open` refuses, naming the role and the privilege
(`PostgresRoleRefusedException`), before it registers a generation: a pool
role not declared as a runtime role, or a lock role not declared as a lock
role; a pool or lock role that owns the schema or a table in it, can
inherit or set the owner, or holds `SUPERUSER` or `CREATEROLE` directly or
through a role it can inherit or set; and a database on which a role
other than the owner and the declared roles holds a write privilege on a
library table, a column of one or a sequence in the schema, or can inherit
or set `pg_write_all_data`, the owner or a declared role, on which `PUBLIC`
holds any privilege on a library table, or on which a role other than the
owner holds `CREATE` on the schema. `SELECT` is admitted, so a reporting
role keeps working, and so is a membership held with the admin option
alone. Several declared runtime roles open side by side, so a canary, a
rotated credential or a separate delivery process runs under a role of its
own. An application that keeps tables of its own in the database puts them
in a schema of its own, under a role of its own that holds no privilege on
a library table beyond `SELECT` and no membership through which it can act
as a declared role, the owner or `pg_write_all_data` (the doc comment of
`postgresRuntimeRoleGrants`, "An application's own tables").

The queue table's guard
(`EVS-DEV-destination-drain/S`) refuses every change to a queue item
outside the shapes of the library's own writes, from any role; it cannot
tell a hand-written change of a legal shape from the library's own, and
only the owner can drop it, which is why no serving process runs as the
owner and no person writes as the runtime role. Reporting uses a
read-only role.

Each `PostgresBackend` also holds one dedicated connection for its
lifetime, the lock session, to `lockUrl` when given and to `url`
otherwise. The library keeps its generation locks there, so it must be one
real server session: a direct connection to the database, or a
session-mode proxy that resets sessions on release -- never a
transaction-mode pooler (the pool's own connections may go through one).
`open` checks this, and that the lock session reaches the pool's server,
database and schema, and throws `LockSessionConfigurationException` on a
mismatch; the check can miss a pooler that happens to return the same
server connection each time. The library sets server-side TCP keepalives
and no idle-session timeout on the lock session; a proxy between the
process and the database has client-side timeouts of its own to
configure. The lock role must be allowed to end its own sessions (as the
role that owns them is): when the library declares a lock session lost, it
ends the old server session before registering again.

Several server processes may share one database. Only one of them drains
its destinations: each starts a delivery cycle, one holds the database's
drain lock on its lock session and delivers, and the others stand by and
take over when it stops (`EVS-PRD-destinations/V`; see "Several processes
sharing one database" under "Cross-process client/server deployments").
Each registers its build's
data generation -- its data-format major and each registered entry type's
major -- when its event store opens, and an open is refused
(`IncompatibleGenerationException`, nothing written) while a live instance
of a conflicting build holds a different major; builds that differ only in
minors, or in which entry types they register, run side by side.

A Sembast database file outside the browser is opened by one isolate of
one process: the generation guard has nothing to guard there, and the
drain lock excludes a second delivery cycle over the same open database
handle in that isolate. Two processes, or two isolates, that open one file
are outside what the library supports.

In the browser the tabs of an origin share one IndexedDB database. The
generation guard and the delivery cycle's drain lock both use the
browser's lock manager (Web Locks), which exists only in a secure context
(HTTPS or localhost): on a page without it `EventStore.open` throws
`GenerationGuardConfigurationException` and `SyncCycle.start` throws
`DrainLockConfigurationException`. The drain lock follows the visible tab:
a tab whose page becomes hidden finishes its sends in flight (waiting at
most one cadence for a send that does not return, whose item the next
drainer sends again), hands the lock over and stands by, so nothing
drains while no tab of the origin is visible (`EVS-PRD-destinations/V`).
A page the browser freezes before its hand-over completes keeps the lock
until it is resumed or discarded, and the visible tab stands by until
then: a liveness limit, not a safety one. Every tab registers the same
destinations, because delivery uses the destinations of the tab that
drains.

Tabs write to the database side by side; a write that keeps losing its
commit to other tabs' writes runs again with them held back, so
contention delays a write but never fails it. A sembast_web handle that
another tab's open compacted past the commits it has seen cannot commit
again, even alone: its writes fail with `TransactionRerunLimitException`
at once, and a delivery cycle over it stops, releases the drain lock to
another tab, and reports the exception as `SyncCycle.stopCause` once
`SyncCycle.stopped` completes. The application closes the database, opens
it again and starts a new cycle.

The backend is trusted for persistence, atomicity, and durability, and a
backend shared by several processes or tabs for running the generation
guard; the lock-session path above, the browser's lock manager on the web,
and one opener of a Sembast database file outside the browser are trusted
too. Everything else is derived from the events the backend holds.

> **Note on Postgres + subscriptions.** Both backends pass the same
> conformance harness for storage, transactions, and view
> materialization, and reactive `subscribe<T>` works on both: live
> updates are published by the `EventStore` after each commit, so a
> subscriber sees every change made through its own `EventStore`
> instance regardless of backend. What no backend provides today is
> cross-process change notification — a write made by a different
> process against the same Postgres database produces no emission, so
> multi-process deployments poll `findViewRows` on a cadence for
> cross-process freshness. Push-based cross-process notification
> (`LISTEN/NOTIFY`) is a roadmap item (`spec/roadmap/storage.md`).

### 2. Build an action registry

```dart
final registry = ActionRegistry();
registry.register(EditBlueNoteAction());
registry.register(PressRedAlarmAction());
// ...
```

The registry collects all the actions your app dispatches. Its
`allDeclaredPermissions` getter is what the seed validator checks the
YAML against, so register before bootstrapping permissions.

### 3. Declare your scope classes

```dart
final scopeClassRegistry = ScopeClassRegistry(
  classes: const <ScopeClassSpec>[ScopeClassSpec(name: 'site')],
  projectionLookup: (_) => null, // no containment hierarchy in this app
);
```

If your app has no scoped permissions at all, you can skip this and
pass `null` to the relevant bootstrap helpers.

### 4. Register projections

```dart
final projections = ProjectionRegistry()
  ..register(rolePermissionGrantsSpec)   // substrate built-in
  ..register(userRoleScopesSpec)         // substrate built-in
  ..register(myAppPatientIndexSpec)      // app projection
  ..register(myAppActiveSessionsSpec);   // app projection
```

The two substrate built-ins (`rolePermissionGrantsSpec` and
`userRoleScopesSpec`) feed the authorization policy. You always
register them. Then add your own.

### 5. Open the event store

```dart
final datastore = await bootstrapEventStore(
  storage: storage,
  source: Source(
    hopId: 'server-1',
    identifier: installId,             // UUIDv4, persisted across boots
    softwareVersion: '0.1.0+1',
  ),
  entryTypes: <EntryTypeDefinition>[
    EntryTypeDefinition(
      id: 'role_permission_grant',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'Role-permission grant',
    ),
    EntryTypeDefinition(
      id: 'user_role_scope',
      registeredVersion: EntryTypeVersion(1, 0),
      name: 'User-role-scope assignment',
    ),
    // ... your app's entry types
  ],
  destinations: <Destination>[myAppOutboundDestination],
  projections: projections,
  onBootProgress: bootHealth.record,   // optional: see "Start-up and
                                       // readiness probes"
);
final eventStore = datastore.eventStore;
```

Every event type your app appends must have its `entryType` registered
here — missing entries fail at append time, not boot. The library's own
reserved system entry types, and its default destination-wedges view, are
registered by the open itself; list only your application's types (an id
the library reserves is refused unless it is the library's own
definition). The
`registeredVersion` -- a major and a minor number -- is what gets
stamped on every event of that type; raising it later signals a schema
change (a minor to add a field with a default, a major to rename or drop
one).

`onBootProgress` is optional (`bootHealth` above stands for the server's
own health tracker). The open reports its boot to it -- the
phase (its checks, and its completion) and the time since the open
began -- so a server can answer a health probe while the boot waits for
its locks (see "Start-up and
readiness probes" below). The observer runs synchronously inside the boot,
and on Postgres the boot holds every instance's appends back while it runs,
so it must only record: its future is not awaited, what it throws is
logged, and a call from it into an event store while the boot runs throws
`StateError`.

### 6. Apply the permissions seed

```dart
final policyBootstrap = await bootstrapActionPermissions(
  eventStore: eventStore,
  declaredPermissions: registry.allDeclaredPermissions,
  scopeClassRegistry: scopeClassRegistry,
  yamlSource: permissionsYaml,
);
```

`bootstrapActionPermissions` returns a sealed
`AuthorizationPolicyBootstrap` — either `PolicyReady(policy)` on
success or `PolicyFailSafe(errors)` if the YAML failed validation
(typos, scope-class references not in the registry, permissions not
declared by any action). In the failure case the wrapped policy is a
`FailSafeAuthorizationPolicy` that denies every request, and
`policyBootstrap.errors` lists what went wrong so the inspector or
caller can surface them. Bootstrapping is idempotent: re-running it
emits only the grants not yet in the log.

### 7. Apply the role-assignment seed

```dart
final roleAssignments = <RoleAssignmentSeedEntry>[
  for (final user in seededUsers)
    if (user.assignedSite != null)
      RoleAssignmentSeedEntry(
        userId: user.id,
        role: user.role,
        scope: BoundScope(class_: 'site', value: user.assignedSite!),
      ),
];
await bootstrapRoleAssignments(
  eventStore: eventStore,
  seed: RoleAssignmentSeed(entries: roleAssignments),
);
```

This emits `role_assigned` events for users you want to start with a
specific assignment. Idempotent: re-running detects existing
assignments by their canonical-JSON aggregate id and emits only the
ones missing.

### 8. Wire the dispatcher

```dart
final idempotencyStore = InMemoryIdempotencyStore();
final dispatcher = bootstrapAuditedActions(
  events: eventStore,
  authorization: policyBootstrap.policy,
  idempotency: idempotencyStore,
  actions: registry.all,
);
```

You hand the dispatcher to whatever takes input from outside — your
HTTP routes, your CLI, your test harness. It's the single entry point
for any code that mutates state.

### 9. Start the delivery cycle

```dart
final cycle = await SyncCycle.start(
  registry: datastore.destinations,
  configurationVersion: revisionId,    // the deployment's revision
);
// ... serve ...
await cycle.close();
```

The delivery cycle fills each registered destination's queue from the log
and sends it, in log order. At most one cycle drains a database: a second
`start` over the same database in one isolate throws `StateError`, and a
cycle in another process or tab stands by until the drain lock is free
(`EVS-PRD-destinations/V`). Every append and every committed registry
operation wakes it, and it runs a pass at least every `cadence` (15 s by
default). "Several processes sharing one database" below covers
deployment, halts and recovery.

## Defining an action

Subclass `Action<TInput, TResult>` and override the parts the
dispatcher will call:

```dart
class EditPatientAction extends Action<EditPatientInput, EditPatientResult> {
  @override
  String get name => 'EditPatientAction';

  @override
  String get description =>
      'Coordinator edits a patient record. Scoped to the patient.';

  @override
  Set<Permission> get permissions => <Permission>{
    const Permission('patient.edit', scopeClass: 'patient'),
  };

  @override
  Idempotency get idempotency => Idempotency.optional;

  @override
  EditPatientInput parseInput(Map<String, Object?> raw) {
    final patientId = raw['patientId'];
    final newName = raw['newName'];
    if (patientId is! String || newName is! String) {
      throw const FormatException(
        'expected {patientId: String, newName: String}',
      );
    }
    return EditPatientInput(patientId: patientId, newName: newName);
  }

  @override
  void validate(EditPatientInput input) {
    if (input.newName.trim().isEmpty) {
      throw ArgumentError.value(input.newName, 'newName', 'must not be empty');
    }
  }

  @override
  ScopeValue? scopeFor(Permission perm, EditPatientInput input) =>
      perm.scopeClass == 'patient'
          ? BoundScope(class_: 'patient', value: input.patientId)
          : null;

  @override
  Future<ExecutionResult<EditPatientResult>> execute(
    EditPatientInput input,
    ActionContext ctx,
  ) async {
    return ExecutionResult<EditPatientResult>(
      result: EditPatientResult(patientId: input.patientId),
      events: <EventDraft>[
        EventDraft(
          aggregateType: 'patient',
          aggregateId: input.patientId,
          entryType: 'patient',
          eventType: 'patient_renamed',
          data: <String, dynamic>{'newName': input.newName},
        ),
      ],
    );
  }
}
```

Things to notice:

- `parseInput` and `validate` are pure (no I/O). They throw on bad
  input; the dispatcher converts the throw into a denial event.
- `permissions` declares **what's required**, not what's granted. The
  matrix decides who gets it.
- `scopeFor` runs only for scoped permissions. The default returns
  null; you override only when the permission has a `scopeClass`.
- `execute` returns events, doesn't append them. The dispatcher does
  the appending inside its transaction.
- `idempotency` is one of `none`, `optional`, `required`. With
  `required`, the dispatcher refuses to run unless the submission
  carries an `idempotencyKey`, and it caches the outcome so retries
  return the original result without re-emitting events.

A few things worth knowing about idempotency-on-identifier (the
matching-content cache hit):

- The cache is keyed by `(actionName, principalId, idempotencyKey)`.
  Two different principals submitting under the same key do not
  collide; cross-user idempotency is intentionally not a thing.
- A retry with the same `(actionName, principalId, idempotencyKey)`
  while the cache entry is unexpired short-circuits at Stage 4: the
  dispatcher returns a `DispatchIdempotencyHit` whose `cachedResult`
  matches the original outcome's result map and whose
  `priorEmittedEventIds` matches the original events. The dispatcher
  does NOT re-run parse, validate, authorize, or execute, and it
  emits no new events to the log.
- `Action.idempotencyTtl` controls how long the cache entry is
  considered fresh; after expiry, the same key+input behaves like a
  brand-new submission.
- **Content-mismatch detection (PRD-action-dispatch/E).** A submission
  reusing an unexpired `(actionName, principalId, idempotencyKey)` with
  a different `rawInput` is NOT silently absorbed. Stage 4
  canonicalizes the submitted `rawInput` (RFC 8785 JCS) and compares
  it against the cached entry's `rawInputCanonicalJson`. On match the
  dispatcher returns `DispatchIdempotencyHit` (cache hit, no new
  events). On mismatch the dispatcher emits an `idempotency_mismatch`
  denial event and returns `DispatchIdempotencyMismatch`. The denial
  event's payload carries SHA-256 hashes of both canonical-JSON
  inputs (`cached_raw_input_hash`, `submitted_raw_input_hash`), the
  `action_name`, and the `idempotency_key`; the inputs themselves
  are deliberately NOT persisted (they may contain sensitive data).
  Cache entries that lack a `raw_input_canonical_json` column on the
  storage row fall back to the plain cache-hit behavior on lookup;
  the substrate never raises a false `idempotency_mismatch` against
  an entry whose canonical form it didn't capture.

## Defining a projection

You don't write a fold function. You describe what to listen to and how
to project, and the library does the rest.

```dart
final patientByIdSpec = TableProjectionSpec(
  viewName: 'patient_by_id',
  interest: const SubscriptionFilter(
    eventTypes: {'patient_created', 'patient_renamed', 'patient_deleted'},
    aggregateTypes: {'patient'},
  ),
  insertEventTypes: const {'patient_created', 'patient_renamed'},
  removeEventTypes: const {'patient_deleted'},
  rowKey: const AggregateIdKey(),
  rowData: const WholePayload(),
);
```

The pieces:

- **`viewName`** — the name of the materialized view (becomes a
  Sembast store or a `view_rows.view_name` predicate on Postgres).
- **`interest`** — a `SubscriptionFilter` listing the event types
  (`Set<String>`), aggregate types (`Set<String>`), and entry types
  (`Set<String>`) the projection cares about. Any field can be
  omitted to mean "all values of that dimension." Per-aggregate-id
  filtering is a subscription concern, not a projection concern — see
  `AggregateMode.aggregates` below.
- **`insertEventTypes` and `removeEventTypes`** — the projection
  upserts a row on insert events and deletes on remove events. Events
  outside both sets are ignored.
- **`rowKey`** — how to identify the row. Two primitives:
  `AggregateIdKey()` (key = `event.aggregateId`) and
  `CompositeKey(['data.field1', 'data.field2'])` (key = the
  concatenated values at those dotted paths).
- **`rowData`** — what columns to write. `WholePayload()` writes the
  entire `event.data` map. `SelectedFields([...])` writes a subset.
  `PayloadField('section')` lifts a sub-map.

For an aggregate-style projection (one row per aggregate, deep-merged
from successive events):

```dart
final invoiceSpec = AggregateProjectionSpec(
  viewName: 'invoices',
  interest: const SubscriptionFilter(aggregateTypes: {'invoice'}),
  tombstoneEventTypes: const {'invoice_deleted'},
);
```

Successive events whose payloads include `{amount: 100}` then
`{status: 'paid'}` produce a row `{amount: 100, status: 'paid'}`.
Missing keys preserve prior values; explicit `null` clears.

The substrate stamps several auto-columns on every aggregate-projection
row: `aggregateId` (the canonical id of the aggregate; matches
`event.aggregateId`), `latestEventId`, `updatedAt`,
`firstEventTimestamp`, and `sequence`. The names are camelCase on the
row map. They sit alongside the columns derived from your event data,
and consumers' row mappers can read them directly. Note that the
substrate's event-row storage uses snake_case keys (`aggregate_id`,
etc.) — those are stored events, not projection rows. The
projection-row convention is the one you write mappers against.

## Subscribing to state

Once your app is up, you read state through `eventStore.subscribe<T>`:

```dart
// Raw events as they happen
final eventsStream = eventStore.subscribe<StoredEvent>(
  const SubscriptionFilter(eventTypes: {'patient_renamed'}),
  const Events(),
);
await for (final update in eventsStream) {
  if (update is Delta<StoredEvent>) {
    print('Patient renamed: ${update.value.data}');
  }
}

// Materialized rows as they change
final patientsStream = eventStore.subscribe<Patient>(
  const SubscriptionFilter(aggregateTypes: {'patient'}),
  AggregateMode<Patient>(
    viewName: 'patient_by_id',
    mapper: (row) => Patient.fromJson(row),
  ),
);
await for (final update in patientsStream) {
  switch (update) {
    case Snapshot<Patient>(:final value):
      // Initial state at subscribe time
    case EndOfReplay<Patient>():
      // Backlog finished; updates are now live
    case Delta<Patient>(:final value):
      // A patient row was updated by a new event
    case Tombstone<Patient>(:final aggregateId):
      // A patient row was deleted
  }
}
```

`Events()` mode emits only `Delta`s as new events arrive — no replay,
no snapshots. Use it when you genuinely want events, not rows.

`AggregateMode<T>` mode replays the current materialized state into the
stream first (one `Snapshot` per matching row, then an `EndOfReplay`),
then delivers live updates. Use it for building UI that mirrors the
state of a view.

Delivery is at-least-once and preserves log order. Subscribers can drop
and re-attach; the library will replay from current state, not from
genesis.

---

## Advanced: metadata, ingest, and the cryptographic trail

The sections above describe the substrate as you'd use it for a
single-installation app: one server, one database, one source of truth.
Everything below is what the substrate also provides, mostly invisibly,
to support audit, regulatory compliance, and cross-installation sync
(the multi-source roadmap item, `spec/roadmap/multi-source-editing.md`).

You can ship a working application without engaging with any of this.
But knowing it's there shapes how you think about debugging, retention,
and integration with other systems.

### Every event carries a metadata record

When an event hits the log, the substrate stamps several fields onto it
that are not part of your payload:

- **`sequence_number`** — the event's position in the global total
  order for this installation.
- **`event_id`** — a UUIDv4 the substrate generates per event.
- **`event_hash`** — a SHA-256 deterministically derived from the
  event's canonical-form content, both version fields, the provenance and
  the causal object below included (see `spec/prd-canonical-json.md` for
  the serialization contract).
- **`previous_event_hash`** — the hash of the event this installation's
  database authored immediately before it, or null for the first. This
  chains the events each database authors: any modification, insertion,
  or deletion of a prior event breaks the chain from that point forward.
- **`causal`** — the event's kind (`version` or `annotation`), whether a
  later event may name it as a parent (`eligible`), the versions of its
  aggregate it follows (`parents`: the aggregate's latest eligible
  version, or none). The entry type's
  `EventTypeDeclaration` for the event type (in the definition's
  `declarations`) decides kind and eligibility, an eligible version when
  it declares none; the substrate stamps the object and producers cannot
  set it.
- **`entry_type_version`** — the version of the entry type at the time
  this event was appended, a major and a minor number
  (`{"major": 1, "minor": 0}`). Read from the `EntryTypeRegistry` you
  passed to `bootstrapAppendOnlyDatastore`. Producers don't choose it;
  the substrate stamps it.
- **`lib_format_version`** — the data-format version of the
  `event_sourcing` build that appended the event
  (`LibVersion.dataFormat`), also a major and a minor number. It
  versions what the library stores and sends, not the package version
  (`LibVersion.version`). Same idea, one level up.
- **`metadata.change_reason`** — a free-form string describing the
  reason for the change. Defaults to `"initial"` if you don't supply
  one.
- **`metadata.provenance`** — a list of `ProvenanceEntry` records,
  each saying "this event passed through hop X at time T running
  software version V", naming the database that stamped it
  (`database_id`) and the `event_sourcing` version that stamped it
  (`library_version`). The originator appends the first entry; each
  forwarder appends another (more on this below).
- **`metadata.action_invocation_id`** and **`metadata.action_name`**
  — for events emitted by an action, the dispatcher stamps both so the
  audit log can correlate all the events from one dispatch.
- **`flow_token`** — an optional caller-supplied correlation id you
  can use to thread a chain of dispatches together for tracing.
You generally don't read most of these — they exist for the substrate's
own bookkeeping. But you can: every `StoredEvent` exposes them, and
`backend.findAllEvents(...)` lets you query the log by `entryType`,
`originatorIdentifier` / `originatorHopId`, `clientTimestamp` range, and
so on.

One related record is *not* stamped onto the event: an optional
`SecurityDetails` — connection/request telemetry (`ipAddress`,
`userAgent`, `sessionId`, `geoCountry`/`geoRegion`, `requestId`) supplied
at append time. The substrate persists it to a **separate**
security-context store keyed by `event_id`, deliberately keeping
request-level PII out of the event record and its metadata.

### Denial event payloads

Every dispatch that fails records a denial event under
`aggregateType: action_attempt`, `entryType: action_denial`, with one
of six `eventType` values. The aggregate id is the dispatcher-
generated `invocationId` (also stamped onto
`metadata.action_invocation_id`), so a query for the denial and any
successful events from the same dispatch joins on a single id.

Common fields stamped by the dispatcher:

- `metadata.action_invocation_id` and `metadata.action_name` on every
  denial event (per "Every event carries a metadata record" above).
- `flowToken` is preserved if the caller supplied one.
- `error_message_sanitized` is run through a sanitizer that strips
  stack-trace lines, file URIs, and absolute paths so the audit log
  doesn't accidentally leak filesystem layout into long-lived
  storage.

Per-eventType payload fields under `data`:

- **`unknown_action`** — Stage-1 lookup failure. The submitted action
  name was not in the registry.
  - `requested_name` — the name as supplied by the caller.
- **`parse_denied`** — Stage-3 parse failure. `Action.parseInput`
  threw, or Stage-pre-3 caught `Idempotency.required` with no key.
  - `action_name` — the registered name.
  - `error_class` — the runtime type of the thrown error
    (typically `FormatException`, `ArgumentError`, or
    `MissingIdempotencyKeyError`).
  - `error_message_sanitized` — the error's stringified message,
    sanitized.
- **`validation_denied`** — Stage-5 validation failure.
  `Action.validate` threw.
  - `action_name`, `error_class`, `error_message_sanitized` as
    above.
  - `field_path` (optional) — when the validation throw was an
    `ArgumentError` whose `name` is set, the dispatcher stamps it
    here for audit drill-down.
- **`authorization_denied`** — Stage-6 authorization failure. Used
  for both `notGranted` (policy said no) and `scopeUnresolvable`
  (the dispatcher's pre-policy scope-resolution check failed: see
  `EVS-DEV-scope-unresolvable-denial`).
  - `action_name` — the registered name.
  - `permission_denied` — the `Permission.name` that was denied.
  - `principal_active_role` (optional) — present when the principal
    is a `UserPrincipal`; the `activeRole` they claimed.
  - `deny_reason` (optional) — `DenyReason.name` for richer audit
    (`notGranted`, `scopeUnresolvable`, etc.).
  - `scope` (optional) — when the dispatcher had a resolved
    `ScopeValue` to record, its `toJson()` payload is stamped here.
    This fires for `notGranted` denials with a valid scope AND for
    `scopeUnresolvable` denials where a class-mismatched scope was
    returned (the offending value, not a successful match). Per
    `EVS-DEV-scope-unresolvable-denial/E`, this lets the audit log
    capture the precise denial circumstance.
- **`idempotency_mismatch`** — Stage-4 content-mismatch failure. An
  unexpired cache entry exists for `(action_name, principal_id,
  idempotency_key)` but the submitted `rawInput`'s canonical-JSON
  differs from the cached value (see PRD-action-dispatch/E above).
  - `action_name` — the registered name.
  - `idempotency_key` — the colliding key.
  - `cached_raw_input_hash` — SHA-256 hex digest of the cached
    entry's canonical-JSON `rawInput`.
  - `submitted_raw_input_hash` — SHA-256 hex digest of the current
    submission's canonical-JSON `rawInput`.
  The full inputs are deliberately NOT carried in the payload —
  they may contain sensitive data and the hashes are sufficient for
  auditors to correlate the collision with the cached entry and the
  original submission's recorded events.
- **`execution_failed`** — Stage-7 (execute) or Stage-8 (persist)
  failure. The action's `execute` threw, or the transaction rolled
  back during the post-execute append.
  - `action_name`, `error_class`, `error_message_sanitized` as
    above.

Denial events are appended through the same `EventStore.appendInTxn`
path as success events, with the same hash-chain stamping and
provenance machinery; they are first-class citizens of the audit
log. The only structural difference is the `aggregateType` /
`entryType` pair — which lets retention sweeps, audit dashboards, and
ingest filters separate "what the system rejected" from "what the
system accepted" without parsing the payload.

### The library records its own version in the log

A build of the library carries two versions: its package version
(`LibVersion.version`) and its data-format version
(`LibVersion.dataFormat`, a major and a minor number that versions what
the library stores and sends). The data format decides; the package
version is recorded for audit.

`EventStore.open` runs its whole boot in one storage transaction. On the
first open of a database it mints the database identity
(`EventStore.databaseId`, the same for every event store over the
database) and appends a `lib_version_initialized` event recording the
identity, the package version and the data format. Each later open:

- If the package version and data format match the latest recorded
  ones: nothing is appended.
- If either differs and the data-format major is the same: append a
  `lib_version_changed` event, whether this build is newer or older (a
  rollback, or an older revision running beside a newer one).
- If the recorded data-format major differs: refuse with
  `DataFormatIncompatibleError` before writing anything. An older build
  cannot read a newer major, and a newer build has no reader for an
  older one; the way out is the build that wrote the database, or a
  restore.

Only the library-version events the database appended itself count.
Events ingested from a peer carry a receiver provenance entry, and the
boot ignores them, so a peer's newer build never reads as this
database's version. A stored identity that is missing or differs from
the recorded one is refused (`DatabaseIdentityMismatchError`), and a
database an earlier data format wrote is refused by name
(`DatabaseResetRequiredError`): it must be reset. There is no override;
`EventStore.openForTest` refuses the same databases and appends no
library-version event, and it is for tests only.

The same boot runs the "entry-type downgrade refusal", before it writes
anything: if any registered entry type's major is less than the major
recorded for that type in the database's generation record, the open
fails. The
substrate will not silently re-interpret an event under an older major.
A build registering an older minor of the same major opens: minor steps
only add fields with defaults, and it folds its views into copies of its
own.

The same boot also checks the database's generation record: the
highest data generation any committed boot registered. A build of another
data-format major, or one registering a lower major of an entry type than
an earlier boot did -- whether or not a view names that entry type -- is
refused before anything is written. While the build runs, a live instance
of a conflicting build cannot open the database at all (see "Open a
storage backend").

On Postgres the boot transaction's first statement locks the table
holding the sequence counter, which every append writes, so the appends
of a revision serving the same database wait for the boot to commit
rather than abort it. That wait lasts for the whole boot transaction: its
reads of the library-version and registry audit events, the latest event and the
view copies' records, its checks, and its creating and marking of view
copies. The boot folds no view row. The library stores a view per
definition: a view whose definition is new or changed, a newer minor of an
entry type it folds included, gets a new copy that catches up with the log
after the open returns, in transactions of at most 200 ms, each ordered
against the appends, one at a time per copy whatever the number of
instances (`spec/dev-view-convergence.md`). Catch-up transactions lock
the sequence counter's table in a mode that does not conflict with other
catch-ups, so an append waits at most one 200 ms bound however many
instances or copies are catching up, and a view becomes converging only
when an event its own definition folds is stored past its watermark.
While a view converges, reads report it as converging, return only its
settled rows and name the rest as pending; the library's own permission
checks refuse with a transient error until the permission views they read
are current (`spec/dev-converging-view-reads.md`). Read a view's
convergence state and progress before you act on a view you need
whole. On the web the boot holds back the other tabs' writes to the
database the same way.
`bootLockWait` (default 60 s; on the web the `SembastBackend`
constructor's) bounds each wait of a boot: for the boot lock another boot
or a provisioning holds, and for the lock that holds the writes back. It
must exceed the longest boot the deployment expects, since a boot holds
the boot lock for its whole duration and another instance's open fails
once its wait runs out.

Deploying. Builds with the same data-format major and the same
entry-type majors share a database in any mix: a no-traffic canary
beside the serving revision, several instances, a restart, a rollback
to the previous release. A build of another data-format major, or one
that raises an entry-type major, is deployed stop-then-start: every
instance of the old revision stops before the first instance of the new
one opens the database, and the old revision's next open is refused
afterwards. The generation guard enforces that order: the new revision's
open is refused while an instance of the old one is still connected.
Recovery after such a deployment is a restore from a backup
taken before the switch, or a roll-forward. Evolve compatibly where you
can: add an optional field as a minor step, and make a real reshape a
new entry type that you append instead of the old one.

A database never receives its own events back in normal operation. A
destination delivers only the events its own database authored, so an
event whose originator entry names the receiving database arriving through
ingest is a clone's or a tamperer's: ingest records a security finding for
it, held or not, and stores it when it is not held. A database restored
from a backup gets its lost events back by being rebuilt as a successor
that restores them from its receiver (see "Delivery channels, and either
end going back in time").

### Schema evolution: entry types and promoters

When you need to evolve an event shape — rename a field, add a default,
drop a column — you raise the entry type's `registeredVersion` and
register a `PromoterSpec` describing the transformation. The substrate
supplies a small fixed set of promoter primitives (`RenameField`,
`DefaultField`, `DropField`) that are deliberately limited to
shape-changes. Adding a field with a default is a minor step, whose
promoters may only be `DefaultField` (or none); renaming or dropping a
field is a major step, to minor 0 of the next major.
`PromoterRegistry.register` refuses any other step:

```dart
final promoters = PromoterRegistry()
  ..register(const PromoterSpec(
    viewName: 'notes',
    entryType: 'note',
    fromVersion: EntryTypeVersion(1, 0),
    toVersion: EntryTypeVersion(1, 1),
    transforms: <TransformPrimitive>[
      DefaultField(fieldName: 'language', defaultValue: 'en'),
    ],
  ))
  ..register(const PromoterSpec(
    viewName: 'notes',
    entryType: 'note',
    fromVersion: EntryTypeVersion(1, 1),
    toVersion: EntryTypeVersion(2, 0),
    transforms: <TransformPrimitive>[
      RenameField(sourceField: 'body', targetField: 'text'),
    ],
  ));
```

The library checks the shape of every step it registers; what it
cannot check is producer code. A minor bump is safe only when it adds
optional fields: producers do not rename, drop or re-type a field
within a major. Make any other change a major step.

Two paths exercise the promoters:

- **Catch-up of a new copy.** A newer minor of an entry type a view
  folds is a new view definition, so after an upgrade the view gets a
  new copy that starts empty and folds the log from the start after the
  open returns, each event promoted through the chain to the registered
  version. The rows are the rows `rebuildView` produces, and until the
  copy is current the view reports itself as converging.
- **Fold-time event promotion.** When an event of an older version is
  folded -- one ingested from an older peer (see below), or one an
  older build appended -- the substrate runs the promoter chain on
  the payload before passing it to the projection fold. The log
  records the event at its original `entry_type_version`; the
  promotion happens in-memory.

A promoted `DefaultField` fills a field only when neither the event nor
the view row it folds into already carries it, looked up under the
name the field has at the registered version (after any later rename
in the chain). An older event that does not mention a field leaves the
row's value as it is, exactly as the fold treats any key an event
omits; a table view has no row to decide against, so there the default
is always supplied.

Both paths exist because the schema-evolution discipline says "the log
is canonical; you can reconstruct any past state by replaying the
events through the current promoter chain."

Builds that define a view alike share one copy of it and fold into it as
they store events. A build whose definition differs -- a canary that adds
a view, or changes a view's interest or shape -- folds a copy of its own,
which catches up in the background while the copy the serving revision
folds stays as it is; a copy no running build registers is deleted. An
interest predicate or a table view's row functions are not part of the
definition the library compares, so run `rebuildView` after changing only
such a function.

### Provenance: where an event has been

Every event carries a list of `ProvenanceEntry` records in
`metadata.provenance`. The first entry is the originator: the
installation that produced the event, identified by its
`source.identifier` (a UUIDv4 you persist per install). Each
subsequent entry, if any, names a forwarder — an installation that
received the event from somewhere else and is now passing it on.

For a single-installation app, every event has exactly one provenance
entry: the install itself. The provenance chain is what makes
multi-installation sync auditable: the chain says exactly which
deployments handled an event and when.

### Cross-installation ingest

The library is designed for multi-installation deployment from the
ground up, though the 0.x substrate treats one source per aggregate
type as canonical. The ingest path is the inbound side of that
design:

- A `Destination` is the outbound transport for forwarding events to
  another installation. You register destinations at composition time
  by passing them to `bootstrapEventStore`; the delivery cycle enqueues
  outbound events through them.
- On the receiving side, the substrate exposes an ingest entry point
  that accepts a `BatchEnvelope` of events from a peer, verifies the
  hash chain against what's stored, extends the provenance chain, and
  admits the events into the local log. Ingested events flow into
  projections and subscriptions identically to locally-produced events.
- The substrate verifies hash-chain integrity on every ingested event:
  the event's `event_hash` against the canonical hash of its content,
  and each receiver hop's arrival hash against the record the hop before
  it stored.
  An event that fails a check is stored as received, and ingest appends
  a reserved security finding recording exactly what failed, with the
  hashes and positions it compared; delivery continues. The chain
  verification operation's `ChainVerdict` lists the same kinds of
  finding for the stored log, and the operation records each of them.
- Reserved system events (the library's own audits, such as a destination
  wedge) are appended only by the library: `append` and `appendInTxn`
  refuse their entry types. Ingest admits a peer's reserved event only in
  the aggregate type and event types the library appends it with (fixed
  within a data-format major), and a destination audit only with a
  destination identifier and the appending database's identity, each a
  non-empty string without `|`, the identity being that of the database
  its originator entry names. Ingest stores no event for anything else,
  nor for a record without a causal record or a library version in every
  provenance entry, nor one whose data has a top-level key starting with
  `$`: it keeps the record in full in a security finding naming the
  reason and admits the rest of the delivery, so a bad record never holds
  the channel. A batch whose delivery hash does not recompute is refused
  as a transient failure, with a finding, and sent again.
- Every store folds the library's default destination-wedges view
  (`defaultDestinationWedgesSpec`, view `default_destination_wedges`): one
  row per wedged destination, keyed `<database identity>|<destination>`,
  inserted by a wedge event and removed by the recovery or deletion that
  ends it. It is a convention over the log, not a read of the queues: rows
  whose `database_id` field is the store's own `databaseId` name the same
  wedged heads as `backend.wedgedFifos()` (read each in its own
  transaction, the two can differ for the moment between them), and rows
  for other databases come from wedge events a peer forwarded, as that peer
  asserts them. Tell local rows from peer rows by that field.

Activating that machinery — canonicalization rules: per-aggregate-type
rules saying who is the canonical authority for that aggregate, who can
approve another deployment's edits, and how conflicts resolve — is the
multi-source roadmap item (`spec/roadmap/multi-source-editing.md`).

### Delivery channels, and either end going back in time

A destination that serializes natively is a delivery channel between your database and the receiver. The library numbers every delivery on it and links each to the one before, and the receiver accepts only the delivery that follows the last one it accepted. The receiver's acknowledgement and refusal carry its record of the channel, and your destination's transport must return it intact: a `SendOk` without it wedges the destination. There is no separate polling. The library realigns a channel on its own in the safe cases: a lost acknowledgement is recognised when the retry's answer names the delivery in flight, and when a receiver was restored to an earlier point, the library sends again exactly the deliveries the receiver lost and records a resume event, which follows the destination's filter. When the receiver's record is ahead of yours and names a delivery your database never attempted, your database went back in time (a device restored from an older copy): the library records a `sender_regressed` security finding and keeps delivering, and your application then rebuilds the device as a successor (below). Every other record no safe path explains -- the two ends disagree about a delivery both should hold, or the receiver answers from a different database -- records a `channel_unexplained` finding, and the channel starts a new generation: its deliveries start again at 1 and the destination refills from the start of your log, and the receiver keeps only what it lacks. Succession events and security findings go to every receiver whatever its filter. Every other integrity anomaly -- an event whose hash does not verify, a fork or a reused origin position, a second live copy of your database delivering -- is a security finding too: the side that detects it appends one reserved finding event with its evidence, stores what it received as it received it, and keeps delivering. No channel stops for an integrity reason. Each detector records an anomaly once: ingest, the chain walk and the sender each record what they detect, so one anomaly may appear once per detector. Run the chain verification operation when you choose, over a range from the last position you verified or over the whole log; it holds nothing your appends wait for, walks up to the last event stored when it starts, and records a finding for each anomaly it reports. A full walk of a large log is slow, so run it where your deployment can afford it, such as a maintenance window. A destination whose receiver is not this library -- a third-party format built by your transform -- is not a channel: the library cannot tell whether that system lost deliveries, and reconciling with it is your application's job. Serve deliveries and restore pulls with the library's receiver endpoint, passing it the sender database identities your authentication binds to the caller; do not hand-build batch envelopes or download responses; your destination implements the pull operation the restore uses. A reset, reinstalled or rebuilt device is a new sender; before it authors any entry of its own, it restores its predecessor's data with the library's restore operation, which records the succession. A restore into a database that has already authored entries is refused. The restore brings back the predecessor's whole succession lineage, every channel and generation the receiver holds, so a device reset twice restores everything its predecessors delivered. The restore trusts the receiver not to invent events under the predecessor's identity. Every receiver your successor delivers to must authenticate the successor's caller for the predecessor as well, or it refuses the succession. A destination never forwards the events its database ingested or restored from another sender.

When a restored database had appended edits before it learned of the restore, its receivers hold both histories. Each fork, and each origin position both histories use, is recorded as a security finding, and the default views fold every entry the findings reach as usual and mark it, in the row's `$integrity.security_findings`, as having an outstanding finding; the mark reaches every entry with an event of that database at or above the fork's lowest position. The mark stays; reviewing and clearing findings is not part of the library yet. The restored device itself records a sender-regressed finding and keeps delivering; rebuild it as a successor that restores everything its receiver holds.

### Hash chain and ALCOA+

The cryptographic discipline of the log is what lets the substrate
make Layer-1 promises to regulators. The hash chain anchors at the
deployment that originated each event, so any tampering with a prior
event invalidates the chain from that point forward. An independent
observer can recompute the chain from the stored log alone, with no
authentication, and verify integrity.

The library aligns with FDA 21 CFR Part 11's ALCOA+ principles:

- **Attributable** — every event records its initiator.
- **Legible** — events are canonical JSON, human- and machine-readable.
- **Contemporaneous** — events record their timestamp at append.
- **Original** — append-only, hash-chained, immutable.
- **Accurate** — validation precedes recording; denials are recorded.
- **Complete** — every dispatch produces a recorded outcome.
- **Consistent** — total ordering across the log; per-aggregate-per-authority ordering.
- **Enduring** — durable persistence; destinations queue across reboots.
- **Available** — read from any position; filtered subscriptions.

You don't have to do anything to get any of this. It's structural in
the substrate.

---

## Cross-process client/server deployments

> The `reaction` package ships both halves: the in-process `Local*`
> impls (used by the substrate's demos) and the cross-process
> `Remote*`-plus-server half. The normative spec lives at
> `spec/reaction-remote.md`; the implementation lives under
> `reaction/lib/src/remote/` and `reaction/lib/src/server/`, exercised
> by the package's unit and end-to-end test suites.

Everything covered so far assumes one process. The Flutter app you
build runs the substrate directly: it opens an `EventStore`, holds the
`ActionDispatcher`, registers projections, and reads view rows out of
its local sembast database. This is the entirety of a mobile-only
deployment — one install, one log, one principal.

Many real deployments are not shaped that way. A server deployment,
for example, has several roles of users working from a Flutter web
app in their browsers, talking to a server that runs the substrate
against a Postgres database. The browser is too constrained to run a
full `EventStore` and shouldn't anyway — the server's database is the
authoritative log for that deployment. The substrate is
single-process; the deployment is two-process. Something has to
bridge them.

That something is `reaction`. It's a sibling package, not part of
`event_sourcing` itself, because adding HTTP + WebSocket + shelf
dependencies to the substrate would force them on every consumer
including embedded-only mobile callers. The substrate stays narrow;
`reaction` layers the wire on top.

### Why is this a different problem from the integrated app?

Splitting a process boundary down the middle of an event-sourced
application introduces five concerns that a single-process app
sidesteps entirely. Each one shows up as a load-bearing piece of the
design.

**1. Identity becomes a wire concern.** Whatever process the
deployment already uses to authenticate browser clients — Firebase,
an in-house OAuth flow, an issued linking code — is also the
process that hands the server an identity for the substrate to use.
The substrate has no built-in opinion about credential format; it
just requires the deployment to supply a `PrincipalAuthValidator`
that turns whatever string arrives over the wire into a `Principal`.
The lib ships `TrustingAuthValidator` for dev/test (accepts any
non-empty string as the user ID). Production deployments mount their
own validator at server-boot time. The substrate's existing
"Principal on faith" trust gap (enumerated in CLAUDE.md) is closed
from outside by this validator, which becomes a new enumerated trust
input alongside `StorageBackend` and `Destination`.

That's all that changes from the substrate's perspective. Nothing
inside the substrate gains authentication logic. The wire layer
brokers identity in; the substrate trusts the result the same way it
always has.

**2. The reactive primitive has to cross a wire.**

The substrate's `subscribe<T>` returns a `Stream<Update<T>>` with
atomic snapshot-then-deltas semantics: `Snapshot<T>` × N →
`EndOfReplay<T>` → live `Delta<T>` / `Tombstone<T>` × ∞. That stream
is the substrate's central reactive guarantee.

A browser-side widget can't subscribe directly to a server-side
substrate. The wire's job is to deliver the same stream faithfully:
the receiver should see exactly the sequence of `Update<T>` values a
co-located in-process subscriber would see, in the same order, with
the same atomicity. The wire is a relay, not a re-interpretation —
this is the load-bearing "no third epistemic layer" promise pinned by
`spec/reaction-remote.md`'s charter section.

Concretely: the wire ships each `Update<T>` as a JSON envelope over a
multiplexed WebSocket. Rows transit as opaque `Map<String, Object?>`;
the consumer's mapper applies client-side, just like in the
in-process case. The server runs `eventStore.subscribe<T>` per
accepted subscription and relays envelopes through a per-connection
write queue that preserves per-subscription ordering on the wire.

**3. Read-path authorization happens at the wire.**

In the integrated case, a UI gating decision ("can this user see
record R-42?") happens client-side against the locally-known
permission state. There's no risk: the user already has the entire
log in front of them, and the UI is just choosing what to render.

Cross-process, the server *must* enforce. A browser-side filter on a
WebSocket subscription is a UI affordance, not a security boundary.
The server must compose the requesting Principal's permitted
scope into the substrate `subscribe<T>` filter so events outside
scope never travel over the wire.

`reaction`'s server applies what the design spec calls "Approach B":
two-tier authorization at the moment of subscribe.

- **View-level deny.** Does the Principal hold the permission named
  by this view? (Substrate default: `view:<viewName>`; deployments
  override.) If not, close the subscription request with a
  `subscription_denied` envelope. The client distinguishes "you can't
  see this view" from "nothing matches your scope" cleanly.
- **Row-level narrowing.** AND-merge the Principal's
  `scopeAssignments` (from `EffectiveAuthorization`) into the
  requested `aggregates` filter, expanding through `ContainmentRef`
  projections where the view's scope class is an ancestor of the
  user's assignment. The expansion is the same `ContainmentResolver`
  the substrate uses for action authorization — read-path and
  write-path use the same scope mechanism, by design.

The wire's per-subscription narrowing consults
`EffectiveAuthorization.scopeAssignments` and walks containment
projections — the scope-aware substrate is what lets it express
"the patients at the sites this user covers" rather than filtering
only by role-to-permission grants.

**4. The closed-under-events guarantee splits cleanly.**

A nice surprise: nothing about the wire layer breaks the substrate's
closed-under-events story. Action outcomes (success vs.
`DispatchAuthorizationDenied`) are still produced by the same
dispatcher with the same `Principal`, recorded into the same log,
reproducible from `(events, lib_version)`.

What the wire adds is an *external* trust input (the
`PrincipalAuthValidator`) that determines which `Principal` the
substrate sees. That input is enumerated and named, the same way
`StorageBackend` is. State reconstruction-from-the-log still holds
*under the recorded Principal*; what the validator does on the way in
is a deployment concern, the same way what the storage backend does
to durability is a deployment concern.

The substrate doesn't need to know about the wire at all. It receives
calls from local code that happens to have been forwarded by shelf
routes from a browser — but those calls look identical to calls from
an embedded mobile app. This is what "substrate-agnostic" means in
practice: the dispatcher, the projection model, the permissions
machinery all behave identically whether the originating click was in
a Flutter app or a browser two thousand miles away.

**5. Network failure is no longer a substrate problem; it's a UX
problem.**

In the integrated case, an action either dispatches or doesn't — the
in-process call returns a `DispatchResult` synchronously. A
subscription delivers events until the app closes.

Cross-process, the wire fails sometimes. WebSocket connections drop
on tab toggles, network changes, idle timeouts. HTTP requests time
out. Tokens expire.

`reaction` handles wire failure for you. On WS drop, `RemoteScope`
transitions its `ConnectionStatus` to `Reconnecting` and auto-reconnects
with exponential backoff (≈250 ms initial delay, ×2, capped at 30 s,
~10 attempts). On recovery it re-issues every active subscription, each
replaying `Snapshot × N → EndOfReplay → Delta…`, so the widget converges
back to live state on its own — typically a brief skeleton/loading flash
while the fresh snapshot streams. Existing subscription streams surface a
`'wire_disconnected'` error only if the reconnect policy gives up (status
→ `Disconnected`) or the connection is closed for auth reasons. Consumers
observe all of this through the authoritative `ConnectionStatus` surface
(see below), not by inferring it from stream liveness.

A resume-from-sequence reconnect mode is a roadmap item
(`spec/roadmap/reaction.md`): it would re-tail from the client-known
sequence on reconnect instead of replaying the full snapshot.

Auth expiry is the parallel concern. The wire surfaces it explicitly:
HTTP 401 from any route and either a `4001 auth_rejected` or `4003
permissions_changed` close-frame on WebSocket flip the `AuthSession`
status to `Expired`. (See *What happens when permissions change
mid-session* below for when 4003 fires.) Other close codes are not auth
events: `4002 server_shutting_down` and abnormal closes such as
`1011`/`1006` are treated as reconnect-able drops, handled by the
auto-reconnect above rather than flipping to `Expired`. The UI distinguishes "log in
again" (expired) from "log in" (never had a session) — useful enough
that `AuthStatus` is a sealed three-variant type (`NotAuthenticated`,
`Authenticated`, `Expired`) and consumer code switch-exhausts over it.

### Four interfaces that absorb the difference

The point of `reaction` is that *consumer code shouldn't have to
change* between the integrated and cross-process deployments. Widget
code that calls a `submit()` and watches a `Stream<Update<T>>` should
be source-identical whether the underlying impl is local or remote.

Four interfaces hold that line. They live in `reaction`'s public
surface; both `Local*` and `Remote*` impls of each exist.

- **`AuthSession`** — credential lifecycle. Surfaces `AuthStatus`
  (`Authenticated` carrying the validated `Principal`,
  `NotAuthenticated`, `Expired`). `setCredential(String?)` to log in
  or out. The active `Principal` flows from here into the other
  three.
- **`ActionSubmitter`** — `Future<DispatchResult> submit(ActionSubmission)`.
  Local impl wraps `ActionDispatcher.dispatch`; Remote impl POSTs to
  `/actions`.
- **`ViewSource`** — `Stream<Update<T>> watch<T>(...)`. Local impl
  wraps `EventStore.subscribe<T>`; Remote impl multiplexes a
  client-chosen `subscriptionId` over the shared WS.
- **`PermissionSource`** — `EffectiveAuthorization? get current` plus
  a `Stream`. Carries the active role's permissions and the user's
  scope assignments so UI code can both gate affordances and
  pre-filter scoped lists. Local impl reads the
  `role_permission_grants` + `user_role_scopes` projections through
  `AuthorizationPolicy.effectivePermissionsFor`; Remote impl fetches
  from `/permissions/snapshot` and refreshes over the WS.

Alongside the four interfaces, the composed scope exposes an
authoritative connection-liveness signal — `ConnectionStatus get
connectionStatus` plus a `Stream<ConnectionStatus> connectionStatusStream`,
with three variants: `Connected`, `Reconnecting`, `Disconnected`.
`LocalScope` reports `Connected` for its entire lifetime (in-process has
no transport to lose); `RemoteScope` drives the transitions from the
WebSocket lifecycle. Consumers read this to render reconnecting/offline
affordances rather than inferring connection state from whether a
subscription stream has gone quiet.

Widgets and other consumers depend only on these. The deployment
choice — composing `LocalScope` (in-process bundle) versus
`RemoteScope` (HTTP+WS bundle) — happens once at app boot. Everything
downstream is shape-identical.

### Server-side adapters

There's no separate "reaction server" process. There's no
`ReactionServer` class either. The lib ships **shelf-compatible
handlers** that you mount inside whatever server you already have.

Deployments that already run `shelf` + `shelf_router` servers — with
their own middleware pipelines (auth, OpenTelemetry, CORS, request
logging) — mount the reaction handlers alongside their existing
routes; bespoke per-view REST read endpoints collapse into the
generic subscription handler, with the per-feature business logic
living in substrate `Action`s — same dispatch pipeline, same audit
trail, same permission story as a single-process app.

What `reaction` exposes to make that work:

```text
reaction/lib/src/server/
  reaction_handlers.dart      ReactionHandlers(eventStore, dispatcher,
                                                policy, viewScopeRegistry)
                                .me            -> shelf.Handler
                                .actions       -> shelf.Handler
                                .permissions   -> shelf.Handler
                                .subscriptions(validator) -> shelf.Handler  (WS upgrade)

  auth_middleware.dart        authMiddleware(PrincipalAuthValidator)
                                -> shelf.Middleware
                              principalFromContext(Request)
                                -> Principal?

  trusting_auth_validator.dart TrustingAuthValidator   (dev/test only)
```

A consumer mounts the handlers into their existing router however
they like:

```text
final reaction = ReactionHandlers(
  eventStore: store, dispatcher: dispatcher,
  policy: policy, viewScopeRegistry: appViewScopes,
);

// HTTP routes go behind the app's existing auth middleware (it
// reads the Bearer header and attaches a Principal to the request
// context). The WS route is registered on the outer router with NO
// middleware — the WS upgrade path cannot carry an Authorization
// header from Flutter web; credentials arrive in the first WS
// message and are validated by the supplied PrincipalAuthValidator.
final httpRouter = Router()
  // The app's existing custom routes:
  ..get('/api/v1/config', appConfigHandler)
  ..post('/api/v1/auth/login', loginHandler)         // pre-auth
  // The reaction HTTP handlers, gated by the app's auth middleware:
  ..get('/api/v1/me',                   reaction.me)
  ..post('/api/v1/actions',             reaction.actions)
  ..get('/api/v1/permissions/snapshot', reaction.permissions);

final httpPipeline = const Pipeline()
    .addMiddleware(appAuthMiddleware)
    .addHandler(httpRouter.call);

final topRouter = Router()
  ..get('/api/v1/subscriptions',
        reaction.subscriptions(validator))
  ..mount('/', httpPipeline);
```

Authentication composes with whatever the consumer is already doing.
A deployment that already runs Firebase ID-token middleware on its
authenticated routes needs no change to that middleware; reaction's
handlers just read `Principal` from the request context via
`principalFromContext(req)`. The existing Firebase middleware
populates that context, and reaction's handlers consume it — one auth
flow, two route consumers. For deployments that don't have their own
middleware yet (early demos, local dev),
`authMiddleware(TrustingAuthValidator(...))` is a one-liner; reaction will
use it identically.

`ReactionHandlers` is a config bundle, not a server. It doesn't own
the `EventStore`, doesn't own the `ActionDispatcher`, doesn't own
the HTTP server lifecycle. It holds the four substrate handles + the
view-scope registry so each handler closure doesn't have to take
them individually. Beyond that it imposes no structure on the
consumer.

The WebSocket handler is the only piece with non-trivial state —
per-connection it tracks the authenticated `Principal`, the set of
active subscriptions, and the substrate `subscribe<T>` streams it's
relaying. That state lives inside the closure
`reaction.subscriptions` returns; consumer code doesn't see it.
Connection cleanup (cancel all subscriptions on WS close) is
automatic.

### What happens when permissions change mid-session

A subtle question this design has to answer: a client opens a
subscription, is happily receiving updates, and an admin revokes the
client's role. What happens?

The natural answers point in three directions. The substrate's
`subscribe<T>` was opened with an `aggregates` filter set at
subscribe-time; that filter doesn't change once the stream is
running. So "the existing subscription continues delivering events"
is the default behavior — which silently shows revoked users data
they shouldn't see, until they reconnect.

The design's response is **asymmetric**, on the principle that
admin-driven security-narrowing is the only kind of permission
change serious enough to interrupt a user mid-session:

- **`role_unassigned` for this user, or `permission_revoked` from a
  role this user holds:** the reaction server closes the WS with a
  new close-frame code `4003 permissions_changed`. The client
  treats this like a token expiry — refetches `/me`, gets a fresh
  `Principal` reflecting the new role/permission state, reopens
  subscriptions against the new (narrower) authorization. From the
  user's perspective: a brief "your role was updated; please log in
  again" prompt. Security gap closed within milliseconds of the
  revocation committing. If the user signs in again with the same
  credential, the auth layer can mint a Principal as before —
  `TrustingAuthValidator` accepts any non-empty credential, and a
  production Firebase validator only verifies token freshness, not
  role state. But the substrate's policy reads `user_role_scopes` on
  every dispatch and now sees no row for the revoked role, so any
  action submission under that role denies with
  `DispatchAuthorizationDenied(<permission>)`. Re-login succeeds;
  submitting doesn't. The closed-under-events trust model holds
  without any cooperation from the auth layer.

- **`role_assigned` for this user, or `permission_granted` to a
  role this user holds:** the server sends a `stale_data` envelope
  on the open WS — carrying a typed `StaleDataReason` (`roleAssigned`,
  `permissionAdded`, or `containmentChanged`) meaning "your cached scope
  state may be out of date." The subscription stays open; the client decides
  whether to refresh. The user is currently authorized to see
  *less* than they could — that's a UX freshness issue, not a
  security one, so it doesn't warrant interrupting them.

- **Containment-projection changes** (a patient was re-parented from
  one site to another — the user's effective scope shifted as a
  side-effect of data movement, not an admin security action):
  by default, no signal. Routine trial operations would otherwise
  emit `stale_data` to every connected coordinator on every patient
  move. Deployments that want the freshness signal can opt in with
  `reaction.watchContainment('patient_site_index')` (or whatever
  containment projection matters); off by default.

Mechanically, the server runs **one** additional substrate
subscription — the `AuthzWatcher` — filtered to permission and
role-assignment event types (`role_permission_grant`,
`user_role_scope`). On each event it decides "this is a revocation"
(close affected WS connections with 4003) or "this is an expansion"
(send `stale_data` to affected WS connections). The watcher needs a
way to find "affected" connections, so the server maintains a
top-level `Map<userId, Set<WebSocketChannel>>` registry; each WS
connection registers on `auth_ok` and unregisters on disconnect.

This is the design's first concession to "the wire is a relay, not
just a relay" — the wire layer now actively translates substrate
events into wire signals. But it's a substrate-shaped translation:
the watcher uses the same `subscribe<T>` primitive every other
reactive consumer does, and the resulting wire messages are existing
envelope types plus one new (`stale_data`). No re-narrowing of
active subscription filters, no per-subscription state machine, no
new epistemic layer.

The `AuthzWatcher` does not cache permission state per-connection. A
per-Principal in-memory mirror would add complexity (per-connection
state, mirror-vs-projection races) for negligible benefit — substrate
policy queries are sub-millisecond and subscribe messages are
infrequent. Instead the watcher reacts to permission events and sends
the appropriate wire signal; it does not keep a live copy of each
Principal's `EffectiveAuthorization`.

### Several processes sharing one database

A server deployment often runs several processes against one Postgres
database: instances behind a load balancer, a canary beside the serving
revision, a replacement starting while the old instance stops. Each opens
its own backend and event store and serves requests. Delivery to
destinations is different: at most one delivery cycle commits queue
changes for a database at a time (`EVS-PRD-destinations/V`), within what
the backend's drain lock supports. The drain lock rests on deployment
properties the library does not audit: on Postgres, a lock session that is
one server session of the database (below); in the browser, Web Locks;
outside the browser, one opener of a Sembast file. A send already in
flight when a drainer loses its lock can still arrive (see "Fencing, and
the late duplicate").

#### One drains, the others stand by

Every process may start a delivery cycle with `SyncCycle.start`. The
backend grants the database's drain lock to one of them, which runs
(`SyncCycleState.running`); every other cycle stands by
(`SyncCycleState.standby`), requests the lock again every cadence, and
takes over when it is released -- when the drainer closes its cycle,
stops, or loses its lock -- without a restart. `start` never fails because
another process drains, nor because the database is briefly unreachable:
such a cycle starts in standby. A lock connection that is misconfigured
fails `start` with `DrainLockConfigurationException`. A second cycle over
the same database in one isolate is a programming error and throws
`StateError`.

On Postgres the drain lock is a session advisory lock whose key derives
from the database, the schema and the database identity, held on the
backend's lock session: the dedicated connection each `PostgresBackend`
keeps for its lifetime, to `lockUrl` when given and to `url` otherwise.
That connection must be one real server session: a direct connection, or
a session-mode proxy that resets sessions on release. A transaction-mode
pooler is not supported for it (the pool's own connections may use one).
`open` checks the session: it sets a random setting and reads it back with
the server process id in three separate statements, compares the database
and schema the session reaches with the pool's, and checks that the
session sees an advisory lock a pool connection takes, so both reach one
server; a mismatch throws `LockSessionConfigurationException`. The check
can miss a pooler that happens to return the same server connection every
time, so the requirement stands on its own. The library sets server-side
TCP keepalives and no idle-session timeout on the lock session; those
cover only the server's side of the connection, and a proxy between the
process and the database has client-side timeouts of its own for the
deployment to configure. Every statement on the lock session is bounded
by `lockQueryTimeout`, and the backend probes it every `lockHeartbeat`: a
failed probe declares the session lost, the backend opens a replacement,
ends the old server session if it still holds a library lock (so the lock
role must be allowed to end its own sessions), and registers its build's
generation again; a delivery cycle whose lock was lost returns to standby
and takes the lock again.

#### Fencing, and the late duplicate

Each acquisition raises the database's drain epoch, and every transaction
of the drainer that changes a queue -- the pass start, the fill, a halt
honour, the fence before a send and every outcome -- first checks that its
epoch is still the current one, and commits nothing otherwise. A drainer
that lost its lock without knowing it (its lock session was ended while
its process ran on) therefore records nothing more, and starts no further
send once it detects the loss. What it cannot stop is a send already on
the wire: that item may arrive at the receiver after the new drainer sent
it again. Delivery is at-least-once, and a receiver deduplicates.

#### What the other processes do

A process whose cycle stands by, or that starts none, keeps its event
store and registry and does everything but drain: it appends, sets
destination dates, requests and cancels halts, recovers a wedged head and
deletes a destination. Every registry operation acts on the persisted
state of the database, whichever process runs it, and none enqueues:
only the drainer's fill does. Its events and halt requests wake the
drainer only through the drainer's cadence (15 s by default), and the
cadence timer is armed again when a pass ends: they wait at most one
cadence after the drainer's current pass ends, and a pass that sends a
large backlog, or a send that hangs, holds them off for as long as it
runs. A halt request that the drainer has seen is honoured before its next
send. An append in the drainer's own process, and every registry
operation there, wake it at once.

Every process that may drain registers the same destinations, because
delivery uses the destinations registered in the process that drains. A
destination that the drainer does not register, that storage no longer
knows, that another process registered again under the same id, or that
a refill guard holds (below) is not filled or sent; its halt requests are
still honoured. The drainer reports each such gap: `SyncCycle.unserved` in
its own process, and `DestinationRegistry.readDeliveryStatus()` from any
process, which also shows the drainer's declared configuration and
heartbeat and each destination's open halt request, wedge and refill
guard. The default destination-wedges view shows wedges only, not these
gaps. Each registration appends a `destination_registered` event recording
the configuration the registering process declared, and the latest
registration's hard-delete opt-in is the one in effect.

#### Deployment requirements

Stated as properties, so that they hold wherever the processes run:

- A process that starts a delivery cycle has CPU while it has no requests
  to serve: the cycle's cadence and heartbeat are timers in that process.
- At least one such process runs at all times; otherwise nothing drains
  until one starts.
- Every process registers the same destinations.
- The lock connection is direct or a session-mode proxy, and the lock role
  may end its own sessions.
- A process that receives no traffic (a canary) either starts no delivery
  cycle, or may become the drainer and fill the queues under the
  configuration it declares; its configuration must then be acceptable to
  fill with.

At a switchover the new revision's processes stand by until the old
revision's drainer stops and its lock frees; then one of them takes over.
Provisioning a schema change for the new revision while the old one serves
is safe (it takes the boot lock and refuses a minimum a live instance does
not meet), and the schema changes of one release stay compatible with the
revision beside it.

#### Browser tabs

In the browser the tabs of an origin share one IndexedDB database, and the
same rule holds. The page must be a secure context (HTTPS or localhost),
or `EventStore.open` and `SyncCycle.start` refuse. A tab of a conflicting
build refuses to open while an older tab is open. One tab drains and the
rest stand by; the drain lock follows the visible tab, so nothing drains
while no tab of the origin is visible, and a page the browser freezes
before it hands the lock over holds it until it is resumed or discarded
(a liveness limit, not a safety one). Every tab registers the same
destinations. "Describe the storage" above has the details.

#### Versions and deployment

A build carries three kinds of version, and each decides something
different:

- The package version (`LibVersion.version`) is recorded in the log at
  every open by a different build (`lib_version_changed`), for audit; it
  decides nothing.
- The data-format version (`LibVersion.dataFormat`, major.minor) is what
  the library stores and sends. Builds of the same data-format major are
  compatible: a newer or older one opens the database and the open is
  recorded. Another major is refused (`DataFormatIncompatibleError`).
- Each entry type's registered version (major.minor) decides how its
  events fold. A minor step adds optional fields (its promoters may only be
  `DefaultField`, or none); a rename or drop is a major step.

Evolve compatibly: add an optional field as a minor step, and make a real
reshape a new entry type that you append instead of the old one. Revisions
whose data-format majors and entry-type majors agree can be canaried beside
the serving revision, scaled, and rolled back to freely, and each open is
recorded. A major bump -- a data-format major, or a rename or drop in an
entry type -- is deployed stop-then-start, and the incompatible-generation
guard enforces it: a new revision's open is refused
(`IncompatibleGenerationException`, nothing written) while an instance of a
conflicting build is connected, and once the new revision has booted, the
old revision's next open is refused. Recovery after a major bump is a
restore from a backup taken before the switch, or a roll-forward. A
deployment pipeline can compare the new build's `LibVersion.dataFormat`
major with the serving one's before it starts a canary. The lock-session
requirement covers the guard's locks as well as the drain lock.

The boot pauses every instance's appends only for its checks and its
creating and marking of view copies, and `bootLockWait` must exceed that:
"The library records its own version in the log" above has the details.
New view copies catch up after the open.

#### Start-up and readiness probes

A boot can wait for another instance's boot lock, and a boot killed
part-way rolls back and pauses the database's appends again when it
restarts. A server therefore listens before it opens its event store and
answers two probes, as the Postgres example server does
(`event_sourcing/example_action_permissions/lib/server/boot_health.dart`):

- `/livez` answers 200 as soon as the process listens. Point the
  platform's startup and liveness probes at it, so a long boot is never
  killed.
- `/health` answers 503 with the boot's phase and the time since the open
  began, from the reports of `onBootProgress`, and 200 once the event
  store is open and the server
  serves. Point the readiness probe at it, so no traffic arrives before
  then. Readiness is the return of the bootstrap, not the boot's
  `complete` report, which means only that the store opened.

During `checks`, which also covers the wait for another instance's boot
lock, no further report arrives, so the endpoint reads elapsed time from
its own clock. Report each view's catch-up progress beside it,
so an operator sees which views are not yet current. The observer only
records: it runs synchronously inside the boot (its own work delays the
boot and, on Postgres, every instance's appends), and a call from it into
an event store while the boot runs throws `StateError`.

#### Halting, recovering and rebuilding a destination

The drainer wedges a queue head when the receiver refuses it permanently
or its retry budget runs out -- an attempt bound and a time bound, the
time counted between recorded attempts, each gap capped at the retry
curve's delay plus the delivery cycle's cadence, so time asleep, offline
or declined does not count -- and appends a
`system.destination_wedged` event in the same transaction, recording the
destination, the item, the cause, the attempt count and both bounds (a
fact in the log). A transform that keeps failing is retried within the
same budget and then wedges its destination under the cause
`transform_failed`. A send outcome stating that no delivery was attempted
(the receiver in a cooldown, say) records no attempt and spends no
budget. Fill and drain failures logged at severe level reach standard
error by default. Every event store folds
the library's default destination-wedges view from the wedge events and
the events that end a wedge: its default interpretation of which
destinations are wedged now. A wedged head halts delivery on that
destination until an operator recovers it.

An operator halts a healthy destination with
`DestinationRegistry.requestHalt(id, initiator: ..., purpose: ...)`. The
request is an event naming the initiator; the drainer honours it before its next send by wedging the head
itself (cause operator halt), so a head is never wedged while it is in
delivery. A request on an empty queue stays open until a head exists; any
wedge consumes an open request; `cancelHalt` withdraws one the drainer has
not honoured yet.

`tombstoneAndRefill(id, rowId, initiator: ...)` recovers a wedged head,
recording the initiator on the recovery event: it tombstones the
head, deletes the pending items behind it and rewinds the fill position
below every event they carried, so the drainer's next fill enqueues those
events again under the configuration it registers. It is refused on a
pending head, which may be in delivery.

To rebuild a destination's pending items under a new delivery
configuration (a changed filter or transform):

- The new configuration is still to be deployed: request a halt with
  purpose `reconfigure`, deploy the new revision, then recover. The
  recovery is refused while the drainer still declares the configuration
  recorded when it honoured the halt, so the refill cannot run under the
  old one. Once accepted, it leaves a refill guard: a drainer that declares
  the halted configuration (an instance of the old revision taking the lock
  during the rollout) does not fill the destination until one with another
  configuration has refilled the rewound range, which removes the guard.
  If the rollout is rolled back, so that every instance declares the halted
  configuration again, restart the drainer with a changed
  `configurationVersion`, or delete the destination.
- The new configuration is already deployed: request a halt with purpose
  `pause`, then recover.

The drainer declares, per destination, the configuration the library can
read (identifier, wire format, accumulation window, filter sets, whether
the filter has a predicate) and `SyncCycle.start`'s `configurationVersion`.
Change `configurationVersion` whenever code the library cannot read
changes (transform, predicate or batching code); a deployment can pass its
build or revision identifier. It is recorded in every wedge and recovery
event, so it is an identifier, not free text.

Moving a destination's start date earlier is refused while its head is
wedged: recover first.

Deleting a destination is refused while its queue head is pending, because
it may be in delivery: halt first, and delete once the drainer has wedged
the head. The deletion tombstones the wedged head, deletes the pending
items and keeps every delivered, wedged and recovered item as the delivery
record. The same id registered again starts a new registration.

Delivery is at-least-once. A receiver sees an event again when a recovery
rewinds below events that `sent` items above the rewind point already
delivered, when a destination deleted and registered again refills events
the earlier registration delivered, and when a send's outcome did not
commit (the drainer lost its lock or stopped before recording it).

### Reading order from here

If you're building toward a cross-process deployment, the canonical
references are:

- `spec/reaction-remote.md` — the normative spec for the wire layer.
  Pins the protocol envelope shapes, the connection lifecycle, the
  per-subscription authorization mechanism, and the trust-boundary
  expansion.
- `spec/prd-reaction.md` — the PRD-level requirements for `reaction`'s
  four interfaces, the wire transport, and the Flutter widget layer
  (`reaction_widgets`, also out of scope for this chapter).
- `reaction/lib/src/local/` — the `Local*` impls of the four
  interfaces. Read these to understand how the substrate-agnostic seam
  composes against the in-process substrate.
- `reaction/lib/src/remote/` and `reaction/lib/src/server/` — the
  `Remote*`-plus-server half. Mirror images of the `Local*` impls,
  just with HTTP/WS instead of direct method calls.

Both halves of `reaction` are shipped and tested. The substrate's
two demos (`event_sourcing/example_action_permissions/` and
`event_sourcing/example/`) exercise the in-process `Local*` impls;
the cross-process `Remote*`-plus-server half is exercised by
`reaction/test/` (unit tests under `test/remote/` and `test/server/`,
end-to-end tests under `test/e2e/`).

---

## A note on file layout

Once you've read the above, the canonical place to look for working code
is `event_sourcing/example_action_permissions/`. It's a small full app
— a Dart `shelf` HTTP server plus a Flutter Linux client — that
exercises every concept in this guide. In particular:

- `lib/server/bootstrap.dart` — the canonical wiring shown in section
  "Wiring up an implementation."
- `lib/server/actions/*.dart` — seven action implementations including
  scoped, unscoped, and idempotent variants.
- `tool/permissions.yaml`, `tool/users.yaml` — the seed YAMLs.
- `test/scope_binding_test.dart` — end-to-end exercises of the
  authorization match algorithm against a real `bootstrapDemoServer`.

The reference spec for the substrate's normative behavior lives in
`spec/`. Of particular relevance to this guide:

- `spec/prd-library-charter.md` — the substrate's purpose and
  commitments.
- `spec/prd-action-dispatch.md` — the dispatch pipeline contract.
- `spec/prd-permissions-as-events.md` — the closed-under-events
  authorization stance.
- `spec/scoped-permissions.md` — the design and `EVS-PRD-scoped-permissions`
  requirements for the scope-aware permission model.
- `spec/prd-portability.md` — the `StorageBackend` abstraction.
