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
- An action dispatcher with parse → validate → authorize → execute →
  persist pipeline.
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
`backend.findViewRows(...)` for one-off queries inside tests and admin
tools.

### Action

An action is the substrate's write API. You subclass
`Action<TInput, TResult>`, declare which permissions it requires,
implement `parseInput`, `validate`, and `execute`, and the dispatcher
runs everything in the right order inside a single storage transaction.
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
- The provenance entries record each hop the event passed through, with
  attribution to initiators and times.
- Per-aggregate-per-`Source` order is preserved.
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
- **The substrate records its own version in the log.** Every
  installation appends `lib_version_initialized` on first boot and
  `lib_version_changed` on each upgrade. Downgrades are refused unless
  explicitly opted in. "What did the library do at sequence N?" is
  answerable from the log alone.
- **Single-source-per-aggregate-type, today.** The multi-source
  machinery exists in design but is dormant in v1. In practice this
  means: in the v1 model, each kind of aggregate is produced by one
  deployment.

## Dispatching an action — what happens

When the dispatcher receives an `ActionSubmission` (a request to run
some action with some input), it walks through six steps. Parse,
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
4. **Authorize and persist.** Inside a fresh storage transaction, for
   each `Permission` in turn, the dispatcher asks the policy whether
   the principal has the grant (unscoped permissions check
   role-permission grants; scoped permissions pass the resolved scope
   value from step 3). The first Deny stops the pipeline and produces
   an authorization-denial event committed inside the same transaction
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
2. Look up `role_permission_grants` for
   `(principal.activeRole, permission.name)`. If no grant exists for
   the active role: `Deny(notGranted)`.
3. If the permission is unscoped: `Allow`.
4. Otherwise look up `user_role_scopes` for
   `(principal.userId, principal.activeRole)` to get all assignments
   under the active role. If empty: `Deny(notGranted)`.
5. The semantics are "at least one assignment must cover the
   requested scope," and the substrate implements this by iterating
   the assignments and returning `Allow` on the first match:
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

### 1. Open a storage backend

```dart
final db = await databaseFactoryMemory.openDatabase('demo');
final backend = SembastBackend(database: db);
```

Or, for server-side:

```dart
final backend = await PostgresBackend.open(
  url: 'postgres://evs:evs@localhost:5432/evs_demo',
  sslMode: SslMode.disable,
);
```

The backend is trusted for persistence, atomicity, and durability.
Everything else is derived from the events it holds.

> **Note on Postgres + subscriptions.** Both backends pass the same
> conformance harness for storage, transactions, and view materialization,
> but reactive `subscribe<T>` over `PostgresBackend` is not yet wired —
> the existing implementation streams via Sembast change notifications.
> Until polling or `LISTEN/NOTIFY` plumbing lands (tracked separately
> in `spec/postgres-backend.md` Future Work), server-side reactive UIs
> against Postgres need to poll `findViewRows` on a cadence.

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
final datastore = await bootstrapAppendOnlyDatastore(
  backend: backend,
  source: Source(
    hopId: 'portal-server',
    identifier: installId,             // UUIDv4, persisted across boots
    softwareVersion: '0.1.0+1',
  ),
  entryTypes: <EntryTypeDefinition>[
    EntryTypeDefinition(
      id: 'role_permission_grant',
      registeredVersion: 1,
      name: 'Role-permission grant',
      materialize: false,
    ),
    EntryTypeDefinition(
      id: 'user_role_scope',
      registeredVersion: 1,
      name: 'User-role-scope assignment',
      materialize: false,
    ),
    // ... your app's entry types
  ],
  destinations: const <Destination>[],
  projections: projections,
);
final eventStore = datastore.eventStore;
```

Every event type your app appends must have its `entryType` registered
here — missing entries fail at append time, not boot. The
`registeredVersion` is what gets stamped on every event of that type;
bumping it later signals a schema change.

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
  (`List<String>`) the projection cares about. Any field can be
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
to support audit, regulatory compliance, and (eventually) syncing
events between installations.

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
  event's canonical-form content (see `spec/prd-canonical-json.md` for
  the serialization contract).
- **`previous_event_hash`** — the hash of the immediately preceding
  event in the log. This chains the log: any modification, insertion,
  or deletion of a prior event breaks the chain from that point
  forward.
- **`entry_type_version`** — the version of the entry type at the time
  this event was appended. Read from the `EntryTypeRegistry` you passed
  to `bootstrapAppendOnlyDatastore`. Producers don't choose it; the
  substrate stamps it.
- **`lib_format_version`** — the version of `event_sourcing` itself
  at the time of append. Same idea, one level up.
- **`metadata.change_reason`** — a free-form string describing the
  reason for the change. Defaults to `"initial"` if you don't supply
  one.
- **`metadata.provenance`** — a list of `ProvenanceEntry` records,
  each saying "this event passed through hop X at time T running
  software version V." The originator appends the first entry; each
  forwarder appends another (more on this below).
- **`metadata.action_invocation_id`** and **`metadata.action_name`**
  — for events emitted by an action, the dispatcher stamps both so the
  audit log can correlate all the events from one dispatch.
- **`flow_token`** — an optional caller-supplied correlation id you
  can use to thread a chain of dispatches together for tracing.
- **`security`** — optional `SecurityDetails` carrying redaction or
  classification metadata. Used by retention sweeps and security-context
  events.

You generally don't read most of these — they exist for the substrate's
own bookkeeping. But you can: every `StoredEvent` exposes them, and
`backend.findAllEvents(...)` lets you query the log by `entryType`,
`originatorId`, `clientTimestamp` range, and so on.

### The library records its own version in the log

On the first boot against a fresh storage backend, the substrate
appends a `lib_version_initialized` event recording the version of
`event_sourcing` you're running. Each subsequent boot:

- If the version matches what's recorded: silent no-op.
- If the version is newer: append a `lib_version_changed` event.
- If the version is older: refuse to open the store, unless you pass
  `allowDowngrade: true` to `EventStore.open`. (This is a safety
  net: an older library may not understand newer event shapes.)

The same boot path runs a check called "entry-type downgrade refusal":
if any registered entry type's `registeredVersion` is less than the
version recorded for that type in the log, boot fails. The substrate
will not silently re-interpret an event under an older schema.

### Schema evolution: entry types and promoters

When you need to evolve an event shape — rename a field, add a default,
drop a column — you bump the entry type's `registeredVersion` and
register a `PromoterSpec` describing the transformation. The substrate
supplies a small fixed set of promoter primitives (`RenameField`,
`DefaultField`, `DropField`) that are deliberately limited to
shape-changes so the promoter chain is commutative with the
deep-merge fold.

Two paths exercise the promoters:

- **Boot-time snapshot promotion.** When `EventStore.open` sees a
  view row written under an older `entry_type_version` than is now
  registered, it walks the promoter chain over the stored row and
  writes the upgraded shape back. This runs once per upgrade.
- **Ingest-time event promotion.** When an event arrives from an
  older peer (see below), the substrate runs the promoter chain on
  the incoming payload before passing it to the projection fold. The
  log records the event at its original `entry_type_version`; the
  promotion happens in-memory.

Both paths exist because the schema-evolution discipline says "the log
is canonical; you can reconstruct any past state by replaying the
events through the current promoter chain."

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
ground up, though the v1 substrate treats one source per aggregate
type as canonical. The ingest path is the inbound side of that
design:

- A `Destination` is the outbound transport for forwarding events to
  another installation. You register destinations at composition time
  by passing them to `bootstrapAppendOnlyDatastore`; the substrate
  enqueues outbound events through them.
- On the receiving side, the substrate exposes an ingest entry point
  that accepts a `BatchEnvelope` of events from a peer, verifies the
  hash chain against what's stored, extends the provenance chain, and
  admits the events into the local log. Ingested events flow into
  projections and subscriptions identically to locally-produced events.
- The substrate verifies hash-chain integrity on every ingested event.
  A break produces a `ChainVerdict` recording exactly where the chain
  diverged from what was expected; the local install can refuse the
  batch and emit an audit event explaining the refusal.

When the multi-source machinery is activated (Phase II, post-v1), the
substrate will add canonicalization rules: per-aggregate-type rules
saying who is the canonical authority for that aggregate, who can
approve another deployment's edits, and how conflicts resolve.

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

> **Status note.** Both halves of the `reaction` package have now
> shipped: the in-process `Local*` impls (used by the substrate's
> demos) and the cross-process `Remote*`-plus-server half. The
> normative spec lives at `spec/reaction-remote.md`; the
> implementation lives under `reaction/lib/src/remote/` and
> `reaction/lib/src/server/`, exercised by the package's unit and
> end-to-end test suites. Names and shapes match this chapter as
> shipped on `main`.

Everything covered so far assumes one process. The Flutter app you
build runs the substrate directly: it opens an `EventStore`, holds the
`ActionDispatcher`, registers projections, and reads view rows out of
its local sembast database. This is the entirety of a mobile-only
deployment — one install, one log, one principal.

Many real deployments are not shaped that way. A clinical-trial
portal, for example, has Study Coordinators and Supervisors using a
Flutter web app from their browsers, talking to a portal server that
runs the substrate against a Postgres database. The browser is too
constrained to run a full `EventStore` and shouldn't anyway — the
portal database is the authoritative log for that deployment. The
substrate is single-process; the deployment is two-process. Something
has to bridge them.

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
an in-house OAuth flow, a portal-issued linking code — is also the
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
patient P-42?") happens client-side against the locally-known
permission state. There's no risk: the user already has the entire
log in front of them, and the UI is just choosing what to render.

Cross-process, the server *must* enforce. A browser-side filter on a
WebSocket subscription is a UI affordance, not a security boundary.
The portal server must compose the requesting Principal's permitted
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

This is also why CUR-1331's scope-aware permissions had to land
before the wire layer's implementation. The wire's per-subscription
narrowing consults `EffectiveAuthorization.scopeAssignments` and
walks containment projections; without the scope-aware substrate
underneath, the wire would only have role-to-permission grants to
filter by, which can't express "the patients at the sites this user
covers."

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

`reaction`'s v1 baseline is intentionally minimal: on WS drop,
existing subscription streams surface a `'wire_disconnected'` error
and the next `watch<T>` call reopens the connection lazily, replaying
`Snapshot × N → EOR → Delta…` from scratch. The widget sees a brief
flash of skeleton/loading while the snapshot replays. Consumer code
decides whether and when to retry — the substrate stays out of policy
decisions about reconnect timing.

The wire reserves space for two natural v1.1 follow-ups:
exponential-backoff auto-reconnect of existing subs without requiring
a fresh `watch<T>` call, and a "resume-from-sequence" optimization
that re-tails from the client-known sequence instead of replaying the
snapshot. Both are non-breaking and can ship later.

Auth expiry is the parallel concern. The wire surfaces it explicitly:
HTTP 401 from any route and either a `4001 auth_rejected` or `4003
permissions_changed` close-frame on WebSocket flip the `AuthSession`
status to `Expired`. (See *What happens when permissions change
mid-session* below for when 4003 fires.) The UI distinguishes "log in
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
- **`PermissionSource`** — `PermissionSnapshot? get current` plus a
  `Stream`. Local impl reads the `role_permission_grants` projection
  directly; Remote impl fetches the snapshot from
  `/permissions/snapshot` and updates over the WS.

Widgets and other consumers depend only on these. The deployment
choice — composing `LocalScope` (in-process bundle) versus
`RemoteScope` (HTTP+WS bundle) — happens once at app boot. Everything
downstream is shape-identical.

### Server-side adapters

There's no separate "reaction server" process. There's no
`ReactionServer` class either. The lib ships **shelf-compatible
handlers** that you mount inside whatever server you already have.

The expected consumers — `portal_server` and `diary_server` in
`hht_diary` — already run `shelf` + `shelf_router` with their own
middleware pipelines (Firebase auth, OpenTelemetry, CORS, request
logging). The plan is to **replace** most of their existing bespoke
REST handlers with reaction handlers, not add a parallel routing
subtree alongside them. Today each server has 30+ hand-written
handlers, each doing its own business logic against Postgres
directly. Post-migration, those collapse to about five generic
reaction handlers, with the per-feature business logic moving into
substrate `Action`s — same dispatch pipeline, same audit trail, same
permission story as a single-process app.

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
  policy: policy, viewScopeRegistry: portalViewScopes,
);

// HTTP routes go behind the portal's existing auth middleware (it
// reads the Bearer header and attaches a Principal to the request
// context). The WS route is registered on the outer router with NO
// middleware — the WS upgrade path cannot carry an Authorization
// header from Flutter web; credentials arrive in the first WS
// message and are validated by the supplied PrincipalAuthValidator.
final httpRouter = Router()
  // The portal's existing custom routes:
  ..get('/api/v1/sponsor/config', sponsorConfigHandler)
  ..post('/api/v1/auth/login', loginHandler)         // pre-auth
  // The reaction HTTP handlers, gated by the portal's auth middleware:
  ..get('/api/v1/portal/me',                   reaction.me)
  ..post('/api/v1/portal/actions',             reaction.actions)
  ..get('/api/v1/portal/permissions/snapshot', reaction.permissions);

final httpPipeline = const Pipeline()
    .addMiddleware(portalAuthMiddleware)
    .addHandler(httpRouter.call);

final topRouter = Router()
  ..get('/api/v1/portal/subscriptions',
        reaction.subscriptions(validator))
  ..mount('/', httpPipeline);
```

Authentication composes with whatever the consumer is already doing.
The portal_server already runs Firebase ID-token middleware on its
authenticated routes; reaction's handlers just read `Principal` from
the request context via `principalFromContext(req)`. The portal's
existing Firebase middleware populates that context, and reaction's
handlers consume it — one auth flow, two route consumers. For
deployments that don't have their own middleware yet (early demos,
local dev), `authMiddleware(TrustingAuthValidator(...))` is a
one-liner; reaction will use it identically.

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
  revocation committing.

- **`role_assigned` for this user, or `permission_granted` to a
  role this user holds:** the server sends a `stale_data` envelope
  on the open WS (a one-bit signal: "your cached scope state may be
  out of date"). The subscription stays open; the client decides
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

The earlier draft of this design had the WS handler open a per-
Principal subscription on `auth_ok` to keep a fresh in-memory mirror
of the Principal's `EffectiveAuthorization` — effectively a server-
side permission cache. That was rejected because the cache adds
real complexity (per-connection state, mirror-vs-projection races)
for negligible benefit at portal scale (substrate policy queries
are sub-millisecond and subscribe messages are infrequent). The
mid-session-change handling described here is a different mechanism
with a different goal: it doesn't cache permission state, it
reacts to permission events.

### Reading order from here

If you're building toward a cross-process deployment, the canonical
references are:

- `spec/reaction-remote.md` — the normative spec for the wire layer.
  Pins the protocol envelope shapes, the connection lifecycle, the
  per-subscription authorization mechanism, and the trust-boundary
  expansion.
- `spec/prd-reaction.md` — the PRD-level requirements for `reaction`'s
  four interfaces, the wire transport, and the future Flutter widget
  layer (`reaction_widgets`, also out of scope for this chapter).
- `reaction/lib/src/local/` — the `Local*` impls of the four
  interfaces. Read these to understand how the substrate-agnostic seam
  composes against the in-process substrate.
- `reaction/lib/src/remote/` and `reaction/lib/src/server/` — the
  `Remote*`-plus-server half. Mirror images of the `Local*` impls,
  just with HTTP/WS instead of direct method calls.
- `docs/superpowers/plans/2026-05-13-reaction-remote-impl.md` — the
  historical task-by-task implementation plan; preserved as a record
  of how the work was structured and what was built in what order.

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
