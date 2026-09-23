# event_sourcing

A pure-Dart substrate for building **append-only, auditable**
applications. Every state change is recorded as an immutable event in a
single ordered log; the rows you read are computed from that history.
Every action dispatch — successful or denied — produces an event, so
"what happened, when, and why?" is always answerable from the log alone.

The library is intentionally narrow and **domain-neutral**: it ships no
domain types (no `Patient`, no `Invoice`), no transport protocols, and no
opinion about what your application is *about*. Your app brings the
vocabulary; the substrate brings the bookkeeping. It aligns with FDA
21 CFR Part 11 / ALCOA+ at the cryptographic-and-structural layer
(see [Two layers of trust](#two-layers-of-trust)).

> **This README is an orientation.** The hands-on, plain-English
> introduction with full examples is
> [`docs/event-sourcing-guide.md`](../docs/event-sourcing-guide.md) — the
> authoritative narrative for everything below. Normative requirements
> live in [`spec/`](../spec).

## What it provides

- An append-only event log with strong ordering and hash-chain integrity.
- A reactive `subscribe<T>` primitive for reading the log and the views
  derived from it.
- Declarative **projections**: you describe a view's shape; the library
  computes and maintains it (no author-supplied fold code).
- An action dispatcher with a parse → validate → resolve-scopes →
  authorize → execute → persist pipeline.
- A role/permission/scope authorization model where every grant and
  assignment is itself an event in the same log.
- A pluggable `StorageBackend`. Two reference backends ship —
  `SembastBackend` (client-side: iOS/Android/desktop/web) and
  `PostgresBackend` (server-side) — both passing the same backend-agnostic
  conformance harness.

## Packages in this repo

| Package | What it is |
| --- | --- |
| `event_sourcing/` | The substrate (this package): storage, sync, ingest, projections, action dispatch, permissions. Pure Dart. |
| `reaction/` | Substrate-agnostic action submission, view subscription, permission snapshots, and credential lifecycle, with in-process `Local*` and cross-process `Remote*`/server impls. |
| `reaction_widgets/` | Headless Flutter widget layer over `reaction` (`ActionBuilder`, `ViewBuilder`, `ViewListener`, `PermissionGate`). Ships no styled widgets. |
| `reaction_widgets_testing/` | Widget-test doubles (`FakeReaction`, `pumpReactionWidget`). |
| `canonical_json_jcs/` | JCS canonical JSON (RFC 8785). |
| `provenance/` | Append-only provenance-chain value types. |

## The mental model

Five concepts carry most of the weight:

- **Event** — an immutable record of something that happened. Carries a
  sequence number, identity (aggregate id + type, event type, entry
  type), a JSON payload, an initiator, a timestamp, and a hash chaining
  to the previous event.
- **Aggregate** — the unit of consistency; events for one aggregate are
  ordered with respect to each other. The aggregate id is a string you
  choose.
- **Projection** — a declarative recipe (`ProjectionSpec`) turning the
  event log into a queryable table. No fold function — you supply data,
  the library computes the view deterministically.
- **View** — the materialized output of a projection, read live via
  `subscribe<T>` or one-off via `backend.findViewRows(...)`.
- **Action** — the write API. Subclass `Action<TInput, TResult>`, declare
  required permissions, implement `parseInput` / `validate` / `execute`;
  the dispatcher runs the pipeline inside a single storage transaction.

## Two layers of trust

The substrate makes two kinds of claims, and the distinction is
load-bearing:

- **Layer 1 — Facts** (cryptographic / structural, absolute): the event
  at sequence N has hash H; the hash chain from genesis to N is intact
  (tamper-evident); provenance records each hop; the append was atomic
  with its view-row writes; per-aggregate-per-`Source` order is preserved.
  ALCOA+ alignment lives entirely here.
- **Layer 2 — Conventions** (library-chosen defaults): a tombstone event
  type deletes the row; missing keys in a delta preserve prior values and
  explicit null clears; one row per aggregate via deep-merge; whoever
  appends the first event for an aggregate is its canonical authority.
  Useful defaults, not unique truths — apps needing a different
  interpretation subscribe to raw events (`Events()` mode) and compute
  their own state on top of Layer 1.

## Quick start

Register a projection, open the store, append one event, read it back via
the view subscription.

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:sembast/sembast_memory.dart';

const kNote = EntryTypeDefinition(
  id: 'note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Note',
);

Future<void> main() async {
  final db = await newDatabaseFactoryMemory().openDatabase('demo.db');
  final backend = SembastBackend(database: db);

  // Declare views before bootstrap. AggregateProjectionSpec produces one
  // row per aggregate, deep-merged from successive events' payloads.
  final projections = ProjectionRegistry()
    ..register(const AggregateProjectionSpec(
      viewName: 'notes',
      interest: SubscriptionFilter(aggregateTypes: {'note'}),
      tombstoneEventTypes: {'note_tombstoned'},
    ));

  final datastore = await bootstrapAppendOnlyDatastore(
    backend: backend,
    source: const Source(
      hopId: 'mobile-device',
      identifier: 'install-uuid-v4-here', // persist across boots
      softwareVersion: 'app@1.0.0+1',
    ),
    entryTypes: const <EntryTypeDefinition>[kNote],
    destinations: const <Destination>[],
    projections: projections,
  );
  final store = datastore.eventStore;

  // The substrate stamps entry_type_version from the registry.
  await store.append(
    entryType: 'note',
    aggregateType: 'note',
    aggregateId: 'note-1',
    eventType: 'note_created',
    data: <String, Object?>{'title': 'Hello', 'body': 'World'},
    initiator: const UserInitiator('user-42'),
  );

  // Live view: Snapshot × N → EndOfReplay → Delta / Tombstone × ∞.
  final stream = store.subscribe<Map<String, Object?>>(
    const SubscriptionFilter(aggregateTypes: {'note'}),
    AggregateMode<Map<String, Object?>>(viewName: 'notes', mapper: (row) => row),
  );
  await for (final update in stream) {
    switch (update) {
      case Snapshot(:final value):        print('snapshot: $value');
      case EndOfReplay():                 print('backlog done; now live');
      case Delta(:final value):           print('delta: $value');
      case Tombstone(:final aggregateId): print('removed: $aggregateId');
    }
  }
}
```

Reserved system entry types (lib-version boot events, security-context and
retention audits, destination-mutation audits) are auto-registered before
your list; their rows appear automatically when the relevant operations
happen.

## Actions and the dispatch pipeline

Subclass `Action<TInput, TResult>` and the dispatcher runs six
author-visible steps: **parse → validate → resolve-scopes** (pure,
outside any transaction) then **authorize → execute → persist** (inside
one storage transaction). `parseInput` and `validate` throw to reject;
`scopeFor` returns the `ScopeValue` a scoped permission applies to; and
`execute` returns an `ExecutionResult` listing the `EventDraft`s to
append — the dispatcher does the appending, audit trail, and
authorization check. Every failed stage records a typed denial event, so
allow-vs-deny outcomes are reproducible from `(events, lib_version)`.
`Idempotency` (`none` / `optional` / `required`) caches an outcome per
`(actionName, principalId, idempotencyKey)`; a reused key with different
content records an `idempotency_mismatch` denial rather than silently
returning the cached result.

## Projections

Two declarative shapes, interpreted by the substrate:

- **`AggregateProjectionSpec`** — one row per aggregate, deep-merged from
  successive events; `tombstoneEventTypes` delete the row.
- **`TableProjectionSpec`** — a flat table; rows upserted on
  `insertEventTypes`, removed on `removeEventTypes`, with declarative
  `rowKey` (`AggregateIdKey` / `CompositeKey`) and `rowData`
  (`WholePayload` / `SelectedFields` / `PayloadField`).

Rows carry substrate-stamped columns (`aggregateId`, `sequence`,
`latestEventId`, `updatedAt`, `firstEventTimestamp`). Schema evolution
uses `PromoterSpec` chains of shape-changing primitives (`RenameField`,
`DefaultField`, `DropField`) applied at boot-time snapshot promotion and
ingest-time event promotion.

## Subscribing

`eventStore.subscribe<T>(filter, mode)` returns a `Stream<Update<T>>`:

- **`Events()`** — raw `Delta`s as events arrive (no replay).
- **`AggregateMode<T>`** — replays current view state (one `Snapshot` per
  row, then `EndOfReplay`), then delivers live `Delta` / `Tombstone`.

Delivery is at-least-once and preserves log order; subscribers can drop
and re-attach (replay is from current state, not genesis).

## Permissions

A role/permission/scope model where every grant and assignment is an
event in the same log (closed-under-events):

- A **permission** is a named capability (`patient.edit`), optionally
  scoped to a `ScopeClass` (e.g. `site`, `patient` with `containedIn`
  hierarchy). A **role** carries permissions; a **role assignment** binds
  a user to a role at a scope.
- `bootstrapActionPermissions(...)` seeds the role→permission matrix from
  YAML (validated against the actions' declared permissions) and returns
  `PolicyReady(policy)` or a fail-safe `PolicyFailSafe(errors)`.
  `bootstrapRoleAssignments(...)` seeds initial user→role→scope rows.
- `TableBackedAuthorizationPolicy` decides each dispatch by reading the
  `role_permission_grants` and `user_role_scopes` projections. It verifies
  the principal actually holds its claimed `activeRole` (the substrate
  trusts the caller's `userId`, but derives role/scope membership from the
  log), then matches the resolved scope against the user's assignments
  through the same `ContainmentResolver` the read path uses.

The authorization policy is **substrate code, not app-supplied** — that
is what keeps allow/deny outcomes reproducible from the log.

## Storage backends

`StorageBackend` is the trusted persistence seam (correct reads/writes,
transaction atomicity, durability). Two reference impls ship and pass the
same conformance harness:

- **`SembastBackend`** — client-side (file / IndexedDB), for mobile and
  desktop.
- **`PostgresBackend`** — server-side; view rows persist as JSONB blobs in
  a `view_rows(view_name, row_key, row_data, …)` table. The schema is
  provisioned once per deployment with `PostgresBackend.provision` (or
  `open(provisionSchema: true)` in development); `open` performs no DDL
  and refuses a schema its build does not support.

Builds that share one database register their data generation (the
data-format major and each entry type's major) with an
incompatible-generation guard before `EventStore.open` writes anything:
a build of another major is refused while an instance of the other build
is live, and a database records the highest generation that booted on it,
so an older build is refused afterwards. On Postgres the guard holds
advisory locks on a dedicated lock session per backend; on the web it holds
Web Locks; a Sembast database outside the browser is used by one process.
A backend several processes or tabs share is trusted to run this guard;
the two reference backends do. Three inputs of the guard are trusted
without a pluggable interface: the Postgres lock-session path (`lockUrl`,
or the pool's URL), trusted to be one server session reaching the pool's
server, database and schema -- a direct connection or a session-mode
proxy, never a transaction-mode pooler -- with keepalives and a role that
may end its own sessions, which the backend checks where it can when it
opens; the browser's lock manager, trusted to grant, report and release
locks as the Web Locks API specifies; and, outside the browser, a Sembast
database file opened by one isolate of one process.

The trust in the storage seam has a precondition: the library's delivery
guarantees, its views and its security-context records hold only while its
persisted state (destination queues, the views it materializes, the
records it keeps beside them, such as fill positions, schedules, replay
requests, wedge records, halt requests, send fences, the registry check
record, the database identity, the generation records and the view
catch-up marks, and the security context it stores beside each event)
changes only through the library's operations, and reserved system events
are appended only by the library's own operations. The event store's
reserved append operations are `@internal`, and so is every
`StorageBackend` member that writes; a consumer uses the reads, `transaction` (for its own reads;
an event-store append runs only inside `EventStore.runTransaction`) and
`close`, and delivers through `SyncCycle` and `DestinationRegistry`. The
marking is an analyzer guard, not a run-time barrier (see
`EVS-PRD-destinations/K` and `EVS-PRD-destinations/L`). It reaches a
backend in another package only if that package marks its own overrides of
the internal members `@internal` (declared under its `lib/src/`); a backend
declared in the application's own package is covered by the precondition
alone.

Reactive `subscribe<T>` is wired over Sembast change-notifications; on
Postgres, reactive UIs poll `findViewRows` on a cadence until
`LISTEN/NOTIFY` plumbing lands (see `spec/postgres-backend.md`).

## Audit, provenance, and sync (advanced)

Mostly invisible, but load-bearing for compliance and multi-installation
deployment — see the guide's "Advanced" chapter for detail:

- **Per-event metadata** — sequence number, `event_id`, `event_hash` +
  `previous_event_hash` (the chain), `entry_type_version`,
  `lib_format_version`, provenance, and action correlation ids. Optional
  `SecurityDetails` (IP / user-agent / session) persist to a *separate*
  security-context store keyed by `event_id`, keeping request PII out of
  the event record.
- **Library version in the log** — the first open appends
  `lib_version_initialized` with the database identity; every open by
  another package or data-format version appends `lib_version_changed`,
  older ones included; a database last opened by another data-format
  major is refused before any write (`DataFormatIncompatibleError`).
- **Delivery** — each `Destination` delivers its queue in order, and a
  queue head the drain cannot deliver (a permanent refusal, or an
  exhausted attempt budget) wedges, halting that destination until an
  operator recovers it. The drain appends a `system.destination_wedged`
  event in the transaction that marks the head wedged, recording the
  cause, the attempt count, the budget in effect and the outcome category
  and numeric status of the last attempt; the attempts' error text stays
  on the queue item. An operator stops delivery on a healthy destination
  with `DestinationRegistry.requestHalt`: the drainer honours the request
  by wedging the head itself before its next send (cause `operator_halt`),
  and `tombstoneAndRefill` then rebuilds the pending items; any wedge
  consumes an open request, and `cancelHalt` withdraws one. A delivery
  cycle fills and sends only the destinations its registry holds, reports
  the others in `SyncCycle.unserved`, and still honours halt requests on
  them. The delivery configuration the application supplies is trusted
  on faith: the `Destination`'s filter (a predicate closure included) and
  transform, its send outcomes, the `SyncPolicy` given to
  `SyncCycle` (statically or through `policyResolver`; its retry curve
  decides backoff and its attempt budget, at least one, decides when an
  item wedges) and the `clock` given to `SyncCycle` (fill computes its
  window from it; its readings are not recorded). The wedge event makes
  each wedge decision auditable from the log. Every store folds the
  library's default destination-wedges view (`default_destination_wedges`,
  registered by `EventStore.open`): one row per wedged destination, keyed
  by the appending database's identity and the destination, removed by
  the recovery or deletion that ends the wedge. Only the library appends
  reserved system events such as these: `append` and `appendInTxn` refuse
  them, and ingest refuses one in a shape the library does not append
  (`IngestReservedEventRefused`).
- **Cross-installation ingest** — a `Destination` is the outbound
  transport; the inbound ingest path verifies the hash chain against
  what's stored, extends the provenance chain, and admits events into the
  same log (flowing into projections identically to local appends).
  0.x treats one source per aggregate type as canonical; multi-source
  canonicalization is designed but dormant.
- **Verification** — `verifyEventChain` / `verifyIngestChain` recompute
  the hash chains from the stored log alone and return a `ChainVerdict`.

## Cross-process deployments

For browser/desktop clients talking to a server that owns the log, the
sibling **`reaction`** package bridges the wire (HTTP + WebSocket) while
keeping consumer code source-identical to the in-process case. See the
guide's "Cross-process client/server deployments" chapter and
`spec/reaction-remote.md`.

## Examples

- `example_action_permissions/` — a `shelf` HTTP server + Flutter client
  exercising the full action/permission/scope/idempotency surface; the
  canonical wiring reference (`lib/server/bootstrap.dart`).
- `example/` — a dual-pane sync/ingest demo.

## Running tests

```sh
# Pure-Dart conformance (no services):
cd event_sourcing && flutter test
# Browser-only tests (sembast_web on IndexedDB, two tabs on one database):
cd event_sourcing && flutter test --platform chrome test/web/
# Postgres conformance/integration is gated on PG_TEST_URL (see
# .github/workflows/conformance-tests.yml). Each file drops and recreates
# the schema, so run them one file at a time.
```

CI runs the analyzer and the full suite of every package in
`.github/workflows/event-sourcing-tests.yml`, and the Postgres-gated files in
`.github/workflows/conformance-tests.yml`.

End-to-end and multi-client scenario suites and how/when to run them are
documented in [`docs/e2e-testing.md`](../docs/e2e-testing.md).

## Further reading

- [`docs/event-sourcing-guide.md`](../docs/event-sourcing-guide.md) — the
  full hands-on guide (authoritative).
- [`spec/`](../spec) — normative `EVS-*` requirements; start with
  `spec/prd-library-charter.md`.
- `CLAUDE.md` — architectural commitments and the trust-boundary
  enumeration.

## License

AGPLv3.
