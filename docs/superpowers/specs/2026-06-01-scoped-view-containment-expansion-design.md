# Scoped view containment expansion (read-path descendant expansion)

Design for closing the read-path half of hierarchy-scoped permissions: a
principal whose scope assignment sits at an *ancestor* class (e.g. an
Investigator assigned `BoundScope('site', 'site-A')`) should, when
subscribing to a *descendant-class* view (e.g. `participants`), see exactly
the rows that descend from their assignment — and nothing else.

The write path (action authorization) already supports this via the
*upward* `ContainmentResolver`. The read path (subscriptions) does not: the
`appliesViaAncestor` + `BoundScope` branch in
`subscription_handler._expandAssignments` is a documented no-op that
conservatively skips (under-grants, never over-grants). This design fills
that branch with a *downward* expander.

## Motivating scenario

A clinical study has a `region -> site -> participant` hierarchy:

```text
region-West
  site-A
    P-1, P-2
  site-B
    P-3
region-East
  site-C
    P-9
```

- An **Investigator** is assigned to a subset of sites
  (`BoundScope('site', 'site-A')`, `BoundScope('site', 'site-B')`). They
  should see participants P-1, P-2, P-3 in the `participants` view — and
  not P-9.
- An **Overseer** is assigned to whole regions
  (`BoundScope('region', 'region-West')`). They should see P-1, P-2, P-3
  via a **two-hop** expansion (region -> site -> participant).
- An **Admin** holds `TotalWildcardScope` and sees everything.

Today the Investigator and Overseer would see *nothing* in the
`participants` view (the ancestor branch is skipped), even though the
action path correctly lets them act on those same participants.

## Scope (locked decisions)

- **Option A — static expansion.** The allowed-aggregate set is computed
  **once at subscribe time** and frozen for the life of the subscription.
  A participant reassigned to (or newly created at) the principal's site
  mid-subscription is reflected only on the principal's **next** subscribe
  (screen reopen / reconnect). This matches how `AggregateMode.aggregates`
  already works and requires no new substrate delivery primitive.
- **No cap.** The expansion is uncapped; a subscription whose allowed set
  is large delivers all rows. Bounding large working sets is a deployment
  concern, not enforced here.
- **Pagination is out of scope.** Subscriptions are a live feed of a
  bounded working set, not a scrollable query. There is no server-side
  paging on the subscribe path today and none is added. Large-set /
  browse-and-search access is future work (a separate paginated scoped
  read API), explicitly **not** part of this feature.
- **Live membership (A+) and column-predicate subscriptions (B) are
  future work.** Both build *on top of* this feature's expander; see
  "Future work" below. Neither is implemented here.

## Architecture

Three layers, smallest-blast-radius first.

### 1. Substrate: `ScopeDescendantExpander`

A new pure scope-logic primitive, sibling to `ContainmentResolver`, in
`event_sourcing/lib/src/permissions/scope_descendant_expander.dart`.

```dart
class ScopeDescendantExpander {
  ScopeDescendantExpander({required this.registry, required this.findRowsInTxn});

  final ScopeClassRegistry registry;
  final FindRowsInTxn findRowsInTxn; // same typedef ContainmentResolver uses

  /// Returns the set of [targetClass] scope-values that descend from
  /// [assignment]. Fail-closed: missing/malformed index rows contribute
  /// nothing; a non-ancestor assignment returns the empty set.
  Future<Set<String>> expand({
    required Transaction txn,
    required BoundScope assignment,
    required String targetClass,
  });
}
```

It reuses the existing `FindRowsInTxn` typedef and `ScopeClassRegistry`
(`byName` / `ancestorChain` / `isAncestor`) — no new backend method.

**Why the substrate, not the reaction layer:** scope/containment semantics
are substrate concerns per the architectural commitments (permission
policy is substrate code; hierarchy comes from event-derived projections
the substrate reads). Keeping `matchScopeClass`, `ContainmentResolver`,
and `ScopeDescendantExpander` as one cohesive set makes the expander
reusable by an in-process `LocalScope` consumer and by future A+/B work.
The reaction layer stays thin (wiring only).

### 2. Reaction: wire the expander into the skipped branch

In `reaction/lib/src/server/subscription_handler.dart`, the
`appliesViaAncestor` + `BoundScope` case of `_expandAssignments`
(currently `break`) calls the expander inside a short read transaction and
unions the resulting aggregate IDs into the frozen allowed set:

```dart
case ScopeClassMatch.appliesViaAncestor:
  switch (scope) {
    case TotalWildcardScope(): return null;     // unchanged
    case ValueWildcardScope(): return null;     // unchanged (whole descendant class)
    case BoundScope():
      final descendants = await eventStore.backend.transaction(
        (txn) => expander.expand(
          txn: txn,
          assignment: scope,
          targetClass: binding.scopeClass,
        ),
      );
      for (final value in descendants) {
        final aggId = binding.aggregateIdResolver(
          BoundScope(class_: binding.scopeClass, value: value),
        );
        if (aggId != null) result.add(aggId);
      }
  }
```

The expander is constructed once per connection (or per handler) from the
`scopeClassRegistry` already passed to `runSubscriptionHandler` and a
`findRowsInTxn` bound to `eventStore.backend.findViewRowsInTxn`. Steps 1
(view permission), 3 (client allow-list intersection), and 4 (open
`AggregateMode`) are untouched.

**Purely additive.** When `scopeClassRegistry == null` (handlers composed
without a registry), the ancestor branch stays skipped exactly as today;
the expander never engages. Existing deployments are byte-for-byte
unchanged.

### 3. Demo: `example_clinical_scopes` GUI app

A new standalone example package modelling the motivating scenario with a
GUI, structured like `reaction/example` (a `bin/server.dart` shelf server
+ a Flutter client over the cross-process WS read path). See "Demo app"
below.

## Algorithm: downward, breadth-first, multi-hop

`ContainmentResolver` walks child -> parent reading
`where: {keyColumn: value}` and returning `parentColumn`. The expander
inverts both the direction and the column roles, and fans out (one parent
has many children), so it is breadth-first over a frontier set.

```text
expand(assignment = BoundScope('region', 'region-West'), targetClass = 'participant'):

  # ancestorChain('participant') yields: participant -> site -> region
  # the assignment's class ('region') must appear in that chain, else {}.
  # resolve top-down along the hops between 'region' and 'participant':
  #   region -> site,   site -> participant

  frontier = {'region-West'}                       # values at class 'region'
  for childClass in [site, participant]:           # descend one class per step
     ref = registry.byName(childClass).containedIn # child's contained-in ref
     next = {}
     for parentValue in frontier:
        rows = findRowsInTxn(ref.projection, where: { ref.parentColumn : parentValue })
        for row in rows:
           key = row[ref.keyColumn]
           if key is non-empty String: next.add(key)
     frontier = next
  return frontier                                  # set of 'participant' values
```

Properties:

- **Inverse query.** Upward: `where {keyColumn: value}` -> read
  `parentColumn`. Downward: `where {parentColumn: value}` -> read
  `keyColumn`. Same `findViewRowsInTxn`, no `limit` (uncapped).
- **Identity short-circuit.** `assignment.class_ == targetClass` returns
  `{assignment.value}`. (The exact-class case is normally handled by the
  binding's `aggregateIdResolver` before the expander runs; the guard
  keeps the function total.)
- **Non-ancestor guard.** `!isAncestor(assignment.class_, targetClass)`
  returns `{}`. Mirrors `ContainmentResolver`'s `null` fail-closed.
- **Fan-out.** `frontier` is a set; each hop expands breadth-first.
  Multi-hop (region -> site -> participant) falls out naturally.
- **Set-union across assignments.** Several assignments (e.g. an
  Investigator at two sites) each run the expander; the handler's existing
  loop unions results into one `result` set.

### Correctness caveat (made executable)

The read-path expander and the write-path `ContainmentResolver` traverse
the **same index data in opposite directions**. They cannot share code
(opposite directions) but they must agree on the same data: if
`ContainmentResolver` resolves `P-1` upward to `site-A`, the expander must
include `P-1` when expanding `site-A` downward. A dedicated symmetry test
exercises both against one shared fixture so they cannot silently drift.

## Error handling (fail-closed)

- **Missing / empty index rows** -> that branch of the frontier
  contributes nothing. A site with zero participants yields an empty set,
  never an error and never a widening.
- **Malformed row** (missing `keyColumn`, or non-string / empty value) ->
  skip that row; do not crash the subscribe. Same posture as
  `ContainmentResolver`.
- **`scopeClassRegistry == null`** -> ancestor branch stays skipped (no
  expander). Feature is dormant; behaviour identical to today.
- **Transaction failure during expansion** -> propagates as a subscribe
  failure; the subscription is not opened. Fail-closed: no feed rather
  than an unscoped feed. The handler MUST NOT fall through to an
  unfiltered subscribe on expander error.

## Data flow (one subscribe)

```text
SubscribeMsg(viewName = 'participants')
  -> step 1  view-permission gate                                  (unchanged)
  -> step 2  binding = viewScopes.lookup('participants')  # scopeClass 'participant'
             eff = effectivePermissionsFor(principal)     # e.g. [BoundScope('site','site-A')]
             for each assignment:
               match = matchScopeClass(assignment, 'participant', registry)
               appliesExact                  -> aggregateIdResolver        (existing)
               appliesViaAncestor + Bound    -> expander.expand(...) -> union  (NEW)
               appliesViaAncestor + Wildcard -> null (unrestricted)      (existing)
               doesNotApply                  -> skip                     (existing)
  -> step 3  intersect with client-supplied aggregates allow-list  (unchanged)
  -> step 4  AggregateMode(aggregates: result) -> stream           (unchanged)
```

## Demo app: `event_sourcing/example_clinical_scopes`

A new example package demonstrating the feature end-to-end with a GUI.

- **Structure:** modelled on `reaction/example` — a `bin/server.dart`
  shelf server hosting the reaction WS + HTTP handlers, and a Flutter
  client rendering a reactive view over the cross-process read path.
- **Domain & projections:**
  - `participants` view — one row per participant
    (`participant_id`, `site_id`, name).
  - `participant_site_index` — `TableProjectionSpec`, key
    `participant_id`, parent column `site_id`.
  - `site_region_index` — `TableProjectionSpec`, key `site_id`, parent
    column `region_id`.
- **Scope classes:** `ScopeClassSpec('region')`,
  `ScopeClassSpec('site', containedIn: site_region_index)`,
  `ScopeClassSpec('participant', containedIn: participant_site_index)` —
  the full multi-hop chain.
- **Roles (seeded via `role_assigned` events):**
  - `Investigator` -> one or more `BoundScope('site', ...)` (one-hop
    expansion to participants).
  - `Overseer` -> `BoundScope('region', ...)` (two-hop expansion).
  - `Admin` -> `TotalWildcardScope` (unrestricted).
- **ViewScopeRegistry:** bind `participants` -> scopeClass `participant`,
  with the direct `aggregateIdResolver` for the exact case; site- and
  region-scoped principals flow through the new descendant expander.
- **Client GUI:** a user-switcher dropdown over preset users; a reactive
  participant list via `ViewBuilder<Participant>` (headless
  `reaction_widgets`, same pattern as `reaction/example`'s
  `notes_list.dart`). Switching user re-subscribes; the list shows only
  in-scope participants. Read-only on participants.

## Testing (TDD)

- **Substrate unit tests** for `ScopeDescendantExpander` (fake
  `FindRowsInTxn`, no backend): single-hop fan-out; multi-hop
  (region -> site -> participant); empty index -> `{}`; non-ancestor
  target -> `{}`; malformed row skipped; identity short-circuit;
  multi-assignment union (driven at the handler level).
- **Symmetry / anti-drift test:** one shared fixture index; assert
  `ContainmentResolver` (upward) and `ScopeDescendantExpander` (downward)
  agree on the same data.
- **Reaction handler test:** a principal assigned `BoundScope('site',
  'site-A')` subscribing to `participants` receives exactly site-A's
  participants in the snapshot; a live `Delta` for an in-set participant
  is delivered; an out-of-set participant's event yields nothing. The
  Overseer/region case asserts two-hop expansion.
- **Demo app tests:** widget test (switch user -> list narrows); e2e
  (Investigator sees only their sites; Overseer sees the whole region via
  two hops; Admin sees all); server smoke test.
- **Conformance:** no new `StorageBackend` method, so the backend
  conformance harness is unchanged; both `SembastBackend` and
  `PostgresBackend` already satisfy `findViewRowsInTxn(where:)`.

## Performance

Expansion runs **once per subscribe**, against the materialized view
tables (never the event stream), using the same `findViewRowsInTxn(where:)`
primitive the write-path authorize stage already uses on every dispatch.

- **Postgres** compiles the predicate to
  `SELECT ... WHERE view_name = @v AND row_data ->> '<col>' = @val`. The
  JSONB sub-field is **not** indexed by default; an optional expression
  index (`CREATE INDEX ... ON view_rows ((row_data ->> '<col>'))`) makes
  the lookup negligible. Adding such an index for the containment columns
  is an **optional** step in the plan, not required for correctness.
- **Sembast** compiles to a `Filter.equals` finder on the view store.

Performance does not distinguish Option A from the deferred A+/B variants:
all seed their initial set with this same query.

## Requirement traceability

- New: `EVS-DEV-scope-descendant-expander` — downward expansion algorithm
  (assertions for: identity short-circuit, non-ancestor empty, per-hop
  inverse query, fail-closed on missing/malformed row, breadth-first
  multi-hop fan-out).
- Extend the read-path requirement that currently documents the deferral
  (`EVS-PRD-cross-process-event-transport/E` realization in
  `subscription_handler`) to cover ancestor-`BoundScope` expansion.
- `EVS-PRD-scoped-permissions/F` and `/G` (hierarchy expansion;
  fail-closed on missing containment) already state the intent at the PRD
  level; this is the read-direction DEV-level realization.
- `// Implements:` annotations on the new file and the handler branch.

## Future work (explicitly deferred)

- **A+ — live membership.** Watch the containment index for changes
  touching the principal's assigned scope values; on a relevant change,
  re-run the (cheap, optionally indexed) expansion and reopen the inner
  `AggregateMode` with the new set, so the existing snapshot/delta
  machinery diffs membership in. Achieved by transparent internal
  resubscribe; the client wire API is unchanged.
- **B — column-predicate subscription mode.** A new sealed
  `SubscriptionMode` variant that filters a view by `column in {values}`,
  evaluated on snapshot and every live delta, including emitting a removal
  when a row leaves the set. Fully live and unifies sites + participants
  under one rule, but adds a new substrate delivery primitive and a new
  delta semantic. Additive (sealed-type variant beside `AggregateMode`);
  reuses this feature's expander to seed the initial set.
- **Paginated scoped read API.** For large / unbounded working sets and
  browse-and-search screens, a paginated view-query API (the backend
  primitive `findViewRows(limit:, offset:)` exists but is not surfaced on
  `EventStore` or the wire). Separate from the streaming read path.
