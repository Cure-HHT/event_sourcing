# Conditional Mutations: optional primitive for collaborative-edit aggregates

**Phase:** I/II boundary.
**Status:** Brainstorm; will migrate to `spec/conditional-mutations.md` when
stable. This is a design sketch for an **additional** substrate primitive
serving a specific class of use cases — aggregates that multiple installs
concurrently mutate (collaborative documents, shared records, multi-editor
systems). It does NOT replace the substrate's existing event-emission
action model or its idempotency mechanism.
**Last updated:** 2026-05-26.

## Scope

The substrate today serves a single dispatch shape: an `Action`'s
`execute` returns an `ExecutionResult<TResult>` carrying a list of
`EventDraft`s, the dispatcher appends them inside an atomic transaction
with the projection update, and the resulting events seal into the
originating install's hash chain. This shape is correct and sufficient
for the substrate's documented target domains where actions emit new
aggregate facts: medical-diary entries, banking postings, retail
transactions, sensor readings, audit events, vote records, dose
administrations.

It is NOT sufficient for collaborative-edit aggregates. A shared
document edited concurrently by two users on two installs has a
different shape: both users are READING existing aggregate state,
proposing changes against that state, and the substrate has to arbitrate
who wins. `docs/scenarios/collaborative-editing.md` already flagged this
as the place where the current substrate is a partial fit — the CRDT/OT
merge logic lives above the substrate, and the substrate provides only a
canonical total order for clients to reconcile against.

This spec proposes the missing primitive: **conditional mutations**
(compare-and-swap at append time, proposal/confirmation choreography
across installs) as a substrate-side coordination primitive that
collaborative-edit aggregates can opt into. It complements the existing
event-emission dispatch path. Both primitives ship; actions declare
which shape they want; the substrate routes accordingly.

## What this primitive is FOR

Two distinct action shapes the substrate must serve:

**Event-emission actions** (the existing model, unchanged): a user takes
an action that emits a new aggregate fact. There is no prior state to
compare against — the action is creating new state, or appending to a
log-shaped aggregate. The substrate's existing dispatch pipeline plus
`EVS-PRD-action-dispatch/E`'s `idempotency_mismatch` denial event
(content-hash comparison on retry) handles these correctly. Most
actions in the substrate's documented domains are this shape.

**Conditional-mutation actions** (the new model proposed here): a user
reads existing aggregate state, decides on a change against that state,
and dispatches a CAS — "if this aggregate is still in state X, advance
it to Y." Multiple installs may concurrently propose conflicting
mutations against the same aggregate; the substrate routes proposals
through a canonical authority that confirms one and conflicts the rest.
This is the natural shape for shared documents, team-owned records,
collaborative checklists, multi-editor configuration, and any aggregate
where "the next state is a function of the observed prior state" is the
operative concurrency model.

Actions declare which shape they want via their `Action<TInput, TResult>`
subtype or a marker mixin. The dispatcher uses the existing pipeline
for event-emission actions and routes conditional-mutation actions
through an additional precheck stage.

## You might be tempted to

A few attractive-but-wrong moves the brainstorm process surfaced.
Preserved here so future implementers don't re-derive them:

### Tempted to: replace idempotency with CAS as "the only mutation primitive"

**Don't.** The substrate serves two distinct action shapes. Event-emission
actions (medical-diary entries, sensor readings, log appends, deposits,
votes) have no prior aggregate state to CAS against — they're creating
new facts. Forcing them through a CAS API requires inventing fake
`expected_prior` values, which is theatre. Keep both primitives: event-
emission for actions creating new aggregate facts, CAS for actions
mutating existing aggregate state.

### Tempted to: argue that TTL-based idempotency caches make the substrate "Layer 1 unsound"

**Don't.** The substrate's idempotency contract is not "exactly-once
forever regardless of cache survival." It is:

1. **Cache hit, matching content** → cached result returned. Deterministic
   short-circuit, not a correctness mechanism — just avoids redundant
   event emission.
2. **Cache hit, mismatched content** → `idempotency_mismatch` denial
   event appended to the log. This is the actual correctness mechanism;
   it is deterministic given the events and inputs and ships in
   `EVS-PRD-action-dispatch/E`.
3. **Cache miss** → re-execute. The new event appears in the log; replay
   reproduces it.

State derivation from `(events, projection_specs, lib_version)` is intact
in all three cases. The cache is operational infrastructure for
performance, not the substrate's correctness story.

An earlier draft of this brainstorm framed the entire conditional-
mutations design as a fix for "TTL is operationally tuned and therefore
unsound." That framing conflated two distinct concerns and proposed a
universal-replacement primitive against a misdiagnosed problem. The
actual content-hash gap was real but small, and the fix shipped as
`EVS-PRD-action-dispatch/E` content-mismatch detection. The
conditional-mutations design that survived from that brainstorm is what
this spec describes — an additive primitive for collaborative-edit
aggregates that the substrate genuinely needs and the existing dispatch
model doesn't provide.

### Tempted to: mandate `expected_prior` on every event

**Don't.** Many events have no prior state to compare against:
`aggregate_created`, `permission_granted` for a not-yet-existing scope,
`device_announced`, `sensor_reading_recorded`, `dose_administered`,
`vote_cast`, `note_added`. The conditional-mutation discipline applies
to actions that mutate existing aggregate rows. The substrate's
existing event-emission dispatch handles everything else.

### Tempted to: pull Phase II multi-source canonicalization into Phase I scope

**Don't.** Conditional mutations across installs ARE a coordination
pattern, but they fit within the substrate's existing single-canonical-
authority model (originator-of-first-event). App2's user proposes a
mutation; App1 (the canonical authority for the aggregate) confirms or
conflicts; App2's events stay sealed in App2's chain, App1's events
stay sealed in App1's chain. No cross-install chain extension; no
shared signing; no rewrite of canonicalization.

Phase II (multi-source canonicalization with explicit rules) is a
separate concern: multiple canonical authorities per aggregate,
settings-event-driven authority transitions, app-supplied
canonicalization rule grammar. Don't conflate. Conditional mutations
work today within Phase I's single-canonical-authority bounds.

### Tempted to: ship conditional mutations as a substrate-wide opt-in flag

**Don't.** Per-action, not per-substrate. An app that has both event-
emission aggregates (audit log, account postings) and conditional-
mutation aggregates (shared workspace settings, collaborative
documents) needs to declare each action's shape independently. A global
flag forces an awkward choice.

## The conditional-mutation primitive

An action that opts into conditional-mutation shape declares, on input:

- The **target view-row(s)** it intends to mutate (one or more
  `(viewName, rowKey)` references).
- The **expected prior values** of the relevant columns (per-field, with
  library-supplied comparators).
- The **new values** to write.

Action submission shape:

```dart
class ConditionalMutation {
  final String viewName;
  final Object rowKey;
  final Map<String, FieldExpectation> expectedFields;
  final Map<String, Object?> newValues;
}

sealed class FieldExpectation {
  // sealed; library-supplied comparators below.
}
class EqualField extends FieldExpectation { final Object? value; }
class AbsentField extends FieldExpectation { }
class PresentAnyValue extends FieldExpectation { }
class VersionField extends FieldExpectation {
  final int monotonicToken;  // for optimistic-concurrency-token semantics
}

class ActionSubmission {
  // ... existing fields ...
  final List<ConditionalMutation>? conditions;
}
```

`FieldExpectation` is sealed; new comparators ship under the
Append-Only Primitives discipline (semantics frozen once a name is
shipped). Per-field rather than whole-row because two callers
concurrently editing different fields of the same row don't generate
spurious conflicts.

The dispatcher inserts a new stage between authorization and execution:
**conditional-mutation precheck**. The check runs inside the same
storage transaction the executor will eventually append into:

1. Read each target row (locked-for-update via `SELECT ... FOR UPDATE`
   on Postgres, equivalent transaction semantics on Sembast).
2. Compare each `expectedFields` entry against the actual row state.
3. Branch on the result:
   - **All match** → proceed to executor invocation.
   - **All expected values already equal `newValues`** → NoOpAccept; do
     not invoke executor; emit a `mutation_already_applied` event for
     audit purposes; return success indication so the caller's retry
     loop terminates.
   - **Some field is neither `expectedPrior` nor `newValue`** →
     Conflict; do not invoke executor; emit a `mutation_conflict` event
     carrying the proposal details and the actual observed state;
     return `DispatchMutationConflict` so the caller (or its
     conflict-resolution layer) can react.

Three new sealed `DispatchResult` variants ship: `DispatchMutationApplied`,
`DispatchMutationNoOpAccept`, `DispatchMutationConflict`. They sit
alongside the existing variants (`DispatchSuccess`, etc.) since
conditional-mutation actions are a distinct outcome shape from event-
emission actions.

## Canonical ownership and cross-install propagation

This is the load-bearing section. The cross-install flow:

> App1's user creates aggregate X. App1 is the originator of X's first
> event; per the substrate's existing originator-of-first-event
> canonicalization convention, App1 is the canonical authority for X.
> This event syncs to App2. App2's user wants to change X.
>
> What App2 CAN do: dispatch a conditional-mutation action proposing the
> change. The action emits a `mutation_proposed` event in App2's local
> log, sealed by App2's hash chain. App2's UI may render the change
> optimistically with a "pending" indicator.
>
> What App2 CANNOT do: pretend the change was authored by App1. App2
> cannot extend App1's hash chain — App1's events are App1's. A forged
> event with App1 as originator would not verify inside App1 (or inside
> any third install holding App1's events).
>
> When App1's substrate sees App2's proposal during sync:
>
> - App1 runs its own conditional-mutation precheck against X's current
>   state.
> - On match: App1 emits a `mutation_confirmed` event (App1 as
>   originator), advancing X's canonical state to the proposed
>   `newValues`. The chain inside App1 stays consistent.
> - On conflict: App1 emits a `mutation_rejected` event (App1 as
>   originator), surfacing the conflict to whatever multi-editor
>   arbitrator the app provides.
>
> App2 (and any other syncing install) eventually receives App1's
> confirmation or rejection event and updates its view accordingly.
>
> Critical property: App2 NEVER authors events as App1. App2's events
> are always App2's events — they propose changes. App1's events
> confirm or reject them. Hash chains inside each install stay
> internally consistent. Provenance attribution survives end-to-end.

Two event-type families circulate in the multi-install world:

- **Proposals** (originated by any install): "I observed X with these
  expected values; I propose these new values." First-class log events
  in the proposing install, sealed in that install's hash chain. The
  proposing install's view of X may update optimistically; the
  canonical state has not yet changed.
- **Confirmations and Rejections** (originated by the canonical
  authority): "Proposal P has been observed; expected values matched at
  the moment of evaluation, so X advances to the new values" — or
  "Proposal P's expected values did not match; X remains in its
  current state, and here is the conflict for the arbitrator." Events
  in the canonical authority's install, sealed in that install's hash
  chain, and what other installs treat as authoritative for X.

The proposing install's optimistic update is a Layer-2 UI convenience.
Until the canonical authority's confirmation arrives, the view shows
"pending"; once the confirmation propagates, the view reconciles to
canonical state. If a rejection arrives instead, the view surfaces the
conflict to its user or invokes app-supplied resolution policy.

Hash-chain integrity is non-negotiable: every event is sealed by the
install that originated it. Cross-install state changes proceed via the
proposal/confirmation choreography. There is no mechanism by which one
install extends another's chain.

## Optimistic UI

While a proposing install waits for the canonical authority's
confirmation, three rendering modes are possible:

- **Optimistic** — view immediately reflects `newValues` with a
  "pending" indicator; on confirmation, indicator clears; on rejection,
  view reverts and surfaces the conflict.
- **Synchronous** — view stays at `expectedPrior` until confirmation
  arrives, then jumps to `newValues`.
- **Hybrid** — optimistic for low-stakes mutations (UI state, draft
  fields), synchronous for high-stakes mutations (financial entries,
  irreversible actions).

The substrate ships **optimistic with explicit pending status**
surfaced through the subscribe stream as the default. Google-Docs-style
UX is the de facto expectation for collaborative apps, and synchronous
waiting feels sluggish over high-latency links.

Concretely, `subscribe<T>(_, AggregateMode<T>(...))` gains a new variant
alongside `Snapshot`/`EndOfReplay`/`Delta`/`Tombstone`: `PendingDelta<T>`
carrying the optimistic next state plus a reference to the proposal
event id. When the canonical authority's confirmation lands, a normal
`Delta<T>` clears the pending state. When a rejection lands, a
`Reversion<T>` (new variant) restores the prior state and references
the rejection event id.

The substrate ships these variants; the widget layer (`reaction_widgets`,
Plan D) ships `ActionBuilder` / `ViewBuilder` patterns that consume
them. Apps that want synchronous mode override per-action.

## Conflict resolution

The substrate emits `mutation_rejected` events. What an app does with
them is **app-level policy**, not substrate-level. Common strategies:

- **Last-writer-wins** — canonical authority confirms the
  most-recently-arrived proposal, rejects earlier ones with conflict.
- **First-writer-wins** — canonical authority confirms the first
  proposal it sees, rejects later ones with conflict.
- **Prompt-user** — the canonical authority emits an
  `arbitration_required` event; the app's UI surfaces the conflicting
  proposals to a human; the human's resolution becomes a fresh
  conditional mutation that supersedes both.
- **Escalate-to-arbitrator** — a specific principal (the aggregate's
  owner, an admin, etc.) receives a notification; their resolution
  follows the same fresh-conditional-mutation pattern.
- **Domain-specific merge** — apps that know how to merge their own
  conflicts (e.g., text edits via OT, structured documents via CRDT)
  provide a merge function; the substrate supplies the conflicting
  proposals and consumes the merged result as a fresh conditional
  mutation.

The substrate guarantees only that conflicts are surfaced. App policy is
expressed either as a registered conflict resolver (a function the
substrate calls with the conflicting proposals) or as events in the log
themselves (an `arbitration_resolved` event sequenced by the canonical
authority advances the aggregate's state).

App-supplied resolvers DO introduce trust questions: they are app code
that participates in deciding which proposal becomes the next confirmed
state. The resolver's output flows through the substrate as a new
conditional mutation, so the trust boundary is honored — the resolver
is not consulted at fold time; it's a separate event-producing actor
whose events pass through the same dispatch path as any other action.

## Layer 1 vs Layer 2

Using the framing from `CLAUDE.md`'s Epistemic Layers and the guide's
"Two layers of trust" chapter:

- **Conditional mutation atomicity (the CAS).** Layer 1 fact. Either
  the swap happens atomically with the append, or the append doesn't
  happen. Backed by the substrate's existing transaction guarantees
  inside `StorageBackend.transaction`.
- **Hash-chain integrity across installs.** Layer 1 fact. The
  proposal/confirmation choreography preserves it; cross-install state
  changes do not extend another install's chain.
- **Canonical-authority routing (the proposal/confirmation pattern).**
  Layer 2 convention. The substrate ships this as the default
  interpretation of "which install confirms an aggregate's state"; apps
  could build alternative coordination patterns (master-master with
  deterministic conflict resolution, leader-election-driven authority
  rotation) on top of Layer 1 facts, but they'd be building outside the
  substrate's defaults.
- **Conflict-resolution strategy.** Layer 2 convention, app-supplied.
- **Optimistic UI rendering.** Layer 2 convention, app-overrideable.

## Authorization

When a proposing install's user dispatches a conditional-mutation
action, whose `AuthorizationPolicy` decides whether the proposal is
permitted?

**Both apply, at different stages:**

- **Proposing install's policy** runs at proposal-submission time
  against the user's principal. If the user lacks permission to even
  propose mutations to the aggregate, the proposing install denies
  locally and no proposal event is appended. This is the substrate's
  existing dispatch authorize stage, unchanged.
- **Canonical authority's policy** runs at confirmation time when the
  proposal flows in. The canonical authority re-checks the proposing
  user's principal against its own role/permission/scope grants and
  may reject independently. This is necessary because the canonical
  authority may know about scope assignments or role revocations the
  proposing install hasn't yet synced.

The proposing install's policy can be permissive (optimistic);
the canonical authority's policy is authoritative. A proposal that
passes the proposing install but fails the canonical authority's
re-check produces a `mutation_rejected` event with
reason: `authorization_denied`.

This dual-check is the substrate-side mechanism for "the proposing
install hasn't yet observed a permission revocation that the canonical
authority has." Such cases are common in genuine cross-install
deployments where permission events propagate asynchronously.

## Trust boundaries

No new substrate-level trust surface is introduced. The substrate
continues to trust:

- `StorageBackend` for persistence integrity.
- `Destination` for outbound transport.
- `Principal` (specifically `userId`) as supplied on faith from the
  consumer's auth layer.

The conditional-mutation machinery operates entirely on existing trusted
inputs. The CAS check uses view-row state, which the substrate reads
from its trusted `StorageBackend` inside the append transaction. The
proposal/confirmation choreography uses existing event types
(originated, attributed, chained, hash-verified) — proposals and
confirmations are event-type subkinds within the existing hash-chain
regime, not a new class of input.

App-supplied conflict resolvers introduce app-level trust questions
(the app's resolution code participates in advancing canonical state),
but those questions are resolved at the resolver's event-producing
boundary: resolvers do not participate in substrate fold-time decisions;
they produce events that pass through the standard dispatch path.

## Decisions and alternatives rejected

- **Whole-row comparison instead of per-field.** Rejected. Two callers
  concurrently editing different fields of the same row would generate
  spurious conflicts. Per-field with library-supplied comparators
  (sealed `FieldExpectation` subtypes) handles partial-row mutations
  without false positives.
- **App-side conditional-mutation precheck (no substrate involvement).**
  Rejected. App-side check + substrate-side unconditional append loses
  atomicity: another install could land an event between the check and
  the append. The check must run inside the same storage transaction
  as the append.
- **Shared signing keys across installs (so any install can extend any
  aggregate's chain).** Rejected on two grounds: (a) breaks the
  attribution property of provenance — you can no longer tell which
  install originated which mutation; (b) any compromise of any install
  becomes a compromise of all aggregates.
- **Every install treated as canonical for every aggregate it touches
  (no canonical authority).** Rejected because hash-chain integrity
  *per install* requires that each install owns its append-stream.
  Without canonical authority, every install would be free to extend
  every aggregate's state, and the resulting chains would not verify
  inside other installs.
- **Mandatory `expected_prior` on every dispatch (covered above in
  "tempted to" but worth recording as a rejected alternative here).**
  Rejected because creations and log-style emissions have no prior
  state to compare against. The conditional-mutation discipline applies
  to a specific class of actions; the substrate's existing dispatch
  handles the rest.

## Open questions

Resolve before implementation lands:

- **Comparator catalogue.** `EqualField`, `AbsentField`, `PresentAnyValue`,
  `VersionField` are the obvious starting set. Are there others worth
  shipping in v1 (`RangeField` for numeric thresholds,
  `ContainsField` for set membership, `RegexField` for string patterns)?
  Each shipped name freezes its semantics under Append-Only Primitives.
- **No-op-accept event audit retention.** When the proposed `newValues`
  match current state, the substrate emits `mutation_already_applied`
  for audit purposes. In high-retry environments these can accumulate;
  apps may want a `tombstoneEventTypes`-like opt-out. Default is
  "emit, mark as no-op so downstream analytics can filter cheaply,
  retain in the log for audit." Pin the retention policy.
- **Multi-authority aggregates.** Some real-world aggregates (shared
  documents, team-owned records) need to be mutable by multiple
  authorities. Originator-of-first-event suffices for the common case;
  for genuinely shared authority, the substrate needs explicit
  `authority_granted` / `authority_transferred` event types that any
  current authority can emit. The substrate ships originator-of-first-
  event as the default; apps that need shared authority register
  authority-transfer event types the substrate honors. Pin the
  authority-transfer event schema.
- **Schema migration from current substrate.** Existing actions don't
  declare a shape (they're all event-emission today). The migration
  is purely additive: a new `ConditionalMutationAction` subclass (or
  marker mixin); existing `Action` subclasses retain event-emission
  semantics; the dispatcher routes based on the declared shape. No
  breaking change to the existing API surface.
- **View-row read-modify-write atomicity at append time.** The CAS
  requires reading the current view-row state, comparing to
  `expectedFields`, then appending the event and applying the
  projection update — all in one transaction. Postgres makes this
  straightforward (SERIALIZABLE isolation with same-Txn reads).
  Sembast supports it. But: the projection interpreter currently runs
  after the append in some paths, and the conditional-mutation check
  needs the projection's current row before the append. The append
  transaction's ordering needs explicit specification — probably
  "read locked view-row, run CAS, append event, run projection fold,
  write updated row, commit", with all five steps inside one Txn.
- **Cross-install propagation timing budget.** A proposing install
  emits a proposal; the proposal flows through `Destination` machinery
  to the canonical authority; the confirmation flows back. What's the
  worst-case latency budget for a proposal-to-confirmation round-trip,
  and what UX patterns work within it? The substrate ships defaults
  (optimistic UI); apps override; this needs documentation more than
  mechanism design.
- **Failure modes when the canonical authority is offline.** A
  proposing install emits a proposal that the canonical authority
  can't see because its install is down for a week. What does the
  proposing UI show? "Pending" indefinitely is unacceptable. Does the
  substrate surface a `proposal_unconfirmed_for_T` timeout? Does the
  app escalate through an app-supplied policy (e.g., "after 24h
  pending, treat as confirmed pending later reconciliation")? This
  interacts with conflict resolution: a long-pending proposal that
  later conflicts has cascading consequences. Probably the substrate
  exposes timeout events; the resolution policy is app-supplied.
- **Read your own writes.** When a proposing install emits a proposal
  optimistically, its user expects to see the change in their own
  view. But the same user might dispatch a second proposal a moment
  later against the optimistic state. The substrate handles "chained
  proposals against unconfirmed state" by tracking pending proposal
  state per-install and checking against it during conditional-mutation
  precheck, with conflict surfaced if the chain of pending proposals
  later conflicts with the canonical authority's resolution. Subtle;
  needs careful design at implementation time.
- **Conditional mutations against non-aggregate projection shapes.**
  `TableProjectionSpec` rows are insert/remove, not in-place update.
  Do conditional mutations make sense against table-shaped views, or
  are they aggregate-only? Probably aggregate-only in v1, with a
  future generalization to table-shaped rows when a real consumer
  needs it.

## Future work

When this brainstorm stabilizes:

1. Author `spec/conditional-mutations.md` with normative requirements
   covering the additive primitive. Cross-reference `EVS-PRD-action-dispatch`
   for the existing event-emission contract that this complements.
2. Author the implementation plan at
   `docs/superpowers/plans/YYYY-MM-DD-conditional-mutations-impl.md`
   following the structure of the Plan B-remote+C plan: Verified-Symbols
   table at the top, per-task failing-test/impl/verify/commit cadence.
3. Add `EVS-PRD-action-dispatch/<next-letter>` for the conditional-
   mutation precheck stage and the three new `DispatchResult` variants.
4. Add new `Update<T>` variants `PendingDelta<T>` and `Reversion<T>` to
   `EVS-PRD-subscription` (or successor) so the subscribe-stream
   envelope covers the proposal/confirmation/rejection lifecycle.
5. Author `EVS-DEV-conditional-mutation-precheck` for the dispatcher's
   new stage and the per-field comparator catalogue.
6. Sketch the multi-editor authority-transfer event-type schema as a
   future addition (probably its own design spec; comes up when a real
   consumer wants shared-authority aggregates).
7. Update `docs/event-sourcing-guide.md` to introduce the two action
   shapes (event-emission vs conditional-mutation) and route readers
   to the right primitive for their use case. The guide's existing
   "Two layers of trust" chapter is a good place to anchor the
   distinction.
8. Update `docs/scenarios/collaborative-editing.md` to point at the new
   primitive as the partial-fit gap closer (currently flagged as
   "CRDT/OT lives above the substrate"; conditional mutations would
   move the canonical-order primitive into the substrate while CRDT
   merge stays above).

## References

- `EVS-PRD-action-dispatch` — the existing dispatch pipeline this
  complements. Assertion E (idempotency_mismatch denial) handles the
  retry-correctness case for event-emission actions; conditional
  mutations handle the concurrent-mutation case for shared aggregates.
- `EVS-PRD-permission-source`, `EVS-PRD-scoped-permissions` — the
  permission model that the canonical authority's confirmation-stage
  policy check applies.
- `docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md`
  — substrate component model. The action-dispatch path that conditional
  mutations extend.
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md`
  — projection model. Conditional mutations couple append to projection
  state at append time; the precheck depends on the projection
  interpreter's in-Txn semantics.
- `docs/scenarios/collaborative-editing.md` — the consumer scenario
  that motivates this primitive. Currently flags the substrate as a
  partial fit; this design closes that gap for the canonical-ordering
  layer (CRDT merge stays in app code).
- `CLAUDE.md` — repo-level architectural commitments. The "single-
  source-per-aggregate-type today" commitment survives this design
  (canonical authority is single-source per aggregate); the
  "originator-of-first-event canonicalization convention" survives
  (originator is the canonical authority by default; authority transfer
  is a separate optional extension).
- `spec/prd-library-charter.md` — the closed-under-events trust model.
  Conditional mutations preserve it: every outcome (Applied / NoOpAccept
  / Conflict) is recorded as events in the log; state derivation from
  `(events, projection_specs, lib_version)` is intact.
