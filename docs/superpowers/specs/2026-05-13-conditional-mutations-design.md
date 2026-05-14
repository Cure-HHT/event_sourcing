# Conditional Mutations Design

**Phase**: I/II boundary
**Status**: Brainstorm; will migrate to `spec/conditional-mutations.md` when
stable. Born from CUR-1330 review: the shipped `IdempotencyStore` design has
correctness windows that fundamentally can't be a Layer 1 substrate
guarantee. This spec proposes the substrate's replacement: conditional
mutations (`expected_prior` + `new_value`, atomic CAS at append time) as the
substrate's only mutation primitive.
**Last updated**: 2026-05-13

## Why now

The substrate's current mutation model is "caller-supplied idempotency key
plus pluggable cache". An action carries a key, the dispatcher looks the
key up in `IdempotencyStore` keyed by `(actionName, principalId, key)`, a
cache hit short-circuits the dispatch and returns the cached outcome, and a
cache miss runs the pipeline and records the result with a TTL. CUR-1330
just shipped a production-targeted `PostgresIdempotencyStore` implementing
this contract, alongside the existing sembast and in-memory variants and a
shared conformance harness. The mechanism works as specified.

The problem isn't the implementation — it's the abstraction. Correctness of
"action X executes at most once across retries" under this design depends
on three things that are not Layer 1 facts:

1. The TTL is long enough that every legitimate retry arrives before
   expiry. Tuning depends on the deployment's network patterns, retry
   backoffs, mobile-radio-sleep behavior, browser-tab-suspension behavior,
   and any operator's idea of how long is "long enough".
2. The cache survives whatever interruption sits between original dispatch
   and retry. Reboots, process kills, container restarts, sembast file
   corruption, Postgres maintenance windows, in-memory variants by
   construction.
3. Retries actually arrive within whatever combined window the above two
   establish. Network partitions longer than that window become correctness
   bugs, not just availability bugs.

A substrate that claims **closed-under-events trust** (state at sequence N
is reproducible from `(events, projection_specs, promoter_specs,
lib_version)`) cannot ship a correctness mechanism whose semantics depend
on cache survival. The cache is not in the log. Replaying the log under a
different deployment with a different TTL or a different cache backend
yields a different answer to "did this action execute once or twice?".
That is a Layer 1 violation dressed as an operational concern.

The fix is structural: the substrate's mutation primitives must be
intrinsically idempotent under retry by construction, not by cache
intervention. Conditional mutations (read-state, compare, swap) are that
construction. Every append carries the caller's view of the prior state;
the substrate's append path performs an atomic CAS against the actual prior
state; the three possible outcomes are themselves part of the log.

## The correctness windows we're closing

Catalogue, so the discussion is concrete:

- **Retry after TTL expiry.** Caller's first attempt times out at the
  transport layer; caller retries six hours later; substrate's cache has
  evicted the entry; substrate re-executes; action runs twice. Twice-credit,
  twice-charge, twice-publish, depending on the action.
- **Reboot between attempt and retry.** In-memory store loses everything.
  Sembast and Postgres stores survive reboot, but the dispatcher's
  *response* path doesn't — the original caller never saw the outcome of
  the first attempt because the process died mid-response; they retry; if
  the original write committed before the crash, the cache lookup finds it
  and returns the cached outcome (good); if the original write was
  in-flight when the crash happened, the cache wasn't populated and we
  re-execute (potentially bad).
- **Cache eviction under storage pressure.** Postgres maintenance vacuums
  expired entries; an aggressive operator's vacuum policy purges entries
  ahead of the TTL the action's author intended. Two installs with the
  same code but different operator configurations have different
  correctness boundaries.
- **TTL skew across installs.** App1 thinks its TTL is 24h; App2 was
  configured for 1h; both append the same kind of event. Whether a duplicate
  is suppressed or not depends on which install observed it first and how
  long ago. The behavior is install-local but the data is shared.
- **Caller's lack of visibility into the deployment's TTL.** A library user
  cannot reason about safety without consulting the deployment's
  configuration. The substrate's contract leaks operational concerns into
  application logic.
- **Lost-update races behind the idempotency mechanism.** Idempotency caches
  protect against *the same caller's retry*, but not against *two
  concurrent callers proposing the same mutation*. Two operators both
  click "approve" on the same record at the same time; neither has a
  cached idempotency key matching the other's; both dispatches execute;
  two `approval_granted` events get appended; the projection's last-wins
  fold quietly hides the conflict.

These aren't edge cases; they're the routine failure modes of any
TTL-based cache deployed in the field. The substrate inherits all of them.

## The conditional-mutation primitive

The proposal: every mutation action declares, on input, the **field(s) it
intends to mutate**, the **prior value(s) it expects to see** in the
view-row state at the moment of execution, and the **new value(s) it
wants to write**. The substrate's append path is no longer "unconditional
write"; it's "atomic compare-and-swap against current view-row state",
running inside the same transaction as the append.

Three outcomes are possible per mutation:

- **Applied** — the prior view-row state matched `expected_prior`. The
  substrate appends the mutation event (a normal log event), the projection
  fold runs in the same transaction, the view-row becomes `new_value`.
  This is the happy path.
- **NoOpAccept** — the prior view-row state was already equal to
  `new_value`. The intended end state is already present. The substrate
  *accepts* the action — it does not raise a conflict, because the
  caller's intent has already been realized — but its handling of the
  append is an open question (see "No-op-accept events: emit or
  suppress?" below). The dispatcher returns a success indication so the
  caller's retry-loop terminates.
- **Conflict** — the prior view-row state is neither `expected_prior` nor
  `new_value`. Some other actor mutated the field between the caller's
  read and the caller's dispatch. The substrate emits a `conflict`
  outcome event analogous to `authorization_denied` today, carrying
  enough information for the caller (or its conflict-resolution layer) to
  re-read and retry.

The action submission shape grows a new field — concretely, something like:

```dart
class ActionSubmission {
  // ... existing fields ...
  final List<ConditionalMutation>? conditions;
}

class ConditionalMutation {
  final String viewName;
  final Object rowKey;          // typed by the row-key extractor's contract
  final Map<String, Object?> expectedPrior;  // or whole-row, or per-field — open question
  final Map<String, Object?> newValue;
}
```

The dispatcher inserts a new stage between authorization (stage 6) and
execution (stage 7): **conditional-mutation precheck**. The check reads the
target row(s) inside the same transaction the executor will eventually
append into, compares each `expectedPrior`, and either short-circuits to
NoOpAccept, returns Conflict, or proceeds to executor invocation with the
row read locked-for-update so the eventual append's CAS is guaranteed to
see the same state.

Open question on shape: per-field comparison versus whole-row comparison.
Per-field is more granular — two callers concurrently editing different
fields of the same row don't generate spurious conflicts — but the
action-authoring API has to express the field set, and the projection
interpreter has to support partial-row comparison primitives. Whole-row
is structurally simpler — the comparison is "is this row exactly this
Map?" — but generates more conflicts when independent fields are
co-mutated. Likely answer: per-field with library-supplied comparators
(`EqualField`, `AbsentField`, `PresentAnyValue`, `RangeField` for numeric
optimistic-concurrency tokens), but settle this before code lands.

Open question on coupling: today `EventStore.append` is fold-independent
in some paths — events get appended and projections fold afterward inside
the same Txn. Conditional mutations couple append to view-row state at
append time, which means the projection's row must be materialized
*before* the CAS runs, which in turn means the projection interpreter
must execute synchronously inside the append transaction for any
ProjectionSpec the conditional-mutation targets. This is mostly already
the case for `AggregateProjectionSpec`'s in-Txn fold, but the
substrate's interaction model needs explicit specification.

## What this replaces

- The `Idempotency` enum (`none` / `optional` / `required`) leaves the
  action contract. Every action is intrinsically idempotent under retry
  because its semantics are stated as a CAS, not a one-shot side effect.
- The `IdempotencyStore` interface — and its sembast, Postgres, and
  in-memory implementations, plus the conformance harness, plus the
  integration tests — either retreats to app-layer utility (a non-
  load-bearing helper for non-event-sourced API surfaces that still want
  HTTP-style idempotency, e.g., outbound calls to third-party payment
  providers) or is removed from the substrate entirely. The shipped code
  doesn't disappear; it loses its "substrate correctness mechanism"
  framing.
- The `EVS-PRD-action-dispatch/D` requirement (which today reads
  "idempotency: same identifier + matching content → same outcome, no
  new event") gets reframed: idempotency becomes a property of the CAS
  primitive applied at append time, not a separate pluggable cache.
- The dispatcher's "Stage 4 — idempotency cache lookup" disappears.
  Stages renumber. The new condition-precheck stage slots in between
  authorization and execution.
- `IdempotencyKey` as a submission field is no longer the substrate's
  uniqueness key. (It may still exist as an opaque audit-trail
  correlation token — separate concern.)

## What about non-idempotent primitives?

Things like a hypothetical `IncrementValue` (counter++) cannot be expressed
as a single conditional mutation against a single field, because the new
value depends on the current value, which the caller doesn't know
synchronously. These operations simply aren't substrate primitives.

Apps that need counter semantics build them as compound conditional
mutations:

```text
loop {
  current := readCurrentValue()
  result  := dispatch(IncrementByOne(
              expectedPrior: current,
              newValue:      current + 1))
  if Applied      -> break  // we got there
  if NoOpAccept   -> break  // someone else got there with the same +1
  if Conflict     -> continue  // retry against fresh state
}
```

This is the standard CRDT / OT counter pattern. The substrate doesn't ship
the loop; apps do. The loop is correct under arbitrary retry, reboot, and
network-partition timing because every iteration is itself an atomic CAS;
the failure modes that bedevilled the TTL-cache design are absent.

The same pattern generalizes: any "non-idempotent" operation in the
RESTful-API sense is, in the event-sourcing sense, a compound read-modify-
write that the app composes on top of intrinsically idempotent CAS
primitives. The substrate stays small; the app expresses the loop where
its concurrency model dictates.

## Canonical ownership across installs

This is the load-bearing section. The conditional-mutation model is not
neutral about multi-install topology; it commits the substrate to a
particular interpretation of canonical authority that has been latent in
the codebase under the "originator-of-first-event canonicalization"
convention. Spelling it out:

Concrete walkthrough — the property the substrate must preserve:

> App1's user creates aggregate X. App1 is the originator of X's first
> event; per the substrate's existing originator-of-first-event
> canonicalization convention, App1 is the canonical authority for X.
> This event syncs to App2. App2's user wants to change X.
>
> What App2 CAN do: emit a conditional-mutation event proposing the
> change. This event is App2's (App2 is the originator hop in
> provenance). It carries `expected_prior` + `new_value`.
>
> What App2 CANNOT do: pretend the change was authored by App1. App2
> cannot extend App1's hash chain — App1's events are App1's. If App2
> forged an event with App1 as originator, the hash chain would not
> verify inside App1 (or inside any third install that has App1's
> events).
>
> When App1's substrate sees App2's proposal during sync:
>
> - App1 checks: was the proposed `expected_prior` consistent with X's
>   state at the time of the proposal?
> - If yes: App1 emits a confirmation event (App1 as originator),
>   advancing X's canonical state to `new_value`. The chain inside App1
>   stays consistent.
> - If no: App1 emits a conflict event (App1 as originator), surfacing
>   the conflict to whatever multi-editor arbitrator the app provides.
>
> App2 (and any other syncing install) eventually receives App1's
> confirmation or conflict event, and updates its view accordingly.
>
> Critical property: App2 NEVER authors events as App1. App2's chain
> inside App1 stays consistent because App2's events are always App2's
> events — they propose changes; App1's events confirm or reject them.

The structure: there are two event-type families circulating in the
multi-install world.

- **Proposals** (originated by App2): "I observed X = `expected_prior`; I
  want X = `new_value`". These are first-class log events in App2's
  install, sealed in App2's hash chain. App2's view of X may update
  optimistically when the proposal is appended, but the canonical state
  has not yet changed.
- **Confirmations or Conflicts** (originated by App1, the canonical
  authority): "Proposal P from App2 has been observed; the proposal's
  `expected_prior` matched X's actual state at the time, so X is now
  `new_value`" — or "Proposal P from App2 has been observed; the proposal
  did not match X's state, so X remains `<actual>`, and here is the
  conflict for the arbitrator to handle". These are events in App1's
  install, sealed in App1's hash chain, and they're what other installs
  treat as authoritative for X.

App2's optimistic update on its own view is a Layer 2 UI convenience, not
a Layer 1 fact about X. Until App1's confirmation arrives, App2 displays
"pending"; once App1's confirmation propagates, App2 reconciles to the
canonical state. If a conflict comes back instead, App2 surfaces the
conflict to its user (or invokes app-supplied resolution policy).

The hash-chain-integrity property is non-negotiable: every event is
sealed by the install that originated it. Cross-install state changes
proceed via the proposal/confirmation choreography. There is no
mechanism by which one install extends another's chain.

**Implication: this commits the substrate to multi-editor as a first-class
concern, not a Phase II afterthought.** CLAUDE.md's current commitment
reads:

> Single-source-per-aggregate-type today. Multi-source machinery exists
> in design but is dormant in v1; Phase II activates it.

That commitment cannot survive the adoption of conditional mutations.
Conditional mutations across installs IS a form of multi-source / multi-
editor coordination. The proposal/confirmation pattern is the multi-
editor coordination machinery. Adopting this spec pulls Phase II's
multi-source design forward into Phase I scope — not in the sense of
shipping a full Phase II rule grammar tomorrow, but in the sense of
acknowledging that the substrate's mutation primitive presupposes the
multi-editor coordination model. The architectural commitments in
CLAUDE.md need revision before this spec stabilizes.

Open question: what about aggregates that are *intended* to be mutable by
multiple authorities (shared documents, collaborative records)? Is
originator-of-first-event still the right default, or do we need a notion
of "authority transfer" events (`authority_granted(aggregate, principal)`,
`authority_transferred(aggregate, from, to)`) that any current authority
can emit and that subsequent installs honor? Probably the latter. The
design is sketched but not pinned.

Open question: what does the canonical authority's `AuthorizationPolicy`
evaluate against? If a user on App2 dispatches a proposal that flows to
App1, whose `AuthorizationPolicy` decides whether the proposal is
permitted — App1's (because App1 is doing the confirmation) or App2's
(because App2 is where the user was authenticated)? Probably **App1's
policy applied to App2's user-identity** — App1 is the canonical
authority and gets to decide who can mutate what, and App2 supplies the
principal identity. But the model needs explicit framing, and the
substrate's existing single-policy-instance shape may need to grow a
notion of "policy-at-the-canonical-authority".

## Optimistic UI

While App2 waits for App1's confirmation of a proposal, what does App2's
UI show? Three modes are possible:

- **Optimistic** — App2's view immediately reflects `new_value` with a
  "pending" indicator; on confirmation, the indicator clears; on
  conflict, the view reverts and surfaces the conflict.
- **Synchronous** — App2's view stays at `expected_prior` until App1's
  confirmation arrives, then jumps to `new_value`.
- **Hybrid** — optimistic for "low-stakes" mutations (UI state, draft
  fields), synchronous for "high-stakes" mutations (financial entries,
  irreversible actions).

The substrate ships defaults; apps override per-mutation. Default for the
brainstorm: **optimistic with explicit `pending` status surfaced through
the subscribe stream**, because Google Docs-style UX is the de facto
expectation for collaborative apps, and synchronous waiting is what
makes legacy web apps feel sluggish over high-latency links.

Concretely: `subscribe<T>(_, AggregateMode<T>(...))` may want to emit a
new variant alongside `Snapshot`/`Delta`/`Tombstone` — call it
`PendingDelta<T>` — carrying the optimistic next-state plus a reference
to the proposal that produced it. When the canonical authority's
confirmation lands, a normal `Delta<T>` clears the pending state. When a
conflict lands, a `Reversion<T>` (new variant) restores the prior state.

This is an open design question; the subscribe envelope was designed
for a single-authority world and probably needs extension.

## Conflict resolution

The substrate emits a `Conflict` outcome from the canonical-authority
install. What to do with it is **app-level**, not substrate-level:

- **Last-writer-wins** — App1 confirms the most-recently-arrived proposal,
  rejects earlier ones with conflict.
- **First-writer-wins** — App1 confirms the first proposal it sees,
  rejects later ones with conflict.
- **Prompt-user** — App1 emits an arbitration-required event; the app's
  UI surfaces the conflicting proposals to a human; the human's
  resolution becomes a new conditional mutation that supersedes both.
- **Escalate-to-arbitrator** — A specific principal (the aggregate's
  owner, an admin, etc.) receives a notification; their resolution
  follows.
- **Domain-specific merge** — Apps that know how to merge their own
  conflicts (e.g., text edits via OT) provide a merge function; the
  substrate supplies the conflicting proposals and consumes the
  merged result as a fresh conditional mutation.

The substrate guarantees only that conflicts are *surfaced*; what to do
with them is app-supplied policy, expressed as registered conflict
resolvers or as events in the log themselves (an
`arbitration_resolved(proposal, decision)` event type, sequenced by
the canonical authority, advances the aggregate's state).

## Trust boundaries

No new trust surface is introduced. The substrate continues to trust:

- `StorageBackend` for persistence integrity.
- `Destination` for outbound transport.
- `Principal` as supplied on faith.

The conditional-mutation machinery operates entirely on existing trusted
inputs. The CAS check uses view-row state, which the substrate already
reads from its trusted `StorageBackend` inside the append transaction.
The proposal/confirmation choreography uses existing event types
(originated, signed, chained) — proposals and confirmations are just
event-type subkinds within the existing hash-chain regime, not a new
class of input.

App-supplied conflict resolvers (above) DO introduce trust questions:
they are app code that participates in deciding which proposal becomes
the next confirmed state. But the resolver's output flows through the
substrate as a new conditional mutation, so the trust boundary is
honored — the resolver is not consulted at fold time; it's a separate
event-producing actor whose events pass through the same dispatch path
as any other action.

## Layer 1 vs Layer 2

Using the framing from CLAUDE.md's Epistemic Layers:

- **Conditional mutation atomicity (the CAS).** Layer 1 fact. Either
  the swap happens atomically with the append, or the append doesn't
  happen. Backed by the substrate's existing transaction guarantees
  inside `StorageBackend.transaction`.
- **Hash-chain integrity across installs.** Layer 1 fact. The proposal/
  confirmation choreography preserves it; cross-install state changes
  do not extend another install's chain.
- **Canonical-authority routing (the proposal/confirmation pattern).**
  Layer 2 convention. The substrate ships this as the default
  interpretation of "which install confirms an aggregate's state"; apps
  could build alternative coordination patterns (master-master with
  deterministic conflict resolution, leader-election-driven authority
  rotation) on top of Layer 1 facts, but they'd be building outside the
  substrate's defaults.
- **Conflict-resolution strategy.** Layer 2 convention, app-supplied.
- **Optimistic UI rendering.** Layer 2 convention, app-supplied or
  app-overrideable. The substrate's default (optimistic with `pending`
  signal) is a Layer 2 default like merge-semantics and tombstone-as-
  delete.

## Decisions rejected

- **`IdempotencyStore` as substrate correctness guarantee.** Rejected on
  Layer 1 grounds. TTL plus cache survival can't be a hard substrate
  promise; deployments with different TTL configurations cannot reliably
  reproduce each other's behavior; the cache is not in the log so
  closed-under-events doesn't hold for action outcomes that depend on
  it. The shipped infrastructure is fine as an app-layer utility; it is
  not fine as the substrate's correctness story for mutation.
- **"All aggregates are global, no canonical authority".** Rejected
  because hash-chain integrity *per install* requires that each install
  owns its append-stream. Without canonical authority, every install
  would be free to extend every aggregate's state, and the resulting
  chains would not verify inside other installs.
- **"Every install can extend every aggregate's chain via shared
  signing keys".** Rejected because (a) shared signing breaks the
  attribution property of provenance — you can no longer tell which
  install originated which mutation — and (b) any compromise of any
  install becomes a compromise of all aggregates.
- **"Keep `IdempotencyStore` as the primary mechanism, add conditional
  mutations as an option".** Rejected because dual-paths fragment the
  substrate's correctness story. If conditional mutations exist as an
  opt-in, then the substrate has two correctness models — one with
  Layer 1 guarantees and one without — and consumers have to know which
  one they're in to reason about safety. Conditional mutations either
  replace idempotency caching or they're not load-bearing.
- **"Mandatory `expected_prior` on every event ever".** Rejected for
  events that don't logically have a prior state — `aggregate_created`,
  permission grants for not-yet-existing scopes, `device_announced`.
  The conditional-mutation discipline applies to *mutations* (state
  transitions on existing rows); creations are a distinct class.

## Open questions

The brainstorm hasn't pinned these. They need resolution before code
lands.

- **Action-authoring API for `expected_prior`.** Per-field with type-safe
  accessors? Whole-row blob comparison? Optional declaration (some
  fields are unconditional)? Library-supplied comparator primitives
  (`EqualField`, `AbsentField`, `PresentAnyValue`, `Version` for
  monotonic-token semantics)? Likely answer: per-field with comparator
  primitives, matching the Append-Only Primitives discipline. Pin
  before code lands.
- **Authorization check at the canonical authority.** When App2's user
  dispatches a proposal that flows to App1, whose `AuthorizationPolicy`
  applies to whom? Probably App1's policy applied to App2's user-
  identity, but the model needs explicit framing and the substrate's
  existing single-policy shape probably needs to acknowledge "policy-
  at-the-canonical-authority" as the operative checkpoint.
- **No-op-accept events: emit or suppress?** When the proposed
  `new_value` matches the current state, the substrate accepts the
  action — but does it emit a "confirmed no-op" event? Audit-trail-
  friendly answer is **emit** (so the log records that the dispatch
  happened, even though state didn't change); storage-cost-friendly
  answer is **suppress** (so logs don't fill with no-ops from
  retry-flooded environments). Probably emit-and-mark, so downstream
  analytics can filter them out cheaply but auditors can still see
  retry history. Pin.
- **Multi-authority aggregates.** Some real-world aggregates (shared
  documents, team-owned records) need to be mutable by multiple
  authorities. Does originator-of-first-event still suffice, or do we
  need explicit `authority_granted` / `authority_transferred` events?
  Probably the latter; the substrate ships a default
  single-canonical-authority interpretation and apps that need shared
  authority register authority-transfer event types that the substrate
  honors.
- **Schema migration from the current substrate.** The shipped substrate
  has actions that don't carry `expected_prior`. How do they coexist
  with new actions during a phased rollout? Probably the dispatcher
  tags each action as "conditional" or "legacy-unconditional"; legacy
  actions ship as a deprecated path scheduled for removal in a future
  library version. The `lib_version` event recording makes the
  transition auditable.
- **View-row read-modify-write atomicity at append time.** The CAS
  requires reading the current view-row state, comparing to
  `expected_prior`, then appending the event and applying the
  projection update — all in one transaction. Postgres makes this
  straightforward (SERIALIZABLE isolation with same-Txn reads); sembast
  also supports it. But: the projection interpreter currently runs
  *after* the append in some paths, and the conditional-mutation check
  needs the projection's current row *before* the append. The append
  transaction's ordering needs explicit specification — probably
  "interpret projection's read primitive, run CAS, append event, run
  projection's fold, write row, commit", with all five steps inside
  one Txn.
- **Cross-install propagation timing.** App2 emits a proposal; the
  proposal flows through `Destination` machinery to App1; App1's
  confirmation flows back through `Destination` to App2 and to every
  other syncing install. What's the worst-case latency budget for a
  proposal-to-confirmation round-trip, and what UX patterns work
  within it? The substrate ships defaults (optimistic UI); apps
  override; this needs documentation more than mechanism design.
- **Failure modes when the canonical authority is offline.** App2
  emits a proposal that App1 can't see because App1's install is
  down for a week. What does App2's UI show in the meantime?
  "Pending" indefinitely is unacceptable. Does the substrate
  surface a `proposal_unconfirmed_for_T` timeout? Does App2 escalate
  through an app-supplied policy (e.g., "after 24h pending, treat
  as confirmed pending later reconciliation")? This interacts with
  conflict resolution (above): a long-pending proposal that later
  conflicts has cascading consequences.
- **Read your own writes.** When App2 emits a proposal optimistically,
  App2's user expects to see the change immediately in their own view
  — that's what optimistic UI promises. But App2's user might also
  dispatch a *second* proposal a moment later, against the optimistic
  state. The substrate needs to handle "chained proposals against
  unconfirmed state" — probably by tracking pending proposal state
  per-install and checking against it during conditional-mutation
  precheck, with conflict surfaced if the chain of pending proposals
  later conflicts with the canonical authority's resolution. Subtle;
  needs careful design.
- **Conditional mutations across non-aggregate projection shapes.**
  `TableProjectionSpec` rows are insert/remove, not in-place update.
  Do conditional mutations make sense against table-shaped views, or
  are they aggregate-only? Probably aggregate-only in v1, with a
  future generalization to table-shaped rows when a real consumer
  needs it.

## Future work

- **Migration plan.** Deprecate `IdempotencyStore` from the substrate's
  promised surface; downstream apps move to their own cache
  implementations if they still want HTTP-style idempotency for
  non-event-sourced surfaces. Plan the deprecation as part of the
  implementation ticket; do not unwind the shipped code in advance of
  the design landing.
- **Snapshot promotion through the conditional-mutation boundary.**
  How does the substrate handle a snapshot rebuild when the rebuild
  needs to re-derive view-row states that have only ever been
  produced by confirmed proposals? Probably trivially — the
  confirmation events carry the state transition explicitly — but
  spell it out.
- **Long-running conflict-resolution workflows.** Some app domains
  want a mediation window: a conflict surfaces, a 24-hour delay
  begins, if no human responds the substrate applies a default
  resolution. This is policy, not mechanism, but the substrate
  needs to expose enough hooks to support it.
- **Optimistic UI rendering conventions and library helpers.** The
  substrate ships a default; downstream apps and downstream UI
  libraries probably want common helpers for the pending/confirmed/
  reverted state machine. Library-supplied? Hht-diary-supplied?
  Decide.
- **Multi-editor authority-transfer protocol design.** The
  `authority_granted` / `authority_transferred` event types sketched
  above need real design: who can grant authority, how transfers
  resolve concurrent attempts to grab authority, what happens to
  in-flight proposals against an aggregate whose authority just
  moved.
- **Reconciliation after long network partitions.** Two installs
  diverge for a week, each accumulating proposals against the same
  aggregate, each thinking it has the canonical state because the
  other install's confirmations haven't arrived. When they re-sync,
  the substrate needs a defined order in which to process the
  backlog — probably "wall-clock arrival at the canonical authority
  determines confirmation order, conflicts surface naturally".

## What this means for CUR-1330

The shipped substrate (CUR-1330, merged) has working `IdempotencyStore`
infrastructure: sembast and Postgres backends, an in-memory variant, a
conformance harness, action-dispatcher integration, integration tests.
PR #14 (the polish PR) builds on it with `listEntries()`. None of this is
wrong — it is correct for the abstraction *as it is currently framed in
the substrate's specs*. But once this conditional-mutations spec
stabilizes and an implementation lands, the shipped `IdempotencyStore`
code either retreats to app-layer or is removed from the substrate's
promised surface.

Plan the migration as part of the implementation ticket. **Do not unwind
PR #14 in advance of the design landing.** The shipped code is the
reference impl of the abstraction the substrate currently ships; the
abstraction is being reconsidered, not the code's correctness against
its current specification. When this brainstorm stabilizes:

1. Author `spec/conditional-mutations.md` with normative requirements
   (the substrate ships conditional mutations as the only mutation
   primitive; pre-existing `IdempotencyStore` retreats to app-layer or
   is removed).
2. Author the implementation plan at
   `docs/superpowers/plans/YYYY-MM-DD-conditional-mutations-
   implementation.md`.
3. Revise CLAUDE.md's "Single-source-per-aggregate-type today" commitment
   to acknowledge that conditional mutations across installs presuppose
   the multi-editor coordination model.
4. Revise `EVS-PRD-action-dispatch`'s assertion D (idempotency-as-pluggable-
   cache) to reframe idempotency as a property of the CAS primitive.
5. Schedule removal of `IdempotencyStore` from the substrate's promised
   surface in a future library-version bump, with `lib_version_changed`
   event recording the transition.

## References

- `docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md`
  — substrate component model, including the action-dispatch path that
  conditional mutations modify.
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md`
  — projection model. Conditional mutations couple append to projection
  state at append time; the conditional-mutation precheck depends on the
  projection interpreter's in-Txn semantics.
- `docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md`
  — entry-type version model. Conditional mutations interact with
  promoter chains: a proposal's `expected_prior` is stated at the
  proposer's library version; the canonical authority interprets it at
  its own library version, possibly after promotion.
- `spec/dev-postgres-backend.md` — the EVS-DEV requirement under which
  the shipped `IdempotencyStore` infrastructure lives. Will need
  revision when this spec stabilizes.
- `CLAUDE.md` — repo-level architectural commitments. The "Single-
  source-per-aggregate-type today" commitment and the
  "originator-of-first-event canonicalization convention" both need
  revisiting in light of this spec.
