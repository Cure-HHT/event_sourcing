# Entry-type version as substrate-owned state

**Status:** Design accepted; ready for implementation plan.
**Authored:** 2026-05-11
**Context:** Closes Phase I open questions Q1 (does the append path need to
invoke promoters?) and Q2 (should `ActionDispatcher.entryTypeVersion` look up
the registry?) from `docs/superpowers/specs/2026-05-11-roadmap.md`. Also
resolves Q3 (does `view_target_versions` retain its purpose?) as a side
effect — it does, with sharper rationale.

## Problem

Three intertwined defects in Phase I's handling of `entry_type_version`:

1. **Producer-side hardcodes.** `ActionDispatcher` (2 sites) and
   `event_seed_applier` hardcode `entryTypeVersion: 1` on append, with a
   comment admitting "hardcoding gets reviewed." The example apps and three
   substrate-internal call sites do the same. The lib has an established
   pattern (`entryTypes.byId(X)!.registeredVersion`) used by
   `clearSecurityContext`, `applyRetentionPolicy`, bootstrap, and
   `destination_registry`, but the pattern is followed inconsistently.

2. **No definition of producer/projection version contract.** The substrate
   does not say whether a producer is allowed to emit events at older
   `entryTypeVersion` values than the current registered version. Nor does it
   say what happens when a projection's stored `view_target_versions` lags
   behind `registeredVersion`. Today, the live append and ingest paths fold
   raw `event.data` regardless of version, so any drift between producer
   version and projection version produces silently-wrong view state.

3. **No story for `registeredVersion` evolution.** When an app author bumps
   an entry type's `registeredVersion` (legitimate schema evolution), the
   substrate has no mechanism to bring existing view rows forward without a
   full `rebuildView` call. Bootstrapping with a bumped registry produces
   mixed-version view rows.

## Decision summary

| Decision | Layer |
|---|---|
| Producers always emit at `registeredVersion`. Substrate stamps the version; `EventStore.append`'s `entryTypeVersion` parameter is removed. | Layer 1 |
| Ingest may receive events at any `entryTypeVersion <= registeredVersion`. Per-event payload promotion is applied in-memory before each matching projection's fold. The stored event keeps its wire payload (hash-chain integrity). | Layer 1 / Layer 2 boundary |
| On `EventStore.open`, the substrate detects per-`(viewName, entryType)` `view_target_versions` lag and snapshot-promotes affected rows before serving subscribers. Snapshot promotion is provably equivalent to event-replay-with-promotion under the restricted promoter primitive set. | Layer 2 (default substrate behavior) |
| `registeredVersion` downgrade is refused unconditionally in Phase I. No `allowDowngrade` flag. Future demotion-spec mechanism is deferred. | Layer 1 |
| `TransformPrimitive` subclasses are restricted to shape-changers: `RenameField`, `DefaultField`, `DropField`. `DeriveField` is deleted (zero production callers; non-commutative with fold). | Layer 2 |
| A version is operationally defined as the set of its promoters. The v2 schema = (v1 schema) ∘ (v1→v2 chain). No independent declaration of "v2's schema" lives separately from the chain. | Authoring discipline |

## Invariants

### Layer 1 — substrate-enforced facts

- **Stamped at append.** Every event in the local log satisfies
  `entryTypeVersion == entryTypes.byId(entryType).registeredVersion` at the
  moment of its append. The substrate stamps this value; callers do not pass
  it.
- **Closed under registry.** The currently-registered version of each entry
  type is the unique reference point. Producers, projection folds, and rebuild
  callers all derive from this single source.
- **Downgrade refusal.** `EventStore.open` refuses to boot if any registered
  entry type's `registeredVersion` is below the highest value recorded for
  that entry type in any view's `view_target_versions`.
- **Hash-chain integrity preserved across promotion.** Ingest-side payload
  promotion is in-memory only; the StoredEvent persisted to the log is the
  wire event, unmodified. Event hashes verify against the wire bytes, not
  against any promoted form.

### Layer 2 — substrate-provided default conventions

- **Promotion fills the gap between wire/historic version and current
  registry.** When the substrate folds an event whose version is below
  `registeredVersion`, it applies the registered promoter chain to a
  payload-shaped working copy before passing it to the fold interpreter.
  This is the lib's default convention; applications that want different
  semantics (e.g., refuse rather than promote, or maintain parallel views at
  multiple version targets) compute their materialization on top of
  `subscribe<T>(_, Events())` or `EventStore.read(...)`.
- **Snapshot promotion at boot is equivalent to replay.** Under the
  restricted primitive set, applying the v1→v2 chain to a snapshot row
  produces the same row state as replaying all events with per-event
  promotion. This equivalence is what allows the substrate to skip a full
  re-replay on `registeredVersion` bump.
- **`view_target_versions` is the per-view, per-entry-type bookkeeping that
  drives auto-promotion.** Its semantics tighten under this design: a stored
  target is the version each view's rows are currently folded at; lag
  triggers promotion; equality short-circuits.

## Architectural commitments locked

These ride alongside the existing commitments in CLAUDE.md:

- **A version is defined by its promoter chain.** The lib does not separately
  declare "v2's schema." Bumping `registeredVersion` from N to N+1 is
  meaningful only if a corresponding `PromoterSpec` (entry type X,
  fromVersion=N, toVersion=N+1) ships in the same release. The promoter
  chain IS the schema delta. Append-Only Primitives discipline applies:
  shipped chains are frozen.
- **Promoter primitives are shape-changers only.** `Rename` / `Default` /
  `Drop` are the entire primitive set. Row-level derivation (display
  formatting, computed denormalizations) belongs in projection
  `derivedFields` if it needs to be materialized, or at the UI/read site
  otherwise. Promoter chains stay fold-commutative by construction.
- **Producer-side discretion does not exist.** Callers do not choose what
  `entryTypeVersion` to emit at. The substrate stamps the current
  `registeredVersion`.

## Design

### Append API change

```dart
// BEFORE
Future<StoredEvent?> append({
  required String entryType,
  required int entryTypeVersion,    // removed
  required String aggregateId,
  ...
});

// AFTER
Future<StoredEvent?> append({
  required String entryType,
  required String aggregateId,
  ...
});
```

- `_validateAppendInputs` already throws for unregistered `entryType`, so the
  substrate's internal `entryTypes.byId(entryType)!.registeredVersion` lookup
  is safe.
- `appendInTxn` undergoes the same parameter removal; the version is
  retrieved once per call inside the function.
- All callers updated. The hardcode sites listed in the problem statement
  disappear by construction.
- One substrate-internal carve-out remains: `_appendLibVersionEventToBackend`
  retains `entryTypeVersion: 1` because it is invoked from inside
  `EventStore.open` BEFORE the `EventStore` instance exists; it cannot call
  `appendInTxn`. The carve-out is comment-documented in place. Lib-version
  entry types are listed in `kSystemEntryTypes` for `byId()` non-nullity, so
  if their `registeredVersion` ever changes from 1, this carve-out becomes a
  divergence point that requires re-evaluation.
- `'ingest-audit'` is registered as a new entry type in `kSystemEntryTypes`,
  which allows `logRejectedBatch` and `_emitDuplicateReceivedInTxn` to call
  `appendInTxn` and have the substrate stamp the registered version. This
  closes two more hardcode sites.

### Ingest-side promotion

`_ingestOneInTxn`, after Chain 1 verify and idempotency, before
`_interpreter.applyEvent`:

```text
if (incoming.entryTypeVersion < entryTypes.byId(incoming.entryType).registeredVersion):
    for each ProjectionSpec whose interest.matches(incoming):
        chain = promoters.chain(viewName, entryType, from=incoming.version, to=registeredVersion)
        promotedData = TransformChain.applyAll(chain, incoming.data, firstEventTimestamp=incoming.clientTimestamp)
        promotedEvent = incoming.copyWith(data: promotedData)
        # Per-view fold; one event may be folded with different promoted
        # payloads into different views.
        fold(spec, promotedEvent)
else:
    # untouched; existing fold path
```

Subtlety: when multiple specs match a single event, each spec gets its own
in-memory promoted copy because the chain is keyed by `(viewName, entryType,
from, to)` and may differ per view (a particular view might not need a
particular transform). The original StoredEvent is persisted unmodified to
the log.

`IngestEntryTypeVersionAhead` (REQ-d00145-M) continues to throw on the
greater-than case.

The local-append path does NOT need this logic — by invariant, every
locally-appended event already has `entryTypeVersion == registeredVersion`.

### `view_target_versions` seeding

Today `view_target_versions` is populated only by `rebuildView`. Under this
design the table is also written at two new points:

1. **First projection observation.** `EventStore.open`, after the downgrade
   refusal check and before the snapshot-promotion pass, ensures every
   registered `ProjectionSpec` has a `view_target_versions` row for every
   entry type its `interest` filter matches. Where a row is absent, the
   substrate seeds it at the entry type's current `registeredVersion`. This
   establishes the "high-water mark per (view, entry type)" the
   snapshot-promotion pass compares against on subsequent boots. On a
   greenfield install, every projection is seeded to current
   `registeredVersion` and the snapshot-promotion pass is a no-op.

2. **Promotion completion.** As described in the snapshot-promotion section
   below, each `(viewName, X)` pair whose stored target lags is updated to
   the current `registeredVersion` once promotion finishes.

`rebuildView` continues to clear and rewrite `view_target_versions` as part
of its existing strict-superset contract; that path is unchanged.

### Boot-time snapshot promotion

`EventStore.open`, after lib-version handling, downgrade refusal, and
`view_target_versions` seeding, before `_subs` is exposed to callers:

For each registered `ProjectionSpec` in `store.projections`, for each entry
type `X` whose `view_target_versions[viewName, X]` is below
`entryTypes.byId(X).registeredVersion`:

1. `chain = promoters.chain(viewName, entryType=X, from=stored_target, to=registeredVersion)`.
2. `affectedAggregateIds = backend.findAllEvents(entryType: X).map((e) => e.aggregateId).toSet()`,
   using the entry-type filter added to `findAllEvents` (see "Generalized
   event query" below).
3. For each `aggregateId` in `affectedAggregateIds`:
   - Read the view row for `(viewName, aggregateId)`. Skip if absent
     (tombstoned).
   - `promotedRow = TransformChain.applyAll(chain, rowState, firstEventTimestamp=rowState['firstEventTimestamp'])`.
   - For `AggregateProjectionSpec` only: re-run `spec.derivedFields` on the
     promoted row. (`TableProjectionSpec` has no derived fields.)
   - Write back the promoted row.
4. Update `view_target_versions[viewName, X] = registeredVersion`.
5. Emit one `view_snapshot_promoted` audit event per `(viewName, X)` pair,
   recording `{viewName, entryType, fromVersion, toVersion, rowsPromoted}`.

Each `(viewName, X)` promotion runs inside a single backend transaction so
that a mid-promotion crash rolls back cleanly and the next boot retries.

The snapshot ≡ replay equivalence makes step 7's TransformChain application
correct without iterating events. This is the cheap-by-design move.

### Generalized event query

Snapshot promotion needs to know "which aggregates have ever produced an
event of entry type `X`?" Rather than adding a narrow helper for that single
case, this design generalizes the existing `StorageBackend.findAllEvents`
read primitive with two additional optional filters:

```dart
Future<List<StoredEvent>> findAllEvents({
  int? afterSequence,
  int? limit,
  String? originatorHopId,
  String? originatorIdentifier,
  String? entryType,                  // new
  DateTime? clientTimestampStart,     // new — inclusive lower bound
  DateTime? clientTimestampEnd,       // new — inclusive upper bound
});
```

Semantics: all supplied filters compose with AND. Existing callers that
omit the new filters see unchanged behavior. The transactional
`findAllEventsInTxn` gets the same parameter additions.

The wider rationale: this is a generally-useful query primitive that
substrate consumers will reach for whenever the audit-stream UX gets richer
than "show me the global log in order." Concrete consumer cases include:

- "Show me every event of type `entry_recorded` in the last 30 days."
- "Audit all `permission_granted` events between 2026-05-01 and 2026-05-31."
- "Investigation: every event of type `action_denial` produced by this
  install (originator filter) of type X (entry-type filter) in the
  incident window (timestamp filter)."

Adding the parameters to the single existing primitive keeps the substrate
surface compact while making these queries trivial to express. Phase II
multi-source filtering composes on top of this without further changes.

The `StorageBackend` interface is deliberately backend-agnostic. Concrete
implementations will need index work to keep the filtered query under-load
on real datasets — composite indexes on `entry_type` and
`client_timestamp` for SQL backends, equivalent secondary-index work for
the sembast reference impl, etc. Filter-translation is naturally a SQL
`WHERE ... AND ...` for SQL-shaped backends; the substrate's API does not
prescribe how. Backend portability is its own concern and is captured
separately in the roadmap.

A thin `EventStore.find(...)` wrapper that exposes the same parameters
without forcing callers to reach for the backend directly is a candidate
expose-point but not strictly required by this design — `EventStore`
already has `backend` accessible to internal use.

### Downgrade refusal

`EventStore.open`, very early — after the existing lib-version downgrade
check, before any other work:

1. For each entry type `X` in the registry, compute
   `highestPriorTarget = max(view_target_versions[v, X] for v in all views)`
   (or 0 if no view has folded X).
2. If `entryTypes.byId(X).registeredVersion < highestPriorTarget`:
   throw `EntryTypeVersionDowngradeError(entryType: X, fromVersion: highestPriorTarget, toVersion: registeredVersion)`.
3. No flag, no override. Operators must pin a lib that supports the higher
   version, OR (future work) ship a `DemotionSpec` for the affected entry
   types and re-attempt with `allowEntryTypeDowngrade: true` (deferred).

A symmetric `entry_type_registry_initialized` audit event already exists; a
downgrade case would never reach the point of emitting one because the boot
fails first.

### Promoter primitive set

Delete `DeriveField` from `lib/src/promoters/primitives/transform.dart`. Its
sole reference is in `test/promoters/primitives/transform_test.dart` (a
group containing one self-test); delete that group at the same time.

Remaining primitives:

- `RenameField(from, to)` — moves a key. No-op on rows without `from`. No-op
  composes correctly with deep-merge fold.
- `DefaultField(fieldName, defaultValue)` — fills in if absent. Equivalent
  to "every v1 event had this field implicitly". Applied to rows whose
  history includes events of the affected entry type (per the row-scope
  decision below).
- `DropField(fieldName)` — removes a key. No-op on rows without the field.

These three are pure shape-changers; all three commute with the
`AggregateProjectionSpec` deep-merge-with-null-as-clear fold. Snapshot
promotion is therefore strictly equivalent to event-replay-promotion.

### Snapshot-promotion row scope

For `(viewName, X)` with stored target < `registeredVersion`, the substrate
applies the chain only to rows whose history includes events of entry type
`X`. The substrate finds these via a query against the event log
(`backend.findAggregateIdsWithEventType(X)` — to be added). This preserves
strict snapshot ≡ replay equivalence: a row that never folded a v1 event of
`X` is, under per-event-replay-with-promotion, untouched; the snapshot pass
likewise leaves it untouched.

The cheaper alternative — applying the chain to every row in any view with a
stale target — was rejected. It's correct for `RenameField` and `DropField`
(no-ops on unaffected rows) but incorrect for `DefaultField`, which would
add the default to rows that have no semantic basis for it. The footnote
would be load-bearing, so we just pay for the query at boot.

## Out of scope

- **Phase II multi-source canonicalization.** `registeredVersion` is
  per-install. Multi-source rule grammar (`set_canonicalizer`, etc.) is
  dormant. This design composes correctly under Phase II's eventual
  event-sourced registry: in Phase II, `register_projection(spec)` events
  drive the registry and a settings-event-driven version bump triggers the
  same snapshot-promote pass as a reactor rather than as boot logic.
- **`TableFold` removal-signal API (Phase I open Q4).** Independent.
- **`REQ-d{NNNNN}` rebinding sweep (Phase I open Q5).** Only files touched
  by this work get rebound to `EVS-DEV-*` IDs.
- **Demotion specs.** Reserved for future work. The Phase I refusal of
  `registeredVersion` downgrade is unconditional; the deferred mechanism
  would mirror `PromoterSpec` for the v2→v1 direction, gated behind an
  explicit author opt-in, with a parallel `view_target_versions` lift in the
  decreasing direction.

## Test plan

- **Unit:** new tests for the stamp-from-registry behavior of
  `EventStore.append` and `appendInTxn` (correct value stamped; throws on
  unregistered entry type; no path that emits a wrong version).
- **Unit:** new tests for ingest-side promotion: peer event at v1 received
  by v2 receiver, folded into matching views with promoted payload;
  multi-spec divergence; greater-than rejection unchanged; equality and
  greater-than paths untouched.
- **Unit:** new tests for boot-time snapshot promotion: lagging view rows
  promoted on `EventStore.open`; non-lagging untouched; rows unaffected by
  the entry type untouched (the row-scope query is correct); audit event
  emitted; crash mid-promotion rolls back and retries on next boot.
- **Unit:** new tests for `view_target_versions` seeding: greenfield boot
  with a fresh projection seeds rows at current `registeredVersion`;
  subsequent boot with no registry change is a no-op; the seeding pass
  ignores entry types not matched by a projection's `interest`.
- **Unit:** new test for downgrade refusal: `EventStore.open` throws
  `EntryTypeVersionDowngradeError` when a registered entry type's
  `registeredVersion` is below the highest `view_target_versions` value.
- **Unit:** `DeriveField` group removed from `transform_test.dart`; remaining
  primitives' tests pass unchanged.
- **Unit:** new tests for the extended `findAllEvents` filters:
  `entryType`-only, `clientTimestampStart`/`clientTimestampEnd`-only,
  combined-filter AND semantics, existing filter axes still honored, empty
  result when no events match.
- **Integration:** both example apps' bootstrap and golden-path actions
  exercised end-to-end with the new substrate-stamped version; no behavioral
  regression.

## Requirements

DEV-level requirements authored alongside implementation. Anticipated
identifiers (final shape determined when authoring):

- `EVS-DEV-append-stamps-registered-version`
- `EVS-DEV-ingest-promotes-before-fold`
- `EVS-DEV-snapshot-promotion-on-open`
- `EVS-DEV-entry-type-downgrade-refusal`
- `EVS-DEV-promoter-primitive-set` (codifying the three primitives)
- `EVS-DEV-version-defined-by-promoter-chain` (the operational definition)

Existing affected requirements requiring revision or supersedure:

- `REQ-d00141-B` — "per-field append API; entryTypeVersion required" — the
  required-parameter clause is reversed; rebinding to a new `EVS-DEV-*` ID
  is the right move.
- `REQ-d00141-F` — "append does NOT validate entryTypeVersion against the
  registry" — superseded; the substrate now stamps from the registry rather
  than accepting and validating.
- `REQ-d00145-M` — "ingest refuses entries with `entryTypeVersion >
  registeredVersion`" — preserved; the new ingest path runs after this
  check.

## Open implementation questions (small)

- Each `StorageBackend` implementation will need index work to keep the
  extended `findAllEvents` query (entry-type + timestamp range) responsive
  on large logs. Concrete index strategy is per-backend and per-deployment
  work, not pinned by this design. Snapshot promotion at boot is rare;
  audit-stream UX queries may be frequent — the index choice is driven by
  the latter. Backend portability and SQL-side translation are tracked
  separately in the roadmap.
- Whether to ship a thin `EventStore.find(...)` wrapper around the
  generalized `findAllEvents` query, or leave callers to reach for
  `store.backend.findAllEvents(...)`. Lightweight question; the
  implementation plan can decide based on call-site ergonomics.

## References

- `docs/superpowers/specs/2026-05-11-roadmap.md` — Phase I open questions
  Q1 and Q2 (this design's source).
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md` —
  authoritative Phase I projection / promoter spec.
- `CLAUDE.md` — "Architectural commitments", "Epistemic layers" (Layer 1 /
  Layer 2 framing applied throughout).
