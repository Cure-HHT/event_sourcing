# CLAUDE.md

Guidance for Claude Code sessions working in this repo.

## What this repo is

Reactive, append-only event-sourcing primitives + companion libs
(`canonical_json_jcs`, `provenance`). Pure Dart. The primary downstream
consumer is `cure-hht/hht_diary`, which pins this repo by git ref.

This repo was extracted from `cure-hht/hht_diary` on 2026-05-08
(CUR-1317). See [README.md](README.md) for the cut-point and roadmap.

As of CUR-1330, the substrate ships two concrete `StorageBackend`
reference implementations — `SembastBackend` (mobile/Flutter) and
`PostgresBackend` (server-side) — both passing the same backend-
agnostic conformance harness. This unblocks the Phase IV portal-server
cutover.

## Layout

- `event_sourcing/` — core lib (storage, sync, ingest, materialization,
  action dispatch, permissions). Contains two intra-lib demos under
  `example/` and `example_action_permissions/`.
- `canonical_json_jcs/` — JCS (RFC 8785).
- `provenance/` — append-only provenance chain types.
- `spec/` — formal requirements in EVS namespace (PRD level today;
  OPS and DEV land alongside implementation work). See
  `spec/requirements-spec.md` for the canonical grammar and
  `spec/README.md` for the directory layout.
- `docs/superpowers/specs/` — design specs that elaborate the PRDs
  into mechanism-level decisions ahead of implementation.
- `docs/superpowers/plans/` — implementation plans that reference the
  specs.

The path-deps inside `event_sourcing/pubspec.yaml` point at
`../canonical_json_jcs` and `../provenance` — siblings at repo root.

## Architectural commitments (load-bearing)

These were brainstormed and committed during the CUR-1192 / CUR-1317
sessions in `hht_diary`. They survive the extraction and shape Phase I /
Phase II work.

- **Domain-neutral lib.** This repo is a substrate. It must not ship
  domain types (e.g., `DiaryEntry`), domain materializers, or domain-
  specific event-type names. Diary code in the lib today is kick-start
  extraction debt being removed in CUR-1317; downstream consumers like
  `hht_diary` author their own `ProjectionSpec`s and register them
  against the lib's `ProjectionRegistry`.
- **Declarative projections (no author-supplied fold).** Materialized
  views are computed by the substrate from declarative `ProjectionSpec`
  data (Aggregate, Table shapes); promoters are declarative
  `PromoterSpec` data composing library-supplied transformation
  primitives. No host-application callbacks; no per-materializer fold
  code. Library primitives are append-only (semantics frozen once
  shipped; bug fixes ship as new primitive names).
- **Permission policy is substrate code.** `AuthorizationPolicy`
  cannot be app-supplied without breaking closed-under-events for
  action outcomes. v1 ships exactly one policy mechanism — the
  role/permission/scope model in `event_sourcing/lib/src/permissions/`.
  Alternative policy models require library extension (same Append-
  Only Primitives discipline as projections), not app-side replacement.
- **Library version recorded in the log.** Substrate emits
  `lib_version_initialized` on first boot under a new lib version and
  `lib_version_changed` on subsequent transitions. Downgrades are
  refused by default. State at sequence N is reconstructable from
  `(events, projection_specs, promoter_specs, lib_version)` — all in
  the log.
- **Entry-type version is substrate-owned.** The substrate stamps
  `entryTypeVersion = entryTypes.byId(entryType).registeredVersion` on
  every appended event. Producers do not choose the version; ingest
  transparently promotes older-peer events before the fold;
  `EventStore.open` snapshot-promotes view rows on a `registeredVersion`
  bump and refuses downgrade. Promoter primitives are restricted to
  shape-changers (`RenameField`, `DefaultField`, `DropField`) so the
  chain commutes with the deep-merge fold — snapshot promotion at boot
  is provably equivalent to event-replay-with-promotion. See
  `docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md`
  for the full design.
- **Originator-of-first-event canonicalization convention.** A Layer-2
  convention (see Epistemic layers): whoever appends the first event
  for an aggregate is treated as the initial canonicalization authority
  for that aggregate. Terminates rule recursion at deployment
  infrastructure. Phase II's multi-source rule grammar may add
  settings-event-driven overrides; the originator-of-first-event
  convention itself remains the substrate's default.
- **Closed-under-events trust model.** Permissions/role-assignment data
  are events in the same log; the substrate's projection interpreter
  reads its own outputs; external systems integrate at ingest, not at
  evaluation-time. Action outcomes (success vs `authorization_denied`)
  are reproducible from the log + lib version.
- **Notifications vs DataInvalidation are separate.** They share the
  substrate but not the interface. Notifications target humans;
  DataInvalidation targets software. Multi-editor work likely subsumes
  DataInvalidation.
- **Reactive substrate intent.** Ingest-always + filters + at-least-
  once delivery + per-aggregate-per-Source ordering. Phase I realizes
  this with a unified `subscribe<T>(filter, mode)` primitive (modes:
  `Events`, `AggregateMode<T>`; `View<T>` deferred) and the declarative
  projection interpreter described above. Cross-process resume /
  persistent watermark lives in `Destination`, not `subscribe<T>`. The
  earlier `watchEvents` / `watchView` / `watchFifo` decomposition is
  abandoned.
- **Single-source-per-aggregate-type today.** Multi-source machinery
  exists in design but is dormant in v1; Phase II activates it.

## Epistemic layers

> **Canonical source:** `spec/prd-library-charter.md`, "Epistemic
> layers" section. The discussion is duplicated here for session-start
> convenience because the distinction is load-bearing; update both
> places together, and treat the spec as authoritative if they drift.

The substrate makes two kinds of claims, and the distinction is
load-bearing. Confusing them leads consumers to either over-trust the
library's defaults or to abandon the substrate when they need a
different interpretation than it ships.

**Layer 1 — Facts (objective, cryptographic / structural).** These are
the substrate's hard guarantees. They are tamper-evident and absolute:

- The event at sequence N has hash H
- The hash chain from genesis to N is intact
- The provenance entries say the event passed through hops A → B → C
  with attribution to initiators I₁, I₂, I₃ at times t₁, t₂, t₃
- The append of this event was atomic with its row writes inside the
  same transaction
- Per-aggregate-per-Source order is preserved

ALCOA+ alignment lives entirely at this layer. The cryptographic and
structural facts are what regulators can be defended against.

**Layer 2 — Conventions (subjective, library-provided defaults).**
These are the library's chosen *interpretations* of the event stream.
They are useful defaults, not unique truths:

- A "tombstone" event type deletes the row (the substrate could equally
  preserve the row with a marker, or hide-not-delete)
- Missing keys in a delta preserve prior; present-null clears (the
  substrate could equally treat null as absent)
- Whoever appends the first event for an aggregate is the canonical
  authority for that aggregate (the substrate could equally require
  out-of-band canonicalization assignment)
- A projection produces one row per aggregate, materialized via
  generic merge (the substrate could equally produce per-event rows or
  derived-only views)
- "Version" is a monotonically-bumped integer per entry type (the
  substrate could equally use content-hash-as-version)

The library bundles these as primitives because most consumers want
them, but they don't carry the same epistemic weight as Layer 1.
Applications that want different interpretations build them on top of
Layer 1 facts — by subscribing to raw events and computing app-side
state, or eventually by registering alternative convention sets shipped
as new library primitives under the Append-Only Primitives discipline.

**What this means for the substrate's other commitments:**

- The **closed-under-events trust model** is precisely scoped: state
  *under the Layer 2 conventions* is reconstructable from the event log
  under a known library version. It does not claim the conventions are
  universally correct.
- The **declarative projection model** ships one Layer 2 materialization
  per registered `ProjectionSpec`. Applications needing different
  materializations build them on top of `subscribe<T>(_, Events())` or
  `EventStore.read(...)` — and that's an expected, supported pattern,
  not a fallback.
- **Append-Only Primitives discipline** applies to Layer 2 conventions
  too. Once a convention ships under a name with given semantics, those
  semantics are frozen; alternative behaviour is a new primitive, not
  a re-interpretation of an existing one.

When proposing new library primitives, code comments, or PRD assertions,
be explicit about which layer the claim sits at. "The library SHALL
preserve hash-chain integrity" is Layer 1 and absolute. "The library
SHALL treat tombstone event types as row deletions" is Layer 2 and
should read more like "The library's default `AggregateProjectionSpec`
interpretation TREATS event types in `tombstoneEventTypes` as row
deletions." The same precision applied to existing surfaces is part of
the ongoing authoring discipline (charter assertion I).

## Trust boundaries

The substrate trusts a small, enumerable set of inputs without
auditing them. Anything outside this list must be derivable from the
event log; any new trust dependency is a load-bearing change requiring
the same deliberation as a new architectural commitment.

The currently-trusted inputs are:

- **`StorageBackend` implementation.** Pluggable interface registered
  at composition time. Trusted for data persistence integrity:
  correct reads/writes, transaction atomicity, durability. Two
  reference impls ship today: `SembastBackend` (mobile,
  `event_sourcing/lib/src/storage/`) and `PostgresBackend` (server,
  `event_sourcing/lib/src/storage/postgres/`). Both pass the same
  backend-agnostic conformance harness. Alternative backends
  (IndexedDB, etc.) are app-supplied; each is the trusted persistence
  layer for that deployment.
- **`Destination` outbound transport.** Per-destination delivery
  transport (HTTP, WebSocket, file, etc.) supplied by the app at
  composition time. Trusted for transport-layer correctness and
  for honouring the FIFO queue's delivery semantics. The substrate
  does not verify that the transport delivered the event to its
  remote endpoint correctly; only that the FIFO queue advanced.
- **Caller-supplied `Principal` on action submissions and event
  metadata.** Currently accepted on faith: the substrate does not
  authenticate the `Principal` claimed in an `ActionSubmission`,
  nor the `initiator` recorded on appended events. The calling
  application is trusted to supply correct identity. **This is a
  known incomplete boundary** — the substrate has no inbound
  authentication flow (no `authentication_attempted` event type)
  and no outbound `AuthenticationProvider` pluggable interface, so
  identity claims live entirely outside the closed-under-events
  guarantee. Closing this gap is future work; not in scope for
  Phase I.

Everything else — projection rules (`ProjectionSpec`), promoter rules
(`PromoterSpec`), policy logic (in-lib for v1, see Architectural
Commitments), event payloads, hash chains, library version, action
outcomes — is derivable from the log under one of the trusted
backends above.

When proposing changes, treat any new external dependency
(callbacks, host-supplied evaluators, app-injected logic that
participates in fold or policy decisions) as a trust-boundary
expansion that requires explicit architectural review. The trust
surface is meant to stay enumerated.

## Requirement traceability

Every formal requirement in this repo has an ID of the form:

```text
EVS-{TYPE}-{name}
```

Where `TYPE ∈ {PRD, OPS, DEV}` (uppercase) and `name` is a kebab-case
slug describing the requirement subject. Examples:

- `EVS-PRD-event-store`
- `EVS-OPS-secret-rotation`
- `EVS-DEV-provenance-entry-schema`

The full grammar — including assertion labels, `Refines:` /
`Satisfies:` metadata, and verification modes — lives in
`spec/requirements-spec.md`.

**Names are stable.** Once a requirement is authored under a given
component name, renaming it is a **breaking change** to every
reference: `// Verifies:` annotations in tests, `// Implements:`
annotations in production code, `Refines:` and `Satisfies:` metadata
in other requirements, and Rationale prose. A rename therefore
requires a coordinated sweep of all references at the same time as
the rename itself, and should be called out explicitly in the commit
that performs it. Treat the policy the same as a breaking change to a
published API: rare, deliberate, and worth a sentence of
justification.

Code annotations from the kick-start commit reference the legacy
`REQ-d{NNNNN}` IDs from `hht_diary`. Those references are stale and
will be re-bound to `EVS-DEV-{name}` IDs as DEV-level requirements are
authored alongside implementation work. Until then, treat the legacy
references as historical pointers.

## Conventions

- **Branch naming**: `CUR-NNNN-{kebab-slug}` — Linear ticket reference plus a short kebab-case description of the change. No user prefix; no slashes. Examples: `CUR-1317-pre-commit`, `CUR-1317-req-naming`.
- **Commit messages**: free-form (no enforced CUR/REQ format on commits).
- **PR titles**: must include the Linear ticket reference (`[CUR-NNNN]`). The org-level branch-protection ruleset enforces this; the squash-merge commit on `main` uses the PR title verbatim.
- **Spec/INDEX maintenance**: via the `elspais` MCP. The repo's `.elspais.toml` is set up under namespace `EVS` with named-component IDs (no numeric assignment); regeneration of `spec/INDEX.md` is automated.

## What downstream consumers see

`hht_diary`'s `clinical_diary/pubspec.yaml` will pin this repo by `git:`
ref. During active Phase I/II development, devs override locally with
`pubspec_overrides.yaml` pointing at a sibling clone. That override is
gitignored in `hht_diary`. No other consumer depends directly on these
packages today.

## Reading the design specs

Design ideas in this repo evolve through a brainstorm → stabilize →
archive lifecycle. **Brainstorm output** is prose-heavy and lives
transiently in `docs/superpowers/specs/`. **Stabilized designs** migrate
into `spec/<topic>.md` files containing the normative requirement blocks
alongside the cross-system narrative (overview, architecture, decisions
rejected, open questions, future work) as remainder sections. The
original brainstorm doc is archived once its content has migrated. See
`spec/README.md` for the full lifecycle convention and file-organization
rules (multi-requirement files, remainder sections, mermaid diagrams).

Brainstorm-stage design specs currently in `docs/superpowers/specs/`
that have NOT yet been migrated to `spec/`:

- `2026-05-09-substrate-and-materializer-design.md` — the original
  Phase I overview. Pins the substrate's component model, the event log
  and hash-chain layout, the action-dispatch flow, the ingest path, and
  the storage abstraction. **Partially superseded:** the "Subscribe
  primitive", "Materializer", "Filter, query, and the closed-set rule",
  and "Multi-source readiness" sections are superseded by the spec
  below.
- `2026-05-09-projections-and-subscribe-design.md` — the authoritative
  spec for the projection model (declarative `ProjectionSpec` shapes
  interpreted by the substrate), the promoter model (declarative
  `PromoterSpec` composing library-supplied transformation primitives),
  the `subscribe<T>` primitive, and the library-version lifecycle.
  Embodies the "Domain-neutral lib", "Declarative projections",
  "Permission policy is substrate code", and "Library version recorded
  in the log" commitments above.
- `2026-05-11-entry-type-version-substrate-owned-design.md` — entry-type
  version is substrate-owned; promoter primitives restricted to shape-
  changers (RenameField/DefaultField/DropField); snapshot-promotion at
  boot is provably equivalent to event-replay-with-promotion.

The implementation plan that turns these specs into working code is at
`docs/superpowers/plans/2026-05-09-projections-and-subscribe-implementation.md`.
DEV-level requirements (`EVS-DEV-*`) are authored alongside the code
that satisfies them per the plan's task structure.

Subsequent design specs land in `docs/superpowers/specs/` as design work
demands; once stabilized they migrate to `spec/<topic>.md` per the
lifecycle above. Roadmap and time-evolving status documents (e.g.,
`docs/superpowers/specs/2026-05-11-roadmap.md`) stay in
`docs/superpowers/specs/` — they are not normative design specs.

## License

AGPLv3.
