# CLAUDE.md

Guidance for Claude Code sessions working in this repo.

## What this repo is

Reactive, append-only event-sourcing primitives + companion libs
(`canonical_json_jcs`, `provenance`). Pure Dart. The primary downstream
consumer is `cure-hht/hht_diary`, which pins this repo by git ref.

This repo was extracted from `cure-hht/hht_diary` on 2026-05-08
(CUR-1317). See [README.md](README.md) for the cut-point and roadmap.

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
- **Originator-of-first-event bootstrap authority.** The single fixed
  rule: whoever appends the first event for an aggregate is the initial
  canonicalization authority. Terminates rule recursion at deployment
  infrastructure.
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

The design layer between PRDs and DEV-level requirements lives in
`docs/superpowers/specs/`. Phase I has two specs:

- `docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md`
  — the original Phase I overview. Pins the substrate's component
  model, the event log and hash-chain layout, the action-dispatch flow,
  the ingest path, and the storage abstraction. **Partially superseded:**
  the "Subscribe primitive", "Materializer", "Filter, query, and the
  closed-set rule", and "Multi-source readiness" sections are
  superseded by the spec below.
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md`
  — the authoritative spec for the projection model (declarative
  `ProjectionSpec` shapes interpreted by the substrate), the promoter
  model (declarative `PromoterSpec` composing library-supplied
  transformation primitives), the `subscribe<T>` primitive, and the
  library-version lifecycle. Embodies the "Domain-neutral lib",
  "Declarative projections", "Permission policy is substrate code", and
  "Library version recorded in the log" commitments above.

The implementation plan that turns these specs into working code is at
`docs/superpowers/plans/2026-05-09-projections-and-subscribe-implementation.md`.
DEV-level requirements (`EVS-DEV-*`) are authored alongside the code
that satisfies them per the plan's task structure.

Subsequent design specs land here as design work demands — for example,
when Phase II's multi-source canonicalization rule grammar needs
pinning, that gets its own spec.

## License

AGPLv3.
