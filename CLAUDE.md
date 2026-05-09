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
- `spec/` — formal requirements (REQ-d numbers carried over from
  hht_diary; same `dev-event-sourcing*.md` files).
- `docs/superpowers/specs/` — design specs for the libraries
  (audited-actions, action-permissions, action-permissions-demo).
- `docs/superpowers/plans/` — implementation plans for the same.

The path-deps inside `event_sourcing/pubspec.yaml` point at
`../canonical_json_jcs` and `../provenance` — siblings at repo root.

## Architectural commitments (load-bearing)

These were brainstormed and committed during the CUR-1192 / CUR-1317
sessions in `hht_diary`. They survive the extraction and shape Phase I /
Phase II work.

- **In-library materializer.** Reconciliation/canonicalization rules
  live in this lib, configured via auditable settings events; no host-
  application callbacks. Deterministic and replayable.
- **Originator-of-first-event bootstrap authority.** The single fixed
  rule: whoever appends the first event for an aggregate is the initial
  canonicalization authority. Terminates rule recursion at deployment
  infrastructure.
- **Closed-under-events trust model.** Permissions/role-assignment data
  are events in the same log; the materializer queries its own
  projections; external systems integrate at ingest, not at
  evaluation-time.
- **Notifications vs DataInvalidation are separate.** They share the
  substrate but not the interface. Notifications target humans;
  DataInvalidation targets software. Multi-editor work likely subsumes
  DataInvalidation.
- **Reactive substrate intent.** Ingest-always + watermark + filters +
  at-least-once delivery + per-aggregate-per-Source ordering. Phase I
  realizes this with a unified `subscribe<T>(filter, mode)` primitive
  and a typed `Materializer<T>`. The earlier `watchEvents` /
  `watchView` / `watchFifo` decomposition is abandoned.
- **Single-source-per-aggregate-type today.** Multi-source machinery
  exists in design but is dormant in v1; Phase II activates it.

## Requirement traceability

REQ-d numbers from hht_diary's `dev-event-sourcing*.md` come along on
extraction (no renumbering). New work cuts new REQ-d numbers in this
repo's `spec/`. See hht_diary's `spec/INDEX.md` for the existing range
and conventions.

Do NOT migrate REQ-d00179 / REQ-d00180 — they were claimed in a now-
superseded substrate spec on the `CUR-1192-actions-demo` branch and
have been retired. Phase I redrafts and claims its own numbers.

## Conventions inherited from hht_diary

- Branch naming: `feature/`, `fix/`, `release/`. Never commit to `main`.
- Commit messages: free-form (no enforced CUR/REQ format on commits).
- PR titles: include the upstream Linear ticket reference if applicable
  (`[CUR-NNNN]`); this repo doesn't yet have its own Linear team.
- Spec/INDEX maintenance via the `elspais` MCP if/when this repo gets
  its own elspais workspace; until then, treat REQ refs as plain text.

## What downstream consumers see

`hht_diary`'s `clinical_diary/pubspec.yaml` will pin this repo by `git:`
ref. During active Phase I/II development, devs override locally with
`pubspec_overrides.yaml` pointing at a sibling clone. That override is
gitignored in `hht_diary`. No other consumer depends directly on these
packages today.

## Reading the plans/specs

Three concentric scopes:

- **Library itself**: `docs/superpowers/specs/2026-04-22-events-and-actions-libs-design.md`
  + plan `2026-04-22-audited-actions-library.md`.
- **Permissions**: spec `2026-04-23-action-permissions-design.md`
  + plan `2026-05-06-action-permissions-library.md`.
- **Demo / e2e exercise**: spec `2026-05-06-action-permissions-demo-design.md`
  + plan `2026-05-06-action-permissions-demo.md`.

The Phase I substrate-and-materializer redesign spec hasn't been written
yet — when it lands it goes in `docs/superpowers/specs/`. That spec
re-anchors on the unified `subscribe<T>` API (not Phase 4.12's three
named methods).

## License

AGPLv3.
