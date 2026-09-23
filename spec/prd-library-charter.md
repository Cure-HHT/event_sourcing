# EVS-PRD-library-charter: Library Charter

**Level**: PRD | **Status**: Active | **Implements**: -

## Purpose

This library provides reactive, append-only event-sourcing primitives for Dart applications that require cryptographically tamper-evident audit trails. It is built primarily for FDA 21 CFR Part 11 / GxP-aligned clinical software, but is not coupled to that domain. It runs unchanged on mobile, server, and desktop targets, and composes into multi-tier deployments where the same code plays different roles (originator of events, relay between tiers, controller of canonical state) without forking.

The library is intentionally narrow. It supplies the substrate — event log, materializer, subscription, action dispatch, permissions-as-events. It does not supply application logic, schemas, transport, or persistence backends beyond reference implementations.

## Assertions

A. The library SHALL provide an append-only, tamper-evident, replayable audit trail of application state changes.

B. The library SHALL deliver event and materialized-state updates reactively to consumers.

C. The library SHALL provide authorization-checked action dispatch in which both the authorization decision and the resulting state change are recorded as events in the same audit log.

D. The library SHALL support configurable event flow between deployments — outbound via destinations, and inbound via an ingest path that preserves upstream event identity and authority.

E. The library SHALL be pure Dart, runnable on mobile, server, and desktop targets.

F. The library SHALL provide canonical-form serialization and provenance-chain tracking sufficient for audit data to remain byte-identical and traceable across tiers and across independent systems.

G. The library SHALL produce an audit trail aligned with FDA 21 CFR Part 11 and the ALCOA+ data-integrity attributes (Attributable, Legible, Contemporaneous, Original, Accurate; Complete, Consistent, Enduring, Available).

H. The library SHALL maintain an enumerable set of trust-boundary interfaces. Every input that participates in state derivation but is not itself derivable from the event log SHALL pass through one of these named, pluggable, app-registered interfaces, or be explicitly identified as an unaudited boundary that future work will close. No new external input may participate in state derivation without expanding this enumeration.

I. The library SHALL distinguish, in documentation and code, between substrate guarantees (cryptographic and structural facts about the event log) and library-provided conventions (default interpretations such as tombstone semantics, null-as-clear merge, originator-of-first-event canonicality). Applications MAY choose conventions; the substrate SHALL NOT impose them as substrate-level truths. Surfaces that ship a Layer-2 convention SHALL name it as one, both in their declared identifiers and in their reference documentation.

## Rationale

**Should you look at this library more closely?** If you are building a Dart application that must (a) produce an immutable audit trail of every state change, (b) reproduce its state exactly from that audit trail, (c) treat authorization as part of the same audit story rather than a separate IAM concern, (d) deploy across mobile clients and server tiers without maintaining parallel codebases, and (e) be defensible against a regulator who wants to verify the data-integrity story end-to-end — then yes.

**Why event sourcing, not a CRUD database with audit logs?** Regulated software demands an immutable audit trail of every state change — who changed what, when, in what order, with what authority. Event sourcing makes that audit trail the source of truth rather than a side-effect log. Replay reproduces history exactly; the audit and the state cannot drift.

**Why an in-library materializer instead of host-supplied callbacks?** Auditors review one codebase, not application code plus an external materialization service. Replay determinism is far easier to guarantee when the state-derivation rules are part of the substrate than when they are host-application hooks that may evolve incompatibly with the event log they read.

**Why closed-under-events trust?** A permission system that lives outside the event log can drift from what the audit trail records. Making permission grants, role assignments, and policy changes themselves events guarantees that "who could do what at time T" is reconstructible from the same log that records "what they did".

**Why role-based tiers from one codebase?** Multi-tier deployments historically use distinct codebases per tier, with cross-tier protocol churn at every change. A single substrate that plays different roles by configuration eliminates a class of cross-tier integration bugs and keeps the audit machinery uniform.

**Why companion libraries (`canonical_json_jcs`, `provenance`)?** Tamper-evidence requires byte-identical serialization across systems; provenance requires a structured chain travelling with each event. Both are concentrated in narrow, dependency-free, pure-Dart packages so that any other component can share the same canonical-form and provenance contracts without pulling the full event-sourcing stack.

**Why enumerate trust boundaries (assertion H)?** The library's central trust commitment is that state at any sequence is reconstructable from the event log under a known library version. That commitment only holds if the substrate doesn't quietly accept additional inputs that participate in state derivation. Enumerating the trust boundaries — a small, named set of pluggable interfaces (storage -- which, for a backend several processes or tabs share, includes serializing their boots and running the incompatible-generation guard, since a backend that does not guard lets builds of different majors write one database side by side, and for every backend includes excluding drainers through its drain lock, since a backend whose lock admits two drainers of one database lets both send and record outcomes --, outbound transport together with the delivery configuration the application registers with it: the destination's filter (a predicate closure included) and transform, its send outcomes, the retry policy and attempt budget, the clock the delivery cycle computes its window from, and the configuration version the delivery cycle is started with, each wedge decision being recorded in the log with the outcome category and the budget in effect, and each wedge and recovery with the declared configuration in effect and its fingerprint, while the clock's readings are not recorded, so its influence on which events are enqueued is an unaudited input, and the configuration version is taken on faith to change whenever code the library cannot read changes, which the log cannot show) plus any explicitly-acknowledged unaudited inputs (today: the caller-supplied `Principal`, pending future authentication-flow work; consumer or external writes to the library's persisted state, or of reserved system events into the log (the event store's public append operations refuse reserved entry types and ingest refuses one in a shape the library does not append, but a direct write to storage passes neither, and a consumer call to the library's internal reserved append operations passes only the analyzer's guard), which the storage boundary's precondition excludes but nothing at run time prevents; the deployment-supplied Postgres lock-session path, trusted to be one server session reaching the pool's server, database and schema, to carry keepalives and to let the lock role end its own sessions, which the library checks where it can when a backend opens, and on which both the generation locks and the drain lock are held; the browser's lock manager, trusted to grant a lock name exclusively or shared as requested, to report the held locks it is asked for, and to release every lock of a closed or discarded page; and a Sembast database file outside the browser, trusted to be opened by one isolate of one process, since two openers there see no lock of each other -- none of the last three is a pluggable interface) — makes the trust surface visible and review-gated. Any proposal to add a trusted input is then a charter-level architectural change, not a quiet API addition. The enumeration itself is maintained in `CLAUDE.md`'s "Trust boundaries" section; downstream PRDs that introduce or modify a boundary interface refine assertion H.

**Why distinguish facts from conventions (assertion I)?** The library's most-load-bearing claims — append-only ordering, hash-chain integrity, provenance attribution, transaction atomicity — are cryptographic or structural facts that the substrate makes true and detects tampering on. But the library also ships default *interpretations* of the event stream: tombstone-as-row-deletion, null-as-clear merge semantics, originator-of-first-event as canonical authority, one-row-per-aggregate materialization. These are useful conventions, not unique truths; an application processing the same event log under different conventions can produce a different (but equally valid) materialization. Conflating the two layers misleads consumers about what the substrate is actually claiming — and consumers who reasonably want a different materialization may believe they need to abandon the substrate rather than build on top of its facts with different conventions. Pinning the distinction explicitly preserves the substrate's epistemic honesty: it can be relied on for the facts; the conventions are defaults, not impositions. Practically, the library's closed-under-events guarantee is precisely scoped — *state under Layer-2 conventions* is reconstructable from the log — rather than overclaiming that the conventions themselves are substrate-mandated. The full discussion of the two layers, with examples and implications for other charter assertions, is in the "Epistemic layers" section below; it is the authoring discipline that surfaces shipping a Layer-2 convention must follow.

**Why no Ops-level requirements in this repo?** The library has no deployment, runtime monitoring, or operational surface of its own. Operational requirements *about* this library belong in the consuming application repos.

## Epistemic layers

This section is the canonical statement of the Layer 1 / Layer 2 distinction that assertion I obliges. The substrate makes two kinds of claims, and the distinction is load-bearing. Confusing them leads consumers to either over-trust the library's defaults or to abandon the substrate when they need a different interpretation than it ships.

**Layer 1 — Facts (objective, cryptographic / structural).** These are the substrate's hard guarantees. They are tamper-evident and absolute:

- The event at sequence N has hash H.
- The hash chain from genesis to N is intact.
- The provenance entries say the event passed through hops A -> B -> C with attribution to initiators I1, I2, I3 at times t1, t2, t3.
- The append of this event was atomic with its row writes inside the same transaction.
- Per-aggregate-per-Source order is preserved.

ALCOA+ alignment (assertion G) lives entirely at this layer. The cryptographic and structural facts are what regulators can be defended against.

**Layer 2 — Conventions (subjective, library-provided defaults).** These are the library's chosen *interpretations* of the event stream. They are useful defaults, not unique truths:

- A "tombstone" event type deletes the row (the substrate could equally preserve the row with a marker, or hide-not-delete).
- Missing keys in a delta preserve prior; present-null clears (the substrate could equally treat null as absent).
- Whoever appends the first event for an aggregate is the canonical authority for that aggregate (the substrate could equally require out-of-band canonicalization assignment).
- A projection produces one row per aggregate, materialized via generic merge (the substrate could equally produce per-event rows or derived-only views).
- "Version" is a major.minor pair per entry type (the substrate could equally use content-hash-as-version).

The library bundles these as primitives because most consumers want them, but they don't carry the same epistemic weight as Layer 1. Applications that want different interpretations build them on top of Layer 1 facts — by subscribing to raw events and computing app-side state, or by registering alternative convention sets shipped as new library primitives under the Append-Only Primitives discipline.

**Implications for other charter assertions.**

- The **closed-under-events trust model** (assertion C, refined by the permissions-as-events PRD) is precisely scoped: state *under the Layer 2 conventions* is reconstructable from the event log under a known library version. It does not claim the conventions are universally correct.
- The **declarative projection model** (refining assertion A) ships one Layer 2 materialization per registered `ProjectionSpec`. Applications needing different materializations build them on top of `subscribe<T>(_, Events())` or `EventStore.read(...)` — and that is an expected, supported pattern, not a fallback.
- **Append-Only Primitives discipline** applies to Layer 2 conventions too. Once a convention ships under a name with given semantics, those semantics are frozen; alternative behaviour is a new primitive, not a re-interpretation of an existing one.

**Authoring guidance.** When proposing new library primitives, code comments, or PRD assertions, be explicit about which layer the claim sits at. "The library SHALL preserve hash-chain integrity" is Layer 1 and absolute. "The library SHALL treat tombstone event types as row deletions" is Layer 2 and should read more like "The library's default `AggregateProjectionSpec` interpretation TREATS event types in `tombstoneEventTypes` as row deletions." Existing and new surfaces alike are held to this precision under the authoring discipline that assertion I imposes.

## Changelog

- 2026-09-23 | 0021ec08 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of H names drainer exclusion under the storage boundary, the drain lock on the Postgres lock session, and the configuration version under the delivery configuration
- 2026-09-23 | 0021ec08 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | H Rationale: reserved system events written directly into the log, or appended by a consumer through the internal reserved appends, join the unaudited writes the storage precondition excludes
- 2026-09-23 | 0021ec08 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Epistemic layers: a version is a major.minor pair per entry type
- 2026-09-23 | 0021ec08 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of H names the delivery configuration registered with a destination as part of the outbound transport boundary
- 2026-08-10 | 0021ec08 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 6b89020b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Library Charter* | **Hash**: 0021ec08
