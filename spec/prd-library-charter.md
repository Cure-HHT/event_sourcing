# EVS-prd-library-charter: Library Charter

**Level**: prd | **Status**: Draft | **Refines**: -

## Purpose

This library provides reactive, append-only event-sourcing primitives for Dart applications that require cryptographically tamper-evident audit trails. It is built primarily for FDA 21 CFR Part 11 / GxP-aligned clinical software, but is not coupled to that domain. It runs unchanged on mobile, server, and desktop targets, and composes into multi-tier deployments where the same code plays different roles (originator of events, relay between tiers, controller of canonical state) without forking.

The library is intentionally narrow. It supplies the substrate — event log, materializer, subscription, action dispatch, permissions-as-events. It does not supply application logic, schemas, transport, or persistence backends beyond reference implementations.

## Assertions

A. The library SHALL provide an append-only, tamper-evident, replayable audit trail of application state changes.

B. The library SHALL deliver event and materialized-state updates reactively to consumers.

C. The library SHALL provide authorization-checked action dispatch in which both the authorization decision and the resulting state change are recorded as events in the same audit log.

D. The library SHALL support multi-tier deployment from a single codebase, with each tier (originator, relay, controller) configured at composition time.

E. The library SHALL be pure Dart, runnable on mobile, server, and desktop targets.

F. The library SHALL provide canonical-form serialization and provenance-chain tracking sufficient for audit data to remain byte-identical and traceable across tiers and across independent systems.

G. The library SHALL produce an audit trail aligned with FDA 21 CFR Part 11 and the ALCOA+ data-integrity attributes (Attributable, Legible, Contemporaneous, Original, Accurate; Complete, Consistent, Enduring, Available).

## Rationale

**Should you look at this library more closely?** If you are building a Dart application that must (a) produce an immutable audit trail of every state change, (b) reproduce its state exactly from that audit trail, (c) treat authorization as part of the same audit story rather than a separate IAM concern, (d) deploy across mobile clients and server tiers without maintaining parallel codebases, and (e) be defensible against a regulator who wants to verify the data-integrity story end-to-end — then yes.

**Why event sourcing, not a CRUD database with audit logs?** Regulated software demands an immutable audit trail of every state change — who changed what, when, in what order, with what authority. Event sourcing makes that audit trail the source of truth rather than a side-effect log. Replay reproduces history exactly; the audit and the state cannot drift.

**Why an in-library materializer instead of host-supplied callbacks?** Auditors review one codebase, not application code plus an external materialization service. Replay determinism is far easier to guarantee when the state-derivation rules are part of the substrate than when they are host-application hooks that may evolve incompatibly with the event log they read.

**Why closed-under-events trust?** A permission system that lives outside the event log can drift from what the audit trail records. Making permission grants, role assignments, and policy changes themselves events guarantees that "who could do what at time T" is reconstructible from the same log that records "what they did".

**Why role-based tiers from one codebase?** Multi-tier deployments historically use distinct codebases per tier, with cross-tier protocol churn at every change. A single substrate that plays different roles by configuration eliminates a class of cross-tier integration bugs and keeps the audit machinery uniform.

**Why companion libraries (`canonical_json_jcs`, `provenance`)?** Tamper-evidence requires byte-identical serialization across systems; provenance requires a structured chain travelling with each event. Both are concentrated in narrow, dependency-free, pure-Dart packages so that any other Cure-HHT component can share the same canonical-form and provenance contracts without pulling the full event-sourcing stack.

**Why no Ops-level requirements in this repo?** The library has no deployment, runtime monitoring, or operational surface of its own. Operational requirements *about* this library belong in the consuming application repos.

## Refinement

This requirement is the top of the PRD hierarchy in this repo. Each headline assertion is refined by one or more downstream PRDs that make the obligation precise. The refining PRDs are introduced as the library's surface area is authored; expect roughly the following decomposition:

- **A** (append-only, tamper-evident, replayable) — refined by PRDs covering event-log structure, hash-chain integrity, and deterministic materialization.
- **B** (reactive delivery) — refined by the subscription-API PRD.
- **C** (authorization-checked dispatch) — refined by the action-dispatch and permissions-as-events PRDs.
- **D** (multi-tier deployment) — refined by the role-based composition PRD.
- **E** (pure Dart) — refined by the portability PRD.
- **F** (canonical form + provenance) — refined by the canonical-JSON and provenance PRDs (each a distinct package's charter).
- **G** (regulatory alignment) — refined by the audit-trail PRD that maps each ALCOA+ attribute to a specific library obligation.

This refinement section is non-normative and exists to orient readers; the binding obligations are the assertions above.

*End* *Library Charter* | **Hash**: 00000000
