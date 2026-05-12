# EVS-PRD-library-charter: Library Charter

**Level**: PRD | **Status**: Draft | **Refines**: -

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

**Why companion libraries (`canonical_json_jcs`, `provenance`)?** Tamper-evidence requires byte-identical serialization across systems; provenance requires a structured chain travelling with each event. Both are concentrated in narrow, dependency-free, pure-Dart packages so that any other Cure-HHT component can share the same canonical-form and provenance contracts without pulling the full event-sourcing stack.

**Why enumerate trust boundaries (assertion H)?** The library's central trust commitment is that state at any sequence is reconstructable from the event log under a known library version. That commitment only holds if the substrate doesn't quietly accept additional inputs that participate in state derivation. Enumerating the trust boundaries — a small, named set of pluggable interfaces (storage, outbound transport) plus any explicitly-acknowledged unaudited inputs (today: the caller-supplied `Principal`, pending future authentication-flow work) — makes the trust surface visible and review-gated. Any proposal to add a fourth trusted input is then a charter-level architectural change, not a quiet API addition. The enumeration itself is maintained in `CLAUDE.md`'s "Trust boundaries" section; downstream PRDs that introduce or modify a boundary interface refine assertion H.

**Why distinguish facts from conventions (assertion I)?** The library's most-load-bearing claims — append-only ordering, hash-chain integrity, provenance attribution, transaction atomicity — are cryptographic or structural facts that the substrate makes true and detects tampering on. But the library also ships default *interpretations* of the event stream: tombstone-as-row-deletion, null-as-clear merge semantics, originator-of-first-event as canonical authority, one-row-per-aggregate materialization. These are useful conventions, not unique truths; an application processing the same event log under different conventions can produce a different (but equally valid) materialization. Conflating the two layers misleads consumers about what the substrate is actually claiming — and consumers who reasonably want a different materialization may believe they need to abandon the substrate rather than build on top of its facts with different conventions. Pinning the distinction explicitly preserves the substrate's epistemic honesty: it can be relied on for the facts; the conventions are defaults, not impositions. Practically, the library's closed-under-events guarantee is precisely scoped — *state under Layer-2 conventions* is reconstructable from the log — rather than overclaiming that the conventions themselves are substrate-mandated. The layer split is maintained in `CLAUDE.md`'s "Epistemic layers" section and is the authoring discipline that surfaces shipping a Layer-2 convention must follow.

**Why no Ops-level requirements in this repo?** The library has no deployment, runtime monitoring, or operational surface of its own. Operational requirements *about* this library belong in the consuming application repos.

## Refinement

This requirement is the top of the PRD hierarchy in this repo. Each headline assertion is refined by one or more downstream PRDs that make the obligation precise. The refining PRDs are introduced as the library's surface area is authored; expect roughly the following decomposition:

- **A** (append-only, tamper-evident, replayable) — refined by PRDs covering event-log structure, hash-chain integrity, deterministic materialization, and multi-source canonicalization.
- **B** (reactive delivery) — refined by the subscription-API PRD.
- **C** (authorization-checked dispatch) — refined by the action-dispatch and permissions-as-events PRDs.
- **D** (event flow) — refined by the destinations PRD (outbound) and the ingest PRD (inbound daisy-chain).
- **E** (pure Dart) — refined by the portability PRD.
- **F** (canonical form + provenance) — refined by the canonical-JSON and provenance PRDs (each a distinct package's charter).
- **G** (regulatory alignment) — refined by the regulatory-alignment PRD that maps each ALCOA+ attribute to a specific library obligation.
- **H** (enumerable trust boundaries) — refined by the storage-backend interface (within event-log PRD), the destinations PRD (outbound transport boundary), and the permissions-as-events PRD (which closes the policy-evaluation boundary into the log). The third currently-trusted input — caller-supplied `Principal` accepted on faith — is documented in `CLAUDE.md`'s "Trust boundaries" section as a known incomplete boundary; closing it (an inbound authentication-attempt event flow plus an outbound `AuthenticationProvider` interface) is future work that will warrant its own PRD when authored.
- **I** (facts vs conventions) — cross-cutting authoring discipline; not refined by a single downstream PRD. Realized in `CLAUDE.md`'s "Epistemic layers" section and surfaced in every PRD that ships a Layer-2 convention (e.g., the materializer PRD's tombstone semantics, the canonical-JSON PRD's merge semantics) by naming the convention as one. Downstream PRDs that propose a new substrate-level *fact* MUST justify why it belongs at Layer 1; those that ship a new convention SHOULD name it explicitly as such.

This refinement section is non-normative and exists to orient readers; the binding obligations are the assertions above.

*End* *Library Charter* | **Hash**: 00000000
