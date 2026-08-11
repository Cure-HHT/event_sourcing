# EVS-PRD-multi-source-canonicalization: Multi-Source Canonicalization

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

When multiple event-sourcing deployments contribute events to the same aggregate — a participant editing on phone and tablet, a coordinator editing a participant's diary entry, a reverse-proxy collecting from many upstreams — the library determines which of those events are canonical for the aggregate. Canonicalization is governed by configurable rules expressed over event authorities, not over connection identities. The default rule preserves single-source semantics; multi-source semantics are activated by adding rules that admit additional authorities.

This PRD specifies the rule grammar at PRD level. Specific authority schemes (per-user, per-role, per-coordinator) are configured per deployment via settings events on the same log.

## Assertions

A. Each event SHALL carry an authority identifier recording which principal produced it.

B. The library SHALL determine which events are canonical for an aggregate via configurable canonicalization rules expressed over event authorities.

C. The default canonicalization rule for an aggregate SHALL be: events from the authority that produced the aggregate's first event are canonical; events from other authorities are non-canonical until a rule grants them authority.

D. Canonicalization rules SHALL themselves be events recorded in the same log; the rules in effect for an aggregate at any point SHALL be reconstructable from the log alone.

E. The library SHALL support an approval pattern: an event from an authority with rule-granted approval power SHALL admit an otherwise-non-canonical edit-event from a different authority, making it canonical.

F. A non-canonical event SHALL remain visible in the log and in subscriptions; the materializer SHALL fold only canonical events into typed state, but consumers MAY observe non-canonical events through subscriptions that opt into them.

## Rationale

**Why canonicalize over authority rather than over Source connection?** Two reasons. First, the same authority may submit events through multiple Source connections — a user from phone and tablet is the same user. Tying canonical status to Source identity rather than authority would force the library to treat distinct devices of one user as distinct authorities, which contradicts the user's intuition. Second, authority is per-event metadata; Source identity is per-connection. Per-event metadata is the right granularity for per-event canonicalization.

**Why is the default "originator authority is canonical"?** Single-source-per-aggregate is the safe default. A new aggregate's first event establishes who produced it; in the absence of any explicit rule admitting other authorities, only events from that originator are canonical. This guarantees that a deployment with no multi-source configuration behaves exactly like a single-source deployment — multi-source machinery is dormant until activated by rules.

**Why are canonicalization rules events?** A rule that lives outside the log can change without record. If the rule that governs which events become canonical for an aggregate can be edited silently, the meaning of the audit log changes silently. By recording rule changes as events, the library guarantees that "what counted as canonical at time T" is reconstructable from the same evidence base as "what events existed at time T". This parallels the rationale in EVS-PRD-materializer for materializer-rules-as-events.

**Why an explicit approval pattern?** Two distinct multi-editor scenarios reduce to the same primitive. (1) A participant edits the same diary entry on phone and tablet — both events are from the same authority (the participant), and the participant's own canonicalization rule auto-approves both. (2) A coordinator edits a participant's diary entry — the edit is from a different authority and is non-canonical until the participant submits an approval event. The approval pattern captures both: scenario (1) is the degenerate case where the approver and the editor are the same authority and the approval is implicit; scenario (2) is the explicit case where they differ and an approval event is required. One mechanism, two configurations.

**Why are non-canonical events still recorded?** Audit-completeness. A canonicalization rule that hides non-canonical events would let edits disappear from history if they failed canonicalization — making "did the coordinator attempt this edit?" un-answerable. Recording all events while canonicalizing only some keeps the audit trail complete and lets approval-pattern UIs (e.g., "the participant has 3 pending coordinator edits") read non-canonical events directly.

**Why is this in the substrate, not the application?** The library is the only component with the necessary visibility — it sees every event, it runs the materializer, it evaluates the rules. Pushing canonicalization to the application would require every consumer to reimplement multi-source resolution, with all the audit-divergence risks that entails. Concentrating it in the substrate keeps the audit story uniform across consumers.

## Status

This PRD pins the rule grammar (assertions A–F). The substrate retains
the per-event authority-identity and ordering seams the rules would
evaluate, but the canonicalization layer itself is unbuilt: no rule
events are emitted or interpreted, and the single-source-per-aggregate-type
invariant holds (the `single-source-per-aggregate-type today`
architectural commitment in CLAUDE.md is in force). Building the
canonicalization layer — and the scenarios it unlocks
(`docs/scenarios/supply-chain.md`, `iot-sensor-network.md`,
`retail-pos.md`, whose cross-authority consolidations single-source
deployments cannot serve end-to-end) — is recorded in
`spec/roadmap/multi-source-editing.md`.

## Changelog

- 2026-08-10 | ccf88a3b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 3e087d41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Multi-Source Canonicalization* | **Hash**: ccf88a3b
