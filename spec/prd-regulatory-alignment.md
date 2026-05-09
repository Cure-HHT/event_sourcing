# EVS-prd-regulatory-alignment: Regulatory Alignment

**Level**: prd | **Status**: Draft | **Refines**: EVS-prd-library-charter

## Purpose

The library is designed for use in software that must defend an audit trail to a regulator. It produces an event log and a materialized state aligned with FDA 21 CFR Part 11 and the ALCOA+ data-integrity attributes. Most of the alignment is provided by the obligations specified in other PRDs in this repo — this PRD adds the remaining obligations not covered elsewhere, and provides a single explicit cross-walk from each ALCOA+ attribute to the library obligation that satisfies it.

This PRD does not assert that any particular consuming application is FDA-compliant; that depends on how the consumer uses the library, on the consumer's own SOPs, and on the deployment environment. What this PRD asserts is that the library, used correctly, supplies the substrate properties a regulated consumer needs.

## Assertions

A. Every event SHALL record the time at which the originating action occurred.

B. Hash-chain mismatch encountered during normal operation SHALL be surfaced as an integrity violation.

C. Canonicalization-rule conflict encountered during normal operation SHALL be surfaced as an integrity violation.

D. Provenance verification failure encountered during normal operation SHALL be surfaced as an integrity violation.

## Rationale

### ALCOA+ cross-walk

| Attribute | What it requires | Where the library satisfies it |
| --- | --- | --- |
| **A**ttributable | Every record identifies who produced it. | EVS-prd-multi-source-canonicalization A — every event carries an authority identifier; EVS-prd-permissions-as-events — authority is event-defined. |
| **L**egible | Records are readable by humans and machines. | EVS-prd-canonical-json — events serialize to canonical JSON, decipherable without library-specific tooling. |
| **C**ontemporaneous | Records reflect the time of the event. | This PRD, A — every event records the originating action's timestamp. |
| **O**riginal | The audit log is the unaltered original record. | EVS-prd-event-log A — append-only and immutable; EVS-prd-hash-chain-integrity — modifications detectable. |
| **A**ccurate | Records reflect what actually happened. | EVS-prd-action-dispatch B — validation precedes recording; EVS-prd-action-dispatch C — denial events record what was rejected and why. |
| **C**omplete | Nothing is omitted from the audit trail. | EVS-prd-action-dispatch C — every dispatched action produces a recorded outcome (success or denial); EVS-prd-event-log A — events are never deleted. |
| **C**onsistent | Records are in correct order. | EVS-prd-event-log B+C — total order across the log; per-aggregate order preserved. |
| **E**nduring | Records survive over time. | EVS-prd-event-log A — append-only persistence; EVS-prd-destinations D — durable destination queues; EVS-prd-portability D — pluggable storage allows long-term retention configurations. |
| **A**vailable | Records can be retrieved when needed. | EVS-prd-event-log D — read events from any starting position; EVS-prd-subscription — reactive delivery and filtered access; EVS-prd-materializer A — typed state derivation on demand. |

### FDA 21 CFR Part 11 cross-walk

The relevant clauses for the library's substrate role:

- **§11.10(b) Records readable** — covered by EVS-prd-canonical-json (canonical JSON is human-decipherable).
- **§11.10(c) Protection of records** — covered by EVS-prd-event-log A (append-only) + EVS-prd-hash-chain-integrity (tamper-evidence) + this PRD, B–D (integrity-violation surfacing).
- **§11.10(e) Audit trails (computer-generated, time-stamped)** — covered by EVS-prd-event-log + EVS-prd-action-dispatch C + this PRD, A (timestamps).
- **§11.10(f) Operational system checks** — covered by this PRD, B–D (the three classes of integrity violation are surfaced).
- **§11.10(g) Authority checks** — covered by EVS-prd-action-dispatch B + EVS-prd-permissions-as-events.

Other 21 CFR Part 11 clauses (validation of systems §11.10(a), system documentation §11.10(k), persons qualified §11.10(i)) are process and SOP obligations on the consuming organization, not library obligations.

### Why timestamps are this PRD's obligation, not event-log's

Timestamps are not strictly necessary for an event log to be a valid event log — order alone makes events sortable. But "the time the action occurred" is independently load-bearing for ALCOA+ Contemporaneous and for FDA 21 CFR Part 11 §11.10(e). Anchoring the obligation in the regulatory PRD makes the regulatory motivation explicit and lets a non-regulated consumer of the library opt out of the timestamping policy — although in practice no consumer would.

### Why integrity-violation surfacing is this PRD's obligation, not hash-chain-integrity's

EVS-prd-hash-chain-integrity defines the verification operation: a holder of the log can recompute and verify. This PRD adds the operational-system-check obligation: the library surfaces verification failures encountered during normal use, rather than absorbing them. Together, these satisfy §11.10(c) and §11.10(f); separately, neither does. The three classes of failure are split into separate assertions (B, C, D) because each is independently testable: hash-chain mismatch is detected via the chain-verification path; canonicalization-rule conflict is detected by the rule-evaluation path; provenance verification failure is detected at the ingest boundary.

*End* *Regulatory Alignment* | **Hash**: 00000000
