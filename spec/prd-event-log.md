# EVS-PRD-event-log: Event Log

**Level**: prd | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The event log is the library's persistent record of every state-changing fact the application has produced. It is append-only, ordered, and immutable. Application state derives from it by replay through a materializer (see EVS-PRD-materializer); audit, debugging, and cross-tier reconciliation read from it directly.

This PRD specifies the structural and ordering properties of the log. Tamper-evidence — the cryptographic property that detects modification of stored events — is specified separately (see EVS-PRD-hash-chain-integrity).

## Assertions

A. The library SHALL persist events in an append-only, immutable log.

B. The library SHALL preserve a stable total order over all stored events such that the relative position of any two events remains fixed for the lifetime of the log.

C. The library SHALL preserve per-aggregate-per-authority order: events for the same aggregate from the same authority appear in the order they were written by that authority.

D. The library SHALL allow consumers to read events from the log in order, from any specified starting position.

## Rationale

**Why append-only?** Audit and regulatory regimes demand that the history of state changes be reconstructable. Mutable storage breaks that reconstructability — once an event can be edited, the relationship between recorded history and reality becomes a matter of trust rather than evidence. Append-only eliminates that trust requirement at the storage layer.

**Why total ordering?** A consumer reading the log needs to answer "did A happen before B?" unambiguously. Without a total order, partial-order ambiguity propagates into every materialized view and every audit query.

**Why per-aggregate ordering?** Aggregates are the unit of consistency for state derivation. Folding events out of order would corrupt the materialized state, even when the global order is otherwise sound. Pinning per-aggregate order independently preserves correctness regardless of how the global order is implemented.

**Why replay from any position?** Two consumers of the same log have different needs: a fresh materializer rebuilds state from the start, an incremental subscriber resumes from its last-processed position, an auditor inspects a specific window. All three reduce to the same primitive: read in order from any starting point.

**Per-aggregate ordering under multi-source.** When events for a single aggregate originate from more than one authority — a participant on phone and tablet, a coordinator editing a participant's entry — each authority's contributions retain their write order within the aggregate. Cross-authority resolution for the aggregate is handled by the canonicalization rules in EVS-PRD-multi-source-canonicalization, not by the log's ordering primitives. The log preserves order; canonicalization decides which ordered events become canonical.

*End* *Event Log* | **Hash**: da8f0f0b
