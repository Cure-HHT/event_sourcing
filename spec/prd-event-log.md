# EVS-PRD-event-log: Event Log

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The event log is the library's persistent record of every state-changing fact the application has produced. It is append-only, ordered, and immutable. Application state derives from it by replay through a materializer (see EVS-PRD-materializer); audit, debugging, and cross-tier reconciliation read from it directly.

This PRD specifies the structural and ordering properties of the log. Tamper-evidence — the cryptographic property that detects modification of stored events — is specified separately (see EVS-PRD-hash-chain-integrity).

## Assertions

A. The library SHALL persist events in an append-only, immutable log.

B. The library SHALL preserve a stable total order over all stored events such that the relative position of any two events remains fixed for the lifetime of the log.

C. The library SHALL preserve per-aggregate-per-authority order: events for the same aggregate from the same authority appear in the order they were written by that authority.

D. The library SHALL allow consumers to read events from the log in order, from any specified starting position.

E. The library SHALL make append progress under concurrent writers: when the storage layer aborts a write because of a transient serialization or deadlock conflict, the library SHALL re-run the write to completion rather than surface the transient conflict to the caller, up to a bounded number of attempts after which the failure is surfaced.

## Rationale

**Why append-only?** Audit and regulatory regimes demand that the history of state changes be reconstructable. Mutable storage breaks that reconstructability — once an event can be edited, the relationship between recorded history and reality becomes a matter of trust rather than evidence. Append-only eliminates that trust requirement at the storage layer.

**Why total ordering?** A consumer reading the log needs to answer "did A happen before B?" unambiguously. Without a total order, partial-order ambiguity propagates into every materialized view and every audit query.

**Why per-aggregate ordering?** Aggregates are the unit of consistency for state derivation. Folding events out of order would corrupt the materialized state, even when the global order is otherwise sound. Pinning per-aggregate order independently preserves correctness regardless of how the global order is implemented.

**Why replay from any position?** Two consumers of the same log have different needs: a fresh materializer rebuilds state from the start, an incremental subscriber resumes from its last-processed position, an auditor inspects a specific window. All three reduce to the same primitive: read in order from any starting point.

**Why retry transient conflicts instead of exposing them?** A storage backend that serializes concurrent writers (for example, one using a strict isolation level to keep the global order and the hash-chain tip consistent) signals an unresolvable concurrent access by aborting the loser with a transient "try again" error. That error is a normal, expected outcome of contention, not a defect — the documented remedy is simply to re-run the aborted work. Re-running is safe because every step of an append (order reservation, hash-tip read, insert, and derived-view writes) happens inside the aborted, rolled-back transaction, so a retry re-derives all of it from the latest committed state. Surfacing the transient error to application code instead would force every caller — including internal reactors — to re-implement the same retry, and a single unguarded caller would turn routine contention into a crash. Bounding the attempts keeps pathological contention from spinning forever; once the bound is reached the failure is surfaced honestly.

**Per-aggregate ordering under multi-source.** When events for a single aggregate originate from more than one authority — a participant on phone and tablet, a coordinator editing a participant's entry — each authority's contributions retain their write order within the aggregate. Cross-authority resolution for the aggregate is handled by the canonicalization rules in EVS-PRD-multi-source-canonicalization, not by the log's ordering primitives. The log preserves order; canonicalization decides which ordered events become canonical.

## Changelog

- 2026-07-02 | e710dcce | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Event Log* | **Hash**: e710dcce
