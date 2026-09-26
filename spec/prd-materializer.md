# EVS-PRD-materializer: Materializer

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The materializer is the library's component that derives typed application state from the event log. Application code consumes materialized state to do its work; auditors and tests reproduce the same state by running the same materializer over the same log. The materializer is part of the library's substrate — it lives alongside the storage and subscription primitives, not in host application code.

## Assertions

A. The library SHALL provide a materializer that derives typed application state from the event log.

B. Materialization SHALL be deterministic: applying the same events in the same order from the same starting state SHALL yield byte-identical resulting state.

C. The materializer's rules SHALL themselves be events recorded in the log; the rules in effect at any point in the log SHALL be reconstructable from the log alone.

D. The library's default views SHALL TREAT as having an outstanding security finding every aggregate named in the `aggregates` of a security finding the holder holds as authored, and every aggregate so named by a security finding the holder received whose events were authored by that finding's originating database or by a database in its succession lineage; and SHALL fold that aggregate's events as they fold any aggregate's.

E. The library's default views SHALL TREAT as having an outstanding security finding every aggregate of which the holder holds an event of a database X at or above the origin position a held finding of kind `position_reused` about X names, or at or above the lowest origin position among the held events of X that carry the `previous_event_hash` a held finding of kind `fork_unrecorded` about X names, counting only findings the holder holds as authored and received findings whose originating database is X or a database whose succession lineage includes X.

F. Every row of a default view SHALL carry the reserved key `$integrity`, an object with exactly `security_findings`: the `finding_id`s, in ascending order, of the held findings that mark the row's aggregate or, for a Table-shape row, an aggregate whose event produces its key; an empty list when there is none.

G. Every copy of a default view SHALL fold each security finding event, whatever the view's interest, into the outstanding-finding marks of the rows the finding marks, in log order with the events the copy folds.

H. The library SHALL refuse, by name and before any write, an append whose data holds a top-level key beginning with `$` and the registration of a projection whose key, column or derived field name begins with `$`, and SHALL store no event, naming the reason, for a record whose data holds such a key that ingest or a restore receives.

## Rationale

**Why typed state, not row-maps or untyped JSON?** Application code relies on the materialized state for its work. Typed state lets the compiler catch shape mismatches between the materializer and its consumers; untyped state pushes those errors to runtime, where they are far harder to catch in regulated environments.

**Why deterministic?** Two audits of the same log must produce the same answer; otherwise the audit has no evidentiary value. Determinism is the property that makes "the materialized state derived from this log" an unambiguous statement rather than a per-run artifact.

**Why are materializer rules themselves events?** A rule that lives outside the log can change without any record. If the rule that maps event-X to state-change-Y can be edited silently, the meaning of every past event-X also changes silently. By recording rule changes as events in the same log, the library guarantees that "what the materializer did at time T" is reconstructable from the same audit trail as "what the application did at time T". The rule history is part of the audit, not adjacent to it.

**Why is the materializer in the library, not the host application?** Two reasons. First, the same library running an audit, a replay, or a fresh-rebuild must use exactly the materializer that produced the original state — host code that has evolved would derive different state. Second, regulators reviewing the audit story should review one component, not application code plus a parallel materialization service.

**Multi-source readiness.** The rules-as-events seam is what admits multi-source canonicalization later (see EVS-PRD-multi-source-canonicalization). The default rule preserves single-source semantics — only events from the aggregate's originating authority fold into state. Multi-source semantics are activated by additional rules expressed over event authorities, recorded as events on the same log; the materializer code path is identical in either case.

**Outstanding findings (assertions D to H).** A finding says the holder stored something it could not verify, not that the data is false. Withholding the aggregate would hide the record a person needs to judge, and choosing a state would decide on their behalf; folding it and marking it keeps the view truthful about both. A fork's branches reach aggregates its evidence does not name, and a holder whose channel filtered events cannot trace which branch an event is on, so the default views mark every aggregate with an event of the forked database at or above the fork's lowest position: they may mark an aggregate only one branch touched, and, wherever the holder recorded or received a fork or reuse finding, never leave one that both branches touched unmarked. A filter can hide every fork and reuse from a holder, and then nothing is marked there. Findings are reserved events, outside most views' interest, but the mark governs rows of every view, so every copy folds each finding in log order, inline while the copy is current and during its catch-up otherwise (EVS-DEV-view-convergence). The mark lasts while the finding is held; clearing a finding is a separate event (`spec/roadmap/security-findings.md`). A finding the holder received is another database's statement, so it marks only what that database, or its succession lineage, authored: a sender cannot mark another database's data at its receivers by stating a finding about it. The rules read only the log, so materialization stays deterministic and a rebuild derives the marks the incremental fold did. The `$` prefix is reserved so an application key never collides with the mark. The finding is a Layer 1 fact; the mark is a Layer 2 convention of the default views.

## Future work

Deferred projection primitives (a `TimeBucketProjectionSpec` for
time-bucketed aggregation, and its open design questions) are recorded
in `spec/roadmap/projections.md`.

## Changelog

- 2026-09-26 | 06c5d8a4 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | E: a received finding counts when its originating database is X or has X in its succession lineage, as for D. Rationale: a filter can hide every fork and reuse from a holder. No code or test references E
- 2026-09-25 | a46d7da0 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | ae58310e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | D and E: a received finding marks only aggregates and events its originating database or that database's succession lineage authored; E reads the fork's lowest position from the held events that carry the predecessor hash a `fork_unrecorded` finding names, and the reused position from a `position_reused` finding, since findings carry fixed coordinates only. No code or test references D or E
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Add D-H: the default views fold and mark every aggregate a held security finding names, and every aggregate with an event of a forked database at or above the fork's lowest position; every row carries the marking findings under the reserved `$integrity` key; every copy folds each finding whatever its interest; the `$` namespace is reserved. Rationale: no branch conflicts or reconciliation. No code or test references D-H
- 2026-09-25 | 88f90336 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: a reconciliation is the aggregate's whole state; no possibly-incomplete rows
- 2026-09-25 | 88f90336 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: branch conflicts are part of the deterministic materialization
- 2026-08-10 | 88f90336 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 02028dcf | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Materializer* | **Hash**: 06c5d8a4
