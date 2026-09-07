# EVS-PRD-materializer: Materializer

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The materializer is the library's component that derives typed application state from the event log. Application code consumes materialized state to do its work; auditors and tests reproduce the same state by running the same materializer over the same log. The materializer is part of the library's substrate — it lives alongside the storage and subscription primitives, not in host application code.

## Assertions

A. The library SHALL provide a materializer that derives typed application state from the event log.

B. Materialization SHALL be deterministic: applying the same events in the same order from the same starting state SHALL yield byte-identical resulting state.

C. The materializer's rules SHALL themselves be events recorded in the log; the rules in effect at any point in the log SHALL be reconstructable from the log alone.

D. Where an entry type does not declare its materialization behaviour, the library SHALL materialize events of that entry type.

## Rationale

**Why typed state, not row-maps or untyped JSON?** Application code relies on the materialized state for its work. Typed state lets the compiler catch shape mismatches between the materializer and its consumers; untyped state pushes those errors to runtime, where they are far harder to catch in regulated environments.

**Why deterministic?** Two audits of the same log must produce the same answer; otherwise the audit has no evidentiary value. Determinism is the property that makes "the materialized state derived from this log" an unambiguous statement rather than a per-run artifact.

**Why are materializer rules themselves events?** A rule that lives outside the log can change without any record. If the rule that maps event-X to state-change-Y can be edited silently, the meaning of every past event-X also changes silently. By recording rule changes as events in the same log, the library guarantees that "what the materializer did at time T" is reconstructable from the same audit trail as "what the application did at time T". The rule history is part of the audit, not adjacent to it.

**Why is the materializer in the library, not the host application?** Two reasons. First, the same library running an audit, a replay, or a fresh-rebuild must use exactly the materializer that produced the original state — host code that has evolved would derive different state. Second, regulators reviewing the audit story should review one component, not application code plus a parallel materialization service.

**Why is materialization opt-out rather than opt-in?** An entry type exists because an application wants to record something, and in the ordinary case it wants to read that something back as state. Making materialization the default means an application declares only the exception. The exception is real and narrow: the substrate's own reserved audit types must land in the log as immutable rows while writing no view state, and they say so explicitly. Inverting the default would move the burden onto every application entry type, and a forgotten declaration would fail silently — the log would be correct while the views stayed empty, which is the hardest class of defect to notice. Whether a given entry type materializes is a Layer 2 convention, chosen because most consumers want it; an application that wants different behaviour declares it per entry type.

**Multi-source readiness.** The rules-as-events seam is what admits multi-source canonicalization later (see EVS-PRD-multi-source-canonicalization). The default rule preserves single-source semantics — only events from the aggregate's originating authority fold into state. Multi-source semantics are activated by additional rules expressed over event authorities, recorded as events on the same log; the materializer code path is identical in either case.

## Future work

Deferred projection primitives (a `TimeBucketProjectionSpec` for
time-bucketed aggregation, and its open design questions) are recorded
in `spec/roadmap/projections.md`.

## Changelog

- 2026-09-07 | 86e14cfa | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-07 | - | - | Michael Lewis (<michael@anspar.org>) | Add D: an entry type that does not declare its materialization behaviour is materialized
- 2026-08-10 | 88f90336 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 02028dcf | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Materializer* | **Hash**: 86e14cfa
