# EVS-DEV-ingest-promotes-before-fold: Ingest-time promoter chain

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-ingest, EVS-PRD-materializer

## Purpose

How the substrate transparently lifts ingested events whose `entryTypeVersion` lags the registered version of the entry type. The `ProjectionInterpreter` consults the per-view promoter chain on each event before dispatching to the fold. The original event remains untouched in the log; the promoter operates on an in-memory `StoredEvent.withData(...)` copy. This keeps the closed-under-events trust model intact (hash-chain integrity preserved) while ensuring materialized views always fold over the up-to-date shape.

## Assertions

A. The `ProjectionInterpreter` SHALL apply the per-view promoter chain to any event whose `entryTypeVersion` is below the current `registeredVersion` of its entry type, BEFORE dispatching to the projection's fold.

B. Promotion SHALL operate on an in-memory `event.withData(newData)` copy. The original `StoredEvent` recorded in the log SHALL NOT be modified.

C. Two views that match the same entry type MAY register different promoter chains and produce different fold inputs from the same source event. The substrate SHALL NOT enforce chain-equality across views matching the same entry type.

D. When an event's `entryTypeVersion` equals the registered version, the substrate SHALL bypass the promoter chain entirely (no-op call elision).

## Rationale

**Why in-memory promotion rather than rewriting the event?** Rewriting would break hash-chain integrity (a Layer 1 substrate fact, per EVS-PRD-hash-chain-integrity and the charter's Assertion A). The in-memory copy mechanism preserves the chain while delivering up-to-date data to the projection fold.

**Why per-view chains?** Two views materialize different facets of the same event; their lift-strategies for an older event are independent. A "comments" view might add a new field with a default; a "summary" view might drop the same field. Forcing one chain across both would couple them artificially.

## Changelog

- 2026-08-10 | e855369a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | a3519bfb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Ingest-time promoter chain* | **Hash**: e855369a
