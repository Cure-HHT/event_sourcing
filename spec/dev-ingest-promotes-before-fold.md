# EVS-DEV-ingest-promotes-before-fold: Ingest-time promoter chain

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-ingest, EVS-PRD-materializer

## Purpose

How the substrate transparently lifts ingested events whose `entryTypeVersion` lags the registered version of the entry type -- a lower major, or the registered major and a lower minor (see EVS-DEV-version-compatibility). The `ProjectionInterpreter` consults the per-view promoter chain on each event before dispatching to the fold. The original event remains untouched in the log; the promoter operates on an in-memory `StoredEvent.withData(...)` copy. This keeps the closed-under-events trust model intact (hash-chain integrity preserved) while ensuring materialized views always fold over the up-to-date shape.

## Assertions

A. The `ProjectionInterpreter` SHALL apply the per-view promoter chain to any event of a lower major than the registered version of its entry type, or of the registered major and a lower minor, BEFORE dispatching to the projection's fold. The library's default promotion TREATS a field that such an event does not carry as a field the event leaves unchanged: a `DefaultField` in the chain supplies its value only when neither the event nor the view's existing row carries the field under the name it has at the registered version, the name the renames that follow the `DefaultField` in the chain give it.

B. Promotion SHALL operate on an in-memory `event.withData(newData)` copy. The original `StoredEvent` recorded in the log SHALL NOT be modified.

C. Two views that match the same entry type MAY register different promoter chains and produce different fold inputs from the same source event. The substrate SHALL NOT enforce chain-equality across views matching the same entry type.

D. When an event is of the registered major and its minor is equal to or higher than the registered minor, the substrate SHALL bypass the promoter chain entirely and fold the event unchanged.

## Rationale

**Why in-memory promotion rather than rewriting the event?** Rewriting would break hash-chain integrity (a Layer 1 substrate fact, per EVS-PRD-hash-chain-integrity and the charter's Assertion A). The in-memory copy mechanism preserves the chain while delivering up-to-date data to the projection fold.

**Why is a promoted default decided against the row (assertion A)?** This is a Layer 2 convention: the library's interpretation of what an older event's silence about a newer field means. An event of an older version does not mention a field a later version added, exactly as any event omits a key it does not change, and the fold preserves a key an event omits. Supplying the default only when neither the event nor the row carries the field keeps that meaning: the default fills a field the aggregate has never had and never overrides a value a newer event set. The row is in the shape of the registered version, so the field is looked up under the name it has there: a field a later major step renames is looked up under its new name, and a default a later step drops has no effect. A table view writes one row per event and has no row to decide against, so there every default in the chain is supplied. Every fold path -- an append's or an ingest's fold and a view copy's catch-up, `rebuildView`'s included -- folds through the same step, so they decide every default alike.

**Why does a higher minor fold unchanged (assertion D)?** A same-major event of a newer minor carries fields this build does not know; the fold stores them as they are, and a build of the newer minor reads them.

**Why per-view chains?** Two views materialize different facets of the same event; their lift-strategies for an older event are independent. A "comments" view might add a new field with a default; a "summary" view might drop the same field. Forcing one chain across both would couple them artificially.

## Changelog

- 2026-09-25 | 6e8f0c6d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of A: the fold paths are an append's or an ingest's fold and a view copy's catch-up
- 2026-09-23 | 6e8f0c6d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A and D: promotion for a lower major or the same major and a lower minor, with each default decided against the row under the field's name at the registered version; a same-major event at an equal or higher minor folds unchanged
- 2026-08-10 | e855369a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | a3519bfb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Ingest-time promoter chain* | **Hash**: 6e8f0c6d
