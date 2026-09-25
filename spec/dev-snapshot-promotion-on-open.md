# EVS-DEV-snapshot-promotion-on-open: Snapshot promotion at EventStore.open

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the substrate lifts materialized view rows whose stored target version lags the currently registered version of the underlying entry type. Snapshot promotion runs as part of the boot-time pass inside `EventStore.open` (see EVS-DEV-event-store-open). Each promotion re-derives the affected view rows from the log, folding their events again with each promoted through the registered promoter chain, and emits a `view_snapshot_promoted` audit event per `(viewName, entryType)` pair.

## Assertions

A. During boot, the substrate SHALL promote every view row whose stored `view_target_versions` value is below the registered version of the relevant entry type -- a lower major, or the same major and a lower minor, including a stored target that a fold under an older minor lowered.

B. The substrate SHALL lift each affected view row by folding the row's events from the log again, each event promoted through the per-(viewName, entryType) promoter chain to the registered version of its entry type, leaving every event in the log unchanged.

C. The substrate SHALL emit exactly one `view_snapshot_promoted` audit event per promoted `(viewName, entryType)` pair via the substrate's internal raw-append path.

D. Snapshot promotion at boot SHALL be provably equivalent to event-replay-with-promotion: the rows it leaves are the rows that replaying the log from genesis through the `ProjectionInterpreter` under the registered versions produces.

## Rationale

**Why promote at boot rather than on read?** Promotion-on-read would push the version-check cost into every subscriber path. Boot-time promotion pays the cost once per library-version transition and lets the subscriber path stay simple.

**Why re-derive rows from the log rather than transform the stored rows (assertions B and D)?** A stored row does not record which events built it or the versions they were folded under, and lifting it as a replay would lift it needs exactly that. A row built only from newer-minor events that never set a defaulted field, a row that newer-minor events recreated after a tombstone, a row built from events of several versions whose chains supply different defaults, and a table-view row keyed by something other than the aggregate each come out differently from a chain applied to the row than from a replay. Re-deriving a row folds its events again through the fold step that the interpreter and `rebuildView` share, so the row is the replayed row by construction, and the equivalence of D needs no argument about how promoter primitives interact with the fold.

**Which rows are re-derived?** For an aggregate view, the rows of the aggregates holding an event of the lagging entry type below its registered version: every other aggregate's events of that entry type fold unchanged under any build of the registered major, so its row is already the replayed row. A table view's row is keyed by what its row key extracts from an event, which need not be the aggregate, so a table view with a lagging pair is refolded whole. The cost is proportional to the events of the re-derived rows rather than to the rows: an upgrade across a major is a planned stop-then-start, and a re-promotion after an older minor's fold (EVS-DEV-version-compatibility/E) re-derives every aggregate holding an event of an older minor.

## Changelog

- 2026-09-23 | a68e72b1 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A, B and D: stored targets below the registered major and minor, including one an older minor's fold lowered, are promoted by re-deriving the affected rows from the log, which equal the replayed rows
- 2026-08-10 | 7ccb1106 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 62425b7b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Snapshot promotion at EventStore.open* | **Hash**: a68e72b1
