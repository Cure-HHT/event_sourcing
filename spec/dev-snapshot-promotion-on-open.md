# EVS-DEV-snapshot-promotion-on-open: Snapshot promotion after EventStore.open

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the substrate lifts materialized view rows whose stored target version lags the registered version of the underlying entry type. The boot of `EventStore.open` finds each lagging `(viewName, entryType)` pair, raises its stored target and records a promotion gap for it (EVS-DEV-event-store-open, EVS-DEV-view-convergence); a fold under an older minor records one too (EVS-DEV-version-compatibility/M). The rows are re-derived from the log after the open returns, by view convergence, folding their events again with each promoted through the registered promoter chain, and the transaction that completes a pair's promotion appends a `view_snapshot_promoted` audit event for it. A pair's promotion completes when a convergence transaction removes the pair's promotion gap and its round version is above its prior version.

## Assertions

A. The substrate SHALL promote, by view convergence after `EventStore.open` returns, every view row of a pair whose stored `view_target_versions` value is below the registered version of the pair's entry type -- a lower major, or the same major and a lower minor, including a stored target that a fold under an older minor lowered.

B. The substrate SHALL lift each affected view row by folding the row's events from the log again, each event promoted through the per-(viewName, entryType) promoter chain to the registered version of its entry type, leaving every event in the log unchanged.

C. The substrate SHALL append exactly one `view_snapshot_promoted` audit event for each completed promotion of a `(viewName, entryType)` pair, via the substrate's internal raw-append path, in the transaction that completes it, recording the pair, the stored target the promotion started from and the version it promoted to.

D. Once the promotion of every lagging pair of a view has completed, snapshot promotion SHALL be provably equivalent to event-replay-with-promotion: the view's rows are the rows that replaying the log from genesis through the `ProjectionInterpreter` under the registered versions produces.

## Rationale

**Why promote once, after the open, rather than on read?** Promotion-on-read would push the version-check cost into every read path and make a read write. Promotion by convergence pays the cost once per version transition, after the open returns and without holding back the database's appends for the length of the work (EVS-DEV-view-convergence-scheduling), and every read reports the view as converging for the promoting build until the promotion completes (EVS-DEV-converging-view-reads).

**Which layer?** Equivalence with a replay (assertion D) is the closed-under-events claim for the library's default projection conventions and the registered promoters: a Layer 2 statement about derived state. The events in the log, their hashes and their versions are Layer 1 facts that promotion only reads (assertion B).

**Why re-derive rows from the log rather than transform the stored rows (assertions B and D)?** A stored row does not record which events built it or the versions they were folded under, and lifting it as a replay would lift it needs exactly that. A row built only from newer-minor events that never set a defaulted field, a row that newer-minor events recreated after a tombstone, a row built from events of several versions whose chains supply different defaults, and a table-view row keyed by something other than the aggregate each come out differently from a chain applied to the row than from a replay. Re-deriving a row folds its events again through the fold step that the interpreter and `rebuildView` share, so the row is the replayed row by construction, and the equivalence of D needs no argument about how promoter primitives interact with the fold.

**Which rows are re-derived?** For an aggregate view, the rows of the aggregates holding an event of the lagging entry type; a row whose events are all at the registered version comes out unchanged, which costs a fold and keeps the promotion free of a per-aggregate version test. A table view's row is keyed by what its row key extracts from an event, which need not be the aggregate, so a table view with a lagging pair is refolded whole. The cost is proportional to the events of the re-derived rows rather than to the rows: an upgrade across a major is a planned stop-then-start, and the re-promotion of rows an older minor folded re-derives the aggregates its folds touched while a newer build runs, or every aggregate holding an event of the entry type when a newer build's boot finds the lowered target.

**Why is the audit appended when the promotion completes, and why does it carry no row count (assertion C)?** The audit records that the version a view folds an entry type under changed. That is true of the rows only once convergence has re-derived them, so the audit is appended in the transaction that removes the promotion gap, and a promotion that an older build's fold reopens is audited once, when it finally completes. A promotion spans many transactions, and a row may be re-derived twice or removed after it was re-derived, so a count of rows re-derived is not a fact any replay reproduces; the audit records only the versions.

## Changelog

- 2026-09-25 | b58c11e9 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend Purpose and Rationale: a promotion completes when a convergence transaction removes the pair's promotion gap; a promotion re-derives every aggregate holding an event of the entry type. No assertion changes. The 2026-09-24 amendments changed the meaning of A (promotion after the open, by convergence), C (one audit per completed promotion, recording versions, no row count) and D (equivalence once the promotion completes); their references are in event_store.dart, snapshot_promotion.dart, snapshot_promotion_test.dart, postgres_versions_test.dart and version_compatibility_conformance.dart
- 2026-09-25 | b58c11e9 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend Purpose, A, C and D: lagging rows are promoted by view convergence after the open; the audit is appended once per completed promotion, in the transaction that completes it, and records the pair and its versions
- 2026-09-23 | a68e72b1 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A, B and D: stored targets below the registered major and minor, including one an older minor's fold lowered, are promoted by re-deriving the affected rows from the log, which equal the replayed rows
- 2026-08-10 | 7ccb1106 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 62425b7b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Snapshot promotion after EventStore.open* | **Hash**: b58c11e9
