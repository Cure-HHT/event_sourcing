# EVS-DEV-view-target-versions-seeding: view_target_versions seeding at boot

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate populates the `view_target_versions` table at boot time so that every (viewName, interest-matched entryType) pair has a known target version, and every view whose interest names no entry type has a whole-view row, which marks the view as registered on the database. The seeding step is part of the fixed boot-pass sequence inside `EventStore.open` (see EVS-DEV-event-store-open): it runs after the downgrade-refusal check (EVS-DEV-entry-type-downgrade-refusal) and before the boot records convergence gaps (EVS-DEV-view-convergence). Seeded rows carry the current `registeredVersion` of the matched entry type.

## Assertions

A. During boot, the substrate SHALL ensure every (viewName, interest-matched entryType) pair has a `view_target_versions` row by inserting a row when none exists for that pair.

B. The substrate SHALL NOT overwrite existing `view_target_versions` rows during seeding; existing target values are preserved.

C. Newly-seeded rows SHALL carry the registered major and minor of the entry type as their target value.

D. The set of (viewName, entryType) pairs to consider for seeding SHALL be derived from each registered `ProjectionSpec`'s interest filter — the entry types that the projection's `interest` matches at the current registry state.

E. During boot, the substrate SHALL ensure that every registered view whose interest names no entry type has one whole-view row in `view_target_versions`, carrying no version, by inserting it when none exists.

## Rationale

**Why seed lazily rather than at registry-mutation time?** The registry is immutable post-`EventStore.open` (per EVS-DEV-event-store-open), so there is no other write moment. Seeding inline with the boot pass guarantees the table is populated before any subscriber can observe it.

**Why preserve existing rows rather than reset to the registered version?** An existing row carries history — it was set to its current value by a prior promotion or initialization. Overwriting it would silently undo promotion progress and confuse the downgrade-refusal check on a subsequent boot. Seeding therefore never overwrites a target. The writers that change one are the boot, which raises a lagging target to the registered version and records a promotion gap for it; a fold under an older minor of the same major, a convergence re-derivation being such a fold (EVS-DEV-version-compatibility/E, EVS-DEV-view-convergence), which lowers it and records a promotion gap naming the rows it folded; the transaction that removes a promotion gap, which writes the converging build's registered version; and `EventStoreBundle.setViewTargetVersion`, an internal writer that writes whatever version it is given and that the library's own tests use to stage a stored target.

## Changelog

- 2026-09-25 | 402e6e14 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: rebuildView writes no target; a convergence re-derivation is an older-minor fold. No assertion changes
- 2026-09-25 | 402e6e14 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add E and amend Purpose: a view whose interest names no entry type gets a whole-view row; seeding precedes the boot's convergence gaps; the Rationale names every writer of a target
- 2026-09-23 | 0ea6c582 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend C: seeded targets carry the registered major and minor; the Rationale names every writer that can lower a target
- 2026-08-10 | 911a148f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | eb373312 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *view_target_versions seeding at boot* | **Hash**: 402e6e14
