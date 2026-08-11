# EVS-DEV-view-target-versions-seeding: view_target_versions seeding at boot

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate populates the `view_target_versions` table at boot time so that every (viewName, interest-matched entryType) pair has a known target version. The seeding step is part of the fixed boot-pass sequence inside `EventStore.open` (see EVS-DEV-event-store-open) and runs after the downgrade-refusal check (EVS-DEV-entry-type-downgrade-refusal) but before snapshot promotion (EVS-DEV-snapshot-promotion-on-open). Seeded rows carry the current `registeredVersion` of the matched entry type.

## Assertions

A. During boot, the substrate SHALL ensure every (viewName, interest-matched entryType) pair has a `view_target_versions` row by inserting a row when none exists for that pair.

B. The substrate SHALL NOT overwrite existing `view_target_versions` rows during seeding; existing target values are preserved.

C. Newly-seeded rows SHALL carry the current `registeredVersion` of the entry type as their target value.

D. The set of (viewName, entryType) pairs to consider for seeding SHALL be derived from each registered `ProjectionSpec`'s interest filter — the entry types that the projection's `interest` matches at the current registry state.

## Rationale

**Why seed lazily rather than at registry-mutation time?** The registry is immutable post-`EventStore.open` (per EVS-DEV-event-store-open), so there is no other write moment. Seeding inline with the boot pass guarantees the table is populated before any subscriber can observe it.

**Why preserve existing rows rather than reset to the registered version?** An existing row carries history — it was set to its current value by a prior promotion or initialization. Overwriting it would silently undo promotion progress and confuse the downgrade-refusal check on a subsequent boot.

## Changelog

- 2026-08-10 | 911a148f | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | eb373312 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *view_target_versions seeding at boot* | **Hash**: 911a148f
