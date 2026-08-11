# EVS-DEV-entry-type-downgrade-refusal: Entry-type version downgrade refusal

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate detects and refuses to open a datastore under an `EntryTypeRegistry` whose `registeredVersion` for any entry type is lower than the highest version recorded in the corresponding `view_target_versions` row. The refusal protects already-promoted views from regressing to a stale schema and runs before any boot-time mutation (see EVS-DEV-event-store-open).

## Assertions

A. The substrate SHALL throw `EntryTypeVersionDowngradeError` from `EventStore.open` when, for any registered entry type, the registry's `registeredVersion` is lower than the highest stored value in `view_target_versions` for that entry type.

B. The downgrade-refusal check SHALL run BEFORE any view-target-versions seeding or snapshot promotion. No view state is mutated on the failing path.

C. The error SHALL carry the offending entry type's id, the registry's `registeredVersion`, and the highest stored target version, in a form callers can inspect for diagnostic logging.

## Rationale

**Why before any mutation?** A failed downgrade-check after partial seeding would leave `view_target_versions` rows seeded at the new (lower) version, contradicting the higher-versioned data already in the views. Eager refusal preserves the invariant `view_target_versions ≥ event.entryTypeVersion` for already-promoted rows.

**Why is downgrade refused rather than downgraded?** Downgrading would require an inverse-promoter primitive set; the project's promoter primitives are intentionally shape-changers (not invertible without information loss for `DropField`). Refusing forces the library version to be a monotonically-rising contract, which simplifies reasoning about replayability and audit reconstruction.

## Changelog

- 2026-08-10 | 3e482dbc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 7b577371 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Entry-type version downgrade refusal* | **Hash**: 3e482dbc
