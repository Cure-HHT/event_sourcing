# EVS-DEV-entry-type-downgrade-refusal: Entry-type version downgrade refusal

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate detects and refuses to open a datastore under an `EntryTypeRegistry` whose registered major for any entry type is lower than the major of the highest version recorded in the corresponding `view_target_versions` rows. Versions are a major and a minor number (see EVS-DEV-version-compatibility); a higher stored minor of the registered major is not a downgrade. The refusal protects already-promoted views from regressing to a stale schema and runs before any boot-time mutation (see EVS-DEV-event-store-open).

## Assertions

A. The substrate SHALL throw `EntryTypeVersionDowngradeError` from `EventStore.open` when, for any registered entry type, the registered major is below the major of the highest stored target in `view_target_versions` for that entry type.

B. The downgrade-refusal check SHALL run BEFORE any view-target-versions seeding or snapshot promotion. No view state is mutated on the failing path.

C. The error SHALL carry the offending entry type's id, the registered version, and the highest stored target version, each as a major and a minor number, in a form callers can inspect for diagnostic logging.

## Rationale

**Why before any mutation?** A failed downgrade-check after partial seeding would leave `view_target_versions` rows seeded at the new (lower) major, contradicting the data of the higher major already in the views. Eager refusal preserves the invariant that the stored target major is at or above the major of every event folded into the view.

**Why compare majors only?** Majors only rise. A minor may move either way within a major, because a minor step only adds fields with defaults (its promoters are `DefaultField` only): a build of an older minor reads rows promoted to a newer minor of its major. When such a build folds, it lowers the stored target to its own minor (EVS-DEV-version-compatibility/E), so the next open under the newer minor re-promotes what it folded. A higher major is refused because downgrading across a major would require inverse promoters, and the major-step primitives `RenameField` and `DropField` are shape-changers that are not invertible without information loss.

## Changelog

- 2026-09-23 | 6c1448cd | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A, C and Rationale: downgrade refusal compares majors; a higher stored minor of the registered major opens
- 2026-08-10 | 3e482dbc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 7b577371 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Entry-type version downgrade refusal* | **Hash**: 6c1448cd
