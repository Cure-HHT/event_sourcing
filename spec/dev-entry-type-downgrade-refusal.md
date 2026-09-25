# EVS-DEV-entry-type-downgrade-refusal: Entry-type version downgrade refusal

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate detects and refuses to open a datastore under an `EntryTypeRegistry` whose registered major for any entry type is lower than the major of the highest version recorded in the corresponding `view_target_versions` rows. Versions are a major and a minor number (see EVS-DEV-version-compatibility); a higher stored minor of the registered major is not a downgrade. The refusal protects already-promoted views from regressing to a stale schema and runs before any boot-time mutation (see EVS-DEV-event-store-open).

## Assertions

A. The substrate SHALL throw `EntryTypeVersionDowngradeError` from `EventStore.open` when, for any registered entry type, the registered major is below the major of the highest stored target in `view_target_versions` for that entry type, or when the database's recorded generation holds a higher major for that entry type.

B. The downgrade-refusal check SHALL run before any write of the boot transaction, including the library-version event, view-target-versions seeding, and the raised targets and convergence gaps the boot records.

C. The error SHALL carry the offending entry type's id, the registered version, and the highest stored target version, each as a major and a minor number, in a form callers can inspect for diagnostic logging.

## Rationale

**Why before any write?** A failed downgrade-check after partial seeding would leave `view_target_versions` rows seeded at the new (lower) major, contradicting the data of the higher major already in the views, and one after the library-version event would leave the log recording a version that never opened the database. Refusing before the boot's first write preserves the invariant that the stored target major is at or above the major of every event folded into the view, and keeps the log's record of opens exact (see EVS-DEV-event-store-open).

**Why also the recorded generation (assertion A)?** Stored view targets exist only for the entry types some view names, so an entry type no view names would never be checked against them. The database's generation record holds the highest major every committed boot registered for every entry type, whether or not a view names it, so a build that registers a lower major is refused after a stop-then-start major bump even when no view folds that entry type (EVS-DEV-version-compatibility/I).

**Why compare majors only?** Majors only rise. A minor may move either way within a major, because a minor step only adds fields with defaults (its promoters are `DefaultField` only): a build of an older minor reads rows promoted to a newer minor of its major. When such a build folds, it lowers the stored target to its own minor (EVS-DEV-version-compatibility/E), so the next open under the newer minor re-promotes what it folded. A higher major is refused because downgrading across a major would require inverse promoters, and the major-step primitives `RenameField` and `DropField` are shape-changers that are not invertible without information loss.

## Changelog

- 2026-09-25 | 472aa00e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Amend B: the boot's writes are the library-version event, seeding, and the raised targets and convergence gaps it records
- 2026-09-23 | ee97c1d7 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A: the database's generation record also refuses a lower entry-type major
- 2026-09-23 | cefee1bc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend B: the check runs before any write of the boot transaction, the library-version event included
- 2026-09-23 | 6c1448cd | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A, C and Rationale: downgrade refusal compares majors; a higher stored minor of the registered major opens
- 2026-08-10 | 3e482dbc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 7b577371 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Entry-type version downgrade refusal* | **Hash**: 472aa00e
