# EVS-DEV-entry-type-downgrade-refusal: Entry-type version downgrade refusal

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate refuses to open a datastore under an `EntryTypeRegistry` whose registered major for any entry type is lower than the highest major an earlier boot of the database registered for it, as the database's generation record holds it (EVS-DEV-version-compatibility). Versions are a major and a minor number; a higher recorded minor of the registered major is not a downgrade. The refusal keeps a build from folding or appending events of an entry type in a shape older than events the database already holds, and it is decided with the boot's other refusals, before the boot writes anything (EVS-DEV-event-store-open).

## Assertions

A. The substrate SHALL throw `EntryTypeVersionDowngradeError` from `EventStore.open` when, for any registered entry type, the database's recorded generation holds a higher major for that entry type than the registered major.

B. <RETIRED> The refusal is decided before the boot's first write, with every other refusal of the boot.

C. The error SHALL carry the offending entry type's id, the registered version as a major and a minor number, and the recorded major, in a form callers can inspect for diagnostic logging.

## Rationale

**Why the generation record?** Every committed boot merges into the record the highest major it registered for every entry type, whether or not a view names it, so the record is the one place that knows every major the database has been opened with. A view copy is created only by such a boot, so no copy holds a major the record lacks, and the refusal needs no reading of views.

**Why compare majors only?** Majors only rise. A minor may move either way within a major, because a minor step only adds fields with defaults (its promoters are `DefaultField` only): a build of an older minor reads and appends events its major's newer minors read too, and folds into a copy of its own definition (EVS-DEV-view-convergence). A higher major is refused because downgrading across a major would require inverse promoters, and the major-step primitives `RenameField` and `DropField` are shape-changers that are not invertible without information loss.

## Changelog

- 2026-09-25 | 3adaac55 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Amend A and C: the refusal compares the registered major with the major the generation record holds; stored view targets no longer exist (views are stored per copy), and the error carries the recorded major (A and C are cited by event_store.dart, generation_record_test.dart, postgres_generation_guard_test.dart and version_compatibility_conformance.dart; the error's fromVersion and recordedByOpen fields and its view_target_versions message contradict the amended C). Retire B: the ordering before the boot's first write is stated with every other refusal of the boot (B is cited by event_store.dart and boot_conformance.dart)
- 2026-09-25 | 06ca2ff3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Amend B: the boot's writes include the registry audit event (references: event_store.dart, boot_conformance.dart)
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

*End* *Entry-type version downgrade refusal* | **Hash**: 3adaac55
