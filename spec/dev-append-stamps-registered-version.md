# EVS-DEV-append-stamps-registered-version: Substrate stamps entryTypeVersion on append

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate ensures every appended event carries the entry type's current `registeredVersion`. The version is sourced from the substrate's `EntryTypeRegistry` rather than supplied by callers, so producers cannot accidentally (or deliberately) emit events stamped against a version other than the registry's current one. This guarantee is load-bearing for the substrate's promotion contract (see EVS-DEV-ingest-promotes-before-fold and EVS-DEV-snapshot-promotion-on-open).

## Assertions

A. `EventStore.append` SHALL stamp the appended event's `entryTypeVersion` field from the registered version returned by `entryTypes.byId(entryType).registeredVersion` at append-time.

B. `EventStore.appendInTxn` SHALL apply the same stamping as `EventStore.append`, using the same registry lookup.

C. The `entryTypeVersion` parameter SHALL NOT appear on the public `append` / `appendInTxn` signatures; callers SHALL NOT be able to override the registry-derived value.

D. The library SHALL refuse a registration for an entry type id that already has one, leaving the existing definition registered.

## Rationale

**Why deny the parameter?** Allowing caller-supplied `entryTypeVersion` creates two failure modes: callers stamp the wrong version (off-by-one bug surfaces months later during a schema bump), or callers deliberately mis-stamp to bypass ingest-time promotion. Denying the parameter eliminates both. Substrate-owned versioning means there is exactly one source of truth (the registry) and one site that consults it (the append call).

**Why does this require `EntryTypeRegistry` lookup on every append?** Performance impact is negligible (constant-time map lookup), and an entry type's registered version cannot change while a store is open. New entry types may be added after boot — a deployment that does so seeds the corresponding view-target-version rows itself — but an existing definition can never be redefined in place, so no lookup returns a version different from the one that type was registered with. That is what eliminates the race-with-promotion a redefinable registry would introduce.

**Why refuse a duplicate registration rather than overwrite?** A map would silently let the later registration win. Because the registry is the sole source of the version stamped onto every appended event of a type, a silent overwrite would change that version mid-run, and events appended before and after the overwrite would claim different versions of the same schema with nothing in the log explaining the discontinuity. Refusing makes two competing definitions a startup failure — the point at which a deployment can still be corrected — rather than a data-integrity defect discovered at the next schema bump. This is also the property the per-append lookup above depends on.

## Changelog

- 2026-09-07 | 6e9c508c | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-07 | - | - | Michael Lewis (<michael@anspar.org>) | Add D: a registration for an already-registered entry type id is refused; correct the Rationale's claim that the registry is immutable after open
- 2026-08-10 | 2a4348d3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 17d2982d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Substrate stamps entryTypeVersion on append* | **Hash**: 6e9c508c
