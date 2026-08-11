# EVS-DEV-append-stamps-registered-version: Substrate stamps entryTypeVersion on append

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate ensures every appended event carries the entry type's current `registeredVersion`. The version is sourced from the substrate's `EntryTypeRegistry` rather than supplied by callers, so producers cannot accidentally (or deliberately) emit events stamped against a version other than the registry's current one. This guarantee is load-bearing for the substrate's promotion contract (see EVS-DEV-ingest-promotes-before-fold and EVS-DEV-snapshot-promotion-on-open).

## Assertions

A. `EventStore.append` SHALL stamp the appended event's `entryTypeVersion` field from the registered version returned by `entryTypes.byId(entryType).registeredVersion` at append-time.

B. `EventStore.appendInTxn` SHALL apply the same stamping as `EventStore.append`, using the same registry lookup.

C. The `entryTypeVersion` parameter SHALL NOT appear on the public `append` / `appendInTxn` signatures; callers SHALL NOT be able to override the registry-derived value.

## Rationale

**Why deny the parameter?** Allowing caller-supplied `entryTypeVersion` creates two failure modes: callers stamp the wrong version (off-by-one bug surfaces months later during a schema bump), or callers deliberately mis-stamp to bypass ingest-time promotion. Denying the parameter eliminates both. Substrate-owned versioning means there is exactly one source of truth (the registry) and one site that consults it (the append call).

**Why does this require `EntryTypeRegistry` lookup on every append?** Performance impact is negligible (constant-time map lookup), and the registry is immutable post-`EventStore.open` (per EVS-DEV-event-store-open). The lookup cannot return a value that differs from the value at boot — eliminating the race-with-promotion that a hot-swappable registry would introduce.

## Changelog

- 2026-08-10 | 2a4348d3 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 17d2982d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Substrate stamps entryTypeVersion on append* | **Hash**: 2a4348d3
