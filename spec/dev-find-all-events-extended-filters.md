# EVS-DEV-find-all-events-extended-filters: Extended findAllEvents filters

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate's storage-backend query surface supports the boot-time snapshot-promotion pass and richer audit-stream UX. `StorageBackend.findAllEvents` and `findAllEventsInTxn` accept three optional filters in addition to the existing pagination/origin parameters. Filters AND-compose; an unspecified filter matches all values.

## Assertions

A. `StorageBackend.findAllEvents` SHALL accept the optional named parameters `entryType` (single string), `clientTimestampStart` (DateTime, inclusive), and `clientTimestampEnd` (DateTime, exclusive).

B. `StorageBackend.findAllEventsInTxn` SHALL accept the same three optional parameters with the same semantics.

C. The new filters SHALL AND-compose with the existing parameters (`afterSequence`, `limit`, `originatorHopId`, `originatorIdentifier`); an event is included in the result iff every supplied filter matches.

D. The reference `SembastBackend` implementation SHALL realize the composed filter via a single shared helper (`_composeFindAllEventsFilter` or equivalent) used by both the in-transaction and out-of-transaction code paths.

## Rationale

**Why these three filters?** The boot-time snapshot-promotion pass (see EVS-DEV-snapshot-promotion-on-open) needs to enumerate "all events of a given entry type" to derive the aggregate-scope for promotion; that is the `entryType` filter. Audit-stream UX often wants windows of events bracketed by client-supplied timestamps (e.g., "show events between yesterday's midnight and now"); that is the timestamp pair.

**Why AND-compose rather than OR?** Coverage queries always narrow; broadening would require a separate union API. AND-composition matches the substrate's existing convention for filter parameters and gives callers a predictable model.

## Changelog

- 2026-07-02 | f24ffdf5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Extended findAllEvents filters* | **Hash**: f24ffdf5
