# EVS-DEV-event-store-open: EventStore.open boot flow

**Level**: DEV | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate realizes the EVS-PRD-event-log obligations at construction time. `EventStore.open` is the single public entry point for obtaining a usable `EventStore` instance; the private `EventStore._` constructor is not exposed. Open performs the fixed sequence of boot-time checks and mutations inside one storage transaction before serving subscribers, then emits a library-version audit event.

## Assertions

A. The substrate SHALL expose `EventStore.open(...)` as the sole public constructor for `EventStore` instances; the private constructor `EventStore._` SHALL NOT be reachable from outside the library.

B. `EventStore.open` SHALL emit `lib_version_initialized` on first boot under a given library version (no prior library-version event in the log).

C. `EventStore.open` SHALL emit `lib_version_changed` on subsequent transitions to a library version different from the most recent one recorded in the log.

D. `EventStore.open` SHALL refuse to construct an instance under a library version older than the most recent recorded version, throwing `DowngradeRefusedError` before any state mutation, unless the caller explicitly passes `allowDowngrade: true`, in which case the older instance opens normally; the asymmetry exists so downgrade is a deliberate decision, not the default.

E. The boot-time pass (downgrade check, view-target-versions seeding, snapshot promotion, library-version event emission) SHALL execute inside a single `storage.transaction(...)` so its mutations are atomic with respect to peer readers.

## Rationale

**Why a single boot entry point?** A two-step construction pattern (raw constructor + post-init `open()` call) historically left intermediate states where the store was partially initialized — accepting subscribers but not yet having emitted library-version events, or having seeded view targets but not yet checked for downgrade. Making `open` the only path eliminates those windows.

**Why one transaction?** Boot-time mutations (view-target-versions seeding, snapshot promotion writes, library-version event append) all read state derived from each other. Doing them in separate transactions admits torn intermediate states visible to concurrent readers. The single-transaction discipline preserves the closed-under-events guarantee that state at any sequence is reconstructable from the log.

*End* *EventStore.open boot flow* | **Hash**: c0cd7e1a
