# EVS-DEV-event-store-open: EventStore.open boot flow

**Level**: DEV | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate realizes the EVS-PRD-event-log obligations at construction time. `EventStore.open` is the single public entry point for obtaining a usable `EventStore` instance; the private `EventStore._` constructor is not exposed. Open runs a fixed sequence of boot phases — each one atomic in its own storage transaction — before serving subscribers: downgrade check, view-target-versions seeding, snapshot promotion, and library-version audit-event emission.

## Assertions

A. The substrate SHALL expose `EventStore.open(...)` as the sole public constructor for `EventStore` instances; the private constructor `EventStore._` SHALL NOT be reachable from outside the library.

B. `EventStore.open` SHALL emit `lib_version_initialized` on first boot under a given library version (no prior library-version event in the log).

C. `EventStore.open` SHALL emit `lib_version_changed` on subsequent transitions to a library version different from the most recent one recorded in the log.

D. `EventStore.open` SHALL refuse to construct an instance under a library version older than the most recent recorded version, throwing `DowngradeRefusedError` before any state mutation, unless the caller explicitly passes `allowDowngrade: true`, in which case the older instance opens normally; the asymmetry exists so downgrade is a deliberate decision, not the default.

E. Each boot phase — downgrade check, view-target-versions seeding, snapshot promotion, and library-version event emission — SHALL execute atomically inside its own `storage.transaction(...)` so its mutations are atomic with respect to peer readers. The boot sequence runs the phases in this order; phase-to-phase atomicity is not asserted (a crash between phases is recoverable on next boot).

## Rationale

**Why a single boot entry point?** A two-step construction pattern (raw constructor + post-init `open()` call) historically left intermediate states where the store was partially initialized — accepting subscribers but not yet having emitted library-version events, or having seeded view targets but not yet checked for downgrade. Making `open` the only path eliminates those windows.

**Why per-phase atomicity rather than one outer transaction?** Each boot phase reads state derived from its own writes (e.g., snapshot promotion seeds `view_target_versions` rows it then updates), and within a phase those mutations MUST commit atomically — otherwise concurrent readers see torn intermediate states and the closed-under-events guarantee breaks. Across phases, however, the boot sequence is restartable: a crash between phases leaves the log in a state the next boot can detect and resume from (the downgrade check is read-only and idempotent; seeding is idempotent given the registry; snapshot promotion re-derives target versions from the registry; the library-version event emission is idempotent at the level of "no event of this version exists yet"). Wrapping all four phases in one outer transaction would conflate the snapshot-promotion sub-transactions (which currently emit per-pair `view_snapshot_promoted` audits) with the lib-version-event append, and force backend implementations to support nested transactions for no semantic gain. The shipped impl therefore commits each phase independently and relies on the per-phase atomicity for correctness, with cross-phase recovery handled by re-running the boot sequence.

*End* *EventStore.open boot flow* | **Hash**: 651d5818
