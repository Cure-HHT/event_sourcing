# EVS-DEV-event-store-open: EventStore.open boot flow

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log

## Purpose

How the substrate realizes the EVS-PRD-event-log obligations at construction time. `EventStore.open` is the single public entry point for obtaining a usable `EventStore` instance; the private `EventStore._` constructor is not exposed. Open runs two boot phases — each one atomic in its own storage transaction — before serving subscribers: a **library-version phase** (downgrade check + `lib_version_initialized`/`lib_version_changed` emission), then a **snapshot-promotion phase** (entry-type downgrade check + `view_target_versions` seeding + snapshot promotion with per-pair `view_snapshot_promoted` audits).

## Assertions

A. The substrate SHALL expose `EventStore.open(...)` as the sole public constructor for `EventStore` instances; the private constructor `EventStore._` SHALL NOT be reachable from outside the library.

B. `EventStore.open` SHALL emit `lib_version_initialized` on first boot under a given library version (no prior library-version event in the log).

C. `EventStore.open` SHALL emit `lib_version_changed` on subsequent transitions to a library version different from the most recent one recorded in the log.

D. `EventStore.open` SHALL refuse to construct an instance under a library version older than the most recent recorded version, throwing `DowngradeRefusedError` before any state mutation, unless the caller explicitly passes `allowDowngrade: true` — in which case the older instance opens normally **and no `lib_version_changed` event is appended**. The downgrade is intentional and unrecorded in the event log; deployments using `allowDowngrade: true` are responsible for their own out-of-band audit trail of which library version ran when. The asymmetry exists so downgrade is a deliberate decision, not the default; the silent-no-emit choice avoids fabricating a "downgrade to" event the rest of the substrate would have to reason about.

E. `EventStore.open` SHALL open two storage transactions in sequence: (1) a **library-version phase** that reads the most-recent recorded library version, refuses to open on un-allowed downgrade, and on version change appends a `lib_version_initialized` or `lib_version_changed` event; (2) a **snapshot-promotion phase** that performs entry-type downgrade checks against `registeredVersion`, seeds `view_target_versions` rows for any (viewName, entry-type) pairs newly bumped, and runs the registered promoter chain over existing view rows for those entry types (emitting one `view_snapshot_promoted` audit per promoted pair). Each phase is internally atomic; phase-to-phase atomicity is not asserted. A crash between phases is recoverable on next boot — phase 1's version check is idempotent (no-op when the recorded version already matches the new one) and phase 2's target-version seed is idempotent (no-op when a row already exists at the registered version).

## Rationale

**Why a single boot entry point?** A two-step construction pattern (raw constructor + post-init `open()` call) would leave intermediate states where the store is partially initialized — accepting subscribers before library-version events have been emitted, or before downgrade has been checked. Making `open` the only path eliminates those windows.

**Why two transactions rather than one outer transaction?** Each boot phase reads state derived from its own writes (e.g., snapshot promotion seeds `view_target_versions` rows it then updates), and within a phase those mutations MUST commit atomically — otherwise concurrent readers see torn intermediate states and the closed-under-events guarantee breaks. Across phases, however, the boot sequence is restartable: a crash between phases leaves the log in a state the next boot can detect and resume from (the library-version check is idempotent — no-op when the recorded version already matches; the entry-type downgrade check is read-only and idempotent; `view_target_versions` seeding is idempotent given the registry; snapshot promotion re-derives target versions from the registry). Wrapping both phases in one outer transaction would conflate the library-version audit-event append with the per-pair `view_snapshot_promoted` audits inside the promotion phase, and would force backend implementations to support nested transactions for no semantic gain. The shipped impl therefore commits each phase independently and relies on the per-phase atomicity for correctness, with cross-phase recovery handled by re-running the boot sequence.

**Why library-version first, then snapshot-promotion?** The library-version event is the audit trail of "which lib version booted at sequence N." Emitting it before snapshot promotion means that any `view_snapshot_promoted` audits emitted by phase 2 sit *after* their causal `lib_version_changed` event in the log — a reader replaying from genesis sees the version bump, then the promotions it triggered, in causal order. The reverse order would surface promotion audits whose justification (the lib-version transition) hadn't been recorded yet.

## Changelog

- 2026-07-02 | 98a3dab0 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *EventStore.open boot flow* | **Hash**: 98a3dab0
