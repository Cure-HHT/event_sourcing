# EVS-DEV-snapshot-promotion-on-open: Snapshot promotion at EventStore.open

**Level**: dev | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the substrate lifts materialized view rows whose stored target version lags the currently registered version of the underlying entry type. Snapshot promotion runs as part of the boot-time pass inside `EventStore.open` (see EVS-DEV-event-store-open). Each promotion applies the registered promoter chain to the affected view rows and emits a `view_snapshot_promoted` audit event per `(viewName, entryType)` pair.

## Assertions

A. During boot, the substrate SHALL promote every view row whose stored `view_target_versions` value is below the current `registeredVersion` of the relevant entry type.

B. The substrate SHALL apply the per-(viewName, entryType) promoter chain to lift the row's data from the stored version to the registered version, leaving the original event in the log unchanged.

C. The substrate SHALL emit exactly one `view_snapshot_promoted` audit event per promoted `(viewName, entryType)` pair via the substrate's internal raw-append path.

D. Snapshot promotion at boot SHALL be provably equivalent to event-replay-with-promotion: applying the promoter chain to lagging rows yields the same materialized state that would result from replaying the affected events from genesis through the `ProjectionInterpreter`.

## Rationale

**Why promote at boot rather than on read?** Promotion-on-read would push the version-check cost into every subscriber path. Boot-time promotion pays the cost once per library-version transition and lets the subscriber path stay simple.

**Why is fold-commutativity required?** The promoter primitive set is restricted to shape-changers (`RenameField`, `DefaultField`, `DropField`); the deep-merge fold commutes with each. Without commutativity, snapshot promotion would diverge from event-replay-with-promotion and the closed-under-events guarantee would break.

*End* *Snapshot promotion at EventStore.open* | **Hash**: 62425b7b
