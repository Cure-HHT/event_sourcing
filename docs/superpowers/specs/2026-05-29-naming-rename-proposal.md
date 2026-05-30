# Naming rename proposal (review only — renaming is a second pass)

Greenfield clean-interface naming review of the `event_sourcing` repo and
siblings, against the ruleset in this document's companion (the R1–R13
rules; see the session summary or `/tmp/naming_rules.md`). A 14-agent
review of 167 library files produced 76 recommendations → **60 unique
symbols**. Nothing is renamed here; this is the proposal to accept/trim.

Severity: high = public heritage/misleading; med = public clarity;
low = internal/style. `[pub]` = exported, `[int]` = internal.

## A. The heritage cluster (R2) — the headline decision

A family of names carries `Datastore` / `Database` / `AppendOnly`
vocabulary from the `hht_diary` origin, plus a `clinical_events.db`
default — none of which belong in a domain-neutral substrate.

**Decision (locked):** conservative entry-point style — keep the
`bootstrap*` family verb and fix only the heritage *nouns*. The
constructor-style alternative (dropping `bootstrap*` for
`EventStore.open(...)` etc.) was considered and rejected: larger blast
radius, and it would break the parallel `bootstrap*` family for no
clarity gain (the heritage noun, not the verb, is what violates R2).

| Current | Kind | Final name |
|---|---|---|
| `AppendOnlyDatastore` | class (bundle of `eventStore` + registries + `securityContexts`) | **`EventStoreBundle`** |
| `bootstrapAppendOnlyDatastore` | function → that bundle | **`bootstrapEventStore`** |
| `DatastoreConfig` | class (+ `.development`/`.production` factories, `copyWith`) | **`EventStoreConfig`** |
| `DatastoreException` | base class (all subclasses re-parent) | **`EventStoreException`** |
| `DatabaseException` | subclass (`StorageException` already exists) | **`StorageBackendException`** |
| `databaseName` | field/param | **`storageName`** |

The `bootstrap*` family stays parallel: `bootstrapEventStore`,
`bootstrapActionPermissions`, `bootstrapRoleAssignments`,
`bootstrapAuditedActions` (R11).

The `clinical_events.db` default value is a **must-fix domain leak** —
change it to a domain-neutral default (e.g. `event_sourcing.db`)
regardless of the rename.

## B. Other HIGH (endorsed)

| Current | Kind | Recommended | Rule | Why |
|---|---|---|---|---|
| `portalInboundPoll` | method | `pollInbound` | R2,R6 | `portal` is domain leak; verb-first |
| `error` | field on `Stale` view-state | `connectionStatus` | R12 | the field holds a `ConnectionStatus`, not an error |
| `EventIdRange` / `eventIdRange` | typedef / field | `SequenceRange` / `sequenceRange` | R12 | it ranges over sequence numbers, not event ids |
| `anyFifoWedged` | method | `hasFifoWedged` | R8 | boolean predicate |
| `Txn` | class | `Transaction` | R9 | expand non-established abbreviation |

## C. MED (public clarity — endorsed unless noted)

- Boolean predicates (R8): `materialize` → `isMaterialized`,
  `inFlight` → `isInFlight`, `progressive` → `isProgressive`.
- Abbreviations (R9): `txnProvider` → `transactionProvider`,
  `ClockFn` → `Clock`.
- Misleading (R12): `SignatureException` → `ChainVerificationException`
  (it's hash-chain verification, not crypto signatures);
  `onAuthRejected`/`onWireUnauthorized` → `handleAuthRejected`/
  `handleWireUnauthorized` (they handle, not subscribe).
- Verbs for functions (R6): `roleAssignmentAggregateId` →
  `computeRoleAssignmentAggregateId`, `scopeClassMatch` →
  `matchScopeClass`.
- Params/fields naming the role (R10/R5/R3): transform `from`/`to` →
  `sourceField`/`targetField`; `context` (security) → `securityContext`;
  `current` (lib version) → `version`; `EventSeedApplier` →
  `PermissionSeedApplier`; `AuthorizationPolicyBootstrap` →
  `AuthorizationBootstrapResult`.

**Excluded (do NOT rename):**

- `ReactionHandlers` → ~~`ReactionEndpoints`~~. **Keep.** It is
  deliberately a bundle of shelf *handlers* (the guide calls it exactly
  that); `Handlers` is accurate, not a vague R5 suffix.
- `SecurityDetails` → ~~`SecurityTelemetry`~~. **Keep.** The guide uses
  `SecurityDetails`; low value, out of scope.
- The `bootstrap*` family stays per the §A locked decision:
  `bootstrapAuditedActions`, `bootstrapActionPermissions`, and
  `bootstrapRoleAssignments` are **NOT** renamed (no `createActionDispatcher`,
  no `initialize*`). Only `bootstrapAppendOnlyDatastore` →
  `bootstrapEventStore` (heritage noun) per §A.

## D. LOW (internal/style — optional cleanup batch)

Cheap, safe, do-with-the-pass-or-skip:

- Single-letter codec params (R10): `r`/`s`/`e` → `result`/`submission`/
  `authorization`; `defn` → `definition`.
- Boolean predicates (R8): `enableEncryption`/`enableTelemetry` →
  `encryptionEnabled`/`telemetryEnabled`; `ok` (ChainVerdict) →
  `isValid`; private `_disposed`/`_reconnecting` → `_isDisposed`/
  `_isReconnecting`.
- Misc (R3/R5/R9/R12): `Uuid4IdempotencyKeyGenerator` →
  `UuidIdempotencyKeyGenerator`; `ContainmentRef` → `ContainmentReference`;
  `AuthzWatcher` → `AuthorizationWatcher`; `AuthMsg` → `AuthMessage`;
  `assertNoEntryTypeDowngrade` → `verifyNoEntryTypeDowngrade`;
  internal route handlers `meRouteHandler`/`permissionRouteHandler`/
  `actionRouteHandler` → `meHandler`/`permissionSnapshotHandler`/
  `actionHandler`; `registry` getter → `projections`;
  `InternalSecurityContextStore` → `MutableSecurityContextStore`.

Several LOW items are debatable (`PromoterExecutor` → `PromoterRunner`,
`ensurePostgresSchema` → `applyPostgresSchema`) — recommend **skip**;
`Executor`/`ensure` are fine.

## Notes

- All renames are **public-API breaking** where `[pub]`; sweep call
  sites (lib, tests, examples) and the `EVS-DEV-*` traceability
  annotations (e.g. `EVS-DEV-event-store-open/A` references the bootstrap
  name) in the same pass.
- No code is changed by this proposal. The full raw machine output is in
  the session's workflow result.
