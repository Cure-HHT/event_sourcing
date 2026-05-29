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

| Current | Kind | Recommended | Notes |
|---|---|---|---|
| `AppendOnlyDatastore` | class (bundle of `eventStore` + registries + `securityContexts`) | **`EventStoreBundle`** (alt: `EventStoreScope`, `Substrate`) | the bundle you get from opening the substrate |
| `bootstrapAppendOnlyDatastore` | function → that bundle | **`bootstrapEventStore`** | keeps the existing `bootstrap*` family verb; fixes only the heritage noun |
| `DatastoreConfig` | class | **`EventStoreConfig`** | + `.development`/`.production` factories, `copyWith` |
| `DatastoreException` | base class | **`EventStoreException`** | all subclasses re-parent |
| `DatabaseException` | subclass | **`StorageBackendException`** | `Database` isn't substrate vocab; `StorageException` already exists |
| `databaseName` (field/param) | field | **`storageName`** | and **change the `clinical_events.db` default** — a domain leak, fix regardless of rename |

**Two coherent options for the entry point:**

1. **Conservative (recommended).** Keep the `bootstrap*` family
   (`bootstrapEventStore`, `bootstrapActionPermissions`,
   `bootstrapRoleAssignments`, `bootstrapAuditedActions` stay parallel);
   fix only the heritage *nouns*. Lowest churn, preserves R11 symmetry.
2. **Constructor-style.** Drop `bootstrap*` in favor of factories
   (`EventStore.open(...)`, `ActionDispatcher.create(...)`, etc.). Cleaner
   per R7 but larger blast radius and changes the whole family.

The `clinical_events.db` default value is a **must-fix domain leak**
independent of any rename.

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

**Pushback (recommend NOT renaming):**

- `ReactionHandlers` → ~~`ReactionEndpoints`~~. **Keep.** It is
  deliberately a bundle of shelf *handlers* (the guide calls it exactly
  that); `Handlers` is accurate, not a vague R5 suffix.
- `SecurityDetails` → `SecurityTelemetry`. **Optional / low value** — the
  guide already uses `SecurityDetails`; rename only if doing a broad pass.
- `bootstrapActionPermissions`/`bootstrapRoleAssignments` →
  `initialize*`. **Defer to the §A entry-point decision** — keep them as
  the `bootstrap*` family unless you choose constructor-style.

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
