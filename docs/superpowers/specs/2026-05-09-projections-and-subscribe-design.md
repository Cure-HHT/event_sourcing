# Projections and Subscribe Primitive Design

**Phase**: I
**Status**: Draft (Phase I implementation target)
**Last updated**: 2026-05-09
**Supersedes**: the "Subscribe primitive", "Materializer", and "Multi-source readiness" sections of `2026-05-09-substrate-and-materializer-design.md` for projection mechanics; that document remains authoritative for `EventStore` log layout, action dispatch, ingest, storage abstraction, and overall component boundaries.

## Scope

This spec pins the design of three load-bearing pieces of the substrate that the prior overview spec left underspecified or that subsequent design work has reframed:

- **The projection model** — how event-derived state is computed and stored.
- **The subscribe primitive** — how reactive consumers read that state.
- **Substrate lifecycle and library-version tracking** — how the substrate boots, validates its own version, and records that history in the log.

In scope:

- Closed-set declarative `ProjectionSpec` shapes (Aggregate, Table) and the substrate's per-shape fold mechanics.
- Library-supplied derivation primitives applied during fold.
- Declarative `PromoterSpec` shape and library-supplied transformation primitives for event-payload schema evolution.
- The `subscribe<T>(filter, mode)` primitive — signature, modes, `Update<T>` envelope, snapshot-then-deltas semantics, atomicity at subscribe.
- Library-version events (`lib_version_initialized`, `lib_version_changed`) and the boot-time version-check.
- `EventStore.open` composition with projection and promoter registries.

Out of scope (deferred to other documents):

- The action-dispatch pipeline (already implemented; existing design).
- The ingest path and chain-verification mechanics (existing design).
- `Destination` outbound shipping, FIFO queue, and persistent watermark mechanism (briefly referenced; spec lives elsewhere).
- Phase II rule grammar for multi-source canonicalization (its own future design).
- The full library-primitive catalogue (transformations, derivations, projection shapes beyond the first two) — the spec defines the shape and discipline; the catalogue grows over Phase I + later releases.

## Architectural commitments

The design follows three commitments that the rest of the document elaborates.

### 1. Projections are closed under events

A materialized view at sequence N must be reproducible from `(events[0..N], specs_in_log[0..N], lib_version_in_log[0..N])` alone. No author-supplied code participates in the projection function. Author code lives outside the substrate; the substrate's projection mechanics interpret declarative specs against events.

This is the same closed-under-events model the action and permissions designs adopt, extended uniformly to materialized state. Permissions, role assignments, action outcomes, and domain projections are all specified as data and computed by the substrate from the event log.

### 2. Library primitives are append-only

The library carries a closed catalogue of:

- **Projection shapes** (Aggregate, Table, ...).
- **Transformation primitives** for promoters (Rename, Default, Derive, Drop, ...).
- **Derivation primitives** for projection-time computed fields (DottedPathLookup, FirstEventTimestamp, ConstantValue, ...).

Once a primitive ships under a name with given semantics, those semantics are frozen. Bug fixes ship as new primitive names; existing PromoterSpecs and ProjectionSpecs continue to invoke the original primitive to preserve historical-state reproducibility. New behavior is always a new primitive, never a re-interpretation of an existing one.

### 3. Library version is recorded in the log

The substrate emits `lib_version_initialized` on first boot under a given library version and `lib_version_changed` on every subsequent version transition. These events are part of the log, so an auditor replaying the log can determine which library version processed any given event range. Downgrades are refused by default to prevent silent state divergence.

## Section 1 — Projection model

### Closed set of shapes

```dart
sealed class ProjectionSpec {
  String get viewName;
  SubscriptionFilter get interest;
}

class AggregateProjectionSpec extends ProjectionSpec {
  final String aggregateType;
  final Set<EventTypeId> tombstoneEventTypes;
  final List<DerivedField> derivedFields;
}

class TableProjectionSpec extends ProjectionSpec {
  final Set<EventTypeId> insertEventTypes;
  final Set<EventTypeId> removeEventTypes;
  final RowKeyExtractor rowKey;
  final RowDataExtractor rowData;
}
```

The set is closed; new shapes are library work. Phase I ships at minimum these two: aggregate-shaped projections (per-aggregate row, generic merge) and table-shaped projections (insert/remove rows by key, no per-aggregate fold).

A projection's `interest` filter is the same `SubscriptionFilter` shape that subscribers use. The substrate indexes filters at registration; on append, only matching projections are invoked. No per-event linear scan over all projections.

### Per-shape fold mechanics — AggregateProjectionSpec

For each event matching `interest`:

1. Look up the aggregate row by `event.aggregateId` from the backend's view named `viewName`. If absent, start from an empty `Map<String, Object?>`.
2. Apply `Merge.applyDelta(prior, event.payload)` — key-wise merge with **null-as-clear** semantics. Each key in `event.payload` overwrites the corresponding key in `prior`; absent keys preserve; explicit `null` clears the prior value. (The existing `mergeAnswers` becomes the library's `Merge.applyDelta` utility.)
3. Apply each `DerivedField` in declaration order: compute the field's value from the merged Map and the event's metadata using the field's declared `DerivedFieldComputation` primitive.
4. Stamp library-managed metadata: `latestEventId`, `updatedAt = event.clientTimestamp`, and on first event for the aggregate, `firstEventTimestamp = event.clientTimestamp`.
5. If `event.eventType ∈ tombstoneEventTypes`: delete the row. Substrate retains the tombstone sequence so `subscribe<T>` can emit `Update<T>.Tombstone` to subscribers.
6. Otherwise: write the new Map back as the aggregate's row.

All steps run inside the same `Txn` as the event append (existing atomicity guarantee carries over).

### Per-shape fold mechanics — TableProjectionSpec

For each event matching `interest`:

- If `event.eventType ∈ insertEventTypes`: extract `key = rowKey.extract(event)` and `data = rowData.extract(event)`; upsert row `(viewName, key) → data`.
- If `event.eventType ∈ removeEventTypes`: extract `key = rowKey.extract(event)`; delete row `(viewName, key)`.
- Otherwise: no-op (filter narrowing should prevent this case but the fold is safe).

Table projections do not emit `Tombstone` updates — they emit `Delta` for inserts and a row-removal `Delta` (or absence) for removes. This shape is for flat lookup tables (permission grants, user catalogues, indexes), not per-aggregate state.

### Library-supplied derivation primitives

```dart
sealed class DerivedFieldComputation {}

class DottedPathLookup extends DerivedFieldComputation {
  final String path;            // e.g., "answers.date_of_event"
  final FallbackValue fallback;
}

sealed class FallbackValue {}
class FirstEventTimestamp extends FallbackValue {}
class ConstantValue extends FallbackValue { final Object? value; }

class DerivedField {
  final String fieldName;
  final DerivedFieldComputation computation;
}
```

Closed-set, deterministic, expressed as data. New derivation kinds are library work (matching the Append-Only Primitives commitment).

### Library-supplied row extractors

```dart
sealed class RowKeyExtractor {
  Object extract(StoredEvent event);
}
class AggregateIdKey extends RowKeyExtractor {}
class CompositeKey extends RowKeyExtractor {
  final List<String> paths;     // dotted paths into payload + metadata
}

sealed class RowDataExtractor {
  Map<String, Object?> extract(StoredEvent event);
}
class WholePayload extends RowDataExtractor {}
class PayloadField extends RowDataExtractor { final String fieldName; }
class SelectedFields extends RowDataExtractor { final List<String> paths; }
```

### Storage shape

Backend stores rows as canonical-JSON-equivalent `Map<String, Object?>` in the view tables already exposed by `StorageBackend.{read,upsert,delete,find,clear}View*`. The substrate writes the merged map directly; no per-projection codec is involved at the storage layer. Typed access for consumers happens at read time via subscribe-supplied mappers (Section 3).

### Registration

```dart
final eventStore = await EventStore.open(
  storage: backend,
  projections: ProjectionRegistry()
    ..register(diaryEntriesSpec)
    ..register(rolePermissionsSpec),
  promoters: PromoterRegistry()
    ..register(diaryEntryPromoterSpec),
);
```

`viewName` is the registry key; one projection per `viewName`. Registration is composition-time configuration in Phase I; Phase II adds `register_projection(spec)` settings events that drive the registry from the log itself, fully closing the closed-under-events loop for projection rules.

### Rebuild

`rebuildView(viewName, ...)` — the existing operation — now rebuilds by replaying the log through the registered ProjectionSpec. The atomic clear-and-replay semantics carry over: clear `view_target_versions` for the view, clear the view rows, write new target-version map, replay. Per-(viewName, entryType) target versions remain part of the rebuild input; promoters apply during replay (Section 2).

## Section 2 — Promoter model

### Declarative PromoterSpec

```dart
class PromoterSpec {
  final String viewName;
  final String entryType;
  final int fromVersion;
  final int toVersion;
  final List<TransformPrimitive> transforms;  // applied in order
}

sealed class TransformPrimitive {}

class RenameField extends TransformPrimitive {
  final String from;
  final String to;
}
class DefaultField extends TransformPrimitive {
  final String fieldName;
  final Object? defaultValue;
}
class DropField extends TransformPrimitive {
  final String fieldName;
}
class DeriveField extends TransformPrimitive {
  final String fieldName;
  final DerivedFieldComputation from;
}
// More primitives added as library work; closed-set discipline.
```

### Promoter chain

When the substrate folds an event whose authored payload version is `vA` and the projection's target version (per `view_target_versions`) is `vT`, it composes the chain of registered `PromoterSpec` instances `vA → vA+1 → … → vT` for `(viewName, entryType)` and applies them in order. Result is the promoted payload that the projection's fold sees. If any step in the chain is missing, boot fails with a clear "no PromoterSpec registered for diary_entries / DiaryEntry / v3 → v4" error.

### Stability

Same Append-Only Primitives discipline as Section 1: `RenameField` etc. semantics never change post-shipping. Bug fixes ship as new primitive names. PromoterSpecs in deployed projects continue to invoke the original primitive.

## Section 3 — Subscribe primitive

### Signature

```dart
Stream<Update<T>> subscribe<T>(
  SubscriptionFilter filter,
  SubscriptionMode<T> mode,
);
```

The single reactive surface of the library. No cursor — cross-process resume lives in `Destination`. Pure live stream with standard Dart Stream lifecycle.

### Subscription filter

```dart
class SubscriptionFilter {
  final Set<EventTypeId>?    eventTypes;     // null = any
  final Set<AggregateId>?    aggregates;     // null = any
  final Set<AggregateType>?  aggregateTypes; // null = any
  final Set<SourceId>?       sources;        // null = any (Phase II hook)
}
```

Pure value; no callbacks or predicates. AND-combined across dimensions. Each dimension nullable; null means "match any".

### Subscription modes

```dart
sealed class SubscriptionMode<T> {}

class Events extends SubscriptionMode<StoredEvent> {
  const Events();
}

class AggregateMode<T> extends SubscriptionMode<T> {
  final String viewName;
  final T Function(Map<String, Object?>) mapper;
}
```

Phase I ships exactly two modes: raw `Events` and aggregate-shaped `AggregateMode<T>`. A `View<T>` mode for whole-state projections (counters, latest-value, small tables) is reserved as a future mode but not implemented in v1 — the only Phase I projection that does not fit `AggregateMode` is the `role_permission_grants` table, which `AuthorizationPolicy` consumes via direct backend queries (`backend.findViewRowsInTxn`) rather than reactive subscribe. View<T>'s concrete shape is deferred to the first design driven by a real consumer.

The `mapper` is consumer-side pure deserialization — it takes a row Map and returns a typed value. The mapper does not affect what the substrate stores or computes; it produces a typed view at read time. Consumers wanting raw access can pass an identity mapper. Closed-under-events is preserved: the substrate's authoritative state remains the Map.

### Update<T> envelope

```dart
sealed class Update<T> {
  int get sequence;
}

class Snapshot<T> extends Update<T> {
  final T? value;        // null only for AggregateMode subscribed to a not-yet-existing aggregate
  final int sequence;    // log sequence the snapshot is as-of
}

class Delta<T> extends Update<T> {
  final T value;
  final int sequence;
  final EventTypeId cause;  // event type that drove the change
}

class Tombstone<T> extends Update<T> {
  final AggregateId aggregateId;
  final int sequence;
}
```

### Per-mode behavior

| Mode | First Update | Subsequent Updates |
|---|---|---|
| `Events` | (none — live only) | `Delta<StoredEvent>` per matching event |
| `AggregateMode<T>` filtered to one aggregate | `Snapshot<T>` of that aggregate (see "snapshot for absent / tombstoned aggregates" below) | `Delta<T>` for that aggregate; `Tombstone` if deleted while subscribed |
| `AggregateMode<T>` filtered to many aggregates | One `Snapshot<T>` per existing non-tombstoned matching aggregate | `Delta<T>` per row change; `Tombstone` per row deletion |

For materialized modes, the snapshot is an O(matching-rows) read of the substrate's already-computed state from the backend's view tables, then mapper-applied. Not a recomputation of the projection — projection state is current as of the last successful append.

### Snapshot for absent and tombstoned aggregates

When `AggregateMode<T>` is filtered to a specific aggregate id:

- **Aggregate has live rows** → `Snapshot<T>(value: T, sequence: lastFoldedSequence)`.
- **Aggregate has never received any event** → `Snapshot<T>(value: null, sequence: currentLogSequence)`. Substrate emits the snapshot anyway so the consumer's stream begins immediately rather than blocking. A subsequent first event for that aggregate produces a `Delta<T>` with the populated value.
- **Aggregate was tombstoned at some prior sequence** → `Snapshot<T>(value: null, sequence: currentLogSequence)`. The substrate does not retain prior state for tombstoned aggregates beyond what the row-deletion implies; subscribers see the same `null` snapshot as a never-existed aggregate. The distinction between "never existed" and "tombstoned" is recoverable from the event log itself, not from the projection.

`Tombstone<T>` Updates are emitted only as a transition signal — to subscribers who were active when the tombstone event was folded. New subscribers to an already-tombstoned aggregate receive only the `null` Snapshot.

When `AggregateMode<T>` is filtered to many aggregates (or unfiltered), the initial snapshot omits tombstoned aggregates entirely; subscribers learn about subsequent tombstones via `Tombstone<T>` Updates.

### History + live composition

`subscribe<T>(_, Events())` is from-now-forward. Historical scans use `EventStore.read(fromSequence, filter)` — finite stream that completes when caught up.

Consumers wanting "history + live" use the documented pattern:

```dart
// Subscribe FIRST so the live stream begins buffering.
final live = eventStore.subscribe(filter, Events()).listen(handle);
// Then read history.
final history = eventStore.read(fromSequence: N, filter: filter);
await for (final event in history) {
  handle(Delta(event, event.sequence, event.eventType));
}
// Dedup downstream by sequence number; events appended between subscribe and
// read completion may appear in both streams.
```

This is documented as the canonical pattern in the API docs. The library deliberately does not provide a `readThenSubscribe` helper — atomicity-via-ordering is simple enough that an additional API surface is not warranted.

### Substrate's responsibilities

- **Routing.** `interest` filters indexed at registration; on append, only matching projections invoked. Subscribers' filters indexed similarly; on projection change, only matching subscribers receive Updates.
- **Snapshot delivery.** On subscribe to a materialized mode, read the substrate's current view rows (filtered to matching aggregates if applicable), apply the mapper, emit Snapshot.
- **Delta delivery.** When a projection's row changes (via the substrate's per-shape fold), emit Delta to all subscribers whose filter matches.
- **Tombstone delivery.** When a projection's row is deleted (per `AggregateProjectionSpec.tombstoneEventTypes`), emit Tombstone to matching `AggregateMode` subscribers.
- **Atomicity at subscribe.** Snapshot and delta-stream attach must be atomic — no event lost between snapshot read and live attach. Substrate uses an internal lock (or per-projection snapshot-and-attach primitive) to serialize the seam. Consumers see one continuous stream.
- **Stream lifecycle.** Standard Dart Stream. Cancellation cleans up subscriber registration; no backend persistence of subscription state.

### At-least-once

Within-process: standard Dart Stream guarantees delivery to listeners; no persistent subscription state is required because the consumer is alive on the receiving end. Cross-process at-least-once (across app restarts, crash recovery, remote-endpoint shipping) is `Destination`'s domain — that primitive carries the persistent FIFO queue + watermark mechanism.

## Section 4 — Lifecycle and library-version tracking

### Library-version events

Two event types are part of the substrate's permanent vocabulary:

```dart
lib_version_initialized {
  version: "0.4.0",
  initializedAt: "2026-05-09T12:34:56Z",
}

lib_version_changed {
  fromVersion: "0.4.0",
  toVersion: "0.4.1",
  changedAt: "2026-05-15T08:01:23Z",
}
```

These events are appended by the substrate itself, not by application code. They live in the same log as domain events and are subject to the same hash-chain integrity guarantees.

### Bootstrap flow

On every `EventStore.open`:

1. Reverse-scan the log for the most recent `lib_version_initialized` or `lib_version_changed` event.
2. Compare the recorded version against the current library version (compiled into the substrate as a constant).
3. **No prior version recorded** → first boot under this library. Append `lib_version_initialized(version: current)`. Covers the libification migration: hht_diary's pre-existing log gets this event on first boot under the new library.
4. **Recorded version equals current** → no-op.
5. **Recorded version less than current** → upgrade. Append `lib_version_changed(from: recorded, to: current)`.
6. **Recorded version greater than current** → downgrade detected. Refuse to boot. The log was processed by a newer library that may have used primitives this version does not recognize. An explicit `EventStore.open(allowDowngrade: true)` flag opens the escape hatch for development workflows.

### Reconstructability formula

State at sequence N is:

```text
state(N) = f(events[0..N], specs_in_log[0..N], lib_version_in_log[0..N])
```

All three inputs live in the log. The substrate's source code at any version interprets the spec set and library-primitive catalogue against the events. No external inputs participate.

### Phase I composition vs Phase II spec-as-events

In Phase I, `ProjectionSpec` and `PromoterSpec` instances are passed into `EventStore.open` as composition-time configuration. The closed-under-events story for the spec set is achieved at deployment time: a given build of the application embeds a known spec set.

Phase II promotes specs to log-resident state via settings events (`register_projection(spec)`, `register_promoter(spec)`, `revoke_*`). At that point the spec set itself becomes reconstructable from the log alone, fully closing the loop.

Phase I ships the substrate ready for this transition: registries are mutable-at-init but immutable-after-open; the boot flow is a single point that Phase II extends to consume settings events from the log before serving subscribers.

## Section 5 — Composition

### EventStore.open

```dart
final eventStore = await EventStore.open(
  storage: backend,
  projections: ProjectionRegistry()
    ..register(diaryEntriesSpec)
    ..register(rolePermissionsSpec),
  promoters: PromoterRegistry()
    ..register(diaryEntryPromoterV1ToV2Spec),
);
```

`open` is async because it performs the version-check bootstrap and may emit a `lib_version_*` event. The `EventStore` constructor is private; `open` is the only public entry point.

### Registries

```dart
class ProjectionRegistry {
  void register(ProjectionSpec spec);   // throws on duplicate viewName
  ProjectionSpec? lookup(String viewName);
  Iterable<ProjectionSpec> all();
}

class PromoterRegistry {
  void register(PromoterSpec spec);     // throws on duplicate (viewName, entryType, fromVersion)
  List<PromoterSpec> chain(String viewName, String entryType, int fromVersion, int toVersion);
}
```

Registries are mutable until `EventStore.open` returns; immutable thereafter. Phase II adds settings-event-driven mutation, gated behind explicit substrate-level events that are themselves audited.

## Section 6 — Migration from existing code (greenfield)

This libify pass is greenfield: no backwards-compatibility shims, no legacy interfaces preserved alongside new ones, no migration adapters. The existing materializer interface is replaced outright.

### What is removed

- `Materializer` abstract class with `appliesTo(event)` predicate, `applyInTxn(txn, backend, ...)`, `targetVersionFor(...)`.
- `EntryPromoter` interface with `promote(payload, fromVersion, toVersion)`.
- The `MaterializerRules` placeholder (Phase II rule grammar arrives as `register_*` settings events directly).
- Per-materializer subclasses: `DiaryEntriesMaterializer`, `RolePermissionGrantsMaterializer`.

### What replaces them

- `ProjectionSpec` instances — one per view, declarative.
- `PromoterSpec` instances — one per (view, entryType, version transition), declarative.
- Library-supplied `Merge.applyDelta`, `DerivedFieldComputation`, `RowKeyExtractor`, `RowDataExtractor`, `TransformPrimitive` catalogues.
- Substrate's per-shape fold interpreter — implements the fold mechanics for `AggregateProjectionSpec` and `TableProjectionSpec`.

### What carries over

- `EventStore.append` atomicity contract (event + projection updates in one transaction).
- `StorageBackend` view-row methods (`readViewRowInTxn`, `upsertViewRowInTxn`, `deleteViewRowInTxn`, `findViewRowsInTxn`, `clearViewInTxn`).
- `view_target_versions` table for per-(view, entryType) target version.
- `rebuildView` operation semantics (clear + replay), now interpreting ProjectionSpecs.

### Existing materializers re-expressed

`DiaryEntriesMaterializer` becomes:

```dart
final diaryEntriesSpec = AggregateProjectionSpec(
  viewName: 'diary_entries',
  aggregateType: 'DiaryEntry',
  interest: SubscriptionFilter(aggregateTypes: {AggregateType('DiaryEntry')}),
  tombstoneEventTypes: {EventTypeId('tombstone')},
  derivedFields: [
    DerivedField(
      'effective_date',
      DottedPathLookup('answers.date_of_event', fallback: FirstEventTimestamp()),
    ),
  ],
);
```

The `is_complete` flag (today derived from `event.eventType` being `finalized` vs `checkpoint`) becomes a payload field on those events: writers include `is_complete: true | false` in the payload, and the generic merge folds it like any other field. This is a domain-side change to event authoring, not substrate logic.

`RolePermissionGrantsMaterializer` becomes:

```dart
final rolePermissionsSpec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(eventTypes: {
    EventTypeId('permission_granted'),
    EventTypeId('permission_revoked'),
  }),
  insertEventTypes: {EventTypeId('permission_granted')},
  removeEventTypes: {EventTypeId('permission_revoked')},
  rowKey: CompositeKey(['payload.role', 'payload.permission', 'payload.scope']),
  rowData: PayloadField('payload'),
);
```

### Stale REQ-d annotations

The 1,894 existing `REQ-d{NNNNN}` annotations covering the removed materializer machinery are deleted alongside the code that bore them. New annotations reference `EVS-DEV-*` requirements authored alongside the new substrate code.

## Section 7 — Phase II hooks

The Phase I substrate carries seams for Phase II without committing to their semantics:

- **`SubscriptionFilter.sources`** is wired into the filter type and matching engine but is functionally unused while the single-source-per-aggregate-type invariant holds. Phase II's multi-source work activates it.
- **Spec-as-event registration**: `register_projection`, `register_promoter`, and `revoke_*` event types are reserved in the substrate vocabulary. Phase I does not emit or consume them; Phase II's settings-event-driven registry feeds them through the same fold path.
- **Multi-source canonicalization rules**: tracked separately under their own future spec; not pinned here. The projection model accommodates future per-source variant rules through new `ProjectionSpec` shapes or new derivation primitives, not through a generic "custom code" escape hatch.

## Section 8 — Phase I implementation order

Earlier tracks are prerequisites for later.

1. **Library-version events + boot flow.** `lib_version_initialized`, `lib_version_changed`, `EventStore.open` version-check. Foundational; everything else relies on it.
2. **Library primitive catalogue (initial).** `Merge.applyDelta`, `DottedPathLookup`, `FirstEventTimestamp`, `ConstantValue`, `AggregateIdKey`, `CompositeKey`, `WholePayload`, `PayloadField`, `RenameField`, `DefaultField`, `DropField`, `DeriveField`. Pure utilities; no I/O.
3. **`ProjectionRegistry` + `PromoterRegistry`** with strict registration semantics (duplicate detection, chain lookup, immutable-after-open).
4. **Substrate per-shape fold interpreters.** `AggregateProjectionSpec` and `TableProjectionSpec` — read row, apply primitive sequence, write row, all inside the append `Txn`.
5. **`subscribe<T>` primitive.** Signature, sealed modes, sealed `Update<T>`, snapshot-then-deltas, atomicity at subscribe, mapper application.
6. **`rebuildView`** rewired to replay through ProjectionSpecs + PromoterSpecs.
7. **Migration of existing materializers** to ProjectionSpec form. Existing tests rewritten against the new shape; old `Materializer` class and subclasses deleted.
8. **Intra-lib demo updates** (`example/`, `example_action_permissions/`) migrated from legacy `watch*` API to `subscribe<T>`.

DEV-level requirements (`EVS-DEV-*`) are authored alongside each track's implementation. Code annotations (`// Implements:`, `// Verifies:`) reference those DEV REQs. Legacy `REQ-d{NNNNN}` annotations are deleted with the code they accompanied; not re-bound.

## References

- `2026-05-09-substrate-and-materializer-design.md` — overall component model, event log, action dispatch, ingest, storage abstraction. Authoritative for the components this spec does not cover.
- `spec/prd-subscription.md` — `EVS-PRD-subscription` charter.
- `spec/prd-materializer.md` — `EVS-PRD-materializer` charter.
- `spec/prd-action-dispatch.md` — `EVS-PRD-action-dispatch` (consumed by subscribe through `IdempotencyStore` projection).
- `spec/prd-permissions-as-events.md` — `EVS-PRD-permissions-as-events` (the closed-under-events trust model this design extends to projections uniformly).
- `CLAUDE.md` — repo-level architectural commitments; this spec realizes the "in-library materializer" and "closed-under-events" commitments at full strength.
