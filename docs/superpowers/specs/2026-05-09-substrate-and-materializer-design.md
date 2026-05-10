# Substrate and Materializer Design

**Phase**: I
**Status**: Draft (Phase I implementation target — partially superseded)
**Last updated**: 2026-05-09

> **Superseded:** The "Subscribe primitive", "Materializer", "Filter, query, and the closed-set rule", and "Multi-source readiness (Phase II hooks)" sections of this document are superseded by `2026-05-09-projections-and-subscribe-design.md`. Notably, the typed `Materializer<T>` contract sketched here is replaced by declarative `ProjectionSpec` shapes; the three-mode subscription (`Events` / `View<T>` / `AggregateMode<T>`) is narrowed to two for Phase I (`Events`, `AggregateMode<T>`); `View<T>` is deferred. This document remains authoritative for the EventStore log layout, action dispatch, ingest, storage abstraction, and the overall component model.

## Scope

This document specifies the design of the event-sourcing substrate — the storage, ingest, materializer, subscription, and action-dispatch components — at the level a Phase I implementer needs to build it. Above this document: the PRDs in `spec/prd-*.md` (what the library commits to). Below this document: implementation, with DEV-level requirements (`EVS-DEV-*`) authored alongside the code.

In scope:

- The `subscribe<T>(filter, mode)` primitive — signature, modes, semantics.
- The typed `Materializer<T>` — fold contract, rules-as-events configuration, replay behavior.
- The event log — append-only structure, per-deployment chain anchoring.
- The action-dispatch flow — pipeline stages, atomic outcome recording.
- The ingest path — preserves upstream identity and provenance, integrates into the local log.
- The storage abstraction — pluggable backend interface that consumers supply per platform.
- Phase II hooks — multi-source canonicalization seams that are present in v1 but dormant.

Out of scope (deferred):

- Multi-source canonicalization rule grammar beyond the bare seams (Phase II authors the rules and the rule grammar).
- Notification delivery semantics (separate design once a concrete consumer demands it).
- DataInvalidation semantics distinct from notifications (subsumed by multi-editor work in Phase II / III).
- Performance benchmarking and optimization (post-Phase I).

## Component model

The substrate decomposes into eight components. Each has a single responsibility; components communicate via well-defined Dart interfaces.

```text
+-------------------+      +-------------------+
|  ActionDispatcher | ---> |    EventStore     |
+-------------------+      +-------------------+
                                |       ^
                                v       |
+-------------------+      +-------------------+
|     Ingest        | ---> |    EventStore     |
+-------------------+      +-------------------+
                                |
                                v
+-------------------+      +-------------------+
|  Materializer<T>  | <--- |  StorageBackend   |
+-------------------+      +-------------------+
        |
        v
+-------------------+
|   Subscription    |
+-------------------+
```

| Component | Responsibility |
|---|---|
| `EventStore` | Append-only log; computes hashes; assigns sequence numbers; persists via `StorageBackend` |
| `StorageBackend` | Pluggable persistence; abstract interface implemented per platform (sembast on mobile/server, IndexedDB on web, etc.) |
| `Materializer<T>` | Folds the event log into typed state `T`; configured by rules (settings events); pure replay |
| `Subscription` | Reactive delivery primitive; serves filtered streams of events or materialized state |
| `ActionDispatcher` | The single path for consumer-initiated state changes; runs the parse → validate → authorize → execute → record pipeline |
| `Ingest` | Admits events from another deployment while preserving upstream identity; the inbound counterpart of `Destination` |
| `Destination` | Outbound delivery channel; FIFO queue + pluggable transport; configured per consumer |
| `AuthorizationPolicy` | Evaluates principal+action against permission projections (also held in the log) |

`EventStore` is the nucleus; everything else either writes to it (dispatcher, ingest) or reads from it (materializer, subscription).

## Subscribe primitive

The substrate exposes a single subscription primitive:

```dart
Stream<Update<T>> subscribe<T>(SubscriptionFilter filter, SubscriptionMode<T> mode);
```

Where:

- `T` is the typed payload of the subscription (see `SubscriptionMode`).
- `filter` selects which events are visible to this subscription.
- `mode` determines what the subscription delivers — events, materialized state, or aggregate state.

There are no other subscription methods. `subscribe<T>` is the entire reactive surface of the library.

### Subscription filter

A `SubscriptionFilter` is a structured value matching events on three orthogonal dimensions:

```dart
class SubscriptionFilter {
  final Set<EventTypeId>? eventTypes;     // null = any
  final Set<AggregateId>? aggregates;     // null = any
  final Set<SourceId>?    sources;        // null = any (multi-source dormant in Phase I)
}
```

Filters are pure values — no callbacks, no expressions. Each dimension is independently nullable; null means "match any". The substrate AND-combines the dimensions.

### Subscription modes

Three modes share the `subscribe<T>` primitive. Each is a typed `SubscriptionMode<T>`:

```dart
sealed class SubscriptionMode<T> {}

class Events           extends SubscriptionMode<StoredEvent>;
class View<T>          extends SubscriptionMode<T>;
class AggregateMode<T> extends SubscriptionMode<AggregateState<T>>;
```

| Mode | What the consumer receives |
|---|---|
| `Events` | Each event matching the filter, in log order |
| `View<T>` | Slices of materialized state derived by the configured materializer; updates whenever an event in the filter changes the slice |
| `AggregateMode<T>` | Per-aggregate state, one stream item per aggregate change; the primary mode for application code |

`AggregateMode<T>` is the primary mode. Most consumer code wants "tell me the current state of aggregate X, and update me when it changes" — this mode answers that question directly. `View<T>` and `Events` are for cases where the consumer needs cross-aggregate views or raw event access.

### Delivery semantics

Per `EVS-PRD-subscription`:

- **Reactive**: deliveries happen as events are ingested, without consumer polling.
- **Order-preserving**: deliveries follow the log's order within the scope of the subscription.
- **At-least-once**: every matching event reaches the consumer at least once, including across reconnect. Consumer-side dedup against the event hash is the standard pattern.

A subscription tracks a watermark (the last delivered event's sequence number). On reconnect or restart, delivery resumes from the watermark plus one. The watermark is per-subscription and persisted with the consumer's state.

### Source filter and multi-source

The `sources` dimension on `SubscriptionFilter` selects events by their originating deployment. In Phase I, every aggregate has exactly one source (single-source-per-aggregate-type invariant), so the filter dimension is functionally unused for routine consumers. The dimension is wired in v1 so Phase II's multi-source work has a no-retrofit place to attach.

## Materializer

`Materializer<T>` derives typed application state from the event log.

```dart
abstract class Materializer<T> {
  T initialState();
  T fold(T state, StoredEvent event);
}
```

The substrate calls `fold` for each event in turn. Replay determinism (`EVS-PRD-materializer-B`) requires `fold` to be a pure function over its inputs.

### Rules as events

Materializer behavior is parameterized by **rules**, which are events of distinguished settings types:

```text
set_canonicalizer(aggregateId, ruleSpec)
delegate_canonicalization(aggregateId, fromAuthority, toAuthority)
revoke_canonicalization_delegation(aggregateId, ...)
... (rule space extended in Phase II)
```

When the materializer encounters a settings event during fold, it updates its rule state for the affected aggregate and applies the new rule to subsequent events. Because rules are events, the rule state at any point in the log is reconstructable from the log alone — satisfying `EVS-PRD-materializer-C`.

In Phase I, the rule space contains exactly one rule type: the implicit `originator-of-first-event-is-canonical` default. No settings events of canonicalization type are written. The rule machinery exists in code (`MaterializerRules`) but operates on a single fixed rule. Phase II expands the rule space.

### Replay and checkpointing

Determinism (`EVS-PRD-materializer-B`) means `fold(state₀, [e₁..eₙ])` is byte-identical regardless of when or how often it is invoked. This admits:

- **Cold start**: rebuild state from event 0.
- **Warm resume**: persist state at sequence N, resume by replaying events from N+1.
- **Audit replay**: a regulator with the log alone reproduces the same state.

The substrate provides cold-start replay in v1. Warm-resume checkpointing is mechanism that consumers manage; the substrate persists no checkpoints itself.

## Event log

`EventStore` is the append-only log.

```dart
abstract class EventStore {
  Future<StoredEvent> append(EventDraft draft);
  Stream<StoredEvent> read({int fromSequence = 0, SubscriptionFilter? filter});
  Future<int> currentSequence();
}
```

Per `EVS-PRD-event-log`:

- `append` is the only mutating operation; it never modifies prior events.
- Each append assigns a globally unique, monotonic sequence number.
- Per-aggregate-per-authority order is preserved (the relative order of events from the same authority within an aggregate is the order the events were appended).
- `read` streams events from any starting sequence.

### Hash chain anchoring

Per `EVS-PRD-hash-chain-integrity`:

- Each event carries a hash deterministically derived from its canonical-form content (via `canonical_json_jcs`).
- Each event chains to its predecessor in its authority's chain.
- Each chain is anchored to the deployment that produced its first event — a stable, deployment-identifying value (e.g., a deployment UUID minted at first-run).

In Phase I (single-source-per-aggregate), each deployment has exactly one chain. The local log is a single chain end-to-end. Phase II's multi-source work introduces additional chains per ingested authority, intersecting in the same log.

### Storage abstraction

`EventStore` delegates persistence to `StorageBackend`:

```dart
abstract class StorageBackend {
  Future<void> writeEvent(StoredEvent event);
  Stream<StoredEvent> readEvents({int fromSequence});
  Future<int> currentSequence();
  Future<void> close();
}
```

The library ships at least one reference backend (sembast-based, suitable for mobile and CLI/server use). Web targets supply an IndexedDB-based backend; high-throughput server targets may supply Postgres or another. Each is a pure-Dart implementation of the same `StorageBackend` interface.

## Action dispatch

`ActionDispatcher` is the single path for consumer-initiated state changes (`EVS-PRD-action-dispatch-F`).

```dart
abstract class ActionDispatcher {
  Future<DispatchResult> dispatch(ActionSubmission submission);
}
```

A dispatch runs five stages, in order, each gated on the previous succeeding:

| Stage | Failure event |
|---|---|
| 1. Parse | `parse_denied` |
| 2. Validate | `validation_denied` |
| 3. Authorize | `authorization_denied` |
| 4. Execute | `execution_failed` |
| 5. Record | (a successful dispatch records one or more domain events here) |

Every dispatch produces exactly one terminal outcome event in the log: either the success events from stage 5, or a denial event identifying the stage of failure with a reason. Denial events include enough detail for audit (`EVS-PRD-action-dispatch-C`).

### Action registry

Actions are registered with the dispatcher at composition time. Each `Action` declares:

- Its action type identifier (a stable string).
- Its parser (deserializes wire input to typed Dart).
- Its validator (shape-and-business-rule checks).
- Its required permission (used by `AuthorizationPolicy`).
- Its executor (the application logic that produces success events).

The registry is closed-set at runtime; new actions require recomposing the dispatcher.

### Idempotency

`EVS-PRD-action-dispatch-D` and `-E`:

- Each `ActionSubmission` carries a stable `actionIdentifier` chosen by the consumer (typically a UUID).
- Re-dispatching a submission with the same identifier and matching content returns the original outcome (the dispatcher consults an `IdempotencyStore` projection of past outcomes).
- Re-dispatching with the same identifier but different content produces a `duplicate_identifier_denied` denial event.

The `IdempotencyStore` is a projection of the dispatch-outcome events — itself derived from the log via the materializer.

## Ingest

`Ingest` is the inbound counterpart of `Destination`. It admits events from another event-sourcing deployment while preserving the upstream's identity, hash chain, and provenance.

```dart
abstract class Ingest {
  Future<IngestResult> admit(IngestedEvent event);
}
```

Per `EVS-PRD-ingest`:

- The admitted event retains its upstream hash, originating authority, and provenance chain.
- The local deployment appends its hop to the provenance chain.
- The hash chain is verified against the upstream's chain before admission; any mismatch produces an `ingest_chain_mismatch` integrity-violation event and rejects the admission.
- Admission is idempotent on the upstream event hash.

The `IngestedEvent` payload is a wire-format transit record that carries the upstream's full event plus the upstream's chain context (predecessor hash, anchor identity). The transport that delivered the event to the ingest path is opaque to the substrate — applications choose HTTP, WebSocket, file transfer, etc.

### Distinction from `ActionDispatcher.dispatch`

`dispatch` produces *new* events whose authority is this deployment's principal. `Ingest.admit` carries *existing* events whose authority is upstream. Both write to the same `EventStore`, but the resulting events are distinguishable by their authority and by their chain anchor.

## Permissions and authorization

Per `EVS-PRD-permissions-as-events`, permission grants and role assignments are events in the same log. The materializer maintains a permissions projection:

```dart
abstract class AuthorizationPolicy {
  AuthorizationDecision decide(Principal principal, ActionContext context);
}
```

The `ActionDispatcher` consults `AuthorizationPolicy` at stage 3. The policy's `decide` method reads from the materializer's permissions projection — never from any external IAM authority at decision time. External identity assertions (OIDC tokens, etc.) are translated into events at the ingest boundary, never consulted at decision time.

The policy returns `AuthorizationGranted` or `AuthorizationDenied(reason, missingPermission)`. Denial reasons feed the `authorization_denied` event content.

## Filter, query, and the closed-set rule

The substrate's queries are expressed via `SubscriptionFilter`. There is no query DSL beyond filter dimensions. Application code that needs cross-cutting queries reads from materializer projections, not from ad-hoc event-log scans.

Materialization is the closed-set boundary: every projection is a function of the event log under the materializer's rules. There are no external write paths into projections, no callback escape hatches, no host-supplied projections. This satisfies the closed-under-events trust model: every value the application reads can be traced to an event in the log.

## Multi-source readiness (Phase II hooks)

Phase I implements single-source-per-aggregate-type. The substrate carries the seams Phase II activates:

- `SubscriptionFilter.sources` is wired but rarely set.
- The materializer's `MaterializerRules` machine processes a single fixed rule (originator-of-first-event-is-canonical) but is structured to load additional rules from settings events.
- Hash chains are stored per-authority (a single chain in v1; multiple intersecting chains in Phase II).
- The dispatcher records authority on every event; subscriptions can filter by authority. Phase II's approval pattern (events approving non-canonical edits) writes via the same dispatcher path.

Phase II authors:

- The rule grammar (concrete `set_canonicalizer`, `delegate_canonicalization`, etc. event types).
- The approval pattern's event types and the materializer rules that consume them.
- Per-entry-type resolution policies (a product decision, not lib code).

Phase II does not add new substrate primitives. It populates rules.

## Phase I implementation order

The Phase I work breaks into ordered tracks. Earlier tracks are prerequisites for later.

1. **Storage backend interface + reference implementation** (sembast-based). Pure persistence; no domain coupling.
2. **EventStore on top of StorageBackend.** Append, read, sequence assignment, hash computation.
3. **Materializer<T> + replay.** Pure folder over the event log; checkpointing as documentation, not as code.
4. **Subscription primitive.** `subscribe<T>(filter, mode)` over EventStore; watermarks; reconnect handling.
5. **AuthorizationPolicy + permissions projection.** A specialized `Materializer<PermissionsState>` plus a policy that reads from it.
6. **ActionDispatcher.** Wires registry + parse/validate/authorize/execute/record stages; idempotency via `IdempotencyStore` projection.
7. **Ingest path.** Hash-chain verification at admission; provenance hop extension.
8. **Destination outbound path.** FIFO queue persisted to the same StorageBackend; pluggable delivery implementation.
9. **Intra-lib demo updates** (`example/`, `example_action_permissions/`). Migrate from the legacy API to `subscribe<T>` and the typed `Materializer<T>`.

DEV-level requirements (`EVS-DEV-*`) are authored alongside each track's implementation. Code annotations (`// Implements:`, `// Verifies:`) reference those DEV REQs, not the legacy `REQ-d{NNNNN}` IDs from the kick-start (which are swept during the DEV REQ pass).

## Versioning

Phase I cuts version `0.4.0`. Phase II cuts `0.5.0`. Phase III in `cure-hht/hht_diary` bumps the lib pin from the kick-start's `0.3.0+5` to `0.5.0` (or later) and sweeps consumer call sites for the new API.
