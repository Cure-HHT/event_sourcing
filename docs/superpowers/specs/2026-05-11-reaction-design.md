# `reaction` and `reaction_widgets` — Design

**Status:** Draft (brainstorm output, awaiting user review).
**Date:** 2026-05-11.
**Authors:** Brainstormed in conversation between Developer and Claude (Opus 4.7).
**Supersedes:** none.
**Related:** `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md`, `docs/superpowers/specs/2026-05-11-roadmap.md`, CLAUDE.md "Architectural commitments" + "Trust boundaries" + "Epistemic layers".

## Purpose

`reaction` is a substrate-agnostic, pure-Dart layer that lets application code (mobile or web) submit actions and subscribe to view changes against either an in-process `EventStore` + `ActionDispatcher` (Use 1: embedded) or a remote portal server over HTTP+WS (Use 2: web frontend talking to a pure-Dart server). `reaction_widgets` is a Flutter widget library on top of `reaction` providing reactive widget primitives, sugar widgets, and an `InheritedWidget` context.

The two packages exist together to give Cure-HHT one widget vocabulary across the mobile diary and the sponsor portal, while keeping the substrate (`event_sourcing/`) free of any wire-protocol or UI concern and free of any application-domain code.

## Goals

1. **One widget vocabulary, two transports.** A Flutter widget consuming an `ActionSubmitter` or `ViewSource` works identically whether the underlying wire is an in-process `EventStore` (mobile) or a remote portal server (web). The widget never knows which transport it is on.
2. **Substrate stays substantially as-is.** The only substrate addition this design needs is one new variant of `Update<T>` (`EndOfReplay<T>`). Everything else lives in `reaction` and `reaction_widgets`.
3. **Reuse the substrate's reactive primitive.** Cross-process change-feed delivery is a JSON serialization of the existing `subscribe<T>` `Stream<Update<T>>` over WebSocket, not a bespoke wire protocol. The substrate's race-free snapshot-then-deltas guarantee carries over to the wire for free.
4. **Auth boundary stays pluggable.** `reaction` defines a `PrincipalAuthValidator` interface and ships two reference impls (trusting; possibly JWT). The mechanism that closes the Principal-on-faith trust boundary (linking-code bootstrap on mobile; Firebase ID tokens on portal) is consumer-supplied and consumer-mounted.
5. **Domain-neutral.** No diary or portal types in either package. All shipped widgets are abstract over (action type, view name, row mapper, principal). hht_diary supplies its own action types, view specs, and row mappers.

## Non-goals (v1)

- Connection-state observability on `Remote*` impls (`Stream<ConnectionState>` for "Reconnecting…" UX). Defer to v1.1.
- Long-running action progress streams. Substrate dispatch is atomic; YAGNI.
- Batch action submission API. Per-action submission preserves audit clarity.
- Optimistic-update primitives. Apps build on top of `ActionSubmitter` + `ViewSource`.
- Substrate-introspection sugar widgets (`EventLogView`, `TraceView`). Substrate-shaped, valuable, but additive — defer to v1.1 once the core is proven.
- File upload primitives. Not used in the current portal; out of scope.
- `reaction_widgets_riverpod` adapter. Defer until demand exists. `reaction_widgets_provider` ships in v1 because portal-ui already uses Provider 6.x.
- View pagination / cursor-based row delivery. Defer until large-view scale demands it.
- Closing the Principal-on-faith trust boundary at the substrate level. `reaction` provides the seam (`PrincipalAuthValidator`); the closure remains app-domain.

## Background

### What exists in the substrate today

- `EventStore.subscribe<T>(filter, mode)` returns `Stream<Update<T>>` with three variants: `Snapshot<T>` × N (initial replay rows), then `Delta<T>` / `Tombstone<T>` × ∞ (live changes). The transition is atomic via the existing `_replayDone` flag pattern (no gap, no overlap). Every variant carries its `sequence`.
- `ActionDispatcher.dispatch(submission)` runs the parse → validate → authorize → execute → record pipeline and returns a `DispatchResult` (Success with emitted events, or one of the denial variants).
- `IdempotencyPolicy.{none, optional, required}` per-action with `IdempotencyStore` keyed on `(principalId, idempotencyKey)`.
- `RolePermissionGrants` is a substrate-shipped `TableProjectionSpec`; `PermissionSnapshot` is computed per Principal.
- The reference shelf-based demo in `event_sourcing/example_action_permissions/` shows the action-permissions flow end-to-end but explicitly leaves out cross-process reactive delivery (its README lists "no SSE/WS/change-feed subscriptions; the inspector polls" as a deliberate omission).
- The substrate's `Destination`/`ingest` mechanism handles cross-install event replication for the existing diary↔portal sync at scale (100s–1000s of mobile installs). `reaction`'s wire is **independent** of this — it is for portal-UI ↔ portal-server traffic only, where concurrency is low (~1 user typically, ~20 max).

### What hht_diary's portal looks like today

The current sponsor portal at `cure-hht/hht_diary/apps/sponsor-portal/` is a working Flutter web client (`portal-ui/`) plus pure-Dart shelf server (`portal_server/`). It is not built on top of `event_sourcing`. It uses:

- Firebase Auth + Identity Platform for credentials (~1535 LOC `AuthService` covering login, multi-role selection, inactivity warning, cross-tab collision detection)
- ~20–30 bespoke REST POST handlers, each with ad-hoc result/error shapes
- Imperative REST GETs in `initState` + `setState` (no real-time, no view streaming)
- Inline role + site checks per handler (no permission projection)
- HTTP only; no WebSocket
- `Provider` 6.x as the dependency container; `ChangeNotifier` for `AuthService`; mostly `StatefulWidget` / `setState` for views

`reaction` is the substrate-agnostic seam that the portal eventually migrates onto, replacing or complementing each of those layers.

## Architectural commitments inherited

Per `event_sourcing/CLAUDE.md`:

- **Substrate stays domain-neutral.** `reaction` is application-facing, but neither it nor `reaction_widgets` ship hht_diary types.
- **Single-source-per-aggregate-type today.** `reaction`'s wire serves portal-UI ↔ portal-server only. The portal-server itself remains the canonical authority for portal-aggregates; mobile installs remain canonical for their own diary aggregates and sync via the existing substrate `Destination`/`ingest` path. Phase II multi-source canonicalization is **not** activated by this design.
- **Permission policy is substrate code.** `reaction`'s `PermissionSource` reads from the substrate's existing `RolePermissionGrants` projection; `reaction` does not introduce alternative policy mechanisms.
- **Closed-under-events trust model.** `reaction`'s `Remote*` impls do not consult any decision-time authority outside the log; they read derived state via `ViewSource` and `PermissionSource`. The auth credential is at the wire boundary; once validated, the Principal flows through standard substrate channels.
- **Trust boundaries enumerated.** `reaction` does **not** expand the substrate's trust boundary set. The Principal-on-faith boundary remains; `reaction` provides the pluggable seam (`PrincipalAuthValidator`) where consumer code closes it for a given deployment.
- **Layer 1 facts vs Layer 2 conventions** (Charter Assertion I). `reaction`'s wire ships **Layer 1 facts** (sequenced `Update<T>` events with intact ordering and identity), and the receiver applies **Layer 2 conventions** (the same `ProjectionSpec` interpretation that the server applies). The wire transport does not introduce a new epistemic layer.

## Architecture overview

```text
event_sourcing-worktrees/CUR-1317-libify-event-sourcing/
  event_sourcing/         (substrate, exists)
    + small additions:
      - EndOfReplay<T> as a 4th Update<T> variant
  canonical_json_jcs/     (exists, unchanged)
  provenance/             (exists, unchanged)
  reaction/               (NEW, pure Dart, depends on event_sourcing)
    lib/
      src/
        interfaces/      AuthSession, ActionSubmitter, ViewSource,
                         PermissionSource, PrincipalAuthValidator
        local/           Local* impls wrapping substrate APIs
        remote/          Remote* impls (HTTP for actions, WS for views)
        server/          shelf-based reference server: HTTP routes for
                         actions + permission snapshots + WS handler that
                         multiplexes subscriptions over one connection
        wire/            JSON codecs for ActionSubmission, DispatchResult,
                         Update<T> envelopes, SubscribeMessage, PrincipalAuthClaim
        state/           ActionState sealed type + idempotency-key generation
  reaction_widgets/       (NEW, Flutter, depends on reaction)
    lib/
      src/
        scope/           ReActionScope InheritedWidget
        action/          ActionBuilder + ActionButton
        view/            ViewBuilder + ViewListView + ViewListener
        permission/      PermissionGate
        error/           ReActionErrorListener (centralized error sink)
  reaction_widgets_provider/ (NEW, Flutter, depends on reaction_widgets + provider)
    lib/                  Tiny adapter; ChangeNotifier wrappers around
                          AuthSession / PermissionSource / ActionState; one-line
                          context.watch<...>() ergonomics for portal-ui's existing
                          Provider patterns
```

Dependency direction is one-way: `reaction_widgets_provider → reaction_widgets → reaction → event_sourcing`. The portal-server depends on `reaction` only (no Flutter in a Dart server). The mobile app depends on `reaction_widgets` (transitively `reaction` and `event_sourcing`). The web portal-ui depends on `reaction_widgets` and `reaction_widgets_provider` (transitively the rest).

## Substrate additions

### `EndOfReplay<T>` — a 4th `Update<T>` variant

Surfaces the existing `_replayDone` flag pattern as an explicit, observable event in the subscription stream. Necessary so widget consumers (`ViewBuilder`, etc.) can render a deterministic loading state until the snapshot completes, then transition to the live view.

```dart
sealed class Update<T> {
  int get sequence;
  const Update();
}

class Snapshot<T>     extends Update<T> { final T? value; final int sequence; }
class EndOfReplay<T>  extends Update<T> { final int sequence; }   // NEW
class Delta<T>        extends Update<T> { final T value; final int sequence; final String cause; }
class Tombstone<T>    extends Update<T> { final String aggregateId; final int sequence; }
```

`sequence` on `EndOfReplay<T>` is the sequence at which initial replay completed — i.e., the high-water mark of the snapshot, useful as a resume cursor for clients that want to track "how far into the log I have observed."

This is a breaking change to anyone exhaustively switching on `Update<T>`. Production impact is negligible (one runtime type check in the demo's `bootstrap.dart`; one doc-comment example in `event_sourcing.dart`). Tests that switch exhaustively on `Update<T>` will break loudly via Dart's exhaustive-switch checker — desired behavior. Lands cleanly under greenfield discipline.

This is the **only** substrate addition the snapshot+tail wire protocol requires.

## Architectural decisions

### Position D: snapshot + tail (the wire's semantic shape)

The web client does not run a full `EventStore` and does not re-derive views from event history. Instead, on subscribe:

1. The server runs `subscribe<T>(filter, AggregateMode(viewName, mapper, aggregates))` against its own `EventStore`. The substrate's atomic snapshot-then-deltas guarantee delivers `Snapshot<T>` × N → `EndOfReplay<T>` → live `Delta<T>` / `Tombstone<T>` × ∞ on its `Stream<Update<T>>`.
2. The server's WS handler serializes each `Update<T>` instance to JSON and ships it to the requesting client over the multiplexed WebSocket.
3. The client's `RemoteViewSource` deserializes incoming envelopes, applies the consumer's `mapper` client-side, and emits the same `Stream<Update<T>>` to widget code.

This means **the snapshot and the tail are the same stream**. There is no race between "fetching a snapshot" and "opening a tail," because the substrate's `subscribe<T>` already prevents that race in-process. The wire is just a transport for the existing semantics.

The mobile client, by contrast, runs a real `EventStore` in-process. `LocalViewSource` calls `eventStore.subscribe<T>(...)` directly. Same `Stream<Update<T>>` shape; no wire involved. **Widget code is identical between the two.**

### Two sibling packages, not in `event_sourcing/`

`reaction` is a sibling of `event_sourcing/`, `provenance/`, `canonical_json_jcs/`, not a sub-module of `event_sourcing/lib/src/`. Reasons:

1. **Dependency hygiene.** `reaction` will pull in `package:http`, `package:web_socket_channel`, and `package:shelf` (for the server module). Folding those into `event_sourcing/pubspec.yaml` forces them onto every substrate consumer.
2. **Epistemic layering.** The substrate is Layer 1 facts + Layer 2 conventions (CLAUDE.md "Epistemic layers"). `reaction` is application-facing API on top of those layers. Different epistemic role; cleaner kept apart.
3. **Pattern consistency.** Repo already established the sibling-package idiom for `provenance` and `canonical_json_jcs`.
4. **Dependency direction is one-way and obvious:** `reaction → event_sourcing`, never reverse.

`reaction_widgets` is a Flutter sibling of `reaction`, again as its own package because its dependencies (Flutter SDK, Material) are non-trivial and a pure-Dart consumer of `reaction` (e.g., the portal-server itself) should not transitively pull them in.

### State management is agnostic; Provider adapter ships

`reaction_widgets`' core widgets are built on Flutter primitives (`StreamBuilder`, `ValueListenableBuilder`, `InheritedWidget`). They make no commitment to Riverpod, BLoC, or any other state-mgmt library. Consumers wrap the exposed Streams/Listenables with whatever they prefer.

A separate `reaction_widgets_provider` package ships in v1 because the existing portal-ui uses Provider 6.x heavily. The adapter is small (~50–100 LOC of `ChangeNotifier` wrappers) and lets portal-ui consume `AuthSession` / `PermissionSource` / `ActionState` via the existing `context.watch<...>()` pattern.

A `reaction_widgets_riverpod` adapter is **deferred** until there is demand. The agnostic core means writing a Riverpod adapter is ~50 LOC of `StreamProvider` wrappers for any consumer who wants one.

### Wire transport: HTTP for actions + WebSocket for view subscriptions

| Concern | Transport |
|---|---|
| Action submission | HTTP POST `/actions/{actionType}` with JSON body, `X-Principal-Auth-Credential` header. Returns JSON `DispatchResult`. |
| View subscription | WebSocket `/ws`. One connection per client multiplexes N subscriptions via small control protocol (`subscribe { subscriptionId, viewName, mapper-key, filter, aggregates }`, `unsubscribe { subscriptionId }`, server-pushed `update { subscriptionId, ...Update<T>... }`). |
| Permission snapshot | HTTP GET `/permissions/snapshot` for initial fetch; subsequent changes flow over the same WS multiplex via a permission-projection subscription scoped to the active Principal. |

Choices:

- HTTP for actions matches the existing portal-server's pattern (shelf POST handlers); apps already familiar with REST tooling stay productive; standard infrastructure (proxies, Idempotency-Key header semantics, observability) all apply.
- WS for view subscriptions because the server pushes changes asynchronously, multiplexing is needed (a portal user typically opens 3–10 subscriptions), and a control plane ("subscribe", "unsubscribe") fits WS more naturally than SSE.
- Submission of actions over the same WS connection was rejected: it would conflate request/response with async push and lose HTTP affordances for actions.
- SSE for view subscriptions was rejected: subscription multiplexing on SSE is awkward (one connection per sub, browser-limited in HTTP/1.1) and SSE has no native control plane.

### Concurrency profile

`reaction`'s WS server-side load is **portal UI users only** — ~1 typical, ~20 maximum, intermittent. It is sized for trivial throughput; no connection pooling, sharded brokers, or fancy scaling primitives. The 100s–1000s of mobile concurrency uses the **substrate's existing `Destination`/`Ingest` sync** — that is a different path, not `reaction`'s WS. Already designed to handle that scale.

## The four interfaces (`reaction/lib/src/interfaces/`)

### `AuthSession` — credential lifecycle and Principal access

```dart
sealed class AuthStatus {}
class Authenticated   extends AuthStatus { final Principal principal; }
class NotAuthenticated extends AuthStatus {}
class Expired         extends AuthStatus {}

abstract interface class AuthSession {
  AuthStatus get current;
  Stream<AuthStatus> get stream;
  void setCredential(String? credential);
  Principal? get principal;
}
```

**`LocalAuthSession`** holds a `Principal` directly. Mobile reads from device storage on boot, calls `setCredential(installUuid)` once at startup; no expiration concept; status is `Authenticated` whenever a Principal is set.

**`RemoteAuthSession`** holds the credential string. Each `Remote*` interface (ActionSubmitter, ViewSource, PermissionSource) consults it for outbound request auth headers / WS handshake field. On any wire response indicating auth failure (HTTP 401, WS auth-rejected close-frame), transitions to `Expired`, emits on `stream`, closes outstanding subscriptions, and outstanding `submit()` futures complete with an `AuthenticationRequired` denial variant. Timeout duration is **derived from the credential's `exp` claim**, not baked into the interface.

The existing portal-ui `AuthService` (1535 LOC, sponsor-configurable inactivity warning, multi-role selection, cross-tab collision detection, etc.) **stays** as the consumer-side wrapper. It calls `authSession.setCredential(firebaseIdToken)` after Firebase login completes and listens to `authSession.stream` to drive its own UX. None of that 1535 LOC moves into `reaction`.

### `ActionSubmitter`

```dart
abstract interface class ActionSubmitter {
  Future<DispatchResult> submit(ActionSubmission submission);
}
```

**`LocalActionSubmitter`** delegates to `ActionDispatcher.dispatch(submission)`.

**`RemoteActionSubmitter`** does HTTP POST `/actions/{actionType}` with the submission JSON-encoded; deserializes the wire `DispatchResult` from the response. Carries the `X-Principal-Auth-Credential` header populated from `AuthSession.current`.

The widget-side `ActionState` sealed type (`Idle` / `Submitting` / `Success` / `Denied` / `Failed`) is the **widget's** local state machine — `ActionBuilder` calls `submit()` and tracks the `Future`'s lifecycle. Not part of the interface contract.

### `ViewSource`

```dart
abstract interface class ViewSource {
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  });
}
```

**`LocalViewSource`** delegates to `eventStore.subscribe<T>(filter, AggregateMode(viewName: ..., mapper: ..., aggregates: ...))`.

**`RemoteViewSource`** opens (or reuses) the shared WS connection, sends a `subscribe` control message with `(subscriptionId, viewName, filter, aggregates)`, receives serialized `Update<Map<String, Object?>>` envelopes, applies the consumer's `mapper` client-side. Each envelope carries its `subscriptionId` for multiplexing.

### `PermissionSource`

```dart
abstract interface class PermissionSource {
  PermissionSnapshot? get current;
  Stream<PermissionSnapshot?> get stream;
}
```

The active Principal is read from the wired-in `AuthSession` (no `setPrincipal` mutator on `PermissionSource` itself). When `AuthSession.principal` changes, `PermissionSource` automatically re-fetches and re-emits.

**`LocalPermissionSource`** wraps the existing `RoleMatrixReader` + `PermissionSnapshot` machinery; subscribes to the `RolePermissionGrants` view via local `subscribe<T>`; recomputes the snapshot when relevant rows change.

**`RemotePermissionSource`** does HTTP GET `/permissions/snapshot?principalId=...` for the initial snapshot; opens a WS subscription on the `RolePermissionGrants` view filtered to the active Principal for incremental updates.

### `PrincipalAuthValidator` (server-side seam)

```dart
abstract interface class PrincipalAuthValidator {
  Future<Principal> authenticate(String credential);  // throws AuthenticationDenied
}
```

Reference impls shipped:

- **`TrustingAuthValidator`** — accepts any string verbatim as `Principal.id`. Dev/test only. Loud "DO NOT USE IN PRODUCTION" docstring.

Deferred (pluggable from day one):

- **`JwtAuthValidator`** — verifies a JWT against a configured public key / issuer. Pure utility; no opinion on what claims mean. Defer until consumer demand.
- **Firebase ID token validator** — for portal-server. App-domain implementation in hht_diary; not shipped by `reaction`.

### Custom-route registration (consumer onboarding endpoints)

`reaction`'s shelf server lets consumers register additional routes (e.g., `/onboard/linking-code`, `/login`, `/auth/refresh`) **outside** the standard `reaction` action/subscribe routes. Routes registered this way **bypass the auth-validator middleware**, so consumer code can implement bootstrap flows (no Principal yet) and credential-issuance flows.

API contract: registered routes do not get auth middleware. Specific syntax (e.g., `server.addCustomRoute(...)` vs exposing the `Router` for direct mutation) is an implementation detail and may be revised.

## The widget layer (`reaction_widgets/lib/src/`)

### `ReActionScope` — root InheritedWidget

Threads the four interfaces + active Principal down the tree. Mounted once near the app root, above `MaterialApp`.

```dart
ReActionScope(
  authSession: authSession,
  actionSubmitter: submitter,
  viewSource: viewSource,
  permissionSource: permissionSource,
  child: MyApp(),
);

// Children resolve via:
final source = ReActionScope.of(context).viewSource;
```

Implemented as nested `InheritedWidget`s; the `_provider` adapter package wraps the same dependencies as `Provider.MultiProvider` for ergonomic compatibility with portal-ui's existing patterns.

### Action widgets

**`ActionBuilder<TAction, TResult>`** — Builder primitive:

```dart
ActionBuilder<SubmitNote, NoteSubmitted>(
  buildSubmission: (ctx) => ActionSubmission(
    actionType: 'submit_note',
    payload: {...},
    principalId: ReActionScope.of(ctx).authSession.principal!.id,
    // idempotencyKey auto-generated unless overridden
  ),
  builder: (ctx, state) => switch (state) {
    Idle()        => ElevatedButton(onPressed: state.submit, child: Text('Save')),
    Submitting()  => CircularProgressIndicator(),
    Success(:final result) => Text('Saved'),
    Denied(:final reason)  => Text('Denied: $reason'),
    Failed(:final error)   => Text('Error'),
  },
)
```

**`ActionButton<TAction, TResult>`** — sugar over `ActionBuilder` for the common case. Default rendering: button label + loading spinner during submit; auto-disabled when `Submitting` or when permission denied (consults `PermissionSource`); failure surfaces through `ReActionErrorListener`.

### View widgets

**`ViewBuilder<TRow>`** — Builder primitive:

```dart
ViewBuilder<DiaryEntry>(
  viewName: 'today_entries',
  mapper: DiaryEntry.fromRow,
  builder: (ctx, state) => switch (state) {
    Loading()             => CircularProgressIndicator(),
    Ready(:final rows)    => ListView.builder(...),
    Errored(:final error) => ErrorWidget(error),
  },
)
```

Internally consumes `viewSource.watch<DiaryEntry>(...)`; tracks the four `Update<T>` variants (Snapshot × N → EndOfReplay → Delta/Tombstone × ∞), accumulates rows, and presents a stable `Loading`/`Ready`/`Errored` projected state.

**`ViewListView<TRow>`** — sugar for the list-rendering case.

**`ViewListener<TRow>`** — imperative side-effect widget (the "modal popup on revoked questionnaire" pattern):

```dart
ViewListener<Questionnaire>(
  viewName: 'my_questionnaires',
  mapper: Questionnaire.fromRow,
  listenWhen: (prev, curr) => /* detect transition */,
  onTransition: (ctx, prev, curr) => showDialog(...),
  child: SomeWidget(),
)
```

Mirrors `BlocListener`/`ProviderListener`. Doesn't rebuild; observes deltas and fires side-effects.

### Permission widget

**`PermissionGate`** — sugar:

```dart
PermissionGate(
  required: Permission.editNotes,
  child: EditNoteButton(...),
  fallback: ReadOnlyNoteView(...),  // optional; default: nothing
)
```

Consults `PermissionSource.current` for the active Principal; rebuilds when permissions change.

### Error sink

**`ReActionErrorListener`** — mounted once at root, centralizes the 98 ad-hoc `ScaffoldMessenger` calls the portal survey identified. Listens to all `ActionSubmitter` failures + `ViewSource` errors via a shared sink in `ReActionScope`. Configurable callbacks per error category (auth-failed → route to login; action-denied → snackbar; view-failed → toast); sensible defaults shipped.

### Idempotency-key generation policy

- `ActionBuilder` generates a UUID v4 at first `submit()` invocation.
- The key is **cached for the lifetime of the in-flight Future** so retries while `Submitting` reuse it.
- After a **terminal state** (Success/Denied/Failed), the next press generates a **fresh key** (new logical action).
- Consumers can override via explicit `idempotencyKey:` on the submission factory (escape hatch for stable per-form keys, server-supplied keys, etc.).
- The substrate's existing per-action `IdempotencyPolicy` is honored automatically — for `none`-policy actions, the generated key is ignored.

This is **distinct** from the substrate's `dedupeByContent` flag, which is a payload-equality optimization at event append time. Different layer, different problem.

### What `reaction_widgets` deliberately does NOT ship

- Login UI (Firebase login flow stays in hht_diary as today)
- Inactivity timer / warning dialog (app-domain; reads from `AuthSession`)
- Cross-tab collision detection (app-domain)
- Navigation primitives (`go_router` or whatever the consumer chooses)
- Optimistic-update primitives (apps build on top of `ActionSubmitter` + `ViewSource`)
- File upload primitives (not used in current portal)
- Substrate-introspection sugar (`EventLogView`, `TraceView`) — see Future Work

## Cross-cutting non-goals (out of `reaction`'s scope, kept in app code)

- Sponsor-configurable inactivity timeout. Portal-side concern; app reads from `AuthSession.principal` lifetime and shows custom warning.
- Sponsor role display name overrides. App-domain display concern; `Principal` and `PermissionSnapshot` stay neutral on display semantics.
- Multi-role selection UX. App-domain.
- Email OTP / TOTP enrollment flows. Identity provider concern (Firebase).

## Future widgets `reaction_widgets` should grow toward (deferred from v1)

Two are substrate-shaped and worth shipping in `reaction_widgets` once the core stabilizes:

- **`EventLogView`** — prettified `Stream<Update<StoredEvent>>` consumer with filter chips (entryType, eventType, principal, time range) and a tap-to-drawer detail panel. Substrate-shaped; useful in any event-sourced app.
- **`TraceView`** — given a root event, walks correlation IDs (`action_invocation_id`, `flowToken`) and renders the related-event tree/timeline. The substrate already records both correlation fields on every event; the trace just queries by them.

Two are app-domain widgets the consumer composes from primitives, not shipped:

- **Portal Settings widget** — composes `ViewBuilder` over a `portal_settings` projection + `ActionButton`s for settings-change actions. The settings projection is a substrate concern (event-sourced configuration); the form layout is portal-specific.
- **Site Data widget** — composes `ViewBuilder` over a `site_portal_data` projection (which augments EDC-sourced site data with portal-specific names/contacts) + `ActionButton`s for editing.

## Open questions tracked here

1. **Snapshot pagination / size limits.** Position D ships the entire view as `Snapshot<T>` × N. For very large views (e.g., 10K patient rows), this is one big stream burst on subscribe. Probably fine for hht_diary's expected scale; document "if a view exceeds N rows, snapshot delivery is split into batches" as a future optimization.
2. **Server-side per-subscription authorization.** When a portal user opens a `ViewSource.watch('participants', ...)`, the server must filter to participants the user is allowed to see (per their study/site assignments). `reaction`'s server-side subscribe handler consults the `PermissionSnapshot` for the requesting Principal and adjusts the filter/aggregates set before opening the underlying `subscribe<T>`. Substrate provides the filter primitives; `reaction` provides the gate.
3. **Reconnect strategy details.** WS drops happen. v1 baseline: refetch the snapshot + re-tail (re-subscribe; substrate's existing `_replayDone` semantics carry over). v1.1 optimization: resume from last applied sequence (server retains events; cheap if recent).
4. **Custom-route registration ergonomics.** Spec pins the contract (registered routes don't get auth middleware) without pinning the syntax — implementation choice.
5. **Optional `JwtAuthValidator` implementation.** Defer until consumer demand. The seam (`PrincipalAuthValidator`) exists from day one.

## Migration story (hht_diary portal)

`reaction` adoption in the existing portal is a substantial but bounded refactor, NOT a wholesale rewrite. Per the survey of `cure-hht/hht_diary/apps/sponsor-portal/`:

| Concern | Today | After `reaction` migration | Migration size |
|---|---|---|---|
| **AuthSession** | Firebase Auth + 1535-LOC `AuthService` | `RemoteAuthSession` wraps Firebase ID token; `AuthService` stays as consumer | Modest — preserves existing UX |
| **ActionSubmitter** | 20–30 bespoke REST POST handlers, ad-hoc shapes | Codify `ActionSubmission` ontology + uniform `DispatchResult` + `ActionDispatcher` on the server | Substantial one-time codification cost |
| **ViewSource** | Imperative REST GETs in `initState` + `setState`; no real-time | Reactive `Stream<Update<T>>` with snapshot+tail; rebuild pages around `ViewBuilder` | **Category change** — the largest net-new value |
| **PermissionSource** | Inline role + site checks in handlers; hardcoded ontology | `RolePermissionGrants` projection + per-Principal snapshot via `RemotePermissionSource` | Lightweight wrapper over existing logic |
| **State mgmt** | Provider 6.x + ChangeNotifier + setState | Agnostic Streams + `InheritedWidget` (`ReActionScope`); `_provider` adapter for existing patterns | Maps cleanly via the adapter package |
| **Wire** | REST only, no WS | REST POST + WS multiplex | Medium refactor; shelf supports WS upgrade via `package:shelf_web_socket` |

The migration also unlocks substrate-shaped opportunities the existing portal expresses informally: patient lifecycle state machines, linking code lifecycle, questionnaire instance lifecycle, immutable audit trail — all become first-class events with declarative `ProjectionSpec`s for the views the UI consumes.

## Testing strategy

- **Substrate addition** (`EndOfReplay<T>`): unit tests + update of any tests that exhaustively switch on `Update<T>`.
- **`reaction` interfaces**: test each `Local*` and `Remote*` impl independently. The wire codecs are unit-testable without any HTTP/WS stack.
- **`reaction` server**: integration tests via `package:shelf` test harness — submit actions, open WS subscriptions, assert correct envelopes flow.
- **`reaction_widgets`**: standard `flutter test` widget tests with fake `ActionSubmitter`/`ViewSource`/`PermissionSource`/`AuthSession` test doubles.
- **End-to-end**: extend `event_sourcing/example_action_permissions/` (or create a new sibling demo) to exercise the full Use-2 flow (Flutter web client + pure-Dart server, action submission + view subscription + auth lifecycle).

## Charter assertion alignment

- **Substrate trust boundaries (Assertion H):** unchanged. `reaction` introduces no new substrate trust dependency. The wire credential is a Layer-2 concern handled by the `PrincipalAuthValidator` seam, which is consumer-supplied.
- **Layer 1 facts vs Layer 2 conventions (Assertion I):** the wire ships Layer-1 facts (sequenced `Update<T>` instances). The receiver applies the same Layer-2 conventions (`ProjectionSpec` interpretation) the server applies. The wire is not a new epistemic layer.
- **Permission policy is substrate code:** `reaction`'s `PermissionSource` consumes the substrate's existing `RolePermissionGrants` projection. It does not introduce alternative policy mechanisms.

## Acceptance for v1

The spec is "shipped" when:

1. Both `reaction` and `reaction_widgets` packages exist as siblings in this repo with the structure above.
2. Mobile `hht_diary` (Use 1) consumes `reaction_widgets` for a non-trivial widget (e.g., the today-entries list and an action button) and the existing diary functionality is unchanged from the user's perspective.
3. Portal `hht_diary` (Use 2) consumes `reaction_widgets` + `reaction_widgets_provider` for at least one page (e.g., the patient list with `ViewBuilder` + an action), with a working `reaction`-server-backed shelf endpoint.
4. The `EndOfReplay<T>` substrate addition is shipped, all consumer tests updated, all existing tests pass.
5. The reference shelf-based server in `reaction/lib/src/server/` is documented and demonstrated end-to-end.
6. A migration guide for hht_diary's portal-server (REST handlers → ActionSubmission ontology) is written.

## References

- `event_sourcing/CLAUDE.md` — load-bearing architectural commitments
- `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md` — projection-and-subscribe design
- `docs/superpowers/specs/2026-05-11-roadmap.md` — Phase I close, Phase II/III plans, recent decisions
- `event_sourcing/example_action_permissions/README.md` — demo's "what is deliberately left out" inventory (lines about polling, no WS) is the gap `reaction` fills
- `cure-hht/hht_diary/apps/sponsor-portal/portal-ui/lib/services/auth_service.dart` — the 1535-LOC reference for what "AuthSession consumer" looks like in practice
- `cure-hht/hht_diary/apps/sponsor-portal/portal_server/lib/src/routes.dart` — the 20–30 bespoke handlers that codify-as-Actions during migration
