# Reaction — cross-process event-sourced UI for Cure-HHT

This file pins the PRD-level obligations for two new sibling packages:

- `reaction/` (pure Dart) — substrate-agnostic interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`), wire transport (HTTP for actions, WebSocket for view subscriptions), shelf-based reference server, in-process and remote implementations.
- `reaction_widgets/` (Flutter) — `ReActionScope` `InheritedWidget`, Builder primitives + sugar widgets, `ViewListener` for imperative side-effects, `PermissionGate`, `ReActionErrorListener`.

Plus the substrate addition the wire requires (an `EndOfReplay<T>` variant on `Update<T>`).

The six normative requirements below appear as `## EVS-PRD-...` blocks. Cross-system narrative (overview, architecture, decisions rejected, open questions, future work, migration story) lives in the other `##` chapters of this file. elspais detects requirement blocks by the `EVS-{TYPE}-{component}` pattern in the heading text, not by heading depth — so the file reads as a book with chapters, some of which happen to be normative.

## Overview

`reaction` lets a Flutter widget submit actions and subscribe to view rows against either an in-process `EventStore` + `ActionDispatcher` (Use 1: mobile diary, embedded) or a remote portal server (Use 2: Flutter web client talking to a pure-Dart shelf server). The widget never knows which transport it is on.

Two architectural moves keep the design small:

1. **Reuse the substrate's reactive primitive across the wire.** Cross-process change-feed delivery is a JSON serialization of the existing `subscribe<T>` `Stream<Update<T>>` over WebSocket — not a bespoke wire protocol. The substrate's atomic snapshot-then-deltas guarantee carries over to the wire for free. The only substrate addition is one new `Update<T>` variant: `EndOfReplay<T>`.
2. **Keep the auth credential opaque to the substrate.** `reaction` defines a `PrincipalAuthValidator` interface; the validator is consumer-supplied. The mechanism that closes the Principal-on-faith trust boundary (linking-code bootstrap on mobile, Firebase ID tokens on portal) lives in app code, not in `reaction`.

The widget layer is two-tier: low-level Builder primitives (`ActionBuilder`, `ViewBuilder`) for full control, plus pre-built sugar widgets (`ActionButton`, `ViewListView`, `PermissionGate`) for the common case. State management is agnostic: the library exposes `Stream`s and `ValueListenable`s, and consumers wrap them with whatever state-management library they prefer. A separate `reaction_widgets_provider` adapter package ships in v1 because the existing portal-ui already uses Provider 6.x.

## Architecture

```text
event_sourcing-worktrees/CUR-1317-libify-event-sourcing/
  event_sourcing/         (substrate, exists)
    + small additions:
      - EndOfReplay<T> as a 4th Update<T> variant
  canonical_json_jcs/     (exists, unchanged)
  provenance/             (exists, unchanged)
  reaction/               (NEW, pure Dart, depends on event_sourcing)
    lib/src/
      interfaces/      AuthSession, ActionSubmitter, ViewSource,
                       PermissionSource, PrincipalAuthValidator
      local/           Local* impls wrapping substrate APIs
      remote/          Remote* impls (HTTP for actions, WS for views)
      server/          shelf-based reference server: HTTP routes for
                       actions + permission snapshots + WS handler that
                       multiplexes subscriptions over one connection
      wire/            JSON codecs for ActionSubmission, DispatchResult,
                       Update<T> envelopes, SubscribeMessage,
                       PrincipalAuthClaim
      state/           ActionState sealed type + idempotency-key generation
  reaction_widgets/       (NEW, Flutter, depends on reaction)
    lib/src/
      scope/           ReActionScope InheritedWidget
      action/          ActionBuilder + ActionButton
      view/            ViewBuilder + ViewListView + ViewListener
      permission/      PermissionGate
      error/           ReActionErrorListener (centralized error sink)
  reaction_widgets_provider/ (NEW, Flutter, tiny adapter)
    lib/                  ChangeNotifier wrappers; one-line context.watch<...>
                          ergonomics for portal-ui's existing patterns
```

Dependency direction is one-way: `reaction_widgets_provider → reaction_widgets → reaction → event_sourcing`.

Position D (snapshot + tail) is the wire's semantic shape. The web client does not run a full `EventStore` and does not re-derive views from event history. On subscribe, the server runs `subscribe<T>(filter, AggregateMode(viewName, mapper, aggregates))` against its own `EventStore`; the substrate's atomic snapshot-then-deltas guarantee delivers `Snapshot<T>` × N → `EndOfReplay<T>` → live `Delta<T>` / `Tombstone<T>` × ∞ on its `Stream<Update<T>>`; the server's WS handler serializes each `Update<T>` to JSON and ships it; the client's `RemoteViewSource` deserializes, applies the consumer's `mapper`, and emits the same `Stream<Update<T>>` to widget code.

Concurrency profile: `reaction`'s WS server-side load is portal UI users only — ~1 typical, ~20 maximum, intermittent. The 100s–1000s of mobile concurrency uses the substrate's existing `Destination`/`Ingest` sync — that is a different path, not `reaction`'s WS.

## Reading order

The PRDs below are best read in this order on first contact, because each later one references concepts pinned by the earlier ones:

1. `EVS-PRD-auth-session` — the credential lifecycle other PRDs assume.
2. `EVS-PRD-action-submitter` — the simplest of the three transport-bridging interfaces.
3. `EVS-PRD-view-subscriber` — the substrate's `Update<T>` stream made transport-agnostic.
4. `EVS-PRD-permission-source` — per-Principal projection access.
5. `EVS-PRD-cross-process-event-transport` — the wire envelope shared by the Remote impls of the three above.
6. `EVS-PRD-reaction-widget-contract` — the Flutter widget layer that consumes all four interfaces.

## EVS-PRD-auth-session: Auth Session

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

### Purpose

Consumer code needs a uniform way to manage credential lifecycle and access the validated `Principal`, regardless of whether the credential was minted by an in-process bootstrap (mobile install UUID) or by a remote authentication flow (portal Firebase ID token). The `AuthSession` interface owns the credential, surfaces its status, and exposes the validated `Principal` for downstream interfaces (`ActionSubmitter`, `ViewSource`, `PermissionSource`) to consult.

Credential format is opaque to `reaction`. The library does not parse JWTs, validate signatures, or know what claims mean. Validation lives in a pluggable `PrincipalAuthValidator` mounted on the server side; the consumer supplies the validator that matches their deployment.

### Assertions

A. The library SHALL define an `AuthSession` interface that exposes the current `AuthStatus` synchronously, a `Stream<AuthStatus>` of status updates, a `setCredential(String?)` mutator, and a `Principal? get principal` getter that returns the validated `Principal` when authenticated.

B. `AuthStatus` SHALL be a sealed type with exactly three variants: `Authenticated` (carrying the validated `Principal`), `NotAuthenticated`, and `Expired`.

C. The library SHALL define a `PrincipalAuthValidator` interface whose `authenticate(String)` method returns the authenticated `Principal` or throws `AuthenticationDenied`.

D. The library SHALL NOT impose a format on the credential string; format selection SHALL be delegated entirely to the consumer-supplied `PrincipalAuthValidator`.

E. On wire-level authentication failure (HTTP 401 or WebSocket auth-rejected close-frame) the Remote implementation of `AuthSession` SHALL transition status to `Expired` and emit the new status on its stream.

F. The library SHALL ship a `TrustingAuthValidator` reference implementation that accepts any credential string verbatim as `Principal.id`. This implementation SHALL be documented as suitable for development and test use only.

G. The `AuthSession`'s active `Principal` SHALL be the source of truth for which Principal downstream interfaces (`ActionSubmitter`, `ViewSource`, `PermissionSource`) operate against.

### Rationale

**Why a separate interface for auth, distinct from `PermissionSource`?** Authentication ("who are you and can you prove it") and authorization ("what are you allowed to do") are different concerns at different layers. The substrate's `permissions-as-events` PRD already pins authorization mechanics; auth is the wire-boundary credential concern that feeds it. Conflating them in one interface would force consumers who only need credential management to drag in the permission-projection machinery, and vice versa.

**Why is the credential opaque to the library?** The `Cure-HHT` deployment story has at least two distinct credential issuance paths: portal users authenticate via username + password + email-TFA through Firebase (which mints Firebase ID tokens); mobile installs bootstrap via a one-time linking code that the portal redeems for a JWT bound to the install UUID. Both paths produce JWT-shaped tokens, but the library has no business knowing which is which. A `String` credential plus a pluggable validator handles both without committing the library to either.

**Why three statuses instead of two?** `Expired` is operationally distinct from `NotAuthenticated`: the consumer's last-known `Principal` was valid until recently and the user almost certainly wants to re-authenticate (e.g., the portal's 5-minute idle timeout). UX wants this signal to drive a "session expired, please log in again" flow rather than a fresh-login flow.

**Why does the validator throw rather than return a discriminated result?** Validation failure is exceptional in the operational sense — the library's typical hot path is a successful validation. An exception keeps the success path uncluttered while still letting the caller catch and translate the failure into wire responses (HTTP 401, WS close-frame). The throw type is named (`AuthenticationDenied`) so callers do not need to catch generic `Exception`.

**Why `TrustingAuthValidator` ships at all?** Development and test environments need *something*; without a default reference impl, every test fixture and every demo would have to author its own. The "DO NOT USE IN PRODUCTION" docstring is the safety; the convenience of having a working default is too high to skip.

**Why no `JwtAuthValidator` in v1?** JWT validation needs a key-loading strategy, an issuer convention, and a claim-mapping policy — all of which depend on the deployment's identity-provider choices. A premature default would either be too narrow (only fits Firebase, only fits Auth0) or too configurable (weighed down with options no consumer actually needs). Defer until consumer demand makes the right shape obvious.

*End* *Auth Session* | **Hash**: 9c087173

## EVS-PRD-action-submitter: Action Submitter

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-action-dispatch, EVS-PRD-library-charter

### Purpose

Consumer code (especially Flutter widgets) needs a uniform way to submit an `ActionSubmission` and receive a `DispatchResult`, regardless of whether the submission is dispatched in-process by an `ActionDispatcher` or via a remote transport. The `ActionSubmitter` interface is the substrate-agnostic seam; Local and Remote implementations are bound at composition time.

### Assertions

A. The library SHALL define an `ActionSubmitter` interface whose `submit(ActionSubmission)` method returns a `Future<DispatchResult>`.

B. The library SHALL ship a `LocalActionSubmitter` implementation that delegates to an in-process `ActionDispatcher.dispatch`.

C. The library SHALL ship a `RemoteActionSubmitter` implementation that submits via HTTP POST and decodes a `DispatchResult` from the response body.

D. The `RemoteActionSubmitter` SHALL include the bearer credential from the co-mounted `AuthSession` on every outbound submission as an authentication header.

E. Consumer code that depends only on the `ActionSubmitter` interface SHALL be source-identical regardless of whether a `LocalActionSubmitter` or `RemoteActionSubmitter` is composed at runtime.

### Rationale

**Why a single Future return rather than a streamed lifecycle?** The substrate's dispatch pipeline (parse → validate → authorize → execute → record) is atomic and synchronous; there is no intermediate state for the caller to observe. The widget-side `ActionState` lifecycle (`Idle` → `Submitting` → `Success` / `Denied` / `Failed`) is the *widget's* state machine — `ActionBuilder` calls `submit()` and tracks the `Future`'s lifecycle. Putting that lifecycle into the interface itself would push widget concerns into a layer that is also consumed by non-widget code (e.g., direct programmatic submission from a CLI or a server-side admin script).

**Why HTTP for the Remote impl rather than WebSocket?** Action submission is intrinsically request/response. HTTP POST matches that shape, gives standard infrastructure (proxies, idempotency-key headers, observability), and matches the existing portal-server's pattern (shelf POST handlers). Submitting actions over the same WebSocket connection used for view subscriptions would conflate request/response with async push and lose those affordances.

**Why is auth-header injection a Remote-impl assertion (D), not abstracted into the interface?** In-process submission has no wire to attach a header to; the `LocalActionSubmitter` reads `AuthSession.principal` directly and constructs the dispatch's `Principal` from it. The Remote impl is the one that has to encode credentials onto a wire. Pinning the obligation on the Remote impl, not on the abstract interface, keeps the abstract interface free of transport details.

**Why "source-identical" widget code (E)?** This is the central user-facing promise of the interface. Without it, mobile widget code and web widget code would diverge structurally, which defeats the purpose of building a substrate-agnostic widget library on top.

*End* *Action Submitter* | **Hash**: 22898b0a

## EVS-PRD-view-subscriber: View Subscriber

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter, EVS-PRD-subscription

### Purpose

Consumer code needs a uniform way to subscribe to view rows and receive the substrate's `Update<T>` stream, regardless of whether the rows live in an in-process `EventStore` (mobile, Use 1) or are streamed from a remote portal server (web, Use 2). The `ViewSource` interface is the substrate-agnostic seam; Local and Remote implementations differ only in where the rows come from.

### Assertions

A. The library SHALL define a `ViewSource` interface whose `watch<T>` method returns `Stream<Update<T>>` for a given `(viewName, mapper, filter, aggregates)`.

B. The library SHALL ship a `LocalViewSource` implementation that delegates to `EventStore.subscribe<T>` with `AggregateMode<T>`.

C. The library SHALL ship a `RemoteViewSource` implementation that consumes the cross-process wire and applies the consumer-supplied row mapper to incoming envelopes client-side.

D. Consumer code that depends only on the `ViewSource` interface SHALL be source-identical regardless of whether a `LocalViewSource` or `RemoteViewSource` is composed at runtime.

### Rationale

**Why does the interface return the substrate's existing `Update<T>` type, rather than wrap it?** The substrate's `Update<T>` already has the four variants the consumer needs (`Snapshot`, `Delta`, `Tombstone`, `EndOfReplay`) with the right semantics. Wrapping it in a parallel `ReactionUpdate<T>` would introduce a translation layer that adds nothing — and would obligate the library to keep the wrapper in sync with substrate changes forever. Reusing `Update<T>` directly means the substrate's contract IS the consumer's contract.

**Why does the mapper apply client-side in the Remote impl (C)?** The wire envelope ships the raw row as `Map<String, Object?>`, not the consumer's typed `T`. The mapper turns the map into `T` — that's a consumer-defined conversion that has no business running on the server. Server-side mapping would also force the wire codec to know about every consumer type, which violates the library's domain-neutrality.

**Why "source-identical" widget code (D)?** Same rationale as `EVS-PRD-action-submitter`-E.

*End* *View Subscriber* | **Hash**: b6801679

## EVS-PRD-permission-source: Permission Source

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter, EVS-PRD-permissions-as-events

### Purpose

Consumer code needs to gate UI affordances on whether the active `Principal` holds a given permission, react when the permission set changes (because the active `Principal` changed, or because grants were modified), and pre-filter scoped item lists by the user's scope assignments. The `PermissionSource` interface exposes the substrate's per-`Principal` `EffectiveAuthorization` to widget code via a synchronous getter and a `Stream` of updates.

### Assertions

A. The library SHALL define a `PermissionSource` interface that exposes the current `EffectiveAuthorization` for the active `Principal` (synchronous getter, nullable) and a `Stream<EffectiveAuthorization?>` of snapshot updates.

B. The library SHALL ship a `LocalPermissionSource` implementation that derives the snapshot via `AuthorizationPolicy.effectivePermissionsFor`. The policy in turn reads the `role_permission_grants` and `user_role_scopes` projections — verifying both that the active role grants the permission AND that the user is currently assigned to that active role. `PermissionSource.current` surfaces `EffectiveAuthorization` directly; no intermediate lossy `PermissionSnapshot` type exists.

C. The library SHALL ship a `RemotePermissionSource` implementation that fetches the initial snapshot via a documented HTTP endpoint and reflects subsequent permission changes via the same wire-side subscription mechanism the `RemoteViewSource` uses.

D. The active `Principal` SHALL be sourced from a co-mounted `AuthSession`; `PermissionSource` SHALL NOT accept Principal mutations directly through its own surface.

E. When the active `Principal` changes, `PermissionSource` SHALL re-fetch and re-emit the corresponding snapshot on its stream.

### Rationale

**Why a separate interface from `ViewSource` if the underlying mechanism is just a projection subscription?** The per-`Principal` scoping and the synchronous "current snapshot" getter are operationally distinct from a generic view subscription. Widget code that asks "can the current user do X?" should not have to thread mapper functions and view names through every call site; a focused interface gives a tight, high-frequency API for the gating use case while delegating the underlying wire mechanism to the same machinery.

**Why no `setPrincipal` mutator on `PermissionSource` (D)?** Two interfaces both holding the active `Principal` would create a synchronization problem: which one is authoritative? `AuthSession` is the source of truth (it knows when credentials become valid, expire, change); `PermissionSource` reads from it. A single mutation point keeps the model unambiguous.

**Why does the Remote impl piggyback on the same wire as `RemoteViewSource` (C)?** `RolePermissionGrants` is just another substrate view. Treating its updates as ordinary view subscriptions means the wire transport has one mechanism for all reactive data, not two. The `RemotePermissionSource` is, internally, a thin specialization of `RemoteViewSource` over the `RolePermissionGrants` view filtered to the active `Principal`.

**Why route through `AuthorizationPolicy.effectivePermissionsFor` rather than reading `role_permission_grants` directly (B)?** An earlier draft of `LocalPermissionSource` read `role_permission_grants` and built the snapshot from grants alone. That bypassed the membership gate — a Principal claiming `activeRole: 'admin'` would see admin permissions even if no `user_role_scopes` row bound that user to the admin role. The trust-model fix (substrate commit `62b2bcc`) moved the membership check into `AuthorizationPolicy.effectivePermissionsFor`, which now reads BOTH projections; the `LocalPermissionSource` realignment (commit `2bb5c03`) routes through the policy so local and remote sources compute identical snapshots. This realizes the closed-under-events trust model (see `EVS-PRD-library-charter` narrative chapter "Closed under events"): the substrate refuses to honour an unverified role claim, regardless of which `PermissionSource` impl serves the snapshot.

*End* *Permission Source* | **Hash**: 1fa3332a

## EVS-PRD-cross-process-event-transport: Cross-Process Event Transport

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter, EVS-PRD-subscription

### Purpose

When `reaction` is configured for cross-process operation, the substrate's `Update<T>` reactive stream is delivered faithfully to the remote consumer via JSON-serialized WebSocket envelopes, and action submissions are delivered via HTTP POST. The wire transport is a faithful relay of the substrate's in-process semantics; it does not introduce a new epistemic layer and does not weaken any of the substrate's ordering or atomicity guarantees.

### Assertions

A. The library SHALL define JSON wire envelopes for each `Update<T>` variant (`Snapshot`, `Delta`, `Tombstone`, `EndOfReplay`) such that round-trip codec preserves all fields.

B. Each wire envelope SHALL include the `sequence` of the update and a subscription identifier the update belongs to.

C. The server-side wire handler SHALL preserve the substrate's snapshot-then-deltas atomicity guarantee end-to-end: a remote consumer SHALL observe the same `Update<T>` ordering for a given `(filter, viewName)` as a co-located in-process subscriber would observe.

D. The server-side wire handler SHALL accept multiple concurrent subscription requests from a single client over a single WebSocket connection, distinguishing them by client-assigned subscription identifier.

E. The server-side wire handler SHALL apply Principal-scoped authorization to each subscription request before opening the underlying `subscribe<T>`, consulting the requesting Principal's `EffectiveAuthorization`.

F. Action submission over HTTP POST SHALL carry the bearer credential from the requesting client's `AuthSession` in an authentication header.

G. The wire transport SHALL NOT introduce a new epistemic layer; the receiver SHALL apply the same Layer-2 conventions as the sender.

### Rationale

**Why JSON rather than a binary protocol?** The wire serves Flutter web clients (where Dart compiles to JavaScript) and pure-Dart server endpoints. JSON has zero-cost ergonomics in both environments, plays nicely with browser dev-tools, and matches the existing portal's transport format. Binary protocols (protobuf, MessagePack) would be a premature optimization at the expected portal-UI scale of ~1–20 concurrent users.

**Why multiplex subscriptions over one WebSocket (D)?** A portal user typically opens 3–10 concurrent subscriptions (one per visible panel). One connection per subscription would burn browser connections (HTTP/1.1 limits ~6 per origin) and add per-subscription handshake latency. Multiplexing over one connection is the standard reactive-UI pattern and matches what frameworks like Phoenix LiveView, Apollo, and others do.

**Why is per-subscription authorization the server's job (E)?** A portal user must not be able to subscribe to events about participants they are not assigned to. The server consults the requesting `Principal`'s `EffectiveAuthorization` (which itself derives from the substrate's `role_permission_grants` and `user_role_scopes` projections) and adjusts the underlying `subscribe<T>` filter accordingly — typically by restricting the `aggregates` set or composing additional filter clauses. The substrate provides the filter primitives; `reaction` provides the gate.

**Why is "no new epistemic layer" (G) load-bearing?** The substrate's Charter Assertion I (Layer 1 facts vs Layer 2 conventions) commits to documenting which guarantees are absolute (cryptographic, structural) versus which are library-provided defaults. The wire ships Layer-1 facts (sequenced `Update<T>` instances with intact ordering and identity); the receiver applies the *same* Layer-2 conventions the sender applies (same `ProjectionSpec` interpretation). The wire does not author a third layer of interpretation. Without this assertion, a future "wire-side optimization" could quietly re-interpret events differently than the in-process path, breaking consumer assumptions.

**Why isn't action-submission idempotency-key handling pinned here?** Idempotency is the substrate's existing dispatcher concern (`EVS-PRD-action-dispatch`) and the widget-side key-generation policy (`EVS-PRD-reaction-widget-contract`-E). The wire just carries the key as an `idempotencyKey` field on the `ActionSubmission`. No special wire treatment is needed.

*End* *Cross-Process Event Transport* | **Hash**: fbe2d2d4

## EVS-PRD-reaction-widget-contract: Reaction Widget Contract

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-action-submitter, EVS-PRD-auth-session, EVS-PRD-permission-source, EVS-PRD-view-subscriber

> **Implementation status:** Designed; `reaction_widgets` package not yet implemented (Plan D). The assertions below are normative — they are the contract the future package MUST satisfy when built. Audit tooling SHOULD treat coverage gaps here as "package not yet built", not as drift between spec and shipped code.

### Purpose

Flutter widget code consuming `reaction`'s four interfaces SHALL be substrate-agnostic; the same widget composition SHALL work whether bound to Local or Remote implementations. The library provides Builder primitives for full control plus pre-built sugar widgets for the common case, an `InheritedWidget` to thread the four interfaces down the tree, and an imperative side-effect widget for "react to a view change without rebuilding" patterns.

### Assertions

A. The widget library SHALL provide an `InheritedWidget` (`ReActionScope`) that threads the four library interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`) down the widget tree, accessible via `ReActionScope.of(context)`.

B. Widget code consuming `ReActionScope.of(context)` SHALL be source-identical when the scope is composed from Local versus Remote implementations of the four interfaces.

C. The widget library SHALL provide Builder primitives for action submission (`ActionBuilder`) and view subscription (`ViewBuilder`), each exposing the operation's state to the caller-supplied builder for rendering.

D. The widget library SHALL provide an imperative side-effect widget (`ViewListener`) that fires a callback on view-state transitions without rebuilding the widget tree.

E. Action-submission widgets SHALL generate a UUID v4 idempotency key at first submission attempt, retain that key for the in-flight submission so retries during `Submitting` reuse it, and generate a fresh key only after the submission reaches a terminal state (Success, Denied, or Failed). Consumers SHALL be able to override the generated key by supplying an explicit `idempotencyKey` on the submission factory.

F. The widget library SHALL be source-organized so that no widget references the substrate's storage backend, dispatcher, or projection registry directly; all substrate access SHALL flow through `reaction`'s four interfaces.

### Rationale

**Why a two-tier API (Builder primitive + sugar widget) instead of one or the other?** Builder-only is verbose for the 80% common case (every action button becomes a 5-line `switch (state)` block). Sugar-only is opinionated and cannot accommodate non-standard UX (a multi-stage progress, a custom denied-message animation, a contextual confirmation dialog). The two-tier approach mirrors Flutter's own pattern: `StreamBuilder`/`FutureBuilder` are primitives, `ListView` and `GridView` are sugar built on top. Consumers who need control go to the primitive; consumers who don't, don't.

**Why `ViewListener` is a separate widget rather than a flag on `ViewBuilder` (D)?** Imperative side effects (showing a modal on state transition) are a different shape from reactive rebuilds. Trying to wedge them into a builder requires either side effects in `build()` (which Flutter explicitly forbids) or a callback parameter that fires from inside `setState` (which violates the principle that `build()` should be pure). A separate widget that uses `didChangeDependencies` / `addListener` cleanly lets consumers attach side effects without polluting the rebuild path. This mirrors `BlocListener`/`ProviderListener` in popular state-management libraries — a well-known pattern.

**Why UUID v4 idempotency keys with the lifetime caching policy (E)?** The substrate's idempotency-store is keyed on `(principalId, idempotencyKey)`; without a key, every submission is a fresh logical action. UUID v4 gives deterministic uniqueness without coordination. Caching the key for the in-flight `Future` lifetime means a user double-tap during `Submitting` is correctly treated as a retry of the same logical action (the dispatcher returns the cached `DispatchResult` for the second tap). Generating a fresh key after a terminal state means a deliberate re-press (e.g., "save then save again") is correctly treated as a new logical action. The consumer override exists for cases where stable per-form keys (e.g., a key derived from a draft document ID) are wanted.

**Why "no direct substrate access from widgets" (F)?** Without this, a widget could reach past `reaction`'s interfaces into `EventStore` or `ActionDispatcher` directly. That would break substrate-agnosticism (the widget would no longer work over the Remote transport) and violate the layered architecture. A structural scan test verifies the assertion.

**Why agnostic state management?** The widget library's value proposition is the substrate-agnostic widget contract, not a state-management opinion. Forcing a state-mgmt choice (Riverpod, BLoC, Provider) excludes consumers who use the others; the existing portal-ui already uses Provider. Agnostic primitives (`Stream`, `ValueListenable`, `InheritedWidget`) are the lingua franca; the optional `reaction_widgets_provider` adapter package bridges to Provider's `context.watch<...>()` ergonomics for the existing portal-ui. Riverpod or BLoC adapters can ship in the same way if/when consumer demand exists.

*End* *Reaction Widget Contract* | **Hash**: d21c8301

## Decisions and alternatives rejected

These shaped the design but live nowhere in the assertions above; they are recorded here so that future authors do not re-litigate them without surfacing new evidence.

**Why not put `reaction` inside the `event_sourcing/` package?** Two reasons. First, `reaction` will pull in `package:http`, `package:web_socket_channel`, and `package:shelf` (for the server module) — folding those into `event_sourcing/pubspec.yaml` would force them onto every substrate consumer, including hypothetical embedded-only callers that never touch a wire. Second, the substrate is Layer 1 facts + Layer 2 conventions; `reaction` is application-facing API on top of those layers. Different epistemic role; cleaner kept apart.

**Why not WebSocket for action submission too?** Conflates request/response with async push. Loses HTTP's affordances (proxies, idempotency-key header semantics, standard observability tooling). The shelf-based portal demo already uses HTTP for actions; matching that pattern is a smaller migration.

**Why not Server-Sent Events for view subscriptions?** Subscription multiplexing on SSE is awkward (one connection per subscription, browser-limited in HTTP/1.1; one multiplexed SSE stream needs an envelope and a control plane that SSE does not natively support). A portal user opens 3–10 concurrent subscriptions; multiplexing them on a single WebSocket with a small control protocol fits the use case better.

**Why not full client-side substrate (Position A — web client runs an `EventStore`)?** Initial download of full event history would be huge (years of clinical data); re-deriving views from scratch on every cold start is wasteful CPU. The web client wants the *current* projection state, then to be kept up-to-date — that is precisely Position D (snapshot + tail).

**Why not server-pushed pre-computed projection deltas (Position C — bespoke wire)?** Loses the substrate's existing reactive primitive. Server has to know how to serialize per-projection diffs; design surface grows. Worse, the mobile case (which uses the in-process substrate) and the web case would have two different binding models in widget code, defeating the substrate-agnostic widget API.

**Why not bake a state-management library (Riverpod, BLoC) into the widget library?** Forces a choice on consumers. Excludes those who use the others. The agnostic core + optional adapter packages handles all cases without lock-in.

**Why not introduce a new substrate-level "snapshot at sequence X" API?** The substrate's existing `subscribe<T>` already does race-free snapshot-then-deltas via the `_replayDone` flag pattern. The wire just serializes the existing `Stream<Update<T>>`; no new substrate API needed for this. Only addition: the existing `_replayDone` flag is surfaced as an explicit `EndOfReplay<T>` variant on `Update<T>` so consumers can detect replay completion deterministically.

**Why not use `dedupeByContent` for action-widget idempotency?** Different layer, different problem. `dedupeByContent` is a payload-equality optimization at event-append time; action idempotency is a request-correlation contract at dispatch entry. The substrate's existing `IdempotencyPolicy` per-action with `IdempotencyStore` keyed on `(principalId, idempotencyKey)` is exactly the right tool for the widget case.

## Open questions

These are tracked here for resolution during implementation; resolution should land as new assertions or as Rationale updates to the affected PRDs.

1. **Snapshot pagination / size limits.** Position D ships the entire view as `Snapshot<T>` × N. For very large views (e.g., 10,000 patient rows), this is one big stream burst on subscribe. Probably fine for hht_diary's expected scale; document "if a view exceeds N rows, snapshot delivery is split into batches" as a future optimization with concrete N when measurement demands it.
2. **Server-side per-subscription authorization details.** `EVS-PRD-cross-process-event-transport`-E pins the obligation; the implementation specifics (does the server compose additional filter clauses, restrict the aggregates set, or short-circuit the subscription with a wire-level denial?) are deferred to implementation.
3. **Reconnect strategy details.** WebSocket drops happen. v1 baseline: refetch the snapshot + re-tail (re-subscribe; substrate's existing snapshot-then-deltas semantics carry over). v1.1 optimization: resume from last applied sequence (server retains events; cheap if recent).
4. **Custom-route registration ergonomics.** `reaction`'s shelf server lets consumers register additional routes (login, linking-code onboarding) outside the standard `reaction` flow; those routes bypass auth-validator middleware. The exact API for this — `server.addCustomRoute(...)` vs exposing the `Router` for direct mutation — is an implementation detail.
5. **`JwtAuthValidator` reference implementation.** Whether to ship a JWT validator alongside `TrustingAuthValidator` is deferred; pluggable seam exists from day one regardless.

## Future work

These are explicitly out of scope for v1 but anticipated as natural extensions:

- **`EventLogView`** — prettified `Stream<Update<StoredEvent>>` consumer with filter chips (entryType, eventType, principal, time range) and a tap-to-drawer detail panel. Substrate-shaped sugar; useful in any event-sourced app for audit, debugging, ops dashboards.
- **`TraceView`** — given a root event, walks correlation IDs (`action_invocation_id`, `flowToken`) and renders the related-event tree/timeline. The substrate already records both correlation fields on every event.
- **`reaction_widgets_riverpod` adapter** — analog of `reaction_widgets_provider` for Riverpod consumers. Defer until demand.
- **Connection-state observability** on `Remote*` impls (`Stream<ConnectionState>`) — for "Reconnecting…" UX. Defer to v1.1.
- **View pagination / cursor-based row delivery** — defer until large-view scale demands it.

## Migration story (hht_diary portal)

`reaction` adoption in the existing `cure-hht/hht_diary/apps/sponsor-portal/` portal is a substantial but bounded refactor, not a wholesale rewrite. Recorded here so that authoring `reaction` keeps the migration path concrete.

| Concern | Today (portal) | After `reaction` migration | Migration size |
|---|---|---|---|
| **AuthSession** | Firebase Auth + 1535-LOC `AuthService` | `RemoteAuthSession` wraps Firebase ID token; `AuthService` stays as consumer | Modest — preserves existing UX |
| **ActionSubmitter** | 20–30 bespoke REST POST handlers, ad-hoc shapes | Codify `ActionSubmission` ontology + uniform `DispatchResult` + `ActionDispatcher` on the server | Substantial one-time codification cost |
| **ViewSource** | Imperative REST GETs in `initState` + `setState`; no real-time | Reactive `Stream<Update<T>>` with snapshot+tail; rebuild pages around `ViewBuilder` | Category change — the largest net-new value |
| **PermissionSource** | Inline role + site checks in handlers; hardcoded ontology | `RolePermissionGrants` projection + per-Principal snapshot via `RemotePermissionSource` | Lightweight wrapper over existing logic |
| **State mgmt** | Provider 6.x + ChangeNotifier + setState | Agnostic Streams + `InheritedWidget` (`ReActionScope`); `_provider` adapter for existing patterns | Maps cleanly via the adapter package |
| **Wire** | REST only, no WebSocket | REST POST + WS multiplex | Medium refactor; shelf supports WS upgrade |

The migration unlocks substrate-shaped opportunities the existing portal expresses informally: patient lifecycle state machines (`linkable → linking_in_progress → connected → disconnected`), linking code lifecycle, questionnaire instance lifecycle, immutable audit trail — all become first-class events with declarative `ProjectionSpec`s for the views the UI consumes.
