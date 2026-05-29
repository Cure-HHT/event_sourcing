# Reaction — cross-process event-sourced UI for Cure-HHT

This file pins the PRD-level obligations for two new sibling packages:

- `reaction/` (pure Dart) — substrate-agnostic interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`), wire transport (HTTP for actions, WebSocket for view subscriptions), shelf-based reference server, in-process and remote implementations.
- `reaction_widgets/` (Flutter, **headless**) — `ReActionScope` `InheritedWidget`, Builder primitives (`ActionBuilder`, `ViewBuilder`), imperative `ViewListener` and `ReActionErrorListener`, and `PermissionGate`. The library ships NO rendered or styled widgets — those live in each downstream consumer app, wrapping the builders with modality-appropriate sugar.
- `reaction_widgets_testing/` (Flutter, dev-deps only) — shipped widget-test doubles (`FakeReaction` + `pumpReactionWidget`) for apps built on `reaction_widgets`. Sibling package so consumers' release builds don't pull `flutter_test`.

`reaction` itself defines a shared `ReactionScope` abstraction (with `LocalScope` and `RemoteScope` implementations) that exposes the four interfaces plus an authoritative `ConnectionStatus` stream. The widget layer threads that scope; it does not infer connection liveness from stream behavior.

Plus the substrate addition the wire requires (an `EndOfReplay<T>` variant on `Update<T>`).

The seven normative requirements below appear as `## EVS-PRD-...` blocks. Cross-system narrative (overview, architecture, decisions rejected, open questions, future work, migration story) lives in the other `##` chapters of this file. elspais detects requirement blocks by the `EVS-{TYPE}-{component}` pattern in the heading text, not by heading depth — so the file reads as a book with chapters, some of which happen to be normative.

## Overview

`reaction` lets a Flutter widget submit actions and subscribe to view rows against either an in-process `EventStore` + `ActionDispatcher` (Use 1: mobile diary, embedded) or a remote portal server (Use 2: Flutter web client talking to a pure-Dart shelf server). The widget never knows which transport it is on.

Two architectural moves keep the design small:

1. **Reuse the substrate's reactive primitive across the wire.** Cross-process change-feed delivery is a JSON serialization of the existing `subscribe<T>` `Stream<Update<T>>` over WebSocket — not a bespoke wire protocol. The substrate's atomic snapshot-then-deltas guarantee carries over to the wire for free. The only substrate addition is one new `Update<T>` variant: `EndOfReplay<T>`.
2. **Keep the auth credential opaque to the substrate.** `reaction` defines a `PrincipalAuthValidator` interface; the validator is consumer-supplied. The mechanism that closes the Principal-on-faith trust boundary (linking-code bootstrap on mobile, Firebase ID tokens on portal) lives in app code, not in `reaction`.

The widget layer is **headless**: it ships Builder primitives (`ActionBuilder`, `ViewBuilder`), imperative listeners (`ViewListener`, `ReActionErrorListener`), the scope-threading `InheritedWidget` (`ReActionScope`), a permission gate (`PermissionGate`), and widget-test doubles — and **no rendered or styled widgets**. Each downstream consumer renders its own sugar (e.g., `DiarySubmitButton`, portal list panels) on top of the builders. The two near-term consumers (hht_diary mobile and the Flutter-web portal UI) run in genuinely different modalities; shipping shared styled widgets in the base would encode the wrong assumption for one of them. The "two-tier" structure (primitives + sugar) still exists — but the tiers live in different packages with the package boundary running along the modality split. State management is agnostic: the library exposes `Stream`s and `ValueListenable`s; consumers wrap them with whatever state-management library they prefer. The opt-in adapter target is **signals** (`reaction_widgets_signals`) — the reactive idiom both near-term consumers land on (hht_diary mobile, and the portal-ui once its Phase IV cutover moves it onto the substrate). The headless base stays on raw streams, so signals — or any other state-management library — remains first-class.

## Architecture

```text
event_sourcing-worktrees/CUR-1317-libify-event-sourcing/
  event_sourcing/         (substrate, exists)
    + small additions:
      - EndOfReplay<T> as a 4th Update<T> variant
  canonical_json_jcs/     (exists, unchanged)
  provenance/             (exists, unchanged)
  reaction/               (pure Dart, depends on event_sourcing)
    lib/src/
      interfaces/      AuthSession, ActionSubmitter, ViewSource,
                       PermissionSource, PrincipalAuthValidator
      scope/           ReactionScope (interface) + LocalScope
                       composing the Local* impls + connection-status
                       surface (RemoteScope lives in remote/)
      local/           Local* impls wrapping substrate APIs
      remote/          Remote* impls + RemoteScope; HTTP for actions,
                       multiplexed WS for views/permissions; owns
                       ConnectionStatus transitions and auto-reconnect
      server/          shelf-based reference server: HTTP routes for
                       actions + permission snapshots + WS handler that
                       multiplexes subscriptions over one connection
      wire/            JSON codecs for ActionSubmission, DispatchResult,
                       Update<T> envelopes, SubscribeMessage,
                       PrincipalAuthClaim
      state/           ActionState sealed type + idempotency-key generation
  reaction_widgets/       (NEW, Flutter, headless; depends on reaction)
    lib/src/
      scope/           ReActionScope InheritedWidget (threads ReactionScope
                       incl. ConnectionStatus) + .test() ctor
      action/          ActionBuilder (Builder primitive only — no
                       rendered widgets)
      view/            ViewBuilder + ViewState<T> (Loading/Ready/
                       Stale) + ViewListener
      permission/      PermissionGate (gates a child or builder on
                       EffectiveAuthorization; no styled UI)
      error/           ReActionErrorListener (auth/transport sink;
                       fires callbacks, renders nothing)
  reaction_widgets_testing/  (NEW, Flutter, dev-deps only;
                              depends on reaction_widgets)
    lib/                 FakeReaction + pumpReactionWidget — widget-
                         test doubles shipped per assertion H without
                         bloating reaction_widgets's main deps.
```

Dependency direction is one-way: `reaction_widgets_testing → reaction_widgets → reaction → event_sourcing`. The opt-in `reaction_widgets_signals` adapter package is additive and sits beside `reaction_widgets` (see Future work).

Position D (snapshot + tail) is the wire's semantic shape. The web client does not run a full `EventStore` and does not re-derive views from event history. On subscribe, the server runs `subscribe<T>(filter, AggregateMode(viewName, mapper, aggregates))` against its own `EventStore`; the substrate's atomic snapshot-then-deltas guarantee delivers `Snapshot<T>` × N → `EndOfReplay<T>` → live `Delta<T>` / `Tombstone<T>` × ∞ on its `Stream<Update<T>>`; the server's WS handler serializes each `Update<T>` to JSON and ships it; the client's `RemoteViewSource` deserializes, applies the consumer's `mapper`, and emits the same `Stream<Update<T>>` to widget code. On WS drop the `RemoteScope` transitions `ConnectionStatus` to `Reconnecting`, auto-reconnects with exponential backoff, and on recovery re-issues every active subscribe — each replays its own fresh `Snapshot × N → EndOfReplay → live` per the same substrate semantics. The widget layer's `ViewBuilder` observes the authoritative `ConnectionStatus` and surfaces `Stale(lastRows)` rather than blanking.

Concurrency profile: `reaction`'s WS server-side load is portal UI users only — ~1 typical, ~20 maximum, intermittent. The 100s–1000s of mobile concurrency uses the substrate's existing `Destination`/`Ingest` sync — that is a different path, not `reaction`'s WS.

## Reading order

The PRDs below are best read in this order on first contact, because each later one references concepts pinned by the earlier ones:

1. `EVS-PRD-auth-session` — the credential lifecycle other PRDs assume.
2. `EVS-PRD-action-submitter` — the simplest of the three transport-bridging interfaces.
3. `EVS-PRD-view-subscriber` — the substrate's `Update<T>` stream made transport-agnostic.
4. `EVS-PRD-permission-source` — per-Principal projection access.
5. `EVS-PRD-cross-process-event-transport` — the wire envelope shared by the Remote impls of the three above.
6. `EVS-PRD-reaction-scope` — the shared scope abstraction (`LocalScope`/`RemoteScope`) that composes the four interfaces and surfaces `ConnectionStatus`. Widgets thread this, not the four interfaces individually.
7. `EVS-PRD-reaction-widget-contract` — the Flutter widget layer that consumes the scope.

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

E. The `ViewSource.watch<T>` contract — its return type (`Stream<Update<T>>`), its `Update<T>` variant set, and the consumer-supplied mapper signature — SHALL be designed so that batched or cursor-based snapshot delivery can be added as a purely **additive** evolution. Any future enhancement to *how* snapshot rows are delivered (chunking, paging, cursor resumption) SHALL NOT change the existing variant shapes or break consumers that ignore the enhancement.

### Rationale

**Why does the interface return the substrate's existing `Update<T>` type, rather than wrap it?** The substrate's `Update<T>` already has the four variants the consumer needs (`Snapshot`, `Delta`, `Tombstone`, `EndOfReplay`) with the right semantics. Wrapping it in a parallel `ReactionUpdate<T>` would introduce a translation layer that adds nothing — and would obligate the library to keep the wrapper in sync with substrate changes forever. Reusing `Update<T>` directly means the substrate's contract IS the consumer's contract.

**Why does the mapper apply client-side in the Remote impl (C)?** The wire envelope ships the raw row as `Map<String, Object?>`, not the consumer's typed `T`. The mapper turns the map into `T` — that's a consumer-defined conversion that has no business running on the server. Server-side mapping would also force the wire codec to know about every consumer type, which violates the library's domain-neutrality.

**Why "source-identical" widget code (D)?** Same rationale as `EVS-PRD-action-submitter`-E.

**Why pin pagination-readiness (E) without building pagination?** Pagination (cursor-based or batched snapshot delivery) is genuine YAGNI today — no consumer has a measured large-view problem, and the substrate's snapshot-then-deltas semantics serve the expected scale. But the library is greenfield; the right move is to define the contract such that adding pagination later is non-breaking, rather than ship a contract that locks consumers in and then has to migrate them. Concretely: `Snapshot<T>` already carries one row at a time, so a future "batched snapshot" can ship as either a new variant (e.g., `SnapshotBatch<T>`) that defaults-translates to a sequence of `Snapshot<T>` for consumers that don't observe it, or as a flag on `Snapshot<T>` (e.g., `lastInBatch`) that consumers may safely ignore. The `EndOfReplay` marker stays definitive. Pinning the additive-evolution promise as a normative assertion prevents future authors from shipping a breaking redesign in the name of "cleanup."

*End* *View Subscriber* | **Hash**: bfaba693

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

H. The Remote-side wire transport SHALL implement automatic reconnection with exponential backoff on WS drops (close-frames other than `4001 auth_rejected` and `4003 permissions_changed`, which route to `AuthSession` / permission-refresh instead). On successful reconnection it SHALL re-authenticate using the currently-stored credential and re-issue every active subscribe; each re-issued subscription SHALL replay a fresh `Snapshot × N → EndOfReplay → live` per the substrate's snapshot-then-deltas semantics.

I. The Remote-side transport SHALL surface its observable connection state via the `ReactionScope`'s `ConnectionStatus` stream defined in `EVS-PRD-reaction-scope`. Status transitions SHALL be driven by observable WS lifecycle events (open, close, reconnect-attempt, retry-exhausted) — the library SHALL NOT synthesize ping requests or poll to derive status, and consumer code SHALL NOT need to infer connection state from subscription-stream liveness.

### Rationale

**Why JSON rather than a binary protocol?** The wire serves Flutter web clients (where Dart compiles to JavaScript) and pure-Dart server endpoints. JSON has zero-cost ergonomics in both environments, plays nicely with browser dev-tools, and matches the existing portal's transport format. Binary protocols (protobuf, MessagePack) would be a premature optimization at the expected portal-UI scale of ~1–20 concurrent users.

**Why multiplex subscriptions over one WebSocket (D)?** A portal user typically opens 3–10 concurrent subscriptions (one per visible panel). One connection per subscription would burn browser connections (HTTP/1.1 limits ~6 per origin) and add per-subscription handshake latency. Multiplexing over one connection is the standard reactive-UI pattern and matches what frameworks like Phoenix LiveView, Apollo, and others do.

**Why is per-subscription authorization the server's job (E)?** A portal user must not be able to subscribe to events about participants they are not assigned to. The server consults the requesting `Principal`'s `EffectiveAuthorization` (which itself derives from the substrate's `role_permission_grants` and `user_role_scopes` projections) and adjusts the underlying `subscribe<T>` filter accordingly — typically by restricting the `aggregates` set or composing additional filter clauses. The substrate provides the filter primitives; `reaction` provides the gate.

**Why is "no new epistemic layer" (G) load-bearing?** The substrate's Charter Assertion I (Layer 1 facts vs Layer 2 conventions) commits to documenting which guarantees are absolute (cryptographic, structural) versus which are library-provided defaults. The wire ships Layer-1 facts (sequenced `Update<T>` instances with intact ordering and identity); the receiver applies the *same* Layer-2 conventions the sender applies (same `ProjectionSpec` interpretation). The wire does not author a third layer of interpretation. Without this assertion, a future "wire-side optimization" could quietly re-interpret events differently than the in-process path, breaking consumer assumptions.

**Why isn't action-submission idempotency-key handling pinned here?** Idempotency is the substrate's existing dispatcher concern (`EVS-PRD-action-dispatch`) and the widget-side key-generation policy (`EVS-PRD-reaction-widget-contract`-E). The wire just carries the key as an `idempotencyKey` field on the `ActionSubmission`. No special wire treatment is needed.

**Why auto-reconnect with backoff in the library (H), not delegated to consumers?** Reconnection is a transport-layer concern, not an application concern; every consumer would otherwise reinvent it (and most would get the backoff wrong). The library knows the WS lifecycle and the close-frame semantics; consumers do not. Centralising the policy keeps it correct, observable, and tunable in one place. The `4001`/`4003` carve-outs preserve the auth-event semantics: an auth-rejected close must surface as `Expired` (re-authenticate), not as a transient drop to retry; a permissions-changed close must trigger a permission refresh, not a blind reconnect of stale subscriptions.

**Why is `ConnectionStatus` defined on the scope (per `EVS-PRD-reaction-scope`) rather than per Remote impl (I)?** Connection state is a property of the shared transport (`RemoteConnection`), not of any one interface. All four Remote impls share the WS; the status belongs in one place. Exposing it on the scope also lets the `LocalScope` report `Connected` trivially, so widget code consuming `ConnectionStatus` is source-identical across Local and Remote (per the substrate-agnostic widget contract). Driving from observable WS events rather than synthetic pings keeps the signal cheap, accurate, and free of contention with normal traffic.

*End* *Cross-Process Event Transport* | **Hash**: a36d45f2

## EVS-PRD-reaction-scope: Reaction Scope

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-action-submitter, EVS-PRD-auth-session, EVS-PRD-cross-process-event-transport, EVS-PRD-library-charter, EVS-PRD-permission-source, EVS-PRD-view-subscriber

### Purpose

Consumer code — and the widget layer in particular — needs a single substrate-agnostic composition root that bundles the four library interfaces together with live transport-connection state. The `ReactionScope` abstraction is that root; `LocalScope` and `RemoteScope` are its two implementations. Without it, each consumer must thread four interface instances individually and has no authoritative signal for whether the transport is up — both of which silently break source-identical Local vs Remote consumption at the composition boundary.

### Assertions

A. The library SHALL define a `ReactionScope` interface that exposes all four library interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`) as getters, a synchronous `ConnectionStatus get connectionStatus` getter, a `Stream<ConnectionStatus>` of connection-state transitions, and an asynchronous `dispose()` for graceful teardown.

B. `ConnectionStatus` SHALL be a sealed type with exactly three variants: `Connected`, `Reconnecting`, and `Disconnected`.

C. The library SHALL ship a `LocalScope` implementation that composes the four `Local*` interface impls and reports `ConnectionStatus.Connected` for the lifetime of the scope. In-process composition has no transport to lose; the trivial always-`Connected` report keeps consumer code source-identical without nil-checking the in-process case.

D. The library SHALL ship a `RemoteScope` implementation that composes the four `Remote*` interface impls over a shared `RemoteConnection` and drives `ConnectionStatus` transitions from the underlying WS lifecycle: `Connected` on initial WS open and on each successful reconnection; `Reconnecting` between WS drop and a successful re-open; `Disconnected` only after the auto-reconnect policy (`EVS-PRD-cross-process-event-transport`-H) gives up.

E. Consumer code that depends only on the `ReactionScope` interface (and the four interfaces it exposes) SHALL be source-identical regardless of which scope implementation is composed at runtime.

### Rationale

**Why a shared scope interface at all?** Each of the four interface PRDs independently pins "source-identical Local vs Remote." But composition needs a unifying type so widget code can hold one reference and pass it down via an `InheritedWidget`. Without `ReactionScope`, widget code holds either a `LocalScope` concrete reference or a `RemoteScope` concrete reference — and the substrate-agnostic promise is structurally broken at the composition boundary even though every individual interface keeps its promise. The shared interface closes the gap.

**Why `ConnectionStatus` on the scope rather than per Remote impl?** Connection state is a property of the shared transport (`RemoteConnection`), not of any one interface. `RemoteAuthSession`, `RemoteActionSubmitter`, `RemoteViewSource`, and `RemotePermissionSource` all share one WS; exposing the status once on the scope avoids per-impl duplication and keeps the observable signal authoritative. `LocalScope`'s trivial always-`Connected` report makes consumer code (especially the widget layer's `ViewBuilder`) source-identical: it can always observe a non-null `connectionStatus` without conditional logic for the in-process case.

**Why three variants (not two)?** `Reconnecting` is operationally distinct from `Disconnected`: the appropriate UX response is "show a reconnecting banner, keep last-known data visible" rather than "show a hard error and prompt for user action." `Disconnected` (after the retry policy gives up) is the actionable error. A two-state online/offline model loses the "we're still trying" affordance, which is the whole point of the auto-reconnect machinery.

**Why drive transitions from the WS lifecycle rather than HTTP?** The HTTP client makes one request at a time; its "is the network up" answer coincides with each request's success/failure and is therefore discontinuous. The WS is a long-lived channel whose liveness is observable directly via close-frames and reopen events. Tying `ConnectionStatus` to WS lifecycle gives a continuous, accurate signal; tying it to HTTP would require synthetic ping requests, which add traffic and add an inference layer that can disagree with WS reality.

*End* *Reaction Scope* | **Hash**: 3752964b

## EVS-PRD-reaction-widget-contract: Reaction Widget Contract

**Level**: PRD | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-action-submitter, EVS-PRD-auth-session, EVS-PRD-permission-source, EVS-PRD-reaction-scope, EVS-PRD-view-subscriber

> **Implementation status:** Designed; `reaction_widgets` package not yet implemented. The assertions below are normative — they are the contract the future package MUST satisfy when built. Audit tooling SHOULD treat coverage gaps here as "package not yet built", not as drift between spec and shipped code.

### Purpose

Flutter widget code consuming `reaction`'s `ReactionScope` SHALL be substrate-agnostic: the same widget composition SHALL work whether bound to a `LocalScope` or `RemoteScope`. The library is **headless** — it ships Builder primitives, imperative listeners, the scope-threading `InheritedWidget`, a permission gate, an error sink, and widget-test doubles, and **nothing else**. The library ships NO rendered or styled widgets (no buttons, no list widgets, no theming, no modality-aware affordances); each downstream consumer renders its own sugar on top of the builders. This split exists because the two near-term consumers (the hht_diary mobile app and the Flutter-web portal UI) run in genuinely different modalities — and any shared styled widget would encode the wrong assumption for one of them by construction.

### Assertions

A. The widget library SHALL provide an `InheritedWidget` (`ReActionScope`) that threads a `ReactionScope` instance — and therefore all four library interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`) plus the authoritative `ConnectionStatus` — down the widget tree, accessible via `ReActionScope.of(context)`.

B. Widget code consuming `ReActionScope.of(context)` SHALL be source-identical when the scope is composed as a `LocalScope` versus a `RemoteScope`.

C. The widget library SHALL provide headless Builder primitives for action submission (`ActionBuilder`) and view subscription (`ViewBuilder`) that delegate rendering entirely to a caller-supplied builder function; the primitives themselves SHALL NOT render any visual content.

D. The widget library SHALL provide an imperative side-effect widget (`ViewListener`) that fires a callback on view-state transitions without rebuilding the widget tree.

E. Action-submission widgets SHALL generate a UUID v4 idempotency key at first submission attempt, retain that key for the in-flight submission so retries during `Submitting` reuse it, and generate a fresh key only after the submission reaches a terminal state (Success, Denied, or Failed). Consumers SHALL be able to override the generated key by supplying an explicit `idempotencyKey` on the submission factory.

F. The widget library SHALL be source-organized so that no widget references the substrate's storage backend, dispatcher, or projection registry directly; all substrate access SHALL flow through `reaction`'s `ReactionScope` and the four interfaces it exposes.

G. The widget library SHALL ship NO rendered or styled widgets. The library SHALL provide ONLY: (i) a scope-threading `InheritedWidget` (`ReActionScope`), (ii) headless Builder primitives that delegate rendering to a caller-supplied builder (`ActionBuilder`, `ViewBuilder`), (iii) imperative side-effect widgets that fire callbacks without rendering content (`ViewListener`, `ReActionErrorListener`), and (iv) a `PermissionGate` that gates display of a caller-supplied child or builder on the active `Principal`'s `EffectiveAuthorization` without rendering any styled UI of its own. Rendered sugar — buttons, lists, theming, modality-aware affordances — SHALL live in downstream consumer applications.

H. The widget library SHALL ship widget-test doubles as a first-class deliverable: a `FakeReaction` (and equivalent `FakeReactionScope` or `ReActionScope.test(...)` constructor) implementing the `ReactionScope` contract for unit/widget tests, plus a pump helper that mounts a widget under test against the fakes. The doubles SHALL allow tests to drive — deterministically and without timing — `AuthStatus` transitions, `ActionSubmitter.submit` results (including each `DispatchResult` variant), view-row updates (`Snapshot` / `Delta` / `Tombstone` / `EndOfReplay`), permission-snapshot changes, and `ConnectionStatus` transitions.

I. `ViewBuilder<T>` SHALL expose its rendering state via a sealed `ViewState<T>` with exactly three variants: `Loading` (pre-`EndOfReplay`, no rows yet), `Ready(List<T> rows)` (post-`EndOfReplay`, live), and `Stale(List<T> lastRows, Object error)` (transport disconnected, last-known rows retained for UX continuity). The transition to `Stale` SHALL be driven by the composed `ReactionScope`'s `ConnectionStatus` — NOT by inference from subscription-stream liveness.

J. `ViewBuilder<T>` SHALL support an opt-in `progressive` mode that exposes partial row sets to the builder during snapshot replay, allowing large-view first-paint without blocking on the full snapshot. The default mode SHALL surface `Loading` until `EndOfReplay`, then transition to `Ready` with the full snapshot.

### Rationale

**Why headless — no rendered or styled widgets in the base (G)?** The two near-term consumers (hht_diary mobile and the Flutter-web portal UI) are very different modalities: a mobile submit button (large tap target, "queued offline" posture, haptic feedback) is wrong for the web portal (refuses offline, desktop affordances, hover states), and vice versa. A shared styled widget would inevitably encode the wrong assumption for one of them, forcing each consumer to fight overrides. The base layer therefore stops at the **headless plumbing** that is identical across modalities — state machines, lifecycle management, scope threading, test doubles — and leaves rendering to per-app sugar. The earlier "two-tier within one package" framing (Builder primitives + sugar widgets like `ActionButton`/`ViewListView` in the same library) is superseded: the two tiers still exist, but they live in different packages with the package boundary running along the modality split. Base provides primitives; each app provides its own sugar.

**Why `ViewListener` is a separate widget rather than a flag on `ViewBuilder` (D)?** Imperative side effects (showing a modal on state transition) are a different shape from reactive rebuilds. Trying to wedge them into a builder requires either side effects in `build()` (which Flutter explicitly forbids) or a callback parameter that fires from inside `setState` (which violates the principle that `build()` should be pure). A separate widget that uses `didChangeDependencies` / `addListener` cleanly lets consumers attach side effects without polluting the rebuild path. This mirrors `BlocListener`/`ProviderListener` in popular state-management libraries — a well-known pattern.

**Why UUID v4 idempotency keys with the lifetime caching policy (E)?** The substrate's idempotency-store is keyed on `(principalId, idempotencyKey)`; without a key, every submission is a fresh logical action. UUID v4 gives deterministic uniqueness without coordination. Caching the key for the in-flight `Future` lifetime means a user double-tap during `Submitting` is correctly treated as a retry of the same logical action (the dispatcher returns the cached `DispatchResult` for the second tap). Generating a fresh key after a terminal state means a deliberate re-press (e.g., "save then save again") is correctly treated as a new logical action. The consumer override exists for cases where stable per-form keys (e.g., a key derived from a draft document ID) are wanted.

**Why "no direct substrate access from widgets" (F)?** Without this, a widget could reach past `reaction`'s interfaces into `EventStore` or `ActionDispatcher` directly. That would break substrate-agnosticism (the widget would no longer work over the Remote transport) and violate the layered architecture. A structural scan test verifies the assertion.

**Why ship test doubles as a first-class, asserted deliverable (H)?** Test doubles for the four interfaces and `ReactionScope` are exactly the kind of "every consumer would otherwise reinvent" plumbing that justifies the headless base's existence. Half the layer's value proposition is correctness; the other half is testability. Without shipped fakes, every downstream app writes them, drifts from each other, and discovers `pumpWidget` ergonomics independently. With shipped fakes, downstream widget tests are turnkey and consistent. Treating doubles as a normative deliverable (rather than an internal test-fixture) makes this guarantee explicit and audit-checkable.

**Why `ViewState` with `Stale` retaining last-known rows (I)?** When the transport drops, the right UX answer is "show stale data with a reconnecting banner," not "blank the screen." Retaining `lastRows` on the `Stale` variant lets apps render that affordance trivially. The variant is named `Stale` (rather than echoing the transport-layer term `Disconnected`) for two reasons: it names what the variant IS at the rendering layer (a stale-data surface), and it avoids a structural identifier collision with `ConnectionStatus.Disconnected` from `package:reaction` — any consumer that uses both `ViewBuilder` and a `ConnectionStatus`-aware widget would otherwise need a `hide`-clause workaround. Driving the transition from the `ReactionScope`'s authoritative `ConnectionStatus` — rather than inferring "the stream stopped" — keeps the widget contract aligned with the transport contract and avoids whack-a-mole edge cases (e.g., is a long-idle stream "disconnected" or "just quiet"?). Earlier drafts considered inferring connection state at the widget layer by observing subscription-stream liveness; that was a workaround for a missing `reaction` surface. The proper fix was to add `ConnectionStatus` to `ReactionScope` (`EVS-PRD-reaction-scope`); the widget then consumes an authoritative signal.

**Why progressive rendering as opt-in, default `Loading`-until-`EndOfReplay` (J)?** The default deterministic behavior matches the substrate's snapshot-then-deltas guarantee semantically: until `EndOfReplay`, the snapshot is incomplete. For most views this is the right default — render once with everything. For very large views (where snapshot delivery takes seconds), opt-in `progressive: true` lets the list paint as rows arrive. Making it opt-in keeps small-view callers from accidentally rendering against partial state, and keeps the contract additive-compatible with future cursor-based snapshot delivery (per `EVS-PRD-view-subscriber`-E): a future `SnapshotBatch` variant simply becomes another source of partial rows under `progressive` mode.

**Why agnostic state management?** The widget library's value proposition is the substrate-agnostic widget contract, not a state-management opinion. Baking in a single choice (signals, Provider, Riverpod, BLoC) excludes consumers who use the others. Agnostic primitives (`Stream`, `ValueListenable`, `InheritedWidget`) are the lingua franca, and a `Stream` bridges cleanly to signals (stream-to-signal), Provider, or Riverpod alike. The opt-in adapter that earns its keep is `reaction_widgets_signals`: signals is the reactive idiom both consumers use — hht_diary mobile, and the portal-ui after its Phase IV substrate cutover. Provider/Riverpod adapters would follow the same additive pattern only behind an external consumer that needs them.

*End* *Reaction Widget Contract* | **Hash**: 53508e6a

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

**Why not ship sugar widgets (`ActionButton`, `ViewListView`) inside `reaction_widgets`?** An earlier two-tier-within-one-package design intended exactly this. The two near-term consumers (hht_diary mobile and Flutter-web portal UI) run in genuinely different modalities; a mobile-shaped `ActionButton` (large tap target, "queued offline" posture) is wrong for a desktop-web portal (refuses offline, hover affordances), and vice versa. Shipping any shared styled widget in the base would encode the wrong assumption for one of the consumers by construction, forcing each to fight overrides. The two-tier idea survives but the boundary moves: base provides headless primitives; each app provides its own sugar matching its modality. See `EVS-PRD-reaction-widget-contract`-G rationale.

**Why not infer connection state at the widget layer from subscription-stream liveness?** Considered as a way to avoid changing `reaction`. Rejected: it is a workaround that locks the widget contract to whatever the Remote impl happens to do today (e.g., does the stream close on WS drop, or buffer silently?). The proper fix was to add an authoritative `ConnectionStatus` surface to `ReactionScope` (`EVS-PRD-reaction-scope`). Greenfield principle: define the library properly, do not paper over a missing surface in a layer above.

**Why ship FakeReaction in a sibling `reaction_widgets_testing` package, not in `reaction_widgets/lib/src/testing/`?** FakeReaction imports `flutter_test`'s `WidgetTester` and `pumpEventQueue` to provide `pumpReactionWidget`. If it lived in `reaction_widgets`'s main `dependencies` (not `dev_dependencies`), every consumer's release build would pull `test_api`/`matcher`/etc. into shipped binaries — pure bloat. The sibling package keeps the test-double deliverable first-class per assertion H while letting consumers add it as a `dev_dependency` only.

**Why not build cursor/batched snapshot delivery into the wire now?** The whole repo is in-scope for this work, so it would be possible. But there is no consumer with measured large-view scale, and the substrate's snapshot-then-deltas semantics serve the expected scale. Building cursor pagination now would be speculative complexity. The right move is to **define the contract to be additive-ready** (`EVS-PRD-view-subscriber`-E) so cursor delivery can ship later as a non-breaking enhancement, plus give the widget layer an opt-in `progressive` mode (`EVS-PRD-reaction-widget-contract`-J) for the rendering half of the problem. This is YAGNI with a non-breaking future, not a workaround.

## Open questions

These are tracked here for resolution during implementation; resolution should land as new assertions or as Rationale updates to the affected PRDs.

1. **Server-side per-subscription authorization details.** `EVS-PRD-cross-process-event-transport`-E pins the obligation; the implementation specifics (does the server compose additional filter clauses, restrict the aggregates set, or short-circuit the subscription with a wire-level denial?) are deferred to implementation.
2. **Custom-route registration ergonomics.** `reaction`'s shelf server lets consumers register additional routes (login, linking-code onboarding) outside the standard `reaction` flow; those routes bypass auth-validator middleware. The exact API for this — `server.addCustomRoute(...)` vs exposing the `Router` for direct mutation — is an implementation detail.
3. **`JwtAuthValidator` reference implementation.** Whether to ship a JWT validator alongside `TrustingAuthValidator` is deferred; pluggable seam exists from day one regardless.

*Previously open (now resolved):*

- *Snapshot pagination / size limits* — resolved by `EVS-PRD-view-subscriber`-E (additive contract) plus `EVS-PRD-reaction-widget-contract`-J (opt-in `progressive` rendering). Cursor delivery itself is deferred-but-additive (see Future work).
- *Reconnect strategy details* — resolved by `EVS-PRD-cross-process-event-transport`-H (auto-reconnect with exponential backoff, full re-subscribe on recovery) and `EVS-PRD-reaction-scope`-D (authoritative `ConnectionStatus` transitions). The v1.1 "resume from last applied sequence" optimization is still future work but no longer an open design question.

## Future work

These are explicitly out of scope for v1 but anticipated as natural extensions:

- **`EventLogView`** — prettified `Stream<Update<StoredEvent>>` consumer with filter chips (entryType, eventType, principal, time range) and a tap-to-drawer detail panel. Substrate-shaped sugar; useful in any event-sourced app for audit, debugging, ops dashboards. Lives in downstream consumer apps (not in headless base, per `EVS-PRD-reaction-widget-contract`-G).
- **`TraceView`** — given a root event, walks correlation IDs (`action_invocation_id`, `flowToken`) and renders the related-event tree/timeline. The substrate already records both correlation fields on every event. Lives in downstream consumer apps.
- **`reaction_widgets_signals` adapter** — the opt-in state-management adapter target: a signals-shaped surface over `reaction`'s `Stream<Update<T>>` (a row-set signal that folds `Snapshot`/`Delta`/`Tombstone`, an action-submission signal, a `ConnectionStatus`/stale signal). signals is the reactive idiom both near-term consumers use — hht_diary mobile, and the portal-ui after its Phase IV substrate cutover — so it is the one adapter with real dual-consumer demand. Sourced from the diary's substrate-adoption work rather than built speculatively; the headless base stays on raw streams. Provider/Riverpod adapters follow the same additive pattern only behind an external consumer that needs one.
- **Cursor / batched snapshot wire delivery** — per `EVS-PRD-view-subscriber`-E the wire contract is **additive-ready**; cursor delivery can ship later as a non-breaking enhancement. Defer until measured large-view scale demands it.
- **Resume-from-sequence on reconnect** — current auto-reconnect (`EVS-PRD-cross-process-event-transport`-H) re-subscribes from scratch with a fresh `Snapshot × N → EndOfReplay → live`. A v1.1 optimization would resume from the last applied sequence (server retains events; cheap if recent). Pure-additive — does not change `ConnectionStatus` semantics or `Update<T>` variants.

## Migration story (hht_diary portal)

`reaction` adoption in the existing `cure-hht/hht_diary/apps/sponsor-portal/` portal is a substantial but bounded refactor, not a wholesale rewrite. Recorded here so that authoring `reaction` keeps the migration path concrete.

| Concern | Today (portal) | After `reaction` migration | Migration size |
|---|---|---|---|
| **AuthSession** | Firebase Auth + 1535-LOC `AuthService` | `RemoteAuthSession` wraps Firebase ID token; `AuthService` stays as consumer | Modest — preserves existing UX |
| **ActionSubmitter** | 20–30 bespoke REST POST handlers, ad-hoc shapes | Codify `ActionSubmission` ontology + uniform `DispatchResult` + `ActionDispatcher` on the server | Substantial one-time codification cost |
| **ViewSource** | Imperative REST GETs in `initState` + `setState`; no real-time | Reactive `Stream<Update<T>>` with snapshot+tail; rebuild pages around `ViewBuilder` | Category change — the largest net-new value |
| **PermissionSource** | Inline role + site checks in handlers; hardcoded ontology | `RolePermissionGrants` projection + per-Principal snapshot via `RemotePermissionSource` | Lightweight wrapper over existing logic |
| **State mgmt** | Provider 6.x + ChangeNotifier + setState | Agnostic Streams + `InheritedWidget` (`ReActionScope`); raw streams usable directly; opt-in `reaction_widgets_signals` adapter is the target | Maps cleanly; portal converges on signals at its Phase IV cutover |
| **Wire** | REST only, no WebSocket | REST POST + WS multiplex | Medium refactor; shelf supports WS upgrade |

The migration unlocks substrate-shaped opportunities the existing portal expresses informally: patient lifecycle state machines (`linkable → linking_in_progress → connected → disconnected`), linking code lifecycle, questionnaire instance lifecycle, immutable audit trail — all become first-class events with declarative `ProjectionSpec`s for the views the UI consumes.
