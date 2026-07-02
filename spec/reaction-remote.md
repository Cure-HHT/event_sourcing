# Reaction Remote Impls, Reference Server, and Wire Protocol

**Status**: Implemented — cross-process client (`reaction/lib/src/remote/`),
reference server handlers (`reaction/lib/src/server/`), and wire codecs
(`reaction/lib/src/wire/`). Normative `EVS-DEV-*` requirement blocks are
in Section 6.
**Elaborates**: `spec/prd-reaction.md` PRDs `EVS-PRD-auth-session`,
`EVS-PRD-action-submitter`, `EVS-PRD-view-subscriber`,
`EVS-PRD-permission-source`, and
`EVS-PRD-cross-process-event-transport`.
**Depends on**: `spec/scoped-permissions.md` for the
`AuthorizationPolicy`, `EffectiveAuthorization`, and `ScopeValue`
shapes that the reference server's per-subscription authorization
consults.

**Note on scope.** This design covers client and server together in one
cohesive document. The wire protocol cannot be specified, codec-tested,
or end-to-end-validated without both sides; the PRD architecture
(`spec/prd-reaction.md`) already puts `local/`, `remote/`,
`server/`, and `wire/` in one package.

## Scope

This spec pins the design of the cross-process half of the `reaction`
package: the `Remote*` client-side implementations of the four
substrate-agnostic interfaces, the JSON wire protocol they speak, and
the shelf-based reference server that terminates the wire and bridges
back to an in-process `EventStore` + `ActionDispatcher`.

In scope:

- Module layout under `reaction/lib/src/{remote, server, wire}/`.
- Wire protocol: HTTP route table, WebSocket control plane, JSON
  envelope shapes, close-frame semantics, reconnect strategy.
- Client-side composition: `RemoteScope`, shared `RemoteConnection`,
  per-impl lifecycle for the four `Remote*` classes.
- Server-side composition: `ReactionHandlers` config bundle,
  optional `authMiddleware`, WS upgrade handler, per-subscription
  authorization mechanism, per-connection write serialization.
- Reference auth validator (`TrustingAuthValidator`) for dev/test.
- Testing strategy across codec, server-unit, and end-to-end layers.

Out of scope (deferred):

- Production validators (Firebase JWT, Auth0, install-UUID linking
  code, etc.) — these live in app code per the substrate's trust-
  boundary discipline; see "Trust boundary expansion" below.
- Reactive re-narrowing of active subscriptions' `aggregates`
  filters mid-stream (mutating an open `subscribe<T>` to narrow it).
  The mid-session permission-change handling (force-logout
  on revocation + stale-data signal on expansion / containment
  change) avoids this entirely; see Section 4.

Further deferred optimizations (resume-from-sequence reconnect, batched
snapshot delivery for very large views) are recorded in
`spec/roadmap/reaction.md`.

## Architectural commitments

The design follows the load-bearing commitments from CLAUDE.md
("Architectural commitments" and "Trust boundaries" sections). The
ones most directly shaping this design:

### 1. Substrate-agnostic widget contract

Consumer code that depends only on the four interfaces (`AuthSession`,
`ActionSubmitter`, `ViewSource`, `PermissionSource`) MUST be source-
identical when composed against `Local*` versus `Remote*`
implementations. This is the central promise pinned by
`EVS-PRD-action-submitter/E`, `EVS-PRD-view-subscriber/D`, and
`EVS-PRD-reaction-widget-contract/B`. The Remote impls in this design
preserve it strictly.

### 2. Wire is a faithful relay (no third epistemic layer)

`EVS-PRD-cross-process-event-transport/G`: the wire ships Layer 1
facts (sequenced `Update<T>` instances with intact ordering and
identity); the receiver applies the **same** Layer 2 conventions the
sender applies. The wire does not author a third layer of
interpretation. Consequence: the consumer-supplied `mapper` runs
client-side, not server-side; the server ships rows as opaque
`Map<String, Object?>`.

### 3. Auth validators are consumer-supplied

The reaction lib ships only the `PrincipalAuthValidator` interface
(the pluggable seam) and `TrustingAuthValidator` (dev/test reference).
Concrete production validators (Firebase, Auth0, linking-code) live
in app code where they can encode the deployment's identity-provider
choices. The lib never commits to one shape. See
`EVS-PRD-auth-session/F` and its "Why no JwtAuthValidator in v1?"
rationale.

### 4. Permission policy stays substrate code

Per-subscription authorization consults the substrate's existing
`role_permission_grants` and `user_role_scopes` projections. The
reaction server introduces no parallel policy mechanism; it composes
substrate filter primitives based on the requesting Principal's
`EffectiveAuthorization`.

### 5. Append-Only Primitives discipline applies to the wire

Once a JSON envelope ships under a given `"type"` discriminator with
given field shape, that shape is frozen. New behavior is a new
envelope type with a new discriminator value. Treat the wire surface
the same way the substrate treats `TransformPrimitive` subclasses.

## Section 1 — Module layout

```text
reaction/lib/src/
  interfaces/      (existing, unchanged)
  local/           (existing, unchanged)
  state/           (existing, unchanged)
  wire/            (NEW)
    envelope.dart                  HTTP + WS envelope types
    update_codec.dart              Update<Map<String, Object?>> codecs
    action_submission_codec.dart   ActionSubmission JSON codec
    dispatch_result_codec.dart     DispatchResult JSON codec
    effective_authorization_codec.dart EffectiveAuthorization JSON codec
                                       (substrate type)
    subscription_messages.dart     Subscribe/Unsubscribe/Update msgs
    filter_codec.dart              SubscriptionFilter JSON codec
    principal_codec.dart           Principal JSON codec
  remote/          (NEW; client-side Remote* impls)
    remote_auth_session.dart
    remote_action_submitter.dart
    remote_view_source.dart
    remote_permission_source.dart
    remote_connection.dart         Shared WS lifecycle, lazy connect,
                                   close on last unsubscribe + 30s grace
    remote_scope.dart              Composition class
  server/          (NEW; shelf-compatible handlers, no "server" class)
    reaction_handlers.dart         ReactionHandlers config bundle;
                                   exposes .me / .actions / .permissions
                                   as shelf.Handler getters and
                                   .subscriptions(validator) as a
                                   shelf.Handler factory (the validator
                                   is passed per-call because the WS
                                   upgrade path cannot carry an
                                   Authorization header from Flutter
                                   web — auth happens in the first WS
                                   message)
    auth_middleware.dart           Optional authMiddleware(validator)
                                   + principalFromContext(req) helper
    action_handler.dart            POST /actions handler factory
    me_handler.dart                GET /me handler factory
    permission_handler.dart        GET /permissions/snapshot factory
    subscription_handler.dart      WS upgrade; per-conn state; relay
    ws_connection_registry.dart    Map<userId, Set<WebSocketChannel>>
    authz_watcher.dart             Server-wide watcher; force-logout on
                                   revocation, stale_data on expansion
    validators/
      trusting_auth_validator.dart Dev/test only
```

There is intentionally no `ReactionServer` class. Consumer deployments
that already run their own shelf servers — `shelf` + `shelf_router`
pipelines with deployment-specific middleware (authentication,
telemetry, CORS, request logging) — compose `reaction`'s handlers into
their existing routers directly. The "server" is whatever consumer-owned shelf app the
handlers are mounted into; `reaction` itself never calls
`shelf_io.serve` and never owns an `HttpServer`. See
"Decisions and alternatives rejected" for the full rationale.

The top-level `reaction.dart` barrel grows exports for:

- `RemoteScope`, `RemoteAuthSession`, `RemoteActionSubmitter`,
  `RemoteViewSource`, `RemotePermissionSource`.
- `ReactionHandlers` (the config bundle).
- `authMiddleware`, `principalFromContext` (optional auth helpers).
- `WsConnectionRegistry` (exposed on `ReactionHandlers.connectionRegistry`
  for ops visibility — connection counts per user, etc.).
- `TrustingAuthValidator`.
- `AuthorizationWatcher` stays package-private; consumers interact with it
  via `ReactionHandlers.watchContainment(...)`.

Wire types stay package-private (not exported from `reaction.dart`).
Codec functions are internal to the package.

`pubspec.yaml` gains: `http`, `web_socket_channel`, `shelf`,
`shelf_web_socket`, `shelf_router`. (`uuid` already a dependency.)

## Section 2 — Wire protocol

### HTTP route table

```text
POST /actions
  Header:  Authorization: Bearer <credential>
  Body:    JSON ActionSubmission envelope
  200:     JSON DispatchResult envelope
  401:     empty body (auth failure)
  400:     malformed envelope

GET /me
  Header:  Authorization: Bearer <credential>
  200:     JSON Principal envelope
  401:     empty body

GET /permissions/snapshot
  Header:  Authorization: Bearer <credential>
  200:     JSON EffectiveAuthorization envelope
  401:     empty body

GET /healthz
  No auth required
  200:     "ok"
```

The `Authorization: Bearer <credential>` header is standard; the
credential is opaque to the lib (passed directly to
`PrincipalAuthValidator.authenticate`). The substrate sees only the
validated `Principal` that the validator returns.

#### DispatchResult wire shape (`POST /actions` response body)

The substrate's `DispatchResult<TResult>` is a sealed type with seven
variants. The wire codec dispatches per variant on a `"type"`
discriminator and otherwise mirrors the substrate field names directly.
All variants ship as flat JSON objects:

```text
{"type": "success",
 "result": <jsonable TResult>,
 "emittedEventIds": ["<eventId>", ...]}

{"type": "unknown_action",
 "requestedName": "<the actionName the client submitted>"}

{"type": "parse_denied",
 "error": "<string; see lossy-error note>"}

{"type": "validation_denied",
 "error": "<string; see lossy-error note>"}

{"type": "authorization_denied",
 "permission": {"name": "<permName>",
                "scopeClass": "<class>" | null}}

{"type": "execution_failed",
 "error": "<string; see lossy-error note>"}

{"type": "idempotency_hit",
 "cachedResult": <jsonable TResult>,
 "priorEmittedEventIds": ["<eventId>", ...]}
```

JSON shape notes:

- `success` ships only the substrate's `emittedEventIds` (a list of
  event-id strings), NOT full `StoredEvent` envelopes. Consumers that
  want the full Layer 1 fact data for an appended event subscribe to
  the relevant view stream and observe the resulting `Update<T>`.
- `unknown_action.requestedName` mirrors the substrate's
  `DispatchUnknownAction.requestedName` (the name the client asked for).
- `parse_denied`, `validation_denied`, and `execution_failed` each carry
  a single `error` field. The substrate field is `Object error`; the
  wire codec emits `error.toString()` and decodes it back as the raw
  String. **This is lossy** — the structured-error path is recorded in
  `spec/roadmap/reaction.md`.
- `authorization_denied` carries only the substrate's `Permission`
  (inlined `{name, scopeClass}`). The substrate's
  `DispatchAuthorizationDenied` carries no `DenyReason` / `scope` /
  `detail`; the wire carries only the `Permission` (name + scopeClass).
- `idempotency_hit` ships `cachedResult` + `priorEmittedEventIds`
  (mirroring the substrate's `DispatchIdempotencyHit.cachedResult` and
  `priorEmittedEventIds`). No `cachedAt` timestamp — the substrate does
  not retain one.

### WebSocket upgrade — `GET /subscriptions`

Auth lives in the first WS message rather than the upgrade headers
because Flutter web's `WebSocket` constructor cannot set custom
headers on the handshake. First-message auth is the only design
uniform across Flutter native and Flutter web.

#### Client -> Server messages

```text
// First message on every connection (and on every reconnect):
{"type": "auth", "credential": "<opaque>"}

// Open a subscription:
{"type": "subscribe",
 "subscriptionId": "<client-chosen UUID v4>",
 "viewName": "<viewName>",
 "filter":   { ...JSON-encoded SubscriptionFilter, optional... },
 "aggregates": ["agg-1", "agg-2"]   // optional allow-list
}

// Cancel a subscription:
{"type": "unsubscribe",
 "subscriptionId": "<UUID>"
}
```

A `{"type":"resume","fromSequence":N}` message is an additive extension
recorded in `spec/roadmap/reaction.md`; the codec layout accommodates it
without breaking change.

#### Server -> Client messages

```text
// Response to auth:
{"type": "auth_ok", "principalId": "<id>"}
// or close-frame 4001 (auth_rejected); no continuation.

// Response to subscribe (exactly one of these or success-via-updates):
{"type": "subscription_denied",
 "subscriptionId": "<UUID>",
 "reason": "view_permission_denied" | "unknown_view"
         | "malformed_filter"}
// (no explicit subscription_ok; update flow IS the success signal)

// Update<T> envelopes — every envelope carries subscriptionId + sequence:
{"type": "snapshot",  "subscriptionId": "<UUID>", "sequence": N,
 "value": {...Map<String,Object?>...} | null}

{"type": "delta",     "subscriptionId": "<UUID>", "sequence": N,
 "value": {...Map<String,Object?>...}, "cause": "<eventId>"}

{"type": "tombstone", "subscriptionId": "<UUID>", "sequence": N,
 "aggregateId": "<id>"}

{"type": "end_of_replay", "subscriptionId": "<UUID>", "sequence": N}

// Stale-data signal: server tells the client that some of its
// cached scope state may be out of date (e.g., a role assignment
// just expanded the user's scope, or a containment projection
// re-parented an aggregate the user holds). The client should
// resubscribe to refresh; the server does NOT force-resub.
// Reason is informational for client UX; the server may also send
// this with no reason for "something changed; refresh if you care."
{"type": "stale_data",
 "reason": "permission_added" | "role_assigned"
         | "containment_changed" | null }

// Out-of-band (not tied to any one subscription):
{"type": "error", "code": "internal_error" | "protocol_error",
 "message": "<human-readable; for logging>"}
```

JSON shape notes:

- All wire envelopes are flat objects with a `"type"` discriminator.
  No nested polymorphic wrappers. Codec dispatch on `"type"`.
- `sequence` is the substrate's monotonic event sequence at the
  moment the update was generated (PRD assertion B).
- `snapshot` and `delta` carry `value` (the materialized row, which by
  `AggregateProjectionSpec` convention contains `aggregateId` as a
  column); `tombstone` has no row and ships `aggregateId` directly;
  `end_of_replay` is a per-subscription marker and carries neither.
- `snapshot.value` is nullable on the wire — the substrate's
  `Snapshot<T>.value` is `T?` and the wire preserves the explicit-null
  case (key present, value `null`).
- `delta.cause` is the substrate's event id that produced this delta
  (substrate `Delta<T>.cause`). Required on the wire; surfaces the
  Layer 1 fact of "which append generated this update."
- `value` (on snapshot / delta) is the raw `Map<String, Object?>` row
  from the substrate; client applies its consumer-supplied mapper.
  Server-side mapping would break domain-neutrality (server would need
  to know every consumer type) — rejected.

### Close frames

- `4001 auth_rejected` — credential failed validation; client maps
  to `AuthStatus.Expired`. Client does NOT reconnect.
- `4002 server_shutting_down` — graceful; client reconnects with
  backoff.
- `4003 permissions_changed` — the user's role or held-role
  permissions changed mid-session in a way that narrows their
  access (`role_unassigned` for this user; `permission_revoked`
  from a role this user holds). The client maps this to
  `AuthStatus.Expired` and refetches `/me` on reconnect to get a
  fresh `Principal` reflecting the new role/permission state. The
  credential itself is still valid (no JWT revocation) — the
  staleness is in the cached server-side authorization context.
- `1011 internal_error` — server bug; client reconnects.
- `1006 abnormal_closure` (network drop) — client reconnects with
  backoff.

### Reconnect strategy (refetch)

`RemoteConnection` reconnects with exponential backoff, governed by an
`ExponentialBackoff` policy whose defaults are: 250ms initial delay, a
2x multiplier per attempt, a 30-second per-attempt cap, and 10 attempts
before the connection gives up and transitions to `Disconnected` (a
total budget of roughly two minutes). The policy is overridable per
deployment via the `reconnectBackoff` parameter on the `RemoteScope`
(and `RemoteConnection`) constructor — tests pass ms-scale intervals so
the suite runs in seconds. On each reconnect attempt:

1. Send `{"type":"auth", "credential": <stored>}` and await the
   server's `auth_ok` before flushing any subscribe.
2. For each active subscription, re-send the original
   `{"type":"subscribe", "subscriptionId": <same as before>, ...}`.
3. Server treats reconnect as fresh: opens new substrate
   `subscribe<T>` per subscription. Snapshot replay re-fires from
   the substrate; client receives `Snapshot×N → EndOfReplay → Delta…`
   again.
4. Client-side `Stream<Update<T>>` (the one returned to widget code)
   sees the snapshot-replay flash.

This full-refetch reconnect is the shipped baseline. The
resume-from-sequence optimization — which would add a
`{"type":"resume","subscriptionId":...,"fromSequence":N}` message and
resume from the last-applied sequence rather than re-replaying — is
wire-compatible (the sequence field is already on every envelope) and
is recorded in `spec/roadmap/reaction.md`.

## Section 3 — Client lifecycle

### `RemoteScope` (composition class)

The production-side `ReactionScope` implementation for cross-process
clients. Composes the four `Remote*` interface impls over a shared
`RemoteConnection`, surfaces an authoritative `ConnectionStatus`
stream driven by WS lifecycle events, and owns graceful teardown.
Constructed once per user-session at app boot. Implements the
`ReactionScope` interface defined by `EVS-PRD-reaction-scope`.

```dart
final scope = RemoteScope(
  baseUrl: Uri.parse('https://api.example.com'),
  // Optional overrides for tests:
  httpClient: ...,
  wsFactory: ...,
  clock: ...,
  reconnectBackoff: ...,
);

final AuthSession      auth   = scope.authSession;
final ActionSubmitter  submit = scope.actionSubmitter;
final ViewSource       views  = scope.viewSource;
final PermissionSource perms  = scope.permissionSource;

// Authoritative transport state (per EVS-PRD-reaction-scope-D):
final ConnectionStatus           now    = scope.connectionStatus;
final Stream<ConnectionStatus>   status = scope.connectionStatusStream;

await scope.dispose();  // on app shutdown
```

`RemoteScope` derives the WS URL from `baseUrl`:

```text
http://...  -> ws://...
https://... -> wss://...
```

with `/subscriptions` appended.

`ConnectionStatus` transitions are driven directly by the underlying
`RemoteConnection`'s WS lifecycle (not synthesized; not polled):

| Trigger                                                | Status         |
| ------------------------------------------------------ | -------------- |
| Initial state (before first WS activation)             | `Disconnected` |
| First WS open succeeds                                 | `Connected`    |
| WS close (codes other than 4001 / 4003)                | `Reconnecting` |
| Reconnect attempt succeeds + auth + re-subscribe sent  | `Connected`    |
| Reconnect-backoff policy exhausted                     | `Disconnected` |

Close-frames `4001 auth_rejected` and `4003 permissions_changed` route
to `AuthSession`/permission-refresh respectively per the existing
behavior; they do NOT drive `ConnectionStatus` to `Reconnecting`.

`LocalScope` (the in-process sibling that composes the `Local*`
impls) reports `ConnectionStatus.Connected` for its entire lifetime.
This keeps widget code consuming `ConnectionStatus` source-identical
across Local and Remote.

### `RemoteConnection` (internal, package-private)

Owns the shared wire-state across all four `Remote*` impls of one
`RemoteScope`:

- **HTTP client** (`package:http`'s `Client`) — used by
  `RemoteAuthSession.setCredential` (GET /me), `RemoteActionSubmitter`
  (POST /actions), and `RemotePermissionSource` (initial GET
  /permissions/snapshot). Bearer header injected from the currently-
  stored credential.
- **WebSocket lifecycle** — lazy: not connected until the first
  `RemoteViewSource.watch` or `RemotePermissionSource` activation.
  Closed once the last subscription cancels AND a 30-second idle
  grace elapses (avoids thrash on tab toggles).
- **Subscription registry** — `Map<String, _ActiveSubscription>`
  keyed by client-chosen UUID v4 `subscriptionId`. Each entry holds
  the consumer's `StreamController<Update<Map<String, Object?>>>`
  so incoming envelopes can be routed.
- **Reconnect loop** — on close-frames 1006 / 1011 / 4002, schedules
  exponential backoff and reconnects. Replays auth message then
  every active subscribe message. Suppressed for 4001 (auth_rejected;
  that path flips `RemoteAuthSession` to `Expired` instead) and 4003
  (permissions_changed).
- **Subscribe-during-reconnect queueing** — a `watch<T>` that opens
  while the socket is down or mid-handshake registers its subscription
  immediately and awaits `_ensureConnected`, which blocks on the
  server's `auth_ok` before flushing the `SubscribeMsg`. So a subscribe
  issued during backoff is queued and sent once the reconnect completes,
  never racing ahead of authentication.
- **Credential storage** — single `String?` field. Writers:
  `RemoteAuthSession.setCredential`. Readers: HTTP bearer injection
  and WS auth message. Atomic across the four impls.

### Per-impl lifecycle

**`RemoteAuthSession`**

Status transitions:

| Trigger                                       | Status            |
| --------------------------------------------- | ----------------- |
| Constructor                                   | `NotAuthenticated`|
| `setCredential(null)`                         | `NotAuthenticated`|
| `setCredential(cred)` + GET /me returns 200   | `Authenticated`   |
| `setCredential(cred)` + GET /me returns 401   | `Expired`         |
| Any HTTP call returns 401                     | `Expired`         |
| WS close-frame 4001 auth_rejected             | `Expired`         |

There is intentionally no "validating" intermediate state. PRD-B
locks `AuthStatus` as exactly three variants; during the async GET
/me, the previous status holds. On completion the status transitions
atomically.

**`RemoteActionSubmitter`**

`submit(submission)` → HTTP POST /actions with JSON body.

- If `AuthSession.current` is not `Authenticated`, throws
  `TransportException` immediately (no submission attempted; matches
  `LocalActionSubmitter`'s symmetric behavior).
- On 200, decodes `DispatchResult` from the response body.
- On 401, throws `TransportException` AND notifies `RemoteAuthSession`
  to flip to `Expired`.
- On other non-2xx, throws `TransportException` with status code in
  message.

**`RemoteViewSource`**

`watch<T>(viewName, mapper, filter, aggregates)`:

1. Generate fresh UUID v4 `subscriptionId`.
2. Ask `RemoteConnection` for the shared WS (triggers lazy connect
   on first call).
3. Send `{"type":"subscribe", subscriptionId, viewName, filter,
   aggregates}`.
4. Return a `Stream<Update<T>>` that:
   - Maps incoming wire envelopes (already deserialized to
     `Update<Map<String, Object?>>` by the connection layer) through
     the consumer's `mapper` to produce `Update<T>`.
   - Emits `Stream.error` with a `TransportException` subclass on
     `subscription_denied`, then closes.
   - Closes cleanly when `.cancel()` is called; sends
     `{"type":"unsubscribe", subscriptionId}` to the server.

**`RemotePermissionSource`**

Two-phase load:

1. **On `RemoteAuthSession` becoming `Authenticated`:** fires
   GET /permissions/snapshot. Seeds `current` and emits on stream.
2. **Subsequent updates:** opens a WS subscription (via
   `RemoteConnection`) on `viewName: "role_permission_grants"`,
   filtered to the active Principal's rows. Each incoming
   `Update<Map<String, Object?>>` is converted via the same
   `EffectiveAuthorization` reducer the `LocalPermissionSource` uses
   (`AuthorizationPolicy.effectivePermissionsFor`).

When `AuthSession` flips to `Expired` or `NotAuthenticated`:
`current` → `null`, snapshot stream emits `null`, underlying WS
subscription cancelled.

### `dispose()` contract

`scope.dispose()`:

1. Cancels all active subscriptions (sends `unsubscribe` for each).
2. Closes the WS (close-frame `1000 normal`).
3. Closes the HTTP client.
4. Closes the four `StreamController`s (one per Remote impl).
5. After `dispose()`, calling anything on the scope or its impls
   throws `StateError`.

## Section 4 — Server-side composition

### `ReactionHandlers` (config bundle)

There is no `ReactionServer` class. The lib exposes a config bundle
that holds the four substrate handles + the view-scope registry and
surfaces one shelf `Handler` per route:

```dart
final reaction = ReactionHandlers(
  eventStore: store,           // already-opened substrate
  dispatcher: dispatcher,      // already-constructed ActionDispatcher
  policy: policy,              // AuthorizationPolicy
  viewScopeRegistry: viewScopes,
  // Optional:
  viewPermissionNamer: ReactionHandlers.defaultViewPermissionNamer,
);

// `reaction.me` / `.actions` / `.permissions` are shelf.Handler
// getters; `.subscriptions(validator)` is a shelf.Handler factory
// that takes the per-call validator (see "Why per-call validator on
// subscriptions" below). The consumer composes them into THEIR
// router however they like:
final router = Router()
  // Consumer's custom routes:
  ..get('/api/v1/app/config', appConfigHandler)
  ..post('/api/v1/auth/login', loginHandler)            // pre-auth
  // Reaction handlers, mounted wherever the consumer chooses:
  ..get('/api/v1/me',                   reaction.me)
  ..post('/api/v1/actions',             reaction.actions)
  ..get('/api/v1/permissions/snapshot', reaction.permissions)
  ..get('/api/v1/subscriptions',
        reaction.subscriptions(validator));

final pipeline = const Pipeline()
    .addMiddleware(existingAuthMiddleware) // consumer-supplied
    .addHandler(router.call);

await shelf_io.serve(pipeline, '0.0.0.0', 8080);
```

**Why `subscriptions` is a method (taking `validator`) and the other
three are getters?** The HTTP routes (`/me`, `/actions`,
`/permissions/snapshot`) read the active `Principal` from the request
context as populated by upstream shelf middleware — either the
consumer's existing auth middleware or the lib-supplied
`authMiddleware(validator)`. The WS upgrade path cannot follow that
pattern: Flutter web's `WebSocket` constructor cannot set custom
headers on the handshake, so the credential cannot ride in an
`Authorization` header and HTTP-bearer middleware mounted in front of
the WS route would reject every upgrade with 401. The credential
arrives in the first WS message after the upgrade completes; the
handler therefore needs the `PrincipalAuthValidator` directly, and
the cleanest place to inject it is the factory call site where the
handler is composed into the router.

`ReactionHandlers` does not call `shelf_io.serve`, does not own an
`HttpServer`, does not own an `EventStore` or `ActionDispatcher` or
`AuthorizationPolicy`. Those stay consumer-owned. It bundles the
four substrate handles so each handler factory closure doesn't have
to take them individually; beyond that, it imposes no structure on
the consumer's app shape.

The `viewPermissionNamer` parameter maps a `viewName` to the required
view-level permission name. Default:
`(viewName) => 'view:$viewName'`. Override per-deployment to relax
particular views (e.g., make `notes_today` require no view-level
permission — pure row-level scoping) or remap names.

### Canonical route table

The lib does not enforce paths — consumers mount the handlers
wherever they like. The canonical paths the design assumes (and the
Remote* impls' URL templates use) are:

```text
GET   /healthz              -> consumer-supplied; recommended
GET   /me                   -> JSON Principal (auth required)
POST  /actions              -> dispatch + JSON DispatchResult (auth)
GET   /permissions/snapshot -> JSON EffectiveAuthorization (auth)
GET   /subscriptions        -> WS upgrade (auth via first WS msg)
```

Consumers that mount under a path prefix (e.g.,
`/api/v1/...`) configure the `Remote*` impls' `baseUrl`
parameter accordingly. There is no `/healthz` handler in
`ReactionHandlers` — the consumer's existing healthz route serves
that purpose. The lib does not impose one.

### Auth: composing with consumer middleware

The reaction handlers read the active `Principal` from the shelf
request context via `principalFromContext(req)`. Two ways the
context gets populated:

1. **Consumer-supplied auth middleware already attaches `Principal`.**
   A consumer's existing auth middleware (for example one that verifies
   a Firebase ID token, looks up the user, and attaches a `Principal`
   to the request context) is read by reaction's handlers directly. No
   `reaction`-supplied middleware is needed.

2. **Mount `reaction`'s `authMiddleware(validator)` on routes the
   consumer is authenticating itself.** For deployments without an
   existing auth flow (dev, demo, embedded test), the lib's
   `authMiddleware(PrincipalAuthValidator)` reads
   `Authorization: Bearer <credential>`, calls
   `validator.authenticate`, and attaches the resulting `Principal`.

Either path produces the same result from the handlers' point of
view: `principalFromContext(req)` returns a non-null `Principal` on
authenticated requests. If neither has run, the handler returns 401.

```text
authMiddleware(PrincipalAuthValidator) shelf.Middleware:
  1. Read Authorization: Bearer <credential> header.
  2. If missing -> 401.
  3. Invoke validator.authenticate(credential).
     - Principal returned -> attach to context; proceed.
     - AuthenticationDenied -> 401 (opaque body).
     - Other exception -> 500 (log internally; opaque to wire).

principalFromContext(Request) -> Principal?:
  Reads the same key authMiddleware writes. Consumer-supplied
  middleware that wants to interoperate writes to the same key.
```

### WebSocket upgrade handler

The `/subscriptions` route accepts the HTTP upgrade then runs a
per-connection state machine. The WS handshake's first message
carries the auth credential (browsers cannot set custom headers on
the WebSocket upgrade, so first-message auth is the only uniform
shape across Flutter native and Flutter web). The handler depends
on the supplied `PrincipalAuthValidator` to interpret it.

```text
On upgrade:
  state = AWAITING_AUTH
  Spawn message loop.

On message in AWAITING_AUTH:
  if msg.type != 'auth' -> close 4001 auth_rejected
  Invoke validator.authenticate(msg.credential).
  On Principal:
    state = AUTHENTICATED
    principal = result
    Send {type: auth_ok, principalId: P.id}
    Register this WS in the server-wide connection registry
    under principal.userId (so the AuthorizationWatcher can find it).
  On AuthenticationDenied:
    close 4001 auth_rejected

On message in AUTHENTICATED:
  switch msg.type:
    'subscribe'   -> run per-sub authorization (see below);
                     each authorization decision is a fresh
                     call into policy.isPermitted /
                     effectivePermissionsFor against the substrate
                     (no per-Principal state cached on the
                     connection — see Decisions section)
    'unsubscribe' -> look up by id; cancel substrate sub; remove
                     from registry
    default       -> send {type: error, code: protocol_error, ...}

On disconnect:
  Cancel every substrate sub registered against this connection.
  Remove this WS from the server-wide connection registry.
  Release connection state.
```

The only per-connection state is `{principal, Map<subscriptionId,
StreamSubscription>}`. The server also maintains a single top-level
connection registry `Map<userId, Set<WebSocketChannel>>` so the
AuthorizationWatcher (described below) can route messages and close-frames
to the right connections. Neither is a permission-state mirror; see
"Why no per-Principal permission cache" in the Decisions section.

### AuthorizationWatcher: mid-session permission/scope changes

Once a WS subscription is open, its row-level narrowing is frozen at
the substrate-subscription level: the substrate's `subscribe<T>` was
constructed with a particular `aggregates` set, and changing the
filter on a running stream isn't supported. So changes to the
Principal's authorization state between subscribe-time and
disconnect need handling at the wire layer.

The reaction server runs **one** server-wide substrate subscription
— the AuthorizationWatcher — on the substrate's permission and
role-assignment event types:

```text
authzWatcher = eventStore.subscribe<Map<String, Object?>>(
  SubscriptionFilter(aggregateTypes: {
    'role_permission_grant',  // permission_granted, permission_revoked
    'user_role_scope',        // role_assigned, role_unassigned
  }),
  EventsMode(),
)

On each Update<T> from this watcher (a substrate event):
  Decode payload + eventType.
  Determine which (userId, action) tuples are affected:
    'role_unassigned'    payload['user_id']           -> hard logout
    'role_assigned'      payload['user_id']           -> stale_data
    'permission_revoked' for any user holding this role -> hard logout
    'permission_granted' for any user holding this role -> stale_data
  For each affected userId:
    Look up the registry's Set<WebSocketChannel> for that userId.
    For each channel:
      hard logout  -> sink.close(4003, 'permissions_changed')
                      (the WS disconnect handler does the rest)
      stale_data   -> send {type: stale_data, reason: <reason>}
```

The watcher is **a single subscription on the server-wide
substrate**, not per-connection. At the deployment scale this library
targets (tens of concurrent connections) this is one substrate
subscription serving the entire shelf process. No state mirror is
maintained.

Determining which users hold a given role (for `permission_*`
events) requires a one-time lookup at watcher-event time: read the
substrate's `role_permission_grants` projection to find the role's
current grants, then walk the connection registry's userIds and
check which Principal's `activeRole` matches. At the deployment scale
this library targets (tens of concurrent connections) the walk is
trivial.

### Force logout vs stale-data signal: when each applies

The asymmetry is principled. Different event categories warrant
different wire responses:

| Event                  | Effect on user's access     | Wire response       |
| ---------------------- | --------------------------- | ------------------- |
| `role_unassigned`      | narrows (security)          | close 4003          |
| `permission_revoked`   | narrows (security)          | close 4003          |
| `role_assigned`        | expands (UX-only)           | `stale_data` msg    |
| `permission_granted`   | expands (UX-only)           | `stale_data` msg    |
| containment change     | data-driven; either dir     | `stale_data` msg    |
| any other event        | not security-affecting      | (no signal)         |

The force-logout response is for **admin-driven security-narrowing**:
the user is currently authorized to see things they shouldn't, and
the admin's intent is to remove that access. Closing the WS clears
the client-side state immediately; the client reconnects, refetches
`/me`, and reopens subscriptions against the new (narrower)
authorization.

The stale-data response is for **UX-only freshness gaps**: the user
is currently authorized to see *less* than they could, or the change
is data-driven (a patient moved sites) rather than security-driven.
Either way, the client is showing a fresh-but-incomplete view, not
a stale-but-overprivileged one. The signal lets the client decide
whether to refresh (some UIs may not care; some may show a "data
updated, refresh?" affordance).

**Containment changes are NOT in the AuthorizationWatcher's filter.** The
watcher only sees role and permission events. Containment projection
changes (e.g., `patient_site_index` updates) trigger their own
signal path:

- Optional: deployments register containment projections with the
  reaction server (`AuthorizationWatcher.watchContainment(projectionName)`),
  and the server opens an additional substrate subscription per
  registered containment projection.
- When that subscription emits an Update<T>, every connected user
  whose role uses scope-classes that traverse this containment
  projection gets a `stale_data` message with
  `reason: containment_changed`.

This is per-deployment opt-in: if patient_site_index changes are
common (routine trial operations) and you don't want every change
spamming every connected coordinator with a stale_data message,
don't register it. If they're infrequent and you want the freshness
signal, do. Default: containment projections are NOT watched.

### Race window

There is a small staleness window between the substrate committing a
permission-changing event and the AuthorizationWatcher firing the close /
stale_data. Concretely:

```text
T+0:   Substrate commits role_unassigned event.
T+ε:   AuthorizationWatcher subscription receives the Update<T>.
T+ε+δ: Server closes affected WS connections with 4003.
[T, T+ε+δ]: revoked user can still receive events through their
           still-open substrate subscriptions.
```

The window is single-digit milliseconds in practice (substrate event
propagation latency). Far smaller than the original snapshot-at-
subscribe window (bounded by token TTL, ~minutes). Acceptable for
the deployment classes this lib targets.

### Per-subscription authorization (two-tier: deny then narrow)

Two-tier: view-level deny + row-level narrowing.

The mechanism consults the API shapes `spec/scoped-permissions.md`
defines: `AuthorizationPolicy.isPermitted(principal,
permission, scopeValue)`, `effectivePermissionsFor(principal) ->
EffectiveAuthorization`, sealed `ScopeValue` with `BoundScope` /
`ValueWildcardScope` / `TotalWildcardScope`, and the `ContainmentReference`
expansion against app-registered scope-class projections. The shipped
implementation is `reaction/lib/src/server/subscription_handler.dart`.

```text
On {type: subscribe, subscriptionId, viewName, filter, aggregates}:

  // Step 1: view-level deny
  // Map viewName -> required Permission (substrate-default
  // 'view:<viewName>'; deployment may override via viewPermissionNamer).
  required = viewPermissionNamer(viewName)
  if required == null:
    // Public view: skip view-level check, fall through to narrowing.
    goto Step 2
  // For view-level permissions, ScopeValue is TotalWildcardScope
  // (the subscribe action is "I want to see this view at all";
  // row-level scoping is the next step).
  decision = policy.isPermitted(
    principal,
    Permission(required, scopeClass: null),   // unscoped view-level
    null,
  )
  if decision is not Allow:
    send {type: subscription_denied,
          subscriptionId, reason: view_permission_denied}
    return

  // Step 2: row-level narrowing
  // The reaction server derives the SET of aggregate IDs the
  // Principal may see by expanding their scope assignments against
  // the view's associated scope-class projection. This requires:
  //   (a) viewName -> (scopeClass, aggregate-id-resolver) mapping
  //       (e.g., 'patient_files' is scoped to 'patient' class;
  //        aggregate-id IS the patient_id directly).
  //   (b) Reading effectivePermissionsFor(principal).scopeAssignments
  //       under the active role.
  //   (c) For each assignment, walking the containment graph
  //       (the substrate's ContainmentResolver) downward to expand
  //       'site A' (ValueAt class 'site') into the set of participant
  //       ids under site A.
  //   (d) Unioning the expansion across all assignments.
  //
  // The expansion is a snapshot at subscribe-time; reactive
  // re-narrowing on permission change is handled separately (the
  // force-logout / stale-data machinery in Section 4).

  viewScope = viewScopeRegistry.lookup(viewName)
  // viewScope: { scopeClass, aggregateIdFromScopeValue,
  //              scopeValuesContainingAggregateId }
  // null for views with no row-level scoping (admin views, etc.).

  if viewScope == null:
    allowedAggregates = null   // unrestricted at row-level
  else:
    eff = policy.effectivePermissionsFor(principal)
    allowedAggregates = expandAssignments(
      assignments: eff.scopeAssignments,
      targetClass: viewScope.scopeClass,
      containmentResolver: containmentResolver,
      activeRole: principal.activeRole,
    )
    // expandAssignments walks each ScopeAssignment via the
    // ContainmentResolver to produce the Set<aggregateId> the
    // Principal covers under their active role.

  effectiveAggregates = aggregates == null
      ? allowedAggregates
      : (allowedAggregates == null ? aggregates
                                   : aggregates ∩ allowedAggregates)

  // Step 3: open substrate sub
  sub = eventStore.subscribe<Map<String, Object?>>(
    filter,
    AggregateMode(viewName, identityMapper, effectiveAggregates),
  )
  // identityMapper: (row) => row

  // Step 4: relay
  sub.listen((Update<Map<String, Object?>> update) {
    enqueueWriteToSink(<wire envelope encoding update>)
  })
  register sub against subscriptionId
```

**New configuration surface introduced here: `viewScopeRegistry`.**
The reaction handlers need a `viewName -> ScopeClass` mapping for
the row-level narrowing step. This is composition-time data the
consumer supplies alongside `viewPermissionNamer`; the lib does NOT
bake in a default beyond "no view-scope, no narrowing." The shape
(`reaction/lib/src/server/view_scope_registry.dart`): a
`ViewScopeRegistry` whose `register` takes `viewName`, `scopeClass`,
and a `String? Function(ScopeValue) aggregateIdResolver`, and whose
`lookup(viewName)` returns the `ViewScopeBinding` (or null for an
unregistered, row-unscoped view):

```text
ReactionHandlers(
  ...,
  viewScopeRegistry: ViewScopeRegistry()
    ..register(
      viewName: 'participant_files',
      scopeClass: 'participant',
      aggregateIdResolver: (scopeValue) => scopeValue.value,
      // participant id IS the aggregate id directly
    )
    ..register(
      viewName: 'site_summaries',
      scopeClass: 'site',
      aggregateIdResolver: (scopeValue) => scopeValue.value,
    ),
)
```

When `aggregateIdResolver` is a 1:1 mapping (participant scope value =
participant aggregate id), expansion is `O(assignments)`. When the
mapping requires containment walks (e.g., view is keyed by
participant id but the assignment is `(site, A)`), expansion goes
through `ContainmentResolver` walks per assignment. This is the same
mechanism the substrate uses for action-side authorize evaluation —
reused on the read path.

**Defensible "no-scope" behavior**: if `effectiveAggregates` resolves
to an empty set (Principal has zero row-level scope on this view but
the view-level perm passed), the subscription is opened anyway. The
substrate will emit `EndOfReplay` with no preceding `Snapshot` rows.
Distinguishes "view exists, you can subscribe, but you have nothing
in scope" from view-level denial.

### Per-connection write serialization

Multiple substrate-subscriptions share one WS connection. The server
must serialize writes within a connection so per-subscription
ordering on the wire matches per-subscription ordering from the
substrate.

Implementation: each connection holds a single async write queue
(`Future<void>` chain or equivalent `synchronized`-style primitive).
Each substrate `Update<T>` arrival enqueues an envelope; writes
drain in order. No interleaving of bytes onto the WS sink.

This is a load-bearing invariant — the wire would otherwise violate
`EVS-PRD-cross-process-event-transport/C` (snapshot-then-deltas
atomicity end-to-end).

### Disposal

`ReactionHandlers` owns one lifecycle-bound resource: the server-wide
`AuthorizationWatcher` subscription started in its constructor. Consumers
call `await reaction.dispose()` on graceful shutdown to cancel that
subscription. Per-connection cleanup (cancelling each connection's
substrate subscriptions, unregistering from the connection registry)
happens automatically when each WS channel's stream closes; the
`subscription_handler` registers an `onDone` listener for exactly
that purpose.

Graceful shutdown sequence:

1. Consumer stops `shelf_io`'s `HttpServer` from accepting new
   connections.
2. Open WS connections close (consumer may force-close with 4002
   server_shutting_down).
3. Each connection's `onDone` fires; substrate subs cancel;
   connection unregisters from `connectionRegistry`.
4. Consumer calls `await reaction.dispose()`; the AuthorizationWatcher's
   substrate subscription cancels.
5. Substrate cleanup (`eventStore.close()` etc.) remains the
   consumer's responsibility — same as in the integrated case.

### What `ReactionHandlers` does NOT do

- Does not open or close the `EventStore` (consumer-owned).
- Does not construct the `ActionDispatcher` (consumer calls
  `dispatcher.dispatch`; the handlers just route to it).
- Does not own an `HttpServer` or call `shelf_io.serve` —
  the consumer composes their own shelf pipeline.
- Does not impose paths — the canonical paths the design uses are
  recommendations; the consumer mounts handlers wherever they like
  and configures the `Remote*` impls' `baseUrl` accordingly.
- Does not mandate a `PrincipalAuthValidator` — for deployments
  whose existing auth middleware already populates `Principal` on
  the request context (e.g., a consumer's own Firebase middleware),
  `authMiddleware(validator)` is not mounted at all and no validator
  is required.

## Section 5 — Testing strategy

### Test pyramid

```text
+-------------------------------------+
|  E2E roundtrip tests        ~25     |
|  (full RemoteScope <-> handlers     |
|   mounted on shelf, in-memory       |
|   substrate)                        |
+-------------------------------------+
|  Server-side unit tests     ~30     |
|  (individual handler factories      |
|   exercised against stub dispatcher |
|   / stub policy, no shelf required) |
+-------------------------------------+
|  Codec round-trip tests     ~40     |
|  (pure; one per envelope type +     |
|   edge cases: null, unicode, large) |
+-------------------------------------+
```

### `ReactionRemoteTestHarness`

Sibling to the existing `ReactionTestHarness`. Composes:

1. In-memory sembast `EventStore` with the test entry types (`note`,
   `greeting`, `role_permission_grant`, `action_denial`),
   `notes_today` projection, `SayHelloAction`. Reuses
   `ReactionTestHarness` fixture data.
2. `ActionDispatcher`.
3. `ReactionHandlers` over the substrate handles.
4. A `Router` composing `authMiddleware(TrustingAuthValidator(...))`
   over the four reaction handlers — the same shape a consumer would
   use in production but with the dev/test validator instead of
   their production one.
5. `shelf_io` bound to ephemeral port (port=0); records the port.
6. `RemoteScope` against `http://localhost:<port>`.

Tests interact with `harness.scope.authSession` etc. exactly as
production code would. The shelf server is real (not mocked); the
wire is real JSON over real HTTP and real WS.

### Assertion coverage

Each PRD assertion gets at least one
`// Verifies: EVS-PRD-<name>/<letter>` test.

**`EVS-PRD-cross-process-event-transport`**:

- `/A` round-trip codec per envelope type — assert all fields
  preserved.
- `/B` every envelope's `sequence` + `subscriptionId` populated;
  matches substrate-side values.
- `/C` ordering: append events alternately for two aggregates;
  assert wire ordering matches a co-located `LocalViewSource` on the
  same substrate.
- `/D` multiplex: 3 subs on one WS; assert independent
  `Snapshot×N → EndOfReplay → Delta…` flows interleave correctly.
- `/E` per-sub authz: see authz tests below.
- `/F` HTTP POST `/actions` carries bearer header; rejects missing
  or bad credential with 401.
- `/G` Layer 2 invariance: `LocalViewSource` and `RemoteViewSource`
  watching the same view emit identical `Update<T>` sequences
  modulo `subscriptionId`.

**`EVS-PRD-auth-session`** (Remote impl portion):

- `/E` 401 from any HTTP call flips status to `Expired`, emits on
  stream.
- WS close-frame `4001 auth_rejected` flips status to `Expired`.

**`EVS-PRD-action-submitter`**:

- `/C` POST round-trip with each `DispatchResult` variant
  (Success / Denied / Failed / etc.) decodes correctly.
- `/D` bearer header on every POST.
- `/E` source-identical: same `switch (state)` widget code works
  against both `LocalActionSubmitter` and `RemoteActionSubmitter`
  with identical behavior per variant.

**`EVS-PRD-view-subscriber`**:

- `/C` consumer's mapper applies client-side; server ships only
  `Map<String, Object?>`.
- `/D` source-identical: same `watch<T>` call pattern produces
  equivalent `Stream<Update<T>>` from Local vs Remote.

**`EVS-PRD-permission-source`**:

- `/C` initial HTTP GET seeds; WS subscription reflects subsequent
  changes.
- `/E` `setCredential(<different cred>)` re-emits the new
  Principal's snapshot.

### Authorization-mechanism tests (Approach B)

- **View-level deny:** seed substrate so test Principal lacks
  `view:audit_log`; subscribe to `audit_log`; assert
  `subscription_denied` with `reason: view_permission_denied`; no
  rows arrive.
- **Row-level narrow:** seed Principal has scope on `[a1, a2]` only;
  subscribe with `aggregates: [a1, a2, a3]`; assert only `a1`, `a2`
  rows arrive.
- **Full scope:** Principal has unrestricted scope; subscribe with
  no aggregates filter; all rows arrive.
- **No scope (empty narrow):** Principal has zero aggregates on
  view; subscribe; assert `EndOfReplay` with zero `Snapshot` rows
  preceding.

### Reconnect tests

- Open subscription, receive `EndOfReplay`, then forcibly close the
  WS server-side with code `1006`. Assert client reconnects with
  backoff, re-auths, re-subscribes, receives fresh
  `Snapshot×N → EndOfReplay` again. No lost subscriptions; no
  duplicate `subscriptionId` collisions.
- Open subscription, get `4001 auth_rejected`. Assert client does
  NOT reconnect; `AuthSession.current` flips to `Expired`.

### Edge cases

- Malformed JSON inbound → 400 / `protocol_error`; does NOT crash
  the connection or the server.
- WS sink concurrency: open 5 subscriptions; pump 1000 events;
  assert per-subscription wire ordering preserved (sequence numbers
  as witness).
- `dispose()` mid-flight: open subs, fire a POST, then `dispose()`.
  Assert no hangs, no resource leaks.
- HTTP timeout: client respects a configurable timeout; surfaces
  `TransportException`.

### Mid-session permission-change tests

The AuthorizationWatcher's behavior gets dedicated coverage:

- **Force-logout on `role_unassigned`:** open a subscription, append
  a `role_unassigned` event for the connected user, assert the WS
  closes with `4003 permissions_changed` within 200 ms.
- **Force-logout on `permission_revoked`:** seed a user with role X,
  open subscriptions, append a `permission_revoked` event for a
  permission held by role X, assert all connections for users with
  role X close with `4003`.
- **Stale-data signal on `role_assigned`:** open subscriptions,
  append a `role_assigned` event for the connected user, assert
  the client receives a `stale_data` envelope with
  `reason: role_assigned` (subscription stays open).
- **Stale-data signal on `permission_granted` to held role:** same
  shape; `reason: permission_added`.
- **Containment change does NOT emit stale_data by default:** seed
  the harness without watching `patient_site_index`; append a
  re-parenting event; assert no `stale_data` is emitted to
  connected users.
- **Containment-watch opt-in emits stale_data:** as above but with
  `watchContainment('patient_site_index')`; assert `stale_data`
  with `reason: containment_changed`.

### What's NOT tested in this plan

- Production validators (Firebase JWT, Auth0, linking-code) — tested
  in app code per the trust-boundary discipline.
- Batched / cursor snapshot delivery — deferred; see
  `spec/roadmap/reaction.md`.

## Section 6 — Normative requirements

The remainder of this file is design prose; the following requirement
blocks are the normative obligations binding the implementation.
Additional `EVS-DEV-*` blocks MAY be added here if further surfaces need
normative pinning.

## EVS-DEV-authz-watcher: Mid-session permission-change signalling

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-auth-session, EVS-PRD-cross-process-event-transport

### Assertions

A. On a `role_unassigned` event for a `userId`, the `AuthorizationWatcher` SHALL close every WS connection registered for that `userId` in the `WsConnectionRegistry` with close code `4003` and reason `permissions_changed` (force-logout on security narrowing).

B. On a `permission_revoked` event for a role `R`, the `AuthorizationWatcher` SHALL force-close (close code `4003`, reason `permissions_changed`) every WS connection whose registered Principal's `activeRole` equals `R` (force-logout on security narrowing).

C. On `role_assigned` for a `userId` or `permission_granted` for a role `R` held by a connected user, the `AuthorizationWatcher` SHALL send a `stale_data` envelope on each affected WS connection — `reason: role_assigned` for `role_assigned`, `reason: permission_added` for `permission_granted` — without closing the connection (UX-only freshness signal on security expansion).

D. Containment-projection changes SHALL emit no signal by default; consumers opt in per containment projection via `ReactionHandlers.watchContainment(aggregateType)`, after which a `Delta` on that projection causes the `AuthorizationWatcher` to send a `stale_data` envelope with `reason: containment_changed` to every currently-connected user.

E. The `AuthorizationWatcher` SHALL maintain exactly one substrate subscription for the core permission/role-assignment event types (`role_permission_grant`, `user_role_scope`) — server-wide, not per-connection — plus one additional subscription per opted-in containment projection. Per-connection state SHALL live in the separate `WsConnectionRegistry` so the watcher remains O(1) in connection count for its substrate subscriptions.

### Rationale

The asymmetry between force-logout (security narrowing) and `stale_data` (security expansion / data-driven change) captures admin intent: revocation is a deliberate "remove this access" action and merits interrupting the user, whereas grants and containment movements are routine and the client may not even care. The single server-wide subscription keeps the substrate's reactive cost flat regardless of connected user count, and the `WsConnectionRegistry` lookup is the only per-user state the watcher reads — no per-Principal permission mirror is maintained (rationale lives in "Why no per-Principal permission cache" in the Decisions section).

### Changelog

- 2026-05-29 | cc1908b5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-05-24 | add96480 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-05-24 | - | - | Michael Lewis (<michael.lewis.c@gmail.com>) | Initial authoring; locks in shipped AuthorizationWatcher behavior

*End* *Mid-session permission-change signalling* | **Hash**: cc1908b5

## Trust boundary expansion

The reaction server (when deployed) introduces a new enumerated
trust input that CLAUDE.md's "Trust boundaries" section should
record alongside `StorageBackend`, `Destination`, and the
Principal-on-faith gap:

- **`PrincipalAuthValidator` (or equivalent consumer middleware)
  that populates `Principal` on the request context.** Whatever
  authentication path is composed into the consumer's shelf
  pipeline — `authMiddleware(validator)` from this lib, or the
  consumer's own Firebase / OAuth / linking-code middleware — is
  trusted to map a wire credential to a `Principal` correctly and
  to refuse invalid credentials. The reaction lib ships only
  `TrustingAuthValidator` (dev/test); production deployments
  supply their own validator or their own middleware that closes
  the Principal-on-faith gap for that deployment.

From the substrate's POV (the four `lib/` packages in this repo),
nothing changes — the substrate still sees `Principal` values it
trusts. The consumer's auth middleware (whether from this lib or
not) is what closes the gap *externally*, inside a single
deployment. A different deployment with different middleware gets
different trust semantics.

CLAUDE.md's "Trust boundaries" section enumerates this input.

## Decisions and alternatives rejected

Recorded here so future authors do not re-litigate them without new
evidence.

**Why cover cross-process client and server impl in one design?**
The wire protocol cannot be specified, codec-tested, or end-to-end-
validated without both sides present. The PRD architecture already puts
client + server + wire in one package, and a single design document
captures the cohesive cross-cutting concerns naturally.

**Why refetch reconnect instead of resume-from-sequence?**
Resume-from-sequence is a wire-compatible optimization; the sequence
field is already on every envelope. The snapshot-replay flash is
acceptable at the deployment scale this library targets (tens of
concurrent connections); the extra moving parts (client sequence
tracking, server `read(from:)` semantics) can wait until measurement
demands them. Recorded in `spec/roadmap/reaction.md`.

**Why no `JwtAuthValidator` reference impl in this plan?** Per
`EVS-PRD-auth-session/F`'s rationale: production validators encode
deployment-specific identity-provider choices (Firebase
issuer/audience/JWKS URL; Auth0 issuer/audience; linking-code-to-UUID
binding logic; etc.). A concrete lib-side validator commits to one
shape and forces it on every consumer. The pluggable seam
(`PrincipalAuthValidator`) is sufficient.

**Why Approach B for per-subscription authorization (two-tier:
deny then narrow) instead of A (narrow only)?** B gives a clean
client error path (`subscription_denied` envelope), distinguishes
"no data" from "no permission" (which Approach A conflates), and
maps cleanly onto the substrate's two-layer scope model (role-to-permission
grants drive view-level deny; user-role-scope assignments expanded
through containment projections drive row-level narrowing). C
(post-filter) was rejected outright — leaks information via
timing/count side-channels and violates substrate-shaped filter
composition.

**Why no `ReactionServer` class? Why ship handlers + middleware
directly?** Consumer server deployments typically already run their own
`shelf` + `shelf_router` pipelines with deployment-specific
middleware (authentication, telemetry, CORS, logging). Forcing
them to mount a `ReactionServer.handler` would either (a) put the
reaction routes in an awkward nested subtree separate from their
other routes, or (b) reduce `ReactionServer` to a property bag for
extracting individual handlers — which is what `ReactionHandlers`
just is, more honestly. The collapse-not-add composition story
(a consumer's existing bespoke REST handlers shrink to a handful of
reaction handlers plus a few stragglers) makes the "framework
wrapper" approach actively unhelpful: the consumer wants the
reaction handlers *interleaved* with their own remaining routes,
not segregated under a different mount point. Every route is a custom
route from the consumer's perspective; the lib supplies the four that
implement the wire and the consumer mounts them wherever they like.

**Why no per-Principal permission cache on the WS connection?**
A server-side cache mirroring the substrate's permission projection
(one `EffectiveAuthorization` subscription per authenticated connection)
was considered and rejected for two reasons: (a) `policy.isPermitted`
and `policy.effectivePermissionsFor` against `findViewRowsInTxn` are
sub-millisecond for the row counts in question, and `subscribe` messages
are not high-frequency (an interactive UI user opens a handful of panels
per session, not thousands), so the optimization has no measurable benefit;
(b) the cache would introduce per-connection state (cache mirror +
substrate subscription managing it) that has to be cleaned up on
disconnect and managed against race conditions when grants change
mid-decision. Each `subscribe` message calls into
`policy.isPermitted` / `effectivePermissionsFor` fresh; the only
per-connection state is the active `Principal` and the
`Map<subscriptionId, StreamSubscription>` registry. Mid-session
permission changes are handled by the AuthorizationWatcher (see below),
not by mirroring permission state per-connection.

**Why force-logout on revocation but stale-data signal on grant
expansion / containment change?** Three design alternatives were
considered for handling mid-session permission changes:

1. *Snapshot at subscribe-time (silent fail-open)*: do nothing;
   accept that revoked users continue receiving events until they
   reconnect (token expiry, network drop, manual logout). Bounds
   the staleness window only by token TTL — minutes in practice.
   Rejected for v1: too long a window for security-narrowing
   admin actions.
2. *Reactive re-narrowing*: on permission change, mutate the open
   subscription's `aggregates` filter and emit `tombstone` /
   `snapshot` envelopes to clean up the client-side view. Cleanest
   semantics but introduces per-subscription state-machine
   complexity (which aggregates are now-out-of-scope; which are
   newly-in-scope; how to handle a tombstone that's just a
   narrowing vs a real delete). Substantial impl surface.
3. *Force logout + stale-data signal* (chosen): on security-
   narrowing events (revocation), close the WS with `4003
   permissions_changed`; the client reconnects, re-fetches `/me`,
   and re-opens subscriptions against the new, narrower
   authorization. On UX-only events (grant expansion, containment
   change), send a `stale_data` message; the client decides
   whether to refresh. The wire stays simple: existing
   close-frames + one new envelope type. The server's state
   addition is a single connection registry
   (`Map<userId, Set<WebSocketChannel>>`) plus one server-wide
   AuthorizationWatcher subscription. No per-connection permission
   mirror; no re-narrowing logic.

The chosen approach captures the asymmetric intent: admin-driven
security-narrowing is the only thing serious enough to interrupt a
user mid-session; UX-only updates can be deferred to the client's
discretion. The "intent vs incidental" distinction is principled:
when an admin revokes a role, they intend to reduce that user's
access; when an admin re-parents a patient, they intend to move the
patient, and any access change is incidental.

Containment changes default to NOT being watched by the AuthorizationWatcher
because they're frequent (routine trial operations) and watching
them would emit `stale_data` to every connected coordinator on every
patient move — operationally noisy. Deployments opt in via
`AuthorizationWatcher.watchContainment(projectionName)` if they want the
freshness signal.

**Why first-message WS auth instead of upgrade-header auth?**
Flutter web's `WebSocket` constructor cannot set custom headers on
the handshake. First-message auth is the only design uniform across
Flutter native and Flutter web. Standard pattern (Phoenix LiveView,
Apollo, etc.). TLS protects the credential in transit
(`wss://` assumed in production).

**Why hand-written codecs instead of `json_serializable` code-gen?**
Consistent with the substrate's existing style (zero code-gen
anywhere in `event_sourcing/`). Explicit, debuggable, no build-time
dependency. Wire JSON shape stays under direct authorial control
(important for append-only-primitive discipline on envelope shapes).

**Why no "validating" intermediate `AuthStatus`?** PRD-B locks
`AuthStatus` as exactly three variants. During an async
`setCredential` GET /me round-trip, the previous status holds; on
completion, transition is atomic. A fourth state would be redundant
(callers can observe the in-flight `Future` if they care).

**Why a 30-second WS idle grace before closing?** Avoids thrash on
tab toggles, drawer-open-drawer-close, etc. Empirical sweet spot
from similar systems; cheap to keep idle WS open for 30s.

## Future work

Deferred work for this area is recorded in `spec/roadmap/reaction.md`.

## Reading order recommendation

For first contact:

1. `spec/prd-reaction.md` Reading Order section (PRDs 1-5) — pins
   the obligations this design satisfies.
2. `spec/scoped-permissions.md` — the substrate
   permission shapes this design's server-side authz consults.
3. This document, Sections 1 → 5 (module → wire → client → server →
   test).
4. The Trust Boundary Expansion section — understand the new trust
   input the deployment is committing to.
5. The Decisions section — for context on why specific choices were
   made.

## References

- `spec/prd-reaction.md` — the 6 PRDs this design elaborates.
- `spec/scoped-permissions.md` — the substrate permission primitives
  the reference server's per-subscription authz consults.
- `spec/prd-library-charter.md` — epistemic-layer framing and AOP
  discipline.
