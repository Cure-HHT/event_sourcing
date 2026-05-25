# Reaction Remote Impls, Reference Server, and Wire Protocol

**Phase**: I (post Plan B-local; impl gated on CUR-1331 impl)
**Status**: Design draft (no normative requirement blocks yet;
`EVS-DEV-*` requirements land in-place against this file as impl
stabilizes)
**Linear**: CUR-1317 (libify); impl ticket TBD
**Elaborates**: `spec/prd-reaction.md` PRDs `EVS-PRD-auth-session`,
`EVS-PRD-action-submitter`, `EVS-PRD-view-subscriber`,
`EVS-PRD-permission-source`, and
`EVS-PRD-cross-process-event-transport`.
**Depends on**: `spec/scoped-permissions.md` (CUR-1331) for the
`AuthorizationPolicy`, `EffectiveAuthorization`, and `ScopeValue`
shapes that the reference server's per-subscription authorization
consults. Plan B-remote+C impl is sequenced to land after CUR-1331
impl so the server's permission-consulting code targets the final
API shape directly rather than being written against the pre-1331
surface and then swept.

> **Lifecycle note.** This file is authored as a design document and
> will grow normative `EVS-{TYPE}-{component}` requirement blocks in
> place as the design stabilizes through implementation. The
> brainstorm → stabilize → migrate lifecycle in `spec/README.md` is
> short-circuited here: the design lives in `spec/` from the start to
> preserve fidelity (avoid information loss in a migration step), and
> the same file is edited to add assertions later. elspais treats
> this file as non-normative prose until a requirement block heading
> is added.

**Note on roadmap supersession.** This design merges the roadmap's
split of Plan B-remote (client only) and Plan C (server only) into a
single implementation. The wire protocol cannot be specified, codec-
tested, or end-to-end-validated without both sides; the PRD
architecture (`spec/prd-reaction.md` lines 26-55) already puts
`local/`, `remote/`, `server/`, and `wire/` in one package. The
roadmap document (`docs/superpowers/specs/2026-05-11-roadmap.md`)
gets an update reflecting the merge alongside this design's commit.

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
- v1.1 `resume-from-sequence` reconnect optimization — wire format
  reserves space; not implemented here.
- Reactive re-narrowing of active subscriptions' `aggregates`
  filters mid-stream (mutating an open `subscribe<T>` to narrow it).
  The chosen mid-session permission-change handling (force-logout
  on revocation + stale-data signal on expansion / containment
  change) avoids this entirely; see Section 4.
- Snapshot pagination for very large views (PRD Open Question 1).
- Connection-state observability stream on the client
  (`Stream<ConnectionState>` for "Reconnecting…" UX) — defer to v1.1.

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
                                       (CUR-1331 substrate type)
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

There is intentionally no `ReactionServer` class. Existing consumers
(`hht_diary`'s `portal_server`, `diary_server`) already run their own
`shelf` + `shelf_router` pipelines with deployment-specific
middleware (Firebase auth, OpenTelemetry, CORS, request logging);
they compose `reaction`'s handlers into their existing routers
directly. The "server" is whatever consumer-owned shelf app the
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
- `AuthzWatcher` stays package-private; consumers interact with it
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
  String. **This is lossy** — see Future work for the structured-error
  path.
- `authorization_denied` carries only the substrate's `Permission`
  (inlined `{name, scopeClass}`). The substrate's
  `DispatchAuthorizationDenied` carries no `DenyReason` / `scope` /
  `detail` — those fields belonged to an earlier, richer variant that
  was simplified.
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

The wire reserves space for a future `{"type":"resume","fromSequence":N}`
message; not implemented in v1 but the codec layout accommodates it
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

### Reconnect strategy (v1 baseline: refetch)

Client reconnects with exponential backoff (250ms initial, 30s cap,
doubling). On reconnect:

1. Send `{"type":"auth", "credential": <stored>}`.
2. For each active subscription, re-send the original
   `{"type":"subscribe", "subscriptionId": <same as before>, ...}`.
3. Server treats reconnect as fresh: opens new substrate
   `subscribe<T>` per subscription. Snapshot replay re-fires from
   the substrate; client receives `Snapshot×N → EndOfReplay → Delta…`
   again.
4. Client-side `Stream<Update<T>>` (the one returned to widget code)
   sees the snapshot-replay flash.

PRD Open Question 3 explicitly frames this as the v1 baseline.
v1.1's `resume-from-sequence` optimization is wire-compatible: it
would add a `{"type":"resume","subscriptionId":N,"fromSequence":N}`
message; the existing sequence field already on every envelope is
sufficient to drive it. Non-breaking add.

## Section 3 — Client lifecycle

### `RemoteScope` (composition class)

The production-side analog to the existing `ReactionTestHarness` (the
in-process composition bundle for Local*). Constructed once per
user-session at app boot; returns the four interface instances plus
a `dispose()` for graceful teardown.

```dart
final scope = RemoteScope(
  baseUrl: Uri.parse('https://portal.cure-hht.example'),
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

await scope.dispose();  // on app shutdown
```

`RemoteScope` derives the WS URL from `baseUrl`:

```text
http://...  -> ws://...
https://... -> wss://...
```

with `/subscriptions` appended.

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
  that path flips `RemoteAuthSession` to `Expired` instead).
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
  ..get('/api/v1/sponsor/config', sponsorConfigHandler)
  ..post('/api/v1/auth/login',    loginHandler)         // pre-auth
  // Reaction handlers, mounted wherever the consumer chooses:
  ..get('/api/v1/portal/me',                   reaction.me)
  ..post('/api/v1/portal/actions',             reaction.actions)
  ..get('/api/v1/portal/permissions/snapshot', reaction.permissions)
  ..get('/api/v1/portal/subscriptions',
        reaction.subscriptions(validator));

final pipeline = const Pipeline()
    .addMiddleware(existingFirebaseAuthMiddleware) // consumer-supplied
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
`/api/v1/portal/...`) configure the `Remote*` impls' `baseUrl`
parameter accordingly. There is no `/healthz` handler in
`ReactionHandlers` — the consumer's existing healthz route serves
that purpose. The lib does not impose one.

### Auth: composing with consumer middleware

The reaction handlers read the active `Principal` from the shelf
request context via `principalFromContext(req)`. Two ways the
context gets populated:

1. **Consumer-supplied auth middleware already attaches `Principal`.**
   Portal_server's existing Firebase middleware extracts and verifies
   the Firebase ID token, looks up the user, and attaches a
   `Principal` to the request context. Reaction's handlers read that
   `Principal` directly. No `reaction`-supplied middleware is needed.

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
    under principal.userId (so the AuthzWatcher can find it).
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
AuthzWatcher (described below) can route messages and close-frames
to the right connections. Neither is a permission-state mirror; see
"Why no per-Principal permission cache" in the Decisions section.

### AuthzWatcher: mid-session permission/scope changes

Once a WS subscription is open, its row-level narrowing is frozen at
the substrate-subscription level: the substrate's `subscribe<T>` was
constructed with a particular `aggregates` set, and changing the
filter on a running stream isn't supported. So changes to the
Principal's authorization state between subscribe-time and
disconnect need handling at the wire layer.

The reaction server runs **one** server-wide substrate subscription
— the AuthzWatcher — on the substrate's permission and
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
substrate**, not per-connection. At portal scale this is one
substrate subscription serving the entire shelf process. No state
mirror is maintained.

Determining which users hold a given role (for `permission_*`
events) requires a one-time lookup at watcher-event time: read the
substrate's `role_permission_grants` projection to find the role's
current grants, then walk the connection registry's userIds and
check which Principal's `activeRole` matches. At portal scale (~20
users) the walk is trivial.

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

**Containment changes are NOT in the AuthzWatcher's filter.** The
watcher only sees role and permission events. Containment projection
changes (e.g., `patient_site_index` updates) trigger their own
signal path:

- Optional: deployments register containment projections with the
  reaction server (`AuthzWatcher.watchContainment(projectionName)`),
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
permission-changing event and the AuthzWatcher firing the close /
stale_data. Concretely:

```text
T+0:   Substrate commits role_unassigned event.
T+ε:   AuthzWatcher subscription receives the Update<T>.
T+ε+δ: Server closes affected WS connections with 4003.
[T, T+ε+δ]: revoked user can still receive events through their
           still-open substrate subscriptions.
```

The window is single-digit milliseconds in practice (substrate event
propagation latency). Far smaller than the original snapshot-at-
subscribe window (bounded by token TTL, ~minutes). Acceptable for
the deployment classes this lib targets.

### Per-subscription authorization (Approach B, aligned to CUR-1331)

Two-tier: view-level deny + row-level narrowing.

The mechanism consults the API shapes `spec/scoped-permissions.md`
(CUR-1331) defines: `AuthorizationPolicy.isPermitted(principal,
permission, scopeValue)`, `effectivePermissionsFor(principal) ->
EffectiveAuthorization`, sealed `ScopeValue` with `BoundScope` /
`ValueWildcardScope` / `TotalWildcardScope`, and the `ContainmentRef`
expansion against app-registered scope-class projections. This
section is the design surface where Plan B-remote+C plugs into
CUR-1331's primitives; the impl ticket runs after CUR-1331 impl
lands so this code is written against the final API.

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
  //       (ContainmentResolver from CUR-1331) downward to expand
  //       'site A' (ValueAt class 'site') into the set of patient_ids
  //       under site A.
  //   (d) Unioning the expansion across all assignments.
  //
  // Implementations should treat this expansion as a snapshot at
  // subscribe-time (consistent with the v1 baseline; reactive
  // re-narrowing on permission change is a separate concern;
  // see Open Question 1).

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
bake in a default beyond "no view-scope, no narrowing." Concrete
shape lands in the impl ticket; provisional sketch:

```text
ReactionHandlers(
  ...,
  viewScopeRegistry: ViewScopeRegistry()
    ..register(
      viewName: 'patient_files',
      scopeClass: 'patient',
      aggregateIdResolver: (scopeValue) => scopeValue.value,
      // patient_id IS the aggregate id directly
    )
    ..register(
      viewName: 'site_summaries',
      scopeClass: 'site',
      aggregateIdResolver: (scopeValue) => scopeValue.value,
    ),
)
```

When `aggregateIdResolver` is a 1:1 mapping (patient scope value =
patient aggregate id), expansion is `O(assignments)`. When the
mapping requires containment walks (e.g., view is keyed by
`patient_id` but the assignment is `(site, A)`), expansion goes
through `ContainmentResolver` walks per assignment. This is the same
mechanism CUR-1331 uses for action-side authorize evaluation —
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
`AuthzWatcher` subscription started in its constructor. Consumers
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
4. Consumer calls `await reaction.dispose()`; the AuthzWatcher's
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
  the request context (e.g., portal_server's Firebase middleware),
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

The AuthzWatcher's behavior gets dedicated coverage:

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
- Snapshot pagination — PRD Open Question 1 defer.

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

Adding this to CLAUDE.md is a follow-up action; the trust-boundary
enumeration is the right place.

## Decisions and alternatives rejected

Recorded here so future authors do not re-litigate them without new
evidence.

**Why merge Plan B-remote and Plan C into one plan?** The wire
protocol cannot be specified, codec-tested, or end-to-end-validated
without both sides present. PRD architecture already puts client +
server + wire in one package. Splitting created throwaway fake-server
scaffolding that the actual server would have replaced. One plan
captures the cohesive design.

**Why refetch reconnect instead of resume-from-sequence?** v1
baseline matches PRD Open Q3. Resume-from-sequence is a wire-
compatible v1.1 optimization; the sequence field is already on every
envelope. UX flash is acceptable at portal scale (~1-20 users); the
extra moving parts (client sequence tracking, server `read(from:)`
semantics) can wait until measurement demands them.

**Why no `JwtAuthValidator` reference impl in this plan?** Per the
saved auth-validators-are-consumer-supplied memory and PRD-auth-
session/F's rationale: production validators encode deployment-
specific identity-provider choices (Firebase issuer/audience/JWKS
URL; Auth0 issuer/audience; linking-code-to-UUID binding logic;
etc.). A concrete lib-side validator commits to one shape and forces
it on every consumer. The pluggable seam (`PrincipalAuthValidator`)
is sufficient.

**Why Approach B for per-subscription authorization (two-tier:
deny then narrow) instead of A (narrow only)?** B gives a clean
client error path (`subscription_denied` envelope), distinguishes
"no data" from "no permission" (which Approach A conflates), and
maps cleanly onto CUR-1331's two-layer model (role-to-permission
grants drive view-level deny; user-role-scope assignments expanded
through containment projections drive row-level narrowing). C
(post-filter) was rejected outright — leaks information via
timing/count side-channels and violates substrate-shaped filter
composition.

**Why no `ReactionServer` class? Why ship handlers + middleware
directly?** The expected first consumers — `portal_server` and
`diary_server` in `hht_diary` — already run their own
`shelf` + `shelf_router` pipelines with deployment-specific
middleware (Firebase auth, OpenTelemetry, CORS, logging). Forcing
them to mount a `ReactionServer.handler` would either (a) put the
reaction routes in an awkward nested subtree separate from their
other routes, or (b) reduce `ReactionServer` to a property bag for
extracting individual handlers — which is what `ReactionHandlers`
just is, more honestly. The collapse-not-add migration story
(the consumers' existing 30+ bespoke REST handlers shrink to ~5
reaction handlers plus a few stragglers) makes the "framework
wrapper" approach actively unhelpful: the consumer wants the
reaction handlers *interleaved* with their own remaining routes,
not segregated under a different mount point. Resolves what was
previously called PRD Open Q4 (custom-route registration) by
making the question moot — every route is a custom route from
the consumer's perspective; the lib just supplies the four that
implement the wire.

**Why no per-Principal permission cache on the WS connection?**
An earlier draft of this design opened a per-Principal
`EffectiveAuthorization` subscription on `auth_ok` to keep a fresh
in-memory copy of the Principal's permissions for fast lookup on
each `subscribe` message — effectively a server-side cache mirroring
the substrate's permission projection. Rejected for v1: (a)
`policy.isPermitted` and `policy.effectivePermissionsFor` against
`findViewRowsInTxn` are sub-millisecond for the row counts in
question, and `subscribe` messages are not high-frequency (a portal
user opens a handful of panels per session, not thousands), so the
optimization has no measurable benefit; (b) the cache would
introduce per-connection state (cache mirror + substrate
subscription managing it) that has to be cleaned up on disconnect
and managed against race conditions when grants change
mid-decision. Each `subscribe` message thus calls into
`policy.isPermitted` / `effectivePermissionsFor` fresh; the only
per-connection state is the active `Principal` and the
`Map<subscriptionId, StreamSubscription>` registry. Mid-session
permission changes are handled by the AuthzWatcher (see below),
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
   AuthzWatcher subscription. No per-connection permission
   mirror; no re-narrowing logic.

The chosen approach captures the asymmetric intent: admin-driven
security-narrowing is the only thing serious enough to interrupt a
user mid-session; UX-only updates can be deferred to the client's
discretion. The "intent vs incidental" distinction is principled:
when an admin revokes a role, they intend to reduce that user's
access; when an admin re-parents a patient, they intend to move the
patient, and any access change is incidental.

Containment changes default to NOT being watched by the AuthzWatcher
because they're frequent (routine trial operations) and watching
them would emit `stale_data` to every connected coordinator on every
patient move — operationally noisy. Deployments opt in via
`AuthzWatcher.watchContainment(projectionName)` if they want the
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

## Open questions

1. **`Principal` wire encoding canonicalization.** The codec must
   handle future `Principal` field additions without breaking older
   clients. Tentative rule: unknown fields are preserved opaquely on
   decode + re-encode (lossless), known fields are typed. Settled
   during implementation as we encounter the case.
2. **HTTP timeout defaults.** What's the right out-of-the-box
   timeout for action submissions and snapshot fetches? 30 seconds
   suggested as a placeholder; refine when we have measurement.
3. **Reconnect backoff parameters.** 250ms initial, 30s cap,
   doubling is a placeholder. Validate during testing; expose
   override via `RemoteScope` constructor parameter (`reconnectBackoff`)
   so deployments can tune.
4. **Concurrent subscribes during reconnect.** If the client opens
   `watch<T>` while the WS is in backoff, behavior should be: queue
   the subscribe message until reconnection completes, then send.
   Confirm during implementation.

## Future work

Explicitly out of scope for this plan but anticipated:

- **`resume-from-sequence` reconnect optimization** (v1.1). Wire-
  compatible add. Client tracks last-applied sequence per
  subscription; server resumes from that sequence on resubscribe.
  Eliminates the snapshot-replay flash on reconnect.
- **Connection-state observability** (v1.1). `Stream<ConnectionState>`
  on `RemoteScope` for "Reconnecting…" UX. Variants:
  `Connected`, `Disconnected`, `Reconnecting(attempt: N)`.
- **Snapshot pagination** for very large views. Defer until measured
  scale demands it. Wire shape: `{"type":"snapshot_batch", ...}` or
  similar; details when implemented.
- **Server-side metrics + observability hooks.** Connection counts,
  subscription counts, message rates, errors. Probably exposed by
  `ReactionHandlers` as a `metrics` getter feeding a consumer-supplied
  sink (or per-handler counters that the consumer's existing
  OpenTelemetry middleware can pick up).
- **`JwtAuthValidator` reference impl in a sibling lib.** If
  multiple consumers converge on the same JWT shape (e.g., a
  `reaction_firebase_auth` package), a concrete validator could ship
  separately from `reaction` proper. Same pluggability discipline.
- **Cross-process predicates.** `SubscriptionFilter.predicate` is a
  Dart closure and cannot be serialized over the wire; the wire codec
  drops it on encode (decoded remote filters always have
  `predicate == null`). If a future consumer needs predicate-based
  filtering on remote subscriptions, the recommended path is a
  named-predicate registry: register the predicate with the same
  string identifier on both the client and the server (via
  `ReactionHandlers` config), and the wire ships only the identifier.
  Today no consumer uses `SubscriptionFilter.predicate`, so this is
  documented as a future path rather than specified.
- **Structured error encoding.** `DispatchParseDenied`,
  `DispatchValidationDenied`, and `DispatchExecutionFailed` each carry
  a substrate `error: Object` field. The wire codec encodes errors via
  `error.toString()`; on decode, the error field is reconstructed as
  the raw String (the substrate's `Object` field accepts it).
  Consequence: structured error types do not round-trip with their
  full structure. If a future consumer needs richer error info on the
  client, the recommended path is either (a) the substrate adopts a
  sealed `Error` type the wire can encode/decode generically, or
  (b) an `ErrorCodec` extension point that consumers register on both
  sides (analogous to the named-predicate path). Today no consumer
  requires structured errors, so this is documented as a future path.

## Roadmap impact and sequencing

This design merges the roadmap's split of Plan B-remote (client only)
and Plan C (server only) into a single plan: "Plan B-remote+C". The
roadmap document (`docs/superpowers/specs/2026-05-11-roadmap.md`)
will be updated alongside this design to reflect:

- Plan B-remote+C scope (Remote* impls + wire codecs + reference
  server + `TrustingAuthValidator`).
- Defers: production validators, resume-from-sequence, reactive
  re-narrowing on permission/scope change.
- **Sequencing:** impl ticket is gated on CUR-1331 impl landing. The
  reaction server's per-subscription authz consults
  `AuthorizationPolicy.isPermitted` /
  `effectivePermissionsFor` / `ContainmentResolver` shapes that
  CUR-1331 reshapes; writing impl against the pre-1331 surface and
  then sweeping would mean two PRs touch the same code in pre-
  shipping greenfield posture. Better to land CUR-1331 impl first
  and target the final API directly.
- Status: design draft committed; impl ticket TBD after CUR-1331
  impl lands.

## Reading order recommendation

For first contact:

1. `spec/prd-reaction.md` Reading Order section (PRDs 1-5) — pins
   the obligations this design satisfies.
2. `spec/scoped-permissions.md` (CUR-1331) — the substrate
   permission shapes this design's server-side authz consults.
3. This document, Sections 1 → 5 (module → wire → client → server →
   test).
4. The Trust Boundary Expansion section — understand the new trust
   input the deployment is committing to.
5. The Decisions section — for context on why specific choices were
   made.

## References

- `spec/prd-reaction.md` — the 6 PRDs this design elaborates.
- `spec/scoped-permissions.md` — CUR-1331 substrate primitives the
  reference server's per-subscription authz consults.
- `spec/prd-library-charter.md` — epistemic-layer framing and AOP
  discipline.
- `docs/superpowers/specs/2026-05-11-roadmap.md` — Phase progress,
  Plan B-remote+C status, sequencing notes.
- Linear: CUR-1317 (libify), CUR-1331 (scope-aware permissions;
  blocks Plan B-remote+C impl).
