# Reaction Remote Impls, Reference Server, and Wire Protocol Design

**Phase**: I (post Plan B-local)
**Status**: Draft (Plan B-remote+C implementation target)
**Last updated**: 2026-05-12
**Elaborates**: `spec/prd-reaction.md` PRDs `EVS-PRD-auth-session`,
`EVS-PRD-action-submitter`, `EVS-PRD-view-subscriber`,
`EVS-PRD-permission-snapshot-source`, and
`EVS-PRD-cross-process-event-transport`.
**Supersedes**: the roadmap's split of Plan B-remote (client only) and
Plan C (server only) into two separate plans. This design merges them
into a single coherent implementation: the wire protocol cannot be
specified, codec-tested, or end-to-end-validated without both sides
present, and the PRD architecture
(`spec/prd-reaction.md` lines 26-55) already puts `local/`, `remote/`,
`server/`, and `wire/` in one package. Roadmap update to reflect the
merge ships alongside this design.

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
- Server-side composition: `ReactionServer`, auth middleware, WS
  upgrade handler, per-subscription authorization mechanism,
  per-connection write serialization, custom-route registration.
- Reference auth validator (`TrustingAuthValidator`) for dev/test.
- Testing strategy across codec, server-unit, and end-to-end layers.

Out of scope (deferred):

- Production validators (Firebase JWT, Auth0, install-UUID linking
  code, etc.) — these live in app code per the substrate's trust-
  boundary discipline; see "Trust boundary expansion" below.
- v1.1 `resume-from-sequence` reconnect optimization — wire format
  reserves space; not implemented here.
- Reactive re-narrowing of active subscriptions on permission grant
  change — deferred to CUR-1331 impl follow-up (the call site that
  will already be re-shaped by scope-aware permission consulting).
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
`RolePermissionGrants` projection. The reaction server introduces no
parallel policy mechanism; it composes substrate filter primitives
based on the requesting Principal's `PermissionSnapshot`.

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
    permission_snapshot_codec.dart PermissionSnapshot JSON codec
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
  server/          (NEW; shelf-based reference server)
    reaction_server.dart           Top-level; mounts routes, holds
                                   validator, owns per-conn state
    auth_middleware.dart           Bearer header -> Principal
    action_route.dart              POST /actions
    me_route.dart                  GET /me
    permission_route.dart          GET /permissions/snapshot
    subscription_handler.dart      WS upgrade; per-sub authz; relay
    validators/
      trusting_auth_validator.dart Dev/test only
```

The top-level `reaction.dart` barrel grows exports for:

- `RemoteScope`, `RemoteAuthSession`, `RemoteActionSubmitter`,
  `RemoteViewSource`, `RemotePermissionSource`.
- `ReactionServer`.
- `TrustingAuthValidator`.

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
  200:     JSON PermissionSnapshot envelope
  401:     empty body

GET /healthz
  No auth required
  200:     "ok"
```

The `Authorization: Bearer <credential>` header is standard; the
credential is opaque to the lib (passed directly to
`PrincipalAuthValidator.authenticate`). The substrate sees only the
validated `Principal` that the validator returns.

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
 "aggregateId": "<id>", "row": {...Map<String,Object?>...}}

{"type": "delta",     "subscriptionId": "<UUID>", "sequence": N,
 "aggregateId": "<id>", "row": {...Map<String,Object?>...}}

{"type": "tombstone", "subscriptionId": "<UUID>", "sequence": N,
 "aggregateId": "<id>"}

{"type": "end_of_replay", "subscriptionId": "<UUID>", "sequence": N}

// Out-of-band (not tied to any one subscription):
{"type": "error", "code": "internal_error" | "protocol_error",
 "message": "<human-readable; for logging>"}
```

JSON shape notes:

- All wire envelopes are flat objects with a `"type"` discriminator.
  No nested polymorphic wrappers. Codec dispatch on `"type"`.
- `sequence` is the substrate's monotonic event sequence at the
  moment the update was generated (PRD assertion B).
- `aggregateId` appears on `snapshot` / `delta` / `tombstone` but NOT
  on `end_of_replay` (per-subscription marker, not per-row).
- `row` is the raw `Map<String, Object?>` from the substrate; client
  applies its consumer-supplied mapper. Server-side mapping would
  break domain-neutrality (server would need to know every consumer
  type) — rejected.
- `DispatchResult.Success.appendedEvents[]` carries `StoredEvent`
  envelopes with full Layer 1 fact data (hash, sequence, initiator
  Principal, etc.). The codec preserves all fields.

### Close frames

- `4001 auth_rejected` — credential failed validation; client maps
  to `AuthStatus.Expired`. Client does NOT reconnect.
- `4002 server_shutting_down` — graceful; client reconnects with
  backoff.
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
   `PermissionSnapshot` reducer the `LocalPermissionSource` uses.

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

## Section 4 — Server lifecycle

### `ReactionServer` (composition class)

```dart
final server = ReactionServer(
  eventStore: store,            // already-opened substrate
  dispatcher: dispatcher,       // already-constructed ActionDispatcher
  validator: TrustingAuthValidator(
    defaultActiveRole: 'install',
  ),
  // Optional:
  permissionViewName: 'role_permission_grants',
  viewPermissionNamer: ReactionServer.defaultViewPermissionNamer,
);

// Mount on shelf:
final handler = server.handler;
final httpServer = await shelf_io.serve(handler, 'localhost', 8080);

// Custom routes (resolves PRD Open Q4):
server.router.get('/login', loginHandler);
server.router.post('/linking-code/redeem', redeemHandler);
// Custom routes bypass reaction's auth middleware; consumer owns
// their semantics (login is by definition pre-auth).

// On shutdown:
await server.dispose();
```

**Resolving PRD Open Q4 (custom-route registration):** exposing the
`Router` directly is simpler than `addCustomRoute(...)`, gives the
consumer full shelf semantics (middleware, sub-routers, request
introspection), and matches how `shelf_router` is typically used.
The wrapper API surface stays small.

The `viewPermissionNamer` parameter maps a viewName to the required
view-level permission name. Default:
`(viewName) => 'view:$viewName'`. Override per-deployment to relax
particular views (e.g., make `notes_today` require no view-level
perm — pure row-level scoping) or remap names.

### Route table

```text
GET   /healthz              -> 'ok' (no auth)
GET   /me                   -> JSON Principal (auth required)
POST  /actions              -> dispatch + JSON DispatchResult (auth)
GET   /permissions/snapshot -> JSON PermissionSnapshot (auth)
GET   /subscriptions        -> WS upgrade (auth via first WS msg)
```

Plus any custom routes registered on `server.router`.

### Auth middleware (HTTP path)

```text
1. Read Authorization: Bearer <credential> header.
2. If missing -> 401.
3. Invoke validator.authenticate(credential).
   - Principal returned -> attach to Request context; proceed.
   - AuthenticationDenied -> 401 (opaque body).
   - Other exception -> 500 (log internally; opaque to wire).
```

Mounted on every route except `/healthz`, `/subscriptions`, and
consumer-registered custom routes.

### WebSocket upgrade handler

The `/subscriptions` route accepts the HTTP upgrade then runs a
per-connection state machine:

```text
On upgrade:
  state = AWAITING_AUTH
  Spawn message loop.

On message in AWAITING_AUTH:
  if msg.type != 'auth' -> close 4001
  Invoke validator.authenticate(msg.credential).
  On Principal:
    state = AUTHENTICATED
    Send {type: auth_ok, principalId: P.id}
    Open per-Principal PermissionSnapshot subscription (so view-
    level authz checks have fresh data without re-querying).
  On AuthenticationDenied:
    close 4001 auth_rejected

On message in AUTHENTICATED:
  switch msg.type:
    'subscribe'   -> run per-sub authorization (see below)
    'unsubscribe' -> look up by id; cancel substrate sub; remove
                     from registry
    default       -> send {type: error, code: protocol_error, ...}

On disconnect:
  Cancel all substrate subs for this connection.
  Cancel the per-Principal PermissionSnapshot subscription.
  Release connection state.
```

### Per-subscription authorization (Approach B)

Two-tier: view-level deny + row-level narrowing.

```text
On {type: subscribe, subscriptionId, viewName, filter, aggregates}:

  // Step 1: view-level deny
  required = viewPermissionNamer(viewName)
  if required != null and !currentSnapshot.has(required, scope: any):
    send {type: subscription_denied,
          subscriptionId, reason: view_permission_denied}
    return

  // Step 2: row-level narrowing
  allowedAggregates = currentSnapshot.aggregatesGrantedFor(viewName)
  // null = unrestricted
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

The call site `currentSnapshot.aggregatesGrantedFor(viewName)` is the
seam that CUR-1331 (scope-aware permissions) will reshape. It is
also the call site where reactive re-narrowing on permission change
would attach — deferring both to the CUR-1331 impl follow-up keeps
related work together.

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

`server.dispose()`:

1. Stops `shelf_io` from accepting new connections.
2. For every open WS: close-frame `4002 server_shutting_down`,
   cancel all substrate subs for that connection.
3. Drain in-flight HTTP responses.
4. Substrate cleanup (`eventStore.close()` etc.) remains the
   consumer's responsibility; the server does not own the substrate.

### What `ReactionServer` does NOT do

- Does not open or close the `EventStore` (consumer-owned).
- Does not construct the `ActionDispatcher` (consumer calls
  `dispatcher.dispatch`; the server just routes to it).
- Does not embed a default validator — `validator` is a required
  constructor parameter.
- Does not provide a CLI entry-point or `main()` — the consumer
  composes shelf hosting in their own binary.

## Section 5 — Testing strategy

### Test pyramid

```text
+-------------------------------------+
|  E2E roundtrip tests        ~25     |
|  (full RemoteScope <-> ReactionServer
|   with in-memory substrate)         |
+-------------------------------------+
|  Server-side unit tests     ~30     |
|  (ReactionServer + shelf,           |
|   minimal HTTP/WS client per case)  |
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
3. `ReactionServer` with `TrustingAuthValidator`.
4. `shelf_io` bound to ephemeral port (port=0); records the port.
5. `RemoteScope` against `http://localhost:<port>`.

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

**`EVS-PRD-permission-snapshot-source`**:

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

### What's NOT tested in this plan

- Production validators (Firebase JWT, Auth0, linking-code) — tested
  in app code per the trust-boundary discipline.
- Snapshot pagination — PRD Open Question 1 defer.
- Reactive re-narrowing on permission grant change — deferred to
  CUR-1331 impl follow-up.

## Trust boundary expansion

The reaction server (when deployed) introduces a new enumerated
trust input that CLAUDE.md's "Trust boundaries" section should
record alongside `StorageBackend`, `Destination`, and the
Principal-on-faith gap:

- **`PrincipalAuthValidator` implementation mounted on
  `ReactionServer`.** Pluggable interface registered at server-boot
  time. Trusted to map a wire-credential string to a `Principal`
  correctly; trusted to refuse invalid credentials by throwing
  `AuthenticationDenied`. The reaction lib ships only
  `TrustingAuthValidator` (dev/test); production deployments mount
  their own validator that closes the Principal-on-faith gap for
  that deployment.

From the substrate's POV (the four `lib/` packages in this repo),
nothing changes — the substrate still sees `Principal` values it
trusts. The reaction server is what closes the gap *externally*,
inside a single deployment. A different deployment with a different
validator gets different trust semantics.

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
client error
path (`subscription_denied` envelope), distinguishes "no data" from
"no permission" (which Approach A conflates), and matches how
`RolePermissionGrants` is already shaped (view-name-scoped and
aggregate-scoped permissions both exist). C (post-filter) was
rejected outright — leaks information via timing/count side-channels
and violates substrate-shaped filter composition.

**Why expose `server.router` directly rather than
`addCustomRoute(...)`?** Simpler; matches how `shelf_router` is
typically used; gives consumers full shelf semantics (middleware,
sub-routers, request introspection); keeps the wrapper API surface
small. Resolves PRD Open Q4.

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

1. **Reactive re-narrowing of active subscriptions on permission
   grant change.** Today's design snapshots the narrowed filter at
   subscribe-time and does not react to subsequent
   `RolePermissionGrants` changes. **Defers to: CUR-1331 impl
   follow-up.** The same call site is reshaped for scope-aware
   permission consulting; tackling both together keeps related work
   in one change.
2. **`Principal` wire encoding canonicalization.** The codec must
   handle future `Principal` field additions without breaking older
   clients. Tentative rule: unknown fields are preserved opaquely on
   decode + re-encode (lossless), known fields are typed. Settled
   during implementation as we encounter the case.
3. **HTTP timeout defaults.** What's the right out-of-the-box
   timeout for action submissions and snapshot fetches? 30 seconds
   suggested as a placeholder; refine when we have measurement.
4. **Reconnect backoff parameters.** 250ms initial, 30s cap,
   doubling is a placeholder. Validate during testing; expose
   override via `RemoteScope` constructor parameter (`reconnectBackoff`)
   so deployments can tune.
5. **Concurrent subscribes during reconnect.** If the client opens
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
  subscription counts, message rates, errors. Probably a
  `ReactionServer.metrics` getter feeding a consumer-supplied sink.
- **`JwtAuthValidator` reference impl in a sibling lib.** If
  multiple consumers converge on the same JWT shape (e.g., a
  `reaction_firebase_auth` package), a concrete validator could ship
  separately from `reaction` proper. Same pluggability discipline.

## Roadmap impact

This plan supersedes the roadmap's split of Plan B-remote (client
only) and Plan C (server only) into a single merged plan: "Plan
B-remote+C". The roadmap document (`docs/superpowers/specs/
2026-05-11-roadmap.md`) will be updated alongside this design to
reflect:

- Plan B-remote+C scope (Remote* impls + wire codecs + reference
  server + `TrustingAuthValidator`).
- Defers: production validators, resume-from-sequence,
  reactive re-narrowing (link to CUR-1331).
- Status: pending implementation per the plan ticket that follows
  this design.

## Reading order recommendation

For first contact:

1. `spec/prd-reaction.md` Reading Order section (PRDs 1-5) — pins
   the obligations this design satisfies.
2. This document, Sections 1 → 5 (module → wire → client → server →
   test).
3. The Trust Boundary Expansion section — understand the new trust
   input the deployment is committing to.
4. The Decisions section — for context on why specific choices were
   made.

The implementation plan that follows this design references the
PRD assertions, the design sections, and the saved feedback memories
in `~/.claude/projects/.../memory/` (auth-validators-are-consumer-
supplied; substrate-trust-boundaries; permission-policy-is-substrate)
to ensure no implementation step quietly drifts from a committed
principle.
