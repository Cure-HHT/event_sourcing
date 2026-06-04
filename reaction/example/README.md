# reaction_example

A two-process reference app for the `reaction` and `reaction_widgets`
packages: a Flutter Linux desktop client talking to a shelf-based Dart
server over HTTP + WebSocket via `RemoteScope` and `ReactionHandlers`.
The client UI is built entirely on the headless `reaction_widgets`
primitives — it is the reference consumer of that package.

The demo is sized to exercise the substrate's full permission model in
miniature: three roles, one scope class with two values, four seeded
users with distinct assignments, three actions, and live reactive UI
that updates when permissions change mid-session.

## What this demonstrates

- **`RemoteScope`** — client-side composition root owning the shared
  HTTP client + multiplexed WS connection. Surfaces `AuthSession`,
  `ActionSubmitter`, `ViewSource`, and `PermissionSource`.
- **`ReactionHandlers`** — server-side bundle exposing `/me`,
  `/actions`, `/permissions/snapshot`, and `/subscriptions` shelf
  handlers against a substrate.
- **`RoleAwareTrustingValidator`** (example-local) — reads
  `user_role_scopes` at authenticate-time and surfaces the user's
  highest-privileged role as their `activeRole`. Production validators
  (Firebase, Auth0, JWT) follow the same shape: trust-on-supply for
  identity, closed-under-events for role/scope.
- **`ScopeClassRegistry` + scoped permissions** — `submit_note` is
  scoped on the `workspace` class. Per-dispatch `scopeFor()` returns a
  `BoundScope('workspace', input.workspace)`; the policy checks the
  user's assignments against it.
- **`AuthzWatcher` security flows** — `role_unassigned` /
  `permission_revoked` force-close affected users' WS with code 4003
  (client flips to `Expired`). `role_assigned` / `permission_granted`
  push a `stale_data` envelope; the client's `RemotePermissionSource`
  re-fetches `/permissions/snapshot` so UI gating updates live.
- **`reaction_widgets` primitives drive the whole client UI** — the
  refactored client consumes the headless widget layer rather than
  hand-rolling stream plumbing:
  - **`ReActionScope`** (`app.dart`) threads the `RemoteScope` down the
    tree once, above `MaterialApp`; every screen reads it via
    `ReActionScope.of(context)`. The same widget tree would work
    unchanged over a `LocalScope` (in-process mobile) — the
    substrate-agnostic contract (`EVS-PRD-reaction-widget-contract`-B).
  - **`ViewBuilder<T>`** (`notes_list.dart`, `admin_panel.dart`) replaces
    the old hand-written accumulators. It owns subscription,
    `Snapshot/EndOfReplay/Delta/Tombstone` accumulation, and surfaces a
    sealed `ViewState` (`Loading` / `Ready` / `Stale`). The `Stale`
    branch renders a reconnecting banner over last-known rows, driven by
    the scope's authoritative `ConnectionStatus`.
  - **`ActionBuilder`** (`submit_note_form.dart`, `admin_panel.dart`)
    owns the submission lifecycle and idempotency-key minting, exposing
    `Idle/Submitting/Success/Denied/Failed` to the builder.
  - **`PermissionGate`** (`home_screen.dart`) gates the admin panel on
    `assign_role`; it appears live the moment a grant lands.
  - **`ViewListener<Note>`** (`home_screen.dart`) toasts on each LIVE
    note (a `Delta`) as an imperative side-effect without rebuilding the
    list.
  - **`ReActionErrorListener`** (`home_screen.dart`) surfaces transport
    reconnecting/disconnected transitions as snackbars.

  The pieces that need the FULL `EffectiveAuthorization` for *display*
  (the identity bar's role, the permissions card, the submit form's
  scope pre-filter) still read the raw `PermissionSource.stream` via a
  `StreamBuilder` — the headless layer deliberately ships only a boolean
  `PermissionGate`, not a snapshot-exposing builder, so this is the
  supported pattern for display-only consumers.

## Seeded users

| User  | Role   | Scope                                |
| ----- | ------ | ------------------------------------ |
| alice | editor | `BoundScope(workspace, west)`        |
| bob   | editor | `BoundScope(workspace, east)`        |
| carol | admin  | `TotalWildcardScope`                 |
| dave  | viewer | `TotalWildcardScope`                 |

Roles' grants:

- **viewer**: `view:notes_today`
- **editor**: viewer + `submit_note` (scoped on `workspace`)
- **admin**: editor + `assign_role`, `unassign_role`,
  `view:user_role_scopes`, `view:role_permission_grants`

Unknown bearers (any non-empty string with no seeded role) authenticate
as `AnonymousPrincipal`. The substrate then denies every permission
check — the closed-under-events trust model in action.

## File layout

```text
reaction/example/
  bin/server.dart                  console entry: shelf_io.serve
  lib/server/
    bootstrap.dart                 substrate + ReactionHandlers + seeds
    role_aware_auth_validator.dart reads user_role_scopes at auth time
    submit_note_action.dart        workspace-scoped Action
    role_admin_actions.dart        AssignRoleAction + UnassignRoleAction
    notes_projection.dart          AggregateProjectionSpec
  lib/client/                      (built on reaction_widgets)
    main.dart                      runApp(NotesApp())
    app.dart                       mounts ReActionScope + AuthStatus routing
    login_screen.dart              username text field
    home_screen.dart               PermissionGate + ViewListener +
                                   ReActionErrorListener composition hub
    notes_list.dart                ViewBuilder<Note> (Loading/Ready/Stale)
    submit_note_form.dart          PermissionGate + ActionBuilder
    admin_panel.dart               ViewBuilder + ActionBuilder (admin-only)
  test/server_smoke_test.dart      11-case server-side end-to-end test
  test/client_widget_test.dart     client widget tests using FakeReaction
```

## Running the demo

Open two terminals (or more — see the multi-user walkthrough).

**Terminal 1 — start the server:**

```sh
cd reaction/example
dart pub get
dart run bin/server.dart
```

Binds `127.0.0.1:8080` by default. Override with `--host` / `--port`.
State is ephemeral — restart resets everything. The server enables
permissive CORS (`Access-Control-Allow-Origin: *`) so a web client served
from another origin can reach it; production deployments scope or drop
that.

**Terminal 2 — start the Flutter client (desktop):**

```sh
cd reaction/example
flutter create .          # first time only: scaffolds linux/ etc.
flutter run -d linux -t lib/client/main.dart
```

**Or run the client in a browser** (the substrate-agnostic widget code is
identical — this is the spec's "Use 2: Flutter web client" path):

```sh
cd reaction/example
flutter run -d chrome -t lib/client/main.dart
# or build once and serve the bundle:
#   flutter build web -t lib/client/main.dart
#   (then serve build/web on any static host)
```

To point at a non-localhost server, pass the `REACTION_SERVER_URL`
compile-time define (works on every target, including web):

```sh
flutter run -d linux -t lib/client/main.dart \
  --dart-define=REACTION_SERVER_URL=http://10.0.0.5:8080
```

## Multi-user walkthrough (the load-bearing demo)

Open **two** Flutter instances pointing at the same server — one as
carol (admin), one as alice (editor-west). Run each in a separate
terminal:

```sh
# Terminal A
cd reaction/example
flutter run -d linux -t lib/client/main.dart  # log in as carol

# Terminal B
cd reaction/example
flutter run -d linux -t lib/client/main.dart  # log in as alice
```

Now exercise the reactive permission flow:

1. **alice's window**: try Submit with the workspace dropdown set to
   'east'. Server denies (alice has no `BoundScope(workspace, east)`).
   The flash reads `Denied: submit_note (workspace=east)`.
2. **carol's window**: in the admin panel, Grant `alice` -> `editor` ->
   `east`. The action emits `role_assigned`; AuthzWatcher pushes
   `stale_data` to alice's WS; `RemoteConnection.onStaleData` fires;
   `RemotePermissionSource.refresh()` re-fetches the snapshot.
3. **alice's window**: try Submit again with workspace='east'. Now it
   succeeds. (No client-side cache invalidation needed; the substrate
   alone drives this.)
4. **carol's window**: Revoke alice's east assignment.
5. **alice's window**: Submit east fails again, immediately.

For the force-logout path:

1. **carol's window**: Revoke alice's original west assignment too. Or
   alice can hit the demo "Force-logout (revoke seed)" button.
2. **alice's window**: WS closes with 4003; `RemoteAuthSession` flips
   to `Expired`; the app routes to "Your session expired — please sign
   in again."

## What the substrate enforces vs the UI gates

The example labels workspaces the user is NOT scoped to as
`(no permission)` in the dropdown — read from
`EffectiveAuthorization.scopeAssignments` on the live snapshot — but
keeps them selectable. Per-dispatch authorization still happens at the
substrate; trying a `(no permission)` workspace produces
`DispatchAuthorizationDenied` and the flash surfaces the denial. This
makes the substrate's role visible: a "wrong workspace" isn't a UI
bug, it's the policy doing its job, and the UI pre-filter is a
courtesy.

Production apps choosing strict pre-filtering (hide unauthorized
workspaces entirely) would `.where()` `kKnownWorkspaces` by the same
`scopeAssignments` walk used here. The substrate-side denial path
remains the perimeter either way.

## Tests

```sh
cd reaction/example
flutter test
```

Two suites run:

- **`test/server_smoke_test.dart`** boots `bootstrap()` onto an ephemeral
  port and verifies 11 server-side cases: per-user `activeRole` on `/me`,
  anonymous fallback for unknown bearers, workspace-scoped `submit_note`
  success/denial across the seeded users, admin-vs-non-admin gating on
  `assign_role` and `unassign_role`, and `/permissions/snapshot` shape
  per role.
- **`test/client_widget_test.dart`** exercises the client widgets against the
  shipped `reaction_widgets_testing` doubles (`FakeReaction` +
  `pumpReactionWidget`, per `EVS-PRD-reaction-widget-contract`-H) — no
  server, no timing. It drives `ViewBuilder`'s `Loading → Ready → Stale`
  transitions deterministically, asserts `PermissionGate` hides the
  submit form without `submit_note`, and walks an `ActionBuilder`
  submission from in-flight `Submitting` to a `Denied` result. This is
  the reference for how downstream consumers test their own sugar.

## `/admin/revoke` is the no-auth escape hatch

The demo retains an unauthenticated `POST /admin/revoke?user=<id>`
endpoint that appends a `role_unassigned` event for the supplied user
without going through the action pipeline. It exists so the
force-logout flow can be triggered from a demo that doesn't have an
admin user — e.g. for a first-time walk-through with a single client.
**The primary mechanism going forward is the in-app `unassign_role`
action** (admin-gated, audited, idempotency-tracked); production
deployments would simply not expose `/admin/revoke`.

## Playwright end-to-end (web UI automation)

The Flutter web client renders through CanvasKit (a single `<canvas>`),
so Playwright drives it via Flutter's accessibility/semantics tree, which
`main.dart` force-enables on web. Widgets carry stable
`Semantics(identifier:)`s that surface as `flt-semantics-identifier` DOM
attributes.

One-shot local run (builds web, boots the demo server, runs the suite):

```sh
cd reaction/example
scripts/run-e2e.sh
```

First time only, install the Playwright browser:

```sh
cd reaction/example/e2e && npm install && npx playwright install chromium
```

The suite lives in `e2e/tests/`. Selectors use
`[flt-semantics-identifier="..."]`. CI wiring (headless Chromium + server
orchestration) is deferred to a follow-up ticket.
