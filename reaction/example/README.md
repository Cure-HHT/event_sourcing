# reaction_example

A two-process reference app for the `reaction` package: a Flutter Linux
desktop client talking to a shelf-based Dart server over HTTP + WebSocket
via `RemoteScope` and `ReactionHandlers`.

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
- **Reactive UI** — every part of the home screen that depends on the
  user's permissions wraps in `StreamBuilder<EffectiveAuthorization?>`
  on `PermissionSource.stream`. The admin panel becomes visible the
  moment an `assign_role` grant lands.

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
  lib/client/
    main.dart                      runApp(NotesApp())
    app.dart                       AuthStatus routing
    login_screen.dart              username text field
    home_screen.dart               StreamBuilder over PermissionSource
    notes_list.dart                reactive notes_today list
    submit_note_form.dart          permission-gated form
    admin_panel.dart               assign/unassign UI (admin-only)
  test/server_smoke_test.dart      11-case end-to-end smoke test
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
State is ephemeral — restart resets everything.

**Terminal 2 — start the Flutter client:**

```sh
cd reaction/example
flutter create .          # first time only: scaffolds linux/ etc.
flutter run -d linux -t lib/client/main.dart
```

To point at a non-localhost server, set `REACTION_SERVER_URL`:

```sh
REACTION_SERVER_URL=http://10.0.0.5:8080 flutter run -d linux ...
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

## Smoke test

```sh
cd reaction/example
flutter test
```

The smoke test boots `bootstrap()` onto an ephemeral port and verifies
11 cases: per-user `activeRole` on `/me`, anonymous fallback for
unknown bearers, workspace-scoped `submit_note` success/denial across
the seeded users, admin-vs-non-admin gating on `assign_role` and
`unassign_role`, and `/permissions/snapshot` shape per role.

## `/admin/revoke` is the no-auth escape hatch

The demo retains an unauthenticated `POST /admin/revoke?user=<id>`
endpoint that appends a `role_unassigned` event for the supplied user
without going through the action pipeline. It exists so the
force-logout flow can be triggered from a demo that doesn't have an
admin user — e.g. for a first-time walk-through with a single client.
**The primary mechanism going forward is the in-app `unassign_role`
action** (admin-gated, audited, idempotency-tracked); production
deployments would simply not expose `/admin/revoke`.
