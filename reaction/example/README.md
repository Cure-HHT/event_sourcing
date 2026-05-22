# reaction_example

A two-process demo of the `reaction` package: a Flutter Linux desktop
client talking to a shelf-based Dart server over HTTP + WebSocket via
`RemoteScope` and `ReactionHandlers`.

The demo deliberately stays small. One action (`submit_note`), one view
(`notes_today`), one role (`editor`), and one out-of-band admin endpoint
(`/admin/revoke`) used to demonstrate the force-logout flow.

## What this demonstrates

- **`RemoteScope`** — single composition root on the client owning
  the shared HTTP client and WS connection.
- **`ReactionHandlers`** — server-side bundle that exposes
  `/me`, `/actions`, `/permissions/snapshot`, and `/subscriptions`
  shelf handlers against a substrate.
- **`TrustingAuthValidator`** — dev-only bearer-token validator that
  accepts any non-empty string as `userId` and surfaces the configured
  active role.
- **`AuthzWatcher` force-logout** — when an admin appends a
  `role_unassigned` event, the watcher closes the affected user's WS
  with code 4003. The client's `RemoteAuthSession` flips to
  `Expired`, and the UI routes to a "session expired" screen.

## File layout

```text
reaction/example/
  bin/server.dart                 console entry: shelf_io.serve
  lib/server/
    bootstrap.dart                substrate + ReactionHandlers + /admin/revoke
    submit_note_action.dart       Action<NoteInput, NoteResult>
    notes_projection.dart         AggregateProjectionSpec
  lib/client/
    main.dart                     runApp(NotesApp())
    app.dart                      NotesApp + AuthStatus -> screen routing
    login_screen.dart             TextField + Sign-in
    home_screen.dart              StreamBuilder + add-note + revoke-role
  test/server_smoke_test.dart     boots bootstrap() and hits /me
```

## Running the demo

Open two terminals.

**Terminal 1 — start the server:**

```sh
cd reaction/example
dart pub get
dart run bin/server.dart
```

The server binds `127.0.0.1:8080` by default. Override with
`--host` / `--port`. State is ephemeral — restart resets everything.

**Terminal 2 — start the Flutter client:**

```sh
cd reaction/example
flutter create .          # first time only, to scaffold linux/ etc.
                          # (creates linux/, android/, web/, ...; only linux/
                          # is needed for the demo — delete the rest if you like)
flutter run -d linux -t lib/client/main.dart
```

To point at a non-localhost server, set `REACTION_SERVER_URL`:

```sh
REACTION_SERVER_URL=http://10.0.0.5:8080 flutter run -d linux ...
```

## What to try

1. Enter any username (e.g. `alice`) and click **Sign in**. The
   server's `TrustingAuthValidator` accepts the credential and gives
   you the `editor` role.
2. Type a title, click **Submit**. A `submit_note` action dispatches;
   the resulting `note_created` event flows into the `notes_today`
   view; your `StreamBuilder` re-renders.
3. Click **Revoke my role**. The client POSTs to `/admin/revoke`;
   the server appends `role_unassigned` for your userId; the
   `AuthzWatcher` closes your WS with code 4003; `RemoteAuthSession`
   flips to `Expired`; the app routes you to a "Your session expired"
   screen.

## Smoke test

A minimal Dart test boots `bootstrap()` onto an ephemeral port and
verifies that `/me` is gated by bearer auth:

```sh
cd reaction/example
flutter test test/server_smoke_test.dart
```

## Caveat: `/admin/revoke` is unauthenticated

By design — this is a demo endpoint that lives outside the reaction
action pipeline so it can revoke the *current user's own* permissions
without first asking them for permission. **In production, this would
be gated behind admin auth.** See `bootstrap.dart` for the explicit
comment.
