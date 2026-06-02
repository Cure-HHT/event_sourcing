# example_clinical_scopes

A two-process reference app demonstrating **hierarchy-scoped reads**: a
Flutter Linux/web client talking to a shelf-based Dart server over
HTTP + WebSocket via `RemoteScope` and `ReactionHandlers`. It shows the
read-path `ScopeDescendantExpander` narrowing a live view subscription
so each user sees only the participants within their assigned scope.

## What this demonstrates

A `region → site → participant` containment hierarchy with three roles,
seeded so each user's reactive participant list is narrowed differently:

| User             | Role         | Assignment                       | Sees                       |
| ---------------- | ------------ | -------------------------------- | -------------------------- |
| `dr-investigator`| Investigator | `site-A` + `site-B` (two grants) | Ann (P-1), Bob (P-2), Cara (P-3) |
| `dr-overseer`    | Overseer     | `region-West` (two-hop)          | Ann, Bob, Cara (P-9 excluded) |
| `dr-admin`       | Admin        | total wildcard                   | all four, incl. Dan (P-9)  |
| `dr-unassigned`  | —            | none                             | nothing (subscription denied) |

Participants P-1/P-2 are at site-A, P-3 at site-B (both in region-West),
and P-9 at site-C (region-East). The Investigator case is a one-hop
expansion (`site → participant`); the Overseer case is a two-hop
expansion (`region → site → participant`) — both resolved by the
substrate's `ScopeDescendantExpander` walking the `participant_site_index`
and `site_region_index` containment projections downward.

The client renders a reactive `ViewBuilder<Participant>` list and a
user-switcher; switching users re-subscribes and re-narrows the list.
Participants are read-only — this demo is about the read path.

## Run it

**Terminal 1 — start the server:**

```sh
cd event_sourcing/example_clinical_scopes
dart pub get
dart run bin/server.dart
```

Binds `127.0.0.1:8080` by default (`--host` / `--port` to override).
State is ephemeral in-memory — restart resets everything. Permissive
CORS is enabled so a web client from another origin can reach it.

**Terminal 2 — start the Flutter client (desktop):**

```sh
cd event_sourcing/example_clinical_scopes
flutter create .          # first time only: scaffolds linux/ etc.
flutter run -d linux -t lib/client/main.dart
```

**Or run the client in a browser:**

```sh
cd event_sourcing/example_clinical_scopes
flutter create .          # first time only: scaffolds web/ etc.
flutter run -d chrome -t lib/client/main.dart
```

Point at a non-localhost server with the `REACTION_SERVER_URL`
compile-time define (works on every target):

```sh
flutter run -d linux -t lib/client/main.dart \
  --dart-define=REACTION_SERVER_URL=http://10.0.0.5:8080
```

Use the dropdown at the top to switch between the four seeded users and
watch the participant list re-narrow to each user's scope.

## Tests

```sh
flutter test
```

The e2e suite (`test/clinical_scoping_e2e_test.dart`) boots this server
on a loopback port and drives real `RemoteScope` clients to assert each
user's visibility set end-to-end through the production expander. The
widget test (`test/clinical_app_widget_test.dart`) verifies the list UI
reacts to the scoped rows it is given.
