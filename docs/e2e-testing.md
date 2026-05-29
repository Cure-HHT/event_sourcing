# End-to-end & scenario tests

This repo's unit and conformance tests run on every change. The suites
described here are **heavier, black-box, end-to-end** tests that drive the
example apps from outside (real HTTP + WebSocket, and — optionally — the
rendered Flutter UI). They are **not** meant to run on every local commit;
they belong in PR CI (Tier 1) or are run on demand (Tier 2).

They were added under CUR-1317 to close a coverage gap: **multiple clients
connected to a single server**, plus user-story scenarios exercising the
permission system and projections in combination.

## Two tiers

| Tier | What | Runs under | Needs |
|------|------|-----------|-------|
| 1 | Automated multi-client + permission/projection scenarios | `flutter test` | nothing extra (boots its own server on a loopback port / subprocess) |
| 2 | UI confirmation through the rendered Flutter client | `flutter drive` | a display + a manually-started server |

Tier 1 is CI-ready and runs in the **e2e scenario tests** PR workflow
(`.github/workflows/e2e-scenario-tests.yml`). Tier 2 is run by hand (or a
manual workflow) because it needs a desktop display and a live server.

## Tier 1 — automated (run in PR CI)

All three commands are plain `flutter test`; each test boots a fresh,
ephemeral, in-memory server, so there is nothing to start or tear down.

```sh
# 1. Multi-client cross-process scenarios over real HTTP + WebSocket
#    (N RemoteScope clients against one reaction_example server):
#    propagation/ordering/convergence, late-joiner snapshot, write-side
#    scope perimeter, reactive grant (stale_data) + revoke (4003),
#    anonymous denial, concurrency, per-principal idempotency.
cd reaction/example
flutter test test/e2e/

# 2. Permission/projection user-story scenarios against the
#    action_permissions_demo server (spawned as a real subprocess):
#    onboarding-brings-authz-alive, cross-team scope perimeter,
#    replay-safe idempotent provisioning, audit completeness.
cd event_sourcing/example_action_permissions
flutter test test/scenarios/

# 3. The reaction_widgets ViewBuilder invariant suite (incl. the
#    pre-EndOfReplay buffering guarantee) — part of the package's tests:
cd reaction_widgets
flutter test test/view/view_builder_test.dart
```

(#3 already runs in the existing `reaction-widgets-tests.yml` workflow,
since it lives in that package's normal test suite.)

## Tier 2 — UI confirmation (run on demand)

Drives the **real** Flutter desktop client via `flutter_driver` while a
second client writes over HTTP, confirming the `reaction_widgets` UI
reflects another client's write live. Requires a display (a desktop
session, or `xvfb-run` on a headless box) and a running server.

```sh
cd reaction/example

# Terminal A — start an ephemeral server on :8080:
dart run bin/server.dart --port 8080

# Terminal B — drive the desktop client against it:
flutter drive \
  --driver=test_driver/app_test.dart \
  --target=lib/client/driver_main.dart \
  -d linux
# headless: prefix with `xvfb-run -a `
```

The driver entrypoint is `lib/client/driver_main.dart` (identical to
`main.dart` plus `enableFlutterDriverExtension()`); production/demo
launches use `main.dart` and never carry the driver extension.

Notes:

- The UI test logs in as **dave** (a *viewer*). A viewer's home screen has
  no admin panel, so the notes `ListView` gets enough vertical space to
  build its first row in a small headless window. Logging in as an admin
  collapses the notes area under the panels on a short window, so the
  lazily-built `ListView` builds no items even though the row is present
  in widget state — a window-size artifact of the demo's single-`Expanded`
  layout, not a reactive defect.
- Point at a non-default server with
  `--dart-define=REACTION_SERVER_URL=http://host:port`.

## When to run

- **Not on pre-commit / not on every commit.** These are slower and (for
  Tier 2) environment-dependent.
- **Tier 1: on PRs.** Wired in `e2e-scenario-tests.yml` (triggers on
  `pull_request` and `push` to `main`), matching the other test workflows.
- **Tier 2: on demand** — before merging UI-affecting changes, or wire a
  manual (`workflow_dispatch`) CI job with `xvfb` + Linux desktop build
  deps if unattended runs are wanted later.

## Layout

```text
reaction/example/test/e2e/
  multiclient_harness.dart        boots reaction_example bootstrap() on a
                                  loopback port; hands out N RemoteScope
                                  clients + observation helpers
  notes_propagation_test.dart     S1–S3 (propagation, late joiner, scope)
  reactive_permissions_test.dart  S4–S6 (grant/revoke mid-session, anon)
  concurrency_idempotency_test.dart S7–S8 (concurrency, idempotency)
reaction/example/test_driver/
  app_test.dart                   Tier-2 flutter_driver UI confirmation
reaction/example/lib/client/
  driver_main.dart                flutter_driver-enabled entrypoint
event_sourcing/example_action_permissions/test/scenarios/
  permission_projection_stories_test.dart  B9–B12 (subprocess HTTP server)
```
