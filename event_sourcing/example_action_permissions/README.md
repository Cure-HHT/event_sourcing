# action_permissions_demo

A Linux-desktop reference application that exercises the `event_sourcing`
library's actions and permissions modules end-to-end. The demo runs a Dart
`shelf` server hosting the dispatcher, event store, and permission matrix,
paired with a Flutter Linux client that renders a dual pane: user-facing
controls on the left, server-side inspector on the right. It is the canonical
hands-on validation surface for the library's action and permission
guarantees — every claim the library makes about action dispatch,
authorization, idempotency, identity decoupling, audit correlation, user
provisioning, and snapshot delivery is demonstrated here as a runnable
scenario, mirrored by an integration test under `test/walkthroughs/`.

## What it exercises

- The `Action` lifecycle (parse, validate, authorize, execute) and
  typed event emission, surfaced in the request history pane.
- Dispatcher pipeline correlation: every dispatch carries a fresh
  v4 `action_invocation_id`, and `authorization_denied` events are emitted by
  the authorize stage. Visible in the inspector's audit view.
- Idempotency policy matrix (none / optional / required) plus
  cache-key composition that includes `principalId`, validated by the
  client-side replay mechanics.
- Denial events for parse failures, validation failures, and
  unknown-action requests, surfaced as audit entries.
- `UserDirectory` materializer/seed-applier loop driven by
  `provision_user` action emissions.
- `AuthorizationPolicy` matrix lookup as the single perimeter
  for all action authorization decisions.
- Per-`userId` `PermissionSnapshot` delivery to the client and
  cache invalidation on identity change.
- Identity decoupling: switching the active `userId` changes the
  effective permission set without restarting the server.
- Delivery to a destination: the server starts a delivery cycle, and a
  demo destination writes every batch it delivers to the server log.
  Operator routes halt, cancel and recover delivery, and several servers
  on one Postgres database share it: one drains, the others stand by.
- Start-up probes: the server listens before its event store opens, and
  answers `/livez` and `/health` during a long boot.

## Architecture

The server is a single-process Dart `shelf` HTTP service that wires the
event-store, dispatcher, permission matrix, action catalog, and user-directory
projection into one in-memory pipeline. The Flutter Linux desktop client talks
to the server over plain HTTP and renders both panes from the same process; the
right pane polls the server's inspector endpoints to expose the audit log,
projected state, and current permission snapshot.

```text
+--------------------------------------------------+
|             Flutter Linux desktop app            |
|                                                  |
|  +------------------+    +-------------------+   |
|  |  Client pane     |    |  Inspector pane   |   |
|  |  - userId picker |    |  - event log      |   |
|  |  - action btns   |    |  - projected      |   |
|  |  - request hist. |    |    state          |   |
|  |                  |    |  - snapshot view  |   |
|  +--------+---------+    +---------+---------+   |
|           |                        |             |
+-----------|------------------------|-------------+
            |   HTTP (shelf, JSON)   |
            v                        v
+--------------------------------------------------+
|                Dart shelf server                 |
|                                                  |
|   dispatcher --> action catalog --> event store  |
|       |              |                  |        |
|       v              v                  v        |
|   matrix       idempotency        projections    |
|  (perimeter)      store         (user dir, etc.) |
+--------------------------------------------------+
```

## File layout

```text
example_action_permissions/
  bin/
    server.dart                     # shelf server entry point
  lib/
    server/
      actions/                      # 7 demo actions
      action_catalog.dart
      boot_health.dart              # /livez and /health during the boot
      bootstrap.dart                # wires the in-memory pipeline
      demo_idempotency_store.dart
      demo_routes.dart              # HTTP route table, operator routes
      demo_server_host.dart         # listens before the event store opens
      log_destination.dart          # demo destination: delivers to the log
      demo_state_projection.dart
      inspect_snapshot.dart
      user_directory.dart
      user_directory_materializer.dart
      user_directory_seed_applier.dart
    client/
      app.dart                      # MaterialApp shell
      main.dart                     # Flutter entry point
      client_pane.dart              # left pane
      server_inspector_pane.dart    # right pane
      action_buttons_panel.dart
      hacker_mode_toggle.dart       # raw-JSON view toggle
      http_client.dart
      permission_snapshot_cache.dart
      request_history_panel.dart
      userid_selector.dart
    shared/
      wire_types.dart               # request/response/snapshot shapes
  tool/
    provision.dart                  # provision the Postgres schema
    run_demo.sh                     # start server + client
    stop_demo.sh                    # graceful shutdown
    permissions.yaml                # demo matrix seed
    users.yaml                      # demo user seed
  test/
    actions/                        # per-action unit tests
    client/                         # widget tests
    walkthroughs/                   # canonical end-to-end scenarios
      test_support/
        demo_server_harness.dart
      walkthrough_01..10_*.dart
    boot_health_test.dart           # probes across a boot
    bootstrap_test.dart
    delivery_routes_test.dart       # operator routes
    demo_routes_test.dart
    demo_state_projection_test.dart
    user_directory_*_test.dart
    wire_types_test.dart
```

## Prerequisites

- Flutter 3.38 or newer (channel `stable` is fine).
- Dart 3.10 or newer (bundled with the matching Flutter version).
- Linux desktop with the GTK packages required by Flutter Linux:
  `libgtk-3-dev`, `libblkid-dev`, `liblzma-dev`, `clang`, `cmake`, `ninja-build`,
  `pkg-config`. On Debian/Ubuntu:

  ```text
  sudo apt install clang cmake ninja-build pkg-config libgtk-3-dev \
                   libblkid-dev liblzma-dev
  ```

Run `flutter doctor` and confirm the "Linux toolchain" line is green before
launching the demo.

## How to run

From this package's directory:

```text
tool/run_demo.sh
```

This builds the server, starts it on a free local port, builds the Flutter
Linux client, and launches it pointed at the running server. Console output
streams from both processes.

To stop:

```text
tool/stop_demo.sh
```

If the scripts do not fit your environment, the manual equivalent is:

1. Start the server: `dart run bin/server.dart`.
2. In another terminal, run the client:
   `flutter run -d linux --dart-define=DEMO_SERVER_URL=http://127.0.0.1:<port>`.
3. Stop both with `Ctrl+C` when finished.

## Running on Postgres

The demo server can run against `PostgresBackend` +
`PostgresIdempotencyStore` instead of the default sembast pair. Same
actions, same audit, same RBAC — events and idempotency rows persist
in Postgres tables.

Bring up the local-dev Postgres (the compose file in this directory):

```text
docker compose up -d --wait
```

The compose file keeps its data in a volume across restarts. A database
written by an earlier build of the library does not open under this
build, whatever its data format: the open refuses it with
`DatabaseResetRequiredError`, and it must be reset. A database of another
data-format major is refused with `DataFormatIncompatibleError`. To
start from an empty database, remove the volume first:

```text
docker compose down -v
docker compose up -d --wait
```

The same holds for the default sembast store, `demo.db` in the data
directory (`--data-dir`, by default `~/.local/share/action_permissions_demo`):
delete it to reset a database an earlier build wrote, or run with
`--ephemeral`.

Provision the schema once, before the first server starts (and again
after upgrading to a build with a newer schema): `--provision` creates or
migrates the tables and exits without serving. A server never changes the
schema itself, and one started against a database that was never
provisioned exits naming `--provision`. Provisioning refuses a database
whose tables an earlier build created without provisioning them; reset it
as above (`docker compose down -v`) and provision again.

```text
dart run bin/server.dart \
  --backend=postgres \
  --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
  --postgres-ssl-mode=disable \
  --provision
```

(`dart run tool/provision.dart --postgres-url=... --postgres-ssl-mode=disable`
does the same.)

Then start the demo server pointed at it:

```text
dart run bin/server.dart \
  --backend=postgres \
  --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
  --postgres-ssl-mode=disable \
  --port=8080 \
  --permissions-yaml=tool/permissions.yaml \
  --users-yaml=tool/users.yaml
```

For production deployments against a managed Postgres, omit
`--postgres-ssl-mode` to use the secure-by-default `require` setting, or
pass `--postgres-ssl-mode=verifyFull` for full certificate validation.

The compose file creates one role, `evs`, a superuser that both provisions
the schema and serves: a development setup only. A deployment provisions as
the role that owns the schema and runs its servers as a runtime role that
neither owns nor can create the tables and holds exactly the privileges of
`postgresRuntimeRoleGrants` (see the library guide, "Open a storage
backend", and `spec/postgres-backend.md`, "Roles and privileges").

Each server holds the library's generation locks on one dedicated lock
connection. It opens it to `--postgres-lock-url` when given, and to
`--postgres-url` otherwise; it must be a real server session -- a direct
connection or a session-mode proxy, never a transaction-mode pooler. Several
servers may share the database. A server whose build is of another
data-format major, or registers another major of an entry type, than a
server already running exits with the guard's message: such a build is
deployed stop-then-start.

## Start-up probes

The server binds its port before it opens the event store, then boots:

- `GET /livez` answers 200 as soon as the process listens.
- `GET /health` answers 503 with `{status, phase, percent, eta_s,
  elapsed_s}` while the event store boots (`phase` is `checks`,
  `promotion`, `catchUp` or `complete`, from the boot's progress reports;
  `percent` and `eta_s` are null while a phase counts no units), and 200
  with `{"status": "ready"}` once the bootstrap has returned and the routes
  are served. Every other route answers 503 until then.

Point a platform's startup and liveness probes at `/livez` and its
readiness probe at `/health`. A boot that promotes a large view or
re-derives one over a long log can take minutes, and a boot killed part-way
rolls back and pauses the database's writes again when it restarts; the
liveness probe keeps it alive, and the readiness probe keeps traffic away
until it is done. During `checks`, which also covers the wait for another
instance's boot, `elapsed_s` comes from the server's own clock.

Readiness also drops when the instance can no longer commit to its
database. A Postgres backend whose generation is no longer admitted (its
lock session was lost and a build of another major registered meanwhile) is
fenced: every transaction throws `GenerationFencedException`, and the
delivery cycle stops for good. The server then marks itself failed (any
probe still answered reads 503), stops listening and exits with status 1,
so the platform's probes fail, traffic stops reaching it and the platform
replaces it. A boot the library refuses ends the same way.

## Delivery: the demo destination and the operator routes

The server registers one destination, `server_log`, which writes every
batch it delivers to the server's standard output (`delivery: server_log
<- {...}`), and starts a delivery cycle over it. It receives the demo's
own events and the library's system events, so an operator action shows up
in the log as the halt, wedge and recovery events the library appends. The
server logs its delivery cycle's state (`running`, `standby`, `stopped`)
at start and then as sampled every 500 ms, so a lock lost and re-acquired
within one sample can go unlogged; the drain epoch in the status route's
`heartbeat` counts every acquisition.
Set `DEMO_CONFIGURATION_VERSION` to the deployment's revision identifier:
the cycle records it as part of the configuration it declares, which is
what lets a halt for reconfiguration be recovered once a revision with
another identifier drains.

Operator routes under `/demo/delivery/` require the `delivery.operate`
permission, which the permissions seed grants to `Admin`: the grant is an
event in the log, and each route asks the authorization policy for it for
the caller's `userId` (a query parameter for `GET`, a body field for
`POST`). A caller without it gets 403 and nothing is written; a registry
refusal comes back as 409 with the registry's message, and a body that is
not a JSON object, or a field of the wrong type, as 400. The caller is
recorded as the initiator of the halt, cancellation and recovery events.

| Route | Body | Does |
| --- | --- | --- |
| `GET /demo/delivery/status?userId=` | | The persisted delivery status (the drainer's declaration and heartbeat, each destination's schedule, open halt request, wedge, refill guard and unserved reason) and the rows of the default destination-wedges view. The two are separate reads, so an operation that commits between them can show in one and not the other. |
| `POST /demo/delivery/halt` | `userId`, `destinationId`, `purpose` (`pause` or `reconfigure`) | Requests a halt; the drainer honours it by wedging the queue head. |
| `POST /demo/delivery/cancel-halt` | `userId`, `destinationId` | Cancels an open halt request. |
| `POST /demo/delivery/recover` | `userId`, `destinationId`, `rowId` | Recovers the wedged head (`tombstoneAndRefill`); refused for a pending head. |
| `POST /demo/delivery/refuse-next` | `userId` | Demo fault injection, not an operator control: simulates a receiver refusal of the next send of this process's demo destination, which wedges the head with cause `permanent_refusal` and names no operator. Only the draining process accepts it; any other answers 409 and arms nothing. An operator who wants a wedge attributed to them requests a halt. |

A walkthrough, with the server on port 8080:

```text
curl -s 'localhost:8080/demo/delivery/status?userId=admin-user'
curl -s -X POST localhost:8080/demo/delivery/halt \
  -d '{"userId":"admin-user","destinationId":"server_log","purpose":"pause"}'
# the next event wedges the head; its row appears under "wedges"
curl -s 'localhost:8080/demo/delivery/status?userId=admin-user'
curl -s -X POST localhost:8080/demo/delivery/recover \
  -d '{"userId":"admin-user","destinationId":"server_log","rowId":"<row_id>"}'
```

## Several servers on one database

Start two servers against the same provisioned database, on two ports:

```text
dart run bin/server.dart --backend=postgres \
  --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
  --postgres-ssl-mode=disable --port=8080
dart run bin/server.dart --backend=postgres \
  --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
  --postgres-ssl-mode=disable --port=8081
```

The first logs `delivery cycle: running` and delivers; the second logs
`delivery cycle: standby`. At most one delivery cycle commits queue changes
for a database at a time, as far as the drain lock on each server's lock
connection reaches: the lock rests on that connection being one server
session of the database, a deployment property the library checks where it
can. Both serve every route: a halt requested through the second
server (`POST localhost:8081/demo/delivery/halt`) is honoured by the first,
the wedge shows in either server's status, and a recovery through either
server rewinds the queue, which the drainer then refills and delivers. Stop the first server (`Ctrl+C`): it closes its
delivery cycle, which releases the lock, and the second logs `delivery
cycle: running` and delivers from then on, without a restart.

What a deployment of such servers needs, stated as properties:

- Every process that starts a delivery cycle has CPU while it has no
  requests to serve: the cycle's cadence and heartbeat are timers in that
  process, and the events and halt requests other processes commit reach the
  drainer at its next pass. At least one such process runs at all times.
- A process that receives no traffic (a canary) either starts no delivery
  cycle, or may become the drainer and fill the queues under the
  configuration it declares.
- Every instance registers the same destinations, because delivery uses the
  destinations registered in the process that drains.
- The lock connection is a direct connection or a session-mode proxy, never
  a transaction-mode pooler.
- Revisions with the same data-format major and the same entry-type majors
  run side by side and can be rolled back to. A major bump is deployed
  stop-then-start: a server of a conflicting build exits at boot with the
  guard's message (`IncompatibleGenerationException`) while another runs,
  and opens once every server of the old build has stopped.

`test/drain_lock_postgres_test.dart` runs two server bootstraps in two
isolates on one database (one drains, one stands by, a halt and a recovery
through the standing-by one, the hand-over when the drainer closes), and
`test/generation_guard_postgres_test.dart` plays the stop-then-start step of
a major bump. `test/server_process_postgres_test.dart` runs the server
itself: the probes while its boot waits, then a second server standing by
and taking over when the first gets SIGTERM.

## The Postgres integration test

The integration test under `test/postgres_integration_test.dart` is the
canonical end-to-end check: it boots the demo server in-process against
the docker-compose Postgres (drops + recreates the `public` schema for
isolation), dispatches actions over HTTP, and verifies the events land
in the `events` table and the role-permission view rows land in
`view_rows`. Gated on `PG_TEST_URL`:

```text
PG_TEST_URL=postgres://evs:evs@localhost:5432/evs_demo \
  flutter test test/postgres_integration_test.dart
```

## The 10 walkthroughs

Each walkthrough below describes a hands-on scenario you can run against the
live demo. The corresponding integration test under `test/walkthroughs/`
canonicalizes the expected behavior — when the prose and the test disagree,
trust the test.

### Walkthrough 1: Onboarding (identity to principal to snapshot)

Launch the demo and pick `green-user-1` from the userId selector. Observe the
client fetch a fresh `PermissionSnapshot`, the inspector pane populate with
that user's effective matrix slice, and the action buttons in the left pane
enable/disable to match the snapshot. This is the cold-start identity flow:
identity selection drives principal resolution, which drives snapshot
delivery, which drives UI affordance.

Canonical test: `test/walkthroughs/walkthrough_01_onboarding_test.dart`.

### Walkthrough 2: Happy paths across scope classes

Still as `green-user-1`, click "Press Green Button" and "Edit Green Note".
Watch a `green_button_pressed` event, then a `green_note_edited` event,
appear in the inspector's event log. Switch to `blue-user-1` and repeat with
the blue actions. Each happy path covers a different scope class in the
permission matrix and confirms typed event emission.

Canonical test: `test/walkthroughs/walkthrough_02_happy_paths_test.dart`.

### Walkthrough 3: Matrix is the perimeter (denial paths)

As `green-user-1`, attempt "Press Blue Button". The button is disabled in
the snapshot, but if you flip the hacker-mode toggle you can fire the request
anyway. The server rejects it with an `authorization_denied` event in the
audit log, sourced from the matrix lookup. The matrix is the only thing
guarding the action — there are no per-action checks downstream.

Canonical test: `test/walkthroughs/walkthrough_03_matrix_perimeter_test.dart`.

### Walkthrough 4: Idempotency policy matrix

Fire each of three actions twice with the same idempotency key:
`press_red_alarm` (policy: required), `request_help` (policy: optional, with a
key supplied), and `press_green_button` (policy: none). Observe that the first
two collapse to a single event on replay while the third double-fires.

Canonical test:
`test/walkthroughs/walkthrough_04_idempotency_policies_test.dart`.

### Walkthrough 5: Cross-user idempotency-store independence

Fire `request_help` with key `K-42` as `green-user-1`. Switch to `blue-user-1`
and fire `request_help` with the same key `K-42`. Both succeed and emit
distinct events: the cache key is `(principalId, key)`, not `key` alone, so
keyspaces are partitioned per user.

Canonical test: `test/walkthroughs/walkthrough_05_cross_user_keys_test.dart`.

### Walkthrough 6: Identity decouples from role

Without restarting the server, switch the userId selector between
`green-user-1`, `blue-user-1`, and `red-user-1`. The snapshot, button states,
and inspector view all change in step. Identity is a runtime input to the
dispatcher, not a server-startup binding.

Canonical test:
`test/walkthroughs/walkthrough_06_identity_decoupling_test.dart`.

### Walkthrough 7: Malformed requests (parse / validation / unknown action)

Enable hacker mode and POST three deliberately broken requests: malformed
JSON, a known action with an out-of-range field, and an `actionType` the
catalog has never heard of. Each produces a distinct denial event
(`parse_failed`, `validation_failed`, `unknown_action`) with the request's
`action_invocation_id` recorded.

Canonical test:
`test/walkthroughs/walkthrough_07_malformed_requests_test.dart`.

### Walkthrough 8: Audit correlation by action_invocation_id

Pick any happy-path action and submit it. Note the `action_invocation_id`
shown in the request history panel. Open the inspector, filter the audit log
by that id, and confirm every event the dispatch produced — accept, execute,
emit — shares the same id. This is the auditor's primary correlation tool.

Canonical test:
`test/walkthroughs/walkthrough_08_audit_correlation_test.dart`.

### Walkthrough 9: User provisioning end-to-end

As an admin user, fire `provision_user` with a new userId. Watch the
`user_provisioned` event land in the audit log, the user-directory
projection update, and the new user appear in the userId selector dropdown
on next refresh. Switch to the new user and confirm a `PermissionSnapshot` is
delivered for them.

Canonical test:
`test/walkthroughs/walkthrough_09_user_provisioning_test.dart`.

### Walkthrough 10: Reset all (ephemeral restart pattern)

Stop the demo with `tool/stop_demo.sh` and restart it with
`tool/run_demo.sh`. The event store, idempotency cache, and projections all
return to seed state. There is no in-process reset endpoint — restart is the
only supported path, and restart is fast enough that this is fine.

Canonical test: `test/walkthroughs/walkthrough_10_reset_test.dart`.

## What this architecture deliberately leaves out

- Real authentication and TLS. The server trusts whatever userId the client
  sends; transport is plain HTTP on loopback.
- Aggregate-level ownership and row-level authorization. The matrix is
  global; there are no "user X owns row Y" checks.
- Reactive primitives on the client. The inspector polls; there are no
  server-sent events, websockets, or change-feed subscriptions.
- A true in-process `/_demo/reset` endpoint. Resetting state means restarting
  the process (see Walkthrough 10).
- Synthetic-burst load testing. The walkthroughs exercise correctness, not
  throughput.
- Rate limiting, CORS hardening, and production observability (structured
  logging, metrics, traces).

## Pointer

The integration tests under `test/walkthroughs/` are the canonical
specification of each scenario. The prose above is a human-readable summary
intended to orient a new reader; if the README and a test diverge, the test
wins.

## Design doc

Full design rationale and REQ traceability for the action-dispatch and
permission model this demo exercises live in `spec/prd-action-dispatch.md`
and `spec/prd-permissions-as-events.md`.
