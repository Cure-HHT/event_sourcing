# Postgres Backend Implementation Plan (CUR-1330)

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `PostgresBackend implements StorageBackend` and `PostgresIdempotencyStore implements IdempotencyStore` as a second reference impl of the substrate's persistence contracts, with a backend-agnostic conformance harness that both Sembast and Postgres backends pass, and a `--backend=postgres` switch on the existing `example_action_permissions` demo server so the impl can be exercised end-to-end.

**Architecture:** Postgres impl lives in `event_sourcing/lib/src/storage/postgres/` behind the existing abstract `StorageBackend` so no caller-side changes are required. View rows use a **JSONB-blob-per-row** representation (one row per `(view_name, row_key)`); this decision is captured in the design spec written in Task 1 and traces to memory `[[project_backend_portability]]`. Schema DDL is emitted by the substrate at backend open (`CREATE TABLE IF NOT EXISTS` for all required tables). FIFO state lives in a single `fifo_entries` table keyed by `(destination_id, sequence_in_queue)` rather than per-destination tables — cleaner DDL surface than the sembast `fifo_<destinationId>` store pattern, equivalent observable behavior. Transactions map to Postgres `BEGIN ... COMMIT`/`ROLLBACK` at `SERIALIZABLE` isolation so the per-device sequence counter advances safely under concurrent writers.

**Tech Stack:** Dart 3.10.7+; `package:postgres` v3.x (canonical Dart Postgres client); existing `package:test` for VM-only conformance tests; Docker (developer-machine) + a `postgres:16` service container for CI.

**Spec context:** This is the first activation of the deferred `[[project_backend_portability]]` memory entry — set its status to `in progress on CUR-1330` when Task 1 lands. Substrate commitments preserved verbatim: domain-neutral; declarative projections; permission policy as substrate code; library version recorded in the log; trust boundaries enumerated (no new trust surface introduced — `PostgresBackend` is just an alternate `StorageBackend`). The view-row representation decision sits at **Layer 2** of the [epistemic-layers](../../CLAUDE.md#epistemic-layers) split — it is the library's chosen *interpretation* of how to materialize `TableProjectionSpec` outputs on SQL, not a Layer 1 fact.

---

## Common Preamble (read once per task)

### Working directory

`/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1330-postgres-backend` (this worktree).

### Branch

Plan executes on the current branch `CUR-1330-postgres-backend`. Commit per task; squash on merge. The Linear-suggested branch name `michael/cur-1330-...` is fine to ignore — this repo's convention is `CUR-NNNN-<kebab>` with no user prefix (see CLAUDE.md "Conventions").

### Commit message style

Free-form body. PR title MUST start with `[CUR-1330]` (org-level ruleset enforces this). Suggested commit subject form: `[CUR-1330] <task summary>`.

### Pre-commit / pre-push

The pre-commit hook handles whitespace and secret scan. The pre-push hook runs `elspais checks --lenient` — your DEV requirement edits need to refer only to existing IDs and your `// Implements:` / `// Verifies:` annotations need to resolve.

### Trust boundaries

`PostgresBackend` is a `StorageBackend` impl — it inherits the existing trust budget (it IS the trusted persistence layer when registered). It does NOT expand the trust surface. No new pluggable interfaces. Connection strings carry credentials but those are caller-supplied, same as `databaseFactory` for sembast.

### Test gating

Conformance tests against Postgres need a running database. Pattern: tests `skip` themselves when `PG_TEST_URL` is unset (no spurious local failures); CI sets the variable. Local-dev `docker-compose.yml` provided in Task 2 makes it one command to bring up.

### TDD discipline

Each impl task lands the conformance harness assertions first (they fail), then the implementation (they pass). The conformance harness is the authoritative spec for the contract — sembast tests added historically that exercise contract behavior get lifted into the harness in Task 3 so they run against both backends.

### Idiom: SQL inside Dart

Use parameterized queries via `package:postgres`'s `Sql.named(...)` or positional `\$1, \$2` form. Never interpolate values into SQL. Identifiers (table names, view names registered by callers) are either compile-time constants or validated by a `_quoteIdent` helper that rejects anything outside `[a-zA-Z0-9_]+` — this prevents injection at the only place where dynamic identifiers appear (per-view-name table names if we ever move off JSONB, and `destinationId` if it ever forms part of a constraint name).

---

## File Structure

**Create:**

- `event_sourcing/lib/src/storage/postgres/postgres_backend.dart` — concrete `StorageBackend` impl.
- `event_sourcing/lib/src/storage/postgres/postgres_txn.dart` — `Txn` subclass holding a `package:postgres` `TxSession`.
- `event_sourcing/lib/src/storage/postgres/postgres_schema.dart` — DDL emission (`CREATE TABLE IF NOT EXISTS ...`), plus the schema-version constant and the migration enumeration.
- `event_sourcing/lib/src/storage/postgres/postgres_sql.dart` — SQL string constants + small helpers (`_quoteIdent`, `_jsonbEncode`).
- `event_sourcing/lib/src/storage/postgres/postgres_idempotency_store.dart` — `IdempotencyStore` impl.
- `event_sourcing/lib/src/storage/postgres/postgres.dart` — public barrel for the four files above (the only path apps import).
- `event_sourcing/test/storage/storage_backend_conformance.dart` — backend-agnostic conformance harness; exports `void runStorageBackendConformanceTests(Future<StorageBackend> Function() factory)`.
- `event_sourcing/test/storage/idempotency_store_conformance.dart` — same for `IdempotencyStore`.
- `event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart` — calls the harness with a Postgres factory; gated on `PG_TEST_URL`.
- `event_sourcing/test/storage/postgres/postgres_idempotency_store_conformance_test.dart` — same pattern.
- `event_sourcing/test/storage/postgres/postgres_backend_schema_test.dart` — Postgres-specific: verifies idempotent schema emission and the migration cursor.
- `spec/dev-postgres-backend.md` — DEV-level requirement, multi-assertion, traced by impl + tests.
- `docs/superpowers/specs/2026-05-12-postgres-backend-design.md` — design brainstorm (migrates to `spec/postgres-backend.md` when stabilized; lifecycle per `spec/README.md`).
- `event_sourcing/example_action_permissions/docker-compose.yml` — postgres:16 service for local example runs.
- `.github/workflows/conformance-tests.yml` — CI job with Postgres service container.

**Modify:**

- `event_sourcing/pubspec.yaml` — add `postgres: ^3.x` to `dependencies`.
- `event_sourcing/lib/event_sourcing.dart` — export the postgres barrel.
- `event_sourcing/example_action_permissions/bin/server.dart` — add `--backend=postgres`, `--postgres-url` flags; route to either `SembastBackend` or `PostgresBackend` based on the flag.
- `event_sourcing/example_action_permissions/lib/server/bootstrap.dart` — change signature to accept a `StorageBackend` directly (callers in `bin/server.dart` construct the backend).
- `event_sourcing/example_action_permissions/lib/server/demo_idempotency_store.dart` — make it accept an injected `IdempotencyStore` so the demo can swap to `PostgresIdempotencyStore`.
- `CLAUDE.md` — small addition under "What this repo is" noting Postgres impl alongside sembast.
- Memory `[[project_backend_portability]]` — flip status from "deferred" to "in progress on CUR-1330".

---

## Subsystem → Requirement Mapping

| Task | New code | Annotations / requirements |
|------|----------|-----------------------------|
| 1 | `spec/dev-postgres-backend.md` | new EVS-DEV |
| 2 | pubspec, docker-compose | — |
| 3 | conformance harness | `// Verifies:` against existing `EVS-PRD-portability/D`, `EVS-PRD-event-log/*` |
| 4 | schema emission | `// Implements: EVS-DEV-postgres-backend/A` (schema emission) |
| 5 | txn + skeleton | `// Implements: EVS-PRD-event-log/A` (transactional atomicity) |
| 6 | events | `// Implements: EVS-PRD-event-log/A,B,C,D` + `EVS-DEV-find-all-events-extended-filters/A,B,C` |
| 7 | view rows | `// Implements: EVS-DEV-postgres-backend/B` (JSONB view rows) |
| 8 | view target versions | `// Implements: EVS-DEV-view-target-versions-seeding` (callers) |
| 9 | FIFO | `// Implements: EVS-PRD-destinations/*` (review which assertions; expect at least the FIFO ordering + wedged behavior assertions) |
| 10 | backend state KV | `// Implements: EVS-PRD-event-log/B` (sequence counter durability) |
| 11 | audit + reverse scan | `// Implements: EVS-PRD-regulatory-alignment/*` (audit join) |
| 12 | idempotency store | `// Implements: EVS-PRD-action-dispatch/D` |
| 13 | example extension | — (demo wiring, no new requirements) |
| 14 | CI | — |

---

## Task 1: Design spec + DEV requirement

**Files:**

- Create: `docs/superpowers/specs/2026-05-12-postgres-backend-design.md`
- Create: `spec/dev-postgres-backend.md`
- Modify: `spec/INDEX.md` (regenerated via elspais; do not hand-edit unless the regen step is broken)
- Modify: `[[project_backend_portability]]` memory — flip status to "in progress on CUR-1330"

- [ ] **Step 1: Write the brainstorm design spec capturing the JSONB-blob decision and the architecture.**

The spec should follow the format used by `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md` — prose-heavy, exploratory, captures rationale for decisions taken and decisions rejected. Sections to include:

```markdown
# Postgres Backend Design (CUR-1330)

**Status:** Brainstorm; will migrate to spec/postgres-backend.md when stable.

## Why now

(Phase IV portal+server cutover; first activation of the deferred
backend-portability memory; the substrate has always been backend-agnostic
behind StorageBackend, and the moment to prove that with a second impl has
arrived.)

## Open architectural decision: view-row representation

Three candidates considered (JSONB blob; per-spec typed cols; hybrid).
Chosen: **JSONB blob per row** (single table `view_rows(view_name, row_key,
row_data JSONB, updated_at)`).

Tradeoff: SQL-native queries on view contents go through JSONB operators
(`row_data->>'field'`) rather than typed columns. The portal-side consumers
of view rows query through the substrate's `findViewRows` API today; they
do not reach past the abstraction to query the table directly. Choosing
JSONB now commits to that pattern.

Future migration path to per-spec typed cols (or hybrid): if a downstream
deployment needs SQL-native view queries, add a new
`SqlNativeTableProjectionSpec` primitive under the Append-Only Primitives
discipline. The JSONB-blob layout stays as the default for
`TableProjectionSpec`.

## Layer 1 vs Layer 2 framing

The JSONB-blob decision is a **Layer 2 convention** (per CLAUDE.md
"Epistemic layers"). It is the library's chosen interpretation of how to
materialize `TableProjectionSpec` outputs on Postgres. Applications
needing different materializations build them on top of Layer 1 facts via
`subscribe<T>(_, Events())` or `EventStore.read(...)`, or via future
substrate primitives.

## Schema overview

(One-paragraph summary of each table; full DDL in postgres_schema.dart;
this section is the narrative.)

## Transactional model

- `StorageBackend.transaction<T>` -> Postgres `BEGIN ... COMMIT` at
  SERIALIZABLE isolation.
- Sequence counter is single-row in `backend_state`; the SERIALIZABLE
  isolation makes concurrent `nextSequenceNumber` calls serialize as
  expected. Substrate is single-writer-per-source by design; this just
  prevents accidental concurrent writers from silently corrupting the
  chain.

## What's the same as sembast

(Same StorageBackend interface; same StoredEvent shape; same FIFO
semantics; same backend_state KV bookkeeping.)

## What's different from sembast

- View rows are JSONB blobs in a single `view_rows` table, not one store
  per view name.
- FIFO is one `fifo_entries` table keyed by `(destination_id,
  sequence_in_queue)`, not per-destination tables.
- Schema DDL is emitted at backend `open()` time as
  `CREATE TABLE IF NOT EXISTS` statements; sembast creates stores
  lazily on first write.

## Decisions rejected

- Per-spec typed columns for view rows: rejected for v1 (deferred to a
  future SqlNativeTableProjectionSpec primitive).
- Per-destination FIFO tables: rejected; cleaner DDL surface with a
  single table; observable behavior unchanged.
- READ COMMITTED transactions: rejected; risk of phantom-read corrupting
  the sequence counter under concurrent writers.

## Open questions

- Reactive change streams (sembast emits StoredEvent on a
  StreamController.broadcast after each commit). Postgres impl skips
  these for v1 — they're not in the abstract StorageBackend contract.
  `subscribe<T>` over Postgres will need polling or `LISTEN`/`NOTIFY`;
  follow-up ticket.
- Connection pool sizing for the portal load profile — out of scope for
  this ticket.
```

- [ ] **Step 2: Write the DEV-level requirement at `spec/dev-postgres-backend.md`.**

Match the format of `spec/dev-find-all-events-extended-filters.md`. Assertions to author:

```markdown
# EVS-DEV-postgres-backend: Postgres backend reference impl

**Level**: dev | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-portability, EVS-PRD-event-log

## Purpose

A second `StorageBackend` implementation alongside `SembastBackend`, targeting
server-side deployments (Cloud SQL / managed Postgres). Demonstrates that the
substrate's persistence contract is backend-agnostic, and provides the
storage layer for the portal-server / diary-server / portal-ui phase-IV
cutover.

## Assertions

A. `PostgresBackend.open` SHALL emit `CREATE TABLE IF NOT EXISTS` DDL for
   every table the backend reads or writes, idempotently. Re-opening an
   already-provisioned database SHALL be a no-op on the schema.

B. The backend SHALL store view rows as JSONB blobs in a single
   `view_rows(view_name TEXT, row_key TEXT, row_data JSONB, updated_at
   TIMESTAMPTZ)` table, with `PRIMARY KEY (view_name, row_key)`.

C. `PostgresBackend.transaction<T>(body)` SHALL execute `body` inside a
   single Postgres transaction at SERIALIZABLE isolation. On any thrown
   exception the transaction SHALL be rolled back; on normal return it
   SHALL be committed. The `Txn` handle passed to `body` SHALL be
   invalidated after `body` returns or throws.

D. Both `PostgresBackend` and `SembastBackend` SHALL pass the conformance
   harness in `event_sourcing/test/storage/storage_backend_conformance.dart`.

E. `PostgresIdempotencyStore` SHALL persist entries in an `idempotency` table
   keyed by `(action_name, principal_id, idempotency_key)`, with the
   policy semantics (`none / optional / required`) enforced by the
   substrate's action-dispatch path, not by the store.

F. `PostgresIdempotencyStore` SHALL pass the conformance harness in
   `event_sourcing/test/storage/idempotency_store_conformance.dart` and the
   `InMemoryIdempotencyStore` SHALL pass it too.

## Rationale

**Why JSONB-blob for view rows?** See design spec
`docs/superpowers/specs/2026-05-12-postgres-backend-design.md`. Briefly:
closest fit to sembast semantics; minimal DDL evolution machinery; the
portal-side consumers query view rows through the substrate's
`findViewRows` API, not directly against the table.

**Why a single `fifo_entries` table?** Cleaner DDL surface than the
sembast `fifo_<destinationId>` store-per-destination layout; observable
behavior is identical because the substrate iterates FIFOs through
`StorageBackend` methods only.

**Why SERIALIZABLE?** The per-device sequence counter is a single row;
SERIALIZABLE isolation guarantees that concurrent `nextSequenceNumber`
calls cannot both read the same value and stamp two events with the same
sequence number. The substrate is single-writer-per-source by design
but the storage layer should not assume the caller has external
synchronization.
```

- [ ] **Step 3: Regenerate `spec/INDEX.md` via elspais.**

Run: `dart run elspais refresh` (or whatever the project's refresh command is — check `.elspais.toml` or `CLAUDE.md`). Expected: the new `dev-postgres-backend.md` appears in `spec/INDEX.md`.

- [ ] **Step 4: Flip the memory.**

Edit `/home/metagamer/.claude/projects/-home-metagamer-cure-hht-event-sourcing/memory/project_backend_portability.md`: change status from "deferred" to "in progress on CUR-1330" and note the chosen view-row representation (JSONB blob per row). The MEMORY.md index line description does not need to change.

- [ ] **Step 5: Commit.**

```bash
git add docs/superpowers/specs/2026-05-12-postgres-backend-design.md \
        spec/dev-postgres-backend.md \
        spec/INDEX.md
git commit -m "[CUR-1330] Design spec + EVS-DEV-postgres-backend requirement"
```

---

## Task 2: Add postgres dependency and local dev infrastructure

**Files:**

- Modify: `event_sourcing/pubspec.yaml`
- Create: `event_sourcing/example_action_permissions/docker-compose.yml`
- Create: `event_sourcing/test/storage/postgres/test_postgres_url.dart` (helper)

- [ ] **Step 1: Add `postgres` to `event_sourcing/pubspec.yaml` dependencies.**

Insert under existing `dependencies:`:

```yaml
  postgres: ^3.5.0
```

Run `dart pub get` from `event_sourcing/`. Expected: success, lock file updated.

- [ ] **Step 2: Create a docker-compose for local development at `event_sourcing/example_action_permissions/docker-compose.yml`.**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: evs
      POSTGRES_PASSWORD: evs
      POSTGRES_DB: evs_demo
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U evs -d evs_demo"]
      interval: 2s
      timeout: 2s
      retries: 20
```

- [ ] **Step 3: Create a test helper at `event_sourcing/test/storage/postgres/test_postgres_url.dart`.**

```dart
import 'dart:io' show Platform;

/// Returns the Postgres URL the conformance harness should connect to,
/// or `null` when the test environment has not provided one. Tests that
/// receive `null` SHALL skip themselves rather than fail.
String? testPostgresUrl() {
  final url = Platform.environment['PG_TEST_URL'];
  if (url == null || url.isEmpty) return null;
  return url;
}
```

- [ ] **Step 4: Verify nothing broke.**

Run: `cd event_sourcing && dart pub get && dart analyze`
Expected: zero issues.

- [ ] **Step 5: Commit.**

```bash
git add event_sourcing/pubspec.yaml \
        event_sourcing/pubspec.lock \
        event_sourcing/example_action_permissions/docker-compose.yml \
        event_sourcing/test/storage/postgres/test_postgres_url.dart
git commit -m "[CUR-1330] Add postgres dep and local docker-compose"
```

---

## Task 3: Lift sembast tests into backend-agnostic conformance harness

**Files:**

- Create: `event_sourcing/test/storage/storage_backend_conformance.dart`
- Modify: existing sembast contract tests to delegate into the harness
- Keep: sembast-specific tests (e.g., watch_*.dart, sembast-internal store layout) unchanged

**Approach:** the existing `storage_backend_contract_test.dart` already operates against the abstract `StorageBackend` via a fake. Lift its body and the contract-level assertions from `sembast_backend_event_test.dart`, `sembast_backend_fifo_test.dart`, `find_all_events_filters_test.dart`, `find_all_events_originator_filter_test.dart`, `storage_backend_views_test.dart`, `view_target_versions_test.dart`, `markfinal_idempotency_test.dart`, `sembast_backend_event_by_id_test.dart` into the harness. The reactive-watch tests stay sembast-specific because the abstract contract does not include reactive streams.

- [ ] **Step 1: Write the harness skeleton.**

```dart
// event_sourcing/test/storage/storage_backend_conformance.dart

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:test/test.dart';

/// Run the full StorageBackend conformance suite against the backend
/// produced by [factory]. Every concrete `StorageBackend` impl SHALL
/// call this from its own test file and pass.
///
/// The factory SHALL return a fresh, empty backend each invocation —
/// it is called in each test's `setUp`. The factory MAY return null
/// to skip the entire suite (used by the Postgres harness when
/// PG_TEST_URL is unset).
void runStorageBackendConformanceTests(
  Future<StorageBackend?> Function() factory, {
  required String backendLabel,
}) {
  group('StorageBackend conformance ($backendLabel)', () {
    late StorageBackend backend;

    setUp(() async {
      final candidate = await factory();
      if (candidate == null) {
        markTestSkipped('no backend available (factory returned null)');
        return;
      }
      backend = candidate;
    });

    tearDown(() async {
      // Cast guarded — setUp may have skipped.
      try {
        await backend.close();
      } catch (_) {/* ignore in teardown */}
    });

    _registerTransactionTests(() => backend);
    _registerEventLogTests(() => backend);
    _registerFindAllEventsFilterTests(() => backend);
    _registerViewRowTests(() => backend);
    _registerViewTargetVersionTests(() => backend);
    _registerFifoTests(() => backend);
    _registerBackendStateTests(() => backend);
    _registerAuditQueryTests(() => backend);
  });
}

// One `void _registerXxxTests(StorageBackend Function() backendOf)` group
// per subsystem. Each lifted from the corresponding sembast test file,
// rewritten to call `backendOf()` instead of `backend` (so the closure
// captures the late variable safely).
```

- [ ] **Step 2: Lift transaction tests from `storage_backend_contract_test.dart`.**

The existing tests (`successful body commits all writes`, `thrown exception rolls back all writes`, `mid-body throw rolls back earlier writes too`, `Txn cannot be used after body returns`, `Txn cannot be used after body throws`, `sequential transactions: second transaction sees first commit`) move verbatim into `_registerTransactionTests`, with `backend` references replaced by `backendOf()`.

Replace `_InMemoryBackend` references with `backendOf()`. The `nested transaction on this fake throws` test stays in `storage_backend_contract_test.dart` because the contract is silent on nested-txn behavior (the test is a sembast-and-fake quirk, not contract).

- [ ] **Step 3: Lift event-log tests (append, find, sequence counter, hash chain reads).**

From `sembast_backend_event_test.dart`. Each test that does NOT touch sembast-internal store layout moves into the harness.

- [ ] **Step 4: Lift the filter tests.**

`find_all_events_filters_test.dart` and `find_all_events_originator_filter_test.dart` are pure-contract (they parameterize on `StorageBackend`). Move bodies into `_registerFindAllEventsFilterTests`.

- [ ] **Step 5: Lift view-row tests.**

From `storage_backend_views_test.dart`. Same approach.

- [ ] **Step 6: Lift view-target-version tests.**

From `view_target_versions_test.dart`.

- [ ] **Step 7: Lift FIFO contract tests.**

From `sembast_backend_fifo_test.dart` and `markfinal_idempotency_test.dart`. Skip sembast-internal store-layout assertions; keep behavioral.

- [ ] **Step 8: Lift backend-state tests.**

Schema version, fill cursor, schedules — from `sembast_backend_fifo_test.dart` and `sembast_backend_event_by_id_test.dart`.

- [ ] **Step 9: Lift audit-query tests.**

From wherever they live today. Check `find_all_events_originator_filter_test.dart` and security_context tests.

- [ ] **Step 10: Update the sembast test entrypoint to call the harness.**

Replace the body of `sembast_backend_event_test.dart` (and similar) with:

```dart
// event_sourcing/test/storage/sembast_backend_conformance_test.dart (new)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:sembast/sembast_memory.dart';
import 'package:test/test.dart';

import 'storage_backend_conformance.dart';

void main() {
  runStorageBackendConformanceTests(
    () async {
      final db = await databaseFactoryMemory.openDatabase('conformance');
      return SembastBackend(database: db);
    },
    backendLabel: 'sembast (memory)',
  );
}
```

Leave the existing test files that exercise sembast-internal behavior (watch streams; store-layout assertions) untouched.

- [ ] **Step 11: Run the sembast conformance suite to confirm parity with the old tests.**

```bash
cd event_sourcing && dart test test/storage/sembast_backend_conformance_test.dart -p vm
```

Expected: all tests pass (same coverage as before, now via the harness).

- [ ] **Step 12: Commit.**

```bash
git add event_sourcing/test/storage/storage_backend_conformance.dart \
        event_sourcing/test/storage/sembast_backend_conformance_test.dart \
        event_sourcing/test/storage/storage_backend_contract_test.dart \
        event_sourcing/test/storage/sembast_backend_*.dart \
        event_sourcing/test/storage/find_all_events_*.dart \
        event_sourcing/test/storage/storage_backend_views_test.dart \
        event_sourcing/test/storage/view_target_versions_test.dart \
        event_sourcing/test/storage/markfinal_idempotency_test.dart
git commit -m "[CUR-1330] Lift sembast contract tests into backend-agnostic conformance harness"
```

---

## Task 4: Postgres schema DDL

**Files:**

- Create: `event_sourcing/lib/src/storage/postgres/postgres_schema.dart`
- Create: `event_sourcing/test/storage/postgres/postgres_backend_schema_test.dart`

- [ ] **Step 1: Write the failing schema test.**

```dart
// event_sourcing/test/storage/postgres/postgres_backend_schema_test.dart

@TestOn('vm')
import 'package:event_sourcing/src/storage/postgres/postgres.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }

  group('PostgresBackend schema', () {
    late Connection conn;

    setUp(() async {
      conn = await _connect(url);
      // Clean slate every test.
      await conn.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
    });

    tearDown(() async {
      await conn.close();
    });

    test('open() emits CREATE TABLE IF NOT EXISTS for every expected table',
        () async {
      final backend = await PostgresBackend.open(url: url);
      addTearDown(backend.close);

      final tables = await _listPublicTables(conn);
      expect(tables, containsAll(<String>[
        'events',
        'view_rows',
        'view_target_versions',
        'fifo_entries',
        'backend_state',
        'security_context',
        'idempotency',
      ]));
    });

    test('open() is idempotent (second open is a no-op on schema)', () async {
      final b1 = await PostgresBackend.open(url: url);
      await b1.close();
      final b2 = await PostgresBackend.open(url: url);
      addTearDown(b2.close);
      // No throw is the assertion. Implicitly: second open SHALL not
      // raise "relation already exists".
    });
  });
}

Future<Connection> _connect(String url) async {
  final uri = Uri.parse(url);
  return Connection.open(
    Endpoint(
      host: uri.host,
      port: uri.port == 0 ? 5432 : uri.port,
      database: uri.pathSegments.first,
      username: uri.userInfo.split(':').first,
      password: uri.userInfo.contains(':')
          ? uri.userInfo.split(':')[1]
          : null,
    ),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
}

Future<List<String>> _listPublicTables(Connection conn) async {
  final result = await conn.execute(
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = 'public' ORDER BY table_name",
  );
  return result.map((row) => row[0] as String).toList();
}
```

- [ ] **Step 2: Verify the test fails.**

Run with PG_TEST_URL set:

```bash
PG_TEST_URL=postgres://evs:evs@localhost:5432/evs_demo \
  cd event_sourcing && dart test test/storage/postgres/postgres_backend_schema_test.dart -p vm
```

Expected: FAIL with "PostgresBackend.open is undefined" (or similar).

- [ ] **Step 3: Write the schema module.**

```dart
// event_sourcing/lib/src/storage/postgres/postgres_schema.dart

import 'package:postgres/postgres.dart';

const int postgresBackendSchemaVersion = 1;

/// Emit CREATE TABLE IF NOT EXISTS for every table the backend uses.
/// Idempotent: running this against an already-provisioned database is
/// a no-op on the schema.
// Implements: EVS-DEV-postgres-backend/A — idempotent schema emission.
Future<void> ensurePostgresSchema(Session session) async {
  await session.execute(_eventsTable);
  await session.execute(_eventsIndexEventId);
  await session.execute(_eventsIndexAggregate);
  await session.execute(_eventsIndexClientTs);
  await session.execute(_viewRowsTable);
  await session.execute(_viewTargetVersionsTable);
  await session.execute(_fifoEntriesTable);
  await session.execute(_fifoIndexHead);
  await session.execute(_backendStateTable);
  await session.execute(_securityContextTable);
  await session.execute(_idempotencyTable);
}

const _eventsTable = r'''
CREATE TABLE IF NOT EXISTS events (
  sequence_number      BIGINT       PRIMARY KEY,
  event_id             TEXT         NOT NULL UNIQUE,
  aggregate_id         TEXT         NOT NULL,
  aggregate_type       TEXT         NOT NULL,
  entry_type           TEXT         NOT NULL,
  entry_type_version   INTEGER      NOT NULL,
  lib_format_version   INTEGER      NOT NULL,
  event_type           TEXT         NOT NULL,
  data                 JSONB        NOT NULL,
  metadata             JSONB        NOT NULL,
  initiator            JSONB        NOT NULL,
  client_timestamp     TIMESTAMPTZ  NOT NULL,
  event_hash           TEXT         NOT NULL,
  flow_token           TEXT,
  previous_event_hash  TEXT
)
''';

const _eventsIndexEventId =
    'CREATE INDEX IF NOT EXISTS events_event_id_idx ON events (event_id)';

const _eventsIndexAggregate = '''
CREATE INDEX IF NOT EXISTS events_aggregate_idx
  ON events (aggregate_id, sequence_number)
''';

const _eventsIndexClientTs = '''
CREATE INDEX IF NOT EXISTS events_client_ts_idx
  ON events (client_timestamp)
''';

const _viewRowsTable = r'''
CREATE TABLE IF NOT EXISTS view_rows (
  view_name   TEXT         NOT NULL,
  row_key     TEXT         NOT NULL,
  row_data    JSONB        NOT NULL,
  updated_at  TIMESTAMPTZ  NOT NULL,
  PRIMARY KEY (view_name, row_key)
)
''';

const _viewTargetVersionsTable = r'''
CREATE TABLE IF NOT EXISTS view_target_versions (
  view_name       TEXT     NOT NULL,
  entry_type      TEXT     NOT NULL,
  target_version  INTEGER  NOT NULL,
  PRIMARY KEY (view_name, entry_type)
)
''';

const _fifoEntriesTable = r'''
CREATE TABLE IF NOT EXISTS fifo_entries (
  destination_id      TEXT          NOT NULL,
  sequence_in_queue   BIGINT        NOT NULL,
  entry_id            TEXT          NOT NULL UNIQUE,
  event_ids           JSONB         NOT NULL,
  event_id_first_seq  BIGINT        NOT NULL,
  event_id_last_seq   BIGINT        NOT NULL,
  wire_format         TEXT          NOT NULL,
  transform_version   INTEGER       NOT NULL,
  enqueued_at         TIMESTAMPTZ   NOT NULL,
  attempts            JSONB         NOT NULL,
  final_status        TEXT,
  sent_at             TIMESTAMPTZ,
  wire_payload        JSONB,
  envelope_metadata   JSONB,
  PRIMARY KEY (destination_id, sequence_in_queue)
)
''';

const _fifoIndexHead = '''
CREATE INDEX IF NOT EXISTS fifo_entries_head_idx
  ON fifo_entries (destination_id, sequence_in_queue)
  WHERE final_status IS NULL OR final_status = 'wedged'
''';

const _backendStateTable = r'''
CREATE TABLE IF NOT EXISTS backend_state (
  key    TEXT   PRIMARY KEY,
  value  JSONB  NOT NULL
)
''';

const _securityContextTable = r'''
CREATE TABLE IF NOT EXISTS security_context (
  event_id     TEXT         PRIMARY KEY,
  recorded_at  TIMESTAMPTZ  NOT NULL,
  ip_address   TEXT,
  payload      JSONB        NOT NULL
)
''';

const _idempotencyTable = r'''
CREATE TABLE IF NOT EXISTS idempotency (
  action_name        TEXT         NOT NULL,
  principal_id       TEXT         NOT NULL,
  idempotency_key    TEXT         NOT NULL,
  result_json        JSONB        NOT NULL,
  emitted_event_ids  JSONB        NOT NULL,
  recorded_at        TIMESTAMPTZ  NOT NULL,
  expires_at         TIMESTAMPTZ  NOT NULL,
  PRIMARY KEY (action_name, principal_id, idempotency_key)
)
''';
```

- [ ] **Step 4: Stub `PostgresBackend.open` enough to make the test pass.**

In a new file `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`, write the minimal skeleton:

```dart
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart';
import 'package:postgres/postgres.dart';

class PostgresBackend extends StorageBackend {
  PostgresBackend._(this._pool);
  final Pool _pool;

  static Future<PostgresBackend> open({required String url}) async {
    final uri = Uri.parse(url);
    final endpoint = Endpoint(
      host: uri.host,
      port: uri.port == 0 ? 5432 : uri.port,
      database: uri.pathSegments.first,
      username: uri.userInfo.isEmpty
          ? null
          : uri.userInfo.split(':').first,
      password: (uri.userInfo.contains(':'))
          ? uri.userInfo.split(':')[1]
          : null,
    );
    final pool = Pool.withEndpoints([endpoint],
        settings: const PoolSettings(maxConnectionCount: 4));
    final backend = PostgresBackend._(pool);
    await pool.runTx((tx) async {
      await ensurePostgresSchema(tx);
    });
    return backend;
  }

  @override
  Future<void> close() => _pool.close();

  // All other StorageBackend methods throw UnimplementedError for now.
  // Subsequent tasks implement them one subsystem at a time.
  // ... noStuchMethod or explicit @override stubs per the analyzer needs.
}
```

Add a barrel `event_sourcing/lib/src/storage/postgres/postgres.dart`:

```dart
export 'postgres_backend.dart';
export 'postgres_idempotency_store.dart';
```

Add an export line in `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/storage/postgres/postgres.dart';
```

- [ ] **Step 5: Run the schema test.**

```bash
PG_TEST_URL=postgres://evs:evs@localhost:5432/evs_demo \
  cd event_sourcing && dart test test/storage/postgres/postgres_backend_schema_test.dart -p vm
```

Expected: both tests PASS.

- [ ] **Step 6: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_schema.dart \
        event_sourcing/lib/src/storage/postgres/postgres_backend.dart \
        event_sourcing/lib/src/storage/postgres/postgres.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/storage/postgres/postgres_backend_schema_test.dart
git commit -m "[CUR-1330] PostgresBackend schema DDL + idempotent open()"
```

---

## Task 5: Transactions + Txn

**Files:**

- Create: `event_sourcing/lib/src/storage/postgres/postgres_txn.dart`
- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`
- Modify: `event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart` (call the harness; the transaction tests inside the harness become the failing tests we drive against)

- [ ] **Step 1: Write the conformance test entrypoint.**

```dart
// event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart

@TestOn('vm')
import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../storage_backend_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  runStorageBackendConformanceTests(
    () async {
      if (url == null) return null;
      // Fresh database each test: drop+recreate public schema so view_rows
      // / events / fifo / etc. are empty.
      final endpoint = _endpointOf(url);
      final tmp = await Connection.open(endpoint,
          settings: const ConnectionSettings(sslMode: SslMode.disable));
      await tmp.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
      await tmp.close();
      return PostgresBackend.open(url: url);
    },
    backendLabel: 'postgres',
  );
}
```

- [ ] **Step 2: Run — expect the transaction tests to fail.**

```bash
PG_TEST_URL=postgres://evs:evs@localhost:5432/evs_demo \
  cd event_sourcing && dart test test/storage/postgres/postgres_backend_conformance_test.dart -p vm -N 'StorageBackend conformance (postgres)/transaction'
```

Expected: FAIL (transaction tests run against unimplemented backend).

- [ ] **Step 3: Implement `PostgresTxn` and `PostgresBackend.transaction`.**

```dart
// event_sourcing/lib/src/storage/postgres/postgres_txn.dart

import 'package:event_sourcing/src/storage/txn.dart';
import 'package:postgres/postgres.dart';

class PostgresTxn extends Txn {
  PostgresTxn(this._session);
  final TxSession _session;
  bool _valid = true;

  TxSession get session {
    if (!_valid) {
      throw StateError('Txn used outside its transaction() body');
    }
    return _session;
  }

  void invalidate() {
    _valid = false;
  }
}
```

In `postgres_backend.dart`:

```dart
@override
Future<T> transaction<T>(Future<T> Function(Txn txn) body) async {
  return _pool.runTx<T>(
    (tx) async {
      final wrapper = PostgresTxn(tx);
      try {
        return await body(wrapper);
      } finally {
        wrapper.invalidate();
      }
    },
    settings: const TransactionSettings(isolationLevel: IsolationLevel.serializable),
  );
}
```

- [ ] **Step 4: Run the transaction subgroup.**

Same command as Step 2. Expected: 6 transaction tests PASS, all others still FAIL (UnimplementedError).

- [ ] **Step 5: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_txn.dart \
        event_sourcing/lib/src/storage/postgres/postgres_backend.dart \
        event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart
git commit -m "[CUR-1330] PostgresBackend transaction() + PostgresTxn (serializable)"
```

---

## Task 6: Event log — append, find, sequence counter

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`
- Create: `event_sourcing/lib/src/storage/postgres/postgres_sql.dart` (helpers)

Methods this task implements: `appendEvent`, `findEventsForAggregate`, `findEventsForAggregateInTxn`, `findAllEvents`, `findAllEventsInTxn`, `readLatestEventHash`, `nextSequenceNumber`, `readSequenceCounter`, `findEventByIdInTxn`, `findEventById`.

- [ ] **Step 1: Confirm the event-log subgroup of the conformance harness fails.**

```bash
PG_TEST_URL=... dart test ... -N 'StorageBackend conformance (postgres)/(event log|find_all_events|findEventById)'
```

Expected: FAIL.

- [ ] **Step 2: Implement `nextSequenceNumber` and `readSequenceCounter`.**

The counter lives in `backend_state` under key `sequence_counter`. `nextSequenceNumber` does `UPDATE backend_state SET value = (value::int + 1)::text::jsonb WHERE key = 'sequence_counter' RETURNING (value::int)`, with an `INSERT ON CONFLICT DO NOTHING` for first-call initialization.

```dart
@override
Future<int> nextSequenceNumber(Txn txn) async {
  final session = _asPgTxn(txn).session;
  // Initialize the row to 0 if absent.
  await session.execute(
    Sql.named("""
      INSERT INTO backend_state (key, value) VALUES ('sequence_counter', '0'::jsonb)
      ON CONFLICT (key) DO NOTHING
    """),
  );
  // Atomically increment and return the new value.
  final result = await session.execute(
    Sql.named("""
      UPDATE backend_state
        SET value = ((value::text::int) + 1)::text::jsonb
        WHERE key = 'sequence_counter'
        RETURNING value::text::int AS new_seq
    """),
  );
  return result.first[0] as int;
}

@override
Future<int> readSequenceCounter() async {
  final result = await _pool.execute(
    "SELECT value::text::int FROM backend_state WHERE key = 'sequence_counter'",
  );
  if (result.isEmpty) return 0;
  return result.first[0] as int;
}

PostgresTxn _asPgTxn(Txn txn) {
  if (txn is! PostgresTxn) {
    throw StateError('Txn does not belong to this backend (PostgresBackend)');
  }
  return txn;
}
```

- [ ] **Step 3: Implement `appendEvent`.**

```dart
@override
Future<AppendResult> appendEvent(Txn txn, StoredEvent event) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO events (
        sequence_number, event_id, aggregate_id, aggregate_type, entry_type,
        entry_type_version, lib_format_version, event_type,
        data, metadata, initiator,
        client_timestamp, event_hash, flow_token, previous_event_hash
      ) VALUES (
        @seq, @eventId, @aggId, @aggType, @entryType,
        @entryTypeV, @libFmtV, @eventType,
        @data::jsonb, @metadata::jsonb, @initiator::jsonb,
        @clientTs, @eventHash, @flowToken, @prevHash
      )
    '''),
    parameters: {
      'seq': event.sequenceNumber,
      'eventId': event.eventId,
      'aggId': event.aggregateId,
      'aggType': event.aggregateType,
      'entryType': event.entryType,
      'entryTypeV': event.entryTypeVersion,
      'libFmtV': event.libFormatVersion,
      'eventType': event.eventType,
      'data': jsonEncode(event.data),
      'metadata': jsonEncode(event.metadata),
      'initiator': jsonEncode(event.initiator.toJson()),
      'clientTs': event.clientTimestamp.toUtc(),
      'eventHash': event.eventHash,
      'flowToken': event.flowToken,
      'prevHash': event.previousEventHash,
    },
  );
  return AppendResult(
    sequenceNumber: event.sequenceNumber,
    eventHash: event.eventHash,
  );
}
```

Note: the StorageBackend contract says `appendEvent` does NOT advance the sequence counter (counter is reserved separately via `nextSequenceNumber`); we follow that here. Per the contract, calling `appendEvent` without a paired `nextSequenceNumber` in the same transaction is a caller bug — the substrate's `EventStore.append` already pairs them; we do not need to detect the bug at this level.

- [ ] **Step 4: Implement `findEventsForAggregate`, `findEventsForAggregateInTxn`, `findAllEvents`, `findAllEventsInTxn`.**

Use a shared private helper that composes the WHERE clause from optional filters. The shared helper is required by `EVS-DEV-find-all-events-extended-filters/D` (the assertion that says the reference impl uses one shared composition helper; the same discipline applies here).

```dart
Future<List<StoredEvent>> _findAllEventsComposed({
  Session? session,
  int? afterSequence,
  int? limit,
  String? originatorHopId,
  String? originatorIdentifier,
  String? entryType,
  DateTime? clientTimestampStart,
  DateTime? clientTimestampEnd,
}) async {
  final s = session ?? _pool;
  final wheres = <String>[];
  final params = <String, dynamic>{};
  if (afterSequence != null) {
    wheres.add('sequence_number > @afterSeq');
    params['afterSeq'] = afterSequence;
  }
  if (entryType != null) {
    wheres.add('entry_type = @entryType');
    params['entryType'] = entryType;
  }
  if (clientTimestampStart != null) {
    wheres.add('client_timestamp >= @ctsStart');
    params['ctsStart'] = clientTimestampStart.toUtc();
  }
  if (clientTimestampEnd != null) {
    wheres.add('client_timestamp <= @ctsEnd');
    params['ctsEnd'] = clientTimestampEnd.toUtc();
  }
  if (originatorHopId != null || originatorIdentifier != null) {
    if (originatorHopId != null) {
      wheres.add("metadata->'provenance'->0->>'hopId' = @origHop");
      params['origHop'] = originatorHopId;
    }
    if (originatorIdentifier != null) {
      wheres.add("metadata->'provenance'->0->>'identifier' = @origId");
      params['origId'] = originatorIdentifier;
    }
  }
  final whereClause = wheres.isEmpty ? '' : 'WHERE ${wheres.join(' AND ')}';
  final limitClause = (limit == null) ? '' : 'LIMIT $limit';
  final sql = '''
    SELECT * FROM events $whereClause
    ORDER BY sequence_number ASC
    $limitClause
  ''';
  final result = await s.execute(Sql.named(sql), parameters: params);
  return result.map(_storedEventFromRow).toList();
}

StoredEvent _storedEventFromRow(ResultRow row) {
  final map = row.toColumnMap();
  return StoredEvent.fromMap({
    'event_id': map['event_id'],
    'aggregate_id': map['aggregate_id'],
    'aggregate_type': map['aggregate_type'],
    'entry_type': map['entry_type'],
    'entry_type_version': map['entry_type_version'],
    'lib_format_version': map['lib_format_version'],
    'event_type': map['event_type'],
    'sequence_number': map['sequence_number'],
    'data': map['data'],          // Already a Map (postgres driver decodes JSONB).
    'metadata': map['metadata'],
    'initiator': map['initiator'],
    'flow_token': map['flow_token'],
    'client_timestamp': (map['client_timestamp'] as DateTime).toIso8601String(),
    'event_hash': map['event_hash'],
    'previous_event_hash': map['previous_event_hash'],
  }, map['sequence_number'] as int);
}
```

The public methods delegate to this helper:

```dart
@override
Future<List<StoredEvent>> findAllEvents({
  int? afterSequence, int? limit,
  String? originatorHopId, String? originatorIdentifier,
  String? entryType,
  DateTime? clientTimestampStart, DateTime? clientTimestampEnd,
}) =>
    _findAllEventsComposed(
      afterSequence: afterSequence, limit: limit,
      originatorHopId: originatorHopId, originatorIdentifier: originatorIdentifier,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );

@override
Future<List<StoredEvent>> findAllEventsInTxn(Txn txn, {
  int? afterSequence, int? limit,
  String? entryType,
  DateTime? clientTimestampStart, DateTime? clientTimestampEnd,
}) =>
    _findAllEventsComposed(
      session: _asPgTxn(txn).session,
      afterSequence: afterSequence, limit: limit,
      entryType: entryType,
      clientTimestampStart: clientTimestampStart,
      clientTimestampEnd: clientTimestampEnd,
    );

@override
Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) async {
  final result = await _pool.execute(
    Sql.named('''
      SELECT * FROM events WHERE aggregate_id = @aggId
      ORDER BY sequence_number ASC
    '''),
    parameters: {'aggId': aggregateId},
  );
  return result.map(_storedEventFromRow).toList();
}

@override
Future<List<StoredEvent>> findEventsForAggregateInTxn(Txn txn, String aggregateId) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('''
      SELECT * FROM events WHERE aggregate_id = @aggId
      ORDER BY sequence_number ASC
    '''),
    parameters: {'aggId': aggregateId},
  );
  return result.map(_storedEventFromRow).toList();
}
```

- [ ] **Step 5: Implement `readLatestEventHash`, `findEventByIdInTxn`, `findEventById`.**

```dart
@override
Future<String?> readLatestEventHash(Txn txn) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    'SELECT event_hash FROM events ORDER BY sequence_number DESC LIMIT 1',
  );
  return result.isEmpty ? null : result.first[0] as String;
}

@override
Future<StoredEvent?> findEventByIdInTxn(Txn txn, String eventId) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
    parameters: {'id': eventId},
  );
  return result.isEmpty ? null : _storedEventFromRow(result.first);
}

@override
Future<StoredEvent?> findEventById(String eventId) async {
  final result = await _pool.execute(
    Sql.named('SELECT * FROM events WHERE event_id = @id LIMIT 1'),
    parameters: {'id': eventId},
  );
  return result.isEmpty ? null : _storedEventFromRow(result.first);
}
```

- [ ] **Step 6: Run the event-log conformance subgroup.**

```bash
PG_TEST_URL=... dart test ... -N 'StorageBackend conformance (postgres)/(event log|find_all_events|findEventById)'
```

Expected: all event-log + filter tests PASS.

- [ ] **Step 7: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart \
        event_sourcing/lib/src/storage/postgres/postgres_sql.dart
git commit -m "[CUR-1330] PostgresBackend events: append/find/sequence/hash"
```

---

## Task 7: View rows (JSONB-blob representation)

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`

Methods: `readViewRowInTxn`, `upsertViewRowInTxn`, `deleteViewRowInTxn`, `findViewRows`, `clearViewInTxn`.

- [ ] **Step 1: Confirm the view-row subgroup fails.**

```bash
PG_TEST_URL=... dart test ... -N 'StorageBackend conformance (postgres)/view rows'
```

Expected: FAIL.

- [ ] **Step 2: Implement the five methods.**

```dart
@override
Future<Map<String, dynamic>?> readViewRowInTxn(
  Txn txn, String viewName, String key,
) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('''
      SELECT row_data FROM view_rows
      WHERE view_name = @v AND row_key = @k
      LIMIT 1
    '''),
    parameters: {'v': viewName, 'k': key},
  );
  if (result.isEmpty) return null;
  final raw = result.first[0];
  return raw is Map<String, dynamic>
      ? raw
      : Map<String, dynamic>.from(raw as Map);
}

@override
Future<void> upsertViewRowInTxn(
  Txn txn, String viewName, String key, Map<String, dynamic> row,
) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO view_rows (view_name, row_key, row_data, updated_at)
      VALUES (@v, @k, @row::jsonb, NOW())
      ON CONFLICT (view_name, row_key)
      DO UPDATE SET row_data = EXCLUDED.row_data, updated_at = NOW()
    '''),
    parameters: {'v': viewName, 'k': key, 'row': jsonEncode(row)},
  );
}

@override
Future<void> deleteViewRowInTxn(Txn txn, String viewName, String key) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('DELETE FROM view_rows WHERE view_name = @v AND row_key = @k'),
    parameters: {'v': viewName, 'k': key},
  );
}

@override
Future<List<Map<String, dynamic>>> findViewRows(
  String viewName, {int? limit, int? offset},
) async {
  final limitClause = limit == null ? '' : 'LIMIT $limit';
  final offsetClause = offset == null ? '' : 'OFFSET $offset';
  final result = await _pool.execute(
    Sql.named('''
      SELECT row_data FROM view_rows
      WHERE view_name = @v
      ORDER BY row_key ASC
      $limitClause $offsetClause
    '''),
    parameters: {'v': viewName},
  );
  return result.map((r) {
    final raw = r[0];
    return raw is Map<String, dynamic>
        ? raw
        : Map<String, dynamic>.from(raw as Map);
  }).toList();
}

@override
Future<void> clearViewInTxn(Txn txn, String viewName) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('DELETE FROM view_rows WHERE view_name = @v'),
    parameters: {'v': viewName},
  );
}
```

- [ ] **Step 3: Run view-row subgroup; expect PASS.**

```bash
PG_TEST_URL=... dart test ... -N 'StorageBackend conformance (postgres)/view rows'
```

- [ ] **Step 4: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart
git commit -m "[CUR-1330] PostgresBackend view rows (JSONB blob per row)"
```

---

## Task 8: View target versions

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`

Methods: `readViewTargetVersionInTxn`, `writeViewTargetVersionInTxn`, `readAllViewTargetVersionsInTxn`, `clearViewTargetVersionsInTxn`.

- [ ] **Step 1: Run the subgroup; expect FAIL.**

- [ ] **Step 2: Implement.**

```dart
@override
Future<int?> readViewTargetVersionInTxn(
  Txn txn, String viewName, String entryType,
) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('''
      SELECT target_version FROM view_target_versions
      WHERE view_name = @v AND entry_type = @et
      LIMIT 1
    '''),
    parameters: {'v': viewName, 'et': entryType},
  );
  return result.isEmpty ? null : result.first[0] as int;
}

@override
Future<void> writeViewTargetVersionInTxn(
  Txn txn, String viewName, String entryType, int targetVersion,
) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO view_target_versions (view_name, entry_type, target_version)
      VALUES (@v, @et, @tv)
      ON CONFLICT (view_name, entry_type)
      DO UPDATE SET target_version = EXCLUDED.target_version
    '''),
    parameters: {'v': viewName, 'et': entryType, 'tv': targetVersion},
  );
}

@override
Future<Map<String, int>> readAllViewTargetVersionsInTxn(
  Txn txn, String viewName,
) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('''
      SELECT entry_type, target_version FROM view_target_versions
      WHERE view_name = @v
    '''),
    parameters: {'v': viewName},
  );
  return {
    for (final row in result) row[0] as String: row[1] as int,
  };
}

@override
Future<void> clearViewTargetVersionsInTxn(Txn txn, String viewName) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('DELETE FROM view_target_versions WHERE view_name = @v'),
    parameters: {'v': viewName},
  );
}
```

- [ ] **Step 3: Run subgroup; expect PASS.**

- [ ] **Step 4: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart
git commit -m "[CUR-1330] PostgresBackend view target versions"
```

---

## Task 9: FIFO queue

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`

Methods: `enqueueFifo`, `enqueueFifoTxn`, `readFifoHead`, `listFifoEntries`, `appendAttempt`, `markFinal`, `anyFifoWedged`, `wedgedFifos`, `readFifoRow`, `setFinalStatusTxn`, `deleteNullRowsAfterSequenceInQueueTxn`, `deleteFifoStoreTxn`.

This is the heaviest task. Each method is straightforward but there are eleven of them. Split into sub-steps:

- [ ] **Step 1: Run FIFO subgroup; expect FAIL across the board.**

- [ ] **Step 2: Implement `enqueueFifoTxn` (centralized row construction).**

```dart
@override
Future<FifoEntry> enqueueFifoTxn(
  Txn txn, String destinationId, List<StoredEvent> batch, {
  WirePayload? wirePayload, BatchEnvelopeMetadata? nativeEnvelope,
}) async {
  if (batch.isEmpty) {
    throw ArgumentError('batch SHALL be non-empty');
  }
  if ((wirePayload == null) == (nativeEnvelope == null)) {
    throw ArgumentError(
      'exactly one of wirePayload / nativeEnvelope SHALL be supplied',
    );
  }
  final session = _asPgTxn(txn).session;

  // Reserve next sequence_in_queue for this destination using the
  // backend_state counter.
  final counterKey = 'fifo_seq_counter_$destinationId';
  await session.execute(
    Sql.named("""
      INSERT INTO backend_state (key, value) VALUES (@k, '0'::jsonb)
      ON CONFLICT (key) DO NOTHING
    """),
    parameters: {'k': counterKey},
  );
  final counterResult = await session.execute(
    Sql.named('''
      UPDATE backend_state
        SET value = ((value::text::int) + 1)::text::jsonb
        WHERE key = @k
        RETURNING value::text::int AS new_seq
    '''),
    parameters: {'k': counterKey},
  );
  final sequenceInQueue = counterResult.first[0] as int;

  final entryId = _uuidGen.v4();
  final wireFormat = wirePayload?.contentType ?? 'esd/batch@1';
  final transformVersion = wirePayload?.transformVersion ?? 1;
  final enqueuedAt = DateTime.now().toUtc();

  await session.execute(
    Sql.named('''
      INSERT INTO fifo_entries (
        destination_id, sequence_in_queue, entry_id,
        event_ids, event_id_first_seq, event_id_last_seq,
        wire_format, transform_version, enqueued_at,
        attempts, final_status, sent_at,
        wire_payload, envelope_metadata
      ) VALUES (
        @dest, @seq, @entryId,
        @eventIds::jsonb, @firstSeq, @lastSeq,
        @wireFmt, @transformV, @enqueuedAt,
        '[]'::jsonb, NULL, NULL,
        @wirePayload::jsonb, @envelope::jsonb
      )
    '''),
    parameters: {
      'dest': destinationId,
      'seq': sequenceInQueue,
      'entryId': entryId,
      'eventIds': jsonEncode(batch.map((e) => e.eventId).toList()),
      'firstSeq': batch.first.sequenceNumber,
      'lastSeq': batch.last.sequenceNumber,
      'wireFmt': wireFormat,
      'transformV': transformVersion,
      'enqueuedAt': enqueuedAt,
      'wirePayload':
          wirePayload == null ? null : jsonEncode(wirePayload.toJson()),
      'envelope':
          nativeEnvelope == null ? null : jsonEncode(nativeEnvelope.toJson()),
    },
  );

  return FifoEntry(
    entryId: entryId,
    sequenceInQueue: sequenceInQueue,
    eventIds: batch.map((e) => e.eventId).toList(),
    eventIdRange:
        (firstSeq: batch.first.sequenceNumber, lastSeq: batch.last.sequenceNumber),
    wireFormat: wireFormat,
    transformVersion: transformVersion,
    enqueuedAt: enqueuedAt,
    attempts: const <AttemptResult>[],
    finalStatus: null,
    sentAt: null,
    wirePayload: wirePayload?.toJson(),
    envelopeMetadata: nativeEnvelope?.toJson(),
  );
}

@override
Future<FifoEntry> enqueueFifo(
  String destinationId, List<StoredEvent> batch, {
  WirePayload? wirePayload, BatchEnvelopeMetadata? nativeEnvelope,
}) =>
    transaction(
      (txn) => enqueueFifoTxn(txn, destinationId, batch,
          wirePayload: wirePayload, nativeEnvelope: nativeEnvelope),
    );
```

- [ ] **Step 3: Implement `readFifoHead`, `readFifoRow`, `listFifoEntries`.**

```dart
@override
Future<FifoEntry?> readFifoHead(String destinationId) async {
  final result = await _pool.execute(
    Sql.named('''
      SELECT * FROM fifo_entries
      WHERE destination_id = @dest
        AND (final_status IS NULL OR final_status = 'wedged')
      ORDER BY sequence_in_queue ASC
      LIMIT 1
    '''),
    parameters: {'dest': destinationId},
  );
  return result.isEmpty ? null : _fifoEntryFromRow(result.first);
}

@override
Future<FifoEntry?> readFifoRow(String destinationId, String entryId) async {
  final result = await _pool.execute(
    Sql.named('''
      SELECT * FROM fifo_entries
      WHERE destination_id = @dest AND entry_id = @e
      LIMIT 1
    '''),
    parameters: {'dest': destinationId, 'e': entryId},
  );
  return result.isEmpty ? null : _fifoEntryFromRow(result.first);
}

@override
Future<List<FifoEntry>> listFifoEntries(
  String destinationId, {int? afterSequenceInQueue, int? limit},
) async {
  final wheres = <String>['destination_id = @dest'];
  final params = <String, dynamic>{'dest': destinationId};
  if (afterSequenceInQueue != null) {
    wheres.add('sequence_in_queue > @after');
    params['after'] = afterSequenceInQueue;
  }
  final limitClause = limit == null ? '' : 'LIMIT $limit';
  final result = await _pool.execute(
    Sql.named('''
      SELECT * FROM fifo_entries
      WHERE ${wheres.join(' AND ')}
      ORDER BY sequence_in_queue ASC
      $limitClause
    '''),
    parameters: params,
  );
  return result.map(_fifoEntryFromRow).toList();
}

FifoEntry _fifoEntryFromRow(ResultRow row) {
  final m = row.toColumnMap();
  return FifoEntry(
    entryId: m['entry_id'] as String,
    sequenceInQueue: m['sequence_in_queue'] as int,
    eventIds: List<String>.from(m['event_ids'] as List),
    eventIdRange: (
      firstSeq: m['event_id_first_seq'] as int,
      lastSeq: m['event_id_last_seq'] as int,
    ),
    wireFormat: m['wire_format'] as String,
    transformVersion: m['transform_version'] as int,
    enqueuedAt: m['enqueued_at'] as DateTime,
    attempts: (m['attempts'] as List).map((j) =>
        AttemptResult.fromJson(Map<String, dynamic>.from(j as Map))).toList(),
    finalStatus: m['final_status'] == null
        ? null
        : FinalStatus.values.byName(m['final_status'] as String),
    sentAt: m['sent_at'] as DateTime?,
    wirePayload: m['wire_payload'] == null
        ? null
        : Map<String, dynamic>.from(m['wire_payload'] as Map),
    envelopeMetadata: m['envelope_metadata'] == null
        ? null
        : Map<String, dynamic>.from(m['envelope_metadata'] as Map),
  );
}
```

- [ ] **Step 4: Implement `appendAttempt` and `markFinal` (with no-op-on-missing-row and idempotent-already-final semantics).**

```dart
@override
Future<void> appendAttempt(
  String destinationId, String entryId, AttemptResult attempt,
) async {
  final result = await _pool.execute(
    Sql.named('''
      UPDATE fifo_entries
        SET attempts = attempts || @attempt::jsonb
        WHERE destination_id = @dest AND entry_id = @e
    '''),
    parameters: {
      'dest': destinationId, 'e': entryId,
      'attempt': jsonEncode([attempt.toJson()]),  // wrap to append a single element
    },
  );
  if (result.affectedRows == 0) {
    _logSink('appendAttempt no-op: row not found for ($destinationId, $entryId)');
  }
}

@override
Future<void> markFinal(
  String destinationId, String entryId, FinalStatus status,
) async {
  await _pool.runTx<void>((session) async {
    final existing = await session.execute(
      Sql.named('''
        SELECT final_status FROM fifo_entries
        WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'dest': destinationId, 'e': entryId},
    );
    if (existing.isEmpty) {
      _logSink('markFinal no-op: row not found for ($destinationId, $entryId)');
      return;
    }
    final current = existing.first[0] as String?;
    if (current != null && current == status.name) {
      return; // idempotent on matching already-final
    }
    if (current != null) {
      throw StateError(
        'markFinal: row ($destinationId, $entryId) is already final with '
        'status=$current; cannot transition to ${status.name}',
      );
    }
    final sentAt = (status == FinalStatus.sent) ? DateTime.now().toUtc() : null;
    await session.execute(
      Sql.named('''
        UPDATE fifo_entries
          SET final_status = @s, sent_at = COALESCE(@sentAt, sent_at)
          WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'s': status.name, 'sentAt': sentAt, 'dest': destinationId, 'e': entryId},
    );
  });
}
```

- [ ] **Step 5: Implement `anyFifoWedged`, `wedgedFifos`.**

```dart
@override
Future<bool> anyFifoWedged() async {
  // Head per destination = min sequence_in_queue with non-terminal status.
  // Wedged at head iff that head row has final_status = 'wedged'.
  final result = await _pool.execute('''
    SELECT EXISTS (
      SELECT 1 FROM (
        SELECT DISTINCT ON (destination_id) destination_id, final_status
        FROM fifo_entries
        WHERE (final_status IS NULL OR final_status = 'wedged')
        ORDER BY destination_id, sequence_in_queue ASC
      ) heads
      WHERE heads.final_status = 'wedged'
    )
  ''');
  return result.first[0] as bool;
}

@override
Future<List<WedgedFifoSummary>> wedgedFifos() async {
  final result = await _pool.execute('''
    SELECT destination_id, entry_id, sequence_in_queue
    FROM (
      SELECT DISTINCT ON (destination_id)
        destination_id, entry_id, sequence_in_queue, final_status
      FROM fifo_entries
      WHERE (final_status IS NULL OR final_status = 'wedged')
      ORDER BY destination_id, sequence_in_queue ASC
    ) heads
    WHERE heads.final_status = 'wedged'
    ORDER BY destination_id
  ''');
  return result.map((r) => WedgedFifoSummary(
    destinationId: r[0] as String,
    entryId: r[1] as String,
    sequenceInQueue: r[2] as int,
  )).toList();
}
```

- [ ] **Step 6: Implement `setFinalStatusTxn`, `deleteNullRowsAfterSequenceInQueueTxn`, `deleteFifoStoreTxn`.**

Setting `null -> terminal` validates per the contract (legal transitions: `null -> sent | wedged | tombstoned`, `wedged -> tombstoned`); on `null -> sent` stamps `sent_at`. Deleting the FIFO store on Postgres is `DELETE FROM fifo_entries WHERE destination_id = @dest`.

```dart
@override
Future<void> setFinalStatusTxn(
  Txn txn, String destinationId, String entryId, FinalStatus? status,
) async {
  final session = _asPgTxn(txn).session;
  final existing = await session.execute(
    Sql.named('''
      SELECT final_status FROM fifo_entries
      WHERE destination_id = @dest AND entry_id = @e
    '''),
    parameters: {'dest': destinationId, 'e': entryId},
  );
  if (existing.isEmpty) {
    throw StateError(
      'setFinalStatusTxn: row absent for ($destinationId, $entryId)',
    );
  }
  final current = existing.first[0] as String?;
  // Enforce legal transitions per contract.
  bool legal;
  if (status == null) {
    legal = false; // null target not allowed via this method.
  } else if (current == null) {
    legal = (status == FinalStatus.sent ||
             status == FinalStatus.wedged ||
             status == FinalStatus.tombstoned);
  } else if (current == 'wedged') {
    legal = (status == FinalStatus.tombstoned);
  } else {
    legal = false; // sent/tombstoned are terminal.
  }
  if (!legal) {
    throw StateError(
      'setFinalStatusTxn: illegal transition $current -> ${status?.name}',
    );
  }
  if (status == FinalStatus.sent) {
    await session.execute(
      Sql.named('''
        UPDATE fifo_entries
          SET final_status = 'sent', sent_at = @t
          WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'t': DateTime.now().toUtc(), 'dest': destinationId, 'e': entryId},
    );
  } else {
    await session.execute(
      Sql.named('''
        UPDATE fifo_entries
          SET final_status = @s
          WHERE destination_id = @dest AND entry_id = @e
      '''),
      parameters: {'s': status!.name, 'dest': destinationId, 'e': entryId},
    );
  }
}

@override
Future<int> deleteNullRowsAfterSequenceInQueueTxn(
  Txn txn, String destinationId, int afterSequenceInQueue,
) async {
  final session = _asPgTxn(txn).session;
  final result = await session.execute(
    Sql.named('''
      DELETE FROM fifo_entries
      WHERE destination_id = @dest
        AND sequence_in_queue > @after
        AND final_status IS NULL
    '''),
    parameters: {'dest': destinationId, 'after': afterSequenceInQueue},
  );
  return result.affectedRows;
}

@override
Future<void> deleteFifoStoreTxn(Txn txn, String destinationId) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('DELETE FROM fifo_entries WHERE destination_id = @dest'),
    parameters: {'dest': destinationId},
  );
}
```

- [ ] **Step 7: Run the FIFO conformance subgroup.**

```bash
PG_TEST_URL=... dart test ... -N 'StorageBackend conformance (postgres)/fifo'
```

Expected: PASS.

- [ ] **Step 8: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart
git commit -m "[CUR-1330] PostgresBackend FIFO queue"
```

---

## Task 10: Backend state KV — schema version, fill cursor, schedules

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`

Methods: `readSchemaVersion`, `writeSchemaVersion`, `readFillCursor`, `writeFillCursor`, `writeFillCursorTxn`, `readSchedule`, `writeSchedule`, `writeScheduleTxn`, `deleteScheduleTxn`.

All persist into `backend_state` under keys: `schema_version`, `fill_cursor_<destId>`, `schedule_<destId>`. Values are JSONB.

- [ ] **Step 1: Run subgroup; expect FAIL.**

- [ ] **Step 2: Implement the nine methods.**

Pattern is uniform — INSERT ON CONFLICT DO UPDATE for writes, SELECT for reads, DELETE for delete:

```dart
@override
Future<int> readSchemaVersion() async {
  final result = await _pool.execute(
    "SELECT value::text::int FROM backend_state WHERE key = 'schema_version'",
  );
  return result.isEmpty ? 0 : result.first[0] as int;
}

@override
Future<void> writeSchemaVersion(Txn txn, int version) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO backend_state (key, value)
      VALUES ('schema_version', @v::text::jsonb)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    '''),
    parameters: {'v': version},
  );
}

@override
Future<int> readFillCursor(String destinationId) async {
  final result = await _pool.execute(
    Sql.named('SELECT value::text::int FROM backend_state WHERE key = @k'),
    parameters: {'k': 'fill_cursor_$destinationId'},
  );
  return result.isEmpty ? -1 : result.first[0] as int;
}

@override
Future<void> writeFillCursor(String destinationId, int sequenceNumber) =>
    transaction((txn) => writeFillCursorTxn(txn, destinationId, sequenceNumber));

@override
Future<void> writeFillCursorTxn(
  Txn txn, String destinationId, int sequenceNumber,
) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO backend_state (key, value)
      VALUES (@k, @v::text::jsonb)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    '''),
    parameters: {'k': 'fill_cursor_$destinationId', 'v': sequenceNumber},
  );
}

@override
Future<DestinationSchedule?> readSchedule(String destinationId) async {
  final result = await _pool.execute(
    Sql.named('SELECT value FROM backend_state WHERE key = @k'),
    parameters: {'k': 'schedule_$destinationId'},
  );
  if (result.isEmpty) return null;
  final raw = result.first[0];
  return DestinationSchedule.fromJson(Map<String, dynamic>.from(raw as Map));
}

@override
Future<void> writeSchedule(
  String destinationId, DestinationSchedule schedule,
) =>
    transaction((txn) => writeScheduleTxn(txn, destinationId, schedule));

@override
Future<void> writeScheduleTxn(
  Txn txn, String destinationId, DestinationSchedule schedule,
) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('''
      INSERT INTO backend_state (key, value) VALUES (@k, @v::jsonb)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    '''),
    parameters: {
      'k': 'schedule_$destinationId',
      'v': jsonEncode(schedule.toJson()),
    },
  );
}

@override
Future<void> deleteScheduleTxn(Txn txn, String destinationId) async {
  final session = _asPgTxn(txn).session;
  await session.execute(
    Sql.named('DELETE FROM backend_state WHERE key = @k'),
    parameters: {'k': 'schedule_$destinationId'},
  );
}
```

- [ ] **Step 3: Run subgroup; expect PASS.**

- [ ] **Step 4: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart
git commit -m "[CUR-1330] PostgresBackend backend-state KV (schema version, fill cursor, schedules)"
```

---

## Task 11: Audit query + reverse event scan

**Files:**

- Modify: `event_sourcing/lib/src/storage/postgres/postgres_backend.dart`

Methods: `readEventsReverse`, `queryAudit`.

- [ ] **Step 1: Run subgroup; expect FAIL.**

- [ ] **Step 2: Implement `readEventsReverse` as a generator-style stream.**

```dart
@override
Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) async* {
  // Use a server-side cursor to stream without buffering the full log.
  await for (final batch in _streamReverseBatches(eventTypes, pageSize: 1024)) {
    for (final row in batch) {
      yield _storedEventFromRow(row);
    }
  }
}

Stream<List<ResultRow>> _streamReverseBatches(
  Set<String>? eventTypes, {required int pageSize}) async* {
  int? lastSeq;
  while (true) {
    final wheres = <String>[];
    final params = <String, dynamic>{};
    if (lastSeq != null) {
      wheres.add('sequence_number < @last');
      params['last'] = lastSeq;
    }
    if (eventTypes != null) {
      wheres.add('event_type = ANY(@types)');
      params['types'] = eventTypes.toList();
    }
    final whereClause = wheres.isEmpty ? '' : 'WHERE ${wheres.join(' AND ')}';
    final result = await _pool.execute(
      Sql.named('''
        SELECT * FROM events $whereClause
        ORDER BY sequence_number DESC
        LIMIT $pageSize
      '''),
      parameters: params,
    );
    if (result.isEmpty) return;
    yield result;
    lastSeq = result.last[0] as int;  // sequence_number is first column.
  }
}
```

- [ ] **Step 3: Implement `queryAudit` (cross-store join with cursor pagination).**

The join is `events INNER JOIN security_context USING (event_id)`. Cursor format is `'{recorded_at_iso}|{event_id}'`. Filters compose with AND. Limit is bounded `[1, 1000]`.

```dart
@override
Future<PagedAudit> queryAudit({
  Initiator? initiator,
  String? flowToken,
  String? ipAddress,
  DateTime? from,
  DateTime? to,
  int limit = 50,
  String? cursor,
}) async {
  if (limit < 1 || limit > 1000) {
    throw ArgumentError('limit must be in [1, 1000]; got $limit');
  }
  final wheres = <String>[];
  final params = <String, dynamic>{};
  if (initiator != null) {
    wheres.add('events.initiator = @init::jsonb');
    params['init'] = jsonEncode(initiator.toJson());
  }
  if (flowToken != null) {
    wheres.add('events.flow_token = @flowTok');
    params['flowTok'] = flowToken;
  }
  if (ipAddress != null) {
    wheres.add('security_context.ip_address = @ip');
    params['ip'] = ipAddress;
  }
  if (from != null) {
    wheres.add('security_context.recorded_at >= @from');
    params['from'] = from.toUtc();
  }
  if (to != null) {
    wheres.add('security_context.recorded_at <= @to');
    params['to'] = to.toUtc();
  }
  if (cursor != null) {
    final parts = cursor.split('|');
    if (parts.length != 2) throw ArgumentError('corrupt cursor');
    final cursorAt = DateTime.parse(parts[0]).toUtc();
    final cursorEventId = parts[1];
    wheres.add('''
      (security_context.recorded_at, events.event_id) < (@curAt, @curEv)
    ''');
    params['curAt'] = cursorAt;
    params['curEv'] = cursorEventId;
  }
  final whereClause = wheres.isEmpty ? '' : 'WHERE ${wheres.join(' AND ')}';

  final result = await _pool.execute(
    Sql.named('''
      SELECT
        events.event_id, events.initiator, events.flow_token,
        events.client_timestamp,
        security_context.recorded_at, security_context.ip_address,
        security_context.payload
      FROM events
      INNER JOIN security_context USING (event_id)
      $whereClause
      ORDER BY security_context.recorded_at DESC, events.event_id DESC
      LIMIT ${limit + 1}
    '''),
    parameters: params,
  );
  final rows = <AuditRow>[];
  String? nextCursor;
  for (int i = 0; i < result.length && i < limit; i++) {
    final r = result[i];
    rows.add(AuditRow(
      eventId: r[0] as String,
      initiator: Initiator.fromJson(Map<String, dynamic>.from(r[1] as Map)),
      flowToken: r[2] as String?,
      clientTimestamp: r[3] as DateTime,
      recordedAt: r[4] as DateTime,
      ipAddress: r[5] as String?,
      securityContextPayload:
          Map<String, dynamic>.from(r[6] as Map),
    ));
  }
  if (result.length > limit) {
    final tail = rows.last;
    nextCursor = '${tail.recordedAt.toIso8601String()}|${tail.eventId}';
  }
  return PagedAudit(rows: rows, nextCursor: nextCursor);
}
```

- [ ] **Step 4: Run subgroup; expect PASS.**

- [ ] **Step 5: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_backend.dart
git commit -m "[CUR-1330] PostgresBackend audit query + reverse event scan"
```

---

## Task 12: PostgresIdempotencyStore

**Files:**

- Create: `event_sourcing/lib/src/storage/postgres/postgres_idempotency_store.dart`
- Create: `event_sourcing/test/storage/idempotency_store_conformance.dart`
- Create: `event_sourcing/test/storage/postgres/postgres_idempotency_store_conformance_test.dart`
- Modify: `event_sourcing/test/actions/idempotency_store_test.dart` to delegate to the harness.

- [ ] **Step 1: Lift the existing in-memory tests into a backend-agnostic harness.**

```dart
// event_sourcing/test/storage/idempotency_store_conformance.dart

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void runIdempotencyStoreConformanceTests(
  Future<IdempotencyStore?> Function() factory, {required String label}) {
  group('IdempotencyStore conformance ($label)', () {
    late IdempotencyStore store;

    setUp(() async {
      final candidate = await factory();
      if (candidate == null) {
        markTestSkipped('no store available');
        return;
      }
      store = candidate;
    });

    test('lookup miss', () async {
      expect(await store.lookup('action', 'principal', 'key'), isNull);
    });

    test('record then lookup hit', () async {
      final expiresAt = DateTime.now().add(const Duration(minutes: 5));
      await store.record(
        actionName: 'a', principalId: 'p', key: 'k',
        resultJson: {'ok': true},
        emittedEventIds: ['ev-1'],
        expiresAt: expiresAt,
      );
      final entry = await store.lookup('a', 'p', 'k');
      expect(entry, isNotNull);
      expect(entry!.resultJson, {'ok': true});
      expect(entry.emittedEventIds, ['ev-1']);
    });

    test('separate (action, principal, key) tuples', () async {
      // ... (lifted from idempotency_store_test.dart)
    });

    test('expiry: lookup returns null past expiresAt', () async {
      // ... (lifted)
    });

    test('sweepExpired removes expired entries and reports count', () async {
      // ... (lifted)
    });
  });
}
```

- [ ] **Step 2: Update `idempotency_store_test.dart` to call the harness.**

```dart
import 'package:event_sourcing/event_sourcing.dart';
import '../storage/idempotency_store_conformance.dart';

void main() {
  runIdempotencyStoreConformanceTests(
    () async => InMemoryIdempotencyStore(),
    label: 'in-memory',
  );
}
```

Confirm with `dart test test/actions/idempotency_store_test.dart -p vm` — expected PASS.

- [ ] **Step 3: Write `PostgresIdempotencyStore`.**

```dart
// event_sourcing/lib/src/storage/postgres/postgres_idempotency_store.dart

import 'dart:convert';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/actions/idempotency.dart';
import 'package:postgres/postgres.dart';

class PostgresIdempotencyStore implements IdempotencyStore {
  PostgresIdempotencyStore._(this._pool);
  final Pool _pool;

  /// Open against an already-opened pool. The substrate's
  /// PostgresBackend.open emits the `idempotency` table DDL alongside
  /// the events tables; if you use this store standalone (without
  /// PostgresBackend) call `ensurePostgresSchema` first.
  static PostgresIdempotencyStore over(Pool pool) =>
      PostgresIdempotencyStore._(pool);

  @override
  Future<IdempotencyEntry?> lookup(
    String actionName, String principalId, String key, {DateTime? now},
  ) async {
    final cutoff = (now ?? DateTime.now()).toUtc();
    final result = await _pool.execute(
      Sql.named('''
        SELECT result_json, emitted_event_ids, recorded_at, expires_at
        FROM idempotency
        WHERE action_name = @a AND principal_id = @p AND idempotency_key = @k
          AND expires_at > @cutoff
        LIMIT 1
      '''),
      parameters: {
        'a': actionName, 'p': principalId, 'k': key, 'cutoff': cutoff,
      },
    );
    if (result.isEmpty) return null;
    final r = result.first;
    return IdempotencyEntry(
      resultJson: Map<String, Object?>.unmodifiable(
          Map<String, dynamic>.from(r[0] as Map)),
      emittedEventIds: List<String>.unmodifiable(
          (r[1] as List).cast<String>()),
      recordedAt: r[2] as DateTime,
      expiresAt: r[3] as DateTime,
    );
  }

  @override
  Future<void> record({
    required String actionName, required String principalId,
    required String key, required Map<String, Object?> resultJson,
    required List<String> emittedEventIds, required DateTime expiresAt,
  }) async {
    await _pool.execute(
      Sql.named('''
        INSERT INTO idempotency (
          action_name, principal_id, idempotency_key,
          result_json, emitted_event_ids, recorded_at, expires_at
        ) VALUES (
          @a, @p, @k, @res::jsonb, @ids::jsonb, @recAt, @expAt
        )
        ON CONFLICT (action_name, principal_id, idempotency_key)
        DO UPDATE SET
          result_json = EXCLUDED.result_json,
          emitted_event_ids = EXCLUDED.emitted_event_ids,
          recorded_at = EXCLUDED.recorded_at,
          expires_at = EXCLUDED.expires_at
      '''),
      parameters: {
        'a': actionName, 'p': principalId, 'k': key,
        'res': jsonEncode(resultJson),
        'ids': jsonEncode(emittedEventIds),
        'recAt': DateTime.now().toUtc(),
        'expAt': expiresAt.toUtc(),
      },
    );
  }

  @override
  Future<int> sweepExpired({DateTime? before}) async {
    final cutoff = (before ?? DateTime.now()).toUtc();
    final result = await _pool.execute(
      Sql.named('DELETE FROM idempotency WHERE expires_at <= @c'),
      parameters: {'c': cutoff},
    );
    return result.affectedRows;
  }
}
```

- [ ] **Step 4: Wire the Postgres conformance test.**

```dart
// event_sourcing/test/storage/postgres/postgres_idempotency_store_conformance_test.dart

@TestOn('vm')
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../idempotency_store_conformance.dart';
import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  runIdempotencyStoreConformanceTests(
    () async {
      if (url == null) return null;
      // Clean slate.
      final endpoint = _endpointOf(url);
      final tmp = await Connection.open(endpoint,
          settings: const ConnectionSettings(sslMode: SslMode.disable));
      await tmp.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;');
      await tmp.close();
      final pool = Pool.withEndpoints([endpoint],
          settings: const PoolSettings(maxConnectionCount: 2));
      await pool.runTx((tx) => ensurePostgresSchema(tx));
      return PostgresIdempotencyStore.over(pool);
    },
    label: 'postgres',
  );
}
```

- [ ] **Step 5: Run both conformance suites.**

```bash
dart test test/actions/idempotency_store_test.dart -p vm
PG_TEST_URL=... dart test test/storage/postgres/postgres_idempotency_store_conformance_test.dart -p vm
```

Expected: both PASS.

- [ ] **Step 6: Commit.**

```bash
git add event_sourcing/lib/src/storage/postgres/postgres_idempotency_store.dart \
        event_sourcing/lib/src/storage/postgres/postgres.dart \
        event_sourcing/test/storage/idempotency_store_conformance.dart \
        event_sourcing/test/actions/idempotency_store_test.dart \
        event_sourcing/test/storage/postgres/postgres_idempotency_store_conformance_test.dart
git commit -m "[CUR-1330] PostgresIdempotencyStore + conformance harness"
```

---

## Task 13: Extend example_action_permissions with --backend=postgres

**Files:**

- Modify: `event_sourcing/example_action_permissions/bin/server.dart`
- Modify: `event_sourcing/example_action_permissions/lib/server/bootstrap.dart`
- Modify: `event_sourcing/example_action_permissions/lib/server/demo_idempotency_store.dart`
- Modify: `event_sourcing/example_action_permissions/pubspec.yaml` (add `postgres` to deps)

- [ ] **Step 1: Modify `bootstrap.dart` to accept a `StorageBackend` directly.**

Change the signature so the caller (server.dart) constructs the backend:

```dart
Future<DemoServerComponents> bootstrapDemoServer({
  required StorageBackend backend,
  required IdempotencyStore idempotencyStore,
  required String permissionsYaml,
  required String usersYaml,
  required String installIdentifier,
}) async {
  // existing body, but skip the sembast-specific opening at lines 59-62;
  // use the passed-in backend directly.
  // ... rest stays the same.
}
```

Update the test fixtures and any callers in `test/` to construct the backend up front.

- [ ] **Step 2: Add CLI flags and backend selection to `bin/server.dart`.**

```dart
// Add to parser:
..addOption(
  'backend',
  allowed: ['sembast', 'postgres'],
  defaultsTo: 'sembast',
  help: 'Storage backend.',
)
..addOption(
  'postgres-url',
  help: 'Postgres URL (required when --backend=postgres). '
        'Example: postgres://evs:evs@localhost:5432/evs_demo',
);

// After parsing:
final backendKind = parsed['backend'] as String;
final postgresUrl = parsed['postgres-url'] as String?;

final StorageBackend backend;
final IdempotencyStore idempotencyStore;

if (backendKind == 'postgres') {
  if (postgresUrl == null) {
    stderr.writeln('error: --postgres-url is required when --backend=postgres');
    exitCode = 64;
    return;
  }
  final pg = await PostgresBackend.open(url: postgresUrl);
  backend = pg;
  idempotencyStore = PostgresIdempotencyStore.over(pg.pool);
  stdout.writeln('  backend: postgres ($postgresUrl)');
} else {
  // Existing sembast path; build the SembastBackend here.
  final Database db = ephemeral
      ? await databaseFactoryMemory.openDatabase('demo')
      : await databaseFactoryIo.openDatabase(dbPath);
  backend = SembastBackend(database: db);
  idempotencyStore = InMemoryIdempotencyStore();
  stdout.writeln('  backend: sembast ($dbPath)');
}

final components = await bootstrapDemoServer(
  backend: backend,
  idempotencyStore: idempotencyStore,
  permissionsYaml: permissionsYaml,
  usersYaml: usersYaml,
  installIdentifier: installId,
);
```

Note: this assumes `PostgresBackend` exposes its `Pool` via a getter so the idempotency store can share connections. Add `Pool get pool => _pool;` to `PostgresBackend`.

- [ ] **Step 3: Add postgres dep to example pubspec.**

`event_sourcing/example_action_permissions/pubspec.yaml`:

```yaml
  postgres: ^3.5.0
```

- [ ] **Step 4: Smoke test.**

```bash
cd event_sourcing/example_action_permissions
docker compose up -d postgres
dart run bin/server.dart \
  --backend=postgres \
  --postgres-url=postgres://evs:evs@localhost:5432/evs_demo \
  --port=8080 \
  --permissions-yaml=tool/permissions.yaml \
  --users-yaml=tool/users.yaml \
  --install-id=00000000-0000-4000-8000-000000000001 &
SERVER_PID=$!
sleep 2

# Hit the demo's HTTP endpoints to provision a user, list users, etc.
curl -sS -X POST http://localhost:8080/actions/ProvisionUser \
  -H 'content-type: application/json' \
  -d '{"principal": "alice", "args": {"name": "Bob"}}'
curl -sS http://localhost:8080/state | jq .

kill $SERVER_PID

# Verify the events landed in Postgres.
docker compose exec postgres psql -U evs -d evs_demo \
  -c "SELECT event_id, entry_type, sequence_number FROM events;"
```

Expected: provisioning event in `events`; view row in `view_rows` for the role-permission-grants spec.

- [ ] **Step 5: Update the example README (or add one).**

`event_sourcing/example_action_permissions/README.md` — add a "Running on Postgres" section with the docker compose + dart run commands above. (If no README exists, create a short one focused on the backend choice.)

- [ ] **Step 6: Commit.**

```bash
git add event_sourcing/example_action_permissions/bin/server.dart \
        event_sourcing/example_action_permissions/lib/server/bootstrap.dart \
        event_sourcing/example_action_permissions/lib/server/demo_idempotency_store.dart \
        event_sourcing/example_action_permissions/pubspec.yaml \
        event_sourcing/example_action_permissions/pubspec.lock \
        event_sourcing/example_action_permissions/README.md
git commit -m "[CUR-1330] example_action_permissions: --backend=postgres"
```

---

## Task 14: CI workflow

**Files:**

- Create: `.github/workflows/conformance-tests.yml`

- [ ] **Step 1: Write the workflow.**

```yaml
name: Conformance tests

on:
  push:
    branches: [main]
  pull_request:

jobs:
  sembast:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dart-lang/setup-dart@v1
        with:
          sdk: stable
      - name: Pub get
        run: cd event_sourcing && dart pub get
      - name: Run sembast conformance + unit tests
        run: cd event_sourcing && dart test -p vm

  postgres:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: evs
          POSTGRES_PASSWORD: evs
          POSTGRES_DB: evs_test
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U evs -d evs_test"
          --health-interval 2s
          --health-timeout 2s
          --health-retries 20
    env:
      PG_TEST_URL: postgres://evs:evs@localhost:5432/evs_test
    steps:
      - uses: actions/checkout@v4
      - uses: dart-lang/setup-dart@v1
        with:
          sdk: stable
      - name: Pub get
        run: cd event_sourcing && dart pub get
      - name: Run Postgres conformance suite
        run: |
          cd event_sourcing && \
          dart test test/storage/postgres/ -p vm
```

- [ ] **Step 2: Commit.**

```bash
git add .github/workflows/conformance-tests.yml
git commit -m "[CUR-1330] CI: conformance suite (sembast + postgres service container)"
```

- [ ] **Step 3: Push the branch and verify CI green on the PR.**

```bash
git push -u origin CUR-1330-postgres-backend
gh pr create --title "[CUR-1330] Substrate PostgresBackend: StorageBackend + IdempotencyStore impls" \
             --body "$(cat <<'EOF'
## Summary

- Adds `PostgresBackend implements StorageBackend` and
  `PostgresIdempotencyStore implements IdempotencyStore` as a second
  reference impl alongside sembast.
- View rows are stored as JSONB blobs in a single `view_rows` table
  (decision captured in
  `docs/superpowers/specs/2026-05-12-postgres-backend-design.md`).
- Lifts existing sembast contract tests into a backend-agnostic
  conformance harness; both backends pass the same suite.
- Extends `example_action_permissions` with `--backend=postgres`.
- Adds GitHub Actions CI job with a Postgres service container.

## Test plan

- [ ] `dart test event_sourcing/ -p vm` — all sembast + in-memory unit tests pass.
- [ ] `PG_TEST_URL=postgres://...` `dart test event_sourcing/test/storage/postgres/ -p vm` —
      all Postgres conformance tests pass against a local postgres:16.
- [ ] Run the extended example end-to-end (compose up postgres, run server with
      --backend=postgres, hit an action endpoint, verify events landed in
      Postgres tables).
- [ ] CI: conformance-tests workflow green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (already performed in writing this plan)

**Spec coverage:**

- View-row representation decision → Task 1 (design spec) + Task 7 (impl). ✓
- `PostgresBackend implements StorageBackend` full surface → Tasks 5–11. ✓
- `PostgresIdempotencyStore implements IdempotencyStore` with 3-policy + TTL → Task 12. Policies are enforced by the action-dispatch code, not the store; the store only persists outcomes — Assertion E captures this. ✓
- Conformance test suite (backend-agnostic harness) → Task 3 (harness creation + sembast lift) + Task 12 (idempotency harness) + Task 5 (postgres entrypoint). ✓
- Schema DDL emitted by substrate → Task 4. ✓
- CI: postgres service container → Task 14. ✓
- ADR / design spec under `docs/superpowers/specs/` → Task 1. ✓
- DEV-level requirements (`EVS-DEV-postgres-backend-*`) → Task 1 (single multi-assertion spec). ✓
- Out of scope items kept out: no portal-IaC, no per-area cutovers, no IndexedDB. ✓

**Placeholder scan:** no TBDs; every step has either code, SQL, or a concrete bash command.

**Type consistency:** `StoredEvent`, `FifoEntry`, `AppendResult`, `Initiator`, `AuditRow`, `PagedAudit`, `WedgedFifoSummary`, `IdempotencyEntry`, `DestinationSchedule`, `WirePayload`, `BatchEnvelopeMetadata` — all referenced consistently against the existing definitions in `lib/src/storage/` and `lib/src/destinations/`. `Pool`, `Sql.named`, `TxSession`, `Endpoint`, `IsolationLevel.serializable`, `TransactionSettings` — all from `package:postgres` v3 API.

**Known limitation surfaced in design spec:** Postgres backend does not emit reactive change streams (sembast's `_eventsController` / `_fifoChangesController` / `_viewChangesController`); follow-up work to support `subscribe<T>` over Postgres via polling or `LISTEN`/`NOTIFY` is called out as a future ticket.

---

**Plan complete and saved to `docs/superpowers/plans/2026-05-12-postgres-backend-implementation.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration. Best for this plan because each task is independent and the conformance harness gives each subagent a clear local "done" signal.

**2. Inline Execution** — execute tasks in this session using `superpowers:executing-plans`, batch execution with checkpoints for review.

**Which approach?**
