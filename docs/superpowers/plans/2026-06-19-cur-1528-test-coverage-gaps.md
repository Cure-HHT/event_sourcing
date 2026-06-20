# CUR-1528 — Close Requirement Test-Coverage Gaps (assertion-level) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every *actionable* uncovered requirement assertion to test-covered in the elspais graph, fencing off the assertions that are uncovered by design.

**Architecture:** Most gaps are NOT missing tests — they are annotation/config defects in how elspais binds existing tests. The work is three kinds: (1) fix `.elspais.toml` + annotation formats so existing tests bind; (2) write a small number of genuinely-new tests (flow-token threading, portability purity scans, a couple of Postgres assertions); (3) record out-of-scope assertions as intentionally uncovered.

**Tech Stack:** Dart / Flutter, `flutter test` per package, the `elspais` MCP for coverage verification.

## Global Constraints

These are copied verbatim from the investigation and apply to **every** task:

- **elspais binding rule 1 — scanned files only.** A `// Verifies:` annotation binds ONLY in a file matching `*_test.dart` or `test_*.dart` under a `[scanning.test].directories` entry in `.elspais.toml`. The shared harness `event_sourcing/test/storage/storage_backend_conformance.dart` is NOT `*_test.dart` and is never scanned — its annotations are dead. Put labels in the per-backend `*_test.dart` entrypoints instead.
- **elspais binding rule 2 — each reference must be fully qualified.** `// Verifies: EVS-PRD-foo/A` binds A. The shorthand `// Verifies: EVS-PRD-foo/A, /C, /E` binds **only A** (the bare `/C, /E` are dropped). Repeat the full id per label: `// Verifies: EVS-PRD-foo/A, EVS-PRD-foo/C, EVS-PRD-foo/E`, or one fully-qualified ref per line.
- **Verify with the MCP, not by eye.** After any annotation/config/test change, run `mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id)` (or `get_uncovered_assertions(source="test")`). `get_requirement().coverage` reports *code-ref* coverage, NOT test coverage — do not trust it for this work.
- **`refines:` now propagates coverage (elspais fix landing 2026-06).** A parent assertion can be marked covered by tests on the child requirements that refine it — this is "coverage by refinement." Practical consequence: **after each task, re-run `get_uncovered_assertions(source="test")` for the whole graph** — an in-scope assertion may already have gone covered transitively (e.g. covering a child closes a parent), in which case **skip its direct annotation/test task**. Prefer letting propagation cover a parent over adding a direct annotation to it; only annotate a parent directly if a post-refresh check shows propagation did not catch it (the fix may not be live in every environment yet).
- **Per-package runner split.** Pure-Dart packages run `dart test`; `event_sourcing`, `reaction`, `reaction_widgets`, `reaction_widgets_testing` run `flutter test`. Some `event_sourcing` test files transitively import `flutter_test` and only compile under `flutter test`; a `dart test` load failure on those is NOT a real failure. The repo's CI / `.elspais.toml` runners use `flutter test` for every package.
- **Domain-neutral substrate.** No `DiaryEntry`-style domain types in any new test.
- **Out of scope — leave uncovered, do not annotate:** `EVS-PRD-multi-source-canonicalization/A–F` (dormant Phase-II machinery), `EVS-PRD-library-charter/G` (ALCOA+/21 CFR Part 11 — needs human attestation; flag as follow-up), `EVS-PRD-library-charter/I` (Layer-1/2 doc discipline, not executable).
- **Commit cadence:** one commit per task (per the repo's batch-commit norm; the pre-commit dart-format hook flakes, so avoid sub-task commits). PR title MUST include `[CUR-1528]`.
- **Spec-file edits** (e.g. Task 1's stale-note fix) commit with `git --no-verify` and `gh` calls run under `env -u GITHUB_TOKEN` (elspais marks `spec/INDEX.md` read-only, tripping the end-of-file hook).

## Live starting state (from `get_uncovered_assertions(source="test")`, graph refreshed 2026-06-19)

```text
EVS-DEV-find-all-events-extended-filters  A,B,C,D   (4/4 uncovered)
EVS-DEV-flow-token                        A,B,C,D   (4/4 uncovered)
EVS-DEV-postgres-backend                  B,C       (2/6 uncovered)
EVS-PRD-portability                       A,B       (2/4 uncovered)
EVS-PRD-reaction-scope                    C         (1/5 uncovered)
EVS-PRD-reaction-widget-contract          A,B,D,F,G,H,I,J,K (9/11 uncovered)
EVS-PRD-scoped-permissions                A         (1/9 uncovered)
EVS-PRD-view-subscriber                   E         (1/5 uncovered)
EVS-PRD-library-charter                   B,F,(G,I) (B,F in scope; G,I out)
EVS-PRD-multi-source-canonicalization     A–F       (out of scope)
```
Graph health: 0 orphans, 0 broken references — must stay that way.

## POST-PROPAGATION STATUS (verified 2026-06-20) — read this first

elspais `refines:` propagation is now live. It is **upward-only and coarse** (requirement-level): any covering child marks the parent's assertions covered, regardless of which assertion the child actually exercises. The elspais update also fixed multi-label shorthand parsing (`// Verifies: EVS-...-foo/A, /C, /E` now binds A, C, and E — not just A).

**This is the accepted, correct outcome for requirement-level `refines:` edges.** (Assertion-level precision would require re-pointing the `refines:` edges at specific assertions rather than whole requirements — a separate concern, not in scope for CUR-1528.) A re-run of `get_uncovered_assertions(source="test")` after the update shows these requirements **already fully covered by propagation — no action needed**, and the corresponding tasks below are struck through:

| Requirement | Resolution | Original task |
|---|---|---|
| `EVS-PRD-reaction-scope/C` | covered (shorthand-parse fix made its real LocalScope test bind) | ~~Task 2~~ done |
| `EVS-PRD-portability/A,B` | covered (propagated up from `postgres-backend`) — accepted as coarse-but-correct | ~~Task 6~~ done |
| `EVS-PRD-library-charter/B,F` | covered (propagated from subscription/materializer/canonical-json/provenance) | ~~Task 7~~ done |
| `EVS-PRD-library-charter/G,I` | covered by propagation — accepted (no longer need separate fencing; the ALCOA+/Layer-distinction children carry it) | n/a |
| `EVS-PRD-scoped-permissions/A` | covered (propagated from `scope-class-registry-validation`) | ~~Task 8 Step 1~~ done |
| `EVS-PRD-view-subscriber/E` | covered (propagated from `reaction-scope`) | ~~Task 8 Step 2~~ done |

**Remaining ACTIONABLE work — leaves that nothing refines, so propagation can't help (do these):**
- **Task 1** — `reaction-widget-contract` A,B,D,F,G,H,I,J,K (scan-config gap)
- **Task 3** — `find-all-events` A,B,C,D (annotations in an unscanned harness file)
- **Task 4** — `postgres-backend` B,C
- **Task 5** — `flow-token` A,B,C,D (genuinely new tests)
- **Task 9** — final verification + PR

**Still out of scope (leaves, intentionally uncovered):** `multi-source-canonicalization/A–F`.

## Open decisions (resolved)

1. **flow-token/D ("opaque; excludes cleartext secrets")** — cover the substrate-side opacity (byte-identical pass-through) in Task 5; the secret-exclusion clause is a consumer obligation, documented in the test header, not substrate-enforced.
2. **Coarse propagation accepted.** Per the ticket author: marking parents covered from requirement-level `refines:` edges is the correct outcome; assertion-targeted edges are the (out-of-scope) path to finer precision.

---

### Task 1: Bind the `reaction_widgets` / `reaction_widgets_testing` widget-contract tests

The widget tests already exist and exercise A,B,D,F,G,H,I,J,K, but elspais scans neither widget package's `test/` dir, and the existing headers use the non-binding `/A, /B` shorthand. Fix both. Net: 9 assertions move to covered with little-to-no new test code.

**Files:**
- Modify: `.elspais.toml` (`[scanning.test].directories` + add two `[[scanning.test.runners]]`)
- Modify: every `*_test.dart` under `reaction_widgets/test/` and `reaction_widgets_testing/test/` whose `// Verifies:` header uses the `/X, /Y` shorthand (inventory: `scope/reaction_scope_widget_test.dart`, `action/action_builder_test.dart`, `view/view_state_test.dart`, `view/view_listener_test.dart`, `view/view_builder_test.dart`, `permission/permission_gate_test.dart`, `error/reaction_error_listener_test.dart`, `structural/no_substrate_imports_test.dart`, `reaction_widgets_testing/test/fake_reaction_test.dart`)
- Modify: `spec/prd-reaction.md` (remove the stale "package not yet implemented" status note on `EVS-PRD-reaction-widget-contract`)

**Interfaces:**
- Consumes: nothing.
- Produces: `EVS-PRD-reaction-widget-contract/{A,B,D,F,G,H,I,J,K}` test-covered. Establishes the fully-qualified annotation convention reused by Tasks 2–8.

- [ ] **Step 1: Add the two widget test dirs + runners to `.elspais.toml`**

In `[scanning.test].directories` (currently ends with `"reaction/test",`) append:
```toml
  "reaction_widgets/test",
  "reaction_widgets_testing/test",
```
After the last `[[scanning.test.runners]]` block (the `reaction` one) add:
```toml
[[scanning.test.runners]]
name = "reaction_widgets"
command = "flutter test"
cwd = "reaction_widgets"

[[scanning.test.runners]]
name = "reaction_widgets_testing"
command = "flutter test"
cwd = "reaction_widgets_testing"
```

- [ ] **Step 2: Refresh and capture the baseline-after-scan**

Run `mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-PRD-reaction-widget-contract")`.
Expected: more than the prior 2 covered (C,E), but some of A,B,D,F,G,H,I,J,K may still be uncovered because of the `/X, /Y` shorthand headers. Record which labels are still uncovered.

- [ ] **Step 3 (fallback only): Rewrite shorthand headers if any label didn't bind**

The elspais update fixed multi-label shorthand parsing, so the existing `/A, /B`-style widget headers should now bind every label once the dirs are scanned (Step 2). ONLY if Step 2 shows some of A,B,D,F,G,H,I,J,K still uncovered, expand that file's header to fully-qualified refs, e.g. `// Verifies: EVS-PRD-reaction-widget-contract/A, /B` → `// Verifies: EVS-PRD-reaction-widget-contract/A, EVS-PRD-reaction-widget-contract/B`. Files to check if needed: `action_builder_test.dart` (`/C, /E, /G`), `view_builder_test.dart` (`/C, /G, /I, /J`), `view_listener_test.dart` (`/D, /G`), `reaction_scope_widget_test.dart` (`/A, /B`).

- [ ] **Step 4: Refresh and verify all 9 in-scope labels bind**

Run `mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-PRD-reaction-widget-contract")`.
Expected: `covered_assertions` includes A,B,C,D,E,F,G,H,I,J,K (all 11). If any of A,B,D,F,G,H,I,J,K is still uncovered, that assertion has a *genuine* test gap — write a focused widget test for it in the matching file (e.g. an explicit `ReActionScope.of(context)` LocalScope-vs-RemoteScope source-identity test for B), annotate it, and re-verify.

- [ ] **Step 5: Remove the stale "not yet implemented" note in the spec**

In `spec/prd-reaction.md`, find the `EVS-PRD-reaction-widget-contract` body line:
`> **Implementation status:** Designed; \`reaction_widgets\` package not yet implemented. ...`
Replace it with a one-line note that the package now ships (both `reaction_widgets` and `reaction_widgets_testing`), so audit tooling treats these assertions as shipped-and-tested. Keep it a remainder-prose edit only — do NOT touch the normative assertion text/labels.

- [ ] **Step 6: Run the widget suites + commit**

```bash
( cd reaction_widgets && flutter test )
( cd reaction_widgets_testing && flutter test )
```
Expected: PASS. Then commit (spec file is included, so):
```bash
git add .elspais.toml reaction_widgets/test reaction_widgets_testing/test spec/prd-reaction.md
git commit --no-verify -m "[CUR-1528] Bind reaction widget-contract tests (scan dirs + fully-qualified Verifies)"
```

---

### ~~Task 2: Fix `EVS-PRD-reaction-scope/C` annotation~~ — RESOLVED BY PROPAGATION (no action)

> The elspais shorthand-parse fix made `local_scope_test.dart`'s existing `/A, /C, /E` header bind `/C`. Verified covered 2026-06-20. Steps below kept for record only.

#### (superseded) Task 2 detail

The LocalScope always-`Connected` tests already exist in `reaction/test/scope/local_scope_test.dart` (lines 64–74), but the header `// Verifies: EVS-PRD-reaction-scope/A, /C, /E` binds only `/A` (shorthand rule). Make `/C` bind.

**Files:**
- Modify: `reaction/test/scope/local_scope_test.dart:1`

**Interfaces:**
- Consumes: the convention from Task 1.
- Produces: `EVS-PRD-reaction-scope/C` test-covered (req goes 5/5).

- [ ] **Step 1: Rewrite the header to fully-qualified refs**

Replace line 1:
```dart
// Verifies: EVS-PRD-reaction-scope/A, /C, /E
```
with:
```dart
// Verifies: EVS-PRD-reaction-scope/A, EVS-PRD-reaction-scope/C, EVS-PRD-reaction-scope/E
```

- [ ] **Step 2: Refresh and verify**

`mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-PRD-reaction-scope")`.
Expected: `covered_assertions` = A,B,C,D,E; `uncovered_assertions` = [].

- [ ] **Step 3: Sanity-run + (defer commit — bundle with Task 3 in the same `reaction`/`event_sourcing` pass, or commit now)**

```bash
( cd reaction && flutter test test/scope/local_scope_test.dart )
```
Expected: PASS. Commit:
```bash
git add reaction/test/scope/local_scope_test.dart
git commit --no-verify -m "[CUR-1528] Bind reaction-scope/C (fully-qualified Verifies on local_scope_test)"
```

---

### Task 3: Bind `EVS-DEV-find-all-events-extended-filters/A,B,C,D`

The extended-filter behavior is fully tested inside the conformance harness `storage_backend_conformance.dart` (test group `findAllEvents extended filters`, ~lines 631–891, exercising entryType, inclusive/exclusive timestamps, AND-composition, and the in-txn path). But the harness file is not `*_test.dart`, so its existing `// Verifies:` line is dead (`test_nodes: []`). Bind the labels on the two scanned entrypoints that run the harness.

**Files:**
- Modify: `event_sourcing/test/storage/sembast_backend_conformance_test.dart` (header)
- Modify: `event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart` (header)
- (Optional cleanup) Modify: `event_sourcing/test/storage/storage_backend_conformance.dart` — fix its dead `/A,B,C` line to the correct format and add `/D`, as documentation, even though it does not bind.

**Interfaces:**
- Consumes: convention from Task 1.
- Produces: `EVS-DEV-find-all-events-extended-filters/{A,B,C,D}` test-covered.

Rationale for D: assertion D (single shared `_composeFindAllEventsFilter` helper used by both in-txn and out-of-txn paths) is exercised behaviorally by the harness running the same filter assertions through both `findAllEvents` and `findAllEventsInTxn` (the in-txn test at ~line 847). Annotate it on the same entrypoints.

- [ ] **Step 1: Add fully-qualified labels to the SembastBackend conformance entrypoint**

In `event_sourcing/test/storage/sembast_backend_conformance_test.dart`, after the existing `// Verifies:` header lines (before `@TestOn('vm')`), add:
```dart
// Verifies: EVS-DEV-find-all-events-extended-filters/A, EVS-DEV-find-all-events-extended-filters/B, EVS-DEV-find-all-events-extended-filters/C, EVS-DEV-find-all-events-extended-filters/D
//   — entryType + client-timestamp filters AND-compose on findAllEvents and
//   findAllEventsInTxn via the shared compose helper; exercised by the
//   conformance harness 'findAllEvents extended filters' group.
```

- [ ] **Step 2: Add the same labels to the Postgres conformance entrypoint**

Apply the identical four-label `// Verifies:` block to `event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart` (after its existing header lines).

- [ ] **Step 3: Refresh and verify**

`mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-DEV-find-all-events-extended-filters")`.
Expected: `covered_assertions` = A,B,C,D; `test_nodes` now non-empty (the two conformance entrypoints).

- [ ] **Step 4: Run + commit (bundle with Task 4 — same Postgres/Sembast files)**

```bash
( cd event_sourcing && flutter test test/storage/sembast_backend_conformance_test.dart )
```
Expected: PASS (Postgres entrypoint self-skips without `PG_TEST_URL`). Commit deferred to Task 4.

---

### Task 4: Bind `EVS-DEV-postgres-backend/B` (view_rows schema) and `/C` (transaction semantics)

Both behaviors are implemented and exercised, but neither label is on a scanned test. B: the `view_rows(view_name, row_key, row_data JSONB, updated_at)` table with `PRIMARY KEY (view_name, row_key)` — add a focused DDL assertion to the existing schema test. C: `transaction<T>` runs at SERIALIZABLE, rolls back on throw / commits on return, and invalidates the handle after body — exercised by the conformance harness transaction tests and the serialization-retry test; bind the label there.

**Files:**
- Modify: `event_sourcing/test/storage/postgres/postgres_backend_schema_test.dart` (add a `/B` view_rows-DDL test + header label)
- Modify: `event_sourcing/test/storage/postgres/postgres_backend_conformance_test.dart` (add `/C` header label) and/or `event_sourcing/test/storage/postgres/postgres_backend_serialization_retry_test.dart` (add `/C` for the SERIALIZABLE-conflict path)

**Interfaces:**
- Consumes: the PG_TEST_URL gating pattern already in `postgres_backend_schema_test.dart` (`testPostgresUrl()`, `_connect`, `_listPublicTables`).
- Produces: `EVS-DEV-postgres-backend/{B,C}` test-covered (req goes 6/6).

Note: PG-gated tests still bind coverage *statically* from the annotation — the label binds even when the test self-skips for lack of `PG_TEST_URL`. The run result (pass/skip) is tracked separately from binding.

- [ ] **Step 1: Add a failing `/B` view_rows-schema test**

In `postgres_backend_schema_test.dart`, inside `group('PostgresBackend schema', ...)`, add:
```dart
    test('view_rows has the JSONB-blob shape with composite PK', () async {
      final backend = await PostgresBackend.open(
        url: url,
        sslMode: SslMode.disable,
      );
      addTearDown(backend.close);
      final conn = await _connect(url);
      addTearDown(conn.close);

      // Columns + types.
      final cols = await conn.execute(
        'SELECT column_name, data_type FROM information_schema.columns '
        "WHERE table_schema = 'public' AND table_name = 'view_rows'",
      );
      final types = {for (final r in cols) r[0]! as String: r[1]! as String};
      expect(types['view_name'], 'text');
      expect(types['row_key'], 'text');
      expect(types['row_data'], 'jsonb');
      expect(types['updated_at'], 'timestamp with time zone');

      // Primary key is exactly (view_name, row_key).
      final pk = await conn.execute(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = 'view_rows'::regclass AND i.indisprimary "
        "ORDER BY a.attname",
      );
      final pkCols = pk.map((r) => r[0]! as String).toList();
      expect(pkCols, ['row_key', 'view_name']);
    });
```
Then add the header label (top of file, after the existing `/A` block):
```dart
// Verifies: EVS-DEV-postgres-backend/B — view_rows stored as a single JSONB-blob
//   table keyed by (view_name, row_key).
```

- [ ] **Step 2: Run the `/B` test against a live Postgres**

```bash
PG_TEST_URL=postgres://... ( cd event_sourcing && flutter test test/storage/postgres/postgres_backend_schema_test.dart )
```
Expected: PASS. If no Postgres is available locally, confirm the file self-skips cleanly (`flutter test` with `PG_TEST_URL` unset → the guard at the top marks skipped), and rely on CI for the live run. Do NOT mark Task 4 done on a skip alone if a DB is reachable — run it green at least once.

- [ ] **Step 3: Bind `/C` on the transaction-semantics tests**

The conformance harness already verifies transaction rollback/commit and post-body handle invalidation, and `postgres_backend_serialization_retry_test.dart` verifies the SERIALIZABLE conflict path. Add to the header of `postgres_backend_serialization_retry_test.dart`:
```dart
// Verifies: EVS-DEV-postgres-backend/C — transaction<T> runs at SERIALIZABLE
//   isolation (conflicting concurrent txns retry/serialize); rollback on throw,
//   commit on return, handle invalidated after body.
```
If the serialization-retry test does not also cover handle-invalidation and rollback/commit, add the `/C` label to `postgres_backend_conformance_test.dart` as well (the harness's transaction subgroup covers those).

- [ ] **Step 4: Refresh, verify, run, commit (bundles Task 3)**

`mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-DEV-postgres-backend")` → expected A–F all covered.
```bash
( cd event_sourcing && flutter test test/storage/ )
git add event_sourcing/test/storage
git commit --no-verify -m "[CUR-1528] Bind find-all-events extended filters + postgres-backend B/C coverage"
```

---

### Task 5: Write flow-token threading tests — `EVS-DEV-flow-token/A,B,C,D`

The feature ships (`ActionSubmission.flowToken`, dispatcher stamps it onto every emitted event incl. denials, `StoredEvent.flowToken` persists, Postgres `events.flow_token` column) but has ZERO bound tests. This is the one bucket needing substantive new tests. Mirror the dispatch harness used in `event_sourcing/test/actions/action_dispatcher_test.dart` / `integration_test.dart`.

**Files:**
- Create: `event_sourcing/test/actions/flow_token_test.dart`
- Test runner: `flutter test` in `event_sourcing/`

**Interfaces:**
- Consumes: `ActionSubmission({required actionName, required rawInput, idempotencyKey, flowToken})` (`event_sourcing/lib/src/actions/action_submission.dart`); `ActionDispatcher.dispatch(submission, context)`; the in-memory dispatch harness pattern in the sibling action tests; `StoredEvent.flowToken`; `StorageBackend.findAllEvents(...)`.
- Produces: `EVS-DEV-flow-token/{A,B,C,D}` test-covered.

- [ ] **Step 1: Read the existing dispatch harness to learn setup**

Open `event_sourcing/test/actions/action_dispatcher_test.dart` and `event_sourcing/test/actions/integration_test.dart`. Identify how they build an `ActionDispatcher` over an in-memory `SembastBackend`/`EventStore`, register a trivial action (one that appends a success event) and an action that will be authorization-denied, and construct an `ActionContext`/`Principal`. Reuse that exact harness in the new file. (Do not invent a new dispatch API — copy the working one.)

- [ ] **Step 2: Write the failing flow-token tests**

Create `event_sourcing/test/actions/flow_token_test.dart` with this header and these four tests (fill the `// harness setup` block by copying from Step 1's files):
```dart
// Verifies: EVS-DEV-flow-token/A — dispatcher accepts an optional opaque
//   correlation token on submission.
// Verifies: EVS-DEV-flow-token/B — token is threaded onto every emitted event,
//   including denial events.
// Verifies: EVS-DEV-flow-token/C — token is preserved unchanged when the event
//   is ingested by another deployment.
// Verifies: EVS-DEV-flow-token/D — token is opaque: stored and returned
//   byte-identically, neither parsed nor interpreted by the substrate.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
// + the imports the sibling dispatch tests use for the harness

void main() {
  group('flow correlation token', () {
    // <copy the in-memory dispatcher + backend setUp from
    //  action_dispatcher_test.dart / integration_test.dart>

    test('A: submission accepts an optional flowToken (null and set)', () {
      const a = ActionSubmission(actionName: 'x', rawInput: {});
      const b = ActionSubmission(
        actionName: 'x',
        rawInput: {},
        flowToken: 'flow-123',
      );
      expect(a.flowToken, isNull);
      expect(b.flowToken, 'flow-123');
    });

    test('B: success event carries the flowToken', () async {
      const token = 'flow-success';
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: '<an action that succeeds>',
          rawInput: {/* valid input */},
          flowToken: token,
        ),
        context, // authorized principal
      );
      final events = await backend.findAllEvents();
      expect(events, isNotEmpty);
      expect(events.every((e) => e.flowToken == token), isTrue);
    });

    test('B: denial event carries the flowToken', () async {
      const token = 'flow-denied';
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: '<an action the principal is NOT authorized for>',
          rawInput: {/* valid input */},
          flowToken: token,
        ),
        unauthorizedContext,
      );
      final events = await backend.findAllEvents(
        entryType: 'action_denial',
      );
      expect(events, isNotEmpty);
      expect(events.single.flowToken, token);
    });

    test('C/D: an arbitrary opaque token round-trips unchanged through '
        'persistence (and ingest preserves it)', () async {
      // Opaqueness: a token the substrate has no schema for must survive
      // byte-identically. Covers D's "neither parsed nor interpreted".
      const weird = 'a/b+c=  {"not":"json-to-the-substrate"} é';
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: '<an action that succeeds>',
          rawInput: {/* valid input */},
          flowToken: weird,
        ),
        context,
      );
      final stored = (await backend.findAllEvents()).last;
      expect(stored.flowToken, weird);

      // C: ingest the stored event into a SECOND backend and assert the
      // token is preserved. Use the same ingest path the ingest tests use
      // (see event_sourcing/test for the ingest/relay harness); assert the
      // re-read event's flowToken == weird.
    });
  });
}
```
Notes: pick a concrete already-registered test action for `<...succeeds>` and an unauthorized one for the denial case from the harness you copied. For the ingest leg of C, mirror whatever ingest/relay test already exists (search `event_sourcing/test` for the ingest harness); if a full ingest harness is heavyweight, the persistence round-trip already covers C's "preserved unchanged" at the storage boundary — but prefer exercising the real ingest path if a reusable harness exists.

- [ ] **Step 3: Run to verify the tests fail for the right reason, then pass**

```bash
( cd event_sourcing && flutter test test/actions/flow_token_test.dart )
```
Expected: tests compile and pass (the feature is implemented). If B/denial fails because the denied-event `entryType` differs, inspect the actual denial event (`denial_events_test.dart` shows `entryType == 'action_denial'`) and adjust the filter.

- [ ] **Step 4: Refresh, verify, commit**

`mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-DEV-flow-token")` → expected A,B,C,D covered.
Document the residual: assertion D's "SHALL exclude cleartext one-time passwords, recovery tokens, and session tokens" is a *consumer* obligation the substrate cannot enforce; the test covers the substrate-side opacity guarantee. Note this in the test file's header comment.
```bash
git add event_sourcing/test/actions/flow_token_test.dart
git commit --no-verify -m "[CUR-1528] Add flow-correlation-token threading/opacity tests"
```

---

### ~~Task 6: Write portability purity tests — `EVS-PRD-portability/A,B`~~ — RESOLVED BY PROPAGATION (no action)

> portability/A,B propagated covered from the `postgres-backend` child. Accepted as coarse-but-correct (ticket author's call). The purity-scan test below is OPTIONAL future hardening (real protection behind the green status) — not required to close CUR-1528. Steps kept for record/optional pickup.

#### (optional / superseded) Task 6 detail

A: the core is pure Dart (no `package:flutter` import in core `lib/`). B: it loads on every runtime (proxy: no platform-locked `dart:io` / `dart:html` / `dart:ffi` imports in core `lib/`, so the same code compiles on web and VM). Mirror the existing structural-scan pattern in `reaction_widgets/test/structural/no_substrate_imports_test.dart`.

**Files:**
- Create: `event_sourcing/test/portability_purity_test.dart`
- Test runner: `flutter test` in `event_sourcing/` (runs on the VM; `dart:io` is available to the *test* for filesystem scanning)

**Interfaces:**
- Consumes: `dart:io` `Directory`/`File` for the scan; relative paths from the `event_sourcing` package root (`flutter test` sets cwd to the package dir, so siblings are `../canonical_json_jcs`, `../provenance`, `../reaction`).
- Produces: `EVS-PRD-portability/{A,B}` test-covered (req goes 4/4).

- [ ] **Step 1: Write the failing purity scan test**

Create `event_sourcing/test/portability_purity_test.dart`:
```dart
// Verifies: EVS-PRD-portability/A — the core libraries are pure Dart: no
//   source under their lib/ imports package:flutter.
// Verifies: EVS-PRD-portability/B — the core loads on every Dart runtime:
//   no source under their lib/ imports a platform-locked dart: library
//   (dart:io / dart:html / dart:ffi / dart:js), so the same code compiles on
//   web and VM. (True multi-runtime execution is verified by CI matrix.)
@TestOn('vm')
library;

import 'dart:io';
import 'package:flutter_test/flutter_test.dart';

/// Core packages whose runtime lib/ must stay portable. The Flutter widget
/// packages (reaction_widgets, reaction_widgets_testing) are intentionally
/// excluded — they are the platform layer.
const _coreLibDirs = <String>[
  '../canonical_json_jcs/lib',
  '../provenance/lib',
  'lib', // event_sourcing
  '../reaction/lib',
];

final _flutterImport = RegExp(r'''import\s+['"]package:flutter/''');
final _platformLocked = RegExp(
  r'''import\s+['"]dart:(io|html|ffi|js|js_interop|js_util)\b''',
);

Iterable<File> _dartFiles(String dir) sync* {
  final d = Directory(dir);
  if (!d.existsSync()) return;
  for (final e in d.listSync(recursive: true)) {
    if (e is File && e.path.endsWith('.dart')) yield e;
  }
}

void main() {
  test('A: no core lib/ source imports package:flutter', () {
    final offenders = <String>[];
    for (final dir in _coreLibDirs) {
      for (final f in _dartFiles(dir)) {
        if (_flutterImport.hasMatch(f.readAsStringSync())) {
          offenders.add(f.path);
        }
      }
    }
    expect(offenders, isEmpty, reason: 'package:flutter in core lib/: $offenders');
  });

  test('B: no core lib/ source imports a platform-locked dart: library', () {
    final offenders = <String>[];
    for (final dir in _coreLibDirs) {
      for (final f in _dartFiles(dir)) {
        if (_platformLocked.hasMatch(f.readAsStringSync())) {
          offenders.add(f.path);
        }
      }
    }
    expect(offenders, isEmpty, reason: 'platform-locked dart: import: $offenders');
  });
}
```

- [ ] **Step 2: Run the test**

```bash
( cd event_sourcing && flutter test test/portability_purity_test.dart )
```
Expected: PASS. **If B fails** because a core lib legitimately needs `dart:io` (e.g. a Sembast IO factory) — that means the file uses a conditional-import shim. In that case refine test B to allow files that use the `if (dart.library.io)` / `if (dart.library.html)` conditional-export pattern (detect a sibling `*_io.dart` + `*_web.dart` + stub), and assert only that no *unconditional* platform-locked import exists. Adjust and re-run until green; the assertion is "compiles on every runtime", which conditional imports satisfy.

- [ ] **Step 3: Refresh, verify, commit**

`mcp__elspais__refresh_graph` then `mcp__elspais__get_test_coverage(req_id="EVS-PRD-portability")` → expected A,B,C,D covered.
```bash
git add event_sourcing/test/portability_purity_test.dart
git commit --no-verify -m "[CUR-1528] Add portability purity scans (no flutter / no platform-locked imports in core lib)"
```

---

### ~~Task 7: Confirm `EVS-PRD-library-charter/B` and `/F` covered by refinement~~ — RESOLVED BY PROPAGATION (no action)

> All 9 charter assertions (incl. B, F, and the out-of-scope G, I) propagated covered from their children. Verified 2026-06-20. No annotation needed. Steps kept for record only.

#### (superseded) Task 7 detail

**Run this task LAST among the coverage tasks** — charter B's refinement chain includes `EVS-PRD-view-subscriber`, whose assertion E is closed in Task 8, so do Task 8 first. With `refines:` propagation, charter B (← subscription + view-subscriber + materializer) and charter F (← canonical-json + provenance) should already be covered once their children are. This task verifies that and only falls back to direct annotation if propagation didn't fire.

**Files:**
- (Fallback only) Modify: a subscription+materializer integration test (e.g. `event_sourcing/test/subscriptions/aggregate_mode_test.dart`) — add `/B`; `canonical_json_jcs/test/canonical_json_test.dart` + `event_sourcing/example/test/portal_sync_test.dart` — add `/F`.

**Interfaces:**
- Consumes: full coverage of subscription, view-subscriber (Task 8), materializer, canonical-json, provenance.
- Produces: `EVS-PRD-library-charter/{B,F}` test-covered. (G, I stay uncovered by design.)

- [ ] **Step 1: Refresh and check whether propagation already covered B and F**

`mcp__elspais__refresh_graph(full=true)` then `mcp__elspais__get_test_coverage(req_id="EVS-PRD-library-charter")`.
- If `covered_assertions` already includes B and F (only G, I uncovered) → **propagation worked; Task 7 is done, no edits, no commit.** Skip to Task 9.
- If B and/or F are still uncovered → propagation isn't live in this environment for these; proceed to Step 2 for the missing one(s).

- [ ] **Step 2 (fallback): Annotate the strongest existing host tests**

Only for whichever of B/F is still uncovered. For B, add to `event_sourcing/test/subscriptions/aggregate_mode_test.dart` header:
```dart
// Verifies: EVS-PRD-library-charter/B — event + materialized-state updates are
//   delivered reactively to subscribers (realized via the AggregateMode path).
```
For F, add to `canonical_json_jcs/test/canonical_json_test.dart`:
```dart
// Verifies: EVS-PRD-library-charter/F — canonical-form serialization yields
//   byte-identical output (the serialization half of charter F).
```
and to `event_sourcing/example/test/portal_sync_test.dart`:
```dart
// Verifies: EVS-PRD-library-charter/F — provenance chain remains traceable and
//   byte-identical across a cross-tier sync (the traceability half of charter F).
```

- [ ] **Step 3 (fallback): Refresh, verify, run, commit**

`mcp__elspais__refresh_graph` then `get_test_coverage(req_id="EVS-PRD-library-charter")` → expected B,F covered; uncovered = G, I only.
```bash
( cd canonical_json_jcs && flutter test test/canonical_json_test.dart )
( cd event_sourcing/example && flutter test test/portal_sync_test.dart )
git add event_sourcing/test/subscriptions canonical_json_jcs/test/canonical_json_test.dart event_sourcing/example/test/portal_sync_test.dart
git commit --no-verify -m "[CUR-1528] Bind library-charter/B and /F (refinement fallback annotations)"
```

---

### ~~Task 8: Verify `scoped-permissions/A` and `view-subscriber/E`~~ — RESOLVED BY PROPAGATION (no action)

> scoped-permissions/A propagated from `scope-class-registry-validation`; view-subscriber/E propagated from `reaction-scope`. Both verified covered 2026-06-20. Steps kept for record only.

#### (superseded) Task 8 detail

These two single-assertion gaps were in the ticket's in-scope list. `scoped-permissions/A` (composition-time `ScopeClassRegistry`; action scoped-permission refers to a scope class by registered name) and `view-subscriber/E` (the `watch<T>` contract is shaped for additive snapshot-delivery evolution). The other 8 scoped-permissions assertions are covered, suggesting A is an annotation gap; E is a design/contract assertion best pinned by a sealed-type/shape test.

**Files:**
- Modify (likely): a scoped-permissions test under `event_sourcing/test/` (registry validation) — add `/A` if a registration test exists.
- Modify or Create: `reaction/test/interfaces/view_source_test.dart` — add `/E` (annotate or add a sealed-`Update<T>`-shape test).

**Interfaces:**
- Consumes: convention from Task 1; the `Update<T>` variant set (`Snapshot`/`Delta`/`Tombstone`/`EndOfReplay`).
- Produces: `EVS-PRD-scoped-permissions/A` and `EVS-PRD-view-subscriber/E` test-covered.

- [ ] **Step 1: scoped-permissions/A — check refinement first, then annotate/write**

`EVS-PRD-scoped-permissions` has DEV children including `EVS-DEV-scope-class-registry-validation` (composition-time registry) which refines A. First `refresh_graph` + `get_test_coverage(req_id="EVS-PRD-scoped-permissions")`: if A is already covered (propagation from the covered DEV child), **done — no edit.** Otherwise: search `event_sourcing/test` for the `ScopeClassRegistry` / scope-class-by-name registration test. If a test exercises "an action's scoped permission refers to a scope class by registered name", add `// Verifies: EVS-PRD-scoped-permissions/A` to it; if none exists, write a focused one that registers a scope class and asserts an action's scoped `Permission` resolves the class by its registered name. Refresh + verify A covered.

- [ ] **Step 2: view-subscriber/E — pin the additive-evolution contract**

In `reaction/test/interfaces/view_source_test.dart`, add a sealed-type exhaustiveness test that pins the `Update<T>` variant set (the property that makes batched/cursor delivery an additive change), then annotate it:
```dart
// Verifies: EVS-PRD-view-subscriber/E — the watch<T> contract (Stream<Update<T>>,
//   the sealed Update<T> variant set, the mapper signature) is shaped so batched/
//   cursor snapshot delivery is a purely additive evolution.

test('Update<T> is a sealed type with exactly the four contracted variants', () {
  const updates = <Update<int>>[
    Snapshot<int>(/* ...as constructed elsewhere in this file/codec tests... */),
    Delta<int>(/* ... */),
    Tombstone<int>(/* ... */),
    EndOfReplay<int>(/* ... */),
  ];
  for (final u in updates) {
    final tag = switch (u) {
      Snapshot<int>() => 'snapshot',
      Delta<int>() => 'delta',
      Tombstone<int>() => 'tombstone',
      EndOfReplay<int>() => 'endOfReplay',
    };
    expect(tag, isNotEmpty);
  }
});
```
(Copy the concrete constructor calls from `reaction/test/wire/update_codec_test.dart`, which already builds all four variants.) The exhaustive `switch` with no `default` is the compile-time proof the variant set is closed.

- [ ] **Step 3: Refresh, verify, run, commit**

`mcp__elspais__refresh_graph`; `get_test_coverage` for both requirements → A / E covered.
```bash
( cd reaction && flutter test test/interfaces/view_source_test.dart )
( cd event_sourcing && flutter test test/permissions/ test/actions/ )
git add reaction/test/interfaces/view_source_test.dart event_sourcing/test
git commit --no-verify -m "[CUR-1528] Bind scoped-permissions/A and view-subscriber/E"
```

---

### Task 9: Final whole-graph verification, out-of-scope fencing, and PR

**Files:**
- (No code) — verification + Linear/PR.

- [ ] **Step 1: Full graph health + uncovered check**

`mcp__elspais__refresh_graph(full=true)`, then:
- `mcp__elspais__get_uncovered_assertions(source="test")` — expected: only `EVS-PRD-multi-source-canonicalization/A–F` remains (charter G/I are now propagation-covered, accepted). Everything else from the starting list is gone.
- `mcp__elspais__get_graph_status` — expected `has_orphans: false`, `has_broken_references: false`.

- [ ] **Step 2: Run the full suites per package**

```bash
( cd canonical_json_jcs && flutter test )
( cd provenance && flutter test )
( cd event_sourcing && flutter test )
( cd reaction && flutter test )
( cd reaction_widgets && flutter test )
( cd reaction_widgets_testing && flutter test )
```
Expected: all green (Postgres-gated tests skip cleanly without `PG_TEST_URL`; run them once against a live DB if available). Record any skips.

- [ ] **Step 3: Record the out-of-scope + propagation notes**

In the PR body: (a) list the one intentionally-uncovered requirement — `multi-source-canonicalization` (dormant Phase-II); (b) note that charter/G, charter/I, portability/A,B, view-subscriber/E, reaction-scope/C, and scoped-permissions/A were closed by elspais `refines:` propagation rather than direct tests, and that this coarse (requirement-level) propagation is the accepted behavior — assertion-targeted `refines:` edges are the path to finer precision and are out of scope for this ticket.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin CUR-1528-test-coverage-gaps
env -u GITHUB_TOKEN gh pr create --title "[CUR-1528] Close requirement test-coverage gaps (assertion-level)" --body "<summary + out-of-scope list from Step 3>"
```

---

## Self-Review

- **Spec coverage vs ticket in-scope list:** find-all-events A–D → Task 3; flow-token A–D → Task 5; postgres-backend B,C → Task 4; portability A,B → Task 6; reaction-scope C → Task 2; scoped-permissions A → Task 8; view-subscriber E → Task 8; reaction-widget-contract A,B,D,F,G,H,I,J,K → Task 1; library-charter B,F → Task 7. Out-of-scope (multi-source, charter G/I) → fenced in Task 9. All ticket items mapped.
- **Acceptance criteria:** "all in-scope move to covered" → Task 9 Step 1; "new tests pass per runner" → per-task runs + Task 9 Step 2; "no regressions" → Task 9 Step 2; "0 orphans / 0 broken refs" → Task 9 Step 1; "PR title `[CUR-1528]`" → Task 9 Step 4.
- **Binding-rule consistency:** every annotation task uses fully-qualified refs (Global Constraints), the rule confirmed empirically against the working `connection_status_test`/`remote_scope_test` headers vs the broken `local_scope_test` header.
- **Residuals flagged, not hidden:** flow-token/D secret-exclusion (consumer obligation) and portability/B true-multi-runtime (CI matrix) are documented in their test headers rather than silently claimed.
