# elspais Annotation Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development`. Each task is independent and sized for one fresh subagent (~30 min).

**Goal:** Drive `elspais checks` from the current state (15/103 PRD assertions implemented at the assertion level, 263 unlinked files) to **100% of code and test files annotated at assertion-level precision**, with the citation graph rolling up cleanly to `EVS-PRD-library-charter`.

**End state per `elspais summary`:**

- PRD assertions implemented: 103/103 (100%)
- PRD assertions validated: high % (some tests may be impl-internal and not map cleanly to PRD assertions; aim for ≥80% direct, balance via indirect)
- DEV assertions implemented + validated: 27/27
- `code.unlinked` = 0
- `tests.unlinked` = 0
- 0 stale `REQ-d{NNNNN}` annotations remaining
- 0 broken references, 0 format violations

**Architecture:** The annotation surface decomposes into independent subsystems (per `event_sourcing/lib/src/` subdir + the sibling packages). Each task annotates one subsystem's lib + tests against a known set of PRDs. The REQ-d sweep is independent and can be parallelized.

**Tech stack:** Dart 3.x source files; `// Implements:` / `// Verifies:` comments at column 0. No code behaviour changes.

**Spec context:** Builds on PR #11 (which annotated the 19 reaction files at assertion level and authored 7 DEV specs). Same discipline applied to the rest of the codebase.

---

## Common Preamble (read once per task)

### Working directory

`/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing` (or your active worktree on this branch).

### Branch

A task may either run on a feature branch off this plan's parent (post-merge), or on a stacked branch. Naming convention: `CUR-1317-annotate-<subsystem>` (e.g., `CUR-1317-annotate-actions`).

### Commit message style

`[CUR-1317] Annotate <subsystem> at assertion level` (or `[CUR-1317] Retire stale REQ-d annotations`). Body explains the file→assertion mapping for non-trivial choices.

### Pre-commit + pre-push

Pre-commit handles whitespace/format/secret-scan. Pre-push runs `elspais checks --lenient` — your changes need to drop the unlinked count for your subsystem to 0 AND introduce no broken references. The hook does NOT enforce assertion-level precision; verify manually via `elspais summary` before opening the PR.

### Discipline

- **One subagent per task.** Don't carry context across subsystems — each one is independent.
- **Read the relevant PRD(s) before writing annotations.** Map each file to its specific assertions, not to a whole PRD if the file only realizes a subset.
- **Annotations are at column 0**, top-of-file (above `import` statements). Use `// Implements:` for `lib/`, `// Verifies:` for `test/`. Multi-assertion form is `EVS-PRD-foo/A/B/C` (slash separator).
- **Whole-req citations are acceptable** when a file genuinely realizes every assertion of its PRD. They expand to all assertions in elspais's coverage rollup. But prefer assertion-level precision where a file's scope is narrower.
- **Stale REQ-d annotations** (`// Verifies: REQ-d{NNNNN}-X`, ~238 occurrences) are kick-start debt. Either delete them outright (if the requirement is gone with the diary code) OR rebind them to the appropriate EVS-* ID. Task 1 handles this.
- **Test fixtures and helpers** that aren't themselves tests (e.g., `test_support/*.dart`) don't need annotations. Skip them.

### How to map a file to assertions

1. Read the file's purpose (header dartdoc, class names, public API).
2. Read the candidate PRD(s) — usually one, sometimes two (e.g., `EVS-PRD-event-log` and `EVS-PRD-portability` for `StorageBackend`).
3. Identify which specific assertions the file realizes. Skip assertions about Remote/wire/server impls if you're annotating in-process code.
4. Pick the most specific (assertion-level) citation. Prefer 1-3 specific assertions over a whole-req fallback.

If the file's scope spans multiple PRDs, list each on its own `// Implements:` line — they're independent edges.

---

## Subsystem → PRD Mapping (use as a starting point, refine per task)

| Subsystem | Primary PRDs | DEV specs |
|---|---|---|
| `event_sourcing/lib/src/actions/` (18 files) | `EVS-PRD-action-dispatch`, `EVS-PRD-permissions-as-events` | `EVS-DEV-append-stamps-registered-version` (callers) |
| `event_sourcing/lib/src/core/` (3 files) | `EVS-PRD-library-charter` | — |
| `event_sourcing/lib/src/destinations/` (6 files) | `EVS-PRD-destinations` | — |
| `event_sourcing/lib/src/ingest/` (4 files) | `EVS-PRD-ingest`, `EVS-PRD-hash-chain-integrity` | `EVS-DEV-ingest-promotes-before-fold` |
| `event_sourcing/lib/src/permissions/` (16 files) | `EVS-PRD-permissions-as-events` | — |
| `event_sourcing/lib/src/projections/` (12 files) | `EVS-PRD-materializer` | `EVS-DEV-snapshot-promotion-on-open`, `EVS-DEV-ingest-promotes-before-fold`, `EVS-DEV-view-target-versions-seeding` |
| `event_sourcing/lib/src/promoters/` (4 files) | `EVS-PRD-materializer` | — |
| `event_sourcing/lib/src/security/` (6 files) | `EVS-PRD-event-log`, `EVS-PRD-regulatory-alignment` | `EVS-DEV-event-store-open` (boot-version event types) |
| `event_sourcing/lib/src/storage/` (14 files) | `EVS-PRD-event-log`, `EVS-PRD-portability` | `EVS-DEV-find-all-events-extended-filters`, `EVS-DEV-append-stamps-registered-version` |
| `event_sourcing/lib/src/subscriptions/` (3 files) | `EVS-PRD-subscription` | — |
| `event_sourcing/lib/src/sync/` (5 files) | `EVS-PRD-destinations` | — |
| `event_sourcing/lib/event_sourcing.dart` (barrel) | `EVS-PRD-library-charter` | — |
| `event_sourcing/lib/src/bootstrap.dart` (top-level) | `EVS-PRD-library-charter` | `EVS-DEV-event-store-open` |
| `event_sourcing/lib/src/event_store.dart` (top-level) | `EVS-PRD-event-log` | `EVS-DEV-event-store-open`, `EVS-DEV-append-stamps-registered-version`, `EVS-DEV-snapshot-promotion-on-open`, `EVS-DEV-entry-type-downgrade-refusal` |
| `event_sourcing/lib/src/event_draft.dart` etc. (top-level value types) | `EVS-PRD-event-log` | — |
| `canonical_json_jcs/lib/` | `EVS-PRD-canonical-json` | — |
| `provenance/lib/` | `EVS-PRD-provenance` | — |

Test directories mirror the lib structure; the same PRD mapping applies, but `// Verifies:` instead of `// Implements:`.

---

## Task 1: Retire stale `REQ-d{NNNNN}` annotations

**Files:** All Dart files in `event_sourcing/`, `canonical_json_jcs/`, `provenance/` (read-only scope; no `reaction/` impact — that package post-dates the kick-start).

**Why first?** These are noise in the graph. Removing them clears the audit baseline so per-subsystem annotation work can be evaluated cleanly.

**Approach:**

1. Run `grep -rn "REQ-d[0-9]" event_sourcing canonical_json_jcs provenance --include="*.dart"` to inventory all occurrences (~238 expected).
2. For each annotation, classify:
   - **Pure deletion:** The cited requirement was diary-domain and the citing code is now unreachable from any EVS-* requirement. Delete the annotation line.
   - **Rebind:** The citing code remains and maps to a current EVS-* requirement. Replace the `// Implements: REQ-dNNNNN-X — <note>` line with `// Implements: EVS-<level>-<component>/<assertion> — <preserved-note>`.
3. Where rebinding is non-obvious, prefer deletion (leaves the file in the per-subsystem task's unlinked queue for that task to address with full context).
4. The annotation TEXT after the ID (e.g., `— stamps the entry type version`) can usually be preserved verbatim when rebinding.

**Verification:**

- `grep -c "REQ-d[0-9]" event_sourcing canonical_json_jcs provenance --include="*.dart"` returns 0.
- `elspais checks` reports no NEW broken references.
- `cd event_sourcing && flutter test` still passes (annotations are pure comments — should be a no-op for runtime).

**Commit:** `[CUR-1317] Retire stale REQ-d{NNNNN} annotations from kick-start commit`

---

## Tasks 2-N: Per-subsystem annotation

Each task follows the same shape. The subagent prompt template:

```text
You are annotating the {SUBSYSTEM} subsystem with EVS-PRD-* and EVS-DEV-* traceability.

Files:
- {SUBSYSTEM}/<lib path>/*.dart
- {SUBSYSTEM}/<test path>/*.dart (mirroring tests if applicable)

Relevant PRDs (load via `mcp__elspais__get_requirement(req_id)` or read from `spec/<file>.md`):
- {PRIMARY PRD 1}
- {PRIMARY PRD 2}
- {RELEVANT DEV SPECS}

For each file:
1. Read the file's dartdoc + public API to understand its scope.
2. Match it to specific assertions from the relevant PRDs.
3. Add a `// Implements:` (lib) or `// Verifies:` (test) comment at column 0, top-of-file, citing the specific assertions (e.g., `EVS-PRD-event-log/A/C`).
4. Where multiple PRDs apply (e.g., StorageBackend impl realizes both event-log + portability), use multi-line annotations.

Acceptance:
- All files in the subsystem annotated.
- `elspais checks` reports 0 broken references.
- Subsystem's unlinked count drops to 0 (verify via `elspais unlinked | grep {SUBSYSTEM}`).
- `cd event_sourcing && flutter test test/{SUBSYSTEM}/` passes.
- Commit message uses `[CUR-1317] Annotate {SUBSYSTEM} at assertion level`.

Pre-commit gotcha: see common preamble. Stage only files in your subsystem.
```

### Tasks (one per subsystem)

- [ ] **Task 2: `event_sourcing/lib/src/actions/` + tests** (18 lib + 17 test files; PRDs: action-dispatch, permissions-as-events)
- [ ] **Task 3: `event_sourcing/lib/src/destinations/` + tests** (6 lib + 11 test; PRD: destinations)
- [ ] **Task 4: `event_sourcing/lib/src/ingest/` + tests** (4 lib + 15 test; PRDs: ingest, hash-chain-integrity; DEV: ingest-promotes-before-fold)
- [ ] **Task 5: `event_sourcing/lib/src/permissions/` + tests** (16 lib + 13 test; PRD: permissions-as-events)
- [ ] **Task 6: `event_sourcing/lib/src/projections/` + tests** (12 lib + 11 test; PRD: materializer; DEV: snapshot-promotion-on-open, ingest-promotes-before-fold, view-target-versions-seeding)
- [ ] **Task 7: `event_sourcing/lib/src/promoters/`** (4 lib; PRD: materializer; substrate-internal, may have no tests of its own)
- [ ] **Task 8: `event_sourcing/lib/src/security/` + tests** (6 lib + 5 test; PRDs: event-log, regulatory-alignment; DEV: event-store-open for system_entry_types.dart)
- [ ] **Task 9: `event_sourcing/lib/src/storage/` + tests** (14 lib + 19 test; PRDs: event-log, portability; DEVs: find-all-events-extended-filters, append-stamps-registered-version)
- [ ] **Task 10: `event_sourcing/lib/src/subscriptions/` + tests** (3 lib + 4 test; PRD: subscription)
- [ ] **Task 11: `event_sourcing/lib/src/sync/` + tests** (5 lib + 7 test; PRD: destinations)
- [ ] **Task 12: `event_sourcing/lib/src/core/`** (3 lib; PRD: library-charter — these are foundational error/config types)
- [ ] **Task 13: Top-level files** — `event_sourcing.dart` (barrel; PRD: library-charter), `bootstrap.dart` (DEV: event-store-open), `event_store.dart` (PRD: event-log; multiple DEVs), `event_draft.dart`, `entry_type_definition.dart`, `entry_type_registry.dart`
- [ ] **Task 14: `event_sourcing/test/event_store/` tests** (9 test files; standalone tests that don't map to a single lib subdir; verify against EVS-PRD-event-log + EVS-DEV-event-store-open)
- [ ] **Task 15: `event_sourcing/example/` test files** (8 test; map to whichever PRDs the demo exercises — likely event-log + subscription + destinations)
- [ ] **Task 16: `event_sourcing/example_action_permissions/` test files** (31 test; PRD: action-dispatch + permissions-as-events; this is the actions+permissions reference app)
- [ ] **Task 17: `canonical_json_jcs/lib/` + `test/`** (small package; PRD: canonical-json)
- [ ] **Task 18: `provenance/lib/` + `test/`** (small package; PRD: provenance)

---

## Task 19: Final verification + status promotion

After Tasks 1-18 land:

1. Run `elspais checks --format text` — expect zero failed checks, zero broken references.
2. Run `elspais summary` — confirm:
   - PRD implemented + validated each ≥80% (allowing for some untestable assertions like Remote-impl-only ones)
   - DEV implemented + validated each ≥80%
   - `code.unlinked = 0`, `tests.unlinked = 0`
3. Promote requirements from `Draft` to `Active` where the implementation is complete. The `[changelog]` section in `.elspais.toml` already requires a changelog entry per Active req — check that each PRD has one before promoting.
4. Optionally: tighten the pre-push hook by dropping `--lenient` from `.pre-commit-config.yaml`. This makes coverage-gap warnings block pushes — useful only when coverage is comprehensive.
5. Update `docs/superpowers/specs/2026-05-11-roadmap.md` to mark this plan complete.

**Commit:** `[CUR-1317] Promote shipped requirements to Active; close annotation sweep`

---

## Self-Review Checklist (per task)

- [ ] All files in the subsystem have a top-of-file `// Implements:` or `// Verifies:` annotation at column 0.
- [ ] Each annotation cites specific assertions (`EVS-PRD-foo/A/B`) where the file's scope is narrower than the whole PRD.
- [ ] `elspais checks` reports zero broken references for the modified files.
- [ ] `elspais unlinked | grep <subsystem>` returns no hits.
- [ ] Relevant test suite still passes (`flutter test`).
- [ ] Commit body lists the file→assertion mapping for non-trivial choices.
- [ ] No `REQ-d{NNNNN}` annotations remain in the modified files (Task 1 should have already cleared them; verify per subsystem).

---

## Notes on workflow

- **Parallelizable:** Tasks 2-18 can run in any order; they don't share files (one file = one subsystem). Tasks 2-11 + 14-16 (lib subdirs + test subdirs) are best paired (lib + matching tests in one task).
- **Order recommendation:** Start with Task 1 (REQ-d sweep) to clear noise. Then prioritize subsystems by either size (smallest first to build momentum) or strategic value (storage + projections + actions first, since they're the substrate's load-bearing surface).
- **One commit per subsystem.** Aim for 30-50 line diffs per file modified; subsystems may produce 200-500 line commit totals (mostly comment additions).
- **The .elspais.toml `[[scanning.test.runners]]` entries** (per-package `flutter test`) can be invoked via `elspais checks --run-tests` to also produce result file ingestion for line-coverage % — currently unconfigured, optional follow-up.
