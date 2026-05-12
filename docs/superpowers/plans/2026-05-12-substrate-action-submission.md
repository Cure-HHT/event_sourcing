# Substrate ActionSubmission Value Type Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `ActionSubmission` as a value type in the substrate (`event_sourcing` package) and refactor `ActionDispatcher.dispatch` to take it. The dispatcher's existing positional `(actionName, rawInput, ctx, {idempotencyKey, flowToken})` signature becomes `(ActionSubmission, ActionContext)`. Symmetric with the already-existing `DispatchResult` value type on the output side.

**Architecture:** New value class `ActionSubmission` lives in `event_sourcing/lib/src/actions/action_submission.dart` and is exported from the public barrel. Bundles `actionName`/`rawInput`/`idempotencyKey`/`flowToken`. `ActionContext` stays a separate argument because the calling code (in-process callers and `reaction`'s `LocalActionSubmitter`) constructs ctx from Principal+timing — it's not "submission data" but "request context." All ~14 dispatch call sites across the substrate, examples, and walkthrough tests update atomically.

**Tech Stack:** Dart 3.x sealed/value types. No new dependencies. Tests use `flutter_test` (matches existing event_sourcing pattern).

**Spec context:** Greenfield refactor under CUR-1317 libification. Drives `spec/prd-reaction.md`'s EVS-PRD-action-submitter Assertion A ("submit(ActionSubmission)"). After this plan lands, `reaction`'s `ActionSubmitter` interface imports `ActionSubmission` from `event_sourcing` instead of defining its own copy.

**Scope check:** This plan is **inserted between Plan A (`EndOfReplay<T>`)** and **Plan B-local (`reaction` package)**. Plan B-local has Tasks 1-3 already committed (skeleton + AuthSession interface + ActionSubmitter interface with a temporarily-local `ActionSubmission` at commit `e2a8fb3`). Plan B-local's resumption deletes that local definition and imports the substrate's.

---

## Common Preamble

### Working directory

`/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing`

### Branch

`CUR-1317-libify-event-sourcing` (already checked out)

### Commit message style

`[CUR-1317] <subject>` with a body describing what + why. Reference `spec/prd-reaction.md` PRD IDs where applicable.

### Pre-commit gotcha

The repo has substantial unstaged WIP — currently ~17 modified spec files plus `.elspais.toml`, `docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md`, untracked `.elspais/` dir, and untracked `spec/INDEX.md`. **Do not modify or commit any of these.** Use `git diff --name-only | grep -v <your-paths>` to enumerate WIP files for stashing, or be specific with `git add` (no `git add -A`).

If pre-commit fails on a WIP file's pre-existing markdownlint:

1. `git stash push -m "WIP" -- $(git diff --name-only)`
2. Make your commit
3. `git stash pop`

DO NOT use `--no-verify`.

### Discipline

- TDD: write failing test first, then minimal implementation
- Greenfield mode: no backwards-compat shims; signature changes touch all call sites in one commit so the build stays green
- One commit per task

---

## File Structure

Files touched:

| File | Action | Why |
|---|---|---|
| `event_sourcing/lib/src/actions/action_submission.dart` | Create | The new value type |
| `event_sourcing/lib/event_sourcing.dart` | Modify (export) | Make `ActionSubmission` public |
| `event_sourcing/lib/src/actions/action_dispatcher.dart` | Modify (signature) | `dispatch(ActionSubmission, ActionContext)` |
| `event_sourcing/test/actions/action_submission_test.dart` | Create | Unit tests for the new value type |
| `event_sourcing/test/actions/action_dispatcher_test.dart` | Modify (~20 call sites) | Update to new signature |
| `event_sourcing/test/actions/integration_test.dart` | Modify (~7 call sites) | Update to new signature |
| `event_sourcing/test/actions/bootstrap_audited_actions_test.dart` | Modify (~1 call site) | Update to new signature |
| `event_sourcing/example_action_permissions/lib/server/demo_routes.dart` | Modify | Server-side dispatch routes |
| `event_sourcing/example_action_permissions/test/walkthroughs/test_support/demo_server_harness.dart` | Modify | Walkthrough harness's dispatch wrapper |
| `event_sourcing/example_action_permissions/test/walkthroughs/walkthrough_*.dart` | Modify (~9 files) | Tests calling `harness.dispatch` — only if harness's signature changed |
| `reaction/lib/src/interfaces/action_submitter.dart` | Modify (Task 3) | Remove local ActionSubmission; import from event_sourcing |

---

## Task 1: Define `ActionSubmission` value type + export

**Files:**

- Create: `event_sourcing/lib/src/actions/action_submission.dart`
- Create: `event_sourcing/test/actions/action_submission_test.dart`
- Modify: `event_sourcing/lib/event_sourcing.dart` (one line in the actions-section export block)

- [ ] **Step 1: Write the failing test**

Create `event_sourcing/test/actions/action_submission_test.dart`:

```dart
import 'package:event_sourcing/src/actions/action_submission.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('ActionSubmission', () {
    test('required fields are populated', () {
      const s = ActionSubmission(
        actionName: 'submit_note',
        rawInput: {'title': 'hello'},
      );
      expect(s.actionName, 'submit_note');
      expect(s.rawInput, equals({'title': 'hello'}));
      expect(s.idempotencyKey, isNull);
      expect(s.flowToken, isNull);
    });

    test('optional fields can be supplied', () {
      const s = ActionSubmission(
        actionName: 'submit_note',
        rawInput: {'title': 'hello'},
        idempotencyKey: 'k-42',
        flowToken: 't-7',
      );
      expect(s.idempotencyKey, equals('k-42'));
      expect(s.flowToken, equals('t-7'));
    });

    test('is a const value type (identical for same args)', () {
      const a = ActionSubmission(
        actionName: 'a',
        rawInput: <String, Object?>{},
      );
      const b = ActionSubmission(
        actionName: 'a',
        rawInput: <String, Object?>{},
      );
      expect(identical(a, b), isTrue);
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd event_sourcing && flutter test test/actions/action_submission_test.dart
```

Expected: "Undefined name 'ActionSubmission'".

- [ ] **Step 3: Create `event_sourcing/lib/src/actions/action_submission.dart`**

```dart
/// The complete input to one [ActionDispatcher.dispatch] call.
///
/// Bundles the action name, raw input, and optional idempotency/flow
/// correlation fields. The substrate's [ActionContext] is passed as a
/// SEPARATE argument to `dispatch`, not carried on this submission —
/// the caller owns Principal construction and timing.
///
/// Symmetric with [DispatchResult] on the output side.
class ActionSubmission {
  /// Registered name of the action to dispatch (e.g. `'submit_note'`).
  /// Matches what `ActionRegistry.lookup` accepts.
  final String actionName;

  /// The raw input the dispatcher's Stage 3 (parse) consumes. Shape is
  /// per-action and verified by the registered action's `parseInput`.
  final Map<String, Object?> rawInput;

  /// Idempotency key per the registered action's `IdempotencyPolicy`.
  /// `null` is valid when the action's policy is `none` or `optional`.
  /// For policy `required`, omitting the key causes the dispatcher to
  /// return a `parse_denied` outcome (Stage pre-3 precondition check;
  /// see REQ-d00170-B in action_dispatcher.dart's docs).
  final String? idempotencyKey;

  /// Optional cross-action correlation token. The dispatcher stamps it
  /// onto every emitted event's metadata so downstream audit can trace
  /// related actions across a single user flow.
  final String? flowToken;

  const ActionSubmission({
    required this.actionName,
    required this.rawInput,
    this.idempotencyKey,
    this.flowToken,
  });
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd event_sourcing && flutter test test/actions/action_submission_test.dart
```

Expected: 3/3 pass.

- [ ] **Step 5: Export from the public barrel**

Modify `event_sourcing/lib/event_sourcing.dart`. Locate the actions-section export block (search for `export 'src/actions/`). Add the new export in alphabetical order:

```dart
export 'src/actions/action_submission.dart' show ActionSubmission;
```

If there's already an export of, e.g., `action_registry.dart`, place the new line so the section stays alphabetical.

- [ ] **Step 6: Verify the barrel compiles**

```bash
cd event_sourcing && flutter analyze lib/event_sourcing.dart
```

Expected: no new errors. If there are existing info-level lints in the file, no change in count.

- [ ] **Step 7: Commit**

```bash
git add event_sourcing/lib/src/actions/action_submission.dart \
        event_sourcing/test/actions/action_submission_test.dart \
        event_sourcing/lib/event_sourcing.dart
git commit -m "[CUR-1317] Add ActionSubmission value type to substrate

Bundles actionName/rawInput/idempotencyKey/flowToken for one dispatch
call. Symmetric with DispatchResult on the output side.
ActionContext stays a separate argument to dispatch — callers own
Principal construction and timing.

Refactoring ActionDispatcher.dispatch to take ActionSubmission lands
in the next task, atomically with all call sites.

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter A — drives this
type's existence as the parameter to reaction's ActionSubmitter.submit)."
```

---

## Task 2: Refactor `ActionDispatcher.dispatch` signature + update all call sites

**Files:**

- Modify: `event_sourcing/lib/src/actions/action_dispatcher.dart` (signature change)
- Modify: `event_sourcing/test/actions/action_dispatcher_test.dart` (~20 call sites)
- Modify: `event_sourcing/test/actions/integration_test.dart` (~7 call sites)
- Modify: `event_sourcing/test/actions/bootstrap_audited_actions_test.dart` (~1 call site)
- Modify: `event_sourcing/example_action_permissions/lib/server/demo_routes.dart` (server dispatch sites)
- Modify: `event_sourcing/example_action_permissions/test/walkthroughs/test_support/demo_server_harness.dart` (the harness's dispatch wrapper)

All changes go in ONE commit because Dart's type system requires the signature and all call sites to be consistent at every compile boundary.

The transformation pattern is mechanical:

**Before:**

```dart
dispatcher.dispatch(
  'action_name',
  const <String, Object?>{...},
  ctx,
  idempotencyKey: 'k',
  flowToken: 't',
);
```

**After:**

```dart
dispatcher.dispatch(
  const ActionSubmission(
    actionName: 'action_name',
    rawInput: {...},
    idempotencyKey: 'k',
    flowToken: 't',
  ),
  ctx,
);
```

When `idempotencyKey` and `flowToken` are not supplied, omit them from the constructor:

```dart
dispatcher.dispatch(
  const ActionSubmission(
    actionName: 'nope',
    rawInput: <String, Object?>{},
  ),
  ctx,
);
```

- [ ] **Step 1: Update the dispatcher's signature**

In `event_sourcing/lib/src/actions/action_dispatcher.dart`, find the `dispatch` method (currently at line ~83):

**Before:**

```dart
Future<DispatchResult<Object?>> dispatch(
  String actionName,
  Map<String, Object?> rawInput,
  ActionContext ctx, {
  String? idempotencyKey,
  String? flowToken,
}) async {
```

**After:**

```dart
Future<DispatchResult<Object?>> dispatch(
  ActionSubmission submission,
  ActionContext ctx,
) async {
  // Unpack for the existing pipeline body (zero-cost destructure).
  final actionName = submission.actionName;
  final rawInput = submission.rawInput;
  final idempotencyKey = submission.idempotencyKey;
  final flowToken = submission.flowToken;
```

The unpacking lines preserve the existing method body unchanged — all local references to `actionName`/`rawInput`/`idempotencyKey`/`flowToken` continue to work without further edits.

Add the import at the top of the file:

```dart
import 'package:event_sourcing/src/actions/action_submission.dart';
```

- [ ] **Step 2: Update `action_dispatcher_test.dart` call sites**

Open `event_sourcing/test/actions/action_dispatcher_test.dart`. Search for `dispatcher.dispatch(` (~20 hits). For each, transform per the pattern above.

Add the import at the top:

```dart
import 'package:event_sourcing/src/actions/action_submission.dart';
```

(If the file already imports from `package:event_sourcing/event_sourcing.dart`, the barrel export from Task 1 makes the import unnecessary — but explicit `src/` imports keep test files self-contained.)

Also update the `allowDispatcher` and any other dispatcher-construction call (line ~330, ~271): the constructor signature itself doesn't change, just the dispatch CALLS.

- [ ] **Step 3: Update `integration_test.dart` call sites (~7 hits)**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
grep -n "dispatcher\.dispatch" event_sourcing/test/actions/integration_test.dart
```

For each match, apply the transformation. Add the import if needed.

- [ ] **Step 4: Update `bootstrap_audited_actions_test.dart` (~1 hit)**

Same transformation as above.

- [ ] **Step 5: Update `demo_routes.dart`**

The example_action_permissions server's route handlers call `dispatcher.dispatch(...)` from inside HTTP POST handlers. Find these sites and update.

- [ ] **Step 6: Update walkthrough `demo_server_harness.dart`**

Open `event_sourcing/example_action_permissions/test/walkthroughs/test_support/demo_server_harness.dart`. Find the `dispatch` wrapper method (it forwards to the underlying dispatcher). Either:

a) Update its internal call to use `ActionSubmission`, keeping the harness's own dispatch signature unchanged (smallest blast radius)

b) Update both the harness's signature AND its callers (the walkthrough tests)

Choose (a) unless the harness's existing signature is materially worse than the new ActionSubmission shape. If chosen (a), the harness's existing parameter shape (e.g., `dispatch(actionName, rawInput, principalId, ...)`) is untouched and the walkthrough tests don't need updates. Document the choice in the commit message.

- [ ] **Step 7: Build & test**

```bash
cd event_sourcing && flutter test test/actions/
```

Expected: all action tests pass. If failures, the per-call-site transformation likely missed a hit or has a typo.

```bash
cd event_sourcing && flutter test
```

Expected: all 801+ event_sourcing tests pass.

```bash
cd event_sourcing/example_action_permissions && flutter test
```

Expected: all 134 example_action_permissions tests pass.

- [ ] **Step 8: Analyze**

```bash
cd event_sourcing && flutter analyze
cd event_sourcing/example_action_permissions && flutter analyze
```

Expected: clean (info-level lints only).

- [ ] **Step 9: Commit**

```bash
git add event_sourcing/lib/src/actions/action_dispatcher.dart \
        event_sourcing/test/actions/action_dispatcher_test.dart \
        event_sourcing/test/actions/integration_test.dart \
        event_sourcing/test/actions/bootstrap_audited_actions_test.dart \
        event_sourcing/example_action_permissions/lib/server/demo_routes.dart \
        event_sourcing/example_action_permissions/test/walkthroughs/test_support/demo_server_harness.dart
git commit -m "[CUR-1317] Refactor ActionDispatcher.dispatch to take ActionSubmission

Signature changes from (String, Map, ActionContext, {kwargs}) to
(ActionSubmission, ActionContext). Symmetric with DispatchResult on
the output side.

All ~30 in-substrate call sites updated atomically (Dart's type system
requires this for the build to stay green): action_dispatcher_test,
integration_test, bootstrap_audited_actions_test, demo_routes,
demo_server_harness.

Walkthrough harness wrapper signature kept unchanged — its internal
dispatch call uses ActionSubmission, but its callers (the walkthrough
tests) see no diff. (See harness.dispatch implementation.)

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter A)."
```

---

## Task 3: Reconcile `reaction` package — remove the local `ActionSubmission`

**Files:**

- Modify: `reaction/lib/src/interfaces/action_submitter.dart` (delete the local `ActionSubmission` class; rely on the substrate export)

After Task 2 lands, `package:event_sourcing/event_sourcing.dart` exports `ActionSubmission`. The temporary local definition in `reaction/lib/src/interfaces/action_submitter.dart` (added at commit `e2a8fb3`) becomes a duplicate that would cause "the name 'ActionSubmission' is exported from multiple libraries" if both were re-exported from `reaction`'s barrel later.

- [ ] **Step 1: Open the file and delete the local definition**

In `reaction/lib/src/interfaces/action_submitter.dart`, delete the class definition that starts with `class ActionSubmission {`. Keep the existing `import 'package:event_sourcing/event_sourcing.dart';` (which now provides `ActionSubmission`).

The result should be just: the file's imports + doc-comments + the `ActionSubmitter` abstract interface + `TransportException` class.

- [ ] **Step 2: Verify the reaction package still compiles**

```bash
cd reaction && flutter analyze lib/src/interfaces/action_submitter.dart
```

Expected: no errors. `ActionSubmission` reference in the doc comment + `submit` method resolves to the substrate's type via the barrel import.

- [ ] **Step 3: Run reaction's tests**

```bash
cd reaction && flutter test
```

Expected: all tests (Task 2-supplied tests so far) pass.

- [ ] **Step 4: Commit**

```bash
git add reaction/lib/src/interfaces/action_submitter.dart
git commit -m "[CUR-1317] reaction: remove local ActionSubmission (use substrate's)

The substrate now exports ActionSubmission (Plan A.5 Task 1).
Deleting the temporary local definition added at commit e2a8fb3.
reaction's ActionSubmitter interface now imports the substrate's
canonical type via the package:event_sourcing barrel.

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter A)."
```

---

## Task 4: Final verification + push

- [ ] **Step 1: Run the full test suite across all packages**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
(cd event_sourcing && flutter test) && \
(cd event_sourcing/example_action_permissions && flutter test) && \
(cd event_sourcing/example && flutter test) && \
(cd canonical_json_jcs && flutter test) && \
(cd provenance && flutter test) && \
(cd reaction && flutter test)
```

Expected: all pass.

- [ ] **Step 2: Analyze all packages**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
(cd event_sourcing && flutter analyze) && \
(cd event_sourcing/example_action_permissions && flutter analyze) && \
(cd event_sourcing/example && flutter analyze) && \
(cd reaction && flutter analyze)
```

Expected: clean (info-level lints only). Compare info-counts to pre-plan baseline; no new lint categories.

- [ ] **Step 3: Verify the WIP files are still in working tree**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing && git status
```

Expected: the user's 17+ modified spec files + .elspais.toml + .elspais/ + spec/INDEX.md untracked are all still present and untouched.

- [ ] **Step 4: Push to origin**

```bash
git push origin CUR-1317-libify-event-sourcing
```

Expected: clean push, no rejections. Pre-commit hooks handle the WIP stash dance automatically.

- [ ] **Step 5: Report completion**

Plan A.5 is complete. Plan B-local can resume at Task 4 (next in sequence: ViewSource interface).

---

## Self-Review Checklist

- [ ] No `TODO`/`TBD` placeholders. Verify: `grep -n 'TBD\|TODO' docs/superpowers/plans/2026-05-12-substrate-action-submission.md` — only the self-review checklist references.
- [ ] Identifier consistency: `ActionSubmission`, `actionName`, `rawInput`, `idempotencyKey`, `flowToken` — spelled identically across all tasks.
- [ ] Each task's commits are scoped — no leaked WIP files.
- [ ] Task 2 is the load-bearing one. The unpacking trick in `dispatch` keeps the existing method body untouched. If a call site is missed, the build fails immediately.
- [ ] The walkthrough harness choice (a) keeps walkthrough tests untouched. If the implementer choose (b) and updates the walkthrough tests too, that's fine — but document it explicitly.

---

## Notes on the spec/plan relationship

After this plan lands, `spec/prd-reaction.md` § `EVS-PRD-action-submitter` Assertion A ("submit(ActionSubmission) method returns Future<DispatchResult>") is fully realizable. The spec did not specify *where* `ActionSubmission` lives; placing it in the substrate is the cleanest interpretation per greenfield discipline.
