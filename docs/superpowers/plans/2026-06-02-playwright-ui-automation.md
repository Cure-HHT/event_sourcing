# Playwright UI Automation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Flutter-web `reaction/example` client reliably drivable by Playwright under the CanvasKit renderer, by force-enabling Flutter's semantics tree, annotating widgets with stable `flt-semantics-identifier`s, and surfacing builder lifecycle state from `reaction_widgets`.

**Architecture:** Three layers. (1) `reaction_widgets` gains an optional `semanticIdentifier` on `ActionBuilder`/`ViewBuilder` that wraps the delegated child in a non-painting `Semantics(identifier:, value: <stateToken>)`. (2) The example app force-enables semantics at boot and annotates its concrete buttons/fields/rows. (3) A TypeScript Playwright harness under `reaction/example/e2e/` drives a served web build, selecting by `[flt-semantics-identifier="..."]`.

**Tech Stack:** Dart/Flutter (`>=3.38.7`), `flutter_test`, `@playwright/test` (TypeScript), Node 18+, elspais (spec).

Design doc: `docs/superpowers/specs/2026-06-02-playwright-ui-automation-design.md`.

---

## File Structure

**Library (`reaction_widgets/`):**

- Modify: `lib/src/action/action_builder.dart` — add `semanticIdentifier`, wrap child.
- Modify: `lib/src/view/view_builder.dart` — add `semanticIdentifier`, wrap child.
- Test: `test/action/action_builder_test.dart` — new group for the hook.
- Test: `test/view/view_builder_test.dart` — new group for the hook.

**Spec (`spec/`):**

- Modify: `spec/prd-reaction.md` — one normative assertion + one non-normative remainder chapter.
- Regenerate: `spec/INDEX.md` (via elspais).

**Example app (`reaction/example/lib/client/`):**

- Modify: `main.dart` — `ensureSemantics()` at boot.
- Modify: `login_screen.dart` — annotate username field + sign-in button.
- Modify: `submit_note_form.dart` — annotate title field, workspace dropdown, submit button, ActionBuilder.
- Modify: `notes_list.dart` — annotate each row.

**Playwright harness (`reaction/example/`):**

- Create: `e2e/package.json`, `e2e/playwright.config.ts`, `e2e/tests/submit-note.spec.ts`, `e2e/.gitignore`.
- Create: `scripts/run-e2e.sh`.
- Modify: `README.md` — add a "Playwright e2e" section.

**State token mapping (used throughout):**

- `ActionState`: `Idle→idle`, `Submitting→submitting`, `Success→success`, `Denied→denied`, `Failed→failed`.
- `ViewState`: `Loading→loading`, `Ready→ready`, `Stale→stale`.

---

## Task 1: `ActionBuilder.semanticIdentifier`

**Files:**

- Modify: `reaction_widgets/lib/src/action/action_builder.dart`
- Test: `reaction_widgets/test/action/action_builder_test.dart`

- [ ] **Step 1: Write the failing tests**

Append this group inside the top-level `group('ActionBuilder', () {...})` in `reaction_widgets/test/action/action_builder_test.dart` (before its closing `});`):

```dart
    group('semanticIdentifier hook', () {
      testWidgets('null identifier adds no Semantics node', (tester) async {
        final handle = tester.ensureSemantics();
        final fake = FakeReaction();

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ActionBuilder(
            submissionFactory: _sub,
            builder: (ctx, state, submit) =>
                const SizedBox(key: ValueKey('leaf')),
          ),
        );

        final node = tester.getSemantics(find.byKey(const ValueKey('leaf')));
        expect(node.identifier, isEmpty);
        handle.dispose();
      });

      testWidgets('identifier surfaces with idle state token', (tester) async {
        final handle = tester.ensureSemantics();
        final fake = FakeReaction();

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ActionBuilder(
            semanticIdentifier: 'submit-note',
            submissionFactory: _sub,
            builder: (ctx, state, submit) =>
                const SizedBox(key: ValueKey('leaf')),
          ),
        );

        final node = tester.getSemantics(find.byKey(const ValueKey('leaf')));
        expect(node.identifier, 'submit-note');
        expect(node.value, 'idle');
        handle.dispose();
      });

      testWidgets('value tracks Idle -> Submitting -> Success', (tester) async {
        final handle = tester.ensureSemantics();
        final fake = FakeReaction();
        final completer = Completer<DispatchResult<Object?>>();
        fake.queueDispatchResultFuture(completer.future);
        late void Function() triggerSubmit;

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ActionBuilder(
            semanticIdentifier: 'submit-note',
            submissionFactory: _sub,
            builder: (ctx, state, submit) {
              triggerSubmit = submit;
              return const SizedBox(key: ValueKey('leaf'));
            },
          ),
        );

        SemanticsNode node() =>
            tester.getSemantics(find.byKey(const ValueKey('leaf')));
        expect(node().value, 'idle');

        triggerSubmit();
        await tester.pump();
        expect(node().value, 'submitting');

        completer.complete(const DispatchResult<Object?>.success('ok', <String>[]));
        await tester.pumpAndSettle();
        expect(node().value, 'success');
        handle.dispose();
      });
    });
```

Add the semantics import at the top of the test file (after the existing imports):

```dart
import 'package:flutter/semantics.dart' show SemanticsNode;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd reaction_widgets && flutter test test/action/action_builder_test.dart`
Expected: FAIL — `ActionBuilder` has no `semanticIdentifier` named parameter (compile error).

- [ ] **Step 3: Add the parameter and wrap the child**

In `reaction_widgets/lib/src/action/action_builder.dart`, add the field to the `ActionBuilder` constructor and class. Change the constructor:

```dart
  const ActionBuilder({
    super.key,
    required this.submissionFactory,
    required this.builder,
    this.idempotencyKeyGenerator,
    this.semanticIdentifier,
  });
```

Add the field declaration (next to the other `final` fields):

```dart
  /// Optional automation identifier. When non-null, the builder wraps its
  /// delegated child in a non-painting [Semantics] node carrying this
  /// `identifier` and the current [ActionState] as a machine-readable
  /// `value` token (`idle | submitting | success | denied | failed`).
  ///
  /// Layout-neutral and charter-compliant (a [Semantics] node is not a
  /// rendered or styled widget). Default `null` => no extra node, no
  /// behavior change. On Flutter web the identifier surfaces as a
  /// `flt-semantics-identifier` DOM attribute for tools like Playwright.
  final String? semanticIdentifier;
```

Replace the `build` method:

```dart
  @override
  Widget build(BuildContext context) {
    final child = widget.builder(context, _state, _submit);
    final id = widget.semanticIdentifier;
    if (id == null) return child;
    return Semantics(
      identifier: id,
      value: _stateToken(_state),
      child: child,
    );
  }

  static String _stateToken(ActionState state) => switch (state) {
        Idle() => 'idle',
        Submitting() => 'submitting',
        Success() => 'success',
        Denied() => 'denied',
        Failed() => 'failed',
      };
```

(`Semantics` is already available via the existing `package:flutter/widgets.dart` import.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd reaction_widgets && flutter test test/action/action_builder_test.dart`
Expected: PASS (all groups, including the existing ones).

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/action/action_builder.dart reaction_widgets/test/action/action_builder_test.dart
git commit -m "[CUR-1307] ActionBuilder: optional semanticIdentifier surfacing lifecycle state"
```

---

## Task 2: `ViewBuilder.semanticIdentifier`

**Files:**

- Modify: `reaction_widgets/lib/src/view/view_builder.dart`
- Test: `reaction_widgets/test/view/view_builder_test.dart`

- [ ] **Step 1: Write the failing test**

Open `reaction_widgets/test/view/view_builder_test.dart`, note how it constructs a `ViewBuilder` (reuse the same `viewName` / `mapper` / `aggregateIdOf` wiring the existing tests use). Add this group inside the top-level `group('ViewBuilder', () {...})`:

```dart
    group('semanticIdentifier hook', () {
      testWidgets('identifier surfaces with loading state token', (
        tester,
      ) async {
        final handle = tester.ensureSemantics();
        final fake = FakeReaction();

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ViewBuilder<Map<String, Object?>>(
            viewName: 'notes',
            semanticIdentifier: 'notes-view',
            mapper: (row) => row,
            aggregateIdOf: (row) => row['aggregateId'] as String,
            builder: (ctx, state) => const SizedBox(key: ValueKey('leaf')),
          ),
        );

        final node = tester.getSemantics(find.byKey(const ValueKey('leaf')));
        expect(node.identifier, 'notes-view');
        expect(node.value, 'loading');
        handle.dispose();
      });

      testWidgets('null identifier adds no Semantics node', (tester) async {
        final handle = tester.ensureSemantics();
        final fake = FakeReaction();

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ViewBuilder<Map<String, Object?>>(
            viewName: 'notes',
            mapper: (row) => row,
            aggregateIdOf: (row) => row['aggregateId'] as String,
            builder: (ctx, state) => const SizedBox(key: ValueKey('leaf')),
          ),
        );

        final node = tester.getSemantics(find.byKey(const ValueKey('leaf')));
        expect(node.identifier, isEmpty);
        handle.dispose();
      });
    });
```

Ensure the test file imports `SemanticsNode`/semantics if it reads `node.value`/`node.identifier` (add `import 'package:flutter/semantics.dart';` if not already present). If the existing `FakeReaction` view wiring differs (e.g. needs a registered view), mirror the setup already used by the other `ViewBuilder` tests in this file rather than inventing new wiring.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd reaction_widgets && flutter test test/view/view_builder_test.dart`
Expected: FAIL — `ViewBuilder` has no `semanticIdentifier` named parameter.

- [ ] **Step 3: Add the parameter and wrap the child**

In `reaction_widgets/lib/src/view/view_builder.dart`, add to the constructor:

```dart
  const ViewBuilder({
    super.key,
    required this.viewName,
    required this.mapper,
    required this.aggregateIdOf,
    required this.builder,
    this.filter,
    this.aggregates,
    this.isProgressive = false,
    this.semanticIdentifier,
  });
```

Add the field (next to the other `final` fields):

```dart
  /// Optional automation identifier. When non-null, the builder wraps its
  /// delegated child in a non-painting [Semantics] node carrying this
  /// `identifier` and the current [ViewState] as a machine-readable
  /// `value` token (`loading | ready | stale`). Default `null` => no extra
  /// node. On Flutter web the identifier surfaces as a
  /// `flt-semantics-identifier` DOM attribute.
  final String? semanticIdentifier;
```

Replace the `build` method on `_ViewBuilderState`:

```dart
  @override
  Widget build(BuildContext context) {
    final child = widget.builder(context, _state);
    final id = widget.semanticIdentifier;
    if (id == null) return child;
    return Semantics(
      identifier: id,
      value: _stateToken(_state),
      child: child,
    );
  }

  static String _stateToken(ViewState<Object?> state) => switch (state) {
        Loading() => 'loading',
        Ready() => 'ready',
        Stale() => 'stale',
      };
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd reaction_widgets && flutter test test/view/view_builder_test.dart`
Expected: PASS.

- [ ] **Step 5: Run the full library suite and commit**

Run: `cd reaction_widgets && flutter test`
Expected: PASS (no regressions).

```bash
git add reaction_widgets/lib/src/view/view_builder.dart reaction_widgets/test/view/view_builder_test.dart
git commit -m "[CUR-1307] ViewBuilder: optional semanticIdentifier surfacing lifecycle state"
```

---

## Task 3: Spec — assertion + remainder chapter in `spec/prd-reaction.md`

**Files:**

- Modify: `spec/prd-reaction.md`
- Regenerate: `spec/INDEX.md` (via elspais)

> Spec commits in this repo bypass hooks and use the keyring token — see project memory. Use `git --no-verify` and `env -u GITHUB_TOKEN` for any `gh` call.

- [ ] **Step 1: Find the current contract assertions**

Run: `grep -nE "EVS-PRD-reaction-widget-contract|^- \*\*[A-Z]\.\*\*|^### Assertions" spec/prd-reaction.md | head -40`
Identify the `EVS-PRD-reaction-widget-contract` requirement block and the next free assertion letter after the last one used (the design references existing /C, /E, /G, /I, /J — pick the next unused letter).

- [ ] **Step 2: Add the normative assertion**

In the `### Assertions` list of `EVS-PRD-reaction-widget-contract`, add (substitute the next free letter for `<N>`):

```markdown
- **<N>.** The Builder primitives (`ActionBuilder`, `ViewBuilder`) MAY
  accept an optional automation identifier. When supplied, a primitive
  SHALL wrap its delegated child in a single non-painting `Semantics`
  node carrying that `identifier` and the primitive's current lifecycle
  state as a machine-readable `value` token, and SHALL introduce no
  layout. A `Semantics` node is not a rendered or styled widget, so this
  does not violate the headless obligation (assertion G). When the
  identifier is absent the primitive SHALL introduce no additional
  semantics node.
```

- [ ] **Step 3: Add the non-normative remainder chapter**

Append a new `##`-level chapter to `spec/prd-reaction.md` (a non-`EVS-` heading => remainder prose). Copy the body of the "Downstream auto-instrumentation" section from the design doc verbatim, under this heading:

```markdown
## Automation instrumentation for downstream widget libraries
```

Include: the define-once pattern with the `MyStandardButton` example, the slug-default-with-override convention, the "one per-instance disambiguator is irreducible" point, the note that `ActionBuilder`/`ViewBuilder`'s `semanticIdentifier` is the auto-instrument path for action/view widgets, and the `ValueKey`-does-not-map-to-`flt-semantics-identifier` caveat.

- [ ] **Step 4: Refresh the graph and regenerate INDEX**

Use the elspais MCP: refresh the graph (`refresh_graph`) so the new assertion is picked up, then regenerate `spec/INDEX.md` per the repo's automated flow. Verify no broken references (`get_broken_references`).

- [ ] **Step 5: Commit (no-verify)**

```bash
git add spec/prd-reaction.md spec/INDEX.md
git --no-verify commit -m "[CUR-1307] reaction-widget-contract: permit non-painting automation semantics + how-to"
```

---

## Task 4: Force-enable semantics in the example web client

**Files:**

- Modify: `reaction/example/lib/client/main.dart`

- [ ] **Step 1: Edit `main.dart`**

Replace the file body with:

```dart
// reaction/example/lib/client/main.dart
//
// Flutter entry point for the reaction example client.
// Run with: `flutter run -d linux -t lib/client/main.dart`

import 'package:flutter/foundation.dart' show kIsWeb;
import 'package:flutter/semantics.dart';
import 'package:flutter/widgets.dart';
import 'package:reaction_example/client/app.dart';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  // On web, the accessibility/semantics tree is off by default (a
  // performance optimization). Force-enable it so the DOM exposes
  // flt-semantics nodes for UI automation (Playwright). Production apps
  // may gate this behind a flag; the demo always enables it.
  if (kIsWeb) {
    SemanticsBinding.instance.ensureSemantics();
  }
  runApp(const NotesApp());
}
```

- [ ] **Step 2: Verify it still compiles/analyzes**

Run: `cd reaction/example && flutter analyze lib/client/main.dart`
Expected: No issues.

- [ ] **Step 3: Commit**

```bash
git add reaction/example/lib/client/main.dart
git commit -m "[CUR-1307] example: force-enable web semantics tree at boot"
```

---

## Task 5: Annotate the login screen

**Files:**

- Modify: `reaction/example/lib/client/login_screen.dart`

- [ ] **Step 1: Wrap the username field and sign-in button**

In `login_screen.dart`, wrap the `TextField` (username) in a `Semantics` and the `FilledButton.icon` in a `Semantics`. The field becomes:

```dart
                  Semantics(
                    identifier: 'login-username',
                    textField: true,
                    explicitChildNodes: true,
                    child: TextField(
                      controller: _controller,
                      decoration: const InputDecoration(
                        labelText: 'Username',
                        hintText: 'alice / bob / carol / dave',
                        helperText:
                            'alice/bob: editor (west/east). carol: admin. '
                            'dave: viewer.',
                        prefixIcon: Icon(Icons.person_outline),
                        border: OutlineInputBorder(),
                      ),
                      onSubmitted: (_) => _signIn(),
                    ),
                  ),
                  const SizedBox(height: 16),
                  Semantics(
                    identifier: 'login-button',
                    button: true,
                    child: FilledButton.icon(
                      onPressed: _signIn,
                      icon: const Icon(Icons.login),
                      label: const Text('Sign in'),
                    ),
                  ),
```

> The `textField: true, explicitChildNodes: true` flags on the username wrapper are the mitigation for flutter/flutter#155323 (TextField semantics merging on web). Task 12 verifies the identifier actually appears in the DOM.

- [ ] **Step 2: Analyze**

Run: `cd reaction/example && flutter analyze lib/client/login_screen.dart`
Expected: No issues.

- [ ] **Step 3: Commit**

```bash
git add reaction/example/lib/client/login_screen.dart
git commit -m "[CUR-1307] example: annotate login username field + sign-in button"
```

---

## Task 6: Annotate the submit-note form

**Files:**

- Modify: `reaction/example/lib/client/submit_note_form.dart`

- [ ] **Step 1: Thread `semanticIdentifier` into the ActionBuilder**

Change the `ActionBuilder(` construction (around line 102) to add the identifier as the first argument:

```dart
      child: ActionBuilder(
        semanticIdentifier: 'submit-note',
        submissionFactory: () => ActionSubmission(
```

- [ ] **Step 2: Annotate the dropdown, field, and button**

Wrap the workspace `DropdownButton<String>` in `Semantics(identifier: 'submit-note-workspace', child: ...)`, the title `TextField` in `Semantics(identifier: 'submit-note-title', textField: true, explicitChildNodes: true, child: ...)`, and the `FilledButton` in `Semantics(identifier: 'submit-note-button', button: true, child: ...)`. Concretely, the three controls in the `Row` become:

```dart
                    Semantics(
                      identifier: 'submit-note-workspace',
                      child: DropdownButton<String>(
                        value: _workspace,
                        icon: const Icon(Icons.arrow_drop_down),
                        onChanged: submitting
                            ? null
                            : (v) {
                                if (v != null) setState(() => _workspace = v);
                              },
                        items: <DropdownMenuItem<String>>[
                          for (final ws in kKnownWorkspaces)
                            DropdownMenuItem<String>(
                              value: ws,
                              child: Text(
                                authorized.contains(ws)
                                    ? ws
                                    : '$ws (no permission)',
                              ),
                            ),
                        ],
                      ),
                    ),
                    const SizedBox(width: 12),
                    Expanded(
                      child: Semantics(
                        identifier: 'submit-note-title',
                        textField: true,
                        explicitChildNodes: true,
                        child: TextField(
                          controller: _titleController,
                          enabled: !submitting,
                          decoration: const InputDecoration(
                            labelText: 'New note title',
                            isDense: true,
                            border: OutlineInputBorder(),
                          ),
                          onSubmitted: (_) => onSubmit(),
                        ),
                      ),
                    ),
                    const SizedBox(width: 12),
                    Semantics(
                      identifier: 'submit-note-button',
                      button: true,
                      child: FilledButton(
                        onPressed: submitting ? null : onSubmit,
                        child: submitting
                            ? const SizedBox(
                                width: 16,
                                height: 16,
                                child: CircularProgressIndicator(strokeWidth: 2),
                              )
                            : const Text('Submit'),
                      ),
                    ),
```

- [ ] **Step 3: Analyze**

Run: `cd reaction/example && flutter analyze lib/client/submit_note_form.dart`
Expected: No issues.

- [ ] **Step 4: Commit**

```bash
git add reaction/example/lib/client/submit_note_form.dart
git commit -m "[CUR-1307] example: annotate submit-note form (status node + controls)"
```

---

## Task 7: Annotate notes-list rows

**Files:**

- Modify: `reaction/example/lib/client/notes_list.dart`

- [ ] **Step 1: Wrap each row in a Semantics carrying the title**

In `_NoteListView.build`, wrap the per-row `Card` returned by `itemBuilder` in a `Semantics` node. The `return Card(...)` becomes `return Semantics(identifier: 'note-row', value: note.title, child: Card(...));`:

```dart
        return Semantics(
          identifier: 'note-row',
          value: note.title,
          child: Card(
            elevation: 0,
            margin: EdgeInsets.zero,
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(14),
              side: BorderSide(color: scheme.outlineVariant),
            ),
            child: ListTile(
              leading: CircleAvatar(
                backgroundColor: color.withValues(alpha: 0.15),
                foregroundColor: color,
                child: const Icon(Icons.sticky_note_2_outlined),
              ),
              title: Text(
                note.title,
                style: const TextStyle(fontWeight: FontWeight.w600),
              ),
              subtitle: Text(note.id, style: const TextStyle(fontSize: 11)),
              trailing: TagChip(label: note.workspace, color: color),
            ),
          ),
        );
```

> Every row shares `identifier: 'note-row'`; rows are distinguished by `value` (the title). Playwright asserts existence via `[flt-semantics-identifier="note-row"]` filtered by the title text. This matches the design's "same id, per-instance value disambiguator" convention.

- [ ] **Step 2: Analyze**

Run: `cd reaction/example && flutter analyze lib/client/notes_list.dart`
Expected: No issues.

- [ ] **Step 3: Commit**

```bash
git add reaction/example/lib/client/notes_list.dart
git commit -m "[CUR-1307] example: annotate notes-list rows with title value"
```

---

## Task 8: Scaffold the Playwright project

**Files:**

- Create: `reaction/example/e2e/package.json`
- Create: `reaction/example/e2e/.gitignore`
- Create: `reaction/example/e2e/playwright.config.ts`

- [ ] **Step 1: Create `e2e/package.json`**

```json
{
  "name": "reaction-example-e2e",
  "private": true,
  "version": "0.1.0",
  "description": "Playwright UI automation for the reaction example Flutter-web client.",
  "scripts": {
    "test": "playwright test",
    "install-browsers": "playwright install chromium"
  },
  "devDependencies": {
    "@playwright/test": "^1.48.0"
  }
}
```

- [ ] **Step 2: Create `e2e/.gitignore`**

```gitignore
node_modules/
playwright-report/
test-results/
```

- [ ] **Step 3: Create `e2e/playwright.config.ts`**

The Flutter web bundle is built by `scripts/run-e2e.sh` into `reaction/example/build/web` and served on `:8000`. The dart demo server is booted by the script on `:8080`. Playwright serves the static bundle and waits for it.

```ts
import { defineConfig, devices } from '@playwright/test';

// The dart demo server (port 8080) and the `flutter build web` step are
// orchestrated by scripts/run-e2e.sh. Playwright only serves the built
// bundle and runs the specs against Chromium.
export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: 'http://localhost:8000',
    trace: 'on-first-retry',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: {
    command: 'npx --yes serve ../build/web -l 8000 --no-clipboard',
    url: 'http://localhost:8000',
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
```

- [ ] **Step 4: Install dependencies and browser**

Run:

```bash
cd reaction/example/e2e
npm install
npx playwright install chromium
```

Expected: dependencies installed; Chromium downloaded.

- [ ] **Step 5: Commit**

```bash
git add reaction/example/e2e/package.json reaction/example/e2e/.gitignore reaction/example/e2e/playwright.config.ts
git commit -m "[CUR-1307] e2e: scaffold Playwright TypeScript project"
```

> Note: `e2e/package-lock.json` may be created by `npm install`; commit it too if present.

---

## Task 9: Write the run script

**Files:**

- Create: `reaction/example/scripts/run-e2e.sh`

- [ ] **Step 1: Create `scripts/run-e2e.sh`**

```bash
#!/usr/bin/env bash
# Build the Flutter web client, boot the demo server, and run the
# Playwright e2e suite against the served bundle.
#
# Usage:  reaction/example/scripts/run-e2e.sh
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$EXAMPLE_DIR"

SERVER_PORT="${REACTION_SERVER_PORT:-8080}"

cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "==> Booting demo server on :$SERVER_PORT"
dart run bin/server.dart --port "$SERVER_PORT" &
SERVER_PID=$!

# Give the server a moment to bind.
sleep 2

echo "==> Building Flutter web bundle"
flutter build web -t lib/client/main.dart \
  --dart-define=REACTION_SERVER_URL="http://127.0.0.1:$SERVER_PORT"

echo "==> Running Playwright suite"
cd e2e
npm install --silent
npx playwright test "$@"
```

- [ ] **Step 2: Make it executable**

Run: `chmod +x reaction/example/scripts/run-e2e.sh`

- [ ] **Step 3: Commit**

```bash
git add reaction/example/scripts/run-e2e.sh
git commit -m "[CUR-1307] e2e: add run-e2e.sh orchestration script"
```

---

## Task 10: Write the submit-note spec

**Files:**

- Create: `reaction/example/e2e/tests/submit-note.spec.ts`

- [ ] **Step 1: Create the test**

```ts
import { test, expect, Page } from '@playwright/test';

// Selectors target Flutter's web semantics tree (force-enabled at boot in
// main.dart). Each Flutter Semantics(identifier:) surfaces as a
// `flt-semantics-identifier` attribute under the flt-semantics-host.
const byId = (id: string) => `[flt-semantics-identifier="${id}"]`;

// Wait for CanvasKit to finish booting and the semantics DOM to exist.
async function waitForApp(page: Page) {
  await page.waitForSelector('flt-semantics-host', { timeout: 30_000 });
  await page.waitForSelector(byId('login-username'), { timeout: 30_000 });
}

test('submit a note and see it appear in the list', async ({ page }) => {
  await page.goto('/');
  await waitForApp(page);

  // Log in as dave (a viewer would not see admin panel; use an editor so
  // submit succeeds — alice is editor-west).
  await page.locator(byId('login-username')).click();
  await page.keyboard.type('alice');
  await page.locator(byId('login-button')).click();

  // The submit-note form should mount; wait for its status node.
  await page.waitForSelector(byId('submit-note'), { timeout: 30_000 });

  const title = `e2e note ${Date.now()}`;
  await page.locator(byId('submit-note-title')).click();
  await page.keyboard.type(title);
  await page.locator(byId('submit-note-button')).click();

  // Assert the ActionBuilder lifecycle node reports success.
  await expect
    .poll(
      async () =>
        page.locator(byId('submit-note')).getAttribute('aria-valuetext'),
      { timeout: 15_000 },
    )
    .toBe('success');

  // Assert a note-row carrying our unique title exists.
  await expect(
    page.locator(`${byId('note-row')}`, { hasText: title }),
  ).toBeVisible({ timeout: 15_000 });
});
```

> The exact DOM attribute Flutter uses for `Semantics.value` (here assumed `aria-valuetext`) is verified in Task 12; if it differs (e.g. text content or `aria-label`), adjust the `expect.poll` accessor there. Likewise the workspace dropdown: alice is authorized for `west`, which is the default `_workspace`, so no dropdown interaction is needed for the happy path. If the default differs, add a `byId('submit-note-workspace')` interaction.

- [ ] **Step 2: Commit (test will be exercised in Task 12)**

```bash
git add reaction/example/e2e/tests/submit-note.spec.ts
git commit -m "[CUR-1307] e2e: add submit-note Playwright spec"
```

---

## Task 11: Document the harness in the README

**Files:**

- Modify: `reaction/example/README.md`

- [ ] **Step 1: Append a "Playwright e2e" section**

Add near the end of `reaction/example/README.md`:

```markdown
## Playwright end-to-end (web UI automation)

The Flutter web client renders through CanvasKit (a single `<canvas>`),
so Playwright drives it via Flutter's accessibility/semantics tree, which
`main.dart` force-enables on web. Widgets carry stable
`Semantics(identifier:)`s that surface as `flt-semantics-identifier` DOM
attributes.

One-shot local run (builds web, boots the demo server, runs the suite):

    cd reaction/example
    scripts/run-e2e.sh

First time only, install the Playwright browser:

    cd reaction/example/e2e && npm install && npx playwright install chromium

The suite lives in `e2e/tests/`. Selectors use
`[flt-semantics-identifier="..."]`. CI wiring (headless Chromium + server
orchestration) is deferred to a follow-up ticket.
```

- [ ] **Step 2: Commit**

```bash
git add reaction/example/README.md
git commit -m "[CUR-1307] example: document Playwright e2e harness in README"
```

---

## Task 12: End-to-end verification and gotcha resolution

**Files:** (potentially) `reaction/example/lib/client/*.dart`, `reaction/example/e2e/tests/submit-note.spec.ts`

This task runs the real harness and resolves the two known web-semantics gotchas against the pinned SDK. No new code is written speculatively — fixes are applied only if a gotcha actually manifests.

- [ ] **Step 1: Run the full harness**

Run: `cd reaction/example && scripts/run-e2e.sh`
Expected: the spec passes. If it passes first try, skip to Step 5.

- [ ] **Step 2: Inspect the actual DOM if selectors miss**

If `login-username` / `submit-note-title` are not found, the TextField-semantics gotcha (flutter/flutter#155323) is live. Run the suite in headed mode and dump the semantics DOM:

Run: `cd reaction/example/e2e && npx playwright test --headed --debug`

In the inspector console, evaluate:

```js
document.querySelectorAll('[flt-semantics-identifier]')
```

Confirm which identifiers are present and what attribute carries the field/value.

- [ ] **Step 3: Apply the TextField fallback if needed**

If the field identifiers are absent despite `textField: true, explicitChildNodes: true`, move the identifier off the `Semantics` wrapper and onto an enclosing container that is not a text field, OR target the field by its label instead. Concretely, replace the field wrapper with a keyed container approach: wrap in `Semantics(identifier: 'submit-note-title', container: true, explicitChildNodes: true, child: ...)` (drop `textField: true`). Re-run Step 1.

- [ ] **Step 4: Fix the lifecycle-value accessor if needed**

If `submit-note`'s success state is not readable via `aria-valuetext`, determine the real carrier from Step 2's DOM dump (commonly the element's text content or `aria-label`). Update the `expect.poll` accessor in `submit-note.spec.ts` accordingly. If `value` does not surface on the wrapper at all (design risk #2), implement the documented fallback: in `submit_note_form.dart`, render a dedicated zero-size status node alongside the form, e.g.

```dart
                Semantics(
                  identifier: 'submit-note-status',
                  value: switch (state) {
                    Idle() => 'idle',
                    Submitting() => 'submitting',
                    Success() => 'success',
                    Denied() => 'denied',
                    Failed() => 'failed',
                  },
                  child: const SizedBox.shrink(),
                ),
```

and assert on `byId('submit-note-status')` instead. (`Idle`/`Submitting`/etc. import via `package:reaction/reaction.dart`, already imported in that file.)

- [ ] **Step 5: Re-run to confirm green, then commit any fixes**

Run: `cd reaction/example && scripts/run-e2e.sh`
Expected: PASS.

```bash
git add -A reaction/example
git commit -m "[CUR-1307] e2e: resolve web-semantics gotchas; suite green"
```

> If no fixes were needed, this commit is empty — skip it.

---

## Self-Review notes

- **Spec coverage:** Library hook (Tasks 1–2), charter assertion + how-to home (Task 3), semantics enablement (Task 4), interactable annotation across login/submit/list (Tasks 5–7), TS Playwright harness + run script + spec + README (Tasks 8–11), risk verification for both gotchas (Task 12). All design sections map to a task.
- **Out of scope (per design):** CI job, admin-panel annotation, shipped helper widget, `reaction_widgets_testing` changes — none planned. Correct.
- **Type consistency:** `semanticIdentifier` (String?) used identically in Tasks 1, 2, 6; state tokens `idle|submitting|success|denied|failed` and `loading|ready|stale` consistent across Tasks 1, 2, 12; selector helper `byId` and identifiers (`login-username`, `login-button`, `submit-note`, `submit-note-title`, `submit-note-workspace`, `submit-note-button`, `note-row`) consistent across Tasks 5–10.
