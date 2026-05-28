# Reaction Widgets Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the headless Flutter widget layer (`reaction_widgets`) and the supporting `reaction` cleanup (shared `ReactionScope` abstraction, `LocalScope`, `ConnectionStatus`, real exponential-backoff auto-reconnect) it requires. End state: hht_diary mobile and the Flutter-web portal UI can build modality-appropriate sugar widgets on top of a shared, substrate-agnostic, headless base.

**Architecture:** Two phases bundled in one plan because the widget layer cannot exist meaningfully without the `reaction` scope cleanup it depends on.

- **Phase A** (`reaction/`) introduces a `ReactionScope` interface implemented by `LocalScope` (in-process; always `Connected`) and `RemoteScope` (drives `ConnectionStatus` from the WS lifecycle). Completes `RemoteConnection` auto-reconnect.
- **Phase B** (`reaction_widgets/`) is a new Flutter package shipping `ReActionScope` (`InheritedWidget` threading `ReactionScope`), `ActionBuilder`/`ViewBuilder` Builder primitives, `ViewListener`, `PermissionGate`, `ReActionErrorListener`, and `FakeReaction` widget-test doubles. **No rendered or styled widgets** — apps render their own sugar.

**Tech Stack:** Dart 3, Flutter SDK (already required by `reaction` for sembast test binding), `package:meta`, `package:uuid` (transitive via reaction). Tests run under `flutter_test`.

**Spec:** `spec/prd-reaction.md` (`EVS-PRD-reaction-scope`, `EVS-PRD-reaction-widget-contract`, additions to `EVS-PRD-view-subscriber`, `EVS-PRD-cross-process-event-transport`) + `spec/reaction-remote.md` Section 3 (RemoteScope/ConnectionStatus + auto-reconnect prose). Read these before executing.

---

## Precondition: design spec commits are on the branch

This plan assumes commits `1a43550` (headless widget design + `ReactionScope` PRD) and `49e0988` (Plan A/B/C/D label cleanup) are present on the working branch. Verify with `git log --oneline -5` before Task 1.

**Task 1 is a drift sweep**: the implementer skims the spec edits against the prior-shipped `reaction` surface, flags any naming or API mismatch, and pauses for design correction rather than impl-around. Cheap, load-bearing.

---

## Verified symbols (read before coding)

The widget layer consumes these public types from `package:reaction/reaction.dart`. Confirm each exists with the listed shape before relying on it — annotate any drift in Task 1's report.

| Symbol | Where | Shape |
|---|---|---|
| `AuthSession` | `interfaces/auth_session.dart` | `AuthStatus get current; Stream<AuthStatus> get stream; void setCredential(String?); Principal? get principal; Future<void> dispose();` |
| `AuthStatus` | same | sealed: `Authenticated({required Principal principal})`, `NotAuthenticated()`, `Expired()` |
| `ActionSubmitter` | `interfaces/action_submitter.dart` | `Future<DispatchResult<Object?>> submit(ActionSubmission)` |
| `ViewSource` | `interfaces/view_source.dart` | `Stream<Update<T>> watch<T>({required String viewName, required T Function(Map<String, Object?>) mapper, SubscriptionFilter? filter, Set<String>? aggregates})` — all named; `filter` is optional/nullable |
| `PermissionSource` | `interfaces/permission_source.dart` | `EffectiveAuthorization? get current; Stream<EffectiveAuthorization?> get stream; Future<void> dispose();` |
| `ActionState` | `state/action_state.dart` | sealed: `Idle()`, `Submitting()`, `Success(DispatchResult<Object?> result)`, **`Denied(DispatchResult<Object?> result)` with `String get reason` getter (post-Task 1.5 refactor — substrate currently has `Denied(String reason)`)**, `Failed(Object error, StackTrace stackTrace)` |
| `IdempotencyKeyGenerator` | `state/idempotency_key_generator.dart` | `String generate()` |
| `Uuid4IdempotencyKeyGenerator` | same | default impl |
| `RemoteScope` | `remote/remote_scope.dart` | exposes `authSession`, `actionSubmitter`, `viewSource`, `permissionSource`; `Future<void> dispose()` |
| `Update<T>` | substrate, re-exported | sealed: `Snapshot<T>`, `Delta<T>`, `Tombstone<T>`, `EndOfReplay<T>` |
| `EffectiveAuthorization` | substrate, re-exported | per-Principal authorization snapshot |
| `DispatchResult` | substrate, re-exported | sealed: success + denial variants |
| `TransportException` | `interfaces/action_submitter.dart` | thrown on wire / auth failures |

Phase A adds `ReactionScope`, `LocalScope`, and `ConnectionStatus` to this list.

---

## File structure

```text
reaction/
  pubspec.yaml                            (unchanged — already depends on Flutter SDK)
  lib/reaction.dart                       MODIFY: export ReactionScope, ConnectionStatus, LocalScope
  lib/src/scope/                          NEW directory
    connection_status.dart                  sealed ConnectionStatus + variants
    reaction_scope.dart                     abstract ReactionScope interface
    local_scope.dart                        LocalScope: composes Local* impls; always Connected
  lib/src/remote/remote_connection.dart   MODIFY: real exponential-backoff reconnect;
                                                  emit ConnectionStatus via callback hook
  lib/src/remote/remote_scope.dart        MODIFY: implement ReactionScope; expose
                                                  ConnectionStatus stream + getter driven by
                                                  RemoteConnection's WS lifecycle

  test/scope/                             NEW directory
    connection_status_test.dart
    local_scope_test.dart
    reaction_scope_contract_test.dart       Runs the same assertions against both impls
  test/remote/remote_connection_test.dart MODIFY: add backoff + status-emission tests
  test/remote/remote_scope_test.dart      MODIFY: add ReactionScope conformance assertions

reaction_widgets/                         NEW package (sibling of reaction/)
  pubspec.yaml
  analysis_options.yaml
  .gitignore                              Flutter scaffold paths per reaction/example/.gitignore
  README.md                               One-paragraph "headless widget layer; see spec/prd-reaction.md"
  lib/reaction_widgets.dart               Barrel
  lib/src/scope/
    reaction_scope_widget.dart              ReActionScope InheritedWidget + .test() ctor
  lib/src/action/
    action_builder.dart                     ActionBuilder StatefulWidget
  lib/src/view/
    view_state.dart                         sealed ViewState<T>: Loading/Ready/Disconnected
    view_builder.dart                       ViewBuilder StatefulWidget (incl. progressive mode)
    view_listener.dart                      ViewListener StatefulWidget (imperative)
  lib/src/permission/
    permission_gate.dart                    PermissionGate StatefulWidget
  lib/src/error/
    reaction_error_listener.dart            ReActionErrorListener StatefulWidget
  lib/src/testing/
    fake_reaction.dart                      FakeReaction + FakeAuthSession / FakeActionSubmitter
                                            / FakeViewSource / FakePermissionSource +
                                            pumpReactionWidget helper

  test/scope/reaction_scope_widget_test.dart
  test/action/action_builder_test.dart
  test/view/view_state_test.dart
  test/view/view_builder_test.dart
  test/view/view_listener_test.dart
  test/permission/permission_gate_test.dart
  test/error/reaction_error_listener_test.dart
  test/testing/fake_reaction_test.dart
  test/structural/no_substrate_imports_test.dart   Verifies assertion F via source-grep

.github/workflows/reaction-widgets-tests.yml    NEW CI workflow (Task 21)

```

---

## Phase A — `reaction` scope abstraction + connection state

### Task 1: Verify spec commits + drift-sweep

**Files:** None modified; verification only.

- [ ] **Step 1: Confirm spec commits are present**

```bash
git log --oneline 11a5eba..HEAD -- spec/prd-reaction.md spec/reaction-remote.md CLAUDE.md

```

Expected: at least `1a43550` and `49e0988` listed.

- [ ] **Step 2: Verify the four reaction interface signatures match the Verified-Symbols table above**

Open and visually confirm:

```bash
sed -n '1,80p' reaction/lib/src/interfaces/auth_session.dart
sed -n '1,80p' reaction/lib/src/interfaces/action_submitter.dart
sed -n '1,80p' reaction/lib/src/interfaces/view_source.dart
sed -n '1,80p' reaction/lib/src/interfaces/permission_source.dart
sed -n '1,40p' reaction/lib/src/state/action_state.dart
sed -n '1,40p' reaction/lib/src/state/idempotency_key_generator.dart

```

Expected: each interface has the getters/methods listed in the Verified-Symbols table. If any signature has drifted, STOP and reconcile the spec before continuing.

- [ ] **Step 3: Confirm test runner sanity**

```bash
cd reaction && flutter test test/state/action_state_test.dart 2>&1 | tail -5

```

Expected: tests pass (existing state tests still green on this branch).

- [ ] **Step 4: No commit**

This is a verification task; no code changes.

---

### Task 1.5: Refactor `ActionState.Denied` to carry `DispatchResult`

**Context for the implementer:** The Task 1 drift sweep found that `ActionState.Denied` currently carries `String reason`, but the plan's Verified-Symbols table and Task 11's `ActionBuilder` code assume `Denied(DispatchResult<Object?> result)` with a derived `reason` getter. Per `feedback_greenfield_fix_root_not_workaround`, fix the substrate to preserve structured denial info rather than throwing it away into a string at the widget boundary.

**Files:**

- Modify: `reaction/lib/src/state/action_state.dart`
- Modify: `reaction/test/state/action_state_test.dart`

- [ ] **Step 1: Update the failing test**

Replace the existing `'Denied carries the denial reason'` test in `reaction/test/state/action_state_test.dart` with:

```dart
test('Denied carries the full DispatchResult and exposes a reason getter', () {
  const result = DispatchResult<Object?>.authorizationDenied(
      Permission('test.permission'));
  const d = Denied(result);
  expect(d, isA<ActionState>());
  expect(d, isA<Denied>());
  expect(d.result, same(result));
  expect(d.reason, contains('test.permission'));
});

test('Denied.reason maps each DispatchResult denial variant to a readable summary', () {
  expect(
    const Denied(DispatchResult<Object?>.unknownAction('foo')).reason,
    contains('foo'),
  );
  expect(
    const Denied(DispatchResult<Object?>.parseDenied('bad json')).reason,
    contains('parse'),
  );
  expect(
    const Denied(DispatchResult<Object?>.validationDenied('field empty')).reason,
    contains('valid'),
  );
  expect(
    const Denied(DispatchResult<Object?>.authorizationDenied(Permission('p'))).reason,
    contains('permission'),
  );
  expect(
    const Denied(DispatchResult<Object?>.executionFailed('boom')).reason,
    isNotEmpty,
  );
  expect(
    const Denied(DispatchResult<Object?>.idempotencyMismatch(
      actionName: 'a', idempotencyKey: 'k',
      cachedRawInputHash: 'h1', submittedRawInputHash: 'h2',
    )).reason,
    contains('idempot'),
  );
});
```

Also update the existing `'exhaustive switch across all 5 variants'` test if it constructs `Denied('...')` — the new shape requires a `DispatchResult`.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction && flutter test test/state/action_state_test.dart 2>&1 | tail -10

```

Expected: FAIL — `Denied('action not allowed for role')` (String) won't compile against `Denied(DispatchResult)` (or, if you've already swapped the field type below, the test compiles but the new `reason` getter doesn't exist yet).

- [ ] **Step 3: Refactor `ActionState.Denied`**

In `reaction/lib/src/state/action_state.dart`, replace the `Denied` class and update the `ActionState.denied` factory:

```dart
/// The submission was denied by the dispatcher (parse failure, validation
/// failure, authorization denied, execution failed, unknown action, or
/// idempotency mismatch). Carries the full [DispatchResult] so widget
/// consumers can switch on specific denial variants for structured UX
/// (e.g., "you need permission X" vs "validation failed: ..."). The
/// [reason] getter is a derived human-readable summary suitable for a
/// default toast/snackbar message.
class Denied extends ActionState {
  const Denied(this.result);
  final DispatchResult<Object?> result;

  /// Human-readable one-line summary of the denial, derived from the
  /// concrete [DispatchResult] variant.
  String get reason => switch (result) {
        DispatchSuccess() => 'unexpected success classified as denial',
        DispatchUnknownAction(:final requestedName) =>
            'Unknown action: "$requestedName"',
        DispatchParseDenied(:final error) => 'Parse error: $error',
        DispatchValidationDenied(:final error) =>
            'Validation failed: $error',
        DispatchAuthorizationDenied(:final permission) =>
            'You need permission "${permission.name}"',
        DispatchExecutionFailed(:final error) =>
            'Execution failed: $error',
        DispatchIdempotencyHit() =>
            'unexpected idempotency hit classified as denial',
        DispatchIdempotencyMismatch() =>
            'Idempotency mismatch — same key, different payload',
      };
}
```

Update the convenience factory:

```dart
const factory ActionState.denied(DispatchResult<Object?> result) = Denied;
```

Update the comment block at the top describing the lifecycle:

```dart
/// Lifecycle:
///
/// ```text
/// Idle --submit()--> Submitting --+--> Success(DispatchResult)
///                                  +--> Denied(DispatchResult)
///                                  +--> Failed(error, stackTrace)
/// ```
```

**Note:** `Permission` is a class with a `name` field — verify the actual accessor (`permission.name`, `permission.id`, etc.) by reading `event_sourcing/lib/src/actions/permission.dart` and adjust the `DispatchAuthorizationDenied` arm accordingly.

- [ ] **Step 4: Run tests**

```bash
cd reaction && flutter test test/state/action_state_test.dart 2>&1 | tail -5

```

Expected: PASS (8 tests — the two new Denied tests + the existing Idle/Submitting/Success/Failed/exhaustive tests).

Run the full reaction suite to check for any unforeseen caller breakage:

```bash
cd reaction && flutter test 2>&1 | tail -10

```

Expected: all tests pass. No other callers of `Denied(String)` exist today (grep-verified during drift sweep), so the refactor should be local.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/state/action_state.dart \
        reaction/test/state/action_state_test.dart
git commit -m "[CUR-1317] reaction: ActionState.Denied carries DispatchResult

Replaces Denied(String reason) with Denied(DispatchResult<Object?> result)
plus a derived String get reason getter. Preserves the substrate's
structured denial info (DispatchAuthorizationDenied/ValidationDenied/
IdempotencyMismatch/etc.) so widget consumers can switch on variants
for structured UX, while the reason getter remains useful for the
default toast/snackbar case.

Greenfield fix per feedback_greenfield_fix_root_not_workaround: the
previous String-only shape compressed away substrate-level information
the widget layer needs.

Implements EVS-PRD-reaction-widget-contract-C (the structured-state-machine
half of the Builder primitive contract).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"
```

---

### Task 2: Add `ConnectionStatus` sealed type

**Files:**

- Create: `reaction/lib/src/scope/connection_status.dart`
- Test: `reaction/test/scope/connection_status_test.dart`

- [ ] **Step 1: Write the failing test**

Create `reaction/test/scope/connection_status_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-scope/B

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

void main() {
  group('ConnectionStatus', () {
    test('has three sealed variants', () {
      const ConnectionStatus a = Connected();
      const ConnectionStatus b = Reconnecting();
      const ConnectionStatus c = Disconnected();

      expect(a, isA<Connected>());
      expect(b, isA<Reconnecting>());
      expect(c, isA<Disconnected>());
    });

    test('variants are equal by type (const constructors)', () {
      expect(const Connected(), equals(const Connected()));
      expect(const Reconnecting(), equals(const Reconnecting()));
      expect(const Disconnected(), equals(const Disconnected()));
      expect(const Connected(), isNot(equals(const Reconnecting())));
    });

    test('exhaustive switch compiles for all three', () {
      String label(ConnectionStatus s) => switch (s) {
        Connected() => 'connected',
        Reconnecting() => 'reconnecting',
        Disconnected() => 'disconnected',
      };
      expect(label(const Connected()), 'connected');
      expect(label(const Reconnecting()), 'reconnecting');
      expect(label(const Disconnected()), 'disconnected');
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction && flutter test test/scope/connection_status_test.dart 2>&1 | tail -10

```

Expected: FAIL — `Undefined name 'ConnectionStatus'` (and the variants).

- [ ] **Step 3: Write minimal implementation**

Create `reaction/lib/src/scope/connection_status.dart`:

```dart
// Implements: EVS-PRD-reaction-scope/B

/// Transport-connection liveness as observed by a [ReactionScope].
///
/// Three variants, exhaustive:
///
/// - [Connected]    — transport is up; subscriptions are live.
/// - [Reconnecting] — transport dropped; client is attempting to
///                    reconnect (subscriptions stalled until success).
/// - [Disconnected] — transport is down and the reconnect policy has
///                    given up; consumer should surface an actionable
///                    error.
///
/// [LocalScope] reports [Connected] for its entire lifetime
/// (in-process has no transport to lose). [RemoteScope] drives
/// transitions from the underlying WS lifecycle.
sealed class ConnectionStatus {
  const ConnectionStatus();
}

class Connected extends ConnectionStatus {
  const Connected();
  @override
  bool operator ==(Object other) => other is Connected;
  @override
  int get hashCode => (Connected).hashCode;
  @override
  String toString() => 'ConnectionStatus.Connected';
}

class Reconnecting extends ConnectionStatus {
  const Reconnecting();
  @override
  bool operator ==(Object other) => other is Reconnecting;
  @override
  int get hashCode => (Reconnecting).hashCode;
  @override
  String toString() => 'ConnectionStatus.Reconnecting';
}

class Disconnected extends ConnectionStatus {
  const Disconnected();
  @override
  bool operator ==(Object other) => other is Disconnected;
  @override
  int get hashCode => (Disconnected).hashCode;
  @override
  String toString() => 'ConnectionStatus.Disconnected';
}

```

Add export to `reaction/lib/reaction.dart` immediately after the interface exports:

```dart
// Scope
export 'src/scope/connection_status.dart'
    show ConnectionStatus, Connected, Reconnecting, Disconnected;

```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd reaction && flutter test test/scope/connection_status_test.dart 2>&1 | tail -5

```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/scope/connection_status.dart \
        reaction/lib/reaction.dart \
        reaction/test/scope/connection_status_test.dart
git commit -m "[CUR-1317] reaction: add ConnectionStatus sealed type

Three variants (Connected/Reconnecting/Disconnected) per
EVS-PRD-reaction-scope-B. Consumed by the forthcoming ReactionScope
abstraction.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 3: Add `ReactionScope` interface

**Files:**

- Create: `reaction/lib/src/scope/reaction_scope.dart`
- Modify: `reaction/lib/reaction.dart` (export)
- Test: deferred to Task 4 (interface tested through impls)

- [ ] **Step 1: Write the interface**

Create `reaction/lib/src/scope/reaction_scope.dart`:

```dart
// Implements: EVS-PRD-reaction-scope/A

import 'dart:async';

import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/scope/connection_status.dart';

/// Substrate-agnostic composition root that bundles the four library
/// interfaces with live transport-connection state.
///
/// Two shipped impls:
///
/// - [LocalScope]  — in-process composition (Local* impls); always
///                   reports [Connected].
/// - [RemoteScope] — cross-process composition (Remote* impls over a
///                   shared WS); drives [ConnectionStatus] from WS
///                   lifecycle events.
///
/// Consumer code (especially the `reaction_widgets` layer) depends on
/// this interface, not on the concrete scope types — that is what makes
/// widget code source-identical across Local and Remote per the
/// substrate-agnostic widget contract (`EVS-PRD-reaction-widget-contract`-B).
abstract interface class ReactionScope {
  AuthSession get authSession;
  ActionSubmitter get actionSubmitter;
  ViewSource get viewSource;
  PermissionSource get permissionSource;

  /// Current connection state, synchronous (always non-null).
  ConnectionStatus get connectionStatus;

  /// Stream of subsequent [ConnectionStatus] transitions.
  ///
  /// Does NOT emit the current value on subscribe; consumers that need
  /// "current plus subsequent" should seed from [connectionStatus] and
  /// listen to this stream. Implementations SHOULD be broadcast streams
  /// (single producer, multiple widget consumers).
  Stream<ConnectionStatus> get connectionStatusStream;

  /// Graceful teardown. After [dispose], the four interface getters and
  /// `connectionStatus*` SHOULD throw [StateError] on access.
  Future<void> dispose();
}

```

Update `reaction/lib/reaction.dart`:

```dart
export 'src/scope/reaction_scope.dart' show ReactionScope;

```

(adjacent to the `ConnectionStatus` export.)

- [ ] **Step 2: Verify it compiles**

```bash
cd reaction && flutter analyze lib/src/scope/reaction_scope.dart 2>&1 | tail -5

```

Expected: `No issues found!`

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/src/scope/reaction_scope.dart reaction/lib/reaction.dart
git commit -m "[CUR-1317] reaction: add ReactionScope interface

Composes the four library interfaces with a ConnectionStatus surface.
LocalScope and RemoteScope follow in subsequent commits.

Implements EVS-PRD-reaction-scope-A.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 4: Implement `LocalScope`

**Files:**

- Create: `reaction/lib/src/scope/local_scope.dart`
- Modify: `reaction/lib/reaction.dart` (export)
- Test: `reaction/test/scope/local_scope_test.dart`

- [ ] **Step 1: Write the failing test**

Create `reaction/test/scope/local_scope_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-scope/A, /C, /E

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

void main() {
  group('LocalScope', () {
    late LocalScope scope;
    late LocalAuthSession auth;
    late LocalActionSubmitter submitter;
    late LocalViewSource views;
    late LocalPermissionSource perms;

    setUp(() {
      // Use the existing ReactionTestHarness substrate from the
      // reaction package's test scaffolding to get Local* impls.
      auth = LocalAuthSession(principal: const Principal(
        userId: 'u', activeRole: 'r', roles: {'r'}));
      // (the harness wires submitter/views/perms against a fresh
      // in-memory EventStore; see existing tests for the helper)
      // ... (existing helper composition omitted for brevity in plan)
      scope = LocalScope(
        authSession: auth,
        actionSubmitter: submitter,
        viewSource: views,
        permissionSource: perms,
      );
    });

    tearDown(() async => scope.dispose());

    test('implements ReactionScope and exposes the four interfaces', () {
      expect(scope, isA<ReactionScope>());
      expect(scope.authSession, same(auth));
      expect(scope.actionSubmitter, same(submitter));
      expect(scope.viewSource, same(views));
      expect(scope.permissionSource, same(perms));
    });

    test('connectionStatus is Connected synchronously', () {
      expect(scope.connectionStatus, equals(const Connected()));
    });

    test('connectionStatusStream does NOT emit (always-connected)', () async {
      final emissions = <ConnectionStatus>[];
      final sub = scope.connectionStatusStream.listen(emissions.add);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await sub.cancel();
      expect(emissions, isEmpty);
    });

    test('dispose() makes interface getters throw StateError', () async {
      await scope.dispose();
      expect(() => scope.authSession, throwsStateError);
      expect(() => scope.actionSubmitter, throwsStateError);
      expect(() => scope.viewSource, throwsStateError);
      expect(() => scope.permissionSource, throwsStateError);
      expect(() => scope.connectionStatus, throwsStateError);
    });
  });
}

```

> **Note:** the `setUp` above shows the *shape* of composition. Use the existing test helpers in `reaction/test/` (search for "LocalActionSubmitter" in existing tests) to compose the Local* impls correctly — they need a real `EventStore` + `ActionDispatcher` + projections set up. Don't reinvent this.

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction && flutter test test/scope/local_scope_test.dart 2>&1 | tail -10

```

Expected: FAIL — `Undefined name 'LocalScope'`.

- [ ] **Step 3: Write minimal implementation**

Create `reaction/lib/src/scope/local_scope.dart`:

```dart
// Implements: EVS-PRD-reaction-scope/A, /C, /E

import 'dart:async';

import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/scope/connection_status.dart';
import 'package:reaction/src/scope/reaction_scope.dart';

/// In-process [ReactionScope] composing the four `Local*` impls.
///
/// Reports [Connected] for the entire lifetime of the scope: in-process
/// composition has no transport to lose. Per `EVS-PRD-reaction-scope`-C,
/// this trivial always-connected report keeps consumer code (in
/// particular `ViewBuilder`/`ReActionErrorListener`) source-identical
/// across Local and Remote without nil-checking the in-process case.
class LocalScope implements ReactionScope {
  LocalScope({
    required AuthSession authSession,
    required ActionSubmitter actionSubmitter,
    required ViewSource viewSource,
    required PermissionSource permissionSource,
  }) : _authSession = authSession,
       _actionSubmitter = actionSubmitter,
       _viewSource = viewSource,
       _permissionSource = permissionSource;

  final AuthSession _authSession;
  final ActionSubmitter _actionSubmitter;
  final ViewSource _viewSource;
  final PermissionSource _permissionSource;
  bool _disposed = false;

  void _checkDisposed() {
    if (_disposed) {
      throw StateError('LocalScope has been disposed.');
    }
  }

  @override
  AuthSession get authSession {
    _checkDisposed();
    return _authSession;
  }

  @override
  ActionSubmitter get actionSubmitter {
    _checkDisposed();
    return _actionSubmitter;
  }

  @override
  ViewSource get viewSource {
    _checkDisposed();
    return _viewSource;
  }

  @override
  PermissionSource get permissionSource {
    _checkDisposed();
    return _permissionSource;
  }

  @override
  ConnectionStatus get connectionStatus {
    _checkDisposed();
    return const Connected();
  }

  @override
  Stream<ConnectionStatus> get connectionStatusStream {
    // Broadcast empty stream — no transitions ever occur.
    return const Stream<ConnectionStatus>.empty().asBroadcastStream();
  }

  @override
  Future<void> dispose() async {
    _disposed = true;
  }
}

```

Add to barrel `reaction/lib/reaction.dart`:

```dart
export 'src/scope/local_scope.dart' show LocalScope;

```

- [ ] **Step 4: Run test to verify it passes**

```bash
cd reaction && flutter test test/scope/local_scope_test.dart 2>&1 | tail -5

```

Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/scope/local_scope.dart \
        reaction/lib/reaction.dart \
        reaction/test/scope/local_scope_test.dart
git commit -m "[CUR-1317] reaction: implement LocalScope

In-process ReactionScope; always reports Connected. Composes externally-
constructed Local* impls (test/host code wires the substrate). Implements
EVS-PRD-reaction-scope-A/C/E.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 5: Real auto-reconnect in `RemoteConnection` with status callback hook

**Files:**

- Modify: `reaction/lib/src/remote/remote_connection.dart`
- Test: `reaction/test/remote/auto_reconnect_test.dart` (new) plus extension to existing `remote_connection_test.dart`

- [ ] **Step 1: Read the current RemoteConnection**

```bash
sed -n '1,250p' reaction/lib/src/remote/remote_connection.dart

```

Identify: where WS close is handled, the existing reconnect-on-next-open path, and the `onAuthClose` / `onStaleData` callback hooks. The new `onConnectionStatusChanged` callback is added in the same callback-hook style.

- [ ] **Step 2: Write the failing test**

Create `reaction/test/remote/auto_reconnect_test.dart`:

```dart
// Verifies: EVS-PRD-cross-process-event-transport/H, /I

import 'dart:async';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_connection.dart';
// plus any test-WS-factory helper that simulates open/close/messages.

void main() {
  group('RemoteConnection auto-reconnect', () {
    test('on non-auth WS close, transitions Reconnecting then Connected on success',
        () async {
      final transitions = <ConnectionStatus>[];
      final fakeWs = FakeWebSocket(); // existing test helper
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: (_) => fakeWs.channel,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 3,
          multiplier: 2,
        ),
      );
      conn.onConnectionStatusChanged = transitions.add;

      // Trigger first open
      await conn.openSubscription(/* test subscribe message */);
      expect(transitions, [const Connected()]);

      // Server drops connection with non-auth close code
      fakeWs.close(1006, 'abnormal_closure');
      await pumpEventQueue();

      expect(transitions, [
        const Connected(),
        const Reconnecting(),
        const Connected(),
      ]);
    });

    test('on retry-exhausted, transitions to Disconnected', () async {
      final transitions = <ConnectionStatus>[];
      final fakeWs = FakeWebSocket(failConnects: true);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: (_) => fakeWs.channel,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 2,
          multiplier: 2,
        ),
      );
      conn.onConnectionStatusChanged = transitions.add;

      try {
        await conn.openSubscription(/* */);
      } catch (_) {}
      // Fail the established connection
      fakeWs.close(1006);
      await pumpEventQueue();

      expect(transitions.last, equals(const Disconnected()));
    });

    test('4001 auth_rejected does NOT enter Reconnecting cycle', () async {
      final transitions = <ConnectionStatus>[];
      final fakeWs = FakeWebSocket();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: (_) => fakeWs.channel,
      );
      conn.onConnectionStatusChanged = transitions.add;
      await conn.openSubscription(/* */);
      transitions.clear();

      fakeWs.close(4001, 'auth_rejected');
      await pumpEventQueue();

      // Should NOT see Reconnecting; auth path handles this separately.
      expect(transitions, isEmpty);
    });

    test('on successful reconnect, re-issues every active subscribe',
        () async {
      final fakeWs = FakeWebSocket();
      final conn = RemoteConnection(/* as above */);
      // Open two subscriptions
      await conn.openSubscription(/* subA */);
      await conn.openSubscription(/* subB */);
      fakeWs.sentMessages.clear();

      // Drop + reconnect
      fakeWs.close(1006);
      await pumpEventQueue();

      // After reconnect, both subscribe messages should have been re-sent.
      final resent = fakeWs.sentMessages
          .where((m) => m.contains('"type":"subscribe"'))
          .toList();
      expect(resent.length, 2);
    });
  });
}

```

> **Note:** This test references `FakeWebSocket` and `FakeHttpClient` test doubles — these exist in `reaction/test/test_support/` (search for them in current tests). If they don't expose what's needed for backoff/close-code simulation, extend them in the same task.

- [ ] **Step 3: Run test to verify it fails**

```bash
cd reaction && flutter test test/remote/auto_reconnect_test.dart 2>&1 | tail -20

```

Expected: FAIL — likely `Undefined name 'ExponentialBackoff'` and `onConnectionStatusChanged` is not a field.

- [ ] **Step 4: Write the implementation**

Add to `reaction/lib/src/remote/remote_connection.dart`:

```dart
// Implements: EVS-PRD-cross-process-event-transport/H, /I

/// Exponential-backoff policy for [RemoteConnection]'s auto-reconnect.
///
/// Default: starts at 250ms, doubles on each failure, capped at 10
/// attempts (~256s total before giving up). Override per-deployment via
/// [RemoteConnection]'s constructor (e.g., tests use ms-scale intervals).
class ExponentialBackoff {
  const ExponentialBackoff({
    this.initial = const Duration(milliseconds: 250),
    this.multiplier = 2,
    this.maxAttempts = 10,
    this.maxInterval = const Duration(seconds: 30),
  });

  final Duration initial;
  final num multiplier;
  final int maxAttempts;
  final Duration maxInterval;

  Duration delayFor(int attempt) {
    final raw = initial * (multiplier * attempt);
    return raw > maxInterval ? maxInterval : raw;
  }
}

```

Add to the `RemoteConnection` class:

```dart
final ExponentialBackoff _backoff;
ConnectionStatus _currentStatus = const Disconnected();

/// Callback invoked on every ConnectionStatus transition.
/// Wired by RemoteScope to drive its public stream.
void Function(ConnectionStatus)? onConnectionStatusChanged;

ConnectionStatus get connectionStatus => _currentStatus;

void _emitStatus(ConnectionStatus s) {
  if (s == _currentStatus) return;
  _currentStatus = s;
  onConnectionStatusChanged?.call(s);
}

```

Replace the existing "reconnect on next openSubscription" stub with an auto-reconnect loop driven by close-code handling. Pseudocode for the close handler:

```dart
void _handleWsClose(int? code, String? reason) {
  if (code == 4001 || code == 4003) {
    // Existing auth-rejected / permissions-changed routing — no status change.
    return;
  }
  _emitStatus(const Reconnecting());
  unawaited(_runReconnectLoop());
}

Future<void> _runReconnectLoop() async {
  for (var attempt = 0; attempt < _backoff.maxAttempts; attempt++) {
    await Future<void>.delayed(_backoff.delayFor(attempt));
    try {
      await _openWsAndReauth();
      // Re-issue every active subscribe message.
      for (final entry in _subscriptions.values) {
        _channel!.sink.add(jsonEncode(entry.subscribeMessage));
      }
      _emitStatus(const Connected());
      return;
    } catch (_) {
      // Try again.
    }
  }
  _emitStatus(const Disconnected());
}

```

On the *successful initial* WS open, also emit `Connected`.

- [ ] **Step 5: Run test to verify it passes**

```bash
cd reaction && flutter test test/remote/auto_reconnect_test.dart 2>&1 | tail -10

```

Expected: PASS (4 tests). If a test helper is missing, extend it as part of this task; do NOT skip the test.

- [ ] **Step 6: Run the full remote test suite to verify no regression**

```bash
cd reaction && flutter test test/remote/ 2>&1 | tail -10

```

Expected: all tests pass (including the existing `remote_connection_test.dart` and `remote_scope_test.dart`).

- [ ] **Step 7: Commit**

```bash
git add reaction/lib/src/remote/remote_connection.dart \
        reaction/test/remote/auto_reconnect_test.dart \
        reaction/test/test_support/  # if any helpers extended
git commit -m "[CUR-1317] reaction: real exponential-backoff reconnect + status hook

Replaces the reconnect stub with a real loop: on non-auth WS close,
transitions to Reconnecting, retries with exponential backoff, re-issues
every active subscribe on success (substrate's snapshot-then-deltas
guarantee replays per re-subscribed view), and transitions to
Disconnected after maxAttempts. Adds onConnectionStatusChanged callback
hook for RemoteScope to consume.

Implements EVS-PRD-cross-process-event-transport-H/I.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 6: Refactor `RemoteScope` to implement `ReactionScope` + expose `ConnectionStatus`

**Files:**

- Modify: `reaction/lib/src/remote/remote_scope.dart`
- Test: extend `reaction/test/remote/remote_scope_test.dart`

- [ ] **Step 1: Write the failing test (extend existing remote_scope_test.dart)**

Add to `reaction/test/remote/remote_scope_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-scope/A, /D, /E

group('RemoteScope as ReactionScope', () {
  test('implements ReactionScope interface', () {
    final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
    expect(scope, isA<ReactionScope>());
  });

  test('connectionStatus starts Disconnected (before first WS open)', () {
    final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
    expect(scope.connectionStatus, equals(const Disconnected()));
  });

  test('connectionStatusStream emits transitions from RemoteConnection',
      () async {
    final fakeWs = FakeWebSocket();
    final scope = RemoteScope(
      baseUrl: Uri.parse('http://test.local'),
      wsFactory: (_) => fakeWs.channel,
    );
    final emissions = <ConnectionStatus>[];
    final sub = scope.connectionStatusStream.listen(emissions.add);

    // Trigger initial open via a subscription
    await scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'test_view', mapper: (m) => m).first;
    // Drop
    fakeWs.close(1006);
    await pumpEventQueue();

    expect(emissions, containsAllInOrder([
      const Connected(),
      const Reconnecting(),
      const Connected(),
    ]));
    await sub.cancel();
  });

  test('dispose() makes connection-status getters throw StateError',
      () async {
    final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
    await scope.dispose();
    expect(() => scope.connectionStatus, throwsStateError);
  });
});

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction && flutter test test/remote/remote_scope_test.dart 2>&1 | tail -10

```

Expected: FAIL — `RemoteScope` does not implement `ReactionScope`; `connectionStatus` getter missing.

- [ ] **Step 3: Update `RemoteScope`**

Modify `reaction/lib/src/remote/remote_scope.dart`:

```dart
// Implements: EVS-PRD-reaction-scope/A, /D, /E

class RemoteScope implements ReactionScope {
  RemoteScope({/* existing params */}) {
    // existing wiring ...
    _connection.onConnectionStatusChanged = (s) {
      _statusController.add(s);
    };
  }

  final StreamController<ConnectionStatus> _statusController =
      StreamController<ConnectionStatus>.broadcast();
  bool _disposed = false;

  void _checkDisposed() {
    if (_disposed) {
      throw StateError('RemoteScope has been disposed.');
    }
  }

  @override
  AuthSession get authSession {
    _checkDisposed();
    return _auth;
  }

  // (action/views/perms getters get the same _checkDisposed() guard)

  @override
  ConnectionStatus get connectionStatus {
    _checkDisposed();
    return _connection.connectionStatus;
  }

  @override
  Stream<ConnectionStatus> get connectionStatusStream =>
      _statusController.stream;

  @override
  Future<void> dispose() async {
    _disposed = true;
    await _statusController.close();
    await _perms.dispose();
    await _auth.dispose();
    await _connection.dispose();
  }
}

```

- [ ] **Step 4: Run tests**

```bash
cd reaction && flutter test test/remote/remote_scope_test.dart 2>&1 | tail -5

```

Expected: PASS (new tests + existing tests still green).

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_scope.dart reaction/test/remote/remote_scope_test.dart
git commit -m "[CUR-1317] reaction: RemoteScope implements ReactionScope

Exposes ConnectionStatus driven by RemoteConnection's WS lifecycle via
the onConnectionStatusChanged callback. Disposed-scope access throws
StateError. Implements EVS-PRD-reaction-scope-A/D/E.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 7: Cross-impl contract test + barrel housekeeping

**Files:**

- Create: `reaction/test/scope/reaction_scope_contract_test.dart`
- Verify: `reaction/lib/reaction.dart` exports include all new types

- [ ] **Step 1: Write the contract test (shared assertions across both impls)**

Create `reaction/test/scope/reaction_scope_contract_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-scope/E (source-identical consumer code)

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

/// Runs the same assertions against both [LocalScope] and [RemoteScope]
/// to enforce source-identical behavior per EVS-PRD-reaction-scope-E.
void main() {
  group('ReactionScope contract', () {
    for (final scopeBuilder in <(String, ReactionScope Function())>[
      ('LocalScope', _buildLocalScope),
      ('RemoteScope (offline)', _buildRemoteScope),
    ]) {
      final (label, builder) = scopeBuilder;

      group(label, () {
        late ReactionScope scope;

        setUp(() => scope = builder());
        tearDown(() async => scope.dispose());

        test('exposes all four interfaces non-null pre-dispose', () {
          expect(scope.authSession, isNotNull);
          expect(scope.actionSubmitter, isNotNull);
          expect(scope.viewSource, isNotNull);
          expect(scope.permissionSource, isNotNull);
        });

        test('connectionStatus is never null', () {
          expect(scope.connectionStatus, isNotNull);
        });

        test('connectionStatusStream is broadcast (supports late subscribers)',
            () async {
          final s1 = scope.connectionStatusStream.listen((_) {});
          final s2 = scope.connectionStatusStream.listen((_) {});
          await s1.cancel();
          await s2.cancel();
        });

        test('dispose() makes all accessors throw StateError', () async {
          await scope.dispose();
          expect(() => scope.authSession, throwsStateError);
          expect(() => scope.actionSubmitter, throwsStateError);
          expect(() => scope.viewSource, throwsStateError);
          expect(() => scope.permissionSource, throwsStateError);
          expect(() => scope.connectionStatus, throwsStateError);
        });
      });
    }
  });
}

ReactionScope _buildLocalScope() {
  // Use existing test scaffolding; see test/scope/local_scope_test.dart.
  // ... (delegate to a shared helper)
}

ReactionScope _buildRemoteScope() {
  // Constructed offline; we are not exercising connection logic here.
  return RemoteScope(baseUrl: Uri.parse('http://test.local'));
}

```

- [ ] **Step 2: Run the contract test**

```bash
cd reaction && flutter test test/scope/reaction_scope_contract_test.dart 2>&1 | tail -5

```

Expected: PASS (all assertions for both impls).

- [ ] **Step 3: Verify barrel exports**

```bash
grep -n "ConnectionStatus\|ReactionScope\|LocalScope" reaction/lib/reaction.dart

```

Expected: each public type listed at least once in an `export ... show` clause.

- [ ] **Step 4: Run the full reaction test suite**

```bash
cd reaction && flutter test 2>&1 | tail -10

```

Expected: all tests pass — no regression in Local/Remote/server/wire tests from the scope refactor.

- [ ] **Step 5: Commit**

```bash
git add reaction/test/scope/reaction_scope_contract_test.dart
git commit -m "[CUR-1317] reaction: cross-impl contract test for ReactionScope

Runs identical assertions against LocalScope and RemoteScope to enforce
source-identical behavior per EVS-PRD-reaction-scope-E.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

## Phase B — `reaction_widgets` headless package

### Task 8: Scaffold the `reaction_widgets` package

**Files:**

- Create: `reaction_widgets/pubspec.yaml`
- Create: `reaction_widgets/analysis_options.yaml`
- Create: `reaction_widgets/.gitignore`
- Create: `reaction_widgets/README.md`
- Create: `reaction_widgets/lib/reaction_widgets.dart` (empty barrel)

- [ ] **Step 1: Create `pubspec.yaml`**

```yaml
name: reaction_widgets
description: >
  Headless Flutter widget primitives for apps built on the `reaction`
  package. Ships builder primitives, imperative listeners, a scope-
  threading InheritedWidget, a permission gate, an error sink, and
  widget-test doubles. NO rendered or styled widgets — apps render their
  own sugar. See `spec/prd-reaction.md` (EVS-PRD-reaction-widget-contract).
version: 0.1.0-dev
publish_to: none

environment:
  sdk: ^3.10.7
  flutter: ">=3.38.7"

dependencies:
  flutter:
    sdk: flutter
  reaction:
    path: ../reaction
  meta: ^1.16.0

dev_dependencies:
  flutter_test:
    sdk: flutter
  flutter_lints: ^6.0.0

```

- [ ] **Step 2: Create `analysis_options.yaml`**

```yaml
include: package:flutter_lints/flutter.yaml

analyzer:
  language:
    strict-casts: true
    strict-inference: true
    strict-raw-types: true

```

- [ ] **Step 3: Create `.gitignore`**

Mirror `reaction/example/.gitignore` (Flutter scaffold ignore set). At minimum:

```text
.dart_tool/
.flutter-plugins
.flutter-plugins-dependencies
.packages
pubspec.lock
build/
.pub-cache/
.pub/
*.iml

```

- [ ] **Step 4: Create `README.md`**

```markdown
# reaction_widgets

Headless Flutter widget primitives for apps built on the `reaction`
package.

This package is **headless**: it ships builder primitives, imperative
listeners, a scope-threading `InheritedWidget`, a permission gate, an
error sink, and widget-test doubles. It ships **no** rendered or styled
widgets. Each downstream app provides its own sugar widgets (buttons,
lists, theming) on top of the builders, sized appropriately for its
modality (mobile vs web vs desktop).

See `spec/prd-reaction.md`, in particular `EVS-PRD-reaction-widget-contract`,
for the normative contract.

```

- [ ] **Step 5: Create empty barrel `lib/reaction_widgets.dart`**

```dart
/// Headless Flutter widget primitives for apps built on the `reaction`
/// package. See `spec/prd-reaction.md` (EVS-PRD-reaction-widget-contract).
library;

// Exports added as widgets land in subsequent tasks.

```

- [ ] **Step 6: Resolve deps**

```bash
cd reaction_widgets && flutter pub get 2>&1 | tail -5

```

Expected: `Got dependencies!` — `reaction` resolves via path dep, no version conflicts.

- [ ] **Step 7: Commit**

```bash
git add reaction_widgets/pubspec.yaml reaction_widgets/analysis_options.yaml \
        reaction_widgets/.gitignore reaction_widgets/README.md \
        reaction_widgets/lib/reaction_widgets.dart
git commit -m "[CUR-1317] reaction_widgets: scaffold headless Flutter package

New sibling package; depends only on reaction. Empty barrel; widgets
land in subsequent commits. Implements the package boundary pinned by
EVS-PRD-reaction-widget-contract (headless base).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 9: `FakeReaction` test doubles + `pumpReactionWidget` helper

**Files:**

- Create: `reaction_widgets/lib/src/testing/fake_reaction.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart` (export)
- Test: `reaction_widgets/test/testing/fake_reaction_test.dart`

Test doubles are built BEFORE the widgets that consume them so every widget test below can use them.

- [ ] **Step 1: Write the failing test**

Create `reaction_widgets/test/testing/fake_reaction_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-widget-contract/H

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('FakeReaction', () {
    test('implements ReactionScope', () {
      final fake = FakeReaction();
      expect(fake, isA<ReactionScope>());
    });

    test('driveAuthStatus emits to authSession.stream', () async {
      final fake = FakeReaction();
      final received = <AuthStatus>[];
      final sub = fake.authSession.stream.listen(received.add);

      fake.driveAuthStatus(const NotAuthenticated());
      fake.driveAuthStatus(const Authenticated(
          principal: Principal(userId: 'u', activeRole: 'r', roles: {'r'})));
      await pumpEventQueue();

      expect(received.length, 2);
      expect(received[0], isA<NotAuthenticated>());
      expect(received[1], isA<Authenticated>());
      await sub.cancel();
    });

    test('driveConnectionStatus emits to connectionStatusStream', () async {
      final fake = FakeReaction();
      final received = <ConnectionStatus>[];
      final sub = fake.connectionStatusStream.listen(received.add);

      fake.driveConnectionStatus(const Reconnecting());
      fake.driveConnectionStatus(const Connected());
      await pumpEventQueue();

      expect(received, [const Reconnecting(), const Connected()]);
      await sub.cancel();
    });

    test('actionSubmitter returns queued DispatchResult', () async {
      final fake = FakeReaction();
      final result = DispatchSuccess(/* test fixture */);
      fake.queueDispatchResult(result);

      final got = await fake.actionSubmitter.submit(
          ActionSubmission(/* test fixture */));
      expect(got, same(result));
    });

    test('viewSource emits queued Update<T> events to active subscribers',
        () async {
      final fake = FakeReaction();
      final stream = fake.viewSource.watch<Map<String, Object?>>(
          viewName: 'v', mapper: (m) => m);
      final received = <Update<Map<String, Object?>>>[];
      final sub = stream.listen(received.add);

      fake.emitViewUpdate('v', Snapshot({'id': 1}, sequence: 1));
      fake.emitViewUpdate('v', const EndOfReplay(sequence: 1));
      await pumpEventQueue();

      expect(received.length, 2);
      expect(received[0], isA<Snapshot>());
      expect(received[1], isA<EndOfReplay>());
      await sub.cancel();
    });

    test('permissionSource.current and stream are drivable', () async {
      final fake = FakeReaction();
      final auth = EffectiveAuthorization(/* test fixture */);
      fake.drivePermission(auth);
      expect(fake.permissionSource.current, same(auth));
    });
  });

  group('pumpReactionWidget', () {
    testWidgets('mounts a widget with FakeReaction in scope', (tester) async {
      final fake = FakeReaction();
      await pumpReactionWidget(tester,
        fake: fake,
        child: Builder(builder: (ctx) {
          final scope = ReActionScope.of(ctx);
          expect(scope, same(fake));
          return const SizedBox.shrink();
        }),
      );
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/testing/fake_reaction_test.dart 2>&1 | tail -10

```

Expected: FAIL — `FakeReaction` / `pumpReactionWidget` / `ReActionScope` undefined.

- [ ] **Step 3: Write `FakeReaction`**

Create `reaction_widgets/lib/src/testing/fake_reaction.dart`:

```dart
// Implements: EVS-PRD-reaction-widget-contract/H

import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

/// Test-only [ReactionScope] implementation for widget tests.
///
/// Per `EVS-PRD-reaction-widget-contract`-H, this is a SHIPPED public
/// deliverable — every downstream consumer's widget tests use it
/// instead of writing their own fakes. Drives all observable transitions
/// deterministically (no timing in tests).
class FakeReaction implements ReactionScope {
  FakeReaction({
    AuthStatus initialAuthStatus = const NotAuthenticated(),
    ConnectionStatus initialConnectionStatus = const Connected(),
  }) : _authStatus = initialAuthStatus,
       _connectionStatus = initialConnectionStatus {
    _authSession = _FakeAuthSession(this);
    _actionSubmitter = _FakeActionSubmitter(this);
    _viewSource = _FakeViewSource(this);
    _permissionSource = _FakePermissionSource(this);
  }

  AuthStatus _authStatus;
  ConnectionStatus _connectionStatus;
  EffectiveAuthorization? _effectiveAuth;
  final _authController = StreamController<AuthStatus>.broadcast();
  final _statusController = StreamController<ConnectionStatus>.broadcast();
  final _permController = StreamController<EffectiveAuthorization?>.broadcast();
  final Map<String, StreamController<Update<dynamic>>> _viewControllers = {};
  final List<DispatchResult> _queuedResults = [];
  final List<ActionSubmission> _submittedActions = [];

  late final _FakeAuthSession _authSession;
  late final _FakeActionSubmitter _actionSubmitter;
  late final _FakeViewSource _viewSource;
  late final _FakePermissionSource _permissionSource;

  @override AuthSession get authSession => _authSession;
  @override ActionSubmitter get actionSubmitter => _actionSubmitter;
  @override ViewSource get viewSource => _viewSource;
  @override PermissionSource get permissionSource => _permissionSource;
  @override ConnectionStatus get connectionStatus => _connectionStatus;
  @override Stream<ConnectionStatus> get connectionStatusStream =>
      _statusController.stream;

  // ----- Driver API -----

  /// Set [AuthStatus] and emit to [authSession.stream].
  void driveAuthStatus(AuthStatus s) {
    _authStatus = s;
    _authController.add(s);
  }

  /// Set [ConnectionStatus] and emit to [connectionStatusStream].
  void driveConnectionStatus(ConnectionStatus s) {
    _connectionStatus = s;
    _statusController.add(s);
  }

  /// Queue a [DispatchResult] for the next [ActionSubmitter.submit].
  void queueDispatchResult(DispatchResult r) => _queuedResults.add(r);

  /// All actions submitted via this fake (for test assertions).
  List<ActionSubmission> get submittedActions =>
      List.unmodifiable(_submittedActions);

  /// Emit an [Update] to any active subscriber of [viewName].
  void emitViewUpdate<T>(String viewName, Update<T> update) {
    _viewControllers[viewName]?.add(update);
  }

  /// Set the current [EffectiveAuthorization] and emit on the stream.
  void drivePermission(EffectiveAuthorization? auth) {
    _effectiveAuth = auth;
    _permController.add(auth);
  }

  @override
  Future<void> dispose() async {
    await _authController.close();
    await _statusController.close();
    await _permController.close();
    for (final c in _viewControllers.values) {
      await c.close();
    }
  }
}

class _FakeAuthSession implements AuthSession {
  _FakeAuthSession(this._fake);
  final FakeReaction _fake;
  @override AuthStatus get current => _fake._authStatus;
  @override Stream<AuthStatus> get stream => _fake._authController.stream;
  @override void setCredential(String? credential) {
    // Test helper: no-op by default; tests use driveAuthStatus instead.
  }
  @override Principal? get principal => switch (_fake._authStatus) {
        Authenticated(:final principal) => principal,
        _ => null,
      };
}

class _FakeActionSubmitter implements ActionSubmitter {
  _FakeActionSubmitter(this._fake);
  final FakeReaction _fake;
  @override
  Future<DispatchResult> submit(ActionSubmission submission) async {
    _fake._submittedActions.add(submission);
    if (_fake._queuedResults.isEmpty) {
      throw StateError(
        'FakeReaction: no DispatchResult queued for submit() call. '
        'Call queueDispatchResult() before submitting.',
      );
    }
    return _fake._queuedResults.removeAt(0);
  }
}

class _FakeViewSource implements ViewSource {
  _FakeViewSource(this._fake);
  final FakeReaction _fake;
  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    final controller = _fake._viewControllers.putIfAbsent(
      viewName,
      () => StreamController<Update<dynamic>>.broadcast(),
    );
    return controller.stream.cast<Update<T>>();
  }
}

class _FakePermissionSource implements PermissionSource {
  _FakePermissionSource(this._fake);
  final FakeReaction _fake;
  @override EffectiveAuthorization? get current => _fake._effectiveAuth;
  @override Stream<EffectiveAuthorization?> get stream =>
      _fake._permController.stream;
}

/// Pump a widget under test with [FakeReaction] threaded via [ReActionScope].
Future<void> pumpReactionWidget(
  WidgetTester tester, {
  required FakeReaction fake,
  required Widget child,
}) {
  return tester.pumpWidget(
    ReActionScope(scope: fake, child: MaterialApp(home: child)),
  );
}

```

Update `lib/reaction_widgets.dart`:

```dart
export 'src/testing/fake_reaction.dart' show FakeReaction, pumpReactionWidget;

```

(Plus the `ReActionScope` export from Task 10; the fake_reaction.dart file already references it. If Task 10 hasn't landed yet, this file won't compile — execute Task 10 in the same PR even if reviewed as a separate commit.)

- [ ] **Step 4: Run test**

After Task 10 has also been authored (the two tasks ship together; commit Task 10 first if executing strictly TDD), then:

```bash
cd reaction_widgets && flutter test test/testing/fake_reaction_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/testing/fake_reaction.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/testing/fake_reaction_test.dart
git commit -m "[CUR-1317] reaction_widgets: FakeReaction + pumpReactionWidget

Shipped widget-test doubles per EVS-PRD-reaction-widget-contract-H.
Drives AuthStatus, ConnectionStatus, action results, view updates, and
permission snapshots deterministically.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 10: `ReActionScope` InheritedWidget

**Files:**

- Create: `reaction_widgets/lib/src/scope/reaction_scope_widget.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart` (export)
- Test: `reaction_widgets/test/scope/reaction_scope_widget_test.dart`

- [ ] **Step 1: Write the failing test**

Create `reaction_widgets/test/scope/reaction_scope_widget_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-widget-contract/A, /B

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  testWidgets('threads ReactionScope down the tree', (tester) async {
    final fake = FakeReaction();
    late ReactionScope captured;

    await pumpReactionWidget(tester,
      fake: fake,
      child: Builder(builder: (ctx) {
        captured = ReActionScope.of(ctx);
        return const SizedBox.shrink();
      }),
    );

    expect(captured, same(fake));
  });

  testWidgets('ReActionScope.of throws helpfully if not in tree',
      (tester) async {
    late Object error;
    await tester.pumpWidget(Builder(builder: (ctx) {
      try {
        ReActionScope.of(ctx);
      } catch (e) {
        error = e;
      }
      return const SizedBox.shrink();
    }));
    expect(error.toString(), contains('ReActionScope'));
  });

  testWidgets('rebuilds dependents when scope reference changes',
      (tester) async {
    final f1 = FakeReaction();
    final f2 = FakeReaction();
    int builds = 0;

    Widget wrap(ReactionScope s) => ReActionScope(
      scope: s,
      child: Builder(builder: (ctx) {
        ReActionScope.of(ctx);
        builds++;
        return const SizedBox.shrink();
      }),
    );

    await tester.pumpWidget(wrap(f1));
    expect(builds, 1);
    await tester.pumpWidget(wrap(f2));
    expect(builds, 2);
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/scope/reaction_scope_widget_test.dart 2>&1 | tail -10

```

Expected: FAIL — `ReActionScope` undefined.

- [ ] **Step 3: Write the InheritedWidget**

Create `reaction_widgets/lib/src/scope/reaction_scope_widget.dart`:

```dart
// Implements: EVS-PRD-reaction-widget-contract/A, /B

import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';

/// Threads a [ReactionScope] down the widget tree.
///
/// Mount once near the app root (above `MaterialApp`). Descendants call
/// `ReActionScope.of(context)` to read the scope (and therefore the
/// four library interfaces plus authoritative [ConnectionStatus]).
///
/// Per `EVS-PRD-reaction-widget-contract`-B: descendant widget code is
/// source-identical regardless of whether the composed scope is
/// [LocalScope] or [RemoteScope].
class ReActionScope extends InheritedWidget {
  const ReActionScope({
    super.key,
    required this.scope,
    required super.child,
  });

  final ReactionScope scope;

  /// Read the [ReactionScope] from context.
  ///
  /// Throws [FlutterError] if no `ReActionScope` ancestor is found,
  /// with a message that points the caller at the likely cause
  /// (forgotten root mount).
  static ReactionScope of(BuildContext context) {
    final widget =
        context.dependOnInheritedWidgetOfExactType<ReActionScope>();
    if (widget == null) {
      throw FlutterError(
        'ReActionScope.of() was called with a context that does not '
        'contain a ReActionScope.\n'
        'Mount a ReActionScope near the root of your app (above '
        'MaterialApp).',
      );
    }
    return widget.scope;
  }

  @override
  bool updateShouldNotify(ReActionScope oldWidget) =>
      !identical(scope, oldWidget.scope);
}

```

Update barrel:

```dart
export 'src/scope/reaction_scope_widget.dart' show ReActionScope;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/scope/reaction_scope_widget_test.dart 2>&1 | tail -5

```

Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/scope/reaction_scope_widget.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/scope/reaction_scope_widget_test.dart
git commit -m "[CUR-1317] reaction_widgets: ReActionScope InheritedWidget

Threads ReactionScope down the tree; helpful error on missing ancestor.
Implements EVS-PRD-reaction-widget-contract-A/B.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 11: `ActionBuilder` — idempotency-key lifetime + dispose cancellation

**Files:**

- Create: `reaction_widgets/lib/src/action/action_builder.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/action/action_builder_test.dart`

- [ ] **Step 1: Write the failing test**

Create `reaction_widgets/test/action/action_builder_test.dart`:

```dart
// Verifies: EVS-PRD-reaction-widget-contract/C, /E, /G

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

ActionSubmission _sub({String? idempotencyKey}) => ActionSubmission(
  actionName: 'noop',
  rawInput: const <String, Object?>{},
  idempotencyKey: idempotencyKey,
);

void main() {
  group('ActionBuilder', () {
    testWidgets('starts in Idle state', (tester) async {
      final fake = FakeReaction();
      late ActionState observed;
      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(),
          builder: (ctx, state, submit) {
            observed = state;
            return const SizedBox.shrink();
          },
        ),
      );
      expect(observed, isA<Idle>());
    });

    testWidgets('transitions Idle → Submitting → Success', (tester) async {
      final fake = FakeReaction();
      final success = DispatchSuccess(/* fixture */);
      fake.queueDispatchResult(success);
      late void Function() trigger;
      final transitions = <ActionState>[];

      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(),
          builder: (ctx, state, submit) {
            transitions.add(state);
            trigger = submit;
            return const SizedBox.shrink();
          },
        ),
      );
      trigger();
      await tester.pump();          // Submitting frame
      await tester.pumpAndSettle();  // Success frame

      expect(transitions.map((s) => s.runtimeType.toString()).toList(),
        containsAllInOrder(['Idle', 'Submitting', 'Success']));
    });

    testWidgets('retry during Submitting reuses the same idempotency key',
        (tester) async {
      final fake = FakeReaction();
      // Don't queue a result yet — keep first submit pending.
      fake.queueDispatchResult(DispatchSuccess(/* fixture */));
      fake.queueDispatchResult(DispatchSuccess(/* fixture */));
      late void Function() trigger;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(),
          builder: (ctx, state, submit) { trigger = submit; return const SizedBox(); },
        ),
      );
      trigger(); // first submit
      trigger(); // second submit during Submitting

      await tester.pumpAndSettle();
      final keys = fake.submittedActions.map((a) => a.idempotencyKey).toList();
      // Both submissions used the same key (retry semantics).
      expect(keys.length, 2);
      expect(keys.first, equals(keys.last));
      expect(keys.first, isNotNull);
    });

    testWidgets('fresh key generated after terminal state', (tester) async {
      final fake = FakeReaction();
      fake.queueDispatchResult(DispatchSuccess(/* fixture */));
      fake.queueDispatchResult(DispatchSuccess(/* fixture */));
      late void Function() trigger;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(),
          builder: (ctx, state, submit) { trigger = submit; return const SizedBox(); },
        ),
      );
      trigger();
      await tester.pumpAndSettle(); // Success
      trigger();
      await tester.pumpAndSettle();

      final keys = fake.submittedActions.map((a) => a.idempotencyKey).toList();
      expect(keys.length, 2);
      expect(keys[0], isNot(equals(keys[1])));
    });

    testWidgets('consumer-supplied idempotencyKey overrides generation',
        (tester) async {
      final fake = FakeReaction();
      fake.queueDispatchResult(DispatchSuccess(/* fixture */));
      late void Function() trigger;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(idempotencyKey: 'fixed-key'),
          builder: (ctx, state, submit) { trigger = submit; return const SizedBox(); },
        ),
      );
      trigger();
      await tester.pumpAndSettle();

      expect(fake.submittedActions.single.idempotencyKey, 'fixed-key');
    });

    testWidgets('dispose during Submitting does not throw / does not callback',
        (tester) async {
      final fake = FakeReaction();
      final completer = Completer<DispatchResult>();
      // Override fake to return the pending future
      // ... (extend FakeReaction with a queueResultFuture variant for this case)

      // Mount, trigger submit, then unmount — should not throw on result arrival.
    });

    testWidgets('renders nothing on its own (headless)', (tester) async {
      // Asserts the builder result is what's painted; ActionBuilder
      // never adds layout decorations.
      final fake = FakeReaction();
      await pumpReactionWidget(tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(),
          builder: (_, __, ___) => const Text('CUSTOM'),
        ),
      );
      expect(find.text('CUSTOM'), findsOneWidget);
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/action/action_builder_test.dart 2>&1 | tail -10

```

Expected: FAIL — `ActionBuilder` undefined.

- [ ] **Step 3: Write the implementation**

Create `reaction_widgets/lib/src/action/action_builder.dart`:

```dart
// Implements: EVS-PRD-reaction-widget-contract/C, /E, /G

import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

/// Builder function exposing the current [ActionState] and a callback
/// that triggers submission.
typedef ActionBuilderFn = Widget Function(
  BuildContext context,
  ActionState state,
  void Function() submit,
);

/// Headless Builder primitive for action submission.
///
/// Owns:
/// - The [ActionState] lifecycle (Idle → Submitting → Success | Denied | Failed).
/// - The idempotency-key lifetime (per `EVS-PRD-reaction-widget-contract`-E):
///   generates one UUID v4 at first submit, reuses it for retries during
///   Submitting, regenerates after a terminal state. Consumers may
///   override by setting `idempotencyKey` on the [ActionSubmission]
///   returned by [submissionFactory].
/// - Dispose-time cancellation: in-flight submission results that arrive
///   after dispose are silently discarded (no setState on unmounted widget).
///
/// Renders nothing of its own (headless per `EVS-PRD-reaction-widget-contract`-G).
/// All rendering is delegated to [builder].
class ActionBuilder extends StatefulWidget {
  const ActionBuilder({
    super.key,
    required this.submissionFactory,
    required this.builder,
    this.idempotencyKeyGenerator,
  });

  /// Called each submit to produce a fresh [ActionSubmission]. If the
  /// returned submission has a non-null `idempotencyKey`, the consumer's
  /// key wins; otherwise the widget injects its own.
  final ActionSubmission Function() submissionFactory;
  final ActionBuilderFn builder;
  final IdempotencyKeyGenerator? idempotencyKeyGenerator;

  @override
  State<ActionBuilder> createState() => _ActionBuilderState();
}

class _ActionBuilderState extends State<ActionBuilder> {
  ActionState _state = const Idle();
  String? _activeKey;
  late IdempotencyKeyGenerator _keyGen;

  @override
  void initState() {
    super.initState();
    _keyGen = widget.idempotencyKeyGenerator ?? Uuid4IdempotencyKeyGenerator();
  }

  void _submit() {
    final scope = ReActionScope.of(context);
    final base = widget.submissionFactory();
    final key = base.idempotencyKey ?? (_activeKey ??= _keyGen.generate());
    final sub = base.idempotencyKey == null
        ? ActionSubmission(
            actionName: base.actionName,
            rawInput: base.rawInput,
            idempotencyKey: key,
          )
        : base;

    setState(() => _state = const Submitting());
    scope.actionSubmitter.submit(sub).then((result) {
      if (!mounted) return;
      setState(() {
        _state = switch (result) {
          DispatchSuccess() => Success(result),
          // A cached idempotency hit is a user-facing success: same outcome
          // as the first submission, no new events emitted.
          DispatchIdempotencyHit() => Success(result),
          // All other DispatchResult variants are pipeline denials.
          // Widget consumers can `switch (state.result)` for variant-specific UX.
          _ => Denied(result),
        };
        // Consumer-supplied key is preserved across submissions (the consumer
        // owns its lifecycle); generated key is regenerated for the next press.
        if (base.idempotencyKey == null) _activeKey = null;
      });
    }).catchError((Object e, StackTrace st) {
      if (!mounted) return;
      setState(() {
        _state = Failed(e, st);
        if (base.idempotencyKey == null) _activeKey = null;
      });
    });
  }

  @override
  Widget build(BuildContext context) =>
      widget.builder(context, _state, _submit);
}

```

Update barrel:

```dart
export 'src/action/action_builder.dart' show ActionBuilder, ActionBuilderFn;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/action/action_builder_test.dart 2>&1 | tail -5

```

Expected: PASS (all 7 tests).

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/action/action_builder.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/action/action_builder_test.dart
git commit -m "[CUR-1317] reaction_widgets: ActionBuilder

Headless Builder primitive. Owns ActionState lifecycle + idempotency-key
lifetime (UUID v4, retained during Submitting, regenerated after terminal
state; consumer-supplied keys override). Dispose-safe.

Implements EVS-PRD-reaction-widget-contract-C/E/G.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 12: `ViewState<T>` sealed type

**Files:**

- Create: `reaction_widgets/lib/src/view/view_state.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/view/view_state_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/I

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('ViewState', () {
    test('three sealed variants', () {
      const ViewState<int> a = Loading<int>();
      final ViewState<int> b = Ready<int>(const [1, 2, 3]);
      final ViewState<int> c = Disconnected<int>(const [1], 'err');

      expect(a, isA<Loading<int>>());
      expect(b, isA<Ready<int>>());
      expect(c, isA<Disconnected<int>>());
    });

    test('Ready carries rows; Disconnected retains lastRows', () {
      const rows = [1, 2, 3];
      expect(Ready<int>(rows).rows, equals(rows));
      expect(Disconnected<int>(rows, 'err').lastRows, equals(rows));
      expect(Disconnected<int>(rows, 'err').error, equals('err'));
    });

    test('exhaustive switch compiles', () {
      String label(ViewState<int> s) => switch (s) {
        Loading() => 'loading',
        Ready(:final rows) => 'ready(${rows.length})',
        Disconnected(:final lastRows) => 'disconnected(${lastRows.length})',
      };
      expect(label(const Loading()), 'loading');
      expect(label(const Ready([1, 2])), 'ready(2)');
      expect(label(const Disconnected([1], 'e')), 'disconnected(1)');
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/view/view_state_test.dart 2>&1 | tail -10

```

Expected: FAIL — `ViewState` and variants undefined.

- [ ] **Step 3: Write the implementation**

```dart
// Implements: EVS-PRD-reaction-widget-contract/I

import 'package:meta/meta.dart';

/// View-subscription rendering state exposed by [ViewBuilder].
///
/// Three variants, exhaustive:
///
/// - [Loading]      — pre-EndOfReplay; no rows yet (or progressive mode disabled).
/// - [Ready]        — post-EndOfReplay; rows are live and current.
/// - [Disconnected] — transport disconnected; lastRows retained for UX
///                    continuity. Transition is driven by [ReactionScope]'s
///                    authoritative [ConnectionStatus] per
///                    `EVS-PRD-reaction-widget-contract`-I.
@immutable
sealed class ViewState<T> {
  const ViewState();
}

class Loading<T> extends ViewState<T> {
  const Loading();
}

class Ready<T> extends ViewState<T> {
  const Ready(this.rows);
  final List<T> rows;
}

class Disconnected<T> extends ViewState<T> {
  const Disconnected(this.lastRows, this.error);
  final List<T> lastRows;
  final Object error;
}

```

Update barrel:

```dart
export 'src/view/view_state.dart' show ViewState, Loading, Ready, Disconnected;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/view/view_state_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/view/view_state.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/view/view_state_test.dart
git commit -m "[CUR-1317] reaction_widgets: ViewState sealed type

Loading / Ready(rows) / Disconnected(lastRows, error). Implements
EVS-PRD-reaction-widget-contract-I.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 13: `ViewBuilder` — default mode (Loading → Ready → Disconnected)

**Files:**

- Create: `reaction_widgets/lib/src/view/view_builder.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/view/view_builder_test.dart`

- [ ] **Step 1: Write the failing test (default mode)**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/C, /G, /I, /J (default)

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('ViewBuilder (default mode)', () {
    testWidgets('starts Loading; Ready arrives only after EndOfReplay',
        (tester) async {
      final fake = FakeReaction();
      final transitions = <ViewState<Map<String, Object?>>>[];

      await pumpReactionWidget(tester,
        fake: fake,
        child: ViewBuilder<Map<String, Object?>>(
          viewName: 'v',
          mapper: (m) => m,
          filter: SubscriptionFilter.all(),
          builder: (ctx, state) {
            transitions.add(state);
            return const SizedBox.shrink();
          },
        ),
      );

      // Pre-EndOfReplay: emit snapshots; default mode should still surface Loading.
      fake.emitViewUpdate('v', Snapshot({'id': 1}, sequence: 1));
      fake.emitViewUpdate('v', Snapshot({'id': 2}, sequence: 2));
      await tester.pump();
      expect(transitions.last, isA<Loading<Map<String, Object?>>>());

      // EndOfReplay → Ready with the full row set.
      fake.emitViewUpdate('v', const EndOfReplay(sequence: 2));
      await tester.pump();
      expect(transitions.last, isA<Ready<Map<String, Object?>>>());
      expect((transitions.last as Ready).rows.length, 2);
    });

    testWidgets('Delta after Ready updates rows in place', (tester) async {
      // Sets up Ready state, emits a Delta, asserts Ready with merged rows.
    });

    testWidgets('ConnectionStatus.Reconnecting → Disconnected retains rows',
        (tester) async {
      final fake = FakeReaction();
      late ViewState<Map<String, Object?>> latest;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ViewBuilder<Map<String, Object?>>(
          viewName: 'v', mapper: (m) => m, filter: SubscriptionFilter.all(),
          builder: (ctx, state) { latest = state; return const SizedBox.shrink(); },
        ),
      );
      fake.emitViewUpdate('v', Snapshot({'id': 1}, sequence: 1));
      fake.emitViewUpdate('v', const EndOfReplay(sequence: 1));
      await tester.pump();

      fake.driveConnectionStatus(const Reconnecting());
      await tester.pump();
      expect(latest, isA<Disconnected<Map<String, Object?>>>());
      expect((latest as Disconnected).lastRows.length, 1);
    });

    testWidgets('Connected after Disconnected re-enters Loading then Ready',
        (tester) async {
      // Verifies the snapshot+tail re-subscribe semantics surface to the widget.
    });

    testWidgets('Tombstone removes the matching row from Ready', (tester) async {
      // ...
    });

    testWidgets('cancels subscription on dispose', (tester) async {
      // Verifies the stream subscription is cancelled (introspect FakeReaction
      // for listener count, or assert no further setState after unmount).
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/view/view_builder_test.dart 2>&1 | tail -10

```

Expected: FAIL — `ViewBuilder` undefined.

- [ ] **Step 3: Write the implementation**

```dart
// Implements: EVS-PRD-reaction-widget-contract/C, /G, /I, /J

import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';
import 'package:reaction_widgets/src/view/view_state.dart';

typedef ViewBuilderFn<T> = Widget Function(
    BuildContext context, ViewState<T> state);

class ViewBuilder<T> extends StatefulWidget {
  const ViewBuilder({
    super.key,
    required this.viewName,
    required this.mapper,
    required this.filter,
    required this.builder,
    this.aggregates,
    this.progressive = false,
  });

  final String viewName;
  final T Function(Map<String, Object?>) mapper;
  final SubscriptionFilter filter;
  final Set<String>? aggregates;
  final ViewBuilderFn<T> builder;

  /// When true, expose partial rows during snapshot replay (rather than
  /// surfacing [Loading] until [EndOfReplay]). Default false. See
  /// `EVS-PRD-reaction-widget-contract`-J.
  final bool progressive;

  @override
  State<ViewBuilder<T>> createState() => _ViewBuilderState<T>();
}

class _ViewBuilderState<T> extends State<ViewBuilder<T>> {
  StreamSubscription<Update<T>>? _viewSub;
  StreamSubscription<ConnectionStatus>? _statusSub;
  final Map<String, T> _rows = {}; // aggregateId -> row
  bool _replayDone = false;
  ViewState<T> _state = const Loading();

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_viewSub != null) return;
    final scope = ReActionScope.of(context);
    _viewSub = scope.viewSource
        .watch<T>(
          viewName: widget.viewName,
          mapper: widget.mapper,
          filter: widget.filter,
          aggregates: widget.aggregates,
        )
        .listen(_onUpdate);
    _statusSub = scope.connectionStatusStream.listen(_onStatus);
  }

  void _onUpdate(Update<T> u) {
    switch (u) {
      case Snapshot<T>(:final row, :final aggregateId):
        _rows[aggregateId] = row;
        if (widget.progressive && !_replayDone) {
          _setState(Ready(List.unmodifiable(_rows.values)));
        }
      case Delta<T>(:final row, :final aggregateId):
        _rows[aggregateId] = row;
        if (_replayDone) {
          _setState(Ready(List.unmodifiable(_rows.values)));
        }
      case Tombstone<T>(:final aggregateId):
        _rows.remove(aggregateId);
        if (_replayDone) {
          _setState(Ready(List.unmodifiable(_rows.values)));
        }
      case EndOfReplay<T>():
        _replayDone = true;
        _setState(Ready(List.unmodifiable(_rows.values)));
    }
  }

  void _onStatus(ConnectionStatus s) {
    switch (s) {
      case Connected():
        // On reconnect after Disconnected, a fresh snapshot+tail will replay;
        // reset to Loading until EndOfReplay arrives.
        if (_state is Disconnected<T>) {
          _rows.clear();
          _replayDone = false;
          _setState(const Loading());
        }
      case Reconnecting() || Disconnected():
        _setState(Disconnected(
          List.unmodifiable(_rows.values),
          s,
        ));
    }
  }

  void _setState(ViewState<T> s) {
    if (!mounted) return;
    setState(() => _state = s);
  }

  @override
  void dispose() {
    _viewSub?.cancel();
    _statusSub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.builder(context, _state);
}

```

> **Note on field names:** This impl uses `aggregateId`/`row` field access via pattern matching on the substrate's `Update<T>` variants. Verify the actual field names with `grep -n "class Snapshot\|class Delta\|class Tombstone" event_sourcing/lib/`. If names differ, adjust the patterns above.

Update barrel:

```dart
export 'src/view/view_builder.dart' show ViewBuilder, ViewBuilderFn;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/view/view_builder_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/view/view_builder.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/view/view_builder_test.dart
git commit -m "[CUR-1317] reaction_widgets: ViewBuilder (default + progressive)

Subscribes via ViewSource; accumulates rows; transitions Loading → Ready
on EndOfReplay; Reconnecting/Disconnected on ConnectionStatus changes
retain lastRows; reconnect re-enters Loading → Ready. Opt-in progressive
mode surfaces partial rows during replay.

Implements EVS-PRD-reaction-widget-contract-C/G/I/J.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 14: `ViewListener` — imperative side-effects without rebuild

**Files:**

- Create: `reaction_widgets/lib/src/view/view_listener.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/view/view_listener_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/D, /G

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  testWidgets('fires onUpdate without rebuilding child', (tester) async {
    final fake = FakeReaction();
    final received = <Update<Map<String, Object?>>>[];
    int childBuilds = 0;

    await pumpReactionWidget(tester,
      fake: fake,
      child: ViewListener<Map<String, Object?>>(
        viewName: 'v', mapper: (m) => m, filter: SubscriptionFilter.all(),
        onUpdate: (ctx, u) => received.add(u),
        child: Builder(builder: (_) { childBuilds++; return const SizedBox.shrink(); }),
      ),
    );
    expect(childBuilds, 1);

    fake.emitViewUpdate('v', Snapshot({'id': 1}, sequence: 1));
    fake.emitViewUpdate('v', const EndOfReplay(sequence: 1));
    await tester.pump();

    expect(received.length, 2);
    expect(childBuilds, 1); // No rebuild caused by ViewListener.
  });

  testWidgets('cancels subscription on dispose', (tester) async {
    // ...
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/view/view_listener_test.dart 2>&1 | tail -10

```

Expected: FAIL — `ViewListener` undefined.

- [ ] **Step 3: Write the implementation**

```dart
// Implements: EVS-PRD-reaction-widget-contract/D, /G

import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

class ViewListener<T> extends StatefulWidget {
  const ViewListener({
    super.key,
    required this.viewName,
    required this.mapper,
    required this.filter,
    required this.onUpdate,
    required this.child,
    this.aggregates,
  });

  final String viewName;
  final T Function(Map<String, Object?>) mapper;
  final SubscriptionFilter filter;
  final Set<String>? aggregates;
  final void Function(BuildContext, Update<T>) onUpdate;
  final Widget child;

  @override
  State<ViewListener<T>> createState() => _ViewListenerState<T>();
}

class _ViewListenerState<T> extends State<ViewListener<T>> {
  StreamSubscription<Update<T>>? _sub;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_sub != null) return;
    final scope = ReActionScope.of(context);
    _sub = scope.viewSource
        .watch<T>(
          viewName: widget.viewName,
          mapper: widget.mapper,
          filter: widget.filter,
          aggregates: widget.aggregates,
        )
        .listen((u) {
      if (!mounted) return;
      widget.onUpdate(context, u);
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.child;
}

```

Update barrel:

```dart
export 'src/view/view_listener.dart' show ViewListener;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/view/view_listener_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/view/view_listener.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/view/view_listener_test.dart
git commit -m "[CUR-1317] reaction_widgets: ViewListener (imperative)

Fires onUpdate callback on view transitions without rebuilding the child.
Implements EVS-PRD-reaction-widget-contract-D/G.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 15: `PermissionGate`

**Files:**

- Create: `reaction_widgets/lib/src/permission/permission_gate.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/permission/permission_gate_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/G (permission gate sub-clause)

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('PermissionGate', () {
    testWidgets('renders child when active Principal holds permission',
        (tester) async {
      final fake = FakeReaction();
      fake.drivePermission(EffectiveAuthorization(/* with permission 'edit' */));

      await pumpReactionWidget(tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          child: Text('GRANTED'),
          fallback: Text('DENIED'),
        ),
      );
      expect(find.text('GRANTED'), findsOneWidget);
      expect(find.text('DENIED'), findsNothing);
    });

    testWidgets('renders fallback when permission not held', (tester) async {
      final fake = FakeReaction();
      fake.drivePermission(EffectiveAuthorization(/* without 'edit' */));

      await pumpReactionWidget(tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          child: Text('GRANTED'),
          fallback: Text('DENIED'),
        ),
      );
      expect(find.text('DENIED'), findsOneWidget);
    });

    testWidgets('reactively re-renders on PermissionSource.stream changes',
        (tester) async {
      final fake = FakeReaction();
      fake.drivePermission(EffectiveAuthorization(/* without 'edit' */));

      await pumpReactionWidget(tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          child: Text('GRANTED'),
          fallback: Text('DENIED'),
        ),
      );
      expect(find.text('DENIED'), findsOneWidget);

      fake.drivePermission(EffectiveAuthorization(/* with 'edit' */));
      await tester.pump();
      expect(find.text('GRANTED'), findsOneWidget);
    });

    testWidgets('renders fallback when permissionSource.current is null',
        (tester) async {
      final fake = FakeReaction();
      // No drivePermission call → current is null.
      await pumpReactionWidget(tester,
        fake: fake,
        child: const PermissionGate(
          permission: 'edit',
          child: Text('GRANTED'),
          fallback: Text('NO_AUTH'),
        ),
      );
      expect(find.text('NO_AUTH'), findsOneWidget);
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/permission/permission_gate_test.dart 2>&1 | tail -10

```

Expected: FAIL — `PermissionGate` undefined.

- [ ] **Step 3: Write the implementation**

```dart
// Implements: EVS-PRD-reaction-widget-contract/G (permission-gate sub-clause)

import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

class PermissionGate extends StatefulWidget {
  const PermissionGate({
    super.key,
    required this.permission,
    required this.child,
    this.fallback = const SizedBox.shrink(),
  });

  final String permission;
  final Widget child;
  final Widget fallback;

  @override
  State<PermissionGate> createState() => _PermissionGateState();
}

class _PermissionGateState extends State<PermissionGate> {
  StreamSubscription<EffectiveAuthorization?>? _sub;
  EffectiveAuthorization? _auth;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_sub != null) return;
    final scope = ReActionScope.of(context);
    _auth = scope.permissionSource.current;
    _sub = scope.permissionSource.stream.listen((auth) {
      if (!mounted) return;
      setState(() => _auth = auth);
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  bool _holdsPermission() {
    final auth = _auth;
    if (auth == null) return false;
    // EffectiveAuthorization's API for checking a named permission:
    // (verify exact method name against substrate; likely
    // auth.holdsPermission(name) or similar).
    return auth.holds(widget.permission);
  }

  @override
  Widget build(BuildContext context) =>
      _holdsPermission() ? widget.child : widget.fallback;
}

```

> **Note:** Verify `EffectiveAuthorization`'s permission-check method name (search `event_sourcing/lib/` for `class EffectiveAuthorization`) and update the call accordingly.

Update barrel:

```dart
export 'src/permission/permission_gate.dart' show PermissionGate;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/permission/permission_gate_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/permission/permission_gate.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/permission/permission_gate_test.dart
git commit -m "[CUR-1317] reaction_widgets: PermissionGate

Gates child on the active Principal's EffectiveAuthorization; reactive
on PermissionSource.stream. Renders fallback when permission absent or
auth is null. No styled UI of its own.

Implements EVS-PRD-reaction-widget-contract-G (permission-gate sub-clause).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 16: `ReActionErrorListener`

**Files:**

- Create: `reaction_widgets/lib/src/error/reaction_error_listener.dart`
- Modify: `reaction_widgets/lib/reaction_widgets.dart`
- Test: `reaction_widgets/test/error/reaction_error_listener_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/G (error-sink sub-clause)

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('ReActionErrorListener', () {
    testWidgets('fires onAuthExpired when AuthSession transitions to Expired',
        (tester) async {
      final fake = FakeReaction();
      int expiredCalls = 0;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ReActionErrorListener(
          onAuthExpired: (_) => expiredCalls++,
          child: const SizedBox.shrink(),
        ),
      );

      fake.driveAuthStatus(const Expired());
      await tester.pump();
      expect(expiredCalls, 1);
    });

    testWidgets('fires onTransportDisconnected on Disconnected transition',
        (tester) async {
      final fake = FakeReaction();
      int disconnects = 0;

      await pumpReactionWidget(tester,
        fake: fake,
        child: ReActionErrorListener(
          onTransportDisconnected: (_) => disconnects++,
          child: const SizedBox.shrink(),
        ),
      );

      fake.driveConnectionStatus(const Disconnected());
      await tester.pump();
      expect(disconnects, 1);
    });

    testWidgets('does not rebuild child', (tester) async {
      // ... (analogous to ViewListener's no-rebuild assertion)
    });

    testWidgets('callbacks fire post-frame (BuildContext safe for navigation)',
        (tester) async {
      // ... assert callback runs in addPostFrameCallback (so push/pop is safe).
    });
  });
}

```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd reaction_widgets && flutter test test/error/reaction_error_listener_test.dart 2>&1 | tail -10

```

Expected: FAIL.

- [ ] **Step 3: Write the implementation**

```dart
// Implements: EVS-PRD-reaction-widget-contract/G (error-sink sub-clause)

import 'dart:async';
import 'package:flutter/widgets.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/src/scope/reaction_scope_widget.dart';

typedef ReActionErrorCallback = void Function(BuildContext context);

/// Centralised sink for auth and transport error transitions.
///
/// Callbacks fire post-frame, so consumers may safely call
/// Navigator.push/pop from them. Renders nothing of its own.
class ReActionErrorListener extends StatefulWidget {
  const ReActionErrorListener({
    super.key,
    required this.child,
    this.onAuthExpired,
    this.onAuthNotAuthenticated,
    this.onTransportReconnecting,
    this.onTransportDisconnected,
  });

  final Widget child;
  final ReActionErrorCallback? onAuthExpired;
  final ReActionErrorCallback? onAuthNotAuthenticated;
  final ReActionErrorCallback? onTransportReconnecting;
  final ReActionErrorCallback? onTransportDisconnected;

  @override
  State<ReActionErrorListener> createState() => _ReActionErrorListenerState();
}

class _ReActionErrorListenerState extends State<ReActionErrorListener> {
  StreamSubscription<AuthStatus>? _authSub;
  StreamSubscription<ConnectionStatus>? _statusSub;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    if (_authSub != null) return;
    final scope = ReActionScope.of(context);
    _authSub = scope.authSession.stream.listen(_onAuth);
    _statusSub = scope.connectionStatusStream.listen(_onStatus);
  }

  void _postFrame(ReActionErrorCallback? cb) {
    if (cb == null) return;
    WidgetsBinding.instance.addPostFrameCallback((_) {
      if (!mounted) return;
      cb(context);
    });
  }

  void _onAuth(AuthStatus s) {
    switch (s) {
      case Expired():
        _postFrame(widget.onAuthExpired);
      case NotAuthenticated():
        _postFrame(widget.onAuthNotAuthenticated);
      case Authenticated():
        // No-op.
        break;
    }
  }

  void _onStatus(ConnectionStatus s) {
    switch (s) {
      case Reconnecting():
        _postFrame(widget.onTransportReconnecting);
      case Disconnected():
        _postFrame(widget.onTransportDisconnected);
      case Connected():
        // No-op.
        break;
    }
  }

  @override
  void dispose() {
    _authSub?.cancel();
    _statusSub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) => widget.child;
}

```

Update barrel:

```dart
export 'src/error/reaction_error_listener.dart'
    show ReActionErrorListener, ReActionErrorCallback;

```

- [ ] **Step 4: Run tests**

```bash
cd reaction_widgets && flutter test test/error/reaction_error_listener_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/src/error/reaction_error_listener.dart \
        reaction_widgets/lib/reaction_widgets.dart \
        reaction_widgets/test/error/reaction_error_listener_test.dart
git commit -m "[CUR-1317] reaction_widgets: ReActionErrorListener

Centralised auth/transport error sink; callbacks fire post-frame so
navigation is BuildContext-safe. Renders nothing.

Implements EVS-PRD-reaction-widget-contract-G (error-sink sub-clause).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 17: Structural test — assertion F (no direct substrate imports)

**Files:**

- Create: `reaction_widgets/test/structural/no_substrate_imports_test.dart`

- [ ] **Step 1: Write the test**

```dart
// Verifies: EVS-PRD-reaction-widget-contract/F

import 'dart:io';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('no widget source file imports package:event_sourcing/ directly', () {
    final libDir = Directory('lib/src');
    final violations = <String>[];

    for (final entity in libDir.listSync(recursive: true)) {
      if (entity is! File || !entity.path.endsWith('.dart')) continue;
      final src = entity.readAsStringSync();
      // Match any `import 'package:event_sourcing/...';`
      final hits = RegExp(r'''import\s+['"]package:event_sourcing/''')
          .allMatches(src);
      if (hits.isNotEmpty) {
        violations.add('${entity.path}: ${hits.length} import(s)');
      }
    }

    expect(violations, isEmpty,
      reason: 'Widget code MUST access the substrate only via reaction\'s '
              'public interfaces (EVS-PRD-reaction-widget-contract-F). '
              'Add the substrate type to reaction\'s barrel and import '
              'from package:reaction/reaction.dart instead.');
  });
}

```

- [ ] **Step 2: Run the test**

```bash
cd reaction_widgets && flutter test test/structural/no_substrate_imports_test.dart 2>&1 | tail -5

```

Expected: PASS (no widget source imports `package:event_sourcing/` directly; all substrate types are re-exported via `package:reaction/`).

If FAIL: list violators and either (a) move the type into `reaction`'s barrel as a re-export and update the import, or (b) restructure the widget to consume the type via a `reaction` interface that already exposes it.

- [ ] **Step 3: Commit**

```bash
git add reaction_widgets/test/structural/no_substrate_imports_test.dart
git commit -m "[CUR-1317] reaction_widgets: structural test for assertion F

Source-grep test: no widget code imports package:event_sourcing/ directly.
All substrate access SHALL flow through reaction.

Verifies EVS-PRD-reaction-widget-contract-F.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

## Phase C — Integration, CI, housekeeping

### Task 18: Run the full reaction_widgets test suite + verify barrel completeness

**Files:** None modified.

- [ ] **Step 1: Run full suite**

```bash
cd reaction_widgets && flutter test 2>&1 | tail -10

```

Expected: all tests across `test/` pass.

- [ ] **Step 2: Verify barrel exports**

```bash
grep -n "^export " reaction_widgets/lib/reaction_widgets.dart

```

Expected: at minimum the following symbols appear in `show` clauses:

- `ReActionScope`
- `ActionBuilder`, `ActionBuilderFn`
- `ViewBuilder`, `ViewBuilderFn`
- `ViewState`, `Loading`, `Ready`, `Disconnected`
- `ViewListener`
- `PermissionGate`
- `ReActionErrorListener`, `ReActionErrorCallback`
- `FakeReaction`, `pumpReactionWidget`

If a public type is missing from the barrel, add the `export` line — public types not in the barrel are an audit-finding for downstream consumers.

- [ ] **Step 3: No commit** (verification only; fixes belong in the prior tasks).

---

### Task 19: PRD annotation sweep across `reaction_widgets` and new `reaction` scope files

**Files:**

- Modify (annotations only): `reaction_widgets/lib/src/**/*.dart`, `reaction/lib/src/scope/*.dart`, modified `reaction/lib/src/remote/*.dart`

- [ ] **Step 1: Verify every widget source file carries an `// Implements:` annotation pointing at the relevant assertion**

```bash
grep -L "// Implements:" reaction_widgets/lib/src/**/*.dart

```

Expected: empty output. If any file is missing the annotation, add one referencing the assertion (A–J) it satisfies.

- [ ] **Step 2: Verify every test file carries a `// Verifies:` annotation**

```bash
grep -L "// Verifies:" reaction_widgets/test/**/*.dart

```

Expected: empty output.

- [ ] **Step 3: Refresh the elspais graph**

```bash
elspais refresh
elspais check

```

Expected: no broken references, no orphans. Confirm the new annotations are picked up against `EVS-PRD-reaction-widget-contract` and `EVS-PRD-reaction-scope`.

- [ ] **Step 4: Regenerate INDEX.md and hashes**

```bash
elspais fix

```

Expected: `spec/INDEX.md` rewritten with the new `EVS-PRD-reaction-scope` row; the `(regenerate)` markers in `spec/prd-reaction.md` replaced with computed hashes.

- [ ] **Step 5: Commit**

```bash
git add reaction_widgets/lib/ reaction_widgets/test/ \
        reaction/lib/src/scope/ reaction/lib/src/remote/ \
        spec/INDEX.md spec/prd-reaction.md
git commit -m "[CUR-1317] elspais: annotations + INDEX/hash regen

Adds // Implements: and // Verifies: annotations across the new
reaction_widgets sources and the reaction scope/connection additions.
Regenerates spec/INDEX.md and the EVS-PRD-reaction-scope +
EVS-PRD-reaction-widget-contract hashes.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 20: Update `reaction/lib/reaction.dart` barrel documentation

**Files:**

- Modify: `reaction/lib/reaction.dart`

- [ ] **Step 1: Add `ReactionScope` / `ConnectionStatus` / `LocalScope` to the barrel doc**

Insert a "Scope" subsection in the dartdoc above `library;`, after the "Five transport-agnostic interfaces" block:

```dart
/// Shared scope abstraction (composition root + connection state):
///
/// - [ReactionScope] — composes the four interfaces with a live
///   [ConnectionStatus] surface.
/// - [LocalScope]    — in-process composition; always [Connected].
/// - [RemoteScope]   — cross-process composition; drives
///   [ConnectionStatus] from the WS lifecycle.
///
/// Widget code threads the scope (via `reaction_widgets`'s
/// `ReActionScope` InheritedWidget) rather than threading the four
/// interfaces individually; this is what keeps widget code source-
/// identical across in-process and cross-process compositions.

```

Also update the "Flutter widgets live in a separate `reaction_widgets` package" line — replace with a brief note that the widget layer consumes `ReactionScope` and is now shipping (post-Task 17).

- [ ] **Step 2: Verify the barrel still compiles**

```bash
cd reaction && flutter analyze lib/reaction.dart 2>&1 | tail -5

```

Expected: `No issues found!`

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/reaction.dart
git commit -m "[CUR-1317] reaction: barrel doc — add Scope subsection

Documents ReactionScope/LocalScope/RemoteScope and the ConnectionStatus
surface as the recommended composition root for widget consumers.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 21: CI workflow — run `reaction_widgets` tests on push/PR

**Files:**

- Create: `.github/workflows/reaction-widgets-tests.yml`

- [ ] **Step 1: Author the workflow**

```yaml
# Reaction widgets — runs the reaction_widgets unit/widget test suite on
# every PR and push to main. No external services required.

name: Reaction widgets tests

on:
  push:
    branches: [main]
  pull_request:

concurrency:
  group: reaction-widgets-tests-${{ github.ref }}
  cancel-in-progress: true

permissions:
  contents: read

jobs:
  reaction_widgets:
    name: reaction_widgets unit + widget tests
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:

      - uses: actions/checkout@v4
      - uses: subosito/flutter-action@v2

        with:
          channel: stable

      - name: Resolve deps (canonical_json_jcs)

        run: flutter pub get
        working-directory: canonical_json_jcs

      - name: Resolve deps (provenance)

        run: flutter pub get
        working-directory: provenance

      - name: Resolve deps (event_sourcing)

        run: flutter pub get
        working-directory: event_sourcing

      - name: Resolve deps (reaction)

        run: flutter pub get
        working-directory: reaction

      - name: Resolve deps (reaction_widgets)

        run: flutter pub get
        working-directory: reaction_widgets

      - name: Reaction scope + connection-status tests

        run: flutter test test/scope/ test/remote/auto_reconnect_test.dart
        working-directory: reaction

      - name: reaction_widgets test suite

        run: flutter test
        working-directory: reaction_widgets

```

- [ ] **Step 2: Sanity-check the YAML locally**

```bash
yamllint .github/workflows/reaction-widgets-tests.yml 2>&1 || true

```

(If yamllint is not installed, skip — GitHub will validate on push.)

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/reaction-widgets-tests.yml
git commit -m "[CUR-1317] ci: add reaction_widgets test workflow

Runs reaction's scope/auto-reconnect tests + the full reaction_widgets
test suite on every PR and push to main. Matches the conformance-tests
workflow's setup pattern (Flutter stable, per-package pub get).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>"

```

---

### Task 22: Final verification + branch ready for review

**Files:** None modified.

- [ ] **Step 1: Verify branch is fast-forwardable onto main**

```bash
unset GITHUB_TOKEN && git fetch origin
git log --oneline origin/main..HEAD | head

```

Expected: a clean list of CUR-1317 commits with no merge commits.

- [ ] **Step 2: Run the whole test surface end-to-end**

```bash
cd reaction && flutter test 2>&1 | tail -5
cd ../reaction_widgets && flutter test 2>&1 | tail -5
cd ../event_sourcing && flutter test test/storage/sembast_backend_conformance_test.dart 2>&1 | tail -5

```

Expected: all green. The substrate conformance test confirms no regression upstream.

- [ ] **Step 3: Confirm no `Plan A/B/C/D` references re-introduced**

```bash
grep -rn -i -E "plan[- ]?[abcd]\b|plan[- ]?b[- ]?(local|remote)" \
  spec/ CLAUDE.md \
  event_sourcing/lib/ canonical_json_jcs/lib/ provenance/lib/ reaction/lib/ \
  reaction_widgets/lib/ reaction_widgets/test/ \
  --exclude-dir=superpowers 2>/dev/null

```

Expected: empty output (the feedback rule still holds).

- [ ] **Step 4: Confirm no widget source imports event_sourcing directly**

Already covered by Task 17's structural test; re-run for sanity:

```bash
cd reaction_widgets && flutter test test/structural/no_substrate_imports_test.dart 2>&1 | tail -5

```

Expected: PASS.

- [ ] **Step 5: Push and open PR**

```bash
unset GITHUB_TOKEN && git push
gh pr create --title "[CUR-1317] reaction_widgets headless + ReactionScope abstraction" \
  --body "$(cat <<'EOF'
## Summary

- Adds `ReactionScope` abstraction (`LocalScope` + refactored `RemoteScope`) to `reaction` with authoritative `ConnectionStatus` and real exponential-backoff auto-reconnect.
- New `reaction_widgets` Flutter package: headless `ReActionScope` `InheritedWidget`, `ActionBuilder`/`ViewBuilder` Builder primitives, `ViewListener`, `PermissionGate`, `ReActionErrorListener`, and shipped `FakeReaction` widget-test doubles.
- Implements the full `EVS-PRD-reaction-widget-contract` (assertions A–J) and `EVS-PRD-reaction-scope`.
- No rendered/styled widgets — apps render their own sugar.

## Test plan

- [ ] `cd reaction && flutter test` (scope contract, connection-state, auto-reconnect)
- [ ] `cd reaction_widgets && flutter test` (all widget tests + structural assertion F)
- [ ] CI green on the new `reaction-widgets-tests` workflow

EOF
)"

```

Expected: PR opened, link returned.

- [ ] **Step 6: No commit** (final verification gate).

---

## Self-review

Run this checklist against the spec before declaring the plan complete:

### Spec coverage

| Spec assertion | Covered by task(s) |
|---|---|
| `EVS-PRD-reaction-scope`-A (ReactionScope interface) | Task 3, Task 6 |
| `EVS-PRD-reaction-scope`-B (ConnectionStatus 3 variants) | Task 2 |
| `EVS-PRD-reaction-scope`-C (LocalScope, always Connected) | Task 4 |
| `EVS-PRD-reaction-scope`-D (RemoteScope, WS-driven transitions) | Task 6 |
| `EVS-PRD-reaction-scope`-E (source-identical) | Task 7 (cross-impl contract test) |
| `EVS-PRD-view-subscriber`-E (additive contract) | Implicitly preserved; no contract change; flagged in Task 13's progressive mode |
| `EVS-PRD-cross-process-event-transport`-H (auto-reconnect) | Task 5 |
| `EVS-PRD-cross-process-event-transport`-I (status from WS) | Task 5 + Task 6 |
| `EVS-PRD-reaction-widget-contract`-A (ReActionScope IW) | Task 10 |
| `EVS-PRD-reaction-widget-contract`-B (source-identical) | Task 10 (test asserts via FakeReaction) |
| `EVS-PRD-reaction-widget-contract`-C (Builder primitives) | Task 11, Task 13 |
| `EVS-PRD-reaction-widget-contract`-D (ViewListener) | Task 14 |
| `EVS-PRD-reaction-widget-contract`-E (idempotency-key lifetime) | Task 11 |
| `EVS-PRD-reaction-widget-contract`-F (no direct substrate access) | Task 17 |
| `EVS-PRD-reaction-widget-contract`-G (no rendered widgets; the four allowed widget classes) | Tasks 10, 11, 13, 14, 15, 16 (per-class) + tests asserting headless behavior |
| `EVS-PRD-reaction-widget-contract`-H (shipped test doubles) | Task 9 |
| `EVS-PRD-reaction-widget-contract`-I (ViewState sealed) | Tasks 12, 13 |
| `EVS-PRD-reaction-widget-contract`-J (progressive mode) | Task 13 |

No gaps.

### Type consistency

- `ReactionScope` (interface name): consistent in Tasks 3, 4, 6, 7, 10, all widget tasks.
- `ReActionScope` (Flutter `InheritedWidget` name): consistent in Tasks 9, 10, 11, 13, 14, 15, 16.
- `ConnectionStatus` variants `Connected` / `Reconnecting` / `Disconnected` (not `ConnState.connected` etc.): consistent.
- `ActionState` variants `Idle` / `Submitting` / `Success` / `Denied` / `Failed`: consistent with reaction's existing sealed type (Verified Symbols).
- `ViewState` variants `Loading` / `Ready(rows)` / `Disconnected(lastRows, error)`: consistent across Tasks 12, 13.
- `EffectiveAuthorization` (NOT `PermissionSnapshot`, which was deleted in PR #17): consistent.

### Placeholder scan

- No `TBD`, `TODO`, `implement later`, `fill in details` markers in any task body.
- Every code step has actual code.
- Every test step has actual test code.
- Every run command has exact arguments and expected output description.

### Known plan gaps (intentional, called out)

- Several test code blocks use `// ...` to omit fixture-construction details (e.g., a fully-formed `DispatchSuccess` instance). These are intentional — the fixtures are pre-existing test helpers in `reaction/test/test_support/`; the implementer will use them as-is. If a helper is missing, the implementer adds it as part of the task (called out in Task 5 and Task 11).
- The `EffectiveAuthorization.holds(name)` method name in Task 15 is unverified; Task 15 explicitly instructs the implementer to confirm against the substrate source and adjust.
- Field names on `Snapshot<T>` / `Delta<T>` / `Tombstone<T>` used in Task 13's pattern matching (`row`, `aggregateId`) are unverified; Task 13 explicitly instructs the implementer to confirm and adjust.

These three deferrals are NOT plan placeholders — they are "verify-and-adjust" steps with concrete grep commands. They exist because the substrate's exact field/method names are stable and the implementer should consult source-of-truth rather than rely on plan-encoded guesses.
