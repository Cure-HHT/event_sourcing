# Reaction Local Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `reaction` package's pure-Dart in-process foundation: 5 abstract interfaces, 2 state types, 4 Local implementations wrapping the existing `event_sourcing` substrate. After this plan ships, pure-Dart and Flutter consumers can submit actions, subscribe to views, and read permissions through transport-agnostic interfaces against an in-process `EventStore` — usable immediately by the mobile diary (Use 1).

**Architecture:** New sibling package at repo root (`reaction/`), pure Dart, depends on `event_sourcing` via path-dep. Five abstract interfaces (`AuthSession`, `ActionSubmitter`, `ViewSource`, `PermissionSource`, `PrincipalAuthValidator`) define the substrate-agnostic seam. Local impls wrap existing substrate APIs (`ActionDispatcher.dispatch`, `EventStore.subscribe<T>`, `RoleMatrixReader`/`PermissionSnapshot`). Two value types in `state/` (`ActionState` sealed type for widget-side submission lifecycle; `IdempotencyKeyGenerator` for auto-generated UUIDs).

**Tech Stack:** Dart 3.x (sealed classes, exhaustive switch). `package:event_sourcing` (path-dep). `package:uuid` for v4 idempotency keys. Tests use `flutter_test` (matches existing `event_sourcing` test pattern — Sembast-backed integration tests need Flutter test binding). No HTTP/WS dependencies in this plan (those land in Plan B-remote).

**Spec reference:** `spec/prd-reaction.md` — primarily the PRDs `EVS-PRD-auth-session`, `EVS-PRD-action-submitter`, `EVS-PRD-view-subscriber`, `EVS-PRD-permission-source`. Cross-system context in the same file's overview sections.

**Scope check:** This plan covers the **client-side in-process pieces only**. Wire codecs and Remote impls are deferred to Plan B-remote. The pure-Dart shelf server is deferred to Plan C. The Flutter widget layer is deferred to Plan D.

---

## Common Preamble

The following applies to **every task** in this plan. Implementers should read this section once at the start of their task and then refer back as needed.

### Working directory

`/home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing`

All commands assume you `cd` to the worktree root unless otherwise specified.

### Branch

`CUR-1317-libify-event-sourcing` (already checked out)

### Commit message style

`[CUR-1317] <subject>` with a body describing what + why. Reference `spec/prd-reaction.md` PRD IDs (`EVS-PRD-<name>`) where applicable.

### Pre-commit gotcha (IMPORTANT — read every task)

The repo has unstaged working-tree changes that you must NOT modify or commit:

- `.elspais.toml`
- `docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md`
- `spec/requirements-spec.md`
- `.elspais/` (untracked dir)

If pre-commit fails on `spec/requirements-spec.md:28` (pre-existing markdownlint MD040 surfaced by the stash/restore mechanic):

1. `git stash push -m "WIP" -- .elspais.toml docs/superpowers/specs/2026-05-09-substrate-and-materializer-design.md spec/requirements-spec.md`
2. Make your commit
3. `git stash pop`

DO NOT use `--no-verify`. DO NOT modify the user's WIP files.

### Discipline

- **TDD:** write failing test first, then minimal implementation. Each task lists the test cases up front.
- **Greenfield mode:** CUR-1317 is pre-shipping. No backwards-compat shims. Breaking changes to internal exhaustive switches are EXPECTED and DESIRED.
- **Domain neutrality:** `reaction` MUST be domain-neutral. No diary, portal, or any application-specific types in the package. (See CLAUDE.md "Architectural commitments / Domain-neutral lib".)
- **Frequent commits:** one commit per task. Each commit is a green-test checkpoint.

### Test framework

`flutter_test` (matches the existing `event_sourcing` test pattern; needed because `event_sourcing`'s `EventStore` depends on Sembast which requires the Flutter test binding).

---

## File Structure

The full structure this plan creates:

```text
reaction/                                       NEW package, pure Dart
  pubspec.yaml
  analysis_options.yaml                         (re-uses lints from event_sourcing pattern)
  README.md                                     (short package overview)
  lib/
    reaction.dart                               (barrel; public API)
    src/
      interfaces/
        auth_session.dart                       (AuthSession interface + AuthStatus sealed type)
        action_submitter.dart                   (ActionSubmitter interface)
        view_source.dart                        (ViewSource interface)
        permission_source.dart                  (PermissionSource interface)
        principal_auth_validator.dart           (PrincipalAuthValidator interface + AuthenticationDenied exception)
      state/
        action_state.dart                       (ActionState sealed: Idle/Submitting/Success/Denied/Failed)
        idempotency_key_generator.dart          (UUID v4 generator interface + Uuid4IdempotencyKeyGenerator default impl)
      local/
        local_auth_session.dart                 (in-memory Principal holder)
        local_action_submitter.dart             (wraps ActionDispatcher.dispatch)
        local_view_source.dart                  (wraps EventStore.subscribe<T>)
        local_permission_source.dart            (wraps RoleMatrixReader + PermissionSnapshot)
  test/
    interfaces/
      auth_status_test.dart                     (sealed-type tests for AuthStatus)
      principal_auth_validator_test.dart        (interface contract test using a stub)
    state/
      action_state_test.dart                    (sealed-type tests for ActionState)
      idempotency_key_generator_test.dart       (UUID v4 format, uniqueness)
    local/
      local_auth_session_test.dart
      local_action_submitter_test.dart
      local_view_source_test.dart
      local_permission_source_test.dart
      test_support/                             (in-memory EventStore harness; see Task 9)
        reaction_test_harness.dart
```

The interfaces themselves are abstract and have no unit tests beyond the sealed-type structural tests already noted — they're verified by their Local impl tests in `test/local/`.

---

## Task 1: Scaffold the `reaction` package

**Files:**

- Create: `reaction/pubspec.yaml`
- Create: `reaction/analysis_options.yaml`
- Create: `reaction/README.md`
- Create: `reaction/lib/reaction.dart` (empty barrel for now)
- Create: `reaction/.gitignore`

- [ ] **Step 1: Create `reaction/pubspec.yaml`**

```yaml
name: reaction
description: >
  Substrate-agnostic action submission, view subscription, permission
  snapshots, and credential lifecycle for apps built on event_sourcing.
  Pure Dart. The Flutter widget library reaction_widgets sits on top.
version: 0.1.0-dev
publish_to: none
environment:
  sdk: ^3.5.0

dependencies:
  event_sourcing:
    path: ../event_sourcing
  uuid: ^4.5.1

dev_dependencies:
  flutter_test:
    sdk: flutter
  sembast: ^3.7.1
```

(Why `flutter_test` in dev_dependencies for a pure-Dart package: `event_sourcing`'s `EventStore` requires the Flutter test binding for sembast. Our integration tests need that. The package itself does NOT import Flutter at runtime.)

- [ ] **Step 2: Create `reaction/analysis_options.yaml`**

Re-use the same analysis options as `event_sourcing`:

```yaml
include: package:flutter_lints/flutter.yaml

analyzer:
  language:
    strict-casts: true
    strict-inference: true
    strict-raw-types: true
  errors:
    todo: ignore
```

(If `flutter_lints` is not available pure-Dart-side, fall back to `include: package:lints/recommended.yaml`. Check `event_sourcing/analysis_options.yaml` for the pattern this repo actually uses, and mirror it.)

- [ ] **Step 3: Create `reaction/README.md`**

```markdown
# reaction

Substrate-agnostic action submission, view subscription, permission
snapshots, and credential lifecycle for apps built on `event_sourcing`.

Pure Dart. The Flutter widget library `reaction_widgets` sits on top.

See `spec/prd-reaction.md` (in the parent repo) for the architectural
spec.

## Status

Pre-shipping. Local in-process impls only (this package's scope per
Plan B-local). Wire codecs + Remote impls land in Plan B-remote.

## Layout

- `lib/src/interfaces/` — the 5 abstract interfaces (transport-agnostic).
- `lib/src/state/` — `ActionState` sealed type + idempotency-key generator.
- `lib/src/local/` — in-process implementations wrapping `event_sourcing`'s
  `ActionDispatcher`, `EventStore.subscribe<T>`, and permission machinery.
```

- [ ] **Step 4: Create empty `reaction/lib/reaction.dart`**

```dart
/// Substrate-agnostic action submission, view subscription, permission
/// snapshots, and credential lifecycle for apps built on `event_sourcing`.
///
/// See `spec/prd-reaction.md` (in the parent repo) for the architectural
/// spec.
///
/// This barrel will grow `export` directives as the public API is built
/// out across subsequent tasks.
library;
```

- [ ] **Step 5: Create `reaction/.gitignore`**

```text
.dart_tool/
build/
pubspec.lock
```

- [ ] **Step 6: Verify the package resolves**

```bash
cd reaction && dart pub get
```

Expected: no errors. May emit a warning that no exports/imports yet exist; that's fine.

- [ ] **Step 7: Verify analyzer is clean on the scaffolded files**

```bash
cd reaction && dart analyze
```

Expected: "No issues found!" or only the existing repo-wide info-level lints.

- [ ] **Step 8: Commit**

```bash
git add reaction/
git commit -m "[CUR-1317] Scaffold reaction package (skeleton + pubspec + README)

New pure-Dart sibling at repo root, path-deps event_sourcing. Empty
barrel + analysis_options + README. Subsequent tasks fill in
interfaces, state types, and Local impls per Plan B-local.

Refs: spec/prd-reaction.md."
```

---

## Task 2: `AuthSession` interface + `AuthStatus` sealed type

**Files:**

- Create: `reaction/lib/src/interfaces/auth_session.dart`
- Create: `reaction/test/interfaces/auth_status_test.dart`

**Spec ref:** `spec/prd-reaction.md` § `EVS-PRD-auth-session`.

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/interfaces/auth_status_test.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/auth_session.dart';

void main() {
  group('AuthStatus', () {
    test('Authenticated carries a Principal', () {
      const p = Principal(id: 'user-1', roles: {});
      const status = Authenticated(principal: p);
      expect(status.principal, equals(p));
      expect(status, isA<AuthStatus>());
    });

    test('NotAuthenticated is a const value', () {
      const a = NotAuthenticated();
      const b = NotAuthenticated();
      expect(identical(a, b), isTrue);
      expect(a, isA<AuthStatus>());
    });

    test('Expired is a const value', () {
      const a = Expired();
      const b = Expired();
      expect(identical(a, b), isTrue);
      expect(a, isA<AuthStatus>());
    });

    test('exhaustive switch across the three variants', () {
      AuthStatus status = const NotAuthenticated();
      final tag = switch (status) {
        Authenticated() => 'authd',
        NotAuthenticated() => 'unauth',
        Expired() => 'expired',
      };
      expect(tag, equals('unauth'));
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/interfaces/auth_status_test.dart
```

Expected: import errors / "Authenticated isn't defined".

- [ ] **Step 3: Create `reaction/lib/src/interfaces/auth_session.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Auth status surfaced by [AuthSession]. Sealed; consumers can switch
/// exhaustively to drive UI (e.g., show login screen on
/// [NotAuthenticated] or [Expired], show app on [Authenticated]).
sealed class AuthStatus {
  const AuthStatus();
}

/// Current credential is valid; [principal] is the validated identity
/// the substrate's `ActionDispatcher`, `PermissionSource`, and audit log
/// will use.
class Authenticated extends AuthStatus {
  final Principal principal;
  const Authenticated({required this.principal});
}

/// No credential is set, or the credential was rejected and never
/// transitioned to [Authenticated]. App should route to login.
class NotAuthenticated extends AuthStatus {
  const NotAuthenticated();
}

/// A previously-valid credential has expired (server returned 401, WS
/// returned an auth-rejected close frame, or local clock detected
/// `exp` passing). App should route to re-auth without discarding
/// app-side state.
class Expired extends AuthStatus {
  const Expired();
}

/// Holds the application's identity credential and exposes the
/// authenticated [Principal] for downstream consumers.
///
/// Implementations:
/// - [LocalAuthSession] (in-process): holds a [Principal] directly; no
///   credential lifecycle (mobile-install case).
/// - [RemoteAuthSession] (cross-process): holds a bearer-token string;
///   transitions to [Expired] on auth failures from the wire.
///
/// The active [principal] flows into all other reaction interfaces
/// ([ActionSubmitter] submissions, [PermissionSource] scope) via the
/// `ReActionScope` InheritedWidget (in `reaction_widgets`) or
/// directly when consumers wire components manually.
abstract interface class AuthSession {
  /// Current status.
  AuthStatus get current;

  /// Emits whenever [current] changes (credential set/cleared/expired).
  Stream<AuthStatus> get stream;

  /// Set the credential (after login or token refresh). Pass `null` to
  /// clear (logout). Local impls treat the string as a `Principal.id`
  /// directly; Remote impls treat it as an opaque bearer token.
  void setCredential(String? credential);

  /// Convenience: the current authenticated Principal, or `null` if
  /// not currently [Authenticated].
  Principal? get principal;

  /// Release any underlying resources (e.g., the status stream's
  /// internal controller). After [dispose], the AuthSession is no
  /// longer usable.
  Future<void> dispose();
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/interfaces/auth_status_test.dart
```

Expected: 4/4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/interfaces/auth_session.dart reaction/test/interfaces/auth_status_test.dart
git commit -m "[CUR-1317] AuthSession interface + AuthStatus sealed type

Defines the credential-lifecycle abstraction the rest of the package
threads through. Sealed AuthStatus has 3 variants (Authenticated /
NotAuthenticated / Expired). Local/Remote impls follow in later tasks.

Refs: spec/prd-reaction.md (EVS-PRD-auth-session)."
```

---

## Task 3: `ActionSubmitter` interface

**Files:**

- Create: `reaction/lib/src/interfaces/action_submitter.dart`
- (No standalone test file — verified by `LocalActionSubmitter` tests in Task 10.)

**Spec ref:** `spec/prd-reaction.md` § `EVS-PRD-action-submitter`.

- [ ] **Step 1: Create `reaction/lib/src/interfaces/action_submitter.dart`**

```dart
import 'package:event_sourcing/event_sourcing.dart';

/// Submits an [ActionSubmission] for dispatch through the substrate's
/// parse → validate → authorize → execute → record pipeline and returns
/// the [DispatchResult].
///
/// Two impls ship with `reaction`:
///
/// - [LocalActionSubmitter] (in-process): delegates to
///   `ActionDispatcher.dispatch`. Returns the resulting [DispatchResult]
///   directly.
/// - [RemoteActionSubmitter] (cross-process; Plan B-remote): HTTP POST
///   to `/actions/{actionType}`; deserializes [DispatchResult] from the
///   response.
///
/// Idempotency key generation is the WIDGET-layer's concern (handled by
/// `ActionBuilder` in `reaction_widgets`). The submitter itself is a
/// pass-through: whatever key the caller put in
/// [ActionSubmission.idempotencyKey] is what the dispatcher sees.
abstract interface class ActionSubmitter {
  /// Submit an action. Returns when the dispatch pipeline has
  /// completed (Success with emitted events, or one of the denial
  /// variants).
  ///
  /// May throw a [TransportException] subclass on transport-level
  /// failures (network down for Remote; argument programming errors
  /// for both). Application-level denials surface as [DispatchResult]
  /// denial variants, NOT as thrown exceptions.
  Future<DispatchResult> submit(ActionSubmission submission);
}

/// Transport-level failures (network unavailable, malformed wire
/// response, etc.). Not used for dispatch-level denials — those flow
/// through [DispatchResult]'s denial variants.
class TransportException implements Exception {
  final String message;
  final Object? cause;
  const TransportException(this.message, {this.cause});

  @override
  String toString() =>
      cause == null ? 'TransportException: $message' : 'TransportException: $message (cause: $cause)';
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd reaction && dart analyze lib/src/interfaces/action_submitter.dart
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/src/interfaces/action_submitter.dart
git commit -m "[CUR-1317] ActionSubmitter interface + TransportException

Transport-agnostic seam over ActionDispatcher.dispatch. The Future
returns the substrate's DispatchResult directly; transport-level
failures (network etc.) surface as a separate TransportException.
Application-level denials remain in DispatchResult variants.

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter)."
```

---

## Task 4: `ViewSource` interface

**Files:**

- Create: `reaction/lib/src/interfaces/view_source.dart`
- (No standalone test file — verified by `LocalViewSource` tests in Task 11.)

**Spec ref:** `spec/prd-reaction.md` § `EVS-PRD-view-subscriber`.

- [ ] **Step 1: Create `reaction/lib/src/interfaces/view_source.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Subscribes to row-level updates for a registered `ProjectionSpec`'s
/// materialized view. Mirrors the substrate's `EventStore.subscribe<T>`
/// API exactly for `AggregateMode<T>`-style subscriptions.
///
/// The returned stream delivers:
///
/// 1. `Snapshot<T>` × N — one per row currently in the view.
/// 2. `EndOfReplay<T>` — exactly one marker; subscriber knows the
///    snapshot is complete and may transition UI from skeleton/loading
///    to live.
/// 3. `Delta<T>` / `Tombstone<T>` × ∞ — live updates as events land.
///
/// Two impls ship with `reaction`:
///
/// - [LocalViewSource] (in-process): delegates to
///   `eventStore.subscribe<T>(filter, AggregateMode(viewName, mapper,
///   aggregates))`.
/// - [RemoteViewSource] (cross-process; Plan B-remote): opens a WS
///   subscription with `(subscriptionId, viewName, filter, aggregates)`;
///   deserializes `Update<Map<String, Object?>>` envelopes and applies
///   the consumer's [mapper] client-side.
abstract interface class ViewSource {
  /// Watch a view's row-level updates.
  ///
  /// - [viewName]: matches a registered `ProjectionSpec.viewName`.
  /// - [mapper]: applied to each row's `Map<String, Object?>` to
  ///   produce typed values.
  /// - [filter]: optional `SubscriptionFilter` on entry/event/aggregate
  ///   types (matches the substrate's filter semantics).
  /// - [aggregates]: optional allow-list of aggregate IDs to scope
  ///   delivery (substrate's `AggregateMode.aggregates`).
  ///
  /// The stream uses Dart's standard cancellation semantics — call
  /// `.cancel()` on the resulting subscription to dispose.
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  });
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd reaction && dart analyze lib/src/interfaces/view_source.dart
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/src/interfaces/view_source.dart
git commit -m "[CUR-1317] ViewSource interface

Transport-agnostic seam over EventStore.subscribe<T>'s AggregateMode.
Method signature mirrors the substrate exactly (viewName, mapper,
filter, aggregates) so the LocalViewSource impl is a trivial
delegation.

Refs: spec/prd-reaction.md (EVS-PRD-view-subscriber)."
```

---

## Task 5: `PermissionSource` interface

**Files:**

- Create: `reaction/lib/src/interfaces/permission_source.dart`
- (No standalone test file — verified by `LocalPermissionSource` tests in Task 12.)

**Spec ref:** `spec/prd-reaction.md` § `EVS-PRD-permission-source`.

- [ ] **Step 1: Create `reaction/lib/src/interfaces/permission_source.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Per-Principal view of the substrate's `RolePermissionGrants`
/// projection — i.e., "what is this user allowed to do?" The active
/// Principal is sourced from an [AuthSession] (set externally; not on
/// this interface) and used to scope the snapshot.
///
/// Two impls ship with `reaction`:
///
/// - [LocalPermissionSource] (in-process): wraps existing
///   `RoleMatrixReader` + `PermissionSnapshot` machinery; subscribes
///   to the `RolePermissionGrants` view via local `subscribe<T>`;
///   recomputes the snapshot when relevant rows change.
/// - [RemotePermissionSource] (cross-process; Plan B-remote): initial
///   HTTP GET `/permissions/snapshot?principalId=...`; subsequent
///   updates via the multiplexed WS subscription on the same
///   projection.
abstract interface class PermissionSource {
  /// Current snapshot for the active Principal, or `null` if no
  /// Principal is set or the snapshot hasn't loaded yet.
  PermissionSnapshot? get current;

  /// Emits whenever [current] changes. The first event delivered to a
  /// listener is the current value (snapshot-on-listen), then deltas
  /// as the snapshot updates.
  Stream<PermissionSnapshot?> get stream;

  /// Release any underlying resources (the stream controller, the
  /// substrate-subscription this source uses internally, etc.).
  /// After [dispose], the source is no longer usable.
  Future<void> dispose();
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd reaction && dart analyze lib/src/interfaces/permission_source.dart
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/src/interfaces/permission_source.dart
git commit -m "[CUR-1317] PermissionSource interface

Per-Principal scoped read of the substrate's RolePermissionGrants
projection. Active Principal is read from the wired-in AuthSession
externally (no setPrincipal mutator on this interface).

Refs: spec/prd-reaction.md (EVS-PRD-permission-source)."
```

---

## Task 6: `PrincipalAuthValidator` interface + `AuthenticationDenied`

**Files:**

- Create: `reaction/lib/src/interfaces/principal_auth_validator.dart`
- Create: `reaction/test/interfaces/principal_auth_validator_test.dart`

**Spec ref:** `spec/prd-reaction.md` § `EVS-PRD-auth-session` (the validator seam is described there).

This interface is server-side (consumed by Plan C's shelf server), but its declaration lives here in the interfaces module so Plan C can depend on `reaction` for it.

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/interfaces/principal_auth_validator_test.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';

class _StubValidator implements PrincipalAuthValidator {
  final Map<String, Principal> _accepts;
  _StubValidator(this._accepts);

  @override
  Future<Principal> authenticate(String credential) async {
    final p = _accepts[credential];
    if (p == null) throw const AuthenticationDenied('unknown credential');
    return p;
  }
}

void main() {
  group('PrincipalAuthValidator contract', () {
    test('accepts a known credential and returns the Principal', () async {
      final validator = _StubValidator({
        'token-a': const Principal(id: 'user-a', roles: {}),
      });
      final p = await validator.authenticate('token-a');
      expect(p.id, equals('user-a'));
    });

    test('throws AuthenticationDenied on unknown credential', () async {
      final validator = _StubValidator(const {});
      expect(
        () => validator.authenticate('bogus'),
        throwsA(isA<AuthenticationDenied>()),
      );
    });

    test('AuthenticationDenied carries a reason message', () {
      const e = AuthenticationDenied('jwt expired at 2025-01-01');
      expect(e.message, contains('jwt expired'));
      expect(e.toString(), contains('jwt expired'));
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/interfaces/principal_auth_validator_test.dart
```

Expected: import failure / "PrincipalAuthValidator isn't defined".

- [ ] **Step 3: Create `reaction/lib/src/interfaces/principal_auth_validator.dart`**

```dart
import 'package:event_sourcing/event_sourcing.dart';

/// Validates an opaque credential string from the wire (HTTP header
/// `X-Principal-Auth-Credential` or WS handshake `auth` field) and
/// returns the authenticated [Principal]. Implementations decide what
/// shape the credential takes (JWT, opaque session token, dev
/// shortcut, etc.) — `reaction` is agnostic.
///
/// Throws [AuthenticationDenied] on rejection. Server-side handlers
/// translate this to a wire-level rejection (HTTP 401, WS auth-rejected
/// close frame).
///
/// Reference impls shipped with `reaction`:
///
/// - `TrustingAuthValidator` — dev/test only; accepts any non-empty
///   credential verbatim as `Principal.id`. Loud "DO NOT USE IN
///   PRODUCTION" docstring.
/// - Optional `JwtAuthValidator` — verifies a JWT against a configured
///   public key/issuer. (Deferred to Plan C or B-remote.)
///
/// This interface lives in the client-side `reaction` package because
/// it is shared with the server-side `reaction` server module (Plan C);
/// dependency direction is one-way (server depends on reaction;
/// reaction does not depend on the server).
abstract interface class PrincipalAuthValidator {
  /// Validate [credential] and return the authenticated [Principal].
  /// Throws [AuthenticationDenied] on rejection.
  Future<Principal> authenticate(String credential);
}

/// Thrown by [PrincipalAuthValidator.authenticate] when a credential
/// is rejected. The [message] is for server-side logging only — do not
/// surface it raw to clients, as it may leak validator internals.
class AuthenticationDenied implements Exception {
  final String message;
  const AuthenticationDenied(this.message);

  @override
  String toString() => 'AuthenticationDenied: $message';
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/interfaces/principal_auth_validator_test.dart
```

Expected: 3/3 pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/interfaces/principal_auth_validator.dart reaction/test/interfaces/principal_auth_validator_test.dart
git commit -m "[CUR-1317] PrincipalAuthValidator interface + AuthenticationDenied

Server-side credential-validation seam. Lives in the client package so
Plan C's reaction server module can depend on it without circular
deps. AuthenticationDenied is the rejection signal; reference impls
(TrustingAuthValidator, JwtAuthValidator) land in Plan C/B-remote.

Refs: spec/prd-reaction.md (EVS-PRD-auth-session)."
```

---

## Task 7: `ActionState` sealed type

**Files:**

- Create: `reaction/lib/src/state/action_state.dart`
- Create: `reaction/test/state/action_state_test.dart`

`ActionState` is the widget-side submission state machine: `Idle → Submitting → (Success | Denied | Failed)`. Used by `ActionBuilder` in `reaction_widgets`. Lives in `reaction` because it's pure data + transitions; no Flutter dependency.

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/state/action_state_test.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/state/action_state.dart';

void main() {
  group('ActionState', () {
    test('Idle is a const value', () {
      const a = ActionState.idle();
      const b = ActionState.idle();
      expect(identical(a, b), isTrue);
      expect(a, isA<ActionState>());
      expect(a, isA<Idle>());
    });

    test('Submitting is a const value', () {
      const a = ActionState.submitting();
      const b = ActionState.submitting();
      expect(identical(a, b), isTrue);
      expect(a, isA<Submitting>());
    });

    test('Success carries the DispatchResult', () {
      final result = DispatchResult.success(
        invocationId: 'inv-1',
        events: const [],
      );
      final s = ActionState.success(result);
      expect(s, isA<Success>());
      expect(s.result.invocationId, equals('inv-1'));
    });

    test('Denied carries the denial reason', () {
      const d = ActionState.denied('action not allowed for role');
      expect(d, isA<Denied>());
      expect(d.reason, contains('not allowed'));
    });

    test('Failed carries the error and stack', () {
      final err = StateError('boom');
      final f = ActionState.failed(err, StackTrace.current);
      expect(f, isA<Failed>());
      expect(f.error, equals(err));
      expect(f.stackTrace, isNotNull);
    });

    test('exhaustive switch across all 5 variants', () {
      ActionState state = const Idle();
      final tag = switch (state) {
        Idle() => 'idle',
        Submitting() => 'submitting',
        Success() => 'success',
        Denied() => 'denied',
        Failed() => 'failed',
      };
      expect(tag, equals('idle'));
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/state/action_state_test.dart
```

Expected: "Idle isn't defined" / similar.

- [ ] **Step 3: Create `reaction/lib/src/state/action_state.dart`**

```dart
import 'package:event_sourcing/event_sourcing.dart';

/// Widget-side submission state machine. Used by `ActionBuilder` in
/// `reaction_widgets` to drive a button's idle/loading/result/error UI.
///
/// Lifecycle:
///
/// ```
/// Idle ──submit()──> Submitting ──┬─> Success(DispatchResult)
///                                  ├─> Denied(reason)
///                                  └─> Failed(error, stackTrace)
/// ```
///
/// After any terminal state, the widget can return to Idle (e.g., on
/// next button press) at the widget's discretion.
sealed class ActionState {
  const ActionState();

  /// Convenience constructors mirroring the sealed variants. Lets
  /// downstream code write `ActionState.idle()` without picking which
  /// subclass to import.
  const factory ActionState.idle() = Idle;
  const factory ActionState.submitting() = Submitting;
  const factory ActionState.success(DispatchResult result) = Success;
  const factory ActionState.denied(String reason) = Denied;
  factory ActionState.failed(Object error, StackTrace stackTrace) = Failed;
}

/// The submission has not been initiated, or has been reset after a
/// terminal state. Button is enabled (subject to permission gating).
class Idle extends ActionState {
  const Idle();
}

/// The submission is in flight. Button is disabled; loading indicator
/// shown.
class Submitting extends ActionState {
  const Submitting();
}

/// The submission completed successfully and the substrate emitted
/// events.
class Success extends ActionState {
  final DispatchResult result;
  const Success(this.result);
}

/// The submission was denied by the dispatcher (parse failure,
/// validation failure, authorization denied, execution failed, or
/// idempotency conflict). [reason] is a human-readable summary for
/// the UI; full denial details are available via the [DispatchResult]
/// the consumer passed to [ActionBuilder] (when applicable).
class Denied extends ActionState {
  final String reason;
  const Denied(this.reason);
}

/// The submission failed with an unexpected error (transport,
/// programming error, etc.). Distinct from [Denied] (which is a
/// pipeline outcome) — [Failed] is a thrown exception.
class Failed extends ActionState {
  final Object error;
  final StackTrace stackTrace;
  Failed(this.error, this.stackTrace);
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/state/action_state_test.dart
```

Expected: 6/6 pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/state/action_state.dart reaction/test/state/action_state_test.dart
git commit -m "[CUR-1317] ActionState sealed type for widget submission state machine

5 variants (Idle/Submitting/Success/Denied/Failed) covering the
ActionBuilder lifecycle. Pure data; no Flutter dependency. Used by
reaction_widgets (Plan D) to drive button rendering.

Refs: spec/prd-reaction.md."
```

---

## Task 8: Idempotency-key generator

**Files:**

- Create: `reaction/lib/src/state/idempotency_key_generator.dart`
- Create: `reaction/test/state/idempotency_key_generator_test.dart`

Generates UUID v4 idempotency keys for action submissions. Defined as an interface (so consumers can inject test doubles) with a default `Uuid4` impl.

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/state/idempotency_key_generator_test.dart`:

```dart
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/state/idempotency_key_generator.dart';

void main() {
  group('Uuid4IdempotencyKeyGenerator', () {
    test('produces a UUID v4 format string', () {
      final gen = Uuid4IdempotencyKeyGenerator();
      final key = gen.generate();
      // UUID v4: 8-4-4-4-12 hex, with version nibble 4 and variant
      // nibble 8/9/a/b.
      final pattern = RegExp(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
      );
      expect(pattern.hasMatch(key), isTrue, reason: 'got: $key');
    });

    test('successive calls produce distinct keys', () {
      final gen = Uuid4IdempotencyKeyGenerator();
      final keys = <String>{
        for (var i = 0; i < 100; i++) gen.generate(),
      };
      expect(keys.length, equals(100));
    });
  });

  group('IdempotencyKeyGenerator (interface)', () {
    test('can be replaced with a deterministic stub for tests', () {
      final stub = _StubIdempotencyKeyGenerator(
        ['key-1', 'key-2', 'key-3'],
      );
      expect(stub.generate(), equals('key-1'));
      expect(stub.generate(), equals('key-2'));
      expect(stub.generate(), equals('key-3'));
    });
  });
}

class _StubIdempotencyKeyGenerator implements IdempotencyKeyGenerator {
  final List<String> _keys;
  int _i = 0;
  _StubIdempotencyKeyGenerator(this._keys);

  @override
  String generate() => _keys[_i++];
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/state/idempotency_key_generator_test.dart
```

Expected: "Uuid4IdempotencyKeyGenerator isn't defined".

- [ ] **Step 3: Create `reaction/lib/src/state/idempotency_key_generator.dart`**

```dart
import 'package:uuid/uuid.dart';

/// Generates idempotency keys for action submissions. Defaults to UUID
/// v4 ([Uuid4IdempotencyKeyGenerator]).
///
/// `ActionBuilder` in `reaction_widgets` (Plan D) caches a generated
/// key for the lifetime of an in-flight submission and resets on
/// terminal state — so retries during `Submitting` reuse the same key
/// (idempotent dedupe), but a new press after Success/Denied/Failed
/// gets a fresh key (new logical action).
///
/// Defining this as an interface lets tests inject a deterministic
/// stub for reproducible assertions.
abstract interface class IdempotencyKeyGenerator {
  String generate();
}

/// Default impl: generates random UUID v4 strings using the `uuid`
/// package.
class Uuid4IdempotencyKeyGenerator implements IdempotencyKeyGenerator {
  final Uuid _uuid;

  Uuid4IdempotencyKeyGenerator() : _uuid = const Uuid();

  /// Injection point for tests that want a deterministic UUID source.
  @visibleForTesting
  Uuid4IdempotencyKeyGenerator.withUuid(this._uuid);

  @override
  String generate() => _uuid.v4();
}
```

Note: `@visibleForTesting` requires importing `package:meta/meta.dart`. Add the import.

```dart
import 'package:meta/meta.dart';
import 'package:uuid/uuid.dart';
```

`meta` is a transitive dep of `flutter_test` and `event_sourcing` already; it's available.

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/state/idempotency_key_generator_test.dart
```

Expected: 3/3 pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/state/idempotency_key_generator.dart reaction/test/state/idempotency_key_generator_test.dart
git commit -m "[CUR-1317] IdempotencyKeyGenerator interface + Uuid4 default impl

UUID v4 keys for action submissions. Test stub pattern documented;
the @visibleForTesting alternate constructor lets tests inject a
deterministic Uuid source.

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter idempotency
policy)."
```

---

## Task 9: Test harness for `Local*` integration tests

**Files:**

- Create: `reaction/test/local/test_support/reaction_test_harness.dart`

The 4 Local impl tasks (10–13) each need an in-memory `EventStore` + `ActionDispatcher` wired up. Existing helper in `event_sourcing/test/actions/test_support/event_store_helper.dart` is close but not directly importable (it's inside `event_sourcing`'s test/ tree). Create a `reaction`-local equivalent that bootstraps the substrate cleanly for our tests.

- [ ] **Step 1: Inspect existing patterns**

Read both reference files to understand the bootstrap shape:

- `event_sourcing/test/actions/test_support/event_store_helper.dart` — the actions-flow harness with `ActionDispatcher` wired up.
- `event_sourcing/test/subscriptions/aggregate_mode_test.dart` (the `_open()` helper at the top) — view-subscription-focused harness.

Both bootstrap an in-memory sembast backend, register `EntryTypeDefinition`s, register `ProjectionSpec`s, and call `EventStore.open(...)`.

- [ ] **Step 2: Create the harness**

Create `reaction/test/local/test_support/reaction_test_harness.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:sembast/sembast_memory.dart';

/// A bundle of substrate components wired up against an in-memory
/// sembast backend, suitable for reaction's Local* impl tests.
///
/// Includes:
/// - One [EntryTypeDefinition] for `note` events
/// - One [AggregateProjectionSpec] for `notes_today` view
/// - The substrate's `RolePermissionGrants` projection (registered so
///   permission tests have something to read)
/// - An [ActionDispatcher] with a `say_hello` test action registered
/// - One pre-seeded `view_user` role with `read_notes` permission
class ReactionTestHarness {
  final EventStore eventStore;
  final ActionDispatcher dispatcher;
  final RoleMatrixReader roleMatrixReader;

  ReactionTestHarness._({
    required this.eventStore,
    required this.dispatcher,
    required this.roleMatrixReader,
  });

  static Future<ReactionTestHarness> open() async {
    final db = await newDatabaseFactoryMemory().openDatabase(
      'reaction-${DateTime.now().microsecondsSinceEpoch}.db',
    );
    final backend = SembastBackend(database: db);

    final entryTypes = EntryTypeRegistry();
    for (final defn in kSystemEntryTypes) {
      entryTypes.register(defn);
    }
    entryTypes
      ..register(
        const EntryTypeDefinition(
          id: 'note',
          registeredVersion: 1,
          name: 'Note',
        ),
      )
      ..register(
        const EntryTypeDefinition(
          id: 'greeting',
          registeredVersion: 1,
          name: 'Greeting',
          materialize: false,
        ),
      );

    final projections = ProjectionRegistry()
      ..register(rolePermissionGrantsSpec)
      ..register(
        AggregateProjectionSpec(
          viewName: 'notes_today',
          interest: const SubscriptionFilter(aggregateTypes: {'note'}),
          tombstoneEventTypes: const {'tombstone'},
        ),
      );

    final securityContexts = SembastSecurityContextStore(backend: backend);

    final store = await EventStore.open(
      storage: backend,
      entryTypes: entryTypes,
      source: const Source(
        hopId: 'reaction-test',
        identifier: 'reaction-test-instance',
        softwareVersion: '0.0.0-test',
      ),
      securityContexts: securityContexts,
      projections: projections,
      promoters: PromoterRegistry(),
    );

    // Wire up an ActionDispatcher with a single test action.
    final actionRegistry = ActionRegistry()
      ..register(_SayHelloAction());

    final roleMatrixReader = MaterializedViewRoleMatrixReader(
      eventStore: store,
    );

    final policy = TableBackedAuthorizationPolicy(
      roleMatrixReader: roleMatrixReader,
    );

    final dispatcher = ActionDispatcher(
      events: store,
      actions: actionRegistry,
      authorizationPolicy: policy,
      idempotencyStore: InMemoryIdempotencyStore(),
    );

    return ReactionTestHarness._(
      eventStore: store,
      dispatcher: dispatcher,
      roleMatrixReader: roleMatrixReader,
    );
  }

  Future<void> close() async {
    await eventStore.close();
  }

  /// Convenience: append a `note` event to the substrate, materializing
  /// into the `notes_today` view.
  Future<StoredEvent> appendNote({
    required String aggregateId,
    required Map<String, Object?> payload,
  }) {
    return eventStore.append(
      entryType: 'note',
      aggregateId: aggregateId,
      aggregateType: 'note',
      eventType: 'note_added',
      data: payload,
      initiator: const Initiator(
        principalId: 'test-user',
        hopId: 'reaction-test',
      ),
    );
  }
}

/// A trivial test action used by LocalActionSubmitter tests. Emits one
/// `greeting` event with `{name: <name>}` payload. Requires permission
/// `say_hello` on the dispatching Principal.
class _SayHelloAction extends Action<_HelloSubmission, _HelloResult> {
  @override
  String get name => 'say_hello';

  @override
  Permission get permission => const Permission(
        name: 'say_hello',
        scope: ScopeClass.global(),
      );

  @override
  IdempotencyPolicy get idempotency => IdempotencyPolicy.optional;

  @override
  _HelloSubmission parse(Map<String, Object?> json) {
    final name = json['name'];
    if (name is! String) {
      throw ParseException('missing or non-string "name"');
    }
    return _HelloSubmission(name: name);
  }

  @override
  void validate(_HelloSubmission submission) {
    if (submission.name.isEmpty) {
      throw ValidationException('name must be non-empty');
    }
  }

  @override
  Future<ExecutionResult<_HelloResult>> execute(
    _HelloSubmission submission,
    ActionContext ctx,
  ) async {
    return ExecutionResult.success(
      result: _HelloResult(),
      events: [
        EventDraft(
          entryType: 'greeting',
          aggregateId: 'greetings',
          aggregateType: 'greeting',
          eventType: 'hello_said',
          data: {'name': submission.name},
        ),
      ],
    );
  }
}

class _HelloSubmission {
  final String name;
  _HelloSubmission({required this.name});
}

class _HelloResult {}
```

(The exact `Action` class shape and dispatcher constructor depend on the substrate's current API. Re-read `event_sourcing/lib/src/actions/action.dart`, `action_registry.dart`, `action_dispatcher.dart`, and the existing `action_dispatcher_test.dart` to confirm signatures. Adjust the constructor calls and method names to match.)

- [ ] **Step 3: Smoke-verify the harness compiles**

```bash
cd reaction && dart analyze test/local/test_support/reaction_test_harness.dart
```

Expected: no errors. There are no tests for the harness itself; it's exercised by Tasks 10-13.

- [ ] **Step 4: Commit**

```bash
git add reaction/test/local/test_support/reaction_test_harness.dart
git commit -m "[CUR-1317] Test harness for reaction's Local* impl tests

In-memory EventStore + ActionDispatcher + RoleMatrixReader bundle;
mirrors event_sourcing/test/actions/test_support patterns. Includes
one note entry type, one notes_today AggregateProjectionSpec, the
RolePermissionGrants projection, and a SayHelloAction for dispatcher
tests. Each Local impl test in Tasks 10-13 calls
ReactionTestHarness.open() in setUp() and harness.close() in tearDown()."
```

---

## Task 10: `LocalActionSubmitter`

**Files:**

- Create: `reaction/lib/src/local/local_action_submitter.dart`
- Create: `reaction/test/local/local_action_submitter_test.dart`

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/local/local_action_submitter_test.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/local/local_action_submitter.dart';

import 'test_support/reaction_test_harness.dart';

void main() {
  group('LocalActionSubmitter', () {
    late ReactionTestHarness harness;
    late LocalActionSubmitter submitter;

    setUp(() async {
      harness = await ReactionTestHarness.open();
      submitter = LocalActionSubmitter(dispatcher: harness.dispatcher);
      // Seed: grant 'say_hello' to a 'greeter' role and assign
      // principal 'alice' to that role.
      await _grantHelloRole(harness);
    });

    tearDown(() async {
      await harness.close();
    });

    test('successful submission returns Success', () async {
      final result = await submitter.submit(
        ActionSubmission(
          actionType: 'say_hello',
          payload: const {'name': 'World'},
          principalId: 'alice',
          idempotencyKey: 'key-1',
        ),
      );
      expect(result, isA<Success>());
      expect((result as Success).events, hasLength(1));
      expect(result.events.first.eventType, equals('hello_said'));
    });

    test('parse failure returns ParseFailed denial', () async {
      final result = await submitter.submit(
        ActionSubmission(
          actionType: 'say_hello',
          payload: const {}, // missing 'name'
          principalId: 'alice',
          idempotencyKey: 'key-parse',
        ),
      );
      expect(result, isA<ParseFailed>());
    });

    test('authorization denial returns AuthorizationDenied', () async {
      final result = await submitter.submit(
        ActionSubmission(
          actionType: 'say_hello',
          payload: const {'name': 'World'},
          principalId: 'bob', // not granted 'say_hello'
          idempotencyKey: 'key-deny',
        ),
      );
      expect(result, isA<AuthorizationDenied>());
    });

    test('idempotent replay returns cached result', () async {
      final r1 = await submitter.submit(
        ActionSubmission(
          actionType: 'say_hello',
          payload: const {'name': 'Once'},
          principalId: 'alice',
          idempotencyKey: 'key-idem',
        ),
      );
      final r2 = await submitter.submit(
        ActionSubmission(
          actionType: 'say_hello',
          payload: const {'name': 'Once'},
          principalId: 'alice',
          idempotencyKey: 'key-idem',
        ),
      );
      expect(r1, isA<Success>());
      expect(r2, isA<Success>());
      // Same invocationId means dispatcher returned the cached
      // outcome (didn't re-execute).
      expect(
        (r1 as Success).invocationId,
        equals((r2 as Success).invocationId),
      );
    });
  });
}

/// Seeds the harness with: role 'greeter' has 'say_hello'; principal
/// 'alice' is assigned to 'greeter'. Mechanism: append the
/// permission_granted + role_assigned events directly via the
/// EventStore (bypasses dispatcher — these are bootstrap events).
Future<void> _grantHelloRole(ReactionTestHarness harness) async {
  // The exact shape of permission_granted / role_assigned events
  // is in event_sourcing/lib/src/permissions/permission_granted_payload.dart
  // and friends. Re-use that contract here.
  // Pseudo-code (adapt to actual API):
  //
  // await harness.eventStore.append(
  //   entryType: kPermissionGrantedEntryType,
  //   ...,
  //   data: PermissionGrantedPayload(
  //     roleId: 'greeter',
  //     permission: 'say_hello',
  //     scope: const ScopeClass.global(),
  //   ).toJson(),
  // );
  //
  // await harness.eventStore.append(
  //   entryType: kRoleAssignedEntryType,
  //   ...,
  //   data: { 'principalId': 'alice', 'roleId': 'greeter' }.
  // );
  throw UnimplementedError(
    'Wire to the substrate permission-seeding API; see '
    'event_sourcing/lib/src/permissions/permission_seed.dart and '
    'event_seed_applier.dart for the existing pattern.',
  );
}
```

**Note for the implementer:** the `_grantHelloRole` helper is a stub. Before running the tests, replace it with real calls — likely using the substrate's `PermissionSeed` + `EventSeedApplier` (see `event_sourcing/lib/src/permissions/`). The pattern is the same as `event_sourcing/example_action_permissions/lib/server/bootstrap.dart` calls `bootstrapActionPermissions(...)`.

- [ ] **Step 2: Run the test — expected red (compilation error + UnimplementedError)**

```bash
cd reaction && flutter test test/local/local_action_submitter_test.dart
```

Expected: fails with import errors initially; after implementing the production file (Step 4), fails on `UnimplementedError` from the stub helper.

- [ ] **Step 3: Wire the `_grantHelloRole` helper**

Replace the stubbed body with the real permission-seeding. Read:

- `event_sourcing/lib/src/permissions/permission_seed.dart`
- `event_sourcing/lib/src/permissions/event_seed_applier.dart`
- `event_sourcing/lib/src/permissions/bootstrap_action_permissions.dart`
- `event_sourcing/example_action_permissions/lib/server/bootstrap.dart` (line ~118)

The wire shape is: build a `PermissionSeed` listing roles and their permissions, run it through `EventSeedApplier` against the harness's `EventStore`. Match the existing pattern exactly.

- [ ] **Step 4: Create `reaction/lib/src/local/local_action_submitter.dart`**

```dart
import 'package:event_sourcing/event_sourcing.dart';

import '../interfaces/action_submitter.dart';

/// In-process [ActionSubmitter] impl: delegates directly to the
/// substrate's [ActionDispatcher]. No transport involved.
class LocalActionSubmitter implements ActionSubmitter {
  final ActionDispatcher dispatcher;

  const LocalActionSubmitter({required this.dispatcher});

  @override
  Future<DispatchResult> submit(ActionSubmission submission) =>
      dispatcher.dispatch(submission);
}
```

- [ ] **Step 5: Run the test — expected green**

```bash
cd reaction && flutter test test/local/local_action_submitter_test.dart
```

Expected: 4/4 pass.

- [ ] **Step 6: Commit**

```bash
git add reaction/lib/src/local/local_action_submitter.dart reaction/test/local/local_action_submitter_test.dart
git commit -m "[CUR-1317] LocalActionSubmitter

Trivial in-process delegation to ActionDispatcher.dispatch. Tests
cover the 4 main DispatchResult outcomes: success, parse failure,
authorization denial, idempotency cache hit. Uses harness +
PermissionSeed wiring per the existing event_sourcing/
example_action_permissions pattern.

Refs: spec/prd-reaction.md (EVS-PRD-action-submitter)."
```

---

## Task 11: `LocalViewSource`

**Files:**

- Create: `reaction/lib/src/local/local_view_source.dart`
- Create: `reaction/test/local/local_view_source_test.dart`

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/local/local_view_source_test.dart`:

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/local/local_view_source.dart';

import 'test_support/reaction_test_harness.dart';

void main() {
  group('LocalViewSource.watch', () {
    late ReactionTestHarness harness;
    late LocalViewSource source;

    setUp(() async {
      harness = await ReactionTestHarness.open();
      source = LocalViewSource(eventStore: harness.eventStore);
    });

    tearDown(() async {
      await harness.close();
    });

    test('empty view emits exactly one EndOfReplay then live deltas', () async {
      final updates = <Update<Map<String, Object?>>>[];
      final endOfReplay = Completer<EndOfReplay<Map<String, Object?>>>();
      final sub = source.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      ).listen((u) {
        updates.add(u);
        if (u is EndOfReplay<Map<String, Object?>> && !endOfReplay.isCompleted) {
          endOfReplay.complete(u);
        }
      });

      await endOfReplay.future.timeout(const Duration(seconds: 5));
      expect(updates.length, equals(1));
      expect(updates.single, isA<EndOfReplay<Map<String, Object?>>>());
      expect(updates.single.sequence, equals(0));

      // Now append a note and verify a Delta arrives.
      await harness.appendNote(
        aggregateId: 'n1',
        payload: const {'body': 'hello'},
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(updates.any((u) => u is Delta<Map<String, Object?>>), isTrue);

      await sub.cancel();
    });

    test('mapper is applied to each row', () async {
      await harness.appendNote(
        aggregateId: 'n1',
        payload: const {'body': 'first'},
      );

      final endOfReplay = Completer<void>();
      final rows = <String>[];
      final sub = source.watch<String>(
        viewName: 'notes_today',
        mapper: (m) => m['body'] as String,
      ).listen((u) {
        if (u is Snapshot<String>) rows.add(u.value!);
        if (u is EndOfReplay<String>) endOfReplay.complete();
      });

      await endOfReplay.future.timeout(const Duration(seconds: 5));
      expect(rows, contains('first'));

      await sub.cancel();
    });

    test('filter is honored — events outside the filter do not emit', () async {
      // The notes_today view's projection spec filters by aggregateType:
      // 'note' — anything else would never appear in rows anyway. To
      // test the watch-time `filter` arg, set a very narrow filter on
      // entryTypes that excludes 'note'.
      final updates = <Update<Map<String, Object?>>>[];
      final endOfReplay = Completer<void>();
      final sub = source.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
        filter: const SubscriptionFilter(
          entryTypes: <String>['greeting'], // excludes 'note'
        ),
      ).listen((u) {
        updates.add(u);
        if (u is EndOfReplay<Map<String, Object?>>) endOfReplay.complete();
      });

      await harness.appendNote(
        aggregateId: 'n1',
        payload: const {'body': 'filtered out'},
      );

      await endOfReplay.future.timeout(const Duration(seconds: 5));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      // Should see only EndOfReplay; no Delta for the 'note' append
      // because the filter excludes it.
      final deltas = updates.whereType<Delta<Map<String, Object?>>>();
      expect(deltas, isEmpty);

      await sub.cancel();
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red (compilation error)**

```bash
cd reaction && flutter test test/local/local_view_source_test.dart
```

Expected: "LocalViewSource isn't defined".

- [ ] **Step 3: Create `reaction/lib/src/local/local_view_source.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import '../interfaces/view_source.dart';

/// In-process [ViewSource] impl: delegates to
/// `EventStore.subscribe<T>(filter, AggregateMode(viewName, mapper,
/// aggregates))`. No transport involved.
class LocalViewSource implements ViewSource {
  final EventStore eventStore;

  const LocalViewSource({required this.eventStore});

  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    return eventStore.subscribe<T>(
      filter ?? const SubscriptionFilter(),
      AggregateMode<T>(
        viewName: viewName,
        mapper: mapper,
        aggregates: aggregates,
      ),
    );
  }
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/local/local_view_source_test.dart
```

Expected: 3/3 pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/local/local_view_source.dart reaction/test/local/local_view_source_test.dart
git commit -m "[CUR-1317] LocalViewSource

Wraps EventStore.subscribe<T>'s AggregateMode. Tests cover empty
view (EndOfReplay only), mapper application, and SubscriptionFilter
respect.

Refs: spec/prd-reaction.md (EVS-PRD-view-subscriber)."
```

---

## Task 12: `LocalPermissionSource`

**Files:**

- Create: `reaction/lib/src/local/local_permission_source.dart`
- Create: `reaction/test/local/local_permission_source_test.dart`

The `LocalPermissionSource` reads permission state per-Principal from the substrate. Mechanism:

1. On construction, subscribe to the `RolePermissionGrants` view via `EventStore.subscribe<T>(AggregateMode(viewName: 'role_permission_grants', ...))`.
2. Maintain a current `PermissionSnapshot?` recomputed whenever the active Principal changes OR the underlying view changes.
3. Active Principal is supplied by the caller via a `setActivePrincipal(p)` method — typically wired to an `AuthSession.principal`.

This task introduces `setActivePrincipal` on the Local impl (NOT on the interface — the interface stays clean per spec). Concrete consumers (e.g., `ReActionScope` in Plan D) wire `LocalPermissionSource.setActivePrincipal(authSession.principal)` whenever auth changes.

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/local/local_permission_source_test.dart`:

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/local/local_permission_source.dart';

import 'test_support/reaction_test_harness.dart';

void main() {
  group('LocalPermissionSource', () {
    late ReactionTestHarness harness;
    late LocalPermissionSource source;

    setUp(() async {
      harness = await ReactionTestHarness.open();
      // Seed: 'greeter' role has 'say_hello'; 'alice' is greeter.
      await _grantHelloRoleToAlice(harness);

      source = LocalPermissionSource(
        eventStore: harness.eventStore,
        roleMatrixReader: harness.roleMatrixReader,
      );
    });

    tearDown(() async {
      await source.dispose();
      await harness.close();
    });

    test('current is null before setActivePrincipal', () {
      expect(source.current, isNull);
    });

    test(
      'after setActivePrincipal, current reflects the principal\'s permissions',
      () async {
        source.setActivePrincipal(const Principal(id: 'alice', roles: {}));
        // Allow the async snapshot fetch to settle.
        await Future<void>.delayed(const Duration(milliseconds: 50));

        final snapshot = source.current;
        expect(snapshot, isNotNull);
        expect(snapshot!.allows(const Permission(
          name: 'say_hello',
          scope: ScopeClass.global(),
        )), isTrue);
      },
    );

    test('stream emits when active principal changes', () async {
      final events = <PermissionSnapshot?>[];
      final sub = source.stream.listen(events.add);

      source.setActivePrincipal(const Principal(id: 'alice', roles: {}));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      source.setActivePrincipal(const Principal(id: 'bob', roles: {}));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      source.setActivePrincipal(null);
      await Future<void>.delayed(const Duration(milliseconds: 50));

      // Expect at least 3 events (one per setActivePrincipal call).
      expect(events.length, greaterThanOrEqualTo(3));

      // alice has hello; bob does not; null clears.
      expect(events[0]?.allows(const Permission(
        name: 'say_hello',
        scope: ScopeClass.global(),
      )), isTrue);
      expect(events[1]?.allows(const Permission(
        name: 'say_hello',
        scope: ScopeClass.global(),
      )), isFalse);
      expect(events[2], isNull);

      await sub.cancel();
    });

    // Optional follow-on test: stream emits when underlying grants change
    // (e.g., a new permission_granted event lands while alice is the
    // active principal). Implementer can add if time permits.
  });
}

Future<void> _grantHelloRoleToAlice(ReactionTestHarness harness) async {
  // Wire to the substrate's permission-seeding API (same as Task 10).
  throw UnimplementedError(
    'Wire to substrate permission-seeding; reuse Task 10\'s helper.',
  );
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/local/local_permission_source_test.dart
```

Expected: compilation error then UnimplementedError.

- [ ] **Step 3: Wire `_grantHelloRoleToAlice` (reuse Task 10's pattern)**

Replace the stub with the same `PermissionSeed`/`EventSeedApplier` call as in Task 10.

- [ ] **Step 4: Create `reaction/lib/src/local/local_permission_source.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import '../interfaces/permission_source.dart';

/// In-process [PermissionSource] impl. Recomputes a [PermissionSnapshot]
/// for the active [Principal] whenever:
///
/// 1. [setActivePrincipal] changes the active principal, OR
/// 2. The substrate's `role_permission_grants` view emits an update
///    (permission/role membership changed for any principal).
///
/// `current` is null until [setActivePrincipal] is called (or after
/// being cleared with `setActivePrincipal(null)`).
class LocalPermissionSource implements PermissionSource {
  final EventStore eventStore;
  final RoleMatrixReader roleMatrixReader;

  Principal? _principal;
  PermissionSnapshot? _current;
  final StreamController<PermissionSnapshot?> _controller =
      StreamController<PermissionSnapshot?>.broadcast();
  StreamSubscription<Update<Map<String, Object?>>>? _grantsSub;

  LocalPermissionSource({
    required this.eventStore,
    required this.roleMatrixReader,
  }) {
    _grantsSub = eventStore
        .subscribe<Map<String, Object?>>(
      const SubscriptionFilter(),
      AggregateMode<Map<String, Object?>>(
        viewName: 'role_permission_grants',
        mapper: (m) => m,
      ),
    )
        .listen((update) async {
      // Any change to grants → recompute for the active principal.
      if (update is Delta || update is Tombstone) {
        await _recompute();
      }
    });
  }

  @override
  PermissionSnapshot? get current => _current;

  @override
  Stream<PermissionSnapshot?> get stream => _controller.stream;

  /// Sets the active principal. Triggers a snapshot recompute. Pass
  /// null to clear.
  void setActivePrincipal(Principal? principal) {
    _principal = principal;
    if (principal == null) {
      _current = null;
      if (!_controller.isClosed) _controller.add(null);
      return;
    }
    // Fire-and-forget recompute (Stream subscribers will see the emit
    // after the async snapshot read completes).
    unawaited(_recompute());
  }

  Future<void> _recompute() async {
    final p = _principal;
    if (p == null) {
      _current = null;
      if (!_controller.isClosed) _controller.add(null);
      return;
    }
    final snapshot = await roleMatrixReader.snapshotFor(p);
    _current = snapshot;
    if (!_controller.isClosed) _controller.add(snapshot);
  }

  @override
  Future<void> dispose() async {
    await _grantsSub?.cancel();
    if (!_controller.isClosed) await _controller.close();
  }
}
```

**Note for the implementer:** the exact method on `RoleMatrixReader` is `snapshotFor(Principal)` in this plan's terms. Verify against the actual substrate API in `event_sourcing/lib/src/permissions/role_matrix_reader.dart` and adapt the call. If the actual method is named differently (e.g., `read(principalId)`), rename in the impl.

- [ ] **Step 5: Run the test — expected green**

```bash
cd reaction && flutter test test/local/local_permission_source_test.dart
```

Expected: 3/3 pass.

- [ ] **Step 6: Commit**

```bash
git add reaction/lib/src/local/local_permission_source.dart reaction/test/local/local_permission_source_test.dart
git commit -m "[CUR-1317] LocalPermissionSource

In-process per-Principal scoped read of the RolePermissionGrants
view. setActivePrincipal mutator (concrete-impl only, not on the
interface) is wired externally by ReActionScope (Plan D) to follow
AuthSession.principal. Recomputes the snapshot on grants-view
deltas.

Refs: spec/prd-reaction.md (EVS-PRD-permission-source)."
```

---

## Task 13: `LocalAuthSession`

**Files:**

- Create: `reaction/lib/src/local/local_auth_session.dart`
- Create: `reaction/test/local/local_auth_session_test.dart`

In-process `AuthSession`: holds a `Principal` directly. The `setCredential(String?)` call treats the string as a `Principal.id` (no validation; the mobile-install case has no JWT). Status flips between `Authenticated` and `NotAuthenticated`; never goes to `Expired` (no credential expiration in-process).

- [ ] **Step 1: Write the failing tests**

Create `reaction/test/local/local_auth_session_test.dart`:

```dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/local/local_auth_session.dart';

void main() {
  group('LocalAuthSession', () {
    late LocalAuthSession session;

    setUp(() {
      session = LocalAuthSession();
    });

    tearDown(() async {
      await session.dispose();
    });

    test('starts NotAuthenticated', () {
      expect(session.current, isA<NotAuthenticated>());
      expect(session.principal, isNull);
    });

    test('setCredential(non-null) transitions to Authenticated', () {
      session.setCredential('install-uuid-123');
      expect(session.current, isA<Authenticated>());
      expect(session.principal?.id, equals('install-uuid-123'));
    });

    test('setCredential(null) transitions back to NotAuthenticated', () {
      session.setCredential('install-uuid-123');
      session.setCredential(null);
      expect(session.current, isA<NotAuthenticated>());
      expect(session.principal, isNull);
    });

    test('stream emits on every status change', () async {
      final events = <AuthStatus>[];
      final sub = session.stream.listen(events.add);

      session.setCredential('a');
      session.setCredential('b');
      session.setCredential(null);

      // Allow microtasks to drain.
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      expect(events.length, equals(3));
      expect(events[0], isA<Authenticated>());
      expect((events[0] as Authenticated).principal.id, equals('a'));
      expect((events[1] as Authenticated).principal.id, equals('b'));
      expect(events[2], isA<NotAuthenticated>());

      await sub.cancel();
    });
  });
}
```

- [ ] **Step 2: Run the test — expected red**

```bash
cd reaction && flutter test test/local/local_auth_session_test.dart
```

Expected: compilation failure.

- [ ] **Step 3: Create `reaction/lib/src/local/local_auth_session.dart`**

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import '../interfaces/auth_session.dart';

/// In-process [AuthSession]. The credential string is treated as a
/// `Principal.id` directly (no JWT validation; mobile-install case).
/// Status flips between [Authenticated] and [NotAuthenticated]; never
/// [Expired] (no credential expiration concept in-process).
class LocalAuthSession implements AuthSession {
  AuthStatus _current = const NotAuthenticated();
  final StreamController<AuthStatus> _controller =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _controller.stream;

  @override
  Principal? get principal {
    final c = _current;
    return c is Authenticated ? c.principal : null;
  }

  @override
  void setCredential(String? credential) {
    final newStatus = credential == null
        ? const NotAuthenticated()
        : Authenticated(
            principal: Principal(id: credential, roles: const {}),
          );
    _current = newStatus;
    if (!_controller.isClosed) _controller.add(newStatus);
  }

  @override
  Future<void> dispose() async {
    if (!_controller.isClosed) await _controller.close();
  }
}
```

- [ ] **Step 4: Run the test — expected green**

```bash
cd reaction && flutter test test/local/local_auth_session_test.dart
```

Expected: 4/4 pass.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/local/local_auth_session.dart reaction/test/local/local_auth_session_test.dart
git commit -m "[CUR-1317] LocalAuthSession

In-process AuthSession: credential string → Principal.id directly.
Status flips between Authenticated/NotAuthenticated; no Expired
(mobile-install case has no credential lifetime). Stream emits per
setCredential call.

Refs: spec/prd-reaction.md (EVS-PRD-auth-session)."
```

---

## Task 14: Update the public barrel + verify full suite

**Files:**

- Modify: `reaction/lib/reaction.dart`

Now that interfaces + state + local impls are implemented, the barrel needs to export the public API.

- [ ] **Step 1: Replace `reaction/lib/reaction.dart` with**

```dart
/// Substrate-agnostic action submission, view subscription, permission
/// snapshots, and credential lifecycle for apps built on `event_sourcing`.
///
/// See `spec/prd-reaction.md` (in the parent repo) for the architectural
/// spec.
///
/// ## What this package provides (Plan B-local — in-process only)
///
/// Five transport-agnostic interfaces:
///
/// - [AuthSession] — credential lifecycle; surfaces [AuthStatus]
///   (sealed: [Authenticated], [NotAuthenticated], [Expired]).
/// - [ActionSubmitter] — submit actions to the substrate's dispatch
///   pipeline.
/// - [ViewSource] — subscribe to a registered view's row-level updates
///   (the substrate's `Update<T>` stream: Snapshot × N → EndOfReplay →
///   Delta/Tombstone × ∞).
/// - [PermissionSource] — per-Principal view of the substrate's
///   `RolePermissionGrants` projection.
/// - [PrincipalAuthValidator] — server-side credential validation seam
///   (consumed by Plan C's reaction server module).
///
/// Two state types:
///
/// - [ActionState] — sealed widget-side submission state machine
///   (Idle/Submitting/Success/Denied/Failed). Used by `ActionBuilder`
///   in `reaction_widgets`.
/// - [IdempotencyKeyGenerator] — UUID v4 by default
///   ([Uuid4IdempotencyKeyGenerator]).
///
/// Four in-process Local implementations:
///
/// - [LocalAuthSession] — holds a [Principal] directly.
/// - [LocalActionSubmitter] — wraps `ActionDispatcher.dispatch`.
/// - [LocalViewSource] — wraps `EventStore.subscribe<T>`.
/// - [LocalPermissionSource] — wraps the `RoleMatrixReader` +
///   `PermissionSnapshot` machinery.
///
/// Remote impls + wire protocol land in Plan B-remote; the pure-Dart
/// shelf server lands in Plan C; Flutter widgets land in Plan D.
library;

// Interfaces
export 'src/interfaces/action_submitter.dart'
    show ActionSubmitter, TransportException;
export 'src/interfaces/auth_session.dart'
    show Authenticated, AuthSession, AuthStatus, Expired, NotAuthenticated;
export 'src/interfaces/permission_source.dart' show PermissionSource;
export 'src/interfaces/principal_auth_validator.dart'
    show AuthenticationDenied, PrincipalAuthValidator;
export 'src/interfaces/view_source.dart' show ViewSource;

// State
export 'src/state/action_state.dart'
    show ActionState, Denied, Failed, Idle, Submitting, Success;
export 'src/state/idempotency_key_generator.dart'
    show IdempotencyKeyGenerator, Uuid4IdempotencyKeyGenerator;

// Local impls
export 'src/local/local_action_submitter.dart' show LocalActionSubmitter;
export 'src/local/local_auth_session.dart' show LocalAuthSession;
export 'src/local/local_permission_source.dart' show LocalPermissionSource;
export 'src/local/local_view_source.dart' show LocalViewSource;
```

- [ ] **Step 2: Verify the package's external API resolves**

Create a smoke-test file that imports ONLY the barrel and exercises a tiny bit of each export:

```bash
cd reaction && dart analyze lib/reaction.dart
```

Expected: no errors.

- [ ] **Step 3: Run the full `reaction` test suite**

```bash
cd reaction && flutter test
```

Expected: all tests pass (Tasks 2, 6, 7, 8, 10, 11, 12, 13 each produced tests; total should be ~20+ tests).

- [ ] **Step 4: Run analyzer on the whole `reaction` package**

```bash
cd reaction && dart analyze
```

Expected: clean (info-level lints only).

- [ ] **Step 5: Run `event_sourcing` tests too (defensive — make sure path-dep didn't break anything)**

```bash
cd event_sourcing && flutter test
```

Expected: 801/801 still pass.

- [ ] **Step 6: Commit**

```bash
git add reaction/lib/reaction.dart
git commit -m "[CUR-1317] Public barrel for reaction package

Exports the 5 interfaces, 2 state types, and 4 Local impls authored
in Tasks 2-13. Consumers import 'package:reaction/reaction.dart'.
Remote impls + wire protocol land in Plan B-remote; this barrel
will grow then.

Refs: spec/prd-reaction.md."
```

---

## Self-Review Checklist

Run BEFORE declaring the plan complete:

- [ ] Each of the 5 spec PRDs (`EVS-PRD-auth-session`, `EVS-PRD-action-submitter`, `EVS-PRD-view-subscriber`, `EVS-PRD-permission-source`, `EVS-PRD-cross-process-event-transport`) has a task covering the in-process scope. The cross-process PRD is split across plans; this plan owns the in-process side.
- [ ] No `TODO`/`TBD`/`fill in` placeholders. Verify: `grep -n 'TBD\|TODO\|fill in' docs/superpowers/plans/2026-05-12-reaction-local-core.md`. (Self-review-checklist references are NOT placeholders; they're documentation of the verification.)
- [ ] Identifier consistency across tasks:
  - `LocalAuthSession`, `LocalActionSubmitter`, `LocalViewSource`, `LocalPermissionSource` — spelled identically everywhere.
  - `Authenticated` / `NotAuthenticated` / `Expired` — sealed AuthStatus.
  - `Idle` / `Submitting` / `Success` / `Denied` / `Failed` — sealed ActionState.
  - `ActionSubmission`, `DispatchResult`, `Principal`, `PermissionSnapshot`, `Update<T>` — all from `event_sourcing`, used consistently.
- [ ] Test fixture (`ReactionTestHarness` in Task 9) is referenced consistently by Tasks 10-13 and not over-specified beyond what those tasks need.
- [ ] All commit messages start with `[CUR-1317]` and reference `spec/prd-reaction.md` PRD IDs where applicable.
- [ ] The barrel (Task 14) exports exactly the public surface; no `src/` internals leak.

---

## Pausing between phases

If execution gets interrupted, the natural resume points are at task boundaries. Each task's commit is a green-test checkpoint. To resume:

1. `git log --oneline` to see which tasks committed
2. Find the next task in this plan
3. Pick up TDD red-green-commit from Step 1 of that task

The plan does not require all tasks be executed in one session.
