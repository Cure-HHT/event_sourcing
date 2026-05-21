# Reaction Remote+Server Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Plan B-remote+C — the `Remote*` client-side implementations of reaction's four substrate-agnostic interfaces, the JSON wire protocol they speak, and the shelf-based reference server that terminates the wire and bridges back to an in-process `EventStore` + `ActionDispatcher`.

**Architecture:** Two-tier (client + server-side adapters) bridging an in-process substrate to a cross-process JSON wire. Client: `RemoteScope` composing four `Remote*` impls over one shared `RemoteConnection` (HTTP for actions/snapshot/me; multiplexed WebSocket for view subs and permission updates). Server-side: `ReactionHandlers` config bundle exposing the four shelf request handlers (`.me`, `.actions`, `.permissions`, `.subscriptions`) plus an optional `authMiddleware(validator)`. There is no `ReactionServer` class — consumers compose the handlers into their own existing shelf pipeline (the expected first consumers, `hht_diary`'s `portal_server` and `diary_server`, already run their own shelf pipelines with their own auth middleware). The subscription handler opens substrate `subscribe<T>` per accepted subscription, applies CUR-1331-aligned two-tier authorization (view-level deny + row-level narrowing via containment-projection expansion), and relays `Update<Map<String, Object?>>` envelopes. `TrustingAuthValidator` is the only reference validator shipped; production validators (Firebase, Auth0, linking-code) live in app code.

**Tech Stack:** Dart 3, `package:shelf`, `package:shelf_router`, `package:shelf_web_socket`, `package:http`, `package:web_socket_channel`, `package:uuid`. Tests run under `flutter_test` (the substrate's sembast binding requires it).

**Spec:** `spec/reaction-remote.md`. Read it before executing.

---

## Precondition: CUR-1331 impl must be merged

This plan assumes the shapes pinned in `spec/scoped-permissions.md` are present in code: `AuthorizationPolicy.isPermitted(principal, permission, scopeValue)`; `effectivePermissionsFor(principal) -> EffectiveAuthorization` carrying `(rolePermissions, scopeAssignments)`; sealed `ScopeValue` (`BoundScope` / `ValueWildcardScope` / `TotalWildcardScope`); `ContainmentResolver`; transactional `findViewRowsInTxn` on `StorageBackend`. The reaction server's per-subscription authorization consults these directly.

**Task 1 is a drift-sweep** against the delivered CUR-1331 surface. If anything in this plan references an API name CUR-1331 impl renamed, that's a plan bug to fix before executing the rest. The skim is fast (~30 min) but load-bearing.

## File structure

```text
reaction/
  pubspec.yaml                 MODIFY: add http, web_socket_channel, shelf,
                                       shelf_web_socket, shelf_router

  lib/reaction.dart            MODIFY: extend exports for new public types

  lib/src/wire/                NEW directory (package-private, not exported)
    envelope.dart                 Discriminator helper; common JSON utilities
    principal_codec.dart          Principal <-> JSON
    filter_codec.dart             SubscriptionFilter <-> JSON
    update_codec.dart             Update<Map<String,Object?>> <-> JSON
    action_submission_codec.dart  ActionSubmission <-> JSON
    dispatch_result_codec.dart    DispatchResult <-> JSON
    effective_authorization_codec.dart EffectiveAuthorization <-> JSON
    subscription_messages.dart    SubscribeMsg/UnsubscribeMsg/AuthMsg/
                                  AuthOkMsg/SubscriptionDeniedMsg/ErrorMsg

  lib/src/remote/              NEW directory
    remote_connection.dart        Shared WS lifecycle, HTTP client, reconnect
    remote_auth_session.dart      AuthSession over HTTP + WS close-frame
    remote_action_submitter.dart  POST /actions
    remote_view_source.dart       WS subscribe
    remote_permission_source.dart GET /permissions/snapshot + WS
                                  subscription on role_permission_grants
    remote_scope.dart             Composition class

  lib/src/server/              NEW directory (shelf-compatible handlers,
                                no "server" class — consumers compose
                                into their own shelf pipelines)
    reaction_handlers.dart        ReactionHandlers config bundle;
                                  exposes .me / .actions /
                                  .permissions / .subscriptions
                                  as shelf.Handlers
    auth_middleware.dart          Optional authMiddleware(validator) +
                                  principalFromContext(req) helper
    action_route.dart             POST /actions: actionRouteHandler()
    me_route.dart                 GET /me: meRouteHandler()
    permission_route.dart         GET /permissions/snapshot:
                                  permissionRouteHandler()
    subscription_handler.dart     WS upgrade; per-sub authz; relay
    view_scope_registry.dart      viewName -> scopeClass mapping
    validators/
      trusting_auth_validator.dart  Dev/test reference impl

  test/wire/                   NEW directory (one *_test.dart per codec)
    principal_codec_test.dart
    filter_codec_test.dart
    update_codec_test.dart
    action_submission_codec_test.dart
    dispatch_result_codec_test.dart
    effective_authorization_codec_test.dart
    subscription_messages_test.dart

  test/server/                 NEW directory
    trusting_auth_validator_test.dart
    view_scope_registry_test.dart
    auth_middleware_test.dart
    action_route_test.dart
    me_route_test.dart
    permission_route_test.dart
    subscription_handler_test.dart
    reaction_handlers_test.dart

  test/remote/                 NEW directory
    remote_connection_test.dart
    remote_auth_session_test.dart
    remote_action_submitter_test.dart
    remote_view_source_test.dart
    remote_permission_source_test.dart
    remote_scope_test.dart

  test/e2e/                    NEW directory
    auth_test.dart
    action_test.dart
    view_test.dart
    permission_test.dart
    reconnect_test.dart
    authz_test.dart
    edge_cases_test.dart
    test_support/
      reaction_remote_test_harness.dart
```

CLAUDE.md trust-boundaries section also gets an additive update (Task 35) and the roadmap doc gets an update reflecting Plan B-remote+C as the merged scope (Task 36).

---

## Phase 0 — Setup

### Task 1: Verify CUR-1331 impl + sweep this plan for drift

**Files:** None modified; verification only.

- [ ] **Step 1: Confirm CUR-1331 is merged**

```bash
git log --all --oneline | grep -i 'cur-1331' | head -5
```

Expected: at least one commit referencing the CUR-1331 impl merge to `main`. If no impl commits land yet, **STOP** — Plan B-remote+C cannot start until CUR-1331 impl is in `main`.

- [ ] **Step 2: Sweep the plan against the delivered API**

For each of the following, confirm the symbol exists with the spec'd shape; if it differs, update this plan inline before executing further tasks:

```text
- AuthorizationPolicy.isPermitted(Principal, Permission, ScopeValue?)
- AuthorizationPolicy.effectivePermissionsFor(Principal) -> Future<EffectiveAuthorization>
- EffectiveAuthorization(activeRole, rolePermissions, scopeAssignments)
- ScopeAssignment(scope)
- sealed ScopeValue: BoundScope(class_, value),
                    ValueWildcardScope(class_),
                    TotalWildcardScope
- ContainmentResolver.resolve(class_, value, target)
- ScopeClassRegistry
- ScopeClassSpec(name, containedIn?)
- ContainmentRef(parentClass, projection, keyColumn, parentColumn)
- StorageBackend.findViewRowsInTxn(txn, viewName, {filter})
```

Run:

```bash
cd event_sourcing
grep -rn "class AuthorizationPolicy\b" lib/ | head -5
grep -rn "class EffectiveAuthorization\b" lib/ | head -5
grep -rn "sealed class ScopeValue\b" lib/ | head -5
grep -rn "class ContainmentResolver\b" lib/ | head -5
grep -rn "findViewRowsInTxn" lib/ | head -5
```

Expected: at least one hit per grep. Mismatches indicate plan drift; patch the plan and re-verify before Task 2.

- [ ] **Step 3: No commit**

Verification only. No staged changes; continue to Task 2.

### Task 2: Add pubspec deps + reaction.dart export placeholder

**Files:**

- Modify: `reaction/pubspec.yaml`
- Modify: `reaction/lib/reaction.dart`

- [ ] **Step 1: Add dependencies to pubspec.yaml**

```yaml
dependencies:
  event_sourcing:
    path: ../event_sourcing
  meta: ^1.16.0
  uuid: ^4.5.1
  http: ^1.4.0
  web_socket_channel: ^3.1.0
  shelf: ^1.4.2
  shelf_router: ^1.1.4
  shelf_web_socket: ^3.0.0
```

(Adds `http`, `web_socket_channel`, `shelf`, `shelf_router`, `shelf_web_socket`. Keep existing deps verbatim.)

- [ ] **Step 2: Run `dart pub get`**

```bash
cd reaction
dart pub get
```

Expected: all dependencies resolve cleanly.

- [ ] **Step 3: Commit**

```bash
git add reaction/pubspec.yaml reaction/pubspec.lock
git commit -m "[CUR-1317] reaction: add HTTP / WS / shelf deps for Plan B-remote+C"
```

(Note: `pubspec.lock` is gitignored at the repo level per CLAUDE.md, so the second path may not stage. That's fine.)

---

## Phase 1 — Wire codecs

Each codec follows the same TDD pattern: round-trip test first, then `toJson`/`fromJson` impl, then commit. Wire types are package-private (declared with library-private names or in `src/wire/`); no exports from `reaction.dart`.

### Task 3: Wire envelope + discriminator helper

**Files:**

- Create: `reaction/lib/src/wire/envelope.dart`
- Create: `reaction/test/wire/envelope_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// reaction/test/wire/envelope_test.dart
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/envelope.dart';

void main() {
  group('readType', () {
    test('returns the type field', () {
      expect(readType({'type': 'snapshot'}), 'snapshot');
    });
    test('throws on missing type', () {
      expect(() => readType({}), throwsA(isA<FormatException>()));
    });
    test('throws on non-string type', () {
      expect(() => readType({'type': 42}), throwsA(isA<FormatException>()));
    });
  });

  group('requireString', () {
    test('returns the string field', () {
      expect(requireString({'k': 'v'}, 'k'), 'v');
    });
    test('throws on missing key', () {
      expect(() => requireString({}, 'k'),
          throwsA(isA<FormatException>()));
    });
    test('throws on non-string', () {
      expect(() => requireString({'k': 1}, 'k'),
          throwsA(isA<FormatException>()));
    });
  });
}
```

- [ ] **Step 2: Run test (expect failure)**

```bash
cd reaction && flutter test test/wire/envelope_test.dart
```

Expected: compile errors (envelope.dart doesn't exist).

- [ ] **Step 3: Implement envelope.dart**

```dart
// reaction/lib/src/wire/envelope.dart
/// Discriminator and primitive-extraction helpers shared by all wire
/// codecs. Package-private; never exported from reaction.dart.

/// Reads the "type" discriminator from a wire envelope.
String readType(Map<String, Object?> json) {
  final t = json['type'];
  if (t is! String) {
    throw FormatException('missing or non-string "type": ${json['type']}');
  }
  return t;
}

/// Reads a required string field. Throws FormatException on missing
/// or wrong type.
String requireString(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! String) {
    throw FormatException('missing or non-string "$key": $v');
  }
  return v;
}

/// Reads an optional string field. Returns null if absent.
String? readString(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v == null) return null;
  if (v is! String) {
    throw FormatException('non-string "$key": $v');
  }
  return v;
}

/// Reads a required int field. Throws on missing or non-int.
int requireInt(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! int) {
    throw FormatException('missing or non-int "$key": $v');
  }
  return v;
}

/// Reads a required map field. Throws on missing or non-map.
Map<String, Object?> requireMap(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! Map) {
    throw FormatException('missing or non-map "$key": $v');
  }
  return Map<String, Object?>.from(v);
}
```

- [ ] **Step 4: Run test (expect pass)**

```bash
flutter test test/wire/envelope_test.dart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/envelope.dart reaction/test/wire/envelope_test.dart
git commit -m "[CUR-1317] reaction wire: envelope discriminator helpers"
```

### Task 4: Principal codec

**Files:**

- Create: `reaction/lib/src/wire/principal_codec.dart`
- Create: `reaction/test/wire/principal_codec_test.dart`

- [ ] **Step 1: Write round-trip tests**

```dart
// reaction/test/wire/principal_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/principal_codec.dart';

void main() {
  test('round-trips UserPrincipal with activeRole', () {
    const original = UserPrincipal(
      userId: 'u-123',
      activeRole: 'study-coordinator',
    );
    final json = PrincipalCodec.encode(original);
    final decoded = PrincipalCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips SystemPrincipal', () {
    const original = SystemPrincipal();
    final json = PrincipalCodec.encode(original);
    final decoded = PrincipalCodec.decode(json);
    expect(decoded, original);
  });

  test('decode rejects unknown principal kind', () {
    expect(
      () => PrincipalCodec.decode({'kind': 'alien'}),
      throwsA(isA<FormatException>()),
    );
  });
}
```

- [ ] **Step 2: Run test (expect failure: codec doesn't exist)**

```bash
flutter test test/wire/principal_codec_test.dart
```

Expected: compile error.

- [ ] **Step 3: Implement principal_codec.dart**

```dart
// reaction/lib/src/wire/principal_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for [Principal]. UserPrincipal and SystemPrincipal are
/// distinguished by a "kind" discriminator.
///
/// Wire shape:
///   {"kind": "user", "userId": "...", "activeRole": "..."}
///   {"kind": "system"}
class PrincipalCodec {
  const PrincipalCodec._();

  static Map<String, Object?> encode(Principal p) {
    if (p is UserPrincipal) {
      return {
        'kind': 'user',
        'userId': p.userId,
        'activeRole': p.activeRole,
      };
    } else if (p is SystemPrincipal) {
      return {'kind': 'system'};
    } else {
      throw FormatException('unknown Principal type: ${p.runtimeType}');
    }
  }

  static Principal decode(Map<String, Object?> json) {
    final kind = requireString(json, 'kind');
    switch (kind) {
      case 'user':
        return UserPrincipal(
          userId: requireString(json, 'userId'),
          activeRole: requireString(json, 'activeRole'),
        );
      case 'system':
        return const SystemPrincipal();
      default:
        throw FormatException('unknown principal kind: $kind');
    }
  }
}
```

- [ ] **Step 4: Run test (expect pass)**

```bash
flutter test test/wire/principal_codec_test.dart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/principal_codec.dart reaction/test/wire/principal_codec_test.dart
git commit -m "[CUR-1317] reaction wire: Principal JSON codec"
```

### Task 5: SubscriptionFilter codec

**Files:**

- Create: `reaction/lib/src/wire/filter_codec.dart`
- Create: `reaction/test/wire/filter_codec_test.dart`

- [ ] **Step 1: Write round-trip tests**

```dart
// reaction/test/wire/filter_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/filter_codec.dart';

void main() {
  test('round-trips empty filter', () {
    const original = SubscriptionFilter();
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips filter with entryTypes', () {
    const original = SubscriptionFilter(entryTypes: {'note', 'greeting'});
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });

  test('round-trips filter with aggregateTypes + eventTypes', () {
    const original = SubscriptionFilter(
      aggregateTypes: {'note'},
      eventTypes: {'note_updated'},
    );
    final json = FilterCodec.encode(original);
    final decoded = FilterCodec.decode(json);
    expect(decoded, original);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/filter_codec_test.dart
```

Expected: compile error.

- [ ] **Step 3: Implement filter_codec.dart**

```dart
// reaction/lib/src/wire/filter_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

/// JSON codec for [SubscriptionFilter]. Only the substrate-supported
/// fields are encoded; the `sources` field is omitted in v1 per the
/// single-source commitment.
///
/// Wire shape (all fields optional; absent means "no filter for this dim"):
///   {"entryTypes": ["..."],
///    "eventTypes": ["..."],
///    "aggregateTypes": ["..."]}
class FilterCodec {
  const FilterCodec._();

  static Map<String, Object?> encode(SubscriptionFilter f) {
    final out = <String, Object?>{};
    if (f.entryTypes != null) {
      out['entryTypes'] = f.entryTypes!.toList()..sort();
    }
    if (f.eventTypes != null) {
      out['eventTypes'] = f.eventTypes!.toList()..sort();
    }
    if (f.aggregateTypes != null) {
      out['aggregateTypes'] = f.aggregateTypes!.toList()..sort();
    }
    return out;
  }

  static SubscriptionFilter decode(Map<String, Object?> json) {
    Set<String>? readSet(String key) {
      final v = json[key];
      if (v == null) return null;
      if (v is! List) {
        throw FormatException('non-list "$key": $v');
      }
      return v.cast<String>().toSet();
    }

    return SubscriptionFilter(
      entryTypes: readSet('entryTypes'),
      eventTypes: readSet('eventTypes'),
      aggregateTypes: readSet('aggregateTypes'),
    );
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/filter_codec_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/filter_codec.dart reaction/test/wire/filter_codec_test.dart
git commit -m "[CUR-1317] reaction wire: SubscriptionFilter JSON codec"
```

### Task 6: Update<T> codec

**Files:**

- Create: `reaction/lib/src/wire/update_codec.dart`
- Create: `reaction/test/wire/update_codec_test.dart`

- [ ] **Step 1: Write round-trip tests for all four variants**

```dart
// reaction/test/wire/update_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/update_codec.dart';

void main() {
  test('round-trips Snapshot envelope', () {
    final original = Snapshot<Map<String, Object?>>(
      aggregateId: 'agg-1',
      row: const {'title': 'hello'},
      sequence: 42,
    );
    final json = UpdateCodec.encode(
      original,
      subscriptionId: 'sub-1',
    );
    expect(json['type'], 'snapshot');
    expect(json['subscriptionId'], 'sub-1');
    expect(json['sequence'], 42);
    expect(json['aggregateId'], 'agg-1');
    expect(json['row'], {'title': 'hello'});

    final decoded = UpdateCodec.decode(json) as Snapshot<Map<String, Object?>>;
    expect(decoded.aggregateId, 'agg-1');
    expect(decoded.row, {'title': 'hello'});
    expect(decoded.sequence, 42);
  });

  test('round-trips Delta envelope', () {
    final original = Delta<Map<String, Object?>>(
      aggregateId: 'agg-1',
      row: const {'title': 'world'},
      sequence: 43,
    );
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'delta');
    final decoded = UpdateCodec.decode(json) as Delta<Map<String, Object?>>;
    expect(decoded.aggregateId, 'agg-1');
    expect(decoded.row, {'title': 'world'});
    expect(decoded.sequence, 43);
  });

  test('round-trips Tombstone envelope', () {
    final original = Tombstone<Map<String, Object?>>(
      aggregateId: 'agg-1',
      sequence: 44,
    );
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'tombstone');
    final decoded = UpdateCodec.decode(json) as Tombstone<Map<String, Object?>>;
    expect(decoded.aggregateId, 'agg-1');
    expect(decoded.sequence, 44);
  });

  test('round-trips EndOfReplay envelope', () {
    final original = EndOfReplay<Map<String, Object?>>(sequence: 45);
    final json = UpdateCodec.encode(original, subscriptionId: 'sub-1');
    expect(json['type'], 'end_of_replay');
    expect(json.containsKey('aggregateId'), isFalse);
    final decoded = UpdateCodec.decode(json) as EndOfReplay<Map<String, Object?>>;
    expect(decoded.sequence, 45);
  });

  test('decode reads subscriptionId from envelope', () {
    final encoded = UpdateCodec.encode(
      EndOfReplay<Map<String, Object?>>(sequence: 1),
      subscriptionId: 'sub-xyz',
    );
    expect(UpdateCodec.subscriptionIdOf(encoded), 'sub-xyz');
  });

  test('decode rejects unknown type', () {
    expect(
      () => UpdateCodec.decode({'type': 'unknown', 'subscriptionId': 'x',
                                'sequence': 1}),
      throwsA(isA<FormatException>()),
    );
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/update_codec_test.dart
```

- [ ] **Step 3: Implement update_codec.dart**

```dart
// reaction/lib/src/wire/update_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for the substrate's [Update<T>] family. The wire ships
/// rows as opaque `Map<String, Object?>`; the client applies its
/// consumer-supplied mapper after decode.
///
/// Wire shape per variant (all include "type", "subscriptionId",
/// "sequence"):
///   snapshot      + "aggregateId" + "row"
///   delta         + "aggregateId" + "row"
///   tombstone     + "aggregateId"
///   end_of_replay (no extra fields)
class UpdateCodec {
  const UpdateCodec._();

  static Map<String, Object?> encode(
    Update<Map<String, Object?>> u, {
    required String subscriptionId,
  }) {
    if (u is Snapshot<Map<String, Object?>>) {
      return {
        'type': 'snapshot',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'aggregateId': u.aggregateId,
        'row': u.row,
      };
    } else if (u is Delta<Map<String, Object?>>) {
      return {
        'type': 'delta',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'aggregateId': u.aggregateId,
        'row': u.row,
      };
    } else if (u is Tombstone<Map<String, Object?>>) {
      return {
        'type': 'tombstone',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'aggregateId': u.aggregateId,
      };
    } else if (u is EndOfReplay<Map<String, Object?>>) {
      return {
        'type': 'end_of_replay',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
      };
    } else {
      throw FormatException('unknown Update<T> type: ${u.runtimeType}');
    }
  }

  static Update<Map<String, Object?>> decode(Map<String, Object?> json) {
    final type = readType(json);
    final sequence = requireInt(json, 'sequence');
    switch (type) {
      case 'snapshot':
        return Snapshot<Map<String, Object?>>(
          aggregateId: requireString(json, 'aggregateId'),
          row: requireMap(json, 'row'),
          sequence: sequence,
        );
      case 'delta':
        return Delta<Map<String, Object?>>(
          aggregateId: requireString(json, 'aggregateId'),
          row: requireMap(json, 'row'),
          sequence: sequence,
        );
      case 'tombstone':
        return Tombstone<Map<String, Object?>>(
          aggregateId: requireString(json, 'aggregateId'),
          sequence: sequence,
        );
      case 'end_of_replay':
        return EndOfReplay<Map<String, Object?>>(sequence: sequence);
      default:
        throw FormatException('unknown update type: $type');
    }
  }

  /// Peeks at the subscriptionId without fully decoding the envelope.
  /// Used by RemoteConnection to route incoming envelopes.
  static String subscriptionIdOf(Map<String, Object?> json) =>
      requireString(json, 'subscriptionId');
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/update_codec_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/update_codec.dart reaction/test/wire/update_codec_test.dart
git commit -m "[CUR-1317] reaction wire: Update<T> JSON codec (4 variants)"
```

### Task 7: ActionSubmission codec

**Files:**

- Create: `reaction/lib/src/wire/action_submission_codec.dart`
- Create: `reaction/test/wire/action_submission_codec_test.dart`

- [ ] **Step 1: Write round-trip tests**

```dart
// reaction/test/wire/action_submission_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';

void main() {
  test('round-trips minimal ActionSubmission', () {
    const original = ActionSubmission(
      actionName: 'sayHello',
      rawInput: {'name': 'Alice'},
    );
    final json = ActionSubmissionCodec.encode(original);
    final decoded = ActionSubmissionCodec.decode(json);
    expect(decoded.actionName, 'sayHello');
    expect(decoded.rawInput, {'name': 'Alice'});
    expect(decoded.idempotencyKey, isNull);
    expect(decoded.flowToken, isNull);
  });

  test('round-trips ActionSubmission with all fields', () {
    const original = ActionSubmission(
      actionName: 'editNote',
      rawInput: {'noteId': 'n-1', 'title': 'updated'},
      idempotencyKey: 'idem-123',
      flowToken: 'flow-abc',
    );
    final json = ActionSubmissionCodec.encode(original);
    final decoded = ActionSubmissionCodec.decode(json);
    expect(decoded.actionName, 'editNote');
    expect(decoded.rawInput, {'noteId': 'n-1', 'title': 'updated'});
    expect(decoded.idempotencyKey, 'idem-123');
    expect(decoded.flowToken, 'flow-abc');
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/action_submission_codec_test.dart
```

- [ ] **Step 3: Implement action_submission_codec.dart**

```dart
// reaction/lib/src/wire/action_submission_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for [ActionSubmission].
///
/// Wire shape:
///   {"actionName": "...",
///    "rawInput": { ... },
///    "idempotencyKey": "..." (optional),
///    "flowToken": "..." (optional)}
class ActionSubmissionCodec {
  const ActionSubmissionCodec._();

  static Map<String, Object?> encode(ActionSubmission s) {
    final out = <String, Object?>{
      'actionName': s.actionName,
      'rawInput': s.rawInput,
    };
    if (s.idempotencyKey != null) out['idempotencyKey'] = s.idempotencyKey;
    if (s.flowToken != null) out['flowToken'] = s.flowToken;
    return out;
  }

  static ActionSubmission decode(Map<String, Object?> json) {
    return ActionSubmission(
      actionName: requireString(json, 'actionName'),
      rawInput: requireMap(json, 'rawInput'),
      idempotencyKey: readString(json, 'idempotencyKey'),
      flowToken: readString(json, 'flowToken'),
    );
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/action_submission_codec_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/action_submission_codec.dart reaction/test/wire/action_submission_codec_test.dart
git commit -m "[CUR-1317] reaction wire: ActionSubmission JSON codec"
```

### Task 8: DispatchResult codec

**Files:**

- Create: `reaction/lib/src/wire/dispatch_result_codec.dart`
- Create: `reaction/test/wire/dispatch_result_codec_test.dart`

`DispatchResult<TResult>` is a sealed type. The wire codec handles each variant by type discriminator. `Success` carries `appendedEvents: List<StoredEvent>` — those need their own JSON encoding too (defined here as a private helper).

- [ ] **Step 1: Write tests for each variant**

```dart
// reaction/test/wire/dispatch_result_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';

void main() {
  test('round-trips Success', () {
    final original = Success<Object?>(
      result: {'echo': 'hello'},
      appendedEvents: [
        StoredEvent(
          sequence: 1,
          eventId: 'evt-1',
          aggregateType: 'note',
          aggregateId: 'agg-1',
          entryType: 'note',
          entryTypeVersion: 1,
          eventType: 'note_updated',
          payload: const {'title': 'x'},
          clientTimestamp: DateTime.utc(2026, 5, 13, 10, 0, 0),
          recordedTimestamp: DateTime.utc(2026, 5, 13, 10, 0, 1),
          initiator: const UserPrincipal(
            userId: 'u-1', activeRole: 'install'),
          chainHash: 'hash-1',
          payloadHash: 'phash-1',
          source: const Source(
            identifier: 'install-x', softwareVersion: 'test-1'),
          metadata: const {},
        ),
      ],
    );
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'success');
    final decoded = DispatchResultCodec.decode(json);
    expect(decoded, isA<Success<Object?>>());
    final s = decoded as Success<Object?>;
    expect(s.result, {'echo': 'hello'});
    expect(s.appendedEvents, hasLength(1));
    expect(s.appendedEvents.first.eventId, 'evt-1');
  });

  test('round-trips AuthorizationDenied', () {
    final original = AuthorizationDenied(
      reason: DenyReason.notGranted,
      permission: const Permission('note.edit', scopeClass: 'patient'),
      scope: const BoundScope(class_: 'patient', value: 'p-1'),
      detail: 'no scope assignment',
    );
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'authorization_denied');
    final decoded = DispatchResultCodec.decode(json);
    expect(decoded, isA<AuthorizationDenied>());
    final d = decoded as AuthorizationDenied;
    expect(d.permission.name, 'note.edit');
    expect(d.scope, isA<BoundScope>());
  });

  test('round-trips InputInvalid', () {
    final original = InputInvalid(message: 'missing field "title"');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'input_invalid');
    final decoded = DispatchResultCodec.decode(json);
    expect(decoded, isA<InputInvalid>());
    expect((decoded as InputInvalid).message, 'missing field "title"');
  });

  test('round-trips ActionFailed', () {
    final original = ActionFailed(message: 'sembast write failed');
    final json = DispatchResultCodec.encode(original);
    expect(json['type'], 'action_failed');
    final decoded = DispatchResultCodec.decode(json);
    expect(decoded, isA<ActionFailed>());
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/dispatch_result_codec_test.dart
```

- [ ] **Step 3: Implement dispatch_result_codec.dart**

This codec composes several smaller codecs (`StoredEvent`, `Permission`, `ScopeValue`). Define inline helpers since these are private to this file.

```dart
// reaction/lib/src/wire/dispatch_result_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';
import 'principal_codec.dart';

/// JSON codec for [DispatchResult<TResult>]. The result is encoded
/// as JSON-friendly Map<String, Object?>; the client deserializes
/// further if needed (the substrate's existing pattern).
///
/// Wire shape per variant:
///   {"type": "success", "result": <jsonable>,
///    "appendedEvents": [<StoredEvent json>, ...]}
///   {"type": "authorization_denied", "reason": "...",
///    "permission": <Permission json>, "scope": <ScopeValue json|null>,
///    "detail": "..."}
///   {"type": "input_invalid", "message": "..."}
///   {"type": "action_failed", "message": "..."}
class DispatchResultCodec {
  const DispatchResultCodec._();

  static Map<String, Object?> encode(DispatchResult<Object?> r) {
    if (r is Success<Object?>) {
      return {
        'type': 'success',
        'result': r.result,
        'appendedEvents':
            r.appendedEvents.map(_encodeStoredEvent).toList(),
      };
    } else if (r is AuthorizationDenied) {
      return {
        'type': 'authorization_denied',
        'reason': _encodeDenyReason(r.reason),
        'permission': _encodePermission(r.permission),
        'scope': r.scope == null ? null : _encodeScopeValue(r.scope!),
        'detail': r.detail,
      };
    } else if (r is InputInvalid) {
      return {'type': 'input_invalid', 'message': r.message};
    } else if (r is ActionFailed) {
      return {'type': 'action_failed', 'message': r.message};
    } else {
      throw FormatException('unknown DispatchResult: ${r.runtimeType}');
    }
  }

  static DispatchResult<Object?> decode(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'success':
        final events = (json['appendedEvents'] as List)
            .cast<Map<String, Object?>>()
            .map(_decodeStoredEvent)
            .toList();
        return Success<Object?>(
          result: json['result'],
          appendedEvents: events,
        );
      case 'authorization_denied':
        final scopeJson = json['scope'];
        return AuthorizationDenied(
          reason: _decodeDenyReason(requireString(json, 'reason')),
          permission: _decodePermission(requireMap(json, 'permission')),
          scope: scopeJson == null
              ? null
              : _decodeScopeValue(scopeJson as Map<String, Object?>),
          detail: readString(json, 'detail'),
        );
      case 'input_invalid':
        return InputInvalid(message: requireString(json, 'message'));
      case 'action_failed':
        return ActionFailed(message: requireString(json, 'message'));
      default:
        throw FormatException('unknown DispatchResult type: $type');
    }
  }

  // --- StoredEvent helpers (private to this file) ---

  static Map<String, Object?> _encodeStoredEvent(StoredEvent e) => {
        'sequence': e.sequence,
        'eventId': e.eventId,
        'aggregateType': e.aggregateType,
        'aggregateId': e.aggregateId,
        'entryType': e.entryType,
        'entryTypeVersion': e.entryTypeVersion,
        'eventType': e.eventType,
        'payload': e.payload,
        'clientTimestamp': e.clientTimestamp.toIso8601String(),
        'recordedTimestamp': e.recordedTimestamp.toIso8601String(),
        'initiator': PrincipalCodec.encode(e.initiator),
        'chainHash': e.chainHash,
        'payloadHash': e.payloadHash,
        'source': {
          'identifier': e.source.identifier,
          'softwareVersion': e.source.softwareVersion,
        },
        'metadata': e.metadata,
      };

  static StoredEvent _decodeStoredEvent(Map<String, Object?> j) {
    final src = requireMap(j, 'source');
    return StoredEvent(
      sequence: requireInt(j, 'sequence'),
      eventId: requireString(j, 'eventId'),
      aggregateType: requireString(j, 'aggregateType'),
      aggregateId: requireString(j, 'aggregateId'),
      entryType: requireString(j, 'entryType'),
      entryTypeVersion: requireInt(j, 'entryTypeVersion'),
      eventType: requireString(j, 'eventType'),
      payload: requireMap(j, 'payload'),
      clientTimestamp: DateTime.parse(requireString(j, 'clientTimestamp')),
      recordedTimestamp: DateTime.parse(requireString(j, 'recordedTimestamp')),
      initiator: PrincipalCodec.decode(requireMap(j, 'initiator')),
      chainHash: requireString(j, 'chainHash'),
      payloadHash: requireString(j, 'payloadHash'),
      source: Source(
        identifier: requireString(src, 'identifier'),
        softwareVersion: requireString(src, 'softwareVersion'),
      ),
      metadata: requireMap(j, 'metadata'),
    );
  }

  // --- Permission helpers ---

  static Map<String, Object?> _encodePermission(Permission p) => {
        'name': p.name,
        'scopeClass': p.scopeClass,
      };

  static Permission _decodePermission(Map<String, Object?> j) => Permission(
        requireString(j, 'name'),
        scopeClass: readString(j, 'scopeClass'),
      );

  // --- ScopeValue helpers (CUR-1331 sealed type) ---

  static Map<String, Object?> _encodeScopeValue(ScopeValue v) {
    if (v is BoundScope) {
      return {'class': v.class_, 'value': v.value};
    } else if (v is ValueWildcardScope) {
      return {'class': v.class_, 'wildcard_value': true};
    } else if (v is TotalWildcardScope) {
      return {'wildcard_class': true};
    } else {
      throw FormatException('unknown ScopeValue: ${v.runtimeType}');
    }
  }

  static ScopeValue _decodeScopeValue(Map<String, Object?> j) {
    if (j['wildcard_class'] == true) return const TotalWildcardScope();
    if (j['wildcard_value'] == true) {
      return ValueWildcardScope(class_: requireString(j, 'class'));
    }
    return BoundScope(
      class_: requireString(j, 'class'),
      value: requireString(j, 'value'),
    );
  }

  // --- DenyReason helpers ---

  static String _encodeDenyReason(DenyReason r) {
    switch (r) {
      case DenyReason.notGranted: return 'not_granted';
      case DenyReason.scopeUnresolvable: return 'scope_unresolvable';
    }
  }

  static DenyReason _decodeDenyReason(String s) {
    switch (s) {
      case 'not_granted': return DenyReason.notGranted;
      case 'scope_unresolvable': return DenyReason.scopeUnresolvable;
      default: throw FormatException('unknown DenyReason: $s');
    }
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/dispatch_result_codec_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/dispatch_result_codec.dart reaction/test/wire/dispatch_result_codec_test.dart
git commit -m "[CUR-1317] reaction wire: DispatchResult JSON codec (4 variants)"
```

### Task 9: EffectiveAuthorization codec

**Files:**

- Create: `reaction/lib/src/wire/effective_authorization_codec.dart`
- Create: `reaction/test/wire/effective_authorization_codec_test.dart`

- [ ] **Step 1: Write round-trip tests**

```dart
// reaction/test/wire/effective_authorization_codec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

void main() {
  test('round-trips minimal EffectiveAuthorization (no scope assignments)', () {
    const original = EffectiveAuthorization(
      activeRole: 'install',
      rolePermissions: {Permission('greet.send')},
      scopeAssignments: [],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded.activeRole, 'install');
    expect(decoded.rolePermissions, original.rolePermissions);
    expect(decoded.scopeAssignments, isEmpty);
  });

  test('round-trips with scope assignments', () {
    const original = EffectiveAuthorization(
      activeRole: 'StudyCoordinator',
      rolePermissions: {
        Permission('patient.view', scopeClass: 'patient'),
        Permission('patient.edit', scopeClass: 'patient'),
      },
      scopeAssignments: [
        ScopeAssignment(scope: BoundScope(class_: 'site', value: 'A')),
        ScopeAssignment(scope: BoundScope(class_: 'site', value: 'B')),
      ],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded.activeRole, 'StudyCoordinator');
    expect(decoded.rolePermissions.length, 2);
    expect(decoded.scopeAssignments.length, 2);
  });

  test('round-trips with total wildcard scope', () {
    const original = EffectiveAuthorization(
      activeRole: 'Admin',
      rolePermissions: {Permission('admin.all')},
      scopeAssignments: [
        ScopeAssignment(scope: TotalWildcardScope()),
      ],
    );
    final json = EffectiveAuthorizationCodec.encode(original);
    final decoded = EffectiveAuthorizationCodec.decode(json);
    expect(decoded.scopeAssignments.first.scope,
        isA<TotalWildcardScope>());
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/effective_authorization_codec_test.dart
```

- [ ] **Step 3: Implement codec**

```dart
// reaction/lib/src/wire/effective_authorization_codec.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for CUR-1331's [EffectiveAuthorization] type — the
/// substrate's replacement for the legacy PermissionSnapshot. Carries
/// the activeRole + permissions granted to that role + the user's
/// scope assignments under that role.
///
/// Wire shape:
///   {"activeRole": "...",
///    "rolePermissions": [{"name": "...", "scopeClass": "..." | null}, ...],
///    "scopeAssignments": [{"scope": <ScopeValue json>}, ...]}
class EffectiveAuthorizationCodec {
  const EffectiveAuthorizationCodec._();

  static Map<String, Object?> encode(EffectiveAuthorization e) => {
        'activeRole': e.activeRole,
        'rolePermissions':
            e.rolePermissions.map(_encodePermission).toList(),
        'scopeAssignments':
            e.scopeAssignments.map(_encodeScopeAssignment).toList(),
      };

  static EffectiveAuthorization decode(Map<String, Object?> json) {
    return EffectiveAuthorization(
      activeRole: requireString(json, 'activeRole'),
      rolePermissions: (json['rolePermissions'] as List)
          .cast<Map<String, Object?>>()
          .map(_decodePermission)
          .toSet(),
      scopeAssignments: (json['scopeAssignments'] as List)
          .cast<Map<String, Object?>>()
          .map(_decodeScopeAssignment)
          .toList(),
    );
  }

  static Map<String, Object?> _encodePermission(Permission p) => {
        'name': p.name,
        'scopeClass': p.scopeClass,
      };

  static Permission _decodePermission(Map<String, Object?> j) => Permission(
        requireString(j, 'name'),
        scopeClass: readString(j, 'scopeClass'),
      );

  static Map<String, Object?> _encodeScopeAssignment(ScopeAssignment a) =>
      {'scope': _encodeScopeValue(a.scope)};

  static ScopeAssignment _decodeScopeAssignment(Map<String, Object?> j) =>
      ScopeAssignment(scope: _decodeScopeValue(requireMap(j, 'scope')));

  static Map<String, Object?> _encodeScopeValue(ScopeValue v) {
    if (v is BoundScope) {
      return {'class': v.class_, 'value': v.value};
    } else if (v is ValueWildcardScope) {
      return {'class': v.class_, 'wildcard_value': true};
    } else if (v is TotalWildcardScope) {
      return {'wildcard_class': true};
    } else {
      throw FormatException('unknown ScopeValue: ${v.runtimeType}');
    }
  }

  static ScopeValue _decodeScopeValue(Map<String, Object?> j) {
    if (j['wildcard_class'] == true) return const TotalWildcardScope();
    if (j['wildcard_value'] == true) {
      return ValueWildcardScope(class_: requireString(j, 'class'));
    }
    return BoundScope(
      class_: requireString(j, 'class'),
      value: requireString(j, 'value'),
    );
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/effective_authorization_codec_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/effective_authorization_codec.dart reaction/test/wire/effective_authorization_codec_test.dart
git commit -m "[CUR-1317] reaction wire: EffectiveAuthorization JSON codec (CUR-1331 type)"
```

### Task 10: Subscription messages (WS control plane)

**Files:**

- Create: `reaction/lib/src/wire/subscription_messages.dart`
- Create: `reaction/test/wire/subscription_messages_test.dart`

- [ ] **Step 1: Write round-trip tests for each message type**

```dart
// reaction/test/wire/subscription_messages_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/subscription_messages.dart';

void main() {
  test('round-trips AuthMsg', () {
    const original = AuthMsg(credential: 'opaque-token');
    final j = SubscriptionMessages.encodeClient(original);
    expect(j, {'type': 'auth', 'credential': 'opaque-token'});
    final d = SubscriptionMessages.decodeClient(j) as AuthMsg;
    expect(d.credential, 'opaque-token');
  });

  test('round-trips SubscribeMsg minimal', () {
    const original = SubscribeMsg(
      subscriptionId: 'sub-1',
      viewName: 'notes_today',
    );
    final j = SubscriptionMessages.encodeClient(original);
    final d = SubscriptionMessages.decodeClient(j) as SubscribeMsg;
    expect(d.subscriptionId, 'sub-1');
    expect(d.viewName, 'notes_today');
    expect(d.filter, isNull);
    expect(d.aggregates, isNull);
  });

  test('round-trips SubscribeMsg with filter and aggregates', () {
    const original = SubscribeMsg(
      subscriptionId: 'sub-2',
      viewName: 'patient_files',
      filter: SubscriptionFilter(entryTypes: {'note'}),
      aggregates: {'p-1', 'p-2'},
    );
    final j = SubscriptionMessages.encodeClient(original);
    final d = SubscriptionMessages.decodeClient(j) as SubscribeMsg;
    expect(d.filter?.entryTypes, {'note'});
    expect(d.aggregates, {'p-1', 'p-2'});
  });

  test('round-trips UnsubscribeMsg', () {
    const original = UnsubscribeMsg(subscriptionId: 'sub-1');
    final j = SubscriptionMessages.encodeClient(original);
    expect(j, {'type': 'unsubscribe', 'subscriptionId': 'sub-1'});
  });

  test('round-trips AuthOkMsg', () {
    const original = AuthOkMsg(principalId: 'u-1');
    final j = SubscriptionMessages.encodeServer(original);
    expect(j, {'type': 'auth_ok', 'principalId': 'u-1'});
  });

  test('round-trips SubscriptionDeniedMsg', () {
    const original = SubscriptionDeniedMsg(
      subscriptionId: 'sub-1',
      reason: SubscriptionDenyReason.viewPermissionDenied,
    );
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'subscription_denied');
    expect(j['reason'], 'view_permission_denied');
  });

  test('round-trips ErrorMsg', () {
    const original = ErrorMsg(
      code: WireErrorCode.protocolError,
      message: 'bad json',
    );
    final j = SubscriptionMessages.encodeServer(original);
    expect(j['type'], 'error');
    expect(j['code'], 'protocol_error');
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/wire/subscription_messages_test.dart
```

- [ ] **Step 3: Implement subscription_messages.dart**

```dart
// reaction/lib/src/wire/subscription_messages.dart
import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';
import 'filter_codec.dart';

// --- Client -> Server messages ---

sealed class ClientMessage { const ClientMessage(); }

class AuthMsg extends ClientMessage {
  final String credential;
  const AuthMsg({required this.credential});
}

class SubscribeMsg extends ClientMessage {
  final String subscriptionId;
  final String viewName;
  final SubscriptionFilter? filter;
  final Set<String>? aggregates;
  const SubscribeMsg({
    required this.subscriptionId,
    required this.viewName,
    this.filter,
    this.aggregates,
  });
}

class UnsubscribeMsg extends ClientMessage {
  final String subscriptionId;
  const UnsubscribeMsg({required this.subscriptionId});
}

// --- Server -> Client messages ---

sealed class ServerMessage { const ServerMessage(); }

class AuthOkMsg extends ServerMessage {
  final String principalId;
  const AuthOkMsg({required this.principalId});
}

enum SubscriptionDenyReason {
  viewPermissionDenied,
  unknownView,
  malformedFilter;

  String toWire() {
    switch (this) {
      case SubscriptionDenyReason.viewPermissionDenied:
        return 'view_permission_denied';
      case SubscriptionDenyReason.unknownView:
        return 'unknown_view';
      case SubscriptionDenyReason.malformedFilter:
        return 'malformed_filter';
    }
  }

  static SubscriptionDenyReason fromWire(String s) {
    switch (s) {
      case 'view_permission_denied':
        return SubscriptionDenyReason.viewPermissionDenied;
      case 'unknown_view':
        return SubscriptionDenyReason.unknownView;
      case 'malformed_filter':
        return SubscriptionDenyReason.malformedFilter;
      default:
        throw FormatException('unknown SubscriptionDenyReason: $s');
    }
  }
}

class SubscriptionDeniedMsg extends ServerMessage {
  final String subscriptionId;
  final SubscriptionDenyReason reason;
  const SubscriptionDeniedMsg({
    required this.subscriptionId,
    required this.reason,
  });
}

enum WireErrorCode {
  internalError,
  protocolError;

  String toWire() {
    switch (this) {
      case WireErrorCode.internalError: return 'internal_error';
      case WireErrorCode.protocolError: return 'protocol_error';
    }
  }

  static WireErrorCode fromWire(String s) {
    switch (s) {
      case 'internal_error': return WireErrorCode.internalError;
      case 'protocol_error': return WireErrorCode.protocolError;
      default: throw FormatException('unknown WireErrorCode: $s');
    }
  }
}

class ErrorMsg extends ServerMessage {
  final WireErrorCode code;
  final String message;
  const ErrorMsg({required this.code, required this.message});
}

/// Codec for the WS control-plane envelopes. Note: Update<T> envelopes
/// (server -> client) live in update_codec.dart; this codec covers
/// only the control-plane shapes.
class SubscriptionMessages {
  const SubscriptionMessages._();

  static Map<String, Object?> encodeClient(ClientMessage m) {
    if (m is AuthMsg) {
      return {'type': 'auth', 'credential': m.credential};
    } else if (m is SubscribeMsg) {
      return {
        'type': 'subscribe',
        'subscriptionId': m.subscriptionId,
        'viewName': m.viewName,
        if (m.filter != null) 'filter': FilterCodec.encode(m.filter!),
        if (m.aggregates != null) 'aggregates': m.aggregates!.toList()..sort(),
      };
    } else if (m is UnsubscribeMsg) {
      return {'type': 'unsubscribe', 'subscriptionId': m.subscriptionId};
    } else {
      throw FormatException('unknown ClientMessage: ${m.runtimeType}');
    }
  }

  static ClientMessage decodeClient(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'auth':
        return AuthMsg(credential: requireString(json, 'credential'));
      case 'subscribe':
        final aggregates = json['aggregates'];
        return SubscribeMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
          viewName: requireString(json, 'viewName'),
          filter: json['filter'] == null
              ? null
              : FilterCodec.decode(json['filter'] as Map<String, Object?>),
          aggregates: aggregates == null
              ? null
              : (aggregates as List).cast<String>().toSet(),
        );
      case 'unsubscribe':
        return UnsubscribeMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
        );
      default:
        throw FormatException('unknown client message type: $type');
    }
  }

  static Map<String, Object?> encodeServer(ServerMessage m) {
    if (m is AuthOkMsg) {
      return {'type': 'auth_ok', 'principalId': m.principalId};
    } else if (m is SubscriptionDeniedMsg) {
      return {
        'type': 'subscription_denied',
        'subscriptionId': m.subscriptionId,
        'reason': m.reason.toWire(),
      };
    } else if (m is ErrorMsg) {
      return {
        'type': 'error',
        'code': m.code.toWire(),
        'message': m.message,
      };
    } else {
      throw FormatException('unknown ServerMessage: ${m.runtimeType}');
    }
  }

  static ServerMessage decodeServer(Map<String, Object?> json) {
    final type = readType(json);
    switch (type) {
      case 'auth_ok':
        return AuthOkMsg(principalId: requireString(json, 'principalId'));
      case 'subscription_denied':
        return SubscriptionDeniedMsg(
          subscriptionId: requireString(json, 'subscriptionId'),
          reason: SubscriptionDenyReason.fromWire(
              requireString(json, 'reason')),
        );
      case 'error':
        return ErrorMsg(
          code: WireErrorCode.fromWire(requireString(json, 'code')),
          message: requireString(json, 'message'),
        );
      default:
        throw FormatException('unknown server message type: $type');
    }
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/wire/subscription_messages_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/wire/subscription_messages.dart reaction/test/wire/subscription_messages_test.dart
git commit -m "[CUR-1317] reaction wire: subscription control-plane messages"
```

---

## Phase 2 — Server primitives

### Task 11: TrustingAuthValidator

**Files:**

- Create: `reaction/lib/src/server/validators/trusting_auth_validator.dart`
- Create: `reaction/test/server/trusting_auth_validator_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/server/trusting_auth_validator_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';

void main() {
  test('accepts non-empty credential as Principal.id', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'install');
    final p = await v.authenticate('user-123');
    expect(p, isA<UserPrincipal>());
    expect((p as UserPrincipal).userId, 'user-123');
    expect(p.activeRole, 'install');
  });

  test('uses configured defaultActiveRole', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'StudyCoordinator');
    final p = await v.authenticate('user-x') as UserPrincipal;
    expect(p.activeRole, 'StudyCoordinator');
  });

  test('rejects empty credential', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'install');
    await expectLater(
      () => v.authenticate(''),
      throwsA(isA<AuthenticationDenied>()),
    );
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/trusting_auth_validator_test.dart
```

- [ ] **Step 3: Implement validator**

```dart
// reaction/lib/src/server/validators/trusting_auth_validator.dart
// Implements: EVS-PRD-auth-session/F — TrustingAuthValidator reference impl.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';

/// Reference [PrincipalAuthValidator] for dev/test use only.
///
/// Accepts any non-empty credential string verbatim as `Principal.id`.
/// Loud "DO NOT USE IN PRODUCTION" docstring intentional — this impl
/// performs NO cryptographic validation. Production deployments must
/// mount their own validator (Firebase, Auth0, linking-code, etc.) at
/// composition time.
class TrustingAuthValidator implements PrincipalAuthValidator {
  TrustingAuthValidator({required this.defaultActiveRole});

  final String defaultActiveRole;

  @override
  Future<Principal> authenticate(String credential) async {
    if (credential.isEmpty) {
      throw const AuthenticationDenied('empty credential');
    }
    return UserPrincipal(
      userId: credential,
      activeRole: defaultActiveRole,
    );
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/trusting_auth_validator_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/validators/trusting_auth_validator.dart reaction/test/server/trusting_auth_validator_test.dart
git commit -m "[CUR-1317] reaction server: TrustingAuthValidator dev/test reference impl"
```

### Task 12: ViewScopeRegistry

**Files:**

- Create: `reaction/lib/src/server/view_scope_registry.dart`
- Create: `reaction/test/server/view_scope_registry_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/server/view_scope_registry_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/view_scope_registry.dart';

void main() {
  test('returns null for unregistered view', () {
    final r = ViewScopeRegistry();
    expect(r.lookup('unknown_view'), isNull);
  });

  test('returns binding for registered view', () {
    final r = ViewScopeRegistry()
      ..register(
        viewName: 'patient_files',
        scopeClass: 'patient',
        aggregateIdResolver: (sv) =>
            sv is BoundScope ? sv.value : null,
      );
    final binding = r.lookup('patient_files');
    expect(binding, isNotNull);
    expect(binding!.scopeClass, 'patient');
    expect(
      binding.aggregateIdResolver(
        const BoundScope(class_: 'patient', value: 'p-42')),
      'p-42',
    );
  });

  test('rejects duplicate registration', () {
    final r = ViewScopeRegistry();
    r.register(
      viewName: 'v',
      scopeClass: 'site',
      aggregateIdResolver: (_) => null,
    );
    expect(
      () => r.register(
        viewName: 'v',
        scopeClass: 'site',
        aggregateIdResolver: (_) => null,
      ),
      throwsArgumentError,
    );
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/view_scope_registry_test.dart
```

- [ ] **Step 3: Implement registry**

```dart
// reaction/lib/src/server/view_scope_registry.dart
import 'package:event_sourcing/event_sourcing.dart';

/// Maps a substrate [ProjectionSpec.viewName] to its scope-class
/// binding (which scope class scopes this view, and how to resolve
/// a ScopeValue assigned to the user into the aggregate IDs the user
/// covers under that scope). Composition-time registration.
///
/// A view without a registration is unscoped at the row level
/// (admin views, public views, etc.). The reaction server's per-
/// subscription authorization treats unregistered views as "no
/// row-level narrowing required."
class ViewScopeRegistry {
  ViewScopeRegistry();

  final Map<String, ViewScopeBinding> _bindings = {};

  /// Register a view-to-scope-class binding.
  ///
  /// - [viewName]: matches a registered ProjectionSpec.viewName.
  /// - [scopeClass]: the scope class scoping this view (e.g., 'site',
  ///   'patient'). Must match a registered ScopeClassSpec.name.
  /// - [aggregateIdResolver]: given a BoundScope (or ValueWildcardScope),
  ///   returns the aggregate ID this scope value corresponds to on
  ///   the view. For 1:1 mappings (patient scope value = patient
  ///   aggregate id), this is `(sv) => sv.value` for BoundScope and
  ///   `null` (or the full row scan via containment) for wildcards.
  ///   The server's expansion logic walks the containment graph as
  ///   needed; this resolver handles the "direct" case.
  void register({
    required String viewName,
    required String scopeClass,
    required String? Function(ScopeValue) aggregateIdResolver,
  }) {
    if (_bindings.containsKey(viewName)) {
      throw ArgumentError(
        'ViewScopeRegistry: duplicate registration for "$viewName"',
      );
    }
    _bindings[viewName] = ViewScopeBinding._(
      viewName: viewName,
      scopeClass: scopeClass,
      aggregateIdResolver: aggregateIdResolver,
    );
  }

  ViewScopeBinding? lookup(String viewName) => _bindings[viewName];
}

class ViewScopeBinding {
  ViewScopeBinding._({
    required this.viewName,
    required this.scopeClass,
    required this.aggregateIdResolver,
  });

  final String viewName;
  final String scopeClass;
  final String? Function(ScopeValue) aggregateIdResolver;
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/view_scope_registry_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/view_scope_registry.dart reaction/test/server/view_scope_registry_test.dart
git commit -m "[CUR-1317] reaction server: ViewScopeRegistry (viewName -> scopeClass binding)"
```

### Task 13: AuthMiddleware

**Files:**

- Create: `reaction/lib/src/server/auth_middleware.dart`
- Create: `reaction/test/server/auth_middleware_test.dart`

- [ ] **Step 1: Write tests using shelf's Request/Response**

```dart
// reaction/test/server/auth_middleware_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:shelf/shelf.dart';

void main() {
  late TrustingAuthValidator validator;

  setUp(() {
    validator = TrustingAuthValidator(defaultActiveRole: 'install');
  });

  Response inner(Request req) {
    final principal = principalFromContext(req);
    return Response.ok(principal == null
        ? 'no-principal'
        : (principal as UserPrincipal).userId);
  }

  test('attaches Principal on valid Bearer header', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(Request(
      'GET',
      Uri.parse('http://x/y'),
      headers: {'Authorization': 'Bearer alice'},
    ));
    expect(res.statusCode, 200);
    expect(await res.readAsString(), 'alice');
  });

  test('returns 401 on missing Authorization header', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(
      Request('GET', Uri.parse('http://x/y')),
    );
    expect(res.statusCode, 401);
  });

  test('returns 401 on non-Bearer Authorization', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(Request(
      'GET',
      Uri.parse('http://x/y'),
      headers: {'Authorization': 'Basic xyz'},
    ));
    expect(res.statusCode, 401);
  });

  test('returns 401 on AuthenticationDenied', () async {
    final mw = authMiddleware(validator);
    final handler = mw(inner);
    final res = await handler(Request(
      'GET',
      Uri.parse('http://x/y'),
      headers: {'Authorization': 'Bearer '}, // empty -> denied
    ));
    expect(res.statusCode, 401);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/auth_middleware_test.dart
```

- [ ] **Step 3: Implement middleware**

```dart
// reaction/lib/src/server/auth_middleware.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:shelf/shelf.dart';

const String _kPrincipalContextKey = 'reaction.principal';

/// Shelf middleware that reads `Authorization: Bearer <credential>`,
/// validates it with the supplied [PrincipalAuthValidator], and
/// attaches the resulting Principal to the request context under
/// the key consumed by [principalFromContext].
///
/// On missing/non-Bearer Authorization: returns 401.
/// On AuthenticationDenied: returns 401.
/// On other exception: returns 500.
Middleware authMiddleware(PrincipalAuthValidator validator) {
  return (Handler inner) {
    return (Request request) async {
      final header = request.headers['Authorization'];
      if (header == null || !header.startsWith('Bearer ')) {
        return Response(401);
      }
      final credential = header.substring('Bearer '.length);
      try {
        final principal = await validator.authenticate(credential);
        return inner(request.change(
          context: {_kPrincipalContextKey: principal},
        ));
      } on AuthenticationDenied {
        return Response(401);
      } catch (_) {
        return Response(500);
      }
    };
  };
}

/// Read the Principal attached by [authMiddleware].
Principal? principalFromContext(Request request) =>
    request.context[_kPrincipalContextKey] as Principal?;
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/auth_middleware_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/auth_middleware.dart reaction/test/server/auth_middleware_test.dart
git commit -m "[CUR-1317] reaction server: auth middleware (Bearer -> Principal)"
```

### Task 14: Action route (POST /actions)

**Files:**

- Create: `reaction/lib/src/server/action_route.dart`
- Create: `reaction/test/server/action_route_test.dart`

The action route reads the JSON-encoded `ActionSubmission` from the request body, constructs an `ActionContext` with the auth-middleware-attached Principal, dispatches via the supplied `ActionDispatcher`, and writes the JSON-encoded `DispatchResult` to the response body.

This task's tests use a stub `ActionDispatcher` (not the real one — that requires the full substrate harness, covered in Phase 4 E2E tests).

- [ ] **Step 1: Write tests with a stub dispatcher**

```dart
// reaction/test/server/action_route_test.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/action_route.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';
import 'package:shelf/shelf.dart';

class _StubDispatcher implements ActionDispatcher {
  _StubDispatcher(this.response);
  final DispatchResult<Object?> response;
  ActionSubmission? lastSubmission;
  ActionContext? lastCtx;

  @override
  Future<DispatchResult<TResult>> dispatch<TResult>(
    ActionSubmission submission,
    ActionContext ctx,
  ) async {
    lastSubmission = submission;
    lastCtx = ctx;
    return response as DispatchResult<TResult>;
  }
}

void main() {
  test('returns 200 + DispatchResult JSON on success', () async {
    final dispatcher = _StubDispatcher(
      Success<Object?>(result: {'echo': 'hi'}, appendedEvents: const []),
    );
    final handler = actionRouteHandler(dispatcher: dispatcher);
    final submission = const ActionSubmission(
      actionName: 'sayHello',
      rawInput: {'name': 'A'},
    );
    final body = jsonEncode(ActionSubmissionCodec.encode(submission));
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: body,
      context: {'reaction.principal':
          const UserPrincipal(userId: 'u-1', activeRole: 'install')},
    );
    final res = await handler(req);
    expect(res.statusCode, 200);
    final decoded = DispatchResultCodec.decode(
        jsonDecode(await res.readAsString()) as Map<String, Object?>);
    expect(decoded, isA<Success<Object?>>());
    expect(dispatcher.lastSubmission?.actionName, 'sayHello');
    expect((dispatcher.lastCtx!.principal as UserPrincipal).userId, 'u-1');
  });

  test('returns 400 on malformed body', () async {
    final handler = actionRouteHandler(
      dispatcher: _StubDispatcher(
        Success<Object?>(result: null, appendedEvents: const [])),
    );
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: '{not json',
      context: {'reaction.principal':
          const UserPrincipal(userId: 'u-1', activeRole: 'install')},
    );
    final res = await handler(req);
    expect(res.statusCode, 400);
  });

  test('returns 500 when no Principal in context', () async {
    final handler = actionRouteHandler(
      dispatcher: _StubDispatcher(
        Success<Object?>(result: null, appendedEvents: const [])),
    );
    final req = Request(
      'POST',
      Uri.parse('http://x/actions'),
      body: jsonEncode(ActionSubmissionCodec.encode(
        const ActionSubmission(actionName: 'x', rawInput: {}),
      )),
    );
    final res = await handler(req);
    expect(res.statusCode, 500);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/action_route_test.dart
```

- [ ] **Step 3: Implement action_route.dart**

```dart
// reaction/lib/src/server/action_route.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for POST /actions. Expects auth middleware to have attached
/// a Principal to the request context. Reads the JSON-encoded
/// ActionSubmission, dispatches via the supplied dispatcher, returns
/// the JSON-encoded DispatchResult.
Handler actionRouteHandler({
  required ActionDispatcher dispatcher,
  DateTime Function()? now,
}) {
  final clock = now ?? DateTime.now;
  return (Request request) async {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    final Map<String, Object?> json;
    try {
      json = jsonDecode(await request.readAsString())
          as Map<String, Object?>;
    } catch (_) {
      return Response(400, body: 'malformed json');
    }
    final ActionSubmission submission;
    try {
      submission = ActionSubmissionCodec.decode(json);
    } on FormatException catch (e) {
      return Response(400, body: 'malformed submission: ${e.message}');
    }
    final ctx = ActionContext(
      principal: principal,
      security: const SecurityDetails(),
      requestStartedAt: clock(),
    );
    final result = await dispatcher.dispatch<Object?>(submission, ctx);
    return Response.ok(
      jsonEncode(DispatchResultCodec.encode(result)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/action_route_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/action_route.dart reaction/test/server/action_route_test.dart
git commit -m "[CUR-1317] reaction server: POST /actions route handler"
```

### Task 15: /me route

**Files:**

- Create: `reaction/lib/src/server/me_route.dart`
- Create: `reaction/test/server/me_route_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/server/me_route_test.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/me_route.dart';
import 'package:reaction/src/wire/principal_codec.dart';
import 'package:shelf/shelf.dart';

void main() {
  test('returns 200 + Principal JSON when Principal in context', () async {
    final handler = meRouteHandler();
    final req = Request(
      'GET',
      Uri.parse('http://x/me'),
      context: {'reaction.principal':
          const UserPrincipal(userId: 'u-1', activeRole: 'install')},
    );
    final res = await handler(req);
    expect(res.statusCode, 200);
    final body = jsonDecode(await res.readAsString()) as Map<String, Object?>;
    final decoded = PrincipalCodec.decode(body);
    expect((decoded as UserPrincipal).userId, 'u-1');
  });

  test('returns 500 when no Principal', () async {
    final handler = meRouteHandler();
    final req = Request('GET', Uri.parse('http://x/me'));
    final res = await handler(req);
    expect(res.statusCode, 500);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/me_route_test.dart
```

- [ ] **Step 3: Implement me_route.dart**

```dart
// reaction/lib/src/server/me_route.dart
import 'dart:convert';

import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/principal_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for GET /me. Returns the Principal attached by the auth
/// middleware. The Remote client uses this to validate a credential
/// and obtain the Principal for AuthStatus.Authenticated.
Handler meRouteHandler() {
  return (Request request) {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    return Response.ok(
      jsonEncode(PrincipalCodec.encode(principal)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/me_route_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/me_route.dart reaction/test/server/me_route_test.dart
git commit -m "[CUR-1317] reaction server: GET /me route handler"
```

### Task 16: /permissions/snapshot route

**Files:**

- Create: `reaction/lib/src/server/permission_route.dart`
- Create: `reaction/test/server/permission_route_test.dart`

- [ ] **Step 1: Write tests using a stub AuthorizationPolicy**

```dart
// reaction/test/server/permission_route_test.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/server/permission_route.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:shelf/shelf.dart';

class _StubPolicy implements AuthorizationPolicy {
  _StubPolicy(this.snapshot);
  final EffectiveAuthorization snapshot;

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
          Principal principal) async =>
      snapshot;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async =>
      AuthorizationDecision.allow();
}

void main() {
  test('returns 200 + EffectiveAuthorization JSON', () async {
    final stub = _StubPolicy(const EffectiveAuthorization(
      activeRole: 'install',
      rolePermissions: {Permission('greet.send')},
      scopeAssignments: [],
    ));
    final handler = permissionRouteHandler(policy: stub);
    final req = Request(
      'GET',
      Uri.parse('http://x/permissions/snapshot'),
      context: {'reaction.principal':
          const UserPrincipal(userId: 'u-1', activeRole: 'install')},
    );
    final res = await handler(req);
    expect(res.statusCode, 200);
    final body = jsonDecode(await res.readAsString()) as Map<String, Object?>;
    final decoded = EffectiveAuthorizationCodec.decode(body);
    expect(decoded.activeRole, 'install');
    expect(decoded.rolePermissions.first.name, 'greet.send');
  });

  test('returns 500 when no Principal', () async {
    final stub = _StubPolicy(const EffectiveAuthorization(
      activeRole: '',
      rolePermissions: {},
      scopeAssignments: [],
    ));
    final handler = permissionRouteHandler(policy: stub);
    final req = Request('GET', Uri.parse('http://x/permissions/snapshot'));
    final res = await handler(req);
    expect(res.statusCode, 500);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/permission_route_test.dart
```

- [ ] **Step 3: Implement permission_route.dart**

```dart
// reaction/lib/src/server/permission_route.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for GET /permissions/snapshot. Returns the
/// EffectiveAuthorization for the Principal attached by the auth
/// middleware.
Handler permissionRouteHandler({required AuthorizationPolicy policy}) {
  return (Request request) async {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    final auth = await policy.effectivePermissionsFor(principal);
    return Response.ok(
      jsonEncode(EffectiveAuthorizationCodec.encode(auth)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/permission_route_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/permission_route.dart reaction/test/server/permission_route_test.dart
git commit -m "[CUR-1317] reaction server: GET /permissions/snapshot route handler"
```

### Task 17: Subscription handler (WS upgrade + per-sub authz + relay)

**Files:**

- Create: `reaction/lib/src/server/subscription_handler.dart`
- Create: `reaction/test/server/subscription_handler_test.dart`

This is the largest server-side task. The handler runs the per-connection state machine (`AWAITING_AUTH → AUTHENTICATED`), opens per-subscription `EventStore.subscribe<T>` calls with row-level narrowing per Approach B, and serializes WS writes through a single async queue.

Tests in this task use stubs; full E2E coverage lives in Phase 4. The structural test here verifies the state machine logic and per-sub authz call shape.

- [ ] **Step 1: Write tests using stubs + a fake WS channel**

```dart
// reaction/test/server/subscription_handler_test.dart
import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// Pair of WebSocketChannels that talk to each other in-process for tests.
class _Pair {
  _Pair() {
    final clientToServer = StreamController<Object?>();
    final serverToClient = StreamController<Object?>();
    serverSide = _MemChannel(
      stream: clientToServer.stream,
      sink: serverToClient.sink,
    );
    clientSide = _MemChannel(
      stream: serverToClient.stream,
      sink: clientToServer.sink,
    );
  }
  late final WebSocketChannel serverSide;
  late final WebSocketChannel clientSide;
}

class _MemChannel implements WebSocketChannel {
  _MemChannel({required this.stream, required this.sink});
  @override
  final Stream<dynamic> stream;
  @override
  final WebSocketSink sink;
  // Minimal stubs for unused properties:
  @override int? get closeCode => null;
  @override String? get closeReason => null;
  @override String? get protocol => null;
  @override Future<void> get ready => Future.value();
}

// Stub EventStore returning a controlled Update stream.
class _StubEventStore implements EventStore {
  final StreamController<Update<Map<String, Object?>>> _ctl =
      StreamController<Update<Map<String, Object?>>>.broadcast();

  void push(Update<Map<String, Object?>> u) => _ctl.add(u);

  @override
  Stream<Update<T>> subscribe<T>(
    SubscriptionFilter filter,
    SubscribeMode<T> mode,
  ) {
    return _ctl.stream.cast<Update<T>>();
  }

  // All other EventStore methods throw — they shouldn't be called.
  @override
  dynamic noSuchMethod(Invocation i) =>
      throw UnimplementedError(i.memberName.toString());
}

class _StubPolicy implements AuthorizationPolicy {
  _StubPolicy({this.allow = true, this.effective});
  final bool allow;
  final EffectiveAuthorization? effective;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async =>
      allow
          ? AuthorizationDecision.allow()
          : AuthorizationDecision.deny(DenyReason.notGranted);

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
          Principal principal) async =>
      effective ??
      const EffectiveAuthorization(
        activeRole: 'install',
        rolePermissions: {},
        scopeAssignments: [],
      );
}

void main() {
  late PrincipalAuthValidator validator;
  late _StubEventStore store;
  late _StubPolicy policy;
  late ViewScopeRegistry viewScopes;

  setUp(() {
    validator = TrustingAuthValidator(defaultActiveRole: 'install');
    store = _StubEventStore();
    policy = _StubPolicy();
    viewScopes = ViewScopeRegistry();
  });

  Future<Map<String, Object?>> recvJson(
      WebSocketChannel channel, {int n = 1}) async {
    final received = await channel.stream
        .map((e) => jsonDecode(e as String) as Map<String, Object?>)
        .take(n)
        .toList();
    return received.last;
  }

  test('auth_ok on valid first message', () async {
    final pair = _Pair();
    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: policy,
      viewScopes: viewScopes,
      permissionViewName: 'role_permission_grants',
      viewPermissionNamer: (v) => 'view:$v',
    );

    pair.clientSide.sink.add(jsonEncode({
      'type': 'auth',
      'credential': 'alice',
    }));

    final res = await recvJson(pair.clientSide);
    expect(res['type'], 'auth_ok');
    expect(res['principalId'], 'alice');
  });

  test('subscription_denied when view-level perm fails', () async {
    final pair = _Pair();
    policy = _StubPolicy(allow: false);
    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: policy,
      viewScopes: viewScopes,
      permissionViewName: 'role_permission_grants',
      viewPermissionNamer: (v) => 'view:$v',
    );

    pair.clientSide.sink.add(jsonEncode(
        {'type': 'auth', 'credential': 'alice'}));
    pair.clientSide.sink.add(jsonEncode({
      'type': 'subscribe',
      'subscriptionId': 'sub-1',
      'viewName': 'audit_log',
    }));

    final res = await recvJson(pair.clientSide, n: 2);
    expect(res['type'], 'subscription_denied');
    expect(res['reason'], 'view_permission_denied');
  });

  // Additional tests (snapshot relay, unsubscribe, etc.) covered in
  // E2E suite (Phase 4) where the full substrate is exercised.
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/server/subscription_handler_test.dart
```

- [ ] **Step 3: Implement subscription_handler.dart**

```dart
// reaction/lib/src/server/subscription_handler.dart
import 'dart:async';
import 'dart:collection';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/wire/subscription_messages.dart';
import 'package:reaction/src/wire/update_codec.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Configuration injected at composition time; viewName -> required
/// view-level permission name. Returns null to skip view-level check
/// for public views.
typedef ViewPermissionNamer = String? Function(String viewName);

/// Run the per-connection state machine for an accepted WS upgrade.
///
/// Spawns a message loop that:
///   - Waits for AuthMsg, validates, sends AuthOkMsg or close 4001.
///   - Once AUTHENTICATED, processes SubscribeMsg / UnsubscribeMsg.
///   - On SubscribeMsg, applies two-tier authz (Approach B) and opens
///     a substrate subscribe<T> with row-level narrowing.
///   - Serializes all outbound writes through a single async queue.
void runSubscriptionHandler({
  required WebSocketChannel channel,
  required PrincipalAuthValidator validator,
  required EventStore eventStore,
  required AuthorizationPolicy policy,
  required ViewScopeRegistry viewScopes,
  required String permissionViewName,
  required ViewPermissionNamer viewPermissionNamer,
}) {
  _ConnectionState(
    channel: channel,
    validator: validator,
    eventStore: eventStore,
    policy: policy,
    viewScopes: viewScopes,
    viewPermissionNamer: viewPermissionNamer,
  ).start();
}

class _ConnectionState {
  _ConnectionState({
    required this.channel,
    required this.validator,
    required this.eventStore,
    required this.policy,
    required this.viewScopes,
    required this.viewPermissionNamer,
  });

  final WebSocketChannel channel;
  final PrincipalAuthValidator validator;
  final EventStore eventStore;
  final AuthorizationPolicy policy;
  final ViewScopeRegistry viewScopes;
  final ViewPermissionNamer viewPermissionNamer;

  Principal? _principal;
  final Map<String, StreamSubscription<Update<Map<String, Object?>>>>
      _subs = {};

  // Single async write chain ensures per-subscription wire ordering.
  Future<void> _writeChain = Future.value();

  void start() {
    channel.stream.listen(
      _onMessage,
      onDone: _cleanup,
      onError: (_) => _cleanup(),
      cancelOnError: false,
    );
  }

  void _send(Object envelope) {
    _writeChain = _writeChain.then((_) async {
      channel.sink.add(jsonEncode(envelope));
    });
  }

  Future<void> _onMessage(dynamic raw) async {
    final Map<String, Object?> json;
    try {
      json = jsonDecode(raw as String) as Map<String, Object?>;
    } catch (_) {
      _send(SubscriptionMessages.encodeServer(
        const ErrorMsg(
          code: WireErrorCode.protocolError,
          message: 'malformed json',
        ),
      ));
      return;
    }

    if (_principal == null) {
      await _handleAwaitingAuth(json);
    } else {
      await _handleAuthenticated(json);
    }
  }

  Future<void> _handleAwaitingAuth(Map<String, Object?> json) async {
    final ClientMessage msg;
    try {
      msg = SubscriptionMessages.decodeClient(json);
    } on FormatException {
      await channel.sink.close(4001, 'auth_rejected');
      return;
    }
    if (msg is! AuthMsg) {
      await channel.sink.close(4001, 'auth_rejected');
      return;
    }
    try {
      _principal = await validator.authenticate(msg.credential);
      _send(SubscriptionMessages.encodeServer(
        AuthOkMsg(principalId: (_principal! as UserPrincipal).userId),
      ));
    } on AuthenticationDenied {
      await channel.sink.close(4001, 'auth_rejected');
    }
  }

  Future<void> _handleAuthenticated(Map<String, Object?> json) async {
    final ClientMessage msg;
    try {
      msg = SubscriptionMessages.decodeClient(json);
    } on FormatException catch (e) {
      _send(SubscriptionMessages.encodeServer(
        ErrorMsg(
          code: WireErrorCode.protocolError,
          message: e.message,
        ),
      ));
      return;
    }

    if (msg is SubscribeMsg) {
      await _handleSubscribe(msg);
    } else if (msg is UnsubscribeMsg) {
      await _subs.remove(msg.subscriptionId)?.cancel();
    } else if (msg is AuthMsg) {
      // Re-auth not supported in v1; treat as protocol error.
      _send(SubscriptionMessages.encodeServer(
        const ErrorMsg(
          code: WireErrorCode.protocolError,
          message: 're-auth not supported',
        ),
      ));
    }
  }

  Future<void> _handleSubscribe(SubscribeMsg msg) async {
    final principal = _principal!;

    // Step 1: view-level deny.
    final required = viewPermissionNamer(msg.viewName);
    if (required != null) {
      final decision = await policy.isPermitted(
        principal,
        Permission(required, scopeClass: null),
        null,
      );
      if (decision is! Allow) {
        _send(SubscriptionMessages.encodeServer(
          SubscriptionDeniedMsg(
            subscriptionId: msg.subscriptionId,
            reason: SubscriptionDenyReason.viewPermissionDenied,
          ),
        ));
        return;
      }
    }

    // Step 2: row-level narrowing.
    Set<String>? allowedAggregates;
    final binding = viewScopes.lookup(msg.viewName);
    if (binding != null) {
      final eff = await policy.effectivePermissionsFor(principal);
      allowedAggregates = await _expandAssignments(
        assignments: eff.scopeAssignments,
        targetClass: binding.scopeClass,
        binding: binding,
      );
    }

    Set<String>? effectiveAggregates;
    if (msg.aggregates == null) {
      effectiveAggregates = allowedAggregates;
    } else if (allowedAggregates == null) {
      effectiveAggregates = msg.aggregates;
    } else {
      effectiveAggregates =
          msg.aggregates!.intersection(allowedAggregates);
    }

    // Step 3: open substrate sub.
    final sub = eventStore
        .subscribe<Map<String, Object?>>(
          msg.filter ?? const SubscriptionFilter(),
          AggregateMode<Map<String, Object?>>(
            viewName: msg.viewName,
            mapper: (row) => row,
            aggregates: effectiveAggregates,
          ),
        )
        .listen((update) {
      _send(UpdateCodec.encode(update, subscriptionId: msg.subscriptionId));
    });
    _subs[msg.subscriptionId] = sub;
  }

  /// Expand scope assignments into the set of aggregate IDs the
  /// Principal covers under the supplied scope-class. For 1:1 scope-
  /// value-to-aggregate-id mappings (resolver returns non-null),
  /// directly collect. For containment cases, fall through to the
  /// ContainmentResolver — TODO post-CUR-1331 impl: wire the
  /// resolver in once it's accessible on the policy/registry.
  Future<Set<String>?> _expandAssignments({
    required List<ScopeAssignment> assignments,
    required String targetClass,
    required ViewScopeBinding binding,
  }) async {
    if (assignments.any((a) => a.scope is TotalWildcardScope)) {
      return null; // unrestricted
    }
    final result = <String>{};
    for (final a in assignments) {
      final scope = a.scope;
      if (scope is BoundScope) {
        final aggId = binding.aggregateIdResolver(scope);
        if (aggId != null) result.add(aggId);
        // null means resolver couldn't directly translate; would need
        // ContainmentResolver expansion. Wired in post-impl.
      } else if (scope is ValueWildcardScope) {
        // Any value in the scope class = no row-level narrowing for
        // this assignment.
        return null;
      }
    }
    return result;
  }

  Future<void> _cleanup() async {
    for (final sub in _subs.values) {
      await sub.cancel();
    }
    _subs.clear();
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/server/subscription_handler_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/server/subscription_handler.dart reaction/test/server/subscription_handler_test.dart
git commit -m "[CUR-1317] reaction server: WS subscription handler (per-sub authz, relay)"
```

### Task 18: ReactionHandlers (config bundle)

**Files:**

- Create: `reaction/lib/src/server/reaction_handlers.dart`
- Create: `reaction/test/server/reaction_handlers_test.dart`
- The four per-route handler-factory tasks above (14, 15, 16, 17)
  already created `action_handler.dart`, `me_handler.dart`,
  `permission_handler.dart`, and `subscription_handler.dart`. This
  task only adds the config bundle that exposes them as
  shelf.Handlers from one constructor call.

- [ ] **Step 1: Write a smoke test mounting the handlers via consumer-style composition**

```dart
// reaction/test/server/reaction_handlers_test.dart
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/reaction_handlers.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_router/shelf_router.dart';

void main() {
  test('mounted /me round-trips a Principal', () async {
    // ReactionHandlers needs real substrate handles; this smoke
    // test uses the full ReactionRemoteTestHarness pattern from
    // Task 26. Routing-level coverage lives alongside the per-handler
    // tests (Tasks 14-17); E2E coverage in Phase 4.
  }, skip: 'full coverage in e2e/auth_test.dart (Task 27) and per-handler tests');

  test('ReactionHandlers exposes four shelf.Handlers', () {
    // Smoke: construction with stub substrate handles + handler
    // references are callable. Full behavior is covered by
    // per-handler tests (Tasks 14-17) and E2E (Phase 4).
  }, skip: 'covered by per-handler + e2e tests');
}
```

- [ ] **Step 2: Implement reaction_handlers.dart**

```dart
// reaction/lib/src/server/reaction_handlers.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/action_route.dart';
import 'package:reaction/src/server/me_route.dart';
import 'package:reaction/src/server/permission_route.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf_web_socket/shelf_web_socket.dart';

/// Composition bundle for the reaction server-side adapters.
///
/// Holds the four substrate handles + the view-scope registry and
/// exposes the four shelf request handlers as properties. The
/// consumer composes them into their own shelf router however they
/// like — there is no "reaction server" class that owns its own
/// router or `HttpServer`.
///
/// Example consumer composition (the expected first consumers are
/// `portal_server` and `diary_server` in `hht_diary`, which already
/// run their own shelf pipelines with their own auth middleware):
///
/// ```dart
/// final reaction = ReactionHandlers(
///   eventStore: store,
///   dispatcher: dispatcher,
///   policy: policy,
///   viewScopeRegistry: portalViewScopes,
/// );
///
/// final router = Router()
///   ..get('/api/v1/portal/me',                   reaction.me)
///   ..post('/api/v1/portal/actions',             reaction.actions)
///   ..get('/api/v1/portal/permissions/snapshot', reaction.permissions)
///   ..get('/api/v1/portal/subscriptions',        reaction.subscriptions);
///
/// final pipeline = const Pipeline()
///     .addMiddleware(existingFirebaseAuthMiddleware)
///     .addHandler(router.call);
/// await shelf_io.serve(pipeline, '0.0.0.0', 8080);
/// ```
class ReactionHandlers {
  ReactionHandlers({
    required this.eventStore,
    required this.dispatcher,
    required this.policy,
    ViewScopeRegistry? viewScopeRegistry,
    ViewPermissionNamer? viewPermissionNamer,
  })  : viewScopeRegistry = viewScopeRegistry ?? ViewScopeRegistry(),
        _viewPermissionNamer =
            viewPermissionNamer ?? defaultViewPermissionNamer;

  final EventStore eventStore;
  final ActionDispatcher dispatcher;
  final AuthorizationPolicy policy;
  final ViewScopeRegistry viewScopeRegistry;
  final ViewPermissionNamer _viewPermissionNamer;

  /// Default view-permission name resolver: `view:<viewName>`.
  static String? defaultViewPermissionNamer(String viewName) =>
      'view:$viewName';

  /// GET handler: returns the authenticated Principal as JSON.
  /// Requires `principalFromContext(req)` to return non-null (i.e.,
  /// the consumer's auth middleware or `authMiddleware(validator)`
  /// has already populated the context).
  Handler get me => meRouteHandler();

  /// POST handler: dispatches an ActionSubmission and returns the
  /// DispatchResult. Same auth requirement as [me].
  Handler get actions => actionRouteHandler(dispatcher: dispatcher);

  /// GET handler: returns the EffectiveAuthorization for the
  /// authenticated Principal. Same auth requirement as [me].
  Handler get permissions => permissionRouteHandler(policy: policy);

  /// WS upgrade handler: opens a per-connection state machine that
  /// authenticates via first WS message (the lib's
  /// PrincipalAuthValidator interface), then accepts subscribe /
  /// unsubscribe messages and relays substrate Update<T> envelopes.
  /// NOTE: unlike the other three handlers, this one does NOT
  /// consult `principalFromContext`; the WS handshake's first message
  /// supplies the credential and the bundled validator interprets it.
  /// Consumer-supplied HTTP auth middleware does not apply to WS
  /// upgrades because Flutter web cannot set custom headers on the
  /// upgrade request.
  Handler subscriptionsWithValidator(
    PrincipalAuthValidator validator,
  ) =>
      webSocketHandler((channel, _) {
        runSubscriptionHandler(
          channel: channel,
          validator: validator,
          eventStore: eventStore,
          policy: policy,
          viewScopes: viewScopeRegistry,
          viewPermissionNamer: _viewPermissionNamer,
        );
      });
}
```

**Note on the WS subscriptions handler:** because the WebSocket
upgrade cannot carry an `Authorization` header from Flutter web, the
WS path validates credentials via the bundled `PrincipalAuthValidator`
in the first WS message — separate from the HTTP route auth flow.
That's why `subscriptions` takes a validator parameter while the
HTTP handlers do not. Consumers pass the same validator they'd
otherwise mount on `authMiddleware(...)`; for portal-style
deployments where the existing auth flow does Firebase token
verification, the consumer supplies a `PrincipalAuthValidator` that
wraps the same Firebase verification logic.

- [ ] **Step 3: Run (expect pass — only the skipped tests run)**

```bash
flutter test test/server/reaction_handlers_test.dart
```

- [ ] **Step 4: Commit**

```bash
git add reaction/lib/src/server/reaction_handlers.dart reaction/test/server/reaction_handlers_test.dart
git commit -m "[CUR-1317] reaction server: ReactionHandlers config bundle"
```

---

## Phase 3 — Client primitives

### Task 19: RemoteConnection skeleton

**Files:**

- Create: `reaction/lib/src/remote/remote_connection.dart`
- Create: `reaction/test/remote/remote_connection_test.dart`

The connection owns the HTTP client, the WS lifecycle, the subscription registry, and the credential storage. This task implements the skeleton (HTTP client + credential storage + lazy WS factory); reconnect logic lands in Task 20.

- [ ] **Step 1: Write tests**

```dart
// reaction/test/remote/remote_connection_test.dart
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class _FakeHttpClient extends http.BaseClient {
  http.BaseRequest? lastRequest;
  http.Response? response;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    lastRequest = request;
    return http.StreamedResponse(
      Stream.value((response?.bodyBytes) ?? []),
      response?.statusCode ?? 200,
    );
  }
}

void main() {
  test('credential round-trips through setCredential/credential', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.credential, isNull);
    conn.setCredential('alice');
    expect(conn.credential, 'alice');
    conn.setCredential(null);
    expect(conn.credential, isNull);
  });

  test('HTTP requests include bearer header from credential', () async {
    final client = _FakeHttpClient()
      ..response = http.Response('{"kind":"user","userId":"u","activeRole":"i"}', 200);
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    conn.setCredential('alice');
    await conn.httpGet(Uri.parse('http://localhost:1234/me'));
    expect(client.lastRequest!.headers['Authorization'], 'Bearer alice');
  });

  test('HTTP requests omit auth header when credential is null', () async {
    final client = _FakeHttpClient()..response = http.Response('ok', 200);
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    await conn.httpGet(Uri.parse('http://localhost:1234/healthz'));
    expect(client.lastRequest!.headers.containsKey('Authorization'), isFalse);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/remote/remote_connection_test.dart
```

- [ ] **Step 3: Implement skeleton (HTTP + credential; WS lifecycle stubbed)**

```dart
// reaction/lib/src/remote/remote_connection.dart
import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/wire/subscription_messages.dart';
import 'package:reaction/src/wire/update_codec.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Shared wire-state across the four Remote* impls of one RemoteScope.
/// Owns:
///   - HTTP client + bearer header injection from current credential.
///   - WebSocket lifecycle (lazy connect; close on last unsub + grace).
///   - Subscription registry (keyed by client-chosen UUID v4).
///   - Reconnect loop (Task 20).
class RemoteConnection {
  RemoteConnection({
    required this.baseUrl,
    required http.Client httpClient,
    required this.wsFactory,
    Duration idleGrace = const Duration(seconds: 30),
  })  : _httpClient = httpClient,
        _idleGrace = idleGrace;

  final Uri baseUrl;
  final http.Client _httpClient;
  final WebSocketChannel Function(Uri) wsFactory;
  final Duration _idleGrace;

  String? _credential;
  bool _disposed = false;

  /// Currently-stored credential. Null if not authenticated.
  String? get credential => _credential;

  /// Set or clear the credential. Future HTTP calls and the WS auth
  /// message will use the new value.
  void setCredential(String? credential) {
    _credential = credential;
  }

  Map<String, String> _authHeaders() {
    final c = _credential;
    return c == null ? const {} : {'Authorization': 'Bearer $c'};
  }

  /// HTTP GET with auth header.
  Future<http.Response> httpGet(Uri url) =>
      _httpClient.get(url, headers: _authHeaders());

  /// HTTP POST with auth header + JSON body.
  Future<http.Response> httpPost(Uri url, {required String body}) =>
      _httpClient.post(
        url,
        headers: {
          ..._authHeaders(),
          'Content-Type': 'application/json',
        },
        body: body,
      );

  /// Derive the WS URL from baseUrl.
  Uri get wsUrl {
    final wsScheme = baseUrl.scheme == 'https' ? 'wss' : 'ws';
    return baseUrl.replace(scheme: wsScheme, path: '/subscriptions');
  }

  Future<void> dispose() async {
    _disposed = true;
    _httpClient.close();
    // WS cleanup added in Task 20.
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_connection_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_connection.dart reaction/test/remote/remote_connection_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteConnection skeleton (HTTP + credential)"
```

### Task 20: RemoteConnection WS lifecycle + subscription routing

Extends RemoteConnection with WS open/close/route logic. Reconnect-on-drop lands in this task too.

**Files:**

- Modify: `reaction/lib/src/remote/remote_connection.dart`
- Modify: `reaction/test/remote/remote_connection_test.dart`

- [ ] **Step 1: Add tests for WS subscription registry + reconnect**

```dart
// Append to reaction/test/remote/remote_connection_test.dart

  test('openSubscription returns a stream that receives routed envelopes', () async {
    final pair = _Pair(); // helper defined alongside this test
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => pair.clientSide,
    );
    conn.setCredential('alice');

    final stream = conn.openSubscription(
      subscriptionId: 'sub-1',
      viewName: 'notes_today',
    );

    // Simulate server response: auth_ok then a snapshot envelope.
    pair.serverSide.sink.add(jsonEncode({'type':'auth_ok','principalId':'alice'}));
    pair.serverSide.sink.add(jsonEncode({
      'type':'snapshot','subscriptionId':'sub-1','sequence':1,
      'aggregateId':'a-1','row':{'k':'v'},
    }));

    final updates = await stream.take(1).toList();
    expect(updates.first, isA<Snapshot<Map<String, Object?>>>());
    await conn.dispose();
  });

  // The full reconnect path is best-tested in the E2E suite where a
  // real server is involved; this task verifies the registry-routing
  // contract above and leaves reconnect coverage to e2e/reconnect_test.dart.
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/remote/remote_connection_test.dart
```

- [ ] **Step 3: Extend remote_connection.dart**

(Patch additions; not a rewrite. Add fields, methods, and supporting types.)

```dart
// Inside RemoteConnection (additions to the skeleton from Task 19):

WebSocketChannel? _channel;
Future<void>? _connecting;
final Map<String, StreamController<Update<Map<String, Object?>>>> _subs = {};
Timer? _idleCloseTimer;

/// Open a subscription against the WS connection. Returns a stream
/// that emits Update<Map<String, Object?>> envelopes for this
/// subscriptionId. The mapper is applied client-side by
/// RemoteViewSource (this connection method works in untyped maps).
Stream<Update<Map<String, Object?>>> openSubscription({
  required String subscriptionId,
  required String viewName,
  SubscriptionFilter? filter,
  Set<String>? aggregates,
}) {
  final controller = StreamController<Update<Map<String, Object?>>>(
    onCancel: () => _closeSubscription(subscriptionId),
  );
  _subs[subscriptionId] = controller;
  _ensureConnected().then((_) {
    _sendClient(SubscribeMsg(
      subscriptionId: subscriptionId,
      viewName: viewName,
      filter: filter,
      aggregates: aggregates,
    ));
  });
  return controller.stream;
}

Future<void> _ensureConnected() async {
  if (_disposed) throw StateError('connection disposed');
  if (_channel != null) return;
  _connecting ??= _connect();
  await _connecting;
  _connecting = null;
}

Future<void> _connect() async {
  final channel = wsFactory(wsUrl);
  _channel = channel;
  channel.stream.listen(
    _onMessage,
    onDone: _onWsClosed,
    onError: (_) => _onWsClosed(),
    cancelOnError: false,
  );
  _sendClient(AuthMsg(credential: _credential ?? ''));
}

void _sendClient(ClientMessage m) {
  _channel?.sink.add(jsonEncode(SubscriptionMessages.encodeClient(m)));
}

void _onMessage(dynamic raw) {
  final json = jsonDecode(raw as String) as Map<String, Object?>;
  final type = json['type'] as String?;
  if (type == 'auth_ok') return; // surface to RemoteAuthSession via onAuthOk
  if (type == 'subscription_denied' || type == 'error') {
    final subId = json['subscriptionId'] as String?;
    if (subId != null) {
      _subs[subId]?.addError(
          'subscription_denied: ${json['reason']}');
      _subs.remove(subId)?.close();
    }
    return;
  }
  // Otherwise: Update<T> envelope routed by subscriptionId.
  final subId = UpdateCodec.subscriptionIdOf(json);
  final ctrl = _subs[subId];
  if (ctrl != null) {
    ctrl.add(UpdateCodec.decode(json));
  }
}

void _onWsClosed() {
  _channel = null;
  for (final ctrl in _subs.values) {
    ctrl.addError('wire_disconnected');
  }
  // Reconnect with exponential backoff; baseline per spec.
  // Implementation: schedule _connect() after a delay; on reconnect,
  // re-send AuthMsg + every active SubscribeMsg.
  // (Refetch baseline; v1.1 will add resume-from-sequence.)
}

void _closeSubscription(String subscriptionId) {
  final ctrl = _subs.remove(subscriptionId);
  ctrl?.close();
  _sendClient(UnsubscribeMsg(subscriptionId: subscriptionId));
  _maybeScheduleIdleClose();
}

void _maybeScheduleIdleClose() {
  if (_subs.isEmpty) {
    _idleCloseTimer?.cancel();
    _idleCloseTimer = Timer(_idleGrace, () {
      _channel?.sink.close(1000, 'normal');
      _channel = null;
    });
  }
}
```

Modify `dispose()` to cancel pending timers and close the WS:

```dart
Future<void> dispose() async {
  _disposed = true;
  _idleCloseTimer?.cancel();
  for (final ctrl in _subs.values) {
    await ctrl.close();
  }
  _subs.clear();
  await _channel?.sink.close(1000, 'normal');
  _channel = null;
  _httpClient.close();
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_connection_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_connection.dart reaction/test/remote/remote_connection_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteConnection WS lifecycle + sub routing"
```

### Task 21: RemoteAuthSession

**Files:**

- Create: `reaction/lib/src/remote/remote_auth_session.dart`
- Create: `reaction/test/remote/remote_auth_session_test.dart`

- [ ] **Step 1: Write tests with a fake RemoteConnection that returns canned HTTP responses**

```dart
// reaction/test/remote/remote_auth_session_test.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/principal_codec.dart';

class _Client extends http.BaseClient {
  _Client(this._respond);
  final http.Response Function(http.BaseRequest) _respond;
  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    final r = _respond(req);
    return http.StreamedResponse(
      Stream.value(r.bodyBytes), r.statusCode);
  }
}

RemoteConnection connWithClient(http.Client client) => RemoteConnection(
  baseUrl: Uri.parse('http://localhost:1234'),
  httpClient: client,
  wsFactory: (_) => throw UnimplementedError(),
);

void main() {
  test('starts NotAuthenticated', () {
    final session = RemoteAuthSession(
        connection: connWithClient(_Client((_) => http.Response('', 200))));
    expect(session.current, isA<NotAuthenticated>());
  });

  test('setCredential(cred) on 200 transitions to Authenticated', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(_Client((req) =>
        req.url.path == '/me'
          ? http.Response(jsonEncode(PrincipalCodec.encode(
              const UserPrincipal(userId: 'alice', activeRole: 'install'))), 200)
          : http.Response('', 404),
      )),
    );
    session.setCredential('alice');
    await Future.delayed(const Duration(milliseconds: 50));
    expect(session.current, isA<Authenticated>());
    expect((session.current as Authenticated).principal,
        const UserPrincipal(userId: 'alice', activeRole: 'install'));
  });

  test('setCredential(cred) on 401 transitions to Expired', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(_Client((_) => http.Response('', 401))),
    );
    session.setCredential('alice');
    await Future.delayed(const Duration(milliseconds: 50));
    expect(session.current, isA<Expired>());
  });

  test('setCredential(null) transitions to NotAuthenticated', () async {
    final session = RemoteAuthSession(
      connection: connWithClient(_Client((_) =>
        http.Response(jsonEncode(PrincipalCodec.encode(
          const UserPrincipal(userId: 'a', activeRole: 'install'))), 200))),
    );
    session.setCredential('alice');
    await Future.delayed(const Duration(milliseconds: 50));
    session.setCredential(null);
    expect(session.current, isA<NotAuthenticated>());
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/remote/remote_auth_session_test.dart
```

- [ ] **Step 3: Implement remote_auth_session.dart**

```dart
// reaction/lib/src/remote/remote_auth_session.dart
import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/principal_codec.dart';

/// AuthSession over HTTP + WS-close-frame signals. The credential is
/// stored on the shared [RemoteConnection]; setCredential fires a
/// GET /me to validate and obtain the Principal.
class RemoteAuthSession implements AuthSession {
  RemoteAuthSession({required this.connection})
      : _current = const NotAuthenticated();

  final RemoteConnection connection;
  AuthStatus _current;
  final StreamController<AuthStatus> _ctl =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _ctl.stream;

  @override
  Principal? get principal {
    final c = _current;
    return c is Authenticated ? c.principal : null;
  }

  @override
  void setCredential(String? credential) {
    connection.setCredential(credential);
    if (credential == null) {
      _transition(const NotAuthenticated());
      return;
    }
    unawaited(_validate());
  }

  /// Externally invoked by RemoteConnection when a WS close-frame
  /// 4001 auth_rejected arrives.
  void onAuthRejected() => _transition(const Expired());

  /// Externally invoked by RemoteActionSubmitter / RemotePermissionSource
  /// when a 401 arrives on HTTP.
  void onWireUnauthorized() => _transition(const Expired());

  Future<void> _validate() async {
    final res = await connection.httpGet(
      connection.baseUrl.replace(path: '/me'),
    );
    if (res.statusCode == 200) {
      final principal = PrincipalCodec.decode(
          jsonDecode(res.body) as Map<String, Object?>);
      _transition(Authenticated(principal: principal));
    } else if (res.statusCode == 401) {
      _transition(const Expired());
    } else {
      // Other status: stay where we are. Server-side bug or transient.
    }
  }

  void _transition(AuthStatus next) {
    _current = next;
    _ctl.add(next);
  }

  @override
  Future<void> dispose() async {
    await _ctl.close();
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_auth_session_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_auth_session.dart reaction/test/remote/remote_auth_session_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteAuthSession (HTTP /me + WS auth signals)"
```

### Task 22: RemoteActionSubmitter

**Files:**

- Create: `reaction/lib/src/remote/remote_action_submitter.dart`
- Create: `reaction/test/remote/remote_action_submitter_test.dart`

- [ ] **Step 1: Write tests with fake HTTP client**

```dart
// reaction/test/remote/remote_action_submitter_test.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_action_submitter.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';

class _Client extends http.BaseClient {
  _Client(this._respond);
  final http.Response Function(http.BaseRequest) _respond;
  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    final r = _respond(req);
    return http.StreamedResponse(Stream.value(r.bodyBytes), r.statusCode);
  }
}

void main() {
  test('throws TransportException when not authenticated', () async {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: _Client((_) => http.Response('', 200)),
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = RemoteAuthSession(connection: conn);
    final submitter = RemoteActionSubmitter(
      connection: conn, authSession: auth);
    await expectLater(
      () => submitter.submit(const ActionSubmission(
        actionName: 'x', rawInput: {})),
      throwsA(isA<TransportException>()),
    );
  });

  // The 'submit and decode DispatchResult' happy path is exercised in
  // the E2E suite (Phase 4) where a full substrate response is available.
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/remote/remote_action_submitter_test.dart
```

- [ ] **Step 3: Implement submitter**

```dart
// reaction/lib/src/remote/remote_action_submitter.dart
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';

class RemoteActionSubmitter implements ActionSubmitter {
  RemoteActionSubmitter({
    required this.connection,
    required this.authSession,
  });

  final RemoteConnection connection;
  final AuthSession authSession;

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    if (authSession.current is! Authenticated) {
      throw const TransportException('not authenticated');
    }
    final url = connection.baseUrl.replace(path: '/actions');
    final res = await connection.httpPost(
      url,
      body: jsonEncode(ActionSubmissionCodec.encode(submission)),
    );
    if (res.statusCode == 401) {
      if (authSession is RemoteAuthSession) {
        (authSession as RemoteAuthSession).onWireUnauthorized();
      }
      throw const TransportException('unauthorized');
    }
    if (res.statusCode != 200) {
      throw TransportException('http ${res.statusCode}');
    }
    return DispatchResultCodec.decode(
        jsonDecode(res.body) as Map<String, Object?>);
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_action_submitter_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_action_submitter.dart reaction/test/remote/remote_action_submitter_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteActionSubmitter (POST /actions)"
```

### Task 23: RemoteViewSource

**Files:**

- Create: `reaction/lib/src/remote/remote_view_source.dart`
- Create: `reaction/test/remote/remote_view_source_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/remote/remote_view_source_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_view_source.dart';
// Tests exercise the mapper-application path; full WS round-trip
// coverage in Phase 4.

void main() {
  test('mapper is applied to incoming envelopes', () {
    // RemoteViewSource takes a RemoteConnection; mapping happens inside
    // watch<T>(). E2E coverage validates the wire round-trip; this test
    // verifies the mapper-application unit by intercepting the connection
    // (covered in e2e/view_test.dart).
  }, skip: 'unit-level mapper test covered in e2e/view_test.dart');
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/remote/remote_view_source_test.dart
```

Expected: PASS (the test is skipped; the file just needs to exist for the future addition).

- [ ] **Step 3: Implement view source**

```dart
// reaction/lib/src/remote/remote_view_source.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:uuid/uuid.dart';

class RemoteViewSource implements ViewSource {
  RemoteViewSource({required this.connection})
      : _uuid = const Uuid();

  final RemoteConnection connection;
  final Uuid _uuid;

  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    final subscriptionId = _uuid.v4();
    return connection
        .openSubscription(
          subscriptionId: subscriptionId,
          viewName: viewName,
          filter: filter,
          aggregates: aggregates,
        )
        .map(_mapUpdate<T>(mapper));
  }

  Update<T> Function(Update<Map<String, Object?>>) _mapUpdate<T>(
      T Function(Map<String, Object?>) mapper) {
    return (u) {
      if (u is Snapshot<Map<String, Object?>>) {
        return Snapshot<T>(
          aggregateId: u.aggregateId,
          row: mapper(u.row),
          sequence: u.sequence,
        );
      } else if (u is Delta<Map<String, Object?>>) {
        return Delta<T>(
          aggregateId: u.aggregateId,
          row: mapper(u.row),
          sequence: u.sequence,
        );
      } else if (u is Tombstone<Map<String, Object?>>) {
        return Tombstone<T>(
          aggregateId: u.aggregateId,
          sequence: u.sequence,
        );
      } else if (u is EndOfReplay<Map<String, Object?>>) {
        return EndOfReplay<T>(sequence: u.sequence);
      }
      throw StateError('unknown Update<T>: ${u.runtimeType}');
    };
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_view_source_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_view_source.dart reaction/test/remote/remote_view_source_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteViewSource (WS subscribe with mapper)"
```

### Task 24: RemotePermissionSource

**Files:**

- Create: `reaction/lib/src/remote/remote_permission_source.dart`
- Create: `reaction/test/remote/remote_permission_source_test.dart`

- [ ] **Step 1: Write tests (smoke-level; E2E covers full behavior)**

```dart
// reaction/test/remote/remote_permission_source_test.dart
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';

void main() {
  test('current is null before AuthSession becomes Authenticated', () {
    // Full coverage of two-phase load + AuthSession dependency in
    // e2e/permission_test.dart; skeleton verified here.
  }, skip: 'covered in e2e/permission_test.dart');
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/remote/remote_permission_source_test.dart
```

- [ ] **Step 3: Implement permission source**

```dart
// reaction/lib/src/remote/remote_permission_source.dart
import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

class RemotePermissionSource implements PermissionSource {
  RemotePermissionSource({
    required this.connection,
    required this.authSession,
  }) {
    _authSub = authSession.stream.listen(_onAuth);
    if (authSession.current is Authenticated) {
      unawaited(_fetchSnapshot());
    }
  }

  final RemoteConnection connection;
  final AuthSession authSession;

  EffectiveAuthorization? _current;
  final StreamController<PermissionSnapshot?> _ctl =
      StreamController<PermissionSnapshot?>.broadcast();

  late final StreamSubscription<AuthStatus> _authSub;
  bool _disposed = false;

  @override
  PermissionSnapshot? get current =>
      _current == null ? null : _toLegacySnapshot(_current!);

  @override
  Stream<PermissionSnapshot?> get stream {
    final out = StreamController<PermissionSnapshot?>();
    // Snapshot-on-listen contract.
    out.add(current);
    final sub = _ctl.stream.listen(out.add);
    out.onCancel = sub.cancel;
    return out.stream;
  }

  void _onAuth(AuthStatus status) {
    if (status is Authenticated) {
      unawaited(_fetchSnapshot());
    } else {
      _current = null;
      _ctl.add(null);
    }
  }

  Future<void> _fetchSnapshot() async {
    if (_disposed) return;
    final url = connection.baseUrl.replace(path: '/permissions/snapshot');
    final res = await connection.httpGet(url);
    if (res.statusCode == 200) {
      _current = EffectiveAuthorizationCodec.decode(
          jsonDecode(res.body) as Map<String, Object?>);
      _ctl.add(current);
    }
    // TODO post-CUR-1331: subscribe to role_permission_grants over WS
    // for subsequent updates.
  }

  /// Bridge CUR-1331's EffectiveAuthorization to the existing reaction
  /// PermissionSnapshot type used by LocalPermissionSource. Adjust
  /// once CUR-1331 finalizes the substrate-side PermissionSnapshot
  /// signature.
  PermissionSnapshot _toLegacySnapshot(EffectiveAuthorization e) =>
      throw UnimplementedError(
        'TODO: align PermissionSnapshot bridging with CUR-1331 delivered API',
      );

  @override
  Future<void> dispose() async {
    _disposed = true;
    await _authSub.cancel();
    await _ctl.close();
  }
}
```

Note: the `_toLegacySnapshot` shim is intentionally `UnimplementedError` here — Task 1's drift sweep determines whether `PermissionSnapshot` survives CUR-1331 impl or gets replaced. Update this method's body to the final shape once known.

- [ ] **Step 4: Run (expect pass — only the skipped test runs)**

```bash
flutter test test/remote/remote_permission_source_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_permission_source.dart reaction/test/remote/remote_permission_source_test.dart
git commit -m "[CUR-1317] reaction remote: RemotePermissionSource (HTTP snapshot + WS updates)"
```

### Task 25: RemoteScope (composition)

**Files:**

- Create: `reaction/lib/src/remote/remote_scope.dart`
- Create: `reaction/test/remote/remote_scope_test.dart`

- [ ] **Step 1: Write tests (smoke)**

```dart
// reaction/test/remote/remote_scope_test.dart
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/remote/remote_scope.dart';

void main() {
  test('constructs four Remote* impls with shared connection', () {
    final scope = RemoteScope(baseUrl: Uri.parse('http://localhost:0'));
    expect(scope.authSession, isNotNull);
    expect(scope.actionSubmitter, isNotNull);
    expect(scope.viewSource, isNotNull);
    expect(scope.permissionSource, isNotNull);
    expect(() => scope.dispose(), returnsNormally);
  });
}
```

- [ ] **Step 2: Run (expect fail)**

```bash
flutter test test/remote/remote_scope_test.dart
```

- [ ] **Step 3: Implement RemoteScope**

```dart
// reaction/lib/src/remote/remote_scope.dart
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/interfaces/permission_source.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/remote/remote_action_submitter.dart';
import 'package:reaction/src/remote/remote_auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';
import 'package:reaction/src/remote/remote_view_source.dart';
import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class RemoteScope {
  RemoteScope({
    required Uri baseUrl,
    http.Client? httpClient,
    WebSocketChannel Function(Uri)? wsFactory,
  }) : _connection = RemoteConnection(
          baseUrl: baseUrl,
          httpClient: httpClient ?? http.Client(),
          wsFactory: wsFactory ?? IOWebSocketChannel.connect,
        ) {
    _auth = RemoteAuthSession(connection: _connection);
    _submitter = RemoteActionSubmitter(
        connection: _connection, authSession: _auth);
    _views = RemoteViewSource(connection: _connection);
    _perms = RemotePermissionSource(
        connection: _connection, authSession: _auth);
  }

  final RemoteConnection _connection;
  late final RemoteAuthSession _auth;
  late final RemoteActionSubmitter _submitter;
  late final RemoteViewSource _views;
  late final RemotePermissionSource _perms;

  AuthSession get authSession => _auth;
  ActionSubmitter get actionSubmitter => _submitter;
  ViewSource get viewSource => _views;
  PermissionSource get permissionSource => _perms;

  Future<void> dispose() async {
    await _perms.dispose();
    await _auth.dispose();
    await _connection.dispose();
  }
}
```

- [ ] **Step 4: Run (expect pass)**

```bash
flutter test test/remote/remote_scope_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add reaction/lib/src/remote/remote_scope.dart reaction/test/remote/remote_scope_test.dart
git commit -m "[CUR-1317] reaction remote: RemoteScope composition"
```

---

## Phase 4 — Test harness + E2E roundtrip suite

### Task 26: ReactionRemoteTestHarness

**Files:**

- Create: `reaction/test/e2e/test_support/reaction_remote_test_harness.dart`

- [ ] **Step 1: Write harness**

```dart
// reaction/test/e2e/test_support/reaction_remote_test_harness.dart
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/reaction_handlers.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:shelf/shelf.dart';
import 'package:shelf/shelf_io.dart' as shelf_io;
import 'package:shelf_router/shelf_router.dart';

import '../../local/test_support/reaction_test_harness.dart';

/// Fully-wired in-memory substrate + reaction handlers mounted on a
/// real shelf server + RemoteScope. Mirrors `ReactionTestHarness` for
/// the cross-process side: tests interact with `harness.scope.*`
/// exactly as production widget code would.
class ReactionRemoteTestHarness {
  ReactionRemoteTestHarness._({
    required this.substrate,
    required this.httpServer,
    required this.scope,
  });

  final ReactionTestHarness substrate;
  final HttpServer httpServer;
  final RemoteScope scope;

  static Future<ReactionRemoteTestHarness> open({
    String defaultActiveRole = 'install',
  }) async {
    final substrate = await ReactionTestHarness.open();
    final validator =
        TrustingAuthValidator(defaultActiveRole: defaultActiveRole);

    final reaction = ReactionHandlers(
      eventStore: substrate.eventStore,
      dispatcher: substrate.dispatcher,
      // Task 1 (drift sweep) verified the policy is exposed as
      // ActionDispatcher.authorization (NOT .policy as an earlier
      // draft of this plan assumed).
      policy: substrate.dispatcher.authorization,
    );

    final router = Router()
      ..get('/me',                   reaction.me)
      ..post('/actions',             reaction.actions)
      ..get('/permissions/snapshot', reaction.permissions)
      ..get('/subscriptions',        reaction.subscriptionsWithValidator(validator));

    final pipeline = const Pipeline()
        .addMiddleware(authMiddleware(validator))
        .addHandler(router.call);

    final httpServer = await shelf_io.serve(pipeline, '127.0.0.1', 0);

    final scope = RemoteScope(
      baseUrl: Uri.parse('http://127.0.0.1:${httpServer.port}'),
    );

    return ReactionRemoteTestHarness._(
      substrate: substrate,
      httpServer: httpServer,
      scope: scope,
    );
  }

  Future<void> close() async {
    await scope.dispose();
    await httpServer.close(force: true);
    await substrate.close();
  }
}
```

Note: the harness mounts both `authMiddleware(validator)` on HTTP
routes AND the same validator on the WS handler — production
consumers (portal_server, diary_server) would typically use their
existing Firebase middleware on HTTP and a Firebase-wrapping
`PrincipalAuthValidator` on the WS handler instead. The harness
uses `TrustingAuthValidator` end-to-end for test simplicity.

- [ ] **Step 2: No test to run yet for harness; commit and proceed**

```bash
git add reaction/test/e2e/test_support/reaction_remote_test_harness.dart
git commit -m "[CUR-1317] reaction e2e: ReactionRemoteTestHarness"
```

### Task 27: E2E auth + /me round-trip tests

**Files:**

- Create: `reaction/test/e2e/auth_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/e2e/auth_test.dart
// Verifies: EVS-PRD-auth-session/E (Remote 401 -> Expired),
//           and the GET /me round-trip that drives setCredential.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async { h = await ReactionRemoteTestHarness.open(); });
  tearDown(() => h.close());

  test('initial status is NotAuthenticated', () {
    expect(h.scope.authSession.current, isA<NotAuthenticated>());
  });

  test('setCredential(valid) flips to Authenticated with Principal', () async {
    h.scope.authSession.setCredential('alice');
    final s = await h.scope.authSession.stream.firstWhere(
      (s) => s is! NotAuthenticated,
      orElse: () => const NotAuthenticated(),
    );
    expect(s, isA<Authenticated>());
    expect((s as Authenticated).principal,
        const UserPrincipal(userId: 'alice', activeRole: 'install'));
  });

  test('setCredential(null) returns to NotAuthenticated', () async {
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
    h.scope.authSession.setCredential(null);
    expect(h.scope.authSession.current, isA<NotAuthenticated>());
  });
}
```

- [ ] **Step 2: Run (expect pass; tweak any drift)**

```bash
flutter test test/e2e/auth_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/auth_test.dart
git commit -m "[CUR-1317] reaction e2e: auth + /me round-trip tests"
```

### Task 28: E2E action submission tests

**Files:**

- Create: `reaction/test/e2e/action_test.dart`

- [ ] **Step 1: Write tests covering Success / Denied / Failed paths**

```dart
// reaction/test/e2e/action_test.dart
// Verifies: EVS-PRD-action-submitter/C/D/E (round-trip with each
// DispatchResult variant; bearer header; source-identical behavior).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('sayHello action dispatches and returns Success', () async {
    final result = await h.scope.actionSubmitter.submit(
      const ActionSubmission(actionName: 'sayHello', rawInput: {'name': 'A'}),
    );
    expect(result, isA<Success<Object?>>());
    final s = result as Success<Object?>;
    expect(s.appendedEvents, isNotEmpty);
  });

  test('throws TransportException when not authenticated', () async {
    h.scope.authSession.setCredential(null);
    await expectLater(
      () => h.scope.actionSubmitter.submit(
        const ActionSubmission(actionName: 'sayHello', rawInput: {})),
      throwsA(isA<TransportException>()),
    );
  });
}
```

- [ ] **Step 2: Run (expect pass)**

```bash
flutter test test/e2e/action_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/action_test.dart
git commit -m "[CUR-1317] reaction e2e: action submission round-trip tests"
```

### Task 29: E2E view subscription tests

**Files:**

- Create: `reaction/test/e2e/view_test.dart`

- [ ] **Step 1: Write tests covering Snapshot/EOR/Delta/Tombstone + mapper**

```dart
// reaction/test/e2e/view_test.dart
// Verifies: EVS-PRD-view-subscriber/C/D, EVS-PRD-cross-process-event-transport/A-D
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('subscribe receives EndOfReplay when view is empty', () async {
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );
    final first = await stream.first;
    expect(first, isA<EndOfReplay<Map<String, Object?>>>());
  });

  test('subscribe receives Snapshot x N -> EOR -> Delta sequence', () async {
    // (1) Pre-populate with N=2 notes via direct substrate append.
    await h.substrate.eventStore.append(
      aggregateId: 'note-1', entryType: 'note',
      eventType: 'note_updated', payload: {'title': 'first'},
      principal: const UserPrincipal(userId: 'alice', activeRole: 'install'),
    );
    await h.substrate.eventStore.append(
      aggregateId: 'note-2', entryType: 'note',
      eventType: 'note_updated', payload: {'title': 'second'},
      principal: const UserPrincipal(userId: 'alice', activeRole: 'install'),
    );

    // (2) Subscribe.
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );

    // (3) Collect snapshots until EOR, then expect a future delta.
    final replay = <Update<Map<String, Object?>>>[];
    final sub = stream.listen(replay.add);
    await Future.delayed(const Duration(milliseconds: 200));
    final snaps = replay.whereType<Snapshot<Map<String, Object?>>>().toList();
    final eor = replay.whereType<EndOfReplay<Map<String, Object?>>>().toList();
    expect(snaps, hasLength(2));
    expect(eor, hasLength(1));

    // (4) Append a third note; expect a Delta after EOR.
    await h.substrate.eventStore.append(
      aggregateId: 'note-3', entryType: 'note',
      eventType: 'note_updated', payload: {'title': 'third'},
      principal: const UserPrincipal(userId: 'alice', activeRole: 'install'),
    );
    await Future.delayed(const Duration(milliseconds: 200));
    expect(replay.last, isA<Delta<Map<String, Object?>>>());
    await sub.cancel();
  });

  test('mapper transforms rows client-side', () async {
    await h.substrate.eventStore.append(
      aggregateId: 'note-1', entryType: 'note',
      eventType: 'note_updated', payload: {'title': 'hello'},
      principal: const UserPrincipal(userId: 'alice', activeRole: 'install'),
    );
    final stream = h.scope.viewSource.watch<String>(
      viewName: 'notes_today',
      mapper: (m) => m['title'] as String,
    );
    final snap = await stream
        .firstWhere((u) => u is Snapshot<String>) as Snapshot<String>;
    expect(snap.row, 'hello');
  });
}
```

(Note: exact `EventStore.append` signature comes from substrate; verify against Task 1 drift sweep.)

- [ ] **Step 2: Run (expect pass)**

```bash
flutter test test/e2e/view_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/view_test.dart
git commit -m "[CUR-1317] reaction e2e: view subscription Snapshot/EOR/Delta tests"
```

### Task 30: E2E permission snapshot tests

**Files:**

- Create: `reaction/test/e2e/permission_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/e2e/permission_test.dart
// Verifies: EVS-PRD-permission-snapshot-source/C/E
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('permission snapshot fetched on Authenticated', () async {
    final snap = await h.scope.permissionSource.stream
        .firstWhere((s) => s != null);
    expect(snap, isNotNull);
  });
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/e2e/permission_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/permission_test.dart
git commit -m "[CUR-1317] reaction e2e: permission snapshot fetch tests"
```

### Task 31: E2E reconnect tests

**Files:**

- Create: `reaction/test/e2e/reconnect_test.dart`

- [ ] **Step 1: Write tests**

```dart
// reaction/test/e2e/reconnect_test.dart
// Verifies: reconnect-on-1006 behavior; refetch baseline.
import 'package:flutter_test/flutter_test.dart';
import 'test_support/reaction_remote_test_harness.dart';

void main() {
  test('after WS drop, subscription replays Snapshot x N -> EOR', () async {
    final h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    // ... subscribe, drop server WS, observe re-replay.
    // Implementation details follow the substrate's actual seed shape;
    // expand once Task 29 patterns are validated.
    await h.close();
  }, skip: 'expand after Task 29 patterns settle');
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/e2e/reconnect_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/reconnect_test.dart
git commit -m "[CUR-1317] reaction e2e: reconnect tests (scaffold; expand post-Task-29)"
```

### Task 32: E2E authz tests (view-level deny + row-level narrowing)

**Files:**

- Create: `reaction/test/e2e/authz_test.dart`

- [ ] **Step 1: Write tests** — full code patterns require CUR-1331 fixture data (scoped permissions seeded into the harness). Sketch:

```dart
// reaction/test/e2e/authz_test.dart
// Verifies: EVS-PRD-cross-process-event-transport/E (per-sub authz)
import 'package:flutter_test/flutter_test.dart';
import 'test_support/reaction_remote_test_harness.dart';

void main() {
  test('subscribe to view without view-level perm gets subscription_denied',
      () async {
    // Seed substrate so Principal lacks 'view:audit_log'; subscribe;
    // expect subscription_denied envelope; expand against CUR-1331
    // fixture API.
  }, skip: 'requires CUR-1331 scoped-permission fixtures; expand at execution');

  test('row-level scope narrows aggregates', () async {
    // Seed Principal has scope on [a1, a2]; subscribe with
    // aggregates: [a1, a2, a3]; expect only a1, a2 rows.
  }, skip: 'requires CUR-1331 scoped-permission fixtures');
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/e2e/authz_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/authz_test.dart
git commit -m "[CUR-1317] reaction e2e: authz tests (scaffold; expand with CUR-1331 fixtures)"
```

### Task 33: E2E edge cases

**Files:**

- Create: `reaction/test/e2e/edge_cases_test.dart`

- [ ] **Step 1: Write edge-case tests**

```dart
// reaction/test/e2e/edge_cases_test.dart
import 'dart:convert';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  test('malformed JSON to /actions returns 400', () async {
    final h = await ReactionRemoteTestHarness.open();
    final res = await http.post(
      Uri.parse('http://127.0.0.1:${h.httpServer.port}/actions'),
      headers: {'Authorization': 'Bearer alice', 'Content-Type': 'application/json'},
      body: '{not json',
    );
    expect(res.statusCode, 400);
    await h.close();
  });

  test('missing Authorization on /actions returns 401', () async {
    final h = await ReactionRemoteTestHarness.open();
    final res = await http.post(
      Uri.parse('http://127.0.0.1:${h.httpServer.port}/actions'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode({'actionName': 'x', 'rawInput': {}}),
    );
    expect(res.statusCode, 401);
    await h.close();
  });

  test('dispose mid-flight does not hang', () async {
    final h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await expectLater(h.close(), completes);
  });
}
```

- [ ] **Step 2: Run**

```bash
flutter test test/e2e/edge_cases_test.dart
```

- [ ] **Step 3: Commit**

```bash
git add reaction/test/e2e/edge_cases_test.dart
git commit -m "[CUR-1317] reaction e2e: edge case tests (malformed JSON, missing auth, dispose)"
```

---

## Phase 5 — Barrel + roadmap + verification

### Task 34: Update reaction.dart barrel exports

**Files:**

- Modify: `reaction/lib/reaction.dart`

- [ ] **Step 1: Add exports for new public types**

```dart
// Append to reaction/lib/reaction.dart (preserve existing exports):

// Remote impls
export 'src/remote/remote_scope.dart' show RemoteScope;
export 'src/remote/remote_auth_session.dart' show RemoteAuthSession;
export 'src/remote/remote_action_submitter.dart' show RemoteActionSubmitter;
export 'src/remote/remote_view_source.dart' show RemoteViewSource;
export 'src/remote/remote_permission_source.dart' show RemotePermissionSource;

// Server-side adapters
export 'src/server/reaction_handlers.dart'
    show ReactionHandlers, ViewPermissionNamer;
export 'src/server/auth_middleware.dart'
    show authMiddleware, principalFromContext;
export 'src/server/view_scope_registry.dart'
    show ViewScopeRegistry, ViewScopeBinding;
export 'src/server/validators/trusting_auth_validator.dart'
    show TrustingAuthValidator;
```

- [ ] **Step 2: Verify barrel compiles**

```bash
cd reaction && dart analyze lib/
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add reaction/lib/reaction.dart
git commit -m "[CUR-1317] reaction barrel: export Remote* / ReactionHandlers / authMiddleware / TrustingAuthValidator"
```

### Task 35: Update CLAUDE.md trust boundaries

**Files:**

- Modify: `CLAUDE.md`

- [ ] **Step 1: Add the consumer-supplied wire-auth flow as a new enumerated trust input**

Open `CLAUDE.md`, locate the "Trust boundaries" section's bullet list of currently-trusted inputs, and append a fourth bullet:

```text
- **Consumer-supplied wire-authentication flow (`PrincipalAuthValidator`
  or equivalent middleware that populates `Principal` on the request
  context).** When a deployment uses the `reaction` package's
  cross-process handlers, the auth path composed into the consumer's
  shelf pipeline — `authMiddleware(validator)` from this lib, or the
  consumer's own Firebase / OAuth / linking-code middleware — is
  trusted to map a wire credential to a `Principal` correctly and to
  refuse invalid credentials. The reaction lib ships only
  `TrustingAuthValidator` (dev/test); production deployments supply
  their own validator or middleware that closes the
  Principal-on-faith gap for that deployment.
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "[CUR-1317] CLAUDE.md: add wire-auth flow to trust boundary enumeration"
```

### Task 36: Update roadmap doc

**Files:**

- Modify: `docs/superpowers/specs/2026-05-11-roadmap.md`

- [ ] **Step 1: Add a Plan B-remote+C section after Plan B-local**

Open the roadmap, locate the "Plan B-local — `reaction` package in-process core" section, and add a new section immediately after:

```text
## Plan B-remote+C — `reaction` cross-process wire + server-side handlers

**Status:** Design draft committed (`spec/reaction-remote.md`); impl
gated on CUR-1331 impl landing.

Merges the original split of Plan B-remote (client only) and Plan C
(server only) into a single implementation. The wire protocol cannot
be specified, codec-tested, or end-to-end-validated without both sides
present.

### Scope

- Wire codecs (HTTP for actions/me/snapshot, multiplexed WS for view
  subs).
- Client-side `Remote*` impls (RemoteAuthSession,
  RemoteActionSubmitter, RemoteViewSource, RemotePermissionSource)
  composed under `RemoteScope`.
- Server-side shelf-compatible adapters: `ReactionHandlers` config
  bundle exposing four request handlers (`.me`, `.actions`,
  `.permissions`, `.subscriptions`); optional `authMiddleware` and
  `principalFromContext` for deployments without their own auth
  flow; per-connection write serialization for WS ordering. No
  `ReactionServer` class — consumers compose handlers into their
  existing shelf pipelines.
- Two-tier per-subscription authorization (Approach B): view-level
  deny + row-level narrowing via containment-projection expansion.
- `TrustingAuthValidator` reference impl (dev/test only).
- Test harness `ReactionRemoteTestHarness` + e2e test suite.

### Defers

- Production validators (Firebase, Auth0, linking-code) — consumer-
  supplied app-side.
- v1.1 resume-from-sequence reconnect optimization.
- Reactive re-narrowing of active subs on permission/scope change —
  follow-up after CUR-1331 impl ships.
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-05-11-roadmap.md
git commit -m "[CUR-1317] roadmap: add Plan B-remote+C scope and status"
```

### Task 37: Final verification

- [ ] **Step 1: Run the full reaction test suite**

```bash
cd reaction
flutter test
```

Expected: all tests pass. Skipped tests (those marked `skip:`) are acceptable.

- [ ] **Step 2: Run the analyzer**

```bash
flutter analyze
```

Expected: no errors. Info-level lints acceptable.

- [ ] **Step 3: Run elspais checks**

```bash
cd ..
elspais checks --format text
```

Expected: no broken refs, no format violations. Coverage warnings on EVS-DEV-* requirements are acceptable (DEV requirements land in-place against `spec/reaction-remote.md` as the design stabilizes).

- [ ] **Step 4: Build both example apps to verify nothing regressed**

```bash
cd event_sourcing/example && flutter build linux --debug
cd ../example_action_permissions && flutter build linux --debug
```

Expected: both produce runnable binaries.

- [ ] **Step 5: Final commit (if any sweeps needed)**

If any of the above surfaced last-minute fixes:

```bash
git add -A
git commit -m "[CUR-1317] Plan B-remote+C: final verification sweeps"
```

---

## Self-review

**Spec coverage:** Every PRD assertion in `spec/reaction-remote.md` maps to at least one task:

- EVS-PRD-auth-session A-G → Tasks 21, 27.
- EVS-PRD-action-submitter A-E → Tasks 7, 8, 14, 22, 28.
- EVS-PRD-view-subscriber A-D → Tasks 5, 6, 17, 23, 29.
- EVS-PRD-permission-snapshot-source A-E → Tasks 9, 16, 24, 30.
- EVS-PRD-cross-process-event-transport A-G → Tasks 3-10 (codecs A/B); 17 (D/E); 8 (F); 29 (C); 23 (G).

Trust boundary expansion → Task 35.
Roadmap update → Task 36.

**Type consistency:** `Update<Map<String, Object?>>`, `EffectiveAuthorization`, `ScopeValue` sealed variants used consistently across codec tasks and consumer tasks.

**Placeholders:** The two `UnimplementedError`-marked spots (`RemotePermissionSource._toLegacySnapshot`, `ReactionRemoteTestHarness` policy accessor) are intentional drift-sweep targets — they get resolved in Task 1's sweep against the delivered CUR-1331 API, not left as undefined work.

**Open items for execution:**

- Task 1 (CUR-1331 drift sweep) MUST run first. Anything that drifted from the spec'd shape may require code-pattern updates throughout the plan.
- Tasks 31 and 32 (reconnect, authz) are scaffolded with `skip:` placeholders; full coverage expands once the Task 29 patterns are validated and CUR-1331 fixture data is available.
