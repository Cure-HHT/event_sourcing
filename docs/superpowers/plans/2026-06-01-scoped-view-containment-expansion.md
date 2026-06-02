# Scoped View Containment Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a principal whose scope assignment is at an *ancestor* class (e.g. an Investigator assigned `BoundScope('site', 'site-A')`) see exactly the descendant-class rows (e.g. participants at site-A) when subscribing to a scoped view — closing the read-path half of hierarchy-scoped permissions.

**Architecture:** Add a substrate `ScopeDescendantExpander` that walks the containment graph *downward* (breadth-first, multi-hop, fail-closed), mirroring the existing upward `ContainmentResolver`. Inject it as a callback into the reaction subscription handler so the currently-skipped `appliesViaAncestor` + `BoundScope` branch unions the expanded aggregate IDs into the frozen `AggregateMode.aggregates` allow-set. Add a `region → site → participant` GUI demo app. Option A only: the allow-set is computed once per subscribe (static); no cap; pagination deferred.

**Tech Stack:** Dart 3 (pure-Dart substrate `event_sourcing`, `reaction`), Flutter (`reaction_widgets`, the demo app), `test` / `flutter_test`, sembast (in-memory backend for the demo + tests), shelf (demo server).

**Design doc:** `docs/superpowers/specs/2026-06-01-scoped-view-containment-expansion-design.md`

**Conventions (from CLAUDE.md / memory):**
- Branch already in use for related work is `CUR-1317-naming-renames`; this feature is new work — create a branch `CUR-1317-scoped-view-expansion` off `main` before Task 1 (see Task 0).
- Commit per task (batch the task's steps into one commit), not per micro-step. The pre-commit dart-format hook can flake on the SDK lock; if a commit is blocked ONLY by that hook, retry, and only use `--no-verify` if it is the read-only `spec/INDEX.md` / markdownlint case (not applicable here since this plan touches no `spec/` files until Task 7).
- Run `dart format .` in each touched package before committing.
- `flutter analyze` must report ZERO errors in every touched package.
- Commit message trailer: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`

---

## File Structure

**New files:**
- `event_sourcing/lib/src/permissions/scope_descendant_expander.dart` — the downward expander (substrate primitive).
- `event_sourcing/test/permissions/scope_descendant_expander_test.dart` — expander unit tests.
- `event_sourcing/test/permissions/containment_symmetry_test.dart` — anti-drift: resolver (up) vs expander (down) on one fixture.
- `reaction/test/server/subscription_handler_containment_test.dart` — handler integration: ancestor `BoundScope` narrows via injected expander.
- The demo package `event_sourcing/example_clinical_scopes/` (multiple files, Task 8).

**Modified files:**
- `event_sourcing/lib/event_sourcing.dart` — export the new expander.
- `reaction/lib/src/server/subscription_handler.dart` — make `_expandAssignments` async; add an injected `DescendantExpansion` callback; fill the skipped branch.
- `reaction/lib/src/server/reaction_handlers.dart` — build the expansion callback from `scopeClassRegistry` + `eventStore.backend` and pass it through.

**Key design refinement over the spec sketch:** the handler does NOT call `eventStore.backend` directly (the existing test stub `_CapturingEventStore implements EventStore` has no real backend). Instead `runSubscriptionHandler` gains an optional injected callback:

```dart
typedef DescendantExpansion =
    Future<Set<String>> Function(BoundScope assignment, String targetClass);
```

`ReactionHandlers` constructs the production callback (open a read txn on `eventStore.backend`, run the expander); tests inject a fake. This keeps the handler decoupled from the backend and the expander independently testable.

---

## Task 0: Create the feature branch

**Files:** none (git only)

- [ ] **Step 1: Branch off main**

Run:
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git fetch origin
git checkout -b CUR-1317-scoped-view-expansion origin/main
```
Expected: `Switched to a new branch 'CUR-1317-scoped-view-expansion'`

- [ ] **Step 2: Cherry-pick the design doc commit (it lives on the other branch)**

The design doc was committed on `CUR-1317-naming-renames`. Bring just that file onto this branch:
```bash
git checkout CUR-1317-naming-renames -- docs/superpowers/specs/2026-06-01-scoped-view-containment-expansion-design.md
git checkout CUR-1317-naming-renames -- docs/superpowers/plans/2026-06-01-scoped-view-containment-expansion.md
git add docs/superpowers/
git commit -m "docs: scoped-view-expansion design + plan

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
Expected: a commit containing the two docs.

Note: if `main` already contains the naming-renames PR (it may have merged), the design/plan docs may not be on it — adjust by copying from wherever they exist. The point is: this branch must contain both docs before proceeding.

---

## Task 1: `ScopeDescendantExpander` — single-hop expansion (TDD)

**Files:**
- Create: `event_sourcing/lib/src/permissions/scope_descendant_expander.dart`
- Test: `event_sourcing/test/permissions/scope_descendant_expander_test.dart`

- [ ] **Step 1: Write the failing test (single-hop fan-out)**

Create `event_sourcing/test/permissions/scope_descendant_expander_test.dart`:

```dart
// Verifies: EVS-DEV-scope-descendant-expander/A/B/C/D/E — downward
//   containment expansion: identity short-circuit, non-ancestor empty,
//   per-hop inverse query, fail-closed on missing/malformed row,
//   breadth-first multi-hop fan-out.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _FakeBackend {
  _FakeBackend(this.rows);
  final Map<String, List<Map<String, dynamic>>> rows;

  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final all = rows[viewName] ?? [];
    if (where == null) return all;
    return all
        .where((r) => where.entries.every((e) => r[e.key] == e.value))
        .toList();
  }
}

class _FakeTxn extends Transaction {
  const _FakeTxn();
}

class _FakeDescriptor implements ScopeProjectionDescriptor {
  const _FakeDescriptor({required this.columns});
  @override
  final Set<String> columns;
}

ScopeClassRegistry _participantInSite() => ScopeClassRegistry(
  classes: const [
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'participant',
      containedIn: ContainmentReference(
        parentClass: 'site',
        projection: 'participant_site_index',
        keyColumn: 'participant_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: (_) =>
      const _FakeDescriptor(columns: {'participant_id', 'site_id'}),
);

void main() {
  group('ScopeDescendantExpander single-hop', () {
    test('expands a site assignment to all its participants', () async {
      final reg = _participantInSite();
      final backend = _FakeBackend({
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': 'P-2', 'site_id': 'site-A'},
          {'participant_id': 'P-9', 'site_id': 'site-C'},
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: reg,
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1', 'P-2'}));
    });
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && dart test test/permissions/scope_descendant_expander_test.dart -n "single-hop"`
Expected: FAIL — `ScopeDescendantExpander` is not defined (compile error).

- [ ] **Step 3: Write the minimal implementation**

Create `event_sourcing/lib/src/permissions/scope_descendant_expander.dart`:

```dart
// Implements: EVS-PRD-scoped-permissions/F — read-path hierarchy expansion:
//   enumerate the descendant-class scope values reachable from an ancestor
//   assignment by reading ContainmentReference projections downward.
// Implements: EVS-PRD-scoped-permissions/G — fail-closed on missing
//   containment rows (a missing/malformed row contributes nothing).
// Implements: EVS-DEV-scope-descendant-expander — downward chain-walk:
//   A identity on equal class; B empty on non-ancestor target; C per-hop
//   inverse projection read (where parentColumn = value -> read keyColumn);
//   D fail-closed skip on missing/malformed row; E breadth-first multi-hop
//   fan-out with set union.

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/containment_resolver.dart'
    show FindRowsInTxn; // reuse the resolver's callback signature
import 'package:event_sourcing/src/permissions/scope_class_registry.dart';
import 'package:event_sourcing/src/storage/transaction.dart';

// NOTE: `FindRowsInTxn` is declared in containment_resolver.dart and
// re-exported from the event_sourcing barrel (event_sourcing.dart:241,
// `show ContainmentResolver, FindRowsInTxn`). Importing the internal `src/`
// path here matches how sibling permission files import each other.

/// Walks the containment graph DOWNWARD from an ancestor [BoundScope] to a
/// descendant class, enumerating the descendant scope values reachable
/// through the `ContainmentReference` projections.
///
/// This is the inverse of [ContainmentResolver]: the resolver answers
/// "what is P-42's site?" (child -> parent, used by the action path); the
/// expander answers "which participants are at site-A?" (parent -> all
/// children, used by the read path to narrow a subscription).
///
/// Fail-closed: a missing or malformed index row contributes nothing
/// (never widens). A non-ancestor assignment returns the empty set.
class ScopeDescendantExpander {
  ScopeDescendantExpander({required this.registry, required this.findRowsInTxn});

  final ScopeClassRegistry registry;
  final FindRowsInTxn findRowsInTxn;

  Future<Set<String>> expand({
    required Transaction txn,
    required BoundScope assignment,
    required String targetClass,
  }) async {
    // A: identity — the assignment is already at the target class.
    if (assignment.class_ == targetClass) return {assignment.value};
    // B: the assignment's class must be an ancestor of the target.
    if (!registry.isAncestor(assignment.class_, targetClass)) {
      return <String>{};
    }

    // Build the list of classes to descend THROUGH, from the class just
    // below the assignment down to (and including) the target. ancestorChain
    // yields target -> ... -> assignment.class_ (child-first), so reverse it
    // and drop the assignment's own class.
    final chain = registry
        .ancestorChain(targetClass)
        .toList(); // [target, ..., assignmentClass]
    final descendOrder = <String>[];
    for (final spec in chain) {
      descendOrder.add(spec.name);
      if (spec.name == assignment.class_) break;
    }
    // descendOrder == [target, ..., assignmentClass]; reverse to top-down,
    // then drop the assignment's class (the frontier starts there).
    final topDown = descendOrder.reversed.toList(); // [assignmentClass, ..., target]
    final childClasses = topDown.sublist(1); // classes to resolve into

    var frontier = <String>{assignment.value};
    for (final childClass in childClasses) {
      final spec = registry.byName(childClass);
      final ref = spec?.containedIn;
      if (ref == null) return <String>{}; // chain broke; fail-closed
      final next = <String>{};
      for (final parentValue in frontier) {
        final rows = await findRowsInTxn(
          txn,
          ref.projection,
          where: {ref.parentColumn: parentValue},
        );
        for (final row in rows) {
          final key = row[ref.keyColumn];
          if (key is String && key.isNotEmpty) next.add(key);
        }
      }
      frontier = next;
    }
    return frontier;
  }
}
```

- [ ] **Step 4: Export it from the barrel**

In `event_sourcing/lib/event_sourcing.dart`, add an export next to the containment resolver export (the file lists exports alphabetically-ish under `src/permissions/`; place after the `containment_resolver.dart` line at 240):

```dart
export 'src/permissions/scope_descendant_expander.dart'
    show ScopeDescendantExpander;
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd event_sourcing && dart test test/permissions/scope_descendant_expander_test.dart -n "single-hop"`
Expected: PASS.

- [ ] **Step 6: Format, analyze, commit**

```bash
cd event_sourcing && dart format . && flutter analyze
```
Expected: analyze reports 0 errors.
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add event_sourcing/lib/src/permissions/scope_descendant_expander.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/scope_descendant_expander_test.dart
git commit -m "feat(event_sourcing): ScopeDescendantExpander single-hop downward expansion

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Expander — multi-hop, identity, non-ancestor, fail-closed (TDD)

**Files:**
- Modify: `event_sourcing/test/permissions/scope_descendant_expander_test.dart` (add cases)

- [ ] **Step 1: Add the failing tests**

Append inside `main()`'s top-level (a new group) in `scope_descendant_expander_test.dart`:

```dart
  group('ScopeDescendantExpander edge cases', () {
    ScopeClassRegistry regionSiteParticipant() => ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'region'),
        ScopeClassSpec(
          name: 'site',
          containedIn: ContainmentReference(
            parentClass: 'region',
            projection: 'site_region_index',
            keyColumn: 'site_id',
            parentColumn: 'region_id',
          ),
        ),
        ScopeClassSpec(
          name: 'participant',
          containedIn: ContainmentReference(
            parentClass: 'site',
            projection: 'participant_site_index',
            keyColumn: 'participant_id',
            parentColumn: 'site_id',
          ),
        ),
      ],
      projectionLookup: (_) => const _FakeDescriptor(
        columns: {'participant_id', 'site_id', 'region_id'},
      ),
    );

    test('two-hop: region expands to all participants in its sites', () async {
      final backend = _FakeBackend({
        'site_region_index': [
          {'site_id': 'site-A', 'region_id': 'region-West'},
          {'site_id': 'site-B', 'region_id': 'region-West'},
          {'site_id': 'site-C', 'region_id': 'region-East'},
        ],
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': 'P-2', 'site_id': 'site-B'},
          {'participant_id': 'P-9', 'site_id': 'site-C'},
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'region', value: 'region-West'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1', 'P-2'}));
    });

    test('identity: assignment class equals target class', () async {
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'participant', value: 'P-7'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-7'}));
    });

    test('non-ancestor target returns empty set', () async {
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: (_, __, {where, limit, offset}) async => [],
      );
      // participant is NOT an ancestor of site.
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'participant', value: 'P-7'),
        targetClass: 'site',
      );
      expect(result, isEmpty);
    });

    test('empty index (fail-closed) yields empty set', () async {
      final backend = _FakeBackend({'participant_site_index': []});
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, isEmpty);
    });

    test('malformed row (missing/empty key) is skipped', () async {
      final backend = _FakeBackend({
        'participant_site_index': [
          {'participant_id': 'P-1', 'site_id': 'site-A'},
          {'participant_id': '', 'site_id': 'site-A'}, // empty -> skip
          {'site_id': 'site-A'}, // missing key -> skip
        ],
      });
      final expander = ScopeDescendantExpander(
        registry: regionSiteParticipant(),
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await expander.expand(
        txn: const _FakeTxn(),
        assignment: const BoundScope(class_: 'site', value: 'site-A'),
        targetClass: 'participant',
      );
      expect(result, equals({'P-1'}));
    });
  });
```

- [ ] **Step 2: Run to verify they pass (implementation from Task 1 already covers these)**

Run: `cd event_sourcing && dart test test/permissions/scope_descendant_expander_test.dart`
Expected: ALL PASS. (These tests validate the Task-1 implementation against its full contract; if any fail, fix `scope_descendant_expander.dart` until green — the algorithm in Task 1 is written to satisfy them.)

- [ ] **Step 3: Format, analyze, commit**

```bash
cd event_sourcing && dart format . && flutter analyze
```
Expected: 0 errors.
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add event_sourcing/test/permissions/scope_descendant_expander_test.dart
git commit -m "test(event_sourcing): ScopeDescendantExpander multi-hop + fail-closed cases

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Symmetry / anti-drift test (resolver up vs expander down)

**Files:**
- Create: `event_sourcing/test/permissions/containment_symmetry_test.dart`

- [ ] **Step 1: Write the test**

Create `event_sourcing/test/permissions/containment_symmetry_test.dart`:

```dart
// Verifies: EVS-DEV-scope-descendant-expander — the read-path expander and
//   the write-path ContainmentResolver traverse the SAME index data in
//   opposite directions and must agree: if the resolver maps a participant
//   UP to a site, the expander must include that participant when expanding
//   the site DOWN. Guards against the two directions silently drifting.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _FakeBackend {
  _FakeBackend(this.rows);
  final Map<String, List<Map<String, dynamic>>> rows;
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) async {
    final all = rows[viewName] ?? [];
    if (where == null) return all;
    return all
        .where((r) => where.entries.every((e) => r[e.key] == e.value))
        .toList();
  }
}

class _FakeTxn extends Transaction {
  const _FakeTxn();
}

class _FakeDescriptor implements ScopeProjectionDescriptor {
  const _FakeDescriptor({required this.columns});
  @override
  final Set<String> columns;
}

void main() {
  test('expander down(site) contains every participant resolver maps up to '
      'that site', () async {
    final reg = ScopeClassRegistry(
      classes: const [
        ScopeClassSpec(name: 'site'),
        ScopeClassSpec(
          name: 'participant',
          containedIn: ContainmentReference(
            parentClass: 'site',
            projection: 'participant_site_index',
            keyColumn: 'participant_id',
            parentColumn: 'site_id',
          ),
        ),
      ],
      projectionLookup: (_) =>
          const _FakeDescriptor(columns: {'participant_id', 'site_id'}),
    );
    final backend = _FakeBackend({
      'participant_site_index': [
        {'participant_id': 'P-1', 'site_id': 'site-A'},
        {'participant_id': 'P-2', 'site_id': 'site-A'},
        {'participant_id': 'P-9', 'site_id': 'site-C'},
      ],
    });
    final resolver = ContainmentResolver(
      registry: reg,
      findRowsInTxn: backend.findViewRowsInTxn,
    );
    final expander = ScopeDescendantExpander(
      registry: reg,
      findRowsInTxn: backend.findViewRowsInTxn,
    );

    final downFromA = await expander.expand(
      txn: const _FakeTxn(),
      assignment: const BoundScope(class_: 'site', value: 'site-A'),
      targetClass: 'participant',
    );

    for (final pid in downFromA) {
      final up = await resolver.resolve(
        txn: const _FakeTxn(),
        from: BoundScope(class_: 'participant', value: pid),
        target: 'site',
      );
      expect(
        up,
        equals(const BoundScope(class_: 'site', value: 'site-A')),
        reason: 'expander included $pid for site-A but resolver disagrees',
      );
    }
    // And the converse: a participant at a different site is NOT in the set.
    expect(downFromA.contains('P-9'), isFalse);
  });
}
```

- [ ] **Step 2: Run to verify it passes**

Run: `cd event_sourcing && dart test test/permissions/containment_symmetry_test.dart`
Expected: PASS.

- [ ] **Step 3: Format, analyze, commit**

```bash
cd event_sourcing && dart format . && flutter analyze
```
Expected: 0 errors.
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add event_sourcing/test/permissions/containment_symmetry_test.dart
git commit -m "test(event_sourcing): containment resolver/expander symmetry guard

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Subscription handler — add injected `DescendantExpansion`, make `_expandAssignments` async (TDD)

**Files:**
- Modify: `reaction/lib/src/server/subscription_handler.dart`
- Create: `reaction/test/server/subscription_handler_containment_test.dart`

- [ ] **Step 1: Write the failing handler test**

Create `reaction/test/server/subscription_handler_containment_test.dart`. It mirrors `subscription_handler_authz_test.dart`'s harness but injects a fake expander and asserts the ancestor `BoundScope` is expanded into the captured `AggregateMode.aggregates`:

```dart
// Verifies: EVS-PRD-cross-process-event-transport/E + EVS-PRD-scoped-
//   permissions/F — read-path hierarchy expansion: an ancestor BoundScope
//   assignment (site) on a descendant-class view (participant) is expanded,
//   via the injected DescendantExpansion callback, into the AggregateMode
//   allow-set the handler opens.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

// ---- In-process WS channel pair + capturing store + scope policy ----
// (Copy the _Pair, _MemChannel, _MemSink, _CapturingEventStore, _ScopePolicy
//  helper classes verbatim from subscription_handler_authz_test.dart; they
//  are package-private to that test file, so they must be duplicated here.
//  Keep them identical to avoid drift.)

void main() {
  late WsConnectionRegistry connectionRegistry;
  late PrincipalAuthValidator validator;

  setUp(() {
    connectionRegistry = WsConnectionRegistry();
    validator = TrustingAuthValidator(defaultActiveRole: 'investigator');
  });

  ScopeClassRegistry participantInSite() => ScopeClassRegistry(
    classes: const [
      ScopeClassSpec(name: 'site'),
      ScopeClassSpec(
        name: 'participant',
        containedIn: ContainmentReference(
          parentClass: 'site',
          projection: 'participant_site_index',
          keyColumn: 'participant_id',
          parentColumn: 'site_id',
        ),
      ),
    ],
    projectionLookup: (_) => _StubDescriptor(),
  );

  test('site BoundScope on participant view expands to its participants',
      () async {
    final pair = _Pair();
    addTearDown(pair.close);
    final store = _CapturingEventStore();
    final viewScopes = ViewScopeRegistry()
      ..register(
        viewName: 'participants',
        scopeClass: 'participant',
        aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
      );

    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: _ScopePolicy(const [
        ScopeAssignment(scope: BoundScope(class_: 'site', value: 'site-A')),
      ]),
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => null,
      connectionRegistry: connectionRegistry,
      scopeClassRegistry: participantInSite(),
      // The injected expander: site-A -> {P-1, P-2}.
      expandDescendants: (assignment, targetClass) async {
        expect(assignment, const BoundScope(class_: 'site', value: 'site-A'));
        expect(targetClass, 'participant');
        return {'P-1', 'P-2'};
      },
    );

    pair.clientSide.stream.listen((_) {});
    pair.clientSide.sink.add(jsonEncode({'type': 'auth', 'credential': 'dr'}));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    pair.clientSide.sink.add(jsonEncode({
      'type': 'subscribe',
      'subscriptionId': 'sub-1',
      'viewName': 'participants',
    }));

    final deadline = DateTime.now().add(const Duration(seconds: 2));
    while (!store.subscribed && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    expect(store.subscribed, isTrue);
    expect(store.capturedAggregates, equals({'P-1', 'P-2'}));
  });
}

class _StubDescriptor implements ScopeProjectionDescriptor {
  @override
  Set<String> get columns => {'participant_id', 'site_id'};
}
```

NOTE for the implementer: copy the `_Pair`, `_MemChannel`, `_MemSink`, `_CapturingEventStore`, and `_ScopePolicy` helper classes verbatim from `reaction/test/server/subscription_handler_authz_test.dart` (lines ~25–154) into this new file, since they are file-private. Do not modify them.

- [ ] **Step 2: Run to verify it fails**

Run: `cd reaction && flutter test test/server/subscription_handler_containment_test.dart`
Expected: FAIL — `runSubscriptionHandler` has no `expandDescendants` named parameter (compile error).

- [ ] **Step 3: Add the `DescendantExpansion` typedef + parameter to the handler**

In `reaction/lib/src/server/subscription_handler.dart`:

(a) Add the typedef near `ViewPermissionNamer` (after line 42):

```dart
/// Expands an ancestor-class [BoundScope] assignment into the set of
/// descendant-class scope values it covers, by walking the containment
/// graph downward (the read-path inverse of `ContainmentResolver`).
///
/// Injected so the handler stays decoupled from the storage backend and
/// the expander is independently testable. `ReactionHandlers` supplies the
/// production implementation (a short read transaction over
/// `ScopeDescendantExpander`); a `null` callback disables ancestor
/// `BoundScope` expansion (the conservative pre-feature behaviour).
typedef DescendantExpansion =
    Future<Set<String>> Function(BoundScope assignment, String targetClass);
```

(b) Add `DescendantExpansion? expandDescendants` to `runSubscriptionHandler`'s parameters (after `scopeClassRegistry`, line 79) and pass it into `_ConnectionState`:

```dart
void runSubscriptionHandler({
  required WebSocketChannel channel,
  required PrincipalAuthValidator validator,
  required EventStore eventStore,
  required AuthorizationPolicy policy,
  required ViewScopeRegistry viewScopes,
  required ViewPermissionNamer viewPermissionNamer,
  required WsConnectionRegistry connectionRegistry,
  ScopeClassRegistry? scopeClassRegistry,
  DescendantExpansion? expandDescendants,
}) {
  _ConnectionState(
    channel: channel,
    validator: validator,
    eventStore: eventStore,
    policy: policy,
    viewScopes: viewScopes,
    viewPermissionNamer: viewPermissionNamer,
    connectionRegistry: connectionRegistry,
    scopeClassRegistry: scopeClassRegistry,
    expandDescendants: expandDescendants,
  ).start();
}
```

(c) Add the field + constructor parameter to `_ConnectionState` (mirror the `scopeClassRegistry` field at lines 102/118):

```dart
    required this.scopeClassRegistry,
    required this.expandDescendants,
  });
```
and the field declaration after `scopeClassRegistry` (line 118):
```dart
  final DescendantExpansion? expandDescendants;
```

- [ ] **Step 4: Make `_expandAssignments` async and fill the skipped branch**

Change the signature (line 329) from `Set<String>? _expandAssignments({` to:

```dart
  Future<Set<String>?> _expandAssignments({
```

Change the call site in `_handleSubscribe` (line 261) from:
```dart
      allowedAggregates = _expandAssignments(
        assignments: eff.scopeAssignments,
        binding: binding,
      );
```
to:
```dart
      allowedAggregates = await _expandAssignments(
        assignments: eff.scopeAssignments,
        binding: binding,
      );
```

Replace the `appliesViaAncestor` + `BoundScope` branch (lines 396–401, the `break`) with:

```dart
            case BoundScope():
              final expand = expandDescendants;
              if (expand == null) {
                // No expander wired: conservatively skip (under-grant,
                // never over-grant) exactly as the pre-feature behaviour.
                break;
              }
              final descendants = await expand(scope, binding.scopeClass);
              for (final value in descendants) {
                final aggId = binding.aggregateIdResolver(
                  BoundScope(class_: binding.scopeClass, value: value),
                );
                if (aggId != null) result.add(aggId);
              }
```

- [ ] **Step 5: Run the new test + the existing authz test to verify both pass**

Run: `cd reaction && flutter test test/server/subscription_handler_containment_test.dart test/server/subscription_handler_authz_test.dart test/server/subscription_handler_test.dart`
Expected: ALL PASS. (The existing authz/handler tests call `runSubscriptionHandler` without `expandDescendants`, which defaults to `null` → unchanged behaviour; making `_expandAssignments` async does not change their assertions.)

- [ ] **Step 6: Format, analyze, commit**

```bash
cd reaction && dart format . && flutter analyze
```
Expected: 0 errors.
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add reaction/lib/src/server/subscription_handler.dart \
        reaction/test/server/subscription_handler_containment_test.dart
git commit -m "feat(reaction): expand ancestor BoundScope into subscription allow-set

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire the production expander through `ReactionHandlers` (TDD)

**Files:**
- Modify: `reaction/lib/src/server/reaction_handlers.dart`
- Modify: `reaction/test/server/reaction_handlers_test.dart` (add a wiring assertion)

- [ ] **Step 1: Write the failing test**

First inspect `reaction/test/server/reaction_handlers_test.dart` to match its existing setup style. Then add a test asserting that when a `ReactionHandlers` is constructed WITH a `scopeClassRegistry`, an end-to-end subscribe by a site-scoped principal to a participant view narrows to that site's participants. Use an in-memory `SembastBackend` + real `EventStore` seeded with a `participant_site_index` table projection and `role_assigned` events (follow the seeding pattern in `reaction/example/lib/server/bootstrap.dart:188-223`).

Concretely, the test:
1. Builds an in-memory sembast `EventStore` with a `participants` AggregateProjectionSpec and a `participant_site_index` TableProjectionSpec.
2. Appends participant events (P-1@site-A, P-2@site-A, P-9@site-C) and the index rows.
3. Seeds a `role_assigned` for user 'dr' → `BoundScope('site','site-A')`.
4. Constructs `ReactionHandlers(... scopeClassRegistry: <participantInSite>, viewScopeRegistry: <participants->participant>)`.
5. Drives a WS subscribe to `participants` and asserts only P-1, P-2 snapshots arrive (not P-9).

```dart
// Verifies: EVS-PRD-scoped-permissions/F — ReactionHandlers wires the
//   production ScopeDescendantExpander so a site-scoped principal sees only
//   their site's participants on a participant-scoped view subscription.
//
// (Test body: build in-memory EventStore, seed participants + index +
//  role_assigned, construct ReactionHandlers with scopeClassRegistry, drive
//  one WS subscribe, assert the snapshot rows are exactly {P-1, P-2}.)
```

Implementer: model the WS drive on `reaction/example/test/e2e/` harnesses (which already exercise `ReactionHandlers.subscriptions`) — reuse that harness if present rather than hand-rolling a channel.

- [ ] **Step 2: Run to verify it fails**

Run: `cd reaction && flutter test test/server/reaction_handlers_test.dart -n "site-scoped"`
Expected: FAIL — P-9 is present in the snapshot (expander not yet wired), or compile error if a new constructor wiring is referenced.

- [ ] **Step 3: Build the expander in `ReactionHandlers` and pass it through**

In `reaction/lib/src/server/reaction_handlers.dart`:

(a) Add an import for the expander (it is exported from the `event_sourcing` barrel already imported).

(b) In the constructor body (after the `_authzWatcher` setup, ~line 90), build the expansion callback when a registry is present:

```dart
    final registry = scopeClassRegistry;
    if (registry != null) {
      final expander = ScopeDescendantExpander(
        registry: registry,
        findRowsInTxn: eventStore.backend.findViewRowsInTxn,
      );
      _expandDescendants = (assignment, targetClass) =>
          eventStore.backend.transaction(
            (txn) => expander.expand(
              txn: txn,
              assignment: assignment,
              targetClass: targetClass,
            ),
          );
    } else {
      _expandDescendants = null;
    }
```

(c) Add the field:
```dart
  late final DescendantExpansion? _expandDescendants;
```

(d) Pass it in `subscriptions(...)` (line 145):
```dart
        runSubscriptionHandler(
          channel: channel,
          validator: validator,
          eventStore: eventStore,
          policy: policy,
          viewScopes: viewScopeRegistry,
          viewPermissionNamer: _viewPermissionNamer,
          connectionRegistry: connectionRegistry,
          scopeClassRegistry: scopeClassRegistry,
          expandDescendants: _expandDescendants,
        );
```

REQUIRED: `reaction_handlers.dart` imports `subscription_handler.dart` with a `show` clause (line 27: `show ViewPermissionNamer`). You MUST extend it to `show ViewPermissionNamer, DescendantExpansion` or the typedef won't resolve in `reaction_handlers.dart`.

- [ ] **Step 4: Run to verify it passes**

Run: `cd reaction && flutter test test/server/reaction_handlers_test.dart`
Expected: ALL PASS (the site-scoped test now narrows to {P-1, P-2}).

- [ ] **Step 5: Run the full reaction suite (no regressions)**

Run: `cd reaction && flutter test`
Expected: All pass.

- [ ] **Step 6: Format, analyze, commit**

```bash
cd reaction && dart format . && flutter analyze
```
Expected: 0 errors.
```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add reaction/lib/src/server/reaction_handlers.dart \
        reaction/test/server/reaction_handlers_test.dart
git commit -m "feat(reaction): wire production ScopeDescendantExpander into ReactionHandlers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Full substrate + reaction regression sweep

**Files:** none (verification only)

- [ ] **Step 1: event_sourcing suite**

Run: `cd event_sourcing && flutter analyze && flutter test`
Expected: 0 analyze errors; all tests pass (Postgres tests skip without `PG_TEST_URL`).

- [ ] **Step 2: reaction suite**

Run: `cd reaction && flutter analyze && flutter test`
Expected: 0 errors; all pass.

- [ ] **Step 3: No commit needed unless a fix was required.** If a regression surfaced, fix it, then commit with a message describing the fix.

---

## Task 7: Author the `EVS-DEV-scope-descendant-expander` requirement

**Files:**
- Modify: `spec/scoped-permissions.md` (add the DEV requirement block alongside the existing scoped-permissions requirements)

- [ ] **Step 1: Read the existing DEV requirement format**

Run: `grep -n "EVS-DEV-containment-resolver" spec/scoped-permissions.md` and read the surrounding requirement block to copy its exact structure (assertion labels A–E, `Refines:`/`Satisfies:` metadata, changelog line format).

- [ ] **Step 2: Add the requirement via the elspais MCP**

Use the elspais MCP `mutate_add_requirement` (NOT hand-editing, to keep INDEX.md + hashes consistent) to add `EVS-DEV-scope-descendant-expander` with assertions:
- A: identity — `expand` returns `{assignment.value}` when `assignment.class_ == targetClass`.
- B: non-ancestor — returns the empty set when `assignment.class_` is not an ancestor of `targetClass`.
- C: per-hop inverse read — at each hop reads the child's `ContainmentReference.projection` with `where: {parentColumn: parentValue}` and collects `keyColumn`.
- D: fail-closed — a missing index row, or a row whose `keyColumn` is absent / non-string / empty, contributes nothing (never widens).
- E: multi-hop fan-out — descends class-by-class from the assignment's class to the target, unioning across all parent values at each hop.
Set `Refines: EVS-PRD-scoped-permissions` (F/G).

- [ ] **Step 3: Add `// Verifies:` annotations**

Add `// Verifies: EVS-DEV-scope-descendant-expander/A` … `/E` annotations to the corresponding tests in `scope_descendant_expander_test.dart` (map each test to the assertion it exercises). Update the file header comment already present.

- [ ] **Step 4: Run elspais checks**

Run: `elspais checks --spec`
Expected: HEALTHY (run `elspais fix` if hashes are stale, as is normal after editing requirement prose).

- [ ] **Step 5: Commit (spec commit — bypass local hooks per repo convention)**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add spec/ event_sourcing/test/permissions/scope_descendant_expander_test.dart
git commit --no-verify -m "spec: EVS-DEV-scope-descendant-expander requirement + test annotations

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
(`--no-verify`: elspais makes `spec/INDEX.md` read-only, tripping the local end-of-file-fixer hook; CI does not gate on it.)

---

## Task 8: `example_clinical_scopes` GUI demo app

This task is larger; it builds a new Flutter package. Split into sub-steps but commit once at the end (a partially-built app doesn't compile).

**Files (all new, under `event_sourcing/example_clinical_scopes/`):**
- `pubspec.yaml`
- `lib/server/clinical_bootstrap.dart` — substrate + projections + scope classes + seed data + ReactionHandlers.
- `lib/server/clinical_projections.dart` — `participants` AggregateProjectionSpec, `participant_site_index` + `site_region_index` TableProjectionSpecs.
- `lib/shared/clinical_types.dart` — `Participant` model + wire/JSON.
- `lib/client/clinical_app.dart` — Flutter app: user-switcher + reactive participant list via `ViewBuilder<Participant>`.
- `bin/server.dart` — shelf server entry point.
- `lib/client/main.dart` — Flutter client entry point.
- `test/clinical_scoping_e2e_test.dart` — Investigator/Overseer/Admin visibility e2e.
- `test/clinical_app_widget_test.dart` — switch user → list narrows.

- [ ] **Step 1: Scaffold the package pubspec**

Create `event_sourcing/example_clinical_scopes/pubspec.yaml` (model on `reaction/example/pubspec.yaml`, fix the relative path depths — this package sits at `event_sourcing/example_clinical_scopes`, so `event_sourcing` is `../`, `reaction`/`reaction_widgets` are `../../reaction` etc.):

```yaml
name: example_clinical_scopes
description: Hierarchy-scoped read-path demo — region/site/participant with Investigator/Overseer roles.
publish_to: none
version: 0.1.0+1

environment:
  sdk: ^3.10.7
  flutter: ">=3.38.7"

dependencies:
  flutter:
    sdk: flutter
  event_sourcing:
    path: ../
  reaction:
    path: ../../reaction
  reaction_widgets:
    path: ../../reaction_widgets
  http: ^1.4.0
  shelf: ^1.4.2
  shelf_router: ^1.1.4
  sembast: ^3.7.1
  uuid: ^4.5.1
  args: ^2.5.0
  meta: ^1.16.0

dev_dependencies:
  flutter_test:
    sdk: flutter
  test: any
  reaction_widgets_testing:
    path: ../../reaction_widgets_testing
  flutter_lints: ^6.0.0

flutter:
  uses-material-design: true
```

Run: `cd event_sourcing/example_clinical_scopes && flutter pub get`
Expected: resolves.

- [ ] **Step 2: Projections + domain types**

Create `lib/server/clinical_projections.dart` defining:
- `participantsProjection` — `AggregateProjectionSpec(viewName: 'participants', interest: SubscriptionFilter(eventTypes: {'participant_registered','participant_moved'}), tombstoneEventTypes: {})`. Rows carry `participant_id`, `name`, `site_id`.
- `participantSiteIndex` — `TableProjectionSpec(viewName: 'participant_site_index', insertEventTypes: {'participant_registered','participant_moved'}, removeEventTypes: {}, rowKey: AggregateIdKey(), rowData: WholePayload())` with columns `participant_id`, `site_id`.
- `siteRegionIndex` — `TableProjectionSpec(viewName: 'site_region_index', insertEventTypes: {'site_registered'}, removeEventTypes: {}, rowKey: ..., rowData: WholePayload())` with columns `site_id`, `region_id`.

Create `lib/shared/clinical_types.dart` with a `Participant` model (`participantId`, `name`, `siteId`, optional resolved `regionId`) + `fromRow` / `toJson`.

Follow the exact projection-construction idioms in `reaction/example/lib/server/notes_projection.dart`.

- [ ] **Step 3: Bootstrap (server composition)**

Create `lib/server/clinical_bootstrap.dart` modeled on `reaction/example/lib/server/bootstrap.dart`:
- In-memory `SembastBackend`, `EventStore.open` with the three projections + the substrate permission projections (`rolePermissionGrantsSpec`, `userRoleScopesSpec`).
- `ScopeClassRegistry` with region/site/participant chain (see design doc "Scope classes").
- `TableBackedAuthorizationPolicy` with that registry.
- `ViewScopeRegistry` binding `participants` → scopeClass `participant`, `aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null`.
- Seed: 2 regions, 3 sites (site-A,site-B in region-West; site-C in region-East), several participants; `site_registered` + `participant_registered` events; role-permission grants for `Investigator` / `Overseer` / `Admin` (all get `view:participants`); `role_assigned` events: `dr-investigator` → `BoundScope('site','site-A')` and `BoundScope('site','site-B')`; `dr-overseer` → `BoundScope('region','region-West')`; `dr-admin` → `TotalWildcardScope`.
- `ReactionHandlers(eventStore, dispatcher, policy, viewScopeRegistry, scopeClassRegistry: registry)` — passing the registry is what activates the production expander (Task 5).

- [ ] **Step 4: Client UI**

Create `lib/client/clinical_app.dart` modeled on `reaction/example/lib/client/notes_list.dart` + `home_screen.dart`:
- A `DropdownButton` selecting among the four preset users (dr-investigator, dr-overseer, dr-admin, plus an unassigned user to show "sees nothing").
- On selection: (re)connect a `RemoteScope` authenticated as that user (reuse the example's RemoteScope/login plumbing) and render `ViewBuilder<Participant>(viewName: 'participants', scope: scope, mapper: Participant.fromRow, builder: ...)` showing a list with `participant_id`, `name`, `site_id`. `import 'package:flutter/material.dart' hide ViewBuilder;` per the existing note in `notes_list.dart`.
- Read-only: no participant mutation actions.

Create `bin/server.dart` and `lib/client/main.dart` entry points (copy structure from `reaction/example/bin/server.dart` and `lib/client/main.dart`).

- [ ] **Step 5: e2e test — role visibility**

Create `test/clinical_scoping_e2e_test.dart`:
- Boot the server in-process (reuse the bootstrap), open WS subscriptions as each preset user, assert:
  - `dr-investigator` (site-A + site-B) sees participants of site-A and site-B only.
  - `dr-overseer` (region-West) sees all participants in site-A + site-B (two-hop), not site-C's.
  - `dr-admin` (total wildcard) sees all participants.
  - the unassigned user sees none.
Model the WS-drive harness on `reaction/example/test/e2e/` (reuse its multiclient harness if importable; otherwise replicate the channel-pair pattern from `subscription_handler_authz_test.dart`).

- [ ] **Step 6: Widget test — switch user narrows the list**

Create `test/clinical_app_widget_test.dart` using `reaction_widgets_testing`'s `FakeReaction` / `pumpReactionWidget`: mount the participant list with a `FakeReaction` seeded to return site-A's two participants for the investigator scope; assert exactly those rows render; switch the fake to the admin scope (all participants) and assert the list grows. (This is a pure widget test — it does not exercise the real expander, which the e2e covers; it verifies the UI reacts to the scoped view.)

- [ ] **Step 7: Run the demo's tests**

Run: `cd event_sourcing/example_clinical_scopes && flutter analyze && flutter test`
Expected: 0 analyze errors; all tests pass.

- [ ] **Step 8: Commit**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add event_sourcing/example_clinical_scopes/
git commit -m "feat(example): clinical region/site/participant scoped-view demo app

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: CI matrix + README pointer

**Files:**
- Modify: the CI workflow that runs Flutter package tests (`.github/workflows/reaction-widgets-tests.yml` or whichever runs `flutter test` per package — inspect `.github/workflows/` first).
- Modify: `README.md` (add `example_clinical_scopes` to the package list / examples section if one exists).

- [ ] **Step 1: Inspect CI**

Run: `ls .github/workflows/ && grep -rn "flutter test\|example" .github/workflows/`
Identify the job(s) that run example/package tests and how packages are enumerated.

- [ ] **Step 2: Add `example_clinical_scopes` to the test matrix**

Add the new package to the same place `reaction/example` / `example_action_permissions` are listed so CI runs `flutter test` on it. Match the existing YAML style exactly.

- [ ] **Step 3: README pointer**

If `README.md` (or `CLAUDE.md`'s Layout section) enumerates the example apps, add a one-line entry for `example_clinical_scopes` describing it as the hierarchy-scoped read-path demo. (CLAUDE.md edits are fine; keep it to the Layout bullet list.)

- [ ] **Step 4: Commit**

```bash
cd /home/metagamer/cure-hht/event_sourcing-worktrees/CUR-1317-libify-event-sourcing
git add .github/ README.md CLAUDE.md
git commit -m "ci+docs: run example_clinical_scopes tests; document the demo

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Final full-repo verification + PR

**Files:** none (verification + PR)

- [ ] **Step 1: Full verification sweep**

Run each and confirm green:
```bash
cd event_sourcing && flutter analyze && flutter test
cd ../reaction && flutter analyze && flutter test
cd ../reaction_widgets && flutter analyze && flutter test
cd ../reaction_widgets_testing && flutter analyze && flutter test
cd ../event_sourcing/example && flutter analyze && flutter test
cd ../example_action_permissions && flutter analyze && flutter test
cd ../example_clinical_scopes && flutter analyze && flutter test
cd ../../reaction/example && flutter analyze && flutter test
cd ../../canonical_json_jcs && dart analyze && dart test
cd ../provenance && dart analyze && dart test
cd .. && elspais checks --spec
```
Expected: 0 analyze errors everywhere; all tests pass (Postgres skips without `PG_TEST_URL`); elspais HEALTHY.

- [ ] **Step 2: dart format check across packages**

Run `dart format --set-exit-if-changed .` in each package (or `dart format .` then `git diff --exit-code`). Expected: no changes.

- [ ] **Step 3: Push and open PR**

```bash
git push --no-verify -u origin CUR-1317-scoped-view-expansion
env -u GITHUB_TOKEN gh pr create --base main \
  --title "[CUR-1317] Scoped view containment expansion (read-path descendant expansion)" \
  --body-file <(printf '...')   # summary + test plan; see pr-update conventions
```
(`--no-verify` push + `env -u GITHUB_TOKEN`: per repo convention — read-only `spec/INDEX.md` trips the pre-push hook; an invalid `GITHUB_TOKEN` env var shadows the working keyring token.)

Expected: PR created.

---

## Self-Review notes (for the implementer)

- **Spec coverage:** Task 1–3 cover the substrate expander (design §"Substrate" + §"Algorithm" + §"correctness caveat"). Task 4–5 cover the reaction wiring (§"Reaction"). Task 7 covers §"Requirement traceability". Task 8 covers §"Demo app". Tasks 6/9/10 cover §"Testing" + CI. Error-handling (§"Error handling") is covered by Task 2's fail-closed/malformed cases and Task 4's `expandDescendants == null` skip.
- **Static-freshness is intentional:** none of the tasks add index-watching or live re-expansion — that is the deferred A+ work named in the design's "Future work". Do not add it.
- **No cap, no pagination:** do not add a size limit to `expand` or a `limit`/`offset` to the subscribe path. Both are explicit non-goals.
- **Injected callback vs direct backend call:** the design doc sketched `eventStore.backend.transaction(...)` inside the handler; this plan injects a `DescendantExpansion` callback instead (Task 4) so the handler stays testable against the existing `EventStore`-stub tests. The production wiring in Task 5 does the `backend.transaction` call. This is a deliberate, documented refinement.
