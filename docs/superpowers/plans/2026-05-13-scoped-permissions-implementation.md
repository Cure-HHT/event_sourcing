# Scoped Permissions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy `ScopeClass{global,site,self}` precondition enum with a generalized scope-class registration mechanism; add scope-value binding via new `role_assigned`/`role_unassigned` event types; rewrite the match algorithm to support equality + wildcard + hierarchy/containment; wire scope binding through `Action.scopeFor` per dispatch; wrap authorize+execute in a single storage transaction.

**Architecture:** Domain-neutral substrate primitive. Apps register `ScopeClassSpec`s (e.g., `site`, `patient`) declaratively at composition time; containment hierarchies point at existing `TableProjectionSpec`s that the substrate reads at evaluate time. Permission grants stay role-keyed (`permission_granted` events); scope binding lives on user-role assignments (`role_assigned` events). The match algorithm is union-within-active-role with first-match-wins. Storage transactions span authorize and execute so a single read-consistent snapshot drives both decision and emission.

**Tech Stack:** Dart 3.10.7+ (sealed classes, pattern matching), Flutter test framework, sembast for the reference storage backend, `canonical_json_jcs` for aggregate-id encoding, elspais for requirement traceability, pre-commit hooks (trim whitespace, markdownlint, dart format).

**Spec:** `spec/scoped-permissions.md`

**Greenfield discipline:** Per CUR-1317 and the spec's pre-ship posture, legacy types (`ScopeClass` enum, `Permission.scope` field, `Principal.activeSite`, `DenyReason.sessionPreconditionMissing`) are deleted, not deprecated. Existing event-type names (`permission_granted`, `permission_revoked`) are reused with reshaped payloads. No backwards-compat shims.

**Out of scope (deferred):** Migration of `hht_diary` consumer (CUR-1170, downstream); permission bundling / PermissionGroup primitive (Future Work F6); substrate-defined CRUD Action templates (Future Work F7); range comparison on scope values (Future Work F1); explicit deny-grants (Future Work F2); authorization caching (Future Work F5).

---

## File Map

### New files

```text
event_sourcing/lib/src/actions/scope_value.dart                    sealed ScopeValue + 3 variants + JSON
event_sourcing/lib/src/permissions/scope_class_spec.dart           ScopeClassSpec, ContainmentRef
event_sourcing/lib/src/permissions/scope_class_registry.dart       ScopeClassRegistry (compose-time validation)
event_sourcing/lib/src/permissions/role_assigned_payload.dart      payload for role_assigned events
event_sourcing/lib/src/permissions/role_unassigned_payload.dart    payload for role_unassigned events
event_sourcing/lib/src/permissions/role_assignment_aggregate_id.dart  canonical-JSON aggregate-id encoder
event_sourcing/lib/src/permissions/user_role_scopes_spec.dart      TableProjectionSpec for user_role_scopes
event_sourcing/lib/src/permissions/containment_resolver.dart       walks containment chain via projections
event_sourcing/lib/src/permissions/scope_assignment.dart           (class, value) sealed-variant carrier
event_sourcing/lib/src/permissions/effective_authorization.dart    return type of effectivePermissionsFor
event_sourcing/lib/src/permissions/role_assignment_seed.dart       declarative seed-list shape
event_sourcing/lib/src/permissions/bootstrap_role_assignments.dart bootstrap orchestration

event_sourcing/test/actions/scope_value_test.dart
event_sourcing/test/permissions/scope_class_spec_test.dart
event_sourcing/test/permissions/scope_class_registry_test.dart
event_sourcing/test/permissions/role_assigned_payload_test.dart
event_sourcing/test/permissions/role_unassigned_payload_test.dart
event_sourcing/test/permissions/role_assignment_aggregate_id_test.dart
event_sourcing/test/permissions/user_role_scopes_spec_test.dart
event_sourcing/test/permissions/containment_resolver_test.dart
event_sourcing/test/permissions/bootstrap_role_assignments_test.dart
```

### Modified files

```text
event_sourcing/lib/src/actions/permission.dart                     drop 'scope' field; add 'scopeClass: String?'
event_sourcing/lib/src/actions/action.dart                         add scopeFor() method
event_sourcing/lib/src/actions/principal.dart                      drop UserPrincipal.activeSite
event_sourcing/lib/src/actions/authorization_decision.dart         drop sessionPreconditionMissing; add scopeUnresolvable
event_sourcing/lib/src/actions/authorization_policy.dart           extended isPermitted signature; replace permissionsFor
event_sourcing/lib/src/actions/action_dispatcher.dart              new authorize stage; transactional wrap
event_sourcing/lib/src/storage/storage_backend.dart                add findViewRowsInTxn (multi-row, in-txn)
event_sourcing/lib/src/storage/sembast_backend.dart                impl findViewRowsInTxn
event_sourcing/lib/src/permissions/permission_granted_payload.dart drop 'scope' field
event_sourcing/lib/src/permissions/permission_revoked_payload.dart drop 'scope' field (if present)
event_sourcing/lib/src/permissions/role_permission_grants_spec.dart payload-shape adjustments
event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart full rewrite (match algorithm + effective)
event_sourcing/lib/src/permissions/fail_safe_authorization_policy.dart replace permissionsFor
event_sourcing/lib/src/permissions/in_memory_role_matrix_reader.dart drop scope handling
event_sourcing/lib/src/permissions/materialized_view_role_matrix_reader.dart drop scope handling
event_sourcing/lib/src/permissions/snapshot_role_matrix_reader.dart drop scope handling
event_sourcing/lib/src/permissions/role_matrix_reader.dart         drop scope from interface
event_sourcing/lib/src/permissions/permission_seed.dart            drop scope from seed shape
event_sourcing/lib/src/permissions/seed_validator.dart             add scope-class-registry validation
event_sourcing/lib/src/permissions/yaml_seed_loader.dart           drop scope field handling
event_sourcing/lib/src/permissions/permission_snapshot.dart        update for new effectivePermissionsFor shape

event_sourcing/example_action_permissions/lib/server/actions/*.dart  drop ScopeClass; add scopeFor where scoped
event_sourcing/example_action_permissions/test/**/*.dart           update assertions
event_sourcing/example/lib/**/*.dart                               drop ScopeClass references (if any)

spec/scoped-permissions.md                                         add EVS-DEV-* DEV requirements alongside impl
```

### Deleted files

```text
event_sourcing/lib/src/actions/scope_class.dart                    ScopeClass{global,site,self} enum
```

---

## Conventions for this plan

- **Test command (per package):** `cd event_sourcing && flutter test <path>` to run one file; `cd event_sourcing && flutter test` for full suite. The same applies to `event_sourcing/example` and `event_sourcing/example_action_permissions`.
- **Verifies / Implements annotations:** every new file with substrate behaviour gets `// Implements: ...` annotations for the requirements it satisfies; every test file gets `// Verifies: ...`. Until Task 30 authors `EVS-PRD-scoped-permissions` and supporting `EVS-DEV-*` requirements, use the closest existing requirement IDs from `spec/prd-permissions-as-events.md` and `spec/prd-action-dispatch.md` and note in the commit message that they'll be re-bound in Task 30.
- **Commits:** every Task ends with one commit. Per the repo convention, commit messages are free-form (no enforced format); the PR title carries the `[CUR-1331]` Linear reference.
- **Pre-commit hooks** run automatically: trim trailing whitespace, fix EOF, markdownlint, dart format. If a commit fails the hook, fix the underlying issue and create a **new** commit — do not `--amend` or `--no-verify`.

---

## Phase 0 — Storage backend extension (prerequisite)

The match algorithm enumerates `user_role_scopes` rows for a given `(userId, role)` inside the dispatch transaction. `StorageBackend` already has `readViewRowInTxn` (single-row, in-txn) and `findViewRows` (multi-row, non-txn). We add the missing combination: multi-row, in-txn, with an optional column-equality filter.

### Task 1: Add `findViewRowsInTxn` to abstract `StorageBackend`

**Files:**

- Modify: `event_sourcing/lib/src/storage/storage_backend.dart` — add the new method to the abstract class
- Modify: `event_sourcing/lib/src/storage/sembast_backend.dart` — concrete impl
- Test: `event_sourcing/test/storage/sembast_backend_test.dart` (or whichever existing test file covers view-row IO) — add a multi-row in-txn read test

- [ ] **Step 1: Write the failing test**

Locate the existing sembast view-row test (likely `event_sourcing/test/storage/sembast_view_rows_test.dart` or similar — `grep -rn "findViewRows\|upsertViewRowInTxn" event_sourcing/test`). Add this test alongside the existing ones:

```dart
test('findViewRowsInTxn returns rows matching column equality filter '
    'inside a transaction', () async {
  final backend = SembastStorageBackend.inMemory();
  await backend.open();
  await backend.runInTxn((txn) async {
    await backend.upsertViewRowInTxn(txn, 'demo', 'k1', {
      'user_id': 'U1', 'role': 'SC',
    });
    await backend.upsertViewRowInTxn(txn, 'demo', 'k2', {
      'user_id': 'U1', 'role': 'SUP',
    });
    await backend.upsertViewRowInTxn(txn, 'demo', 'k3', {
      'user_id': 'U2', 'role': 'SC',
    });
  });

  final rows = await backend.runInTxn((txn) =>
      backend.findViewRowsInTxn(txn, 'demo',
          where: {'user_id': 'U1', 'role': 'SC'}));
  expect(rows, hasLength(1));
  expect(rows.single['user_id'], 'U1');
  expect(rows.single['role'], 'SC');
});
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd event_sourcing && flutter test test/storage/sembast_view_rows_test.dart
```

Expected: FAIL with "method `findViewRowsInTxn` isn't defined for the class `SembastStorageBackend`".

- [ ] **Step 3: Add the abstract method to `StorageBackend`**

In `event_sourcing/lib/src/storage/storage_backend.dart`, add (after the existing `findViewRows` declaration around line 190):

```dart
/// Iterate rows in [viewName] inside [txn] optionally filtered by
/// column equality. `where` is interpreted as "every key/value pair
/// must match the row's column of that name." Returns rows in
/// unspecified order; callers needing sort must sort the result.
///
/// Used by the authorization policy to enumerate user_role_scopes
/// for the current (userId, role) inside the dispatch transaction
/// so that the authorize-stage read sees the same snapshot as the
/// execute-stage append.
Future<List<Map<String, dynamic>>> findViewRowsInTxn(
  Txn txn,
  String viewName, {
  Map<String, Object?>? where,
  int? limit,
  int? offset,
});
```

- [ ] **Step 4: Implement in `SembastStorageBackend`**

In `event_sourcing/lib/src/storage/sembast_backend.dart`, locate the existing `findViewRows` impl and add the `findViewRowsInTxn` method:

```dart
@override
Future<List<Map<String, dynamic>>> findViewRowsInTxn(
  Txn txn,
  String viewName, {
  Map<String, Object?>? where,
  int? limit,
  int? offset,
}) async {
  final store = stringMapStoreFactory.store(viewName);
  Finder? finder;
  if (where != null && where.isNotEmpty) {
    final filters = <Filter>[
      for (final e in where.entries)
        Filter.equals(e.key, e.value),
    ];
    finder = Finder(
      filter: filters.length == 1 ? filters.single : Filter.and(filters),
      limit: limit,
      offset: offset,
    );
  } else if (limit != null || offset != null) {
    finder = Finder(limit: limit, offset: offset);
  }
  final records = await store.find(txn.sembastTxn, finder: finder);
  return [
    for (final r in records) Map<String, dynamic>.from(r.value),
  ];
}
```

(If `Txn` is a thin wrapper that exposes the underlying sembast transaction differently, match the wrapper's existing accessor — `grep -n "sembastTxn\|database\|asSembast" event_sourcing/lib/src/storage/sembast_backend.dart`. The other `*InTxn` methods already use this pattern.)

- [ ] **Step 5: Run test to verify it passes**

```bash
cd event_sourcing && flutter test test/storage/sembast_view_rows_test.dart
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/storage/storage_backend.dart \
        event_sourcing/lib/src/storage/sembast_backend.dart \
        event_sourcing/test/storage/sembast_view_rows_test.dart
git commit -m "[CUR-1331] Add findViewRowsInTxn to StorageBackend interface

Required for the scoped-permissions authorize stage to enumerate
user_role_scopes rows inside the dispatch transaction so that the
authorize read and the execute append share one read-consistent
snapshot.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 1 — Foundational types

### Task 2: Sealed `ScopeValue` type with three variants

**Files:**

- Create: `event_sourcing/lib/src/actions/scope_value.dart`
- Test: `event_sourcing/test/actions/scope_value_test.dart`

- [ ] **Step 1: Write failing tests**

Create `event_sourcing/test/actions/scope_value_test.dart`:

```dart
// Verifies: EVS-PRD-permissions-as-events (scope-value shape pinned by spec/scoped-permissions.md)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeValue', () {
    test('BoundScope round-trips through JSON', () {
      const v = BoundScope(class_: 'site', value: 'A');
      expect(v.toJson(), {'class': 'site', 'value': 'A'});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    test('ValueWildcardScope round-trips through JSON', () {
      const v = ValueWildcardScope(class_: 'site');
      expect(v.toJson(), {'class': 'site', 'wildcard_value': true});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    test('TotalWildcardScope round-trips through JSON', () {
      const v = TotalWildcardScope();
      expect(v.toJson(), {'wildcard_class': true});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    test('BoundScope and ValueWildcardScope with same class are unequal', () {
      expect(
        const BoundScope(class_: 'site', value: 'A'),
        isNot(equals(const ValueWildcardScope(class_: 'site'))),
      );
    });

    test('fromJson rejects ambiguous objects (both value and wildcard_value)',
        () {
      expect(
        () => ScopeValue.fromJson(
            {'class': 'site', 'value': 'A', 'wildcard_value': true}),
        throwsA(isA<FormatException>()),
      );
    });

    test('fromJson rejects total_wildcard combined with class', () {
      expect(
        () => ScopeValue.fromJson({'wildcard_class': true, 'class': 'site'}),
        throwsA(isA<FormatException>()),
      );
    });

    test('fromJson rejects empty object', () {
      expect(() => ScopeValue.fromJson(<String, Object?>{}),
          throwsA(isA<FormatException>()));
    });

    test('fromJson rejects bound shape with empty value', () {
      expect(() => ScopeValue.fromJson({'class': 'site', 'value': ''}),
          throwsA(isA<FormatException>()));
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/actions/scope_value_test.dart
```

Expected: FAIL with "Undefined name `BoundScope`" / "Undefined class".

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/actions/scope_value.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events (scope-value carrier for grants and dispatch)

/// The scope a permission grant or an action-dispatch operation targets.
///
/// Sealed: every consumer-side switch must exhaustively handle all three
/// variants. Adding a fourth variant is a deliberate code-plus-REQ change.
///
/// JSON parse contract: the three shapes are mutually exclusive by key set.
/// `fromJson` rejects any object whose keys do not match exactly one shape.
sealed class ScopeValue {
  const ScopeValue();

  factory ScopeValue.fromJson(Map<String, Object?> json) {
    final hasClass = json.containsKey('class');
    final hasValue = json.containsKey('value');
    final hasValueWildcard = json.containsKey('wildcard_value');
    final hasClassWildcard = json.containsKey('wildcard_class');

    if (hasClassWildcard && !hasClass && !hasValue && !hasValueWildcard) {
      if (json['wildcard_class'] != true) {
        throw FormatException(
            'wildcard_class must be the literal `true`, got ${json['wildcard_class']}');
      }
      if (json.length != 1) {
        throw FormatException('wildcard_class object has unexpected keys: $json');
      }
      return const TotalWildcardScope();
    }

    if (hasClass && hasValueWildcard && !hasValue && !hasClassWildcard) {
      if (json['wildcard_value'] != true) {
        throw FormatException(
            'wildcard_value must be the literal `true`, got ${json['wildcard_value']}');
      }
      if (json.length != 2) {
        throw FormatException(
            'value-wildcard object has unexpected keys: $json');
      }
      final cls = json['class'];
      if (cls is! String || cls.isEmpty) {
        throw FormatException('class must be a non-empty string, got $cls');
      }
      return ValueWildcardScope(class_: cls);
    }

    if (hasClass && hasValue && !hasValueWildcard && !hasClassWildcard) {
      if (json.length != 2) {
        throw FormatException('bound object has unexpected keys: $json');
      }
      final cls = json['class'];
      final val = json['value'];
      if (cls is! String || cls.isEmpty) {
        throw FormatException('class must be a non-empty string, got $cls');
      }
      if (val is! String || val.isEmpty) {
        throw FormatException('value must be a non-empty string, got $val');
      }
      return BoundScope(class_: cls, value: val);
    }

    throw FormatException(
        'ScopeValue JSON shape unrecognized; expected one of: '
        '{"class","value"}, {"class","wildcard_value":true}, '
        '{"wildcard_class":true}. Got: $json');
  }

  Map<String, Object?> toJson();
}

/// Specific value of a specific scope class.
final class BoundScope extends ScopeValue {
  const BoundScope({required this.class_, required this.value})
      : assert(class_ != '', 'class_ must not be empty'),
        assert(value != '', 'value must not be empty');

  final String class_;
  final String value;

  @override
  Map<String, Object?> toJson() => {'class': class_, 'value': value};

  @override
  bool operator ==(Object other) =>
      other is BoundScope && class_ == other.class_ && value == other.value;

  @override
  int get hashCode => Object.hash('Bound', class_, value);

  @override
  String toString() => 'BoundScope($class_, $value)';
}

/// Any value of a specific scope class.
final class ValueWildcardScope extends ScopeValue {
  const ValueWildcardScope({required this.class_})
      : assert(class_ != '', 'class_ must not be empty');

  final String class_;

  @override
  Map<String, Object?> toJson() => {'class': class_, 'wildcard_value': true};

  @override
  bool operator ==(Object other) =>
      other is ValueWildcardScope && class_ == other.class_;

  @override
  int get hashCode => Object.hash('ValueWildcard', class_);

  @override
  String toString() => 'ValueWildcardScope($class_)';
}

/// Any class, any value (the global/admin grant).
final class TotalWildcardScope extends ScopeValue {
  const TotalWildcardScope();

  @override
  Map<String, Object?> toJson() => {'wildcard_class': true};

  @override
  bool operator ==(Object other) => other is TotalWildcardScope;

  @override
  int get hashCode => 'TotalWildcard'.hashCode;

  @override
  String toString() => 'TotalWildcardScope()';
}
```

- [ ] **Step 4: Export from the package**

In `event_sourcing/lib/event_sourcing.dart`, add (alphabetized with the other `actions/` exports):

```dart
export 'src/actions/scope_value.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/actions/scope_value_test.dart
```

Expected: 8 passed.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/actions/scope_value.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/actions/scope_value_test.dart
git commit -m "[CUR-1331] Add sealed ScopeValue type with three JSON variants

Bound, ValueWildcard, TotalWildcard. JSON parse contract enforces
mutual exclusivity of the three shapes by key set; fromJson rejects
ambiguous objects.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: `ScopeClassSpec` and `ContainmentRef`

**Files:**

- Create: `event_sourcing/lib/src/permissions/scope_class_spec.dart`
- Test: `event_sourcing/test/permissions/scope_class_spec_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (scope-class registration shape)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClassSpec', () {
    test('top-level class has no containment', () {
      const spec = ScopeClassSpec(name: 'site');
      expect(spec.name, 'site');
      expect(spec.containedIn, isNull);
    });

    test('contained class carries a ContainmentRef', () {
      const spec = ScopeClassSpec(
        name: 'patient',
        containedIn: ContainmentRef(
          parentClass: 'site',
          projection: 'patient_site_index',
          keyColumn: 'patient_id',
          parentColumn: 'site_id',
        ),
      );
      expect(spec.containedIn?.parentClass, 'site');
      expect(spec.containedIn?.projection, 'patient_site_index');
      expect(spec.containedIn?.keyColumn, 'patient_id');
      expect(spec.containedIn?.parentColumn, 'site_id');
    });

    test('constructor rejects empty name', () {
      expect(() => ScopeClassSpec(name: ''), throwsA(isA<AssertionError>()));
    });

    test('ContainmentRef rejects empty fields', () {
      expect(
        () => ContainmentRef(
            parentClass: '',
            projection: 'p',
            keyColumn: 'k',
            parentColumn: 'pc'),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
            parentClass: 'p',
            projection: '',
            keyColumn: 'k',
            parentColumn: 'pc'),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
            parentClass: 'p',
            projection: 'p',
            keyColumn: '',
            parentColumn: 'pc'),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
            parentClass: 'p',
            projection: 'p',
            keyColumn: 'k',
            parentColumn: ''),
        throwsA(isA<AssertionError>()),
      );
    });

    test('equality compares all fields', () {
      const a = ScopeClassSpec(name: 'site');
      const b = ScopeClassSpec(name: 'site');
      const c = ScopeClassSpec(name: 'patient');
      expect(a, equals(b));
      expect(a, isNot(equals(c)));
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/scope_class_spec_test.dart
```

Expected: FAIL with "Undefined class `ScopeClassSpec`".

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/scope_class_spec.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events (scope-class registration; substrate ships the mechanism, apps declare classes)

import 'package:meta/meta.dart';

/// A named scope dimension along which permissions can be scoped.
/// Apps register these at composition time with `ScopeClassRegistry`.
///
/// Top-level scope classes (no containment) match by direct equality.
/// Classes with `containedIn` participate in hierarchy expansion: a
/// principal's assignment at the parent class covers descendants whose
/// containment lookup resolves to the assigned value.
@immutable
class ScopeClassSpec {
  const ScopeClassSpec({required this.name, this.containedIn})
      : assert(name != '', 'name must not be empty');

  final String name;
  final ContainmentRef? containedIn;

  @override
  bool operator ==(Object other) =>
      other is ScopeClassSpec &&
      name == other.name &&
      containedIn == other.containedIn;

  @override
  int get hashCode => Object.hash(name, containedIn);

  @override
  String toString() => containedIn == null
      ? 'ScopeClassSpec($name)'
      : 'ScopeClassSpec($name in ${containedIn!.parentClass})';
}

/// Points a scope class at the projection that records its containment
/// in a parent class. The substrate reads `projection` at evaluate time,
/// indexes by `keyColumn`, and reads the parent value from `parentColumn`.
///
/// Example:
///   ContainmentRef(parentClass: 'site',
///                  projection: 'patient_site_index',
///                  keyColumn: 'patient_id',
///                  parentColumn: 'site_id')
///
/// At evaluate time, to resolve "what site is P-42 at?", the substrate
/// queries `patient_site_index` for the row with `patient_id == 'P-42'`
/// and returns the value in the `site_id` column.
@immutable
class ContainmentRef {
  const ContainmentRef({
    required this.parentClass,
    required this.projection,
    required this.keyColumn,
    required this.parentColumn,
  })  : assert(parentClass != '', 'parentClass must not be empty'),
        assert(projection != '', 'projection must not be empty'),
        assert(keyColumn != '', 'keyColumn must not be empty'),
        assert(parentColumn != '', 'parentColumn must not be empty');

  final String parentClass;
  final String projection;
  final String keyColumn;
  final String parentColumn;

  @override
  bool operator ==(Object other) =>
      other is ContainmentRef &&
      parentClass == other.parentClass &&
      projection == other.projection &&
      keyColumn == other.keyColumn &&
      parentColumn == other.parentColumn;

  @override
  int get hashCode =>
      Object.hash(parentClass, projection, keyColumn, parentColumn);

  @override
  String toString() => 'ContainmentRef(in $parentClass via $projection'
      '[$keyColumn -> $parentColumn])';
}
```

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/scope_class_spec.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/scope_class_spec_test.dart
```

Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/scope_class_spec.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/scope_class_spec_test.dart
git commit -m "[CUR-1331] Add ScopeClassSpec and ContainmentRef

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `ScopeClassRegistry` with composition-time validation

**Files:**

- Create: `event_sourcing/lib/src/permissions/scope_class_registry.dart`
- Test: `event_sourcing/test/permissions/scope_class_registry_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (composition-time validation refuses cycles, dangling refs, missing columns)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClassRegistry', () {
    test('accepts a flat registry of top-level classes', () {
      final r = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(name: 'lab'),
        ],
        projectionLookup: (_) => const _FakeProjection(columns: {}),
      );
      expect(r.byName('site')!.name, 'site');
      expect(r.byName('lab')!.name, 'lab');
      expect(r.byName('nonexistent'), isNull);
    });

    test('accepts a hierarchy of two classes', () {
      final r = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentRef(
              parentClass: 'site',
              projection: 'patient_site_index',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (name) => name == 'patient_site_index'
            ? const _FakeProjection(columns: {'patient_id', 'site_id'})
            : null,
      );
      expect(r.byName('patient')!.containedIn!.parentClass, 'site');
    });

    test('rejects duplicate class names', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(name: 'site'),
            ScopeClassSpec(name: 'site'),
          ],
          projectionLookup: (_) => null,
        ),
        throwsA(isA<StateError>().having(
            (e) => e.message, 'message', contains('duplicate'))),
      );
    });

    test('rejects parentClass that is not a registered class', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(
              name: 'patient',
              containedIn: ContainmentRef(
                parentClass: 'site',     // never registered
                projection: 'patient_site_index',
                keyColumn: 'patient_id',
                parentColumn: 'site_id',
              ),
            ),
          ],
          projectionLookup: (_) =>
              const _FakeProjection(columns: {'patient_id', 'site_id'}),
        ),
        throwsA(isA<StateError>().having(
            (e) => e.message, 'message', contains('parentClass'))),
      );
    });

    test('rejects projection that is not registered', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(name: 'site'),
            ScopeClassSpec(
              name: 'patient',
              containedIn: ContainmentRef(
                parentClass: 'site',
                projection: 'patient_site_index',
                keyColumn: 'patient_id',
                parentColumn: 'site_id',
              ),
            ),
          ],
          projectionLookup: (_) => null,
        ),
        throwsA(isA<StateError>().having(
            (e) => e.message, 'message', contains('projection'))),
      );
    });

    test('rejects projection missing the keyColumn or parentColumn', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(name: 'site'),
            ScopeClassSpec(
              name: 'patient',
              containedIn: ContainmentRef(
                parentClass: 'site',
                projection: 'patient_site_index',
                keyColumn: 'patient_id',
                parentColumn: 'site_id',
              ),
            ),
          ],
          projectionLookup: (_) =>
              const _FakeProjection(columns: {'patient_id'}),
        ),
        throwsA(isA<StateError>().having(
            (e) => e.message, 'message', contains('parentColumn'))),
      );
    });

    test('rejects cycles in the containment graph', () {
      expect(
        () => ScopeClassRegistry(
          classes: const [
            ScopeClassSpec(
              name: 'a',
              containedIn: ContainmentRef(
                parentClass: 'b',
                projection: 'p',
                keyColumn: 'k',
                parentColumn: 'pc',
              ),
            ),
            ScopeClassSpec(
              name: 'b',
              containedIn: ContainmentRef(
                parentClass: 'a',
                projection: 'p',
                keyColumn: 'k',
                parentColumn: 'pc',
              ),
            ),
          ],
          projectionLookup: (_) => const _FakeProjection(columns: {'k', 'pc'}),
        ),
        throwsA(isA<StateError>().having(
            (e) => e.message, 'message', contains('cycle'))),
      );
    });

    test('ancestorChain returns chain from class to top', () {
      final r = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'region'),
          ScopeClassSpec(
            name: 'site',
            containedIn: ContainmentRef(
              parentClass: 'region',
              projection: 'site_region',
              keyColumn: 'site_id',
              parentColumn: 'region_id',
            ),
          ),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentRef(
              parentClass: 'site',
              projection: 'patient_site',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (_) =>
            const _FakeProjection(columns: {'patient_id', 'site_id', 'region_id'}),
      );
      expect(r.ancestorChain('patient').map((s) => s.name),
          ['patient', 'site', 'region']);
      expect(r.ancestorChain('region').map((s) => s.name), ['region']);
    });
  });
}

class _FakeProjection {
  const _FakeProjection({required this.columns});
  final Set<String> columns;
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/scope_class_registry_test.dart
```

Expected: FAIL with undefined `ScopeClassRegistry`.

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/scope_class_registry.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events (composition-time validation; refuses cycles, dangling parent refs, missing projection columns)

import 'package:event_sourcing/event_sourcing.dart';

/// Helper returned by the projection-lookup callback; lets the registry
/// verify named columns exist without depending on the concrete
/// TableProjectionSpec type (the registry doesn't otherwise need to read
/// projection internals).
abstract class ScopeProjectionDescriptor {
  Set<String> get columns;
}

/// Compose-time registry of [ScopeClassSpec]s. Validates the registry
/// against the projection registry (via [projectionLookup]) so that
/// containment references are guaranteed to resolve at evaluate time.
///
/// Throws `StateError` on validation failure with a message naming the
/// specific defect (duplicate, dangling parent, missing column, cycle).
class ScopeClassRegistry {
  ScopeClassRegistry({
    required List<ScopeClassSpec> classes,
    required ScopeProjectionDescriptor? Function(String projectionName)
        projectionLookup,
  }) : _byName = _buildAndValidate(classes, projectionLookup);

  final Map<String, ScopeClassSpec> _byName;

  ScopeClassSpec? byName(String name) => _byName[name];

  Iterable<ScopeClassSpec> get all => _byName.values;

  /// Returns [className]'s ancestor chain starting at [className] itself
  /// and ending at the top-of-graph class. Returns [className] alone if
  /// the class has no containment.
  Iterable<ScopeClassSpec> ancestorChain(String className) sync* {
    var current = _byName[className];
    while (current != null) {
      yield current;
      final ref = current.containedIn;
      if (ref == null) return;
      current = _byName[ref.parentClass];
    }
  }

  /// True iff [ancestor] is in [descendant]'s ancestor chain (or equals it).
  bool isAncestor(String ancestor, String descendant) {
    for (final c in ancestorChain(descendant)) {
      if (c.name == ancestor) return true;
    }
    return false;
  }

  static Map<String, ScopeClassSpec> _buildAndValidate(
    List<ScopeClassSpec> classes,
    ScopeProjectionDescriptor? Function(String) projectionLookup,
  ) {
    final byName = <String, ScopeClassSpec>{};
    for (final c in classes) {
      if (byName.containsKey(c.name)) {
        throw StateError(
            'ScopeClassRegistry: duplicate class name "${c.name}"');
      }
      byName[c.name] = c;
    }

    for (final c in classes) {
      final ref = c.containedIn;
      if (ref == null) continue;
      if (!byName.containsKey(ref.parentClass)) {
        throw StateError(
            'ScopeClassRegistry: class "${c.name}" has parentClass '
            '"${ref.parentClass}" which is not a registered class');
      }
      final p = projectionLookup(ref.projection);
      if (p == null) {
        throw StateError(
            'ScopeClassRegistry: class "${c.name}" references projection '
            '"${ref.projection}" which is not a registered projection');
      }
      if (!p.columns.contains(ref.keyColumn)) {
        throw StateError(
            'ScopeClassRegistry: class "${c.name}" containment '
            'keyColumn "${ref.keyColumn}" is not a column of projection '
            '"${ref.projection}" (columns: ${p.columns})');
      }
      if (!p.columns.contains(ref.parentColumn)) {
        throw StateError(
            'ScopeClassRegistry: class "${c.name}" containment '
            'parentColumn "${ref.parentColumn}" is not a column of '
            'projection "${ref.projection}" (columns: ${p.columns})');
      }
    }

    // Cycle check: walk each class's containment chain, refusing repeats.
    for (final start in classes) {
      final seen = <String>{};
      var current = start;
      while (true) {
        if (!seen.add(current.name)) {
          throw StateError(
              'ScopeClassRegistry: containment cycle detected starting '
              'at class "${start.name}"');
        }
        final ref = current.containedIn;
        if (ref == null) break;
        current = byName[ref.parentClass]!;
      }
    }

    return byName;
  }
}
```

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/scope_class_registry.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/scope_class_registry_test.dart
```

Expected: 8 passed.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/scope_class_registry.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/scope_class_registry_test.dart
git commit -m "[CUR-1331] Add ScopeClassRegistry with composition-time validation

Refuses duplicate names, dangling parentClass references, projection
references that don't resolve, missing keyColumn/parentColumn, and
containment cycles.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 2 — Permission, denial reasons, principal cleanup

### Task 5: Reshape `Permission` (drop `scope`, add `scopeClass`)

This is a cascading change — every Permission construction site in the codebase needs updating. The legacy `scope: ScopeClass.X` becomes either `scopeClass: 'site'` for the renamed-from-`site` case, or omitted (defaulting to null = unscoped) for the renamed-from-`global` / `self` cases. The `self` precondition is dropped entirely (was always "principal must be authenticated"; the auth boundary handles this).

**Files:**

- Modify: `event_sourcing/lib/src/actions/permission.dart`
- Modify: every call site of `Permission(...)` — sweep with grep, see Step 2
- Test: `event_sourcing/test/permissions/permission_test.dart` (create if absent)

- [ ] **Step 1: Write the failing test**

Create `event_sourcing/test/permissions/permission_test.dart` (or extend if it exists):

```dart
// Verifies: EVS-PRD-permissions-as-events (Permission carries optional scopeClass identifier; legacy ScopeClass enum removed)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('Permission', () {
    test('unscoped permission has null scopeClass', () {
      const p = Permission('users.provision');
      expect(p.scopeClass, isNull);
      expect(p.name, 'users.provision');
    });

    test('scoped permission carries scopeClass identifier', () {
      const p = Permission('patient.edit', scopeClass: 'patient');
      expect(p.scopeClass, 'patient');
    });

    test('equality is by name only', () {
      const a = Permission('p');
      const b = Permission('p', scopeClass: 'site');
      expect(a, equals(b));
    });

    test('checked() rejects empty name', () {
      expect(() => Permission.checked(''), throwsArgumentError);
      expect(() => Permission.checked('   '), throwsArgumentError);
    });
  });
}
```

- [ ] **Step 2: Sweep call sites of the old shape**

```bash
grep -rn 'Permission(' event_sourcing/lib event_sourcing/test event_sourcing/example_action_permissions event_sourcing/example | grep -v '/build/'
grep -rn 'ScopeClass\.' event_sourcing/lib event_sourcing/test event_sourcing/example_action_permissions event_sourcing/example | grep -v '/build/'
```

Capture every site. The mapping is:

```text
Permission('foo', scope: ScopeClass.global)  -> Permission('foo')
Permission('foo', scope: ScopeClass.site)    -> Permission('foo', scopeClass: 'site')
Permission('foo', scope: ScopeClass.self)    -> Permission('foo')    (self precondition dropped)
```

- [ ] **Step 3: Run the new test to verify it fails**

```bash
cd event_sourcing && flutter test test/permissions/permission_test.dart
```

Expected: FAIL with "The named parameter 'scopeClass' isn't defined" (compile error).

- [ ] **Step 4: Replace `Permission`**

Replace the full contents of `event_sourcing/lib/src/actions/permission.dart`:

```dart
// Implements: EVS-PRD-action-dispatch/B (Permission is the unit checked by the authorize stage; Action.permissions declares what is required)
// Implements: EVS-PRD-permissions-as-events/A (Permission names are the subject of permission-grant events in the same log)
// Implements: EVS-PRD-permissions-as-events/B (AuthorizationPolicy.isPermitted receives Permission; evaluates from event-derived projections)

/// A named permission, by convention `<aggregate>.<verb>` (e.g.
/// `user.invite`, `patient.enroll`). Used by `Action.permissions` to
/// declare what the action requires; used by `AuthorizationPolicy` to
/// decide whether a principal may execute it.
///
/// [scopeClass] (optional) is the name of a registered `ScopeClassSpec`
/// when the permission is scoped (e.g., `patient.edit` is `scopeClass:
/// 'patient'`). Null means unscoped: the permission applies globally and
/// no `ScopeValue` is required at dispatch.
class Permission {
  const Permission(this.name, {this.scopeClass})
      : assert(name != '', 'name must not be empty');

  /// Throws `ArgumentError` if `name` is empty or whitespace-only.
  factory Permission.checked(String name, {String? scopeClass}) {
    if (name.trim().isEmpty) {
      throw ArgumentError.value(
        name,
        'name',
        'must not be empty or whitespace',
      );
    }
    return Permission(name, scopeClass: scopeClass);
  }

  final String name;

  /// The registered ScopeClassSpec name this permission is scoped to,
  /// or null if the permission is unscoped.
  final String? scopeClass;

  @override
  bool operator ==(Object other) => other is Permission && other.name == name;

  @override
  int get hashCode => name.hashCode;

  @override
  String toString() => scopeClass == null
      ? 'Permission($name)'
      : 'Permission($name, scoped: $scopeClass)';
}
```

- [ ] **Step 5: Update every call site identified in Step 2**

For each file from Step 2's grep output, replace the old shape with the new one per the mapping. Run after each file:

```bash
cd event_sourcing && flutter analyze --no-fatal-warnings 2>&1 | grep -E "(error|warning)" | head -20
```

Expected: errors decrease as you fix each site.

- [ ] **Step 6: Run the test**

```bash
cd event_sourcing && flutter test test/permissions/permission_test.dart
```

Expected: 4 passed.

- [ ] **Step 7: Commit**

```bash
git add event_sourcing/lib/src/actions/permission.dart \
        event_sourcing/test/permissions/permission_test.dart \
        $(git diff --name-only event_sourcing/ event_sourcing/example_action_permissions/ event_sourcing/example/)
git commit -m "[CUR-1331] Reshape Permission: drop ScopeClass enum field; add scopeClass identifier

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Refresh `DenyReason`

**Files:**

- Modify: `event_sourcing/lib/src/actions/authorization_decision.dart`
- Modify: `event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart` — drop the `_scopePreconditionMet` usage (TBP will be rewritten in Task 16 anyway, but keep it compilable in the interim)
- Test: any existing test that asserted `DenyReason.sessionPreconditionMissing`

- [ ] **Step 1: Locate references to drop**

```bash
grep -rn 'sessionPreconditionMissing' event_sourcing/
```

- [ ] **Step 2: Modify `DenyReason`**

In `event_sourcing/lib/src/actions/authorization_decision.dart`, replace the `DenyReason` enum:

```dart
enum DenyReason {
  /// Principal's active role doesn't carry the permission, OR no scope
  /// assignment under that role covers the requested scope (including
  /// the fail-closed containment-miss case).
  notGranted,

  /// `Action.scopeFor` returned null for a scoped permission, or
  /// returned a `ScopeValue` whose class does not match the permission's
  /// declared `scopeClass`. A programmer-bug surface.
  scopeUnresolvable,
}
```

- [ ] **Step 3: Update call sites that referenced the dropped reason**

Remove or rewire any test or production code from Step 1's grep output. In `table_backed_authorization_policy.dart`, temporarily replace `DenyReason.sessionPreconditionMissing` with `DenyReason.notGranted` if the file still references it (the policy is fully rewritten in Task 16).

- [ ] **Step 4: Run the full test suite**

```bash
cd event_sourcing && flutter test
```

Expected: tests still pass (modulo the inevitable later-task failures from the cascading reshape; analyze should be clean).

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/actions/authorization_decision.dart \
        event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart \
        $(git diff --name-only event_sourcing/test/)
git commit -m "[CUR-1331] Refresh DenyReason: drop sessionPreconditionMissing, add scopeUnresolvable

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Drop `UserPrincipal.activeSite`

**Files:**

- Modify: `event_sourcing/lib/src/actions/principal.dart`
- Modify: every call site of `UserPrincipal(... activeSite: ...)` — sweep with grep
- Test: `event_sourcing/test/actions/principal_test.dart` (or existing test for Principal)

- [ ] **Step 1: Sweep call sites**

```bash
grep -rn 'activeSite' event_sourcing/
```

- [ ] **Step 2: Modify `Principal`**

In `event_sourcing/lib/src/actions/principal.dart`, replace the `UserPrincipal` class — drop `activeSite` and its handling. Keep `userId`, `roles`, `activeRole`:

```dart
final class UserPrincipal extends Principal {
  const UserPrincipal({
    required this.userId,
    required this.roles,
    required this.activeRole,
  })  : assert(userId != '', 'userId must not be empty'),
        assert(activeRole != '', 'activeRole must not be empty');

  final String userId;
  final Set<String> roles;
  final String activeRole;

  @override
  String get id => userId;

  @override
  Initiator toInitiator() => UserInitiator(userId);
}
```

Also update the `Principal.user` factory in the sealed parent (drop `activeSite` from its parameter list).

- [ ] **Step 3: Update call sites**

For each file from Step 1's grep, remove `activeSite:` from the `UserPrincipal` constructor. Run analyze:

```bash
cd event_sourcing && flutter analyze 2>&1 | grep -E "error" | head -20
```

- [ ] **Step 4: Run the principal test (and any test that constructed UserPrincipal)**

```bash
cd event_sourcing && flutter test test/actions/
```

Expected: all tests in that subtree pass.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/actions/principal.dart \
        $(git diff --name-only event_sourcing/)
git commit -m "[CUR-1331] Drop UserPrincipal.activeSite (legacy hht_diary debt)

Site is no longer a substrate-baked concept. Per the new scope model,
the user's scope assignments are discovered from user_role_scopes
projection at authorize time; there is no per-session 'active site'.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Delete `ScopeClass` enum

**Files:**

- Delete: `event_sourcing/lib/src/actions/scope_class.dart`
- Modify: `event_sourcing/lib/event_sourcing.dart` — drop the export

- [ ] **Step 1: Verify no remaining references**

```bash
grep -rn 'ScopeClass' event_sourcing/ ; echo "exit=$?"
```

Expected: exit 1 (no matches) — all references purged by Tasks 5 and 6.

- [ ] **Step 2: Delete the file and the export**

```bash
git rm event_sourcing/lib/src/actions/scope_class.dart
```

In `event_sourcing/lib/event_sourcing.dart`, remove the `export 'src/actions/scope_class.dart';` line.

- [ ] **Step 3: Run the full test suite**

```bash
cd event_sourcing && flutter test
```

Expected: PASS (everything that referenced ScopeClass is gone).

- [ ] **Step 4: Commit**

```bash
git add event_sourcing/lib/event_sourcing.dart
git commit -m "[CUR-1331] Delete legacy ScopeClass enum (superseded by ScopeClassSpec registry)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 3 — Event payloads (Layer A + Layer B)

### Task 9: Strip `scope` from `PermissionGrantedPayload` / `PermissionRevokedPayload`

**Files:**

- Modify: `event_sourcing/lib/src/permissions/permission_granted_payload.dart`
- Modify: `event_sourcing/lib/src/permissions/permission_revoked_payload.dart`
- Test: existing `permission_granted_payload_test.dart` / `permission_revoked_payload_test.dart`

- [ ] **Step 1: Update tests**

Replace the existing tests in `event_sourcing/test/permissions/permission_granted_payload_test.dart` to remove `scope` from the assertions. The new shape:

```dart
test('toJson/fromJson round-trips', () {
  const p = PermissionGrantedPayload(role: 'SC', permissionName: 'patient.edit');
  final j = p.toJson();
  expect(j, {'role': 'SC', 'permissionName': 'patient.edit'});
  expect(PermissionGrantedPayload.fromJson(j), equals(p));
});

test('fromJson throws on missing role', () {
  expect(
    () => PermissionGrantedPayload.fromJson({'permissionName': 'x'}),
    throwsA(isA<TypeError>().or(isA<FormatException>())),
  );
});
```

Same shape for `permission_revoked_payload_test.dart` (drop scope assertions).

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/permission_granted_payload_test.dart \
                                  test/permissions/permission_revoked_payload_test.dart
```

Expected: FAIL (constructor signature mismatch).

- [ ] **Step 3: Strip `scope` from the payload classes**

Replace `event_sourcing/lib/src/permissions/permission_granted_payload.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events/A — payload for the
// permission_granted event type, which records the grant as an immutable
// log entry. Scope class lives on the registered Permission definition,
// not in the per-grant payload.

import 'package:meta/meta.dart';

@immutable
class PermissionGrantedPayload {
  const PermissionGrantedPayload({
    required this.role,
    required this.permissionName,
  });

  factory PermissionGrantedPayload.fromJson(Map<String, Object?> json) {
    return PermissionGrantedPayload(
      role: json['role']! as String,
      permissionName: json['permissionName']! as String,
    );
  }

  final String role;
  final String permissionName;

  Map<String, Object?> toJson() => <String, Object?>{
        'role': role,
        'permissionName': permissionName,
      };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PermissionGrantedPayload &&
          role == other.role &&
          permissionName == other.permissionName;

  @override
  int get hashCode => Object.hash(role, permissionName);
}
```

Apply the parallel change to `permission_revoked_payload.dart` (it has the same shape; just drop `scope` if present).

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/permission_granted_payload_test.dart \
                                  test/permissions/permission_revoked_payload_test.dart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/permissions/permission_granted_payload.dart \
        event_sourcing/lib/src/permissions/permission_revoked_payload.dart \
        event_sourcing/test/permissions/permission_granted_payload_test.dart \
        event_sourcing/test/permissions/permission_revoked_payload_test.dart
git commit -m "[CUR-1331] Strip scope field from permission_granted / permission_revoked payloads

Scope class now lives on the registered Permission definition (code),
not per-grant. Payload simplifies to (role, permission_name).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Canonical-JSON aggregate-id encoder for role assignments

**Files:**

- Create: `event_sourcing/lib/src/permissions/role_assignment_aggregate_id.dart`
- Test: `event_sourcing/test/permissions/role_assignment_aggregate_id_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (aggregate-id encoding is collision-free)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('roleAssignmentAggregateId', () {
    test('encodes a bound scope as canonical JSON', () {
      final id = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'),
      );
      // Order is canonical (JCS); spaces normalized.
      expect(id, '{"role":"SC","scope":{"class":"site","value":"A"},"user_id":"U1"}');
    });

    test('encodes a value-wildcard scope', () {
      final id = roleAssignmentAggregateId(
        userId: 'U1',
        role: 'SC',
        scope: const ValueWildcardScope(class_: 'site'),
      );
      expect(id, contains('"wildcard_value":true'));
      expect(id, contains('"class":"site"'));
    });

    test('encodes a total wildcard scope', () {
      final id = roleAssignmentAggregateId(
        userId: 'U2',
        role: 'ADMIN',
        scope: const TotalWildcardScope(),
      );
      expect(id, contains('"wildcard_class":true'));
    });

    test('distinct tuples produce distinct ids', () {
      final a = roleAssignmentAggregateId(
        userId: 'U1', role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A:B'));
      final b = roleAssignmentAggregateId(
        userId: 'U1', role: 'SC',
        scope: const BoundScope(class_: 'site-X', value: 'A'));
      expect(a, isNot(equals(b)));
    });

    test('same tuple produces identical id', () {
      final a = roleAssignmentAggregateId(
        userId: 'U1', role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'));
      final b = roleAssignmentAggregateId(
        userId: 'U1', role: 'SC',
        scope: const BoundScope(class_: 'site', value: 'A'));
      expect(a, equals(b));
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/role_assignment_aggregate_id_test.dart
```

Expected: FAIL with undefined `roleAssignmentAggregateId`.

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/role_assignment_aggregate_id.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events (aggregate-id for role_assigned / role_unassigned events)

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';

/// Canonical-JSON encoding of the (user_id, role, scope) tuple. Used as
/// the aggregate id for `role_assigned` and `role_unassigned` events so
/// that the projection's insert/remove discipline keys per-tuple
/// uniqueness without segment-encoding ambiguity.
String roleAssignmentAggregateId({
  required String userId,
  required String role,
  required ScopeValue scope,
}) {
  final m = <String, Object?>{
    'user_id': userId,
    'role': role,
    'scope': scope.toJson(),
  };
  return canonicalJson(m);
}
```

(If the canonical-JSON function is named differently in `canonical_json_jcs`, check its public surface with `grep -n "^[A-Za-z].*=" canonical_json_jcs/lib/canonical_json_jcs.dart` and adapt.)

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/role_assignment_aggregate_id.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/role_assignment_aggregate_id_test.dart
```

Expected: 5 passed.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/role_assignment_aggregate_id.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/role_assignment_aggregate_id_test.dart
git commit -m "[CUR-1331] Add canonical-JSON aggregate-id encoder for role assignments

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: `RoleAssignedPayload`

**Files:**

- Create: `event_sourcing/lib/src/permissions/role_assigned_payload.dart`
- Test: `event_sourcing/test/permissions/role_assigned_payload_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (role_assigned payload shape)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('RoleAssignedPayload', () {
    test('round-trips a bound scope', () {
      const p = RoleAssignedPayload(
        userId: 'U1',
        role: 'SC',
        scope: BoundScope(class_: 'site', value: 'A'),
      );
      final j = p.toJson();
      expect(j['user_id'], 'U1');
      expect(j['role'], 'SC');
      expect(j['scope'], {'class': 'site', 'value': 'A'});
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('round-trips a value-wildcard scope', () {
      const p = RoleAssignedPayload(
        userId: 'U1', role: 'SC',
        scope: ValueWildcardScope(class_: 'site'));
      final j = p.toJson();
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('round-trips a total wildcard scope', () {
      const p = RoleAssignedPayload(
        userId: 'U2', role: 'ADMIN', scope: TotalWildcardScope());
      final j = p.toJson();
      expect(RoleAssignedPayload.fromJson(j), equals(p));
    });

    test('fromJson rejects missing user_id', () {
      expect(
        () => RoleAssignedPayload.fromJson({'role': 'r',
            'scope': {'wildcard_class': true}}),
        throwsA(isA<TypeError>().or(isA<FormatException>())),
      );
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/role_assigned_payload_test.dart
```

Expected: FAIL with undefined class.

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/role_assigned_payload.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events/A — payload for role_assigned events
// (user-to-role assignment with scope binding, append-only event in the log).

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:meta/meta.dart';

@immutable
class RoleAssignedPayload {
  const RoleAssignedPayload({
    required this.userId,
    required this.role,
    required this.scope,
  })  : assert(userId != '', 'userId must not be empty'),
        assert(role != '', 'role must not be empty');

  factory RoleAssignedPayload.fromJson(Map<String, Object?> json) {
    return RoleAssignedPayload(
      userId: json['user_id']! as String,
      role: json['role']! as String,
      scope: ScopeValue.fromJson(
          (json['scope']! as Map).cast<String, Object?>()),
    );
  }

  final String userId;
  final String role;
  final ScopeValue scope;

  Map<String, Object?> toJson() => <String, Object?>{
        'user_id': userId,
        'role': role,
        'scope': scope.toJson(),
      };

  @override
  bool operator ==(Object other) =>
      other is RoleAssignedPayload &&
      userId == other.userId &&
      role == other.role &&
      scope == other.scope;

  @override
  int get hashCode => Object.hash(userId, role, scope);
}
```

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/role_assigned_payload.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/role_assigned_payload_test.dart
```

Expected: 4 passed.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/role_assigned_payload.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/role_assigned_payload_test.dart
git commit -m "[CUR-1331] Add RoleAssignedPayload

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 12: `RoleUnassignedPayload`

Identical shape to Task 11; identical fields; the only semantic difference is that the projection treats it as a remove event.

**Files:**

- Create: `event_sourcing/lib/src/permissions/role_unassigned_payload.dart`
- Test: `event_sourcing/test/permissions/role_unassigned_payload_test.dart`

- [ ] **Step 1: Write failing tests**

Copy the test file from Task 11 to `role_unassigned_payload_test.dart` and replace `RoleAssignedPayload` with `RoleUnassignedPayload` throughout.

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/role_unassigned_payload_test.dart
```

- [ ] **Step 3: Create the implementation**

Copy `role_assigned_payload.dart` to `role_unassigned_payload.dart` and rename the class. The serialized field shape is identical.

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/role_unassigned_payload.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/role_unassigned_payload_test.dart
```

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/role_unassigned_payload.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/role_unassigned_payload_test.dart
git commit -m "[CUR-1331] Add RoleUnassignedPayload

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 4 — Projection for user_role_scopes

### Task 13: `user_role_scopes` `TableProjectionSpec`

**Files:**

- Create: `event_sourcing/lib/src/permissions/user_role_scopes_spec.dart`
- Test: `event_sourcing/test/permissions/user_role_scopes_spec_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (user_role_scopes projection insert/remove discipline)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('userRoleScopesSpec', () {
    test('has correct view name and interest filter', () {
      expect(userRoleScopesSpec.viewName, 'user_role_scopes');
      expect(userRoleScopesSpec.interest.eventTypes,
          {'role_assigned', 'role_unassigned'});
      expect(userRoleScopesSpec.interest.aggregateTypes, {'user_role_scope'});
      expect(userRoleScopesSpec.insertEventTypes, {'role_assigned'});
      expect(userRoleScopesSpec.removeEventTypes, {'role_unassigned'});
    });
  });
}
```

Add an end-to-end fold test using the existing sembast harness (see `event_sourcing/test/permissions/test_support/sembast_event_store_harness.dart`):

```dart
test('appending role_assigned upserts a row keyed by aggregate id', () async {
  final harness = await SembastEventStoreHarness.create(
    projectionSpecs: [userRoleScopesSpec],
  );
  await harness.append(
    aggregateType: 'user_role_scope',
    aggregateId: roleAssignmentAggregateId(
      userId: 'U1', role: 'SC',
      scope: const BoundScope(class_: 'site', value: 'A'),
    ),
    eventType: 'role_assigned',
    payload: const RoleAssignedPayload(
      userId: 'U1', role: 'SC',
      scope: BoundScope(class_: 'site', value: 'A'),
    ).toJson(),
  );
  final rows = await harness.findRows('user_role_scopes');
  expect(rows, hasLength(1));
  expect(rows.single['user_id'], 'U1');
  expect(rows.single['role'], 'SC');
});

test('appending role_unassigned removes the matching row', () async {
  final harness = await SembastEventStoreHarness.create(
    projectionSpecs: [userRoleScopesSpec],
  );
  final aggId = roleAssignmentAggregateId(
    userId: 'U1', role: 'SC',
    scope: const BoundScope(class_: 'site', value: 'A'),
  );
  await harness.append(
    aggregateType: 'user_role_scope', aggregateId: aggId,
    eventType: 'role_assigned',
    payload: const RoleAssignedPayload(
      userId: 'U1', role: 'SC',
      scope: BoundScope(class_: 'site', value: 'A'),
    ).toJson(),
  );
  await harness.append(
    aggregateType: 'user_role_scope', aggregateId: aggId,
    eventType: 'role_unassigned',
    payload: const RoleUnassignedPayload(
      userId: 'U1', role: 'SC',
      scope: BoundScope(class_: 'site', value: 'A'),
    ).toJson(),
  );
  expect(await harness.findRows('user_role_scopes'), isEmpty);
});
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/user_role_scopes_spec_test.dart
```

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/user_role_scopes_spec.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events/A — user-role-scope assignments
//   are events in the same log as application state changes.
// Implements: EVS-PRD-permissions-as-events/B — the substrate's authorize
//   stage reads user_role_scopes (via TableBackedAuthorizationPolicy) to
//   evaluate scope coverage.
// Implements: EVS-PRD-permissions-as-events/C — the projection is fully
//   reconstructable from the event log alone.

import 'package:event_sourcing/src/destinations/subscription_filter.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';

final userRoleScopesSpec = TableProjectionSpec(
  viewName: 'user_role_scopes',
  interest: const SubscriptionFilter(
    eventTypes: {'role_assigned', 'role_unassigned'},
    aggregateTypes: {'user_role_scope'},
  ),
  insertEventTypes: const {'role_assigned'},
  removeEventTypes: const {'role_unassigned'},
  rowKey: const AggregateIdKey(),
  rowData: const WholePayload(),
);
```

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/user_role_scopes_spec.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/user_role_scopes_spec_test.dart
```

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/user_role_scopes_spec.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/user_role_scopes_spec_test.dart
git commit -m "[CUR-1331] Add user_role_scopes TableProjectionSpec

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 5 — ContainmentResolver

### Task 14: `ContainmentResolver`

**Files:**

- Create: `event_sourcing/lib/src/permissions/containment_resolver.dart`
- Test: `event_sourcing/test/permissions/containment_resolver_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (containment lookup via projection; fail-closed on miss)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _FakeBackend {
  _FakeBackend(this.rows);
  final Map<String, List<Map<String, dynamic>>> rows;

  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Object txn, String viewName,
    {Map<String, Object?>? where, int? limit, int? offset}) async {
    final all = rows[viewName] ?? [];
    if (where == null) return all;
    return all
        .where((r) => where.entries.every((e) => r[e.key] == e.value))
        .toList();
  }
}

void main() {
  group('ContainmentResolver', () {
    test('resolves a single-hop containment', () async {
      final reg = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentRef(
              parentClass: 'site',
              projection: 'patient_site_index',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (n) =>
            const _FakeDescriptor(columns: {'patient_id', 'site_id'}),
      );
      final backend = _FakeBackend({
        'patient_site_index': [
          {'patient_id': 'P-42', 'site_id': 'A'},
        ],
      });
      final resolver = ContainmentResolver(
        registry: reg,
        // The resolver takes a function-shaped backend dependency to keep
        // it unit-testable; the production policy wires the real backend.
        findRowsInTxn: backend.findViewRowsInTxn,
      );
      final result = await resolver.resolve(
        txn: Object(),
        from: const BoundScope(class_: 'patient', value: 'P-42'),
        target: 'site',
      );
      expect(result, equals(const BoundScope(class_: 'site', value: 'A')));
    });

    test('resolves a two-hop containment', () async {
      final reg = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'region'),
          ScopeClassSpec(
            name: 'site',
            containedIn: ContainmentRef(
              parentClass: 'region',
              projection: 'site_region',
              keyColumn: 'site_id',
              parentColumn: 'region_id',
            ),
          ),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentRef(
              parentClass: 'site',
              projection: 'patient_site',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (n) => const _FakeDescriptor(
            columns: {'patient_id', 'site_id', 'region_id'}),
      );
      final backend = _FakeBackend({
        'patient_site': [
          {'patient_id': 'P-42', 'site_id': 'A'},
        ],
        'site_region': [
          {'site_id': 'A', 'region_id': 'East'},
        ],
      });
      final resolver = ContainmentResolver(
          registry: reg, findRowsInTxn: backend.findViewRowsInTxn);
      final result = await resolver.resolve(
        txn: Object(),
        from: const BoundScope(class_: 'patient', value: 'P-42'),
        target: 'region',
      );
      expect(result, equals(const BoundScope(class_: 'region', value: 'East')));
    });

    test('returns null when intermediate row is missing (fail-closed)',
        () async {
      final reg = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentRef(
              parentClass: 'site',
              projection: 'patient_site',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (n) =>
            const _FakeDescriptor(columns: {'patient_id', 'site_id'}),
      );
      final backend = _FakeBackend({'patient_site': []});
      final resolver = ContainmentResolver(
          registry: reg, findRowsInTxn: backend.findViewRowsInTxn);
      final result = await resolver.resolve(
        txn: Object(),
        from: const BoundScope(class_: 'patient', value: 'P-42'),
        target: 'site',
      );
      expect(result, isNull);
    });

    test('returns from itself when target equals from.class_', () async {
      final reg = ScopeClassRegistry(
          classes: const [ScopeClassSpec(name: 'site')],
          projectionLookup: (_) => null);
      final resolver = ContainmentResolver(
          registry: reg, findRowsInTxn: (_, __, {where, limit, offset}) async => []);
      final result = await resolver.resolve(
        txn: Object(),
        from: const BoundScope(class_: 'site', value: 'A'),
        target: 'site',
      );
      expect(result, equals(const BoundScope(class_: 'site', value: 'A')));
    });

    test('returns null when target is not in from\'s ancestor chain',
        () async {
      final reg = ScopeClassRegistry(classes: const [
        ScopeClassSpec(name: 'site'),
        ScopeClassSpec(name: 'lab'),
      ], projectionLookup: (_) => null);
      final resolver = ContainmentResolver(
          registry: reg, findRowsInTxn: (_, __, {where, limit, offset}) async => []);
      final result = await resolver.resolve(
        txn: Object(),
        from: const BoundScope(class_: 'site', value: 'A'),
        target: 'lab',
      );
      expect(result, isNull);
    });
  });
}

class _FakeDescriptor implements ScopeProjectionDescriptor {
  const _FakeDescriptor({required this.columns});
  @override
  final Set<String> columns;
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/containment_resolver_test.dart
```

Expected: FAIL with undefined `ContainmentResolver`.

- [ ] **Step 3: Create the implementation**

Create `event_sourcing/lib/src/permissions/containment_resolver.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events (substrate-evaluated containment lookup via TableProjections; fail-closed on missing row)

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/scope_class_registry.dart';

/// Signature matching `StorageBackend.findViewRowsInTxn`. Passed by the
/// production policy; unit tests provide a fake.
typedef FindRowsInTxn = Future<List<Map<String, dynamic>>> Function(
  Object txn,
  String viewName, {
  Map<String, Object?>? where,
  int? limit,
  int? offset,
});

/// Walks the containment chain from a [BoundScope]'s class up toward a
/// target class, reading each hop's parent value from the
/// `ContainmentRef.projection`. Returns the resolved ancestor scope or
/// null if any hop misses (fail-closed).
class ContainmentResolver {
  ContainmentResolver({
    required this.registry,
    required this.findRowsInTxn,
  });

  final ScopeClassRegistry registry;
  final FindRowsInTxn findRowsInTxn;

  Future<BoundScope?> resolve({
    required Object txn,
    required BoundScope from,
    required String target,
  }) async {
    if (from.class_ == target) return from;
    if (!registry.isAncestor(target, from.class_)) return null;

    var current = from;
    while (current.class_ != target) {
      final spec = registry.byName(current.class_);
      final ref = spec?.containedIn;
      if (ref == null) return null;
      final rows = await findRowsInTxn(
        txn,
        ref.projection,
        where: {ref.keyColumn: current.value},
        limit: 1,
      );
      if (rows.isEmpty) return null;
      final parentValue = rows.single[ref.parentColumn];
      if (parentValue is! String || parentValue.isEmpty) return null;
      current = BoundScope(class_: ref.parentClass, value: parentValue);
    }
    return current;
  }
}
```

- [ ] **Step 4: Export from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/containment_resolver.dart';
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/containment_resolver_test.dart
```

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/containment_resolver.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/containment_resolver_test.dart
git commit -m "[CUR-1331] Add ContainmentResolver

Walks the class-containment chain from from.class_ up to target,
reading each hop from its declared TableProjection. Fail-closed on
missing rows.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 6 — Authorization policy rewrite

### Task 15: `EffectiveAuthorization` and `ScopeAssignment`

**Files:**

- Create: `event_sourcing/lib/src/permissions/scope_assignment.dart`
- Create: `event_sourcing/lib/src/permissions/effective_authorization.dart`

These are plain data classes; no separate test file (covered by policy tests).

- [ ] **Step 1: Create `scope_assignment.dart`**

```dart
// Implements: EVS-PRD-permissions-as-events (raw assignment record exposed to clients/UI via effectivePermissionsFor)

import 'package:event_sourcing/src/actions/scope_value.dart';

/// One row of the user's scope assignments under their active role,
/// surfaced to clients via [EffectiveAuthorization].
class ScopeAssignment {
  const ScopeAssignment({required this.scope});

  final ScopeValue scope;

  @override
  bool operator ==(Object other) =>
      other is ScopeAssignment && scope == other.scope;

  @override
  int get hashCode => scope.hashCode;

  @override
  String toString() => 'ScopeAssignment($scope)';
}
```

- [ ] **Step 2: Create `effective_authorization.dart`**

```dart
// Implements: EVS-PRD-permissions-as-events (effectivePermissionsFor surface for client-side UI gating)

import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/permissions/scope_assignment.dart';

/// Materials a client needs to gate UI per scope: the active role's
/// permission set plus the user's scope assignments under that role.
/// Apps compose this with their own data projections to build
/// "items I can act on" lists; per-decision gating still goes through
/// `AuthorizationPolicy.isPermitted`.
class EffectiveAuthorization {
  const EffectiveAuthorization({
    required this.activeRole,
    required this.rolePermissions,
    required this.scopeAssignments,
  });

  final String activeRole;
  final Set<Permission> rolePermissions;
  final List<ScopeAssignment> scopeAssignments;

  static const EffectiveAuthorization empty = EffectiveAuthorization(
    activeRole: '',
    rolePermissions: <Permission>{},
    scopeAssignments: <ScopeAssignment>[],
  );
}
```

- [ ] **Step 3: Export both from the package**

Add to `event_sourcing/lib/event_sourcing.dart`:

```dart
export 'src/permissions/scope_assignment.dart';
export 'src/permissions/effective_authorization.dart';
```

- [ ] **Step 4: Run analyze**

```bash
cd event_sourcing && flutter analyze
```

Expected: no new errors.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/permissions/scope_assignment.dart \
        event_sourcing/lib/src/permissions/effective_authorization.dart \
        event_sourcing/lib/event_sourcing.dart
git commit -m "[CUR-1331] Add ScopeAssignment and EffectiveAuthorization types

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: Reshape `AuthorizationPolicy` interface

**Files:**

- Modify: `event_sourcing/lib/src/actions/authorization_policy.dart`

This breaks all implementers; the next two tasks (TableBacked, FailSafe) fix them.

- [ ] **Step 1: Replace the interface**

Replace `event_sourcing/lib/src/actions/authorization_policy.dart`:

```dart
// Implements: EVS-PRD-action-dispatch/B (authorize stage pluggable interface)
// Implements: EVS-PRD-permissions-as-events/B (concrete impls evaluate decisions from event-derived projections only)
// Implements: EVS-PRD-library-charter/H (trust-boundary interface: AuthorizationPolicy is the named, registered policy surface)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/effective_authorization.dart';

/// Pluggable authorization decision-maker. Concrete impls live in the
/// permissions module within `event_sourcing`
/// (`TableBackedAuthorizationPolicy` over storage; `FailSafeAuthorizationPolicy`
/// for boot-failure).
abstract class AuthorizationPolicy {
  const AuthorizationPolicy();

  /// Decide whether [principal] may exercise [permission] against the
  /// optional [scopeValue]. The dispatcher guarantees scopeValue is
  /// non-null iff permission.scopeClass is non-null; impls may assert
  /// this invariant.
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  );

  /// Materials for client-side UI gating and app-side scope-aware
  /// queries: the active role's permission set + the user's scope
  /// assignments under that role.
  Future<EffectiveAuthorization> effectivePermissionsFor(Principal principal);
}
```

- [ ] **Step 2: Run analyze (expect breaks)**

```bash
cd event_sourcing && flutter analyze 2>&1 | grep "error" | head -20
```

Expected: errors at the two implementers (TableBacked, FailSafe). Tasks 17-18 fix them.

- [ ] **Step 3: Commit (interim broken state is intentional and contained)**

```bash
git add event_sourcing/lib/src/actions/authorization_policy.dart
git commit -m "[CUR-1331] Reshape AuthorizationPolicy interface for scope-aware permissions

isPermitted now takes a ScopeValue?; permissionsFor replaced by
effectivePermissionsFor. Implementers in following commits.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 17: Rewrite `TableBackedAuthorizationPolicy`

**Files:**

- Modify: `event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart` (full rewrite)
- Modify: `event_sourcing/lib/src/permissions/role_matrix_reader.dart` and impls — drop `scope` parameter (since `isGranted` no longer takes it)
- Test: `event_sourcing/test/permissions/table_backed_authorization_policy_test.dart` (rewrite)

This is the largest single change. The policy now reads `role_permission_grants` + `user_role_scopes` + (optionally) containment projections through the storage backend's `findViewRowsInTxn`. The match algorithm is per the spec.

- [ ] **Step 1: Drop scope from `RoleMatrixReader` and impls**

In `event_sourcing/lib/src/permissions/role_matrix_reader.dart`, remove any scope parameters from `isGranted` / `grantsForRole`. The role matrix only answers "does role R carry permission P?" now.

In each concrete reader (`InMemoryRoleMatrixReader`, `MaterializedViewRoleMatrixReader`, `SnapshotRoleMatrixReader`), drop scope handling.

Run `flutter analyze` after each file edit; the analyzer guides you to remaining references.

- [ ] **Step 2: Write the failing policy test**

Replace `event_sourcing/test/permissions/table_backed_authorization_policy_test.dart` with the match-algorithm scenarios from the subagent review:

```dart
// Verifies: EVS-PRD-permissions-as-events (match algorithm: equality, wildcard, containment, fail-closed)
// Verifies: EVS-PRD-action-dispatch/B (authorize stage receives Allow/Deny decisions)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('TableBackedAuthorizationPolicy match algorithm', () {
    // Helper builds a fully-wired policy + storage harness with the
    // standard hht_diary-like containment: patient -> site.
    Future<_PolicyHarness> harness({
      required List<_Grant> grants,
      required List<_Assignment> assignments,
      required Map<String, String> patientToSite, // patient_id -> site_id
    }) async => _PolicyHarness.create(
        grants: grants,
        assignments: assignments,
        patientToSite: patientToSite);

    test('bound site assignment + patient-scoped permission via containment '
        '-> Allow when patient is at the assigned site', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [_Assignment(
          userId: 'U1', role: 'SC',
          scope: const BoundScope(class_: 'site', value: 'A'))],
        patientToSite: {'P-42': 'A'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('bound site assignment but patient is at a different site '
        '-> Deny(notGranted)', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [_Assignment(
          userId: 'U1', role: 'SC',
          scope: const BoundScope(class_: 'site', value: 'A'))],
        patientToSite: {'P-42': 'B'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Deny>().having(
          (d) => d.reason, 'reason', DenyReason.notGranted));
    });

    test('TotalWildcardScope assignment -> Allow on any scoped permission',
        () async {
      final h = await harness(
        grants: [_Grant(role: 'SUP', perm: 'patient.view')],
        assignments: [_Assignment(
          userId: 'U2', role: 'SUP', scope: const TotalWildcardScope())],
        patientToSite: {'P-42': 'A'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U2', 'SUP'),
        const Permission('patient.view', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('ValueWildcardScope(class=site) assignment -> Allow for any patient '
        'via containment (any site covers any patient at any site)', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [_Assignment(
          userId: 'U3', role: 'SC',
          scope: const ValueWildcardScope(class_: 'site'))],
        patientToSite: {'P-42': 'B'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U3', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('patient-scoped assignment cannot cover site-scoped permission '
        '(narrower than) -> Deny', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'site.read')],
        assignments: [_Assignment(
          userId: 'U4', role: 'SC',
          scope: const BoundScope(class_: 'patient', value: 'P-42'))],
        patientToSite: {'P-42': 'A'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U4', 'SC'),
        const Permission('site.read', scopeClass: 'site'),
        const BoundScope(class_: 'site', value: 'A'),
      );
      expect(decision, isA<Deny>());
    });

    test('containment lookup miss -> fail-closed Deny(notGranted)', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [_Assignment(
          userId: 'U1', role: 'SC',
          scope: const BoundScope(class_: 'site', value: 'A'))],
        patientToSite: {}, // no row for P-42
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Deny>().having(
          (d) => d.reason, 'reason', DenyReason.notGranted));
    });

    test('union within active role: U1 has SC at site A AND site B; '
        'P-42 is at B -> Allow', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'patient.edit')],
        assignments: [
          _Assignment(userId: 'U1', role: 'SC',
              scope: const BoundScope(class_: 'site', value: 'A')),
          _Assignment(userId: 'U1', role: 'SC',
              scope: const BoundScope(class_: 'site', value: 'B')),
        ],
        patientToSite: {'P-42': 'B'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      expect(decision, isA<Allow>());
    });

    test('active role filter: U1 holds SC AND SUP, but activeRole=SC; '
        'SUP perms are ignored', () async {
      final h = await harness(
        grants: [
          _Grant(role: 'SC', perm: 'patient.view'),
          _Grant(role: 'SUP', perm: 'patient.edit'),
        ],
        assignments: [
          _Assignment(userId: 'U1', role: 'SC',
              scope: const BoundScope(class_: 'site', value: 'A')),
          _Assignment(userId: 'U1', role: 'SUP',
              scope: const TotalWildcardScope()),
        ],
        patientToSite: {'P-42': 'A'},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('patient.edit', scopeClass: 'patient'),
        const BoundScope(class_: 'patient', value: 'P-42'),
      );
      // SC does NOT carry patient.edit; SUP does, but is not active.
      expect(decision, isA<Deny>().having(
          (d) => d.reason, 'reason', DenyReason.notGranted));
    });

    test('unscoped permission with role grant -> Allow', () async {
      final h = await harness(
        grants: [_Grant(role: 'SC', perm: 'report.generate')],
        assignments: [_Assignment(
            userId: 'U1', role: 'SC',
            scope: const BoundScope(class_: 'site', value: 'A'))],
        patientToSite: {},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('report.generate'),  // scopeClass: null
        null,
      );
      expect(decision, isA<Allow>());
    });

    test('unscoped permission without role grant -> Deny', () async {
      final h = await harness(
        grants: [],  // role has no grants at all
        assignments: [_Assignment(
            userId: 'U1', role: 'SC',
            scope: const BoundScope(class_: 'site', value: 'A'))],
        patientToSite: {},
      );
      final decision = await h.policy.isPermitted(
        h.user('U1', 'SC'),
        const Permission('report.generate'),
        null,
      );
      expect(decision, isA<Deny>());
    });
  });

  group('TableBackedAuthorizationPolicy.effectivePermissionsFor', () {
    test('returns active role permissions + user\'s assignments for it',
        () async {
      // ... assert shape: rolePermissions matches grants for activeRole;
      //     scopeAssignments matches user_role_scopes filtered by
      //     (user_id, role=activeRole).
    });

    test('returns empty for AnonymousPrincipal', () async {
      // ...
    });
  });
}

// _PolicyHarness, _Grant, _Assignment are test fixtures that:
// - construct a sembast-backed event store
// - register role_permission_grants_spec, user_role_scopes_spec, and a
//   patient_site_index TableProjectionSpec
// - register a ScopeClassRegistry with site (top-level) and patient
//   (contained in site via patient_site_index)
// - emit the grant/assignment/containment events
// - wait for projection catch-up
// - construct TableBackedAuthorizationPolicy(backend, scopeClassRegistry)
//
// Implement inline in this file or as a helper under test_support/.
```

The test fixture (`_PolicyHarness`) is sizable; implement it under `event_sourcing/test/permissions/test_support/policy_harness.dart` and import from the test file. Use `SembastEventStoreHarness` as the base (it already exists for projection tests).

- [ ] **Step 3: Run tests to verify they fail**

```bash
cd event_sourcing && flutter test test/permissions/table_backed_authorization_policy_test.dart
```

Expected: FAIL (the policy still has the old signature; impl not yet rewritten).

- [ ] **Step 4: Rewrite `TableBackedAuthorizationPolicy`**

Replace `event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart`:

```dart
// Implements: EVS-PRD-permissions-as-events/B — evaluates authorization
//   decisions solely from event-derived projections (role_permission_grants,
//   user_role_scopes, and containment projections via ContainmentResolver).
// Implements: EVS-PRD-permissions-as-events/A — reads grants and assignments
//   that are themselves recorded as events.
// Implements: EVS-PRD-action-dispatch/B — Allow/Deny decisions delivered to
//   the dispatcher's authorize stage.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';

class TableBackedAuthorizationPolicy implements AuthorizationPolicy {
  TableBackedAuthorizationPolicy({
    required this.backend,
    required this.scopeClassRegistry,
    required this.txnProvider,
  }) : _resolver = ContainmentResolver(
          registry: scopeClassRegistry,
          findRowsInTxn: backend.findViewRowsInTxn,
        );

  final StorageBackend backend;
  final ScopeClassRegistry scopeClassRegistry;

  /// In production the dispatcher passes the active storage transaction.
  /// Tests can pass a one-shot supplier that opens a tx per call.
  final Future<T> Function<T>(Future<T> Function(Txn txn)) txnProvider;

  final ContainmentResolver _resolver;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async {
    // 1. Anonymous principals carry no role assignments.
    if (principal is! UserPrincipal) {
      return Deny(permission: permission, reason: DenyReason.notGranted);
    }

    // 2. Invariant: scopeValue non-null iff permission.scopeClass non-null.
    if ((permission.scopeClass == null) != (scopeValue == null)) {
      return Deny(
          permission: permission, reason: DenyReason.scopeUnresolvable);
    }

    return txnProvider((txn) async {
      // 3. Role-level grant: does the active role carry this permission name?
      final grants = await backend.findViewRowsInTxn(
        txn, 'role_permission_grants',
        where: {'role': principal.activeRole,
                'permissionName': permission.name},
        limit: 1,
      );
      if (grants.isEmpty) {
        return Deny(permission: permission, reason: DenyReason.notGranted);
      }

      // 4. Unscoped permission: role grant is sufficient.
      if (permission.scopeClass == null) {
        return const Allow();
      }

      // 5. Scoped permission: enumerate user's assignments under active role.
      final assignments = await backend.findViewRowsInTxn(
        txn, 'user_role_scopes',
        where: {'user_id': principal.userId, 'role': principal.activeRole},
      );
      if (assignments.isEmpty) {
        return Deny(permission: permission, reason: DenyReason.notGranted);
      }

      // 6. Match (first-match-wins = union semantics).
      final requested = scopeValue!;
      for (final row in assignments) {
        final assignedScope = ScopeValue.fromJson(
            (row['scope'] as Map).cast<String, Object?>());
        if (await _matches(txn, assigned: assignedScope, requested: requested)) {
          return const Allow();
        }
      }
      return Deny(permission: permission, reason: DenyReason.notGranted);
    });
  }

  Future<bool> _matches(
    Txn txn, {
    required ScopeValue assigned,
    required ScopeValue requested,
  }) async {
    // requested is always BoundScope coming from action.scopeFor; defensive:
    if (requested is! BoundScope) return false;

    switch (assigned) {
      case TotalWildcardScope():
        return true;
      case ValueWildcardScope(class_: final ac):
        if (ac == requested.class_) return true;
        if (scopeClassRegistry.isAncestor(ac, requested.class_)) {
          // Any value of an ancestor class matches any descendant.
          return true;
        }
        return false;
      case BoundScope(class_: final ac, value: final av):
        if (ac == requested.class_ && av == requested.value) return true;
        if (scopeClassRegistry.isAncestor(ac, requested.class_)) {
          final resolved = await _resolver.resolve(
            txn: txn, from: requested, target: ac);
          return resolved?.value == av;
        }
        return false;
    }
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
      Principal principal) async {
    if (principal is! UserPrincipal) {
      return EffectiveAuthorization.empty;
    }
    return txnProvider((txn) async {
      final grants = await backend.findViewRowsInTxn(
        txn, 'role_permission_grants',
        where: {'role': principal.activeRole},
      );
      final perms = <Permission>{
        for (final g in grants)
          Permission(g['permissionName']! as String),
      };
      final assignmentRows = await backend.findViewRowsInTxn(
        txn, 'user_role_scopes',
        where: {'user_id': principal.userId, 'role': principal.activeRole},
      );
      final assignments = <ScopeAssignment>[
        for (final r in assignmentRows)
          ScopeAssignment(
              scope: ScopeValue.fromJson(
                  (r['scope'] as Map).cast<String, Object?>())),
      ];
      return EffectiveAuthorization(
        activeRole: principal.activeRole,
        rolePermissions: perms,
        scopeAssignments: assignments,
      );
    });
  }
}
```

Notes:

- The `txnProvider` parameter is the seam that lets the dispatcher pass its shared transaction (Phase 8). For unit tests, the harness provides a one-shot supplier (`<T>(f) => backend.runInTxn(f)`).
- `Permission(g['permissionName']! as String)` in `effectivePermissionsFor` does NOT carry scopeClass because the grant row doesn't store it (scope class is code-registered). If the caller needs full scopeClass info, they look it up against their own `Permission` registry. Document this on `EffectiveAuthorization`.

- [ ] **Step 5: Run tests to verify they pass**

```bash
cd event_sourcing && flutter test test/permissions/table_backed_authorization_policy_test.dart
```

Expected: all 10+ tests pass.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/permissions/table_backed_authorization_policy.dart \
        event_sourcing/lib/src/permissions/role_matrix_reader.dart \
        event_sourcing/lib/src/permissions/in_memory_role_matrix_reader.dart \
        event_sourcing/lib/src/permissions/materialized_view_role_matrix_reader.dart \
        event_sourcing/lib/src/permissions/snapshot_role_matrix_reader.dart \
        event_sourcing/test/permissions/table_backed_authorization_policy_test.dart \
        event_sourcing/test/permissions/test_support/policy_harness.dart
git commit -m "[CUR-1331] Rewrite TableBackedAuthorizationPolicy for scope-aware permissions

Match algorithm: equality + value-wildcard + total-wildcard + hierarchy
containment via ContainmentResolver. Reads role_permission_grants and
user_role_scopes via findViewRowsInTxn so the authorize stage runs
inside the dispatch transaction.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 18: Update `FailSafeAuthorizationPolicy`

**Files:**

- Modify: `event_sourcing/lib/src/permissions/fail_safe_authorization_policy.dart`
- Modify: `event_sourcing/test/permissions/fail_safe_authorization_policy_test.dart`

- [ ] **Step 1: Update tests**

Replace the failsafe test contents:

```dart
test('isPermitted always denies', () async {
  const policy = FailSafeAuthorizationPolicy();
  final d = await policy.isPermitted(
    const UserPrincipal(userId: 'U', roles: {'r'}, activeRole: 'r'),
    const Permission('any'),
    null,
  );
  expect(d, isA<Deny>());
});

test('effectivePermissionsFor returns empty EffectiveAuthorization', () async {
  const policy = FailSafeAuthorizationPolicy();
  final ea = await policy.effectivePermissionsFor(
    const UserPrincipal(userId: 'U', roles: {'r'}, activeRole: 'r'));
  expect(ea.rolePermissions, isEmpty);
  expect(ea.scopeAssignments, isEmpty);
});
```

- [ ] **Step 2: Run to verify failure**

```bash
cd event_sourcing && flutter test test/permissions/fail_safe_authorization_policy_test.dart
```

Expected: FAIL (the impl still has old signatures).

- [ ] **Step 3: Update `FailSafeAuthorizationPolicy`**

Replace `event_sourcing/lib/src/permissions/fail_safe_authorization_policy.dart`:

```dart
// Implements: EVS-PRD-action-dispatch/B (fail-safe policy denies everything
//   when bootstrap fails; preserves the closed-set of authorize outcomes)
// Implements: EVS-PRD-permissions-as-events/B (no decisions consult any
//   authority outside the log; the empty result is the only safe answer
//   when projections are unavailable)

import 'package:event_sourcing/event_sourcing.dart';

class FailSafeAuthorizationPolicy implements AuthorizationPolicy {
  const FailSafeAuthorizationPolicy();

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  ) async =>
      Deny(permission: permission, reason: DenyReason.notGranted);

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
      Principal principal) async =>
      EffectiveAuthorization.empty;
}
```

- [ ] **Step 4: Run tests**

```bash
cd event_sourcing && flutter test test/permissions/fail_safe_authorization_policy_test.dart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/permissions/fail_safe_authorization_policy.dart \
        event_sourcing/test/permissions/fail_safe_authorization_policy_test.dart
git commit -m "[CUR-1331] Update FailSafeAuthorizationPolicy for new interface

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 7 — Action interface and dispatcher

### Task 19: Add `Action.scopeFor`

**Files:**

- Modify: `event_sourcing/lib/src/actions/action.dart`
- Test: `event_sourcing/test/actions/action_default_scope_for_test.dart` (new)

- [ ] **Step 1: Write failing test**

Create `event_sourcing/test/actions/action_default_scope_for_test.dart`:

```dart
// Verifies: EVS-PRD-action-dispatch/B (Action.scopeFor default impl)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

class _MyInput { const _MyInput(this.foo); final String foo; }

class _UnscopedAction extends Action<_MyInput, void> {
  const _UnscopedAction();
  @override String get name => 'a.unscoped';
  @override String get description => '';
  @override Set<Permission> get permissions => const {Permission('foo')};
  @override Idempotency get idempotency => Idempotency.none;
  @override _MyInput parseInput(Map<String, Object?> raw) =>
      _MyInput(raw['foo']! as String);
  @override void validate(_MyInput input) {}
  @override Future<ExecutionResult<void>> execute(
          _MyInput input, ActionContext ctx) async =>
      ExecutionResult(result: null, events: const []);
  // scopeFor not overridden -> default returns null.
}

void main() {
  test('default scopeFor returns null', () {
    const a = _UnscopedAction();
    expect(a.scopeFor(const Permission('foo'), const _MyInput('x')), isNull);
  });
}
```

- [ ] **Step 2: Run to verify failure**

```bash
cd event_sourcing && flutter test test/actions/action_default_scope_for_test.dart
```

Expected: FAIL with undefined `scopeFor`.

- [ ] **Step 3: Add `scopeFor` to `Action`**

In `event_sourcing/lib/src/actions/action.dart`, add (after `execute`):

```dart
/// Per dispatch, supply the scope value for each scoped permission this
/// action requires. Pure: no I/O. Returns null for unscoped permissions
/// (default impl). For scoped permissions, the returned `ScopeValue`'s
/// `scopeClass` MUST equal the permission's declared `scopeClass`;
/// mismatch is `Deny(scopeUnresolvable)`.
///
/// `TotalWildcardScope` MUST NOT be returned (carries no class_ to
/// match against permission.scopeClass); dispatcher denies as
/// scopeUnresolvable if returned. `ValueWildcardScope` is technically
/// valid when an action genuinely operates on all values of a class.
ScopeValue? scopeFor(Permission perm, TInput input) => null;
```

Add the import:

```dart
import 'package:event_sourcing/src/actions/scope_value.dart';
```

- [ ] **Step 4: Run test**

```bash
cd event_sourcing && flutter test test/actions/action_default_scope_for_test.dart
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/actions/action.dart \
        event_sourcing/test/actions/action_default_scope_for_test.dart
git commit -m "[CUR-1331] Add Action.scopeFor with null default

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 20: Rewire `ActionDispatcher.authorize` stage

**Files:**

- Modify: `event_sourcing/lib/src/actions/action_dispatcher.dart`
- Modify: relevant tests under `event_sourcing/test/actions/` and `event_sourcing/test/permissions/`

- [ ] **Step 1: Find the current authorize-stage implementation**

```bash
grep -n "isPermitted\|authorize" event_sourcing/lib/src/actions/action_dispatcher.dart | head -20
```

- [ ] **Step 2: Write/update failing tests**

In the dispatcher's test file, add scenarios:

```dart
test('authorize: scoped perm + scopeFor returns matching BoundScope -> allow',
    () { /* ... */ });

test('authorize: scoped perm + scopeFor returns null -> deny scopeUnresolvable',
    () { /* ... */ });

test('authorize: scoped perm + scopeFor returns class-mismatched scope '
    '-> deny scopeUnresolvable', () { /* ... */ });

test('authorize: scoped perm + scopeFor returns TotalWildcardScope '
    '-> deny scopeUnresolvable', () { /* ... */ });

test('authorize: emits authorization_denied with scopeValue stamped',
    () { /* assert denial event payload includes scope */ });
```

- [ ] **Step 3: Run tests to confirm failure**

```bash
cd event_sourcing && flutter test test/actions/action_dispatcher_test.dart
```

(Path may differ; locate the dispatcher's test file.)

- [ ] **Step 4: Rewire the authorize stage**

In `action_dispatcher.dart`, replace the existing authorize loop:

```dart
// Inside ActionDispatcher.dispatch (after parse+validate):
for (final perm in action.permissions) {
  ScopeValue? scopeValue;
  if (perm.scopeClass != null) {
    scopeValue = action.scopeFor(perm, parsedInput);
    if (scopeValue == null) {
      return _denyAndEmit(
        principal: submission.principal,
        permission: perm,
        reason: DenyReason.scopeUnresolvable,
        detail: 'action did not supply scope value for scoped permission',
        scopeValue: null,
      );
    }
    if (scopeValue is TotalWildcardScope) {
      return _denyAndEmit(/* ... scopeUnresolvable, detail re Total ... */);
    }
    if (scopeValue is BoundScope && scopeValue.class_ != perm.scopeClass) {
      return _denyAndEmit(/* ... scopeUnresolvable, detail re class mismatch ... */);
    }
    if (scopeValue is ValueWildcardScope && scopeValue.class_ != perm.scopeClass) {
      return _denyAndEmit(/* ... */);
    }
  }
  final decision = await policy.isPermitted(
      submission.principal, perm, scopeValue);
  if (decision is Deny) {
    return _denyAndEmit(
      principal: submission.principal,
      permission: perm,
      reason: decision.reason,
      scopeValue: scopeValue,
    );
  }
}
```

`_denyAndEmit` is whatever helper the dispatcher uses today to emit `authorization_denied`; extend it to stamp `scope` into the denial payload when non-null.

- [ ] **Step 5: Run tests**

```bash
cd event_sourcing && flutter test test/actions/
```

Expected: all dispatcher tests pass.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/actions/action_dispatcher.dart \
        $(git diff --name-only event_sourcing/test/actions/)
git commit -m "[CUR-1331] Rewire ActionDispatcher authorize stage for scope-aware permissions

Calls Action.scopeFor for each scoped permission; denies with
scopeUnresolvable for null/TotalWildcard/class-mismatch returns.
Stamps the scope value onto authorization_denied events.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 21: Wrap authorize+execute in a single storage transaction

**Files:**

- Modify: `event_sourcing/lib/src/actions/action_dispatcher.dart`
- Modify: tests that verify dispatch-stage atomicity

- [ ] **Step 1: Locate the existing transaction boundary**

```bash
grep -n "runInTxn\|beginTxn\|executeInTxn" event_sourcing/lib/src/actions/action_dispatcher.dart
```

The current dispatcher likely wraps only the `execute` stage's event-append in a tx. We extend the tx to wrap authorize's policy reads too.

- [ ] **Step 2: Write a failing test for snapshot consistency**

Add to `action_dispatcher_test.dart` (the exact test name varies by harness; this is the shape):

```dart
test('authorize and execute see the same projection snapshot: a '
    'concurrent role_unassigned committed between authorize and append '
    'does not affect the in-flight dispatch', () async {
  // Setup: U1 holds (SC, site, A); registers an EditPatient action targeting
  //   P-42 at site A.
  // Start dispatch on a "slow" action whose execute() yields-and-waits on a
  //   completer.
  // While dispatch is mid-execute (still inside its tx), append a
  //   role_unassigned event for U1's assignment.
  // Resume the dispatch.
  // Expectation: dispatch SUCCEEDS (authorize evaluated against the
  //   pre-revoke snapshot inside its tx). Subsequent dispatches see the
  //   post-revoke state.
});
```

- [ ] **Step 3: Run to confirm it fails**

```bash
cd event_sourcing && flutter test test/actions/action_dispatcher_test.dart
```

Expected: depends on current behavior. If the dispatcher currently re-reads outside the tx, this test exposes the gap.

- [ ] **Step 4: Wrap the dispatch in a single tx**

Extend the dispatcher so the same transaction the event-append uses is also the one passed to `AuthorizationPolicy.isPermitted` (the policy was already wired via `txnProvider` in Task 17; the dispatcher passes its own active tx here).

```dart
// Sketch (adapt to existing dispatcher shape):
Future<DispatchResult<T>> dispatch<I, T>(ActionSubmission submission) async {
  return backend.runInTxn<DispatchResult<T>>((txn) async {
    // parse + validate (pure; no tx needed but cheap)
    final parsed = action.parseInput(submission.rawInput);
    action.validate(parsed);

    // authorize: pass txn so policy reads use this snapshot
    final deny = await _authorize(txn, parsed, submission.principal);
    if (deny != null) {
      await _appendDenialInTxn(txn, deny);
      return DispatchResult.denied(deny);
    }

    // execute: returns events to append
    final result = await action.execute(parsed, _ctxFor(txn, ...));

    // append in same tx
    await eventStore.appendInTxn(txn, result.events);
    return DispatchResult.allowed(result);
  });
}
```

The policy was constructed with `txnProvider` in Task 17; rewire the production wiring so the dispatcher's `txn` flows through. Easiest pattern: the policy's `txnProvider` is replaced at dispatch time with a "use this txn" closure.

- [ ] **Step 5: Run tests**

```bash
cd event_sourcing && flutter test test/actions/
```

Expected: all pass, including the snapshot-consistency test.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/actions/action_dispatcher.dart \
        $(git diff --name-only event_sourcing/test/actions/)
git commit -m "[CUR-1331] Wrap dispatch authorize+execute in a single storage transaction

Authorize stage's policy reads now share the same tx (and read snapshot)
as the execute stage's event append. A revocation committed between
authorize's read and execute's append takes effect on subsequent
dispatches, not the in-flight one.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 8 — Bootstrap, seed, and validator updates

### Task 22: Update YAML seed loader and `PermissionSeed`

**Files:**

- Modify: `event_sourcing/lib/src/permissions/yaml_seed_loader.dart`
- Modify: `event_sourcing/lib/src/permissions/permission_seed.dart`
- Modify: `event_sourcing/test/permissions/yaml_seed_loader_test.dart`

- [ ] **Step 1: Update tests to expect the simplified shape**

The YAML grammar is unchanged from today's form (role -> list of permission names). Tests should drop any assertion that the loader returns a `scope` field.

- [ ] **Step 2: Run tests to confirm failure (if any)**

```bash
cd event_sourcing && flutter test test/permissions/yaml_seed_loader_test.dart
```

- [ ] **Step 3: Strip scope handling from the loader and seed shape**

In `yaml_seed_loader.dart`, remove any code that reads a `scope:` field. The output is a `PermissionSeed` carrying just `(role, permissionName)` pairs.

In `permission_seed.dart`, drop the scope field from the `PermissionSeed` shape.

- [ ] **Step 4: Run tests**

```bash
cd event_sourcing && flutter test test/permissions/yaml_seed_loader_test.dart \
                                  test/permissions/permission_seed_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/permissions/yaml_seed_loader.dart \
        event_sourcing/lib/src/permissions/permission_seed.dart \
        event_sourcing/test/permissions/yaml_seed_loader_test.dart \
        event_sourcing/test/permissions/permission_seed_test.dart
git commit -m "[CUR-1331] Strip scope from YAML seed loader and PermissionSeed

YAML grammar is unchanged in shape (role -> [permission names]); the
loader no longer reads or emits a scope field because Permission's
scope class is code-registered.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 23: Update `SeedValidator` to validate scope-class registry references

**Files:**

- Modify: `event_sourcing/lib/src/permissions/seed_validator.dart`
- Modify: `event_sourcing/test/permissions/seed_validator_test.dart`

- [ ] **Step 1: Write failing test**

Add to `seed_validator_test.dart`:

```dart
test('rejects seed referencing a permission whose scopeClass is not '
    'registered in ScopeClassRegistry', () {
  final declared = <Permission>{
    const Permission('patient.edit', scopeClass: 'patient'),
  };
  final registry = ScopeClassRegistry(
    classes: const [ScopeClassSpec(name: 'site')],  // 'patient' absent
    projectionLookup: (_) => null,
  );
  expect(
    () => SeedValidator().validate(
      seed: PermissionSeed(grants: {'r': ['patient.edit']}),
      declaredPermissions: declared,
      scopeClassRegistry: registry,
    ),
    throwsA(isA<SeedValidationError>().having(
        (e) => e.toString(), 'message', contains('patient'))),
  );
});
```

- [ ] **Step 2: Extend `SeedValidator.validate`**

Accept a `ScopeClassRegistry` parameter and, for each permission in the seed, look up its `scopeClass`. If non-null, assert it's a registered class; if not, throw `SeedValidationError`.

- [ ] **Step 3: Update callers**

`bootstrapActionPermissions` accepts a `ScopeClassRegistry` parameter and passes it to the validator.

- [ ] **Step 4: Run tests**

```bash
cd event_sourcing && flutter test test/permissions/seed_validator_test.dart
```

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/permissions/seed_validator.dart \
        event_sourcing/lib/src/permissions/bootstrap_action_permissions.dart \
        event_sourcing/test/permissions/seed_validator_test.dart \
        event_sourcing/test/permissions/bootstrap_action_permissions_test.dart
git commit -m "[CUR-1331] SeedValidator: enforce that referenced scopeClasses are registered

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 24: `RoleAssignmentSeed` and `bootstrapRoleAssignments`

**Files:**

- Create: `event_sourcing/lib/src/permissions/role_assignment_seed.dart`
- Create: `event_sourcing/lib/src/permissions/bootstrap_role_assignments.dart`
- Test: `event_sourcing/test/permissions/bootstrap_role_assignments_test.dart`

- [ ] **Step 1: Write failing tests**

```dart
// Verifies: EVS-PRD-permissions-as-events (role-assignment bootstrap parallels permission bootstrap; idempotent over the log)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  test('idempotent: re-running emits no duplicate events for existing '
      'assignments', () async {
    // Setup an event store with one role_assigned already in the log.
    // Call bootstrapRoleAssignments with a seed containing the same +
    // a new assignment.
    // Expect: only one new event emitted.
  });

  test('emits role_assigned for each seed entry not yet in the log',
      () async {
    // Setup an empty event store.
    // Call bootstrapRoleAssignments with two seed entries.
    // Expect: two role_assigned events appended.
  });

  test('rejects seed entries with empty userId or role', () async {
    // ...
  });
}
```

- [ ] **Step 2: Run tests to fail**

```bash
cd event_sourcing && flutter test test/permissions/bootstrap_role_assignments_test.dart
```

- [ ] **Step 3: Create `role_assignment_seed.dart`**

```dart
// Implements: EVS-PRD-permissions-as-events (declarative seed shape for user-role-scope assignments)

import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:meta/meta.dart';

@immutable
class RoleAssignmentSeed {
  const RoleAssignmentSeed({required this.entries});
  final List<RoleAssignmentSeedEntry> entries;
}

@immutable
class RoleAssignmentSeedEntry {
  const RoleAssignmentSeedEntry({
    required this.userId,
    required this.role,
    required this.scope,
  })  : assert(userId != '', 'userId must not be empty'),
        assert(role != '', 'role must not be empty');

  final String userId;
  final String role;
  final ScopeValue scope;
}
```

- [ ] **Step 4: Create `bootstrap_role_assignments.dart`**

Mirror the existing `bootstrap_action_permissions.dart`. Sketch:

```dart
Future<void> bootstrapRoleAssignments({
  required EventStore eventStore,
  required RoleAssignmentSeed seed,
  Initiator seedInitiator = const AutomationInitiator(
    service: 'event_sourcing_role_assignments_seed',
  ),
}) async {
  // 1. For each entry, compute aggregate id.
  // 2. Query existing user_role_scopes for those aggregate ids
  //    (or read the log for role_assigned events keyed by them).
  // 3. Append role_assigned for each missing.
  // 4. Idempotent: re-runs that find all entries already present emit nothing.
}
```

(Pattern-match against `bootstrap_action_permissions.dart` for the precise idiom this repo uses for "find existing, emit missing.")

- [ ] **Step 5: Run tests**

```bash
cd event_sourcing && flutter test test/permissions/bootstrap_role_assignments_test.dart
```

- [ ] **Step 6: Export and commit**

```bash
# Add exports to event_sourcing/lib/event_sourcing.dart
git add event_sourcing/lib/src/permissions/role_assignment_seed.dart \
        event_sourcing/lib/src/permissions/bootstrap_role_assignments.dart \
        event_sourcing/lib/event_sourcing.dart \
        event_sourcing/test/permissions/bootstrap_role_assignments_test.dart
git commit -m "[CUR-1331] Add RoleAssignmentSeed and bootstrapRoleAssignments

Mirrors bootstrap_action_permissions: declarative seed list, idempotent
event emission so re-runs are no-ops when the log already contains
matching aggregates.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 9 — Example migration

### Task 25: Update `example_action_permissions/`

The example package currently uses `ScopeClass.site` / `ScopeClass.global` / `ScopeClass.self` on its sample Permission declarations. Migrate.

**Files:**

- Modify: all `event_sourcing/example_action_permissions/lib/server/actions/*.dart`
- Modify: relevant tests

- [ ] **Step 1: Sweep references**

```bash
grep -rn 'ScopeClass\|activeSite' event_sourcing/example_action_permissions/
```

- [ ] **Step 2: Migrate each file**

For each Permission declaration, apply the mapping from Task 5's Step 2. For `ScopeClass.site`-flavoured permissions that operate on a site target, also implement `scopeFor` returning a `BoundScope(class_: 'site', value: input.siteId)` (or whatever the action's input shape is).

- [ ] **Step 3: Add a scoped action sample**

Pick one action (e.g., `edit_blue_note_action.dart`) that is genuinely site-scoped. Implement `scopeFor` against its input. Add a test that exercises the scope binding end-to-end against a `_PolicyHarness`-like fixture in the example test tree.

- [ ] **Step 4: Run example tests**

```bash
cd event_sourcing/example_action_permissions && flutter test
```

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/example_action_permissions/
git commit -m "[CUR-1331] Migrate example_action_permissions to scope-aware Permission shape

Drops ScopeClass.* references; replaces with scopeClass identifier and
scopeFor implementations on actions that operate on scoped resources.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 26: Update `event_sourcing/example/`

Smaller example, same pattern. Verify it still builds and passes tests after the substrate changes.

**Files:**

- Modify: any file under `event_sourcing/example/lib/` that references `ScopeClass` or `activeSite`
- Modify: tests as needed

- [ ] **Step 1: Sweep references**

```bash
grep -rn 'ScopeClass\|activeSite' event_sourcing/example/
```

- [ ] **Step 2: Migrate**

Apply the same mapping. If `event_sourcing/example/` has no scoped permissions, this is a no-op modulo possibly removing the `Permission(..., scope: ScopeClass.global)` boilerplate.

- [ ] **Step 3: Run example tests**

```bash
cd event_sourcing/example && flutter test
```

- [ ] **Step 4: Commit**

```bash
git add event_sourcing/example/
git commit -m "[CUR-1331] Migrate event_sourcing/example to scope-aware Permission shape

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Phase 10 — Full-suite validation and traceability

### Task 27: Full test suite + analyze

- [ ] **Step 1: Run all test suites**

```bash
cd event_sourcing && flutter test 2>&1 | tail -10
cd event_sourcing/example && flutter test 2>&1 | tail -10
cd event_sourcing/example_action_permissions && flutter test 2>&1 | tail -10
cd canonical_json_jcs && flutter test 2>&1 | tail -10
cd provenance && flutter test 2>&1 | tail -10
cd reaction && flutter test 2>&1 | tail -10
```

Expected: all green.

- [ ] **Step 2: Run analyzer across all packages**

```bash
for d in event_sourcing event_sourcing/example event_sourcing/example_action_permissions \
         canonical_json_jcs provenance reaction; do
  echo "== $d =="; (cd "$d" && flutter analyze)
done
```

Expected: no errors.

- [ ] **Step 3: No commit (validation step)**

If anything fails, return to the relevant Task to fix.

---

### Task 28: Add DEV-level requirement annotations

The new code introduces obligations that should be captured as `EVS-DEV-*` requirements in `spec/scoped-permissions.md`. Author them in-place per the spec's lifecycle note.

**Files:**

- Modify: `spec/scoped-permissions.md` — append `## EVS-DEV-scope-class-registry-validation`, `## EVS-DEV-scoped-permissions-match-algorithm`, `## EVS-DEV-transactional-authorize-execute`, etc.
- Modify: source files — update `// Implements: ...` annotations to reference the new DEV ids

- [ ] **Step 1: Author DEV requirement blocks**

For each load-bearing implementation behavior, add a requirement block to `spec/scoped-permissions.md` under a `## EVS-DEV-<component>` heading. Follow the grammar in `spec/requirements-spec.md`.

Minimum set:

- `EVS-DEV-scope-class-registry-validation` — compose-time validation refuses duplicates, dangling refs, missing columns, cycles.
- `EVS-DEV-scope-value-json` — sealed-variant JSON contract.
- `EVS-DEV-containment-resolver` — walks ancestor chain via projections; fail-closed on miss.
- `EVS-DEV-scoped-permissions-match-algorithm` — equality + wildcards + containment.
- `EVS-DEV-effective-permissions-shape` — what effectivePermissionsFor returns.
- `EVS-DEV-transactional-authorize-execute` — dispatch tx encompasses both stages.
- `EVS-DEV-role-assignment-aggregate-id` — canonical-JSON encoding.
- `EVS-DEV-scope-unresolvable-denial` — when scopeFor returns null/Total/class-mismatch.

- [ ] **Step 2: Author one or more `EVS-PRD-scoped-permissions` requirement blocks**

Promote the design's key normative claims into PRD-level assertions, since the design has stabilized through impl. At minimum a single `## EVS-PRD-scoped-permissions` block with assertions covering: scope-class registration is composition-time; grants and assignments are events; matching is event-derived; transactional consistency.

- [ ] **Step 3: Update source-file `// Implements:` annotations**

For each new source file, replace any placeholder requirement IDs from earlier tasks with the just-authored DEV/PRD IDs. Run `mcp__elspais__refresh_graph` or `cd event_sourcing && elspais check` (whatever the repo's verifier is) to confirm traceability is clean.

- [ ] **Step 4: Run elspais checks**

```bash
# Per the pre-commit hook output during earlier commits, elspais runs as
# part of the pre-push hook. Trigger it explicitly:
.git/hooks/pre-push origin <branch> < /dev/null 2>&1 || true
```

Or:

```bash
# If the repo has a manual elspais command, run it.
which elspais && elspais check
```

Expected: no broken references; full coverage from new assertions to implementing code.

- [ ] **Step 5: Commit**

```bash
git add spec/scoped-permissions.md \
        $(git diff --name-only event_sourcing/)
git commit -m "[CUR-1331] Author EVS-PRD/DEV requirements for scope-aware permissions

In-place stabilization: design prose grows normative assertions
against the same file per the lifecycle note. Source files'
// Implements: annotations now reference the new requirement IDs.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

### Task 29: Push and open PR

- [ ] **Step 1: Push the branch**

```bash
git push
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --title "[CUR-1331] Scope-aware permissions" --body "$(cat <<'EOF'
## Summary

- Generalized scope-class registration mechanism (`ScopeClassSpec`, `ScopeClassRegistry`) — apps register their own scope classes; substrate is domain-neutral.
- Sealed `ScopeValue` type with three JSON-distinct variants (bound, value-wildcard, total-wildcard).
- New event types `role_assigned` / `role_unassigned` with canonical-JSON aggregate-id encoding.
- New `user_role_scopes` TableProjection feeding the authorize stage.
- `ContainmentResolver` walks app-supplied projection hierarchies to expand scope matches; fail-closed on missing rows.
- `TableBackedAuthorizationPolicy` rewritten around the match algorithm: equality + wildcard + containment, union within active role.
- `Action.scopeFor(perm, input)` per-dispatch scope binding; dispatcher stamps scope onto `authorization_denied` events.
- Dispatch authorize + execute now share a single storage transaction (read-consistent snapshot).
- Cleanup: `ScopeClass` enum, `Permission.scope`, `Principal.activeSite`, `DenyReason.sessionPreconditionMissing` all dropped (pre-ship, no shims).
- `EVS-PRD-scoped-permissions` and supporting `EVS-DEV-*` requirements authored in `spec/scoped-permissions.md`.

Spec: `spec/scoped-permissions.md`

## Test plan

- [ ] `flutter test` clean across event_sourcing, event_sourcing/example, event_sourcing/example_action_permissions
- [ ] `flutter analyze` clean across all packages
- [ ] elspais traceability clean (no broken references, full coverage of new assertions to implementing code)
- [ ] Manual scenario walk-through (per the spec's match-algorithm test list): equality match, equality miss, value-wildcard, total-wildcard, hierarchy via containment, fail-closed on containment miss, union within active role, active-role filter excludes other roles' grants, unscoped permission allow/deny

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Return the PR URL to the user**

---

## Plan-self-review notes

- **Spec coverage:** every section of `spec/scoped-permissions.md` maps to one or more tasks. §1 -> Tasks 3, 4, 14; §2 (Layer A) -> Tasks 9; §2 (Layer B) -> Tasks 10-13; §2 (Match algorithm) -> Task 17; §2 (YAML) -> Task 22; §2 (Bootstrap) -> Task 24; §3 (Permission) -> Task 5; §3 (Action) -> Task 19; §3 (AuthorizationPolicy) -> Tasks 16-18; §3 (Dispatcher) -> Tasks 20-21; §3 (Denial reasons) -> Task 6; §4 (Cleanup) -> Tasks 7, 8, 25, 26; §4 (StorageBackend pre-req) -> Task 1; §4 (FailSafe) -> Task 18; §4 (Spec/PRD work) -> Task 28; §5 (Open questions) -> validated through tests in Tasks 14, 17, 20.
- **Placeholders:** every step has the actual content needed. Two intentional defer-to-impl-time notes: the exact name of `findViewRowsInTxn` (Task 1) and the precise idiom for "find existing, emit missing" in `bootstrapRoleAssignments` (Task 24) — both point at concrete patterns in adjacent files for reference.
- **Type consistency:** `ScopeValue` variants (`BoundScope`, `ValueWildcardScope`, `TotalWildcardScope`) used identically across Tasks 2, 10-14, 17, 19, 20. `EffectiveAuthorization` shape declared in Task 15 and consumed in Tasks 16-18. `Permission(name, {scopeClass})` consistent across all sites.

---

## Execution Handoff

Two execution options:

1. **Subagent-driven** (recommended) — fresh subagent per task, review between tasks, fast iteration. Best for a 29-task refactor with cascading invariants.
2. **Inline execution** — execute tasks in this session using `executing-plans`, batched with checkpoints.

The plan is saved at `docs/superpowers/plans/2026-05-13-scoped-permissions-implementation.md`.
