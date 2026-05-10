# Projections and Subscribe Primitive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the existing untyped `Materializer.applyInTxn` machinery with declarative `ProjectionSpec` data interpreted by a substrate fold engine; ship a typed `subscribe<T>` reactive primitive; record library version in the event log; migrate the two existing materializers (DiaryEntries, RolePermissionGrants) to the new model.

**Architecture:** Closed-under-events projections — author code lives outside the substrate; the substrate interprets declarative ProjectionSpecs (Aggregate, Table) and PromoterSpecs against events. `subscribe<T>` is a pure live stream with snapshot-then-deltas for materialized modes; the consumer-supplied mapper turns the substrate's `Map<String, Object?>` row into typed T at read time. Library version is recorded in the log via `lib_version_*` events; downgrades are refused.

**Tech Stack:** Dart 3.10.7+ (sealed classes, pattern matching), Flutter test framework, sembast for the reference storage backend, `canonical_json_jcs` for canonical serialization, existing `provenance` lib for chain hops.

**Spec:** `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md`

**Out of scope (deferred to follow-up plan):** Migration of `event_sourcing/example/` and `event_sourcing/example_action_permissions/` from legacy `watchEvents`/`watchView`/`watchFifo` to `subscribe<T>` (Track 8 of the spec's implementation order).

**Greenfield discipline (per CUR-1317 commitment):** Old materializer machinery is removed in this plan, not preserved alongside the new code. No backwards-compat shims. Tests tied to the old shape are rewritten against the new shape, not duplicated.

---

## Phase 0 — StorageBackend genericization (prerequisite)

### Task 0: Rename diary-flavoured `StorageBackend` view methods to generic `view`-prefixed equivalents

**Why this comes first:** the 2026-05-10 audit identified that the existing `StorageBackend` exposes diary-named view-row methods (e.g., `readEntryInTxn`) that the new substrate fold interpreter (Phase D, Task 13) needs to call generically. Without this rename, Task 13's aggregate-fold interpreter cannot compile against the backend interface. The other view-row methods (`upsertViewRowInTxn`, `deleteViewRowInTxn`, `findViewRowsInTxn`, `clearViewInTxn`) are already generic and unchanged.

**Files:**

- Modify: `event_sourcing/lib/src/storage/storage_backend.dart` — rename diary-flavoured method(s) to generic `readViewRowInTxn(viewName, rowId)` form
- Modify: `event_sourcing/lib/src/storage/sembast_backend.dart` — rename the implementation
- Modify: every call site of the renamed method(s) — sweep with grep
- Modify: any test that references the old method name(s)

- [ ] **Step 1: Identify the methods to rename**

Run:

```bash
grep -nE 'readEntryInTxn|clearEntries\b' event_sourcing/lib/src/storage/storage_backend.dart event_sourcing/lib/src/storage/sembast_backend.dart
```

Capture every diary-named view-row method on the abstract `StorageBackend` and its sembast implementation. Per the audit, at minimum `readEntryInTxn` is one such; surface any others.

- [ ] **Step 2: Sweep all call sites of those methods**

Run:

```bash
grep -rn 'readEntryInTxn\|clearEntries\b' event_sourcing/lib event_sourcing/test
```

Capture the full call-site list. Most will be in materializer / rebuild / entry-service code — some of which Task 21 deletes outright. Note which call sites survive Task 21 vs which get deleted.

- [ ] **Step 3: Rename in the abstract `StorageBackend`**

In `event_sourcing/lib/src/storage/storage_backend.dart`, rename `readEntryInTxn(txn, aggregateId)` to `readViewRowInTxn(txn, viewName, rowId)`. The new signature takes the view name as a parameter so it works for any view, not just `diary_entries`.

```dart
/// Reads a single row from a materialized view by row key. Returns
/// null when the row is absent. Used by the substrate's projection
/// interpreter (and by direct ad-hoc queries from authorization
/// policies that bypass subscribe<T>).
Future<Map<String, Object?>?> readViewRowInTxn(
  Txn txn,
  String viewName,
  String rowId,
);
```

If `clearEntries` (or other diary-named methods) exist, rename to `clearViewInTxn(viewName)` etc. Match the same generic shape.

- [ ] **Step 4: Update `SembastBackend` implementation**

In `event_sourcing/lib/src/storage/sembast_backend.dart`, rename the implementation to match the new signature. The body changes from a hardcoded `'diary_entries'` store reference to using the supplied `viewName` parameter — same lookup logic, parameterised store name.

- [ ] **Step 5: Update every surviving call site**

For each call site identified in Step 2 that survives Task 21, replace `backend.readEntryInTxn(txn, aggregateId)` with `backend.readViewRowInTxn(txn, viewName, aggregateId)`. The view name is whatever view the caller is reading from (e.g., `'diary_entries'` for the legacy diary path that's about to be deleted; `spec.viewName` for the new ProjectionInterpreter).

- [ ] **Step 6: Update tests that reference the old names**

Same sweep on `event_sourcing/test/`. Tests that exercise the storage backend directly need the new method name; tests that go through higher-level APIs (e.g., the diary materializer test) may not need changes if they don't touch the backend method directly.

- [ ] **Step 7: Run full suite to verify the rename is complete**

Run: `cd event_sourcing && flutter test`
Expected: PASS — every test that previously called the diary-named method now calls the generic equivalent.

- [ ] **Step 8: Commit**

```bash
git add event_sourcing/lib/src/storage/storage_backend.dart \
        event_sourcing/lib/src/storage/sembast_backend.dart \
        event_sourcing/lib/  event_sourcing/test/  # for swept call sites
git commit -m "[CUR-1317] Genericize StorageBackend view-row methods (readEntryInTxn → readViewRowInTxn)"
```

---

## Phase A — Library version foundation

### Task 1: Define library version constant + version event payload types

**Files:**

- Create: `event_sourcing/lib/src/lifecycle/lib_version.dart`
- Create: `event_sourcing/test/lifecycle/lib_version_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// event_sourcing/test/lifecycle/lib_version_test.dart
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('LibVersion', () {
    test('current version is a non-empty string', () {
      expect(LibVersion.current, isNotEmpty);
    });

    test('compare returns expected ordering', () {
      expect(LibVersion.compare('0.4.0', '0.4.1'), lessThan(0));
      expect(LibVersion.compare('0.4.1', '0.4.0'), greaterThan(0));
      expect(LibVersion.compare('0.4.0', '0.4.0'), 0);
      expect(LibVersion.compare('0.10.0', '0.9.0'), greaterThan(0));
    });

    test('event type ids are stable strings', () {
      expect(LibVersionEvents.initialized, 'lib_version_initialized');
      expect(LibVersionEvents.changed, 'lib_version_changed');
    });
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && flutter test test/lifecycle/lib_version_test.dart`
Expected: FAIL — `lib_version.dart` does not exist.

- [ ] **Step 3: Write minimal implementation**

```dart
// event_sourcing/lib/src/lifecycle/lib_version.dart

/// Substrate-level library version metadata. The version is recorded in
/// the event log via `lib_version_initialized` and `lib_version_changed`
/// events; the boot flow refuses to start when the log was last processed
/// by a newer version than this constant.
class LibVersion {
  /// The version of the event_sourcing library compiled into this build.
  /// Update in lockstep with `pubspec.yaml`'s `version` field.
  static const String current = '0.4.0';

  /// Returns negative if [a] < [b], positive if [a] > [b], 0 if equal.
  /// Compares dot-separated integer components left to right; trailing
  /// missing components count as zero.
  static int compare(String a, String b) {
    final aParts = a.split('.').map(int.parse).toList();
    final bParts = b.split('.').map(int.parse).toList();
    final maxLen = aParts.length > bParts.length ? aParts.length : bParts.length;
    for (var i = 0; i < maxLen; i++) {
      final av = i < aParts.length ? aParts[i] : 0;
      final bv = i < bParts.length ? bParts[i] : 0;
      if (av != bv) return av - bv;
    }
    return 0;
  }
}

class LibVersionEvents {
  static const String initialized = 'lib_version_initialized';
  static const String changed = 'lib_version_changed';
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd event_sourcing && flutter test test/lifecycle/lib_version_test.dart`
Expected: PASS — all three test cases.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/lifecycle/lib_version.dart \
        event_sourcing/test/lifecycle/lib_version_test.dart
git commit -m "[CUR-1317] Library version constant and event type ids"
```

---

### Task 2: Implement version-check helper that scans the log for the most recent version event

**Files:**

- Create: `event_sourcing/lib/src/lifecycle/version_check.dart`
- Create: `event_sourcing/test/lifecycle/version_check_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// event_sourcing/test/lifecycle/version_check_test.dart
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'vc-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

StoredEvent _versionEvent(String type, Map<String, Object?> data, {required int sequence}) =>
    StoredEvent.synthetic(
      eventId: 'lvi-$sequence',
      aggregateId: '_lib',
      aggregateType: '_lib',
      entryType: type,
      eventType: type,
      sequence: sequence,
      eventHash: 'h-$sequence',
      data: data,
    );

void main() {
  group('VersionCheck.findMostRecent', () {
    test('returns null when no version events exist', () async {
      final backend = await _openBackend();
      final result = await VersionCheck.findMostRecent(backend);
      expect(result, isNull);
    });

    test('returns the most recent lib_version_initialized event', () async {
      final backend = await _openBackend();
      await backend.appendEvent(_versionEvent(
        LibVersionEvents.initialized,
        {'version': '0.4.0', 'initializedAt': '2026-05-09T00:00:00Z'},
        sequence: 1,
      ));
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, '0.4.0');
    });

    test('returns the most recent lib_version_changed when newer than initialized', () async {
      final backend = await _openBackend();
      await backend.appendEvent(_versionEvent(
        LibVersionEvents.initialized,
        {'version': '0.4.0', 'initializedAt': '2026-05-09T00:00:00Z'},
        sequence: 1,
      ));
      await backend.appendEvent(_versionEvent(
        LibVersionEvents.changed,
        {'fromVersion': '0.4.0', 'toVersion': '0.4.1', 'changedAt': '2026-05-15T00:00:00Z'},
        sequence: 5,
      ));
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, '0.4.1');
    });
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && flutter test test/lifecycle/version_check_test.dart`
Expected: FAIL — `version_check.dart` does not exist.

- [ ] **Step 3: Write minimal implementation**

```dart
// event_sourcing/lib/src/lifecycle/version_check.dart
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';

class VersionCheckResult {
  final String recordedVersion;
  final int sequence;
  final String eventType;
  const VersionCheckResult({
    required this.recordedVersion,
    required this.sequence,
    required this.eventType,
  });
}

class VersionCheck {
  /// Reverse-scans [backend] for the most recent lib_version_initialized
  /// or lib_version_changed event. Returns null when the log contains
  /// no version event (first boot under this library).
  static Future<VersionCheckResult?> findMostRecent(StorageBackend backend) async {
    // Backend exposes a generic event scan; filter to the two version event types.
    // Reverse iteration to terminate on first match.
    final stream = backend.readEventsReverse(eventTypes: {
      LibVersionEvents.initialized,
      LibVersionEvents.changed,
    });
    await for (final event in stream) {
      final version = event.eventType == LibVersionEvents.initialized
          ? event.data['version'] as String
          : event.data['toVersion'] as String;
      return VersionCheckResult(
        recordedVersion: version,
        sequence: event.sequence,
        eventType: event.eventType,
      );
    }
    return null;
  }
}
```

- [ ] **Step 4: Add `readEventsReverse` to `StorageBackend` interface and implement in `SembastBackend`**

If `readEventsReverse` doesn't already exist, add to `event_sourcing/lib/src/storage/storage_backend.dart`:

```dart
/// Reverse stream of stored events, optionally filtered to a set of
/// event types. Used by lifecycle scans that need to terminate on the
/// first match without paging through the entire log.
Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes});
```

Implement in `event_sourcing/lib/src/storage/sembast_backend.dart` using the existing sembast index in reverse order, with a server-side filter on `event_type` when `eventTypes` is supplied.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd event_sourcing && flutter test test/lifecycle/version_check_test.dart`
Expected: PASS — all three test cases.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/lifecycle/version_check.dart \
        event_sourcing/lib/src/storage/storage_backend.dart \
        event_sourcing/lib/src/storage/sembast_backend.dart \
        event_sourcing/test/lifecycle/version_check_test.dart
git commit -m "[CUR-1317] Reverse-scan helper for most recent lib_version event"
```

---

### Task 3: Wire version-check into `EventStore.open` boot flow

**Files:**

- Modify: `event_sourcing/lib/src/event_store.dart` — add `EventStore.open` factory + `BootResult` plumbing
- Create: `event_sourcing/test/event_store/boot_version_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/event_store/boot_version_test.dart
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'bv-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

void main() {
  group('EventStore.open boot version flow', () {
    test('emits lib_version_initialized on first boot', () async {
      final backend = await _openBackend();
      final store = await EventStore.open(storage: backend);
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, LibVersion.current);
      expect(result?.eventType, LibVersionEvents.initialized);
      await store.close();
    });

    test('no-op when recorded version equals current', () async {
      final backend = await _openBackend();
      await EventStore.open(storage: backend); // first boot writes init
      final beforeSecond = await VersionCheck.findMostRecent(backend);
      await EventStore.open(storage: backend); // second boot at same version
      final afterSecond = await VersionCheck.findMostRecent(backend);
      expect(afterSecond?.sequence, beforeSecond?.sequence);
    });

    test('emits lib_version_changed on upgrade', () async {
      final backend = await _openBackend();
      // Simulate an older recorded version by appending a synthetic init event.
      await backend.appendEvent(StoredEvent.synthetic(
        eventId: 'older-init',
        aggregateId: '_lib',
        aggregateType: '_lib',
        entryType: LibVersionEvents.initialized,
        eventType: LibVersionEvents.initialized,
        sequence: 1,
        eventHash: 'h1',
        data: {'version': '0.3.0', 'initializedAt': '2026-04-01T00:00:00Z'},
      ));
      final store = await EventStore.open(storage: backend);
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.eventType, LibVersionEvents.changed);
      expect(result?.recordedVersion, LibVersion.current);
      await store.close();
    });

    test('refuses to boot on downgrade by default', () async {
      final backend = await _openBackend();
      // Simulate a future version recorded.
      await backend.appendEvent(StoredEvent.synthetic(
        eventId: 'newer-init',
        aggregateId: '_lib',
        aggregateType: '_lib',
        entryType: LibVersionEvents.initialized,
        eventType: LibVersionEvents.initialized,
        sequence: 1,
        eventHash: 'h1',
        data: {'version': '99.0.0', 'initializedAt': '2099-01-01T00:00:00Z'},
      ));
      expect(
        () => EventStore.open(storage: backend),
        throwsA(isA<DowngradeRefusedError>()),
      );
    });

    test('allowDowngrade: true bypasses downgrade refusal', () async {
      final backend = await _openBackend();
      await backend.appendEvent(StoredEvent.synthetic(
        eventId: 'newer-init',
        aggregateId: '_lib',
        aggregateType: '_lib',
        entryType: LibVersionEvents.initialized,
        eventType: LibVersionEvents.initialized,
        sequence: 1,
        eventHash: 'h1',
        data: {'version': '99.0.0', 'initializedAt': '2099-01-01T00:00:00Z'},
      ));
      final store = await EventStore.open(storage: backend, allowDowngrade: true);
      // No new lib_version event should be emitted on downgrade — the existing
      // record stays authoritative until the next upgrade re-passes through.
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, '99.0.0');
      await store.close();
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/event_store/boot_version_test.dart`
Expected: FAIL — `EventStore.open` does not exist; `DowngradeRefusedError` does not exist.

- [ ] **Step 3: Implement `EventStore.open` factory + `DowngradeRefusedError`**

In `event_sourcing/lib/src/event_store.dart`, make the existing public constructor private (`EventStore._`) and add:

```dart
class DowngradeRefusedError extends Error {
  final String recordedVersion;
  final String currentVersion;
  DowngradeRefusedError(this.recordedVersion, this.currentVersion);

  @override
  String toString() =>
      'DowngradeRefusedError: log was processed by lib version '
      '$recordedVersion which is newer than this build ($currentVersion). '
      'Pass EventStore.open(allowDowngrade: true) to override (development use only).';
}

class EventStore {
  // ... existing fields and private constructor ...

  /// Opens an EventStore against [storage]. Performs the lib-version
  /// boot check: emits lib_version_initialized on first boot, refuses
  /// to start on downgrade unless [allowDowngrade] is true, otherwise
  /// emits lib_version_changed when the recorded version differs.
  static Future<EventStore> open({
    required StorageBackend storage,
    bool allowDowngrade = false,
  }) async {
    final store = EventStore._(storage: storage);
    final recorded = await VersionCheck.findMostRecent(storage);
    if (recorded == null) {
      // First boot — emit lib_version_initialized.
      await store._appendLibVersionEvent(
        LibVersionEvents.initialized,
        {
          'version': LibVersion.current,
          'initializedAt': DateTime.now().toUtc().toIso8601String(),
        },
      );
    } else {
      final cmp = LibVersion.compare(recorded.recordedVersion, LibVersion.current);
      if (cmp > 0 && !allowDowngrade) {
        throw DowngradeRefusedError(recorded.recordedVersion, LibVersion.current);
      } else if (cmp < 0) {
        await store._appendLibVersionEvent(
          LibVersionEvents.changed,
          {
            'fromVersion': recorded.recordedVersion,
            'toVersion': LibVersion.current,
            'changedAt': DateTime.now().toUtc().toIso8601String(),
          },
        );
      }
      // cmp == 0 → no-op
      // cmp > 0 with allowDowngrade → no event emitted; recorded version stays authoritative
    }
    return store;
  }

  Future<void> _appendLibVersionEvent(String eventType, Map<String, Object?> data) async {
    // Append directly via the storage backend, bypassing the public append
    // path because lib_version_* events are substrate-emitted, not domain.
    final draft = EventDraft.systemSubstrate(
      aggregateId: '_lib',
      aggregateType: '_lib',
      entryType: eventType,
      eventType: eventType,
      data: data,
    );
    await _appendInternal(draft);
  }
}
```

If `EventDraft.systemSubstrate` does not exist, add it to `event_sourcing/lib/src/event_draft.dart` as a named constructor mirroring the existing factory but with a `SystemInitiator()` (substrate-emitted, not user-emitted).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/event_store/boot_version_test.dart`
Expected: PASS — all five test cases.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/event_store.dart \
        event_sourcing/lib/src/event_draft.dart \
        event_sourcing/test/event_store/boot_version_test.dart
git commit -m "[CUR-1317] EventStore.open boot flow with lib version check + downgrade refusal"
```

---

## Phase B — Library primitives

### Task 4: `Merge.applyDelta` utility (key-wise merge with null-as-clear)

**Files:**

- Create: `event_sourcing/lib/src/projections/primitives/merge.dart`
- Create: `event_sourcing/test/projections/primitives/merge_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/primitives/merge_test.dart
import 'package:event_sourcing/src/projections/primitives/merge.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Merge.applyDelta', () {
    test('present-non-null overwrites prior', () {
      final result = Merge.applyDelta(
        const {'a': 1, 'b': 2},
        const {'a': 10},
      );
      expect(result, {'a': 10, 'b': 2});
    });

    test('absent key preserves prior', () {
      final result = Merge.applyDelta(
        const {'a': 1, 'b': 2},
        const {'a': 10},
      );
      expect(result['b'], 2);
    });

    test('present-null clears prior', () {
      final result = Merge.applyDelta(
        const {'a': 1, 'b': 2},
        const {'a': null},
      );
      expect(result.containsKey('a'), isTrue);
      expect(result['a'], isNull);
    });

    test('returns unmodifiable map', () {
      final result = Merge.applyDelta(const {'a': 1}, const {'b': 2});
      expect(() => result['c'] = 3, throwsUnsupportedError);
    });

    test('empty delta returns equivalent of prior', () {
      final result = Merge.applyDelta(const {'a': 1}, const {});
      expect(result, {'a': 1});
    });

    test('empty prior returns equivalent of delta', () {
      final result = Merge.applyDelta(const {}, const {'a': 1});
      expect(result, {'a': 1});
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/primitives/merge_test.dart`
Expected: FAIL — `merge.dart` does not exist.

- [ ] **Step 3: Implement `Merge.applyDelta`**

```dart
// event_sourcing/lib/src/projections/primitives/merge.dart

/// Library-supplied projection primitive: key-wise merge with
/// null-as-clear semantics.
///
/// Each key present in [delta] overwrites the corresponding key in
/// [prior], including when the delta's value is `null` (explicit clear).
/// Each key absent from [delta] preserves the prior value.
///
/// The iteration uses [delta.keys] rather than indexing into delta, so
/// "key absent" and "key present with null value" are distinguished.
///
/// Returns an unmodifiable map.
class Merge {
  static Map<String, Object?> applyDelta(
    Map<String, Object?> prior,
    Map<String, Object?> delta,
  ) {
    final merged = Map<String, Object?>.from(prior);
    for (final key in delta.keys) {
      merged[key] = delta[key];
    }
    return Map<String, Object?>.unmodifiable(merged);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/primitives/merge_test.dart`
Expected: PASS — all six test cases.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/primitives/merge.dart \
        event_sourcing/test/projections/primitives/merge_test.dart
git commit -m "[CUR-1317] Merge.applyDelta projection primitive (null-as-clear)"
```

---

### Task 5: `DerivedFieldComputation` primitives

**Files:**

- Create: `event_sourcing/lib/src/projections/primitives/derived_field.dart`
- Create: `event_sourcing/test/projections/primitives/derived_field_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/primitives/derived_field_test.dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('DottedPathLookup', () {
    test('resolves a single segment', () {
      const lookup = DottedPathLookup('answers', fallback: ConstantValue(null));
      final value = lookup.resolve(
        rowState: const {'answers': {'date': '2026-05-09'}},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, {'date': '2026-05-09'});
    });

    test('resolves a dotted path', () {
      const lookup = DottedPathLookup('answers.date', fallback: ConstantValue(null));
      final value = lookup.resolve(
        rowState: const {'answers': {'date': '2026-05-09'}},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, '2026-05-09');
    });

    test('falls back when path resolves to non-Map mid-traversal', () {
      const lookup = DottedPathLookup('answers.date.year', fallback: ConstantValue('NA'));
      final value = lookup.resolve(
        rowState: const {'answers': {'date': '2026-05-09'}},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, 'NA');
    });

    test('falls back when key absent', () {
      const lookup = DottedPathLookup('missing.key', fallback: ConstantValue('NA'));
      final value = lookup.resolve(
        rowState: const {'answers': {'date': 'x'}},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, 'NA');
    });

    test('FirstEventTimestamp fallback returns ISO8601 string', () {
      const lookup = DottedPathLookup('missing', fallback: FirstEventTimestamp());
      final value = lookup.resolve(
        rowState: const {},
        firstEventTimestamp: DateTime.utc(2026, 5, 9, 12, 0, 0),
      );
      expect(value, '2026-05-09T12:00:00.000Z');
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/primitives/derived_field_test.dart`
Expected: FAIL — `derived_field.dart` does not exist.

- [ ] **Step 3: Implement primitives**

```dart
// event_sourcing/lib/src/projections/primitives/derived_field.dart

sealed class FallbackValue {
  const FallbackValue();
  Object? resolve({required DateTime firstEventTimestamp});
}

class ConstantValue extends FallbackValue {
  final Object? value;
  const ConstantValue(this.value);
  @override
  Object? resolve({required DateTime firstEventTimestamp}) => value;
}

class FirstEventTimestamp extends FallbackValue {
  const FirstEventTimestamp();
  @override
  Object? resolve({required DateTime firstEventTimestamp}) =>
      firstEventTimestamp.toUtc().toIso8601String();
}

sealed class DerivedFieldComputation {
  const DerivedFieldComputation();
  Object? resolve({
    required Map<String, Object?> rowState,
    required DateTime firstEventTimestamp,
  });
}

class DottedPathLookup extends DerivedFieldComputation {
  final String path;
  final FallbackValue fallback;
  const DottedPathLookup(this.path, {required this.fallback});

  @override
  Object? resolve({
    required Map<String, Object?> rowState,
    required DateTime firstEventTimestamp,
  }) {
    final segments = path.split('.');
    Object? current = rowState;
    for (final seg in segments) {
      if (current is! Map) {
        return fallback.resolve(firstEventTimestamp: firstEventTimestamp);
      }
      if (!current.containsKey(seg)) {
        return fallback.resolve(firstEventTimestamp: firstEventTimestamp);
      }
      current = current[seg];
    }
    return current ?? fallback.resolve(firstEventTimestamp: firstEventTimestamp);
  }
}

class DerivedField {
  final String fieldName;
  final DerivedFieldComputation computation;
  const DerivedField(this.fieldName, this.computation);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/primitives/derived_field_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/primitives/derived_field.dart \
        event_sourcing/test/projections/primitives/derived_field_test.dart
git commit -m "[CUR-1317] DerivedFieldComputation primitives (DottedPathLookup, fallbacks)"
```

---

### Task 6: `RowKeyExtractor` primitives

**Files:**

- Create: `event_sourcing/lib/src/projections/primitives/row_key.dart`
- Create: `event_sourcing/test/projections/primitives/row_key_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/primitives/row_key_test.dart
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _event({
  String aggregateId = 'agg-1',
  Map<String, Object?>? data,
}) => StoredEvent.synthetic(
      eventId: 'e-1',
      aggregateId: aggregateId,
      aggregateType: 'X',
      entryType: 'x',
      eventType: 'permission_granted',
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 5, 9),
      eventHash: 'h',
      data: data ?? {},
    );

void main() {
  group('AggregateIdKey', () {
    test('returns event.aggregateId', () {
      const k = AggregateIdKey();
      expect(k.extract(_event(aggregateId: 'foo')), 'foo');
    });
  });

  group('CompositeKey', () {
    test('joins extracted path values with a separator', () {
      const k = CompositeKey(['data.role', 'data.permission', 'data.scope']);
      final key = k.extract(_event(data: {
        'role': 'admin',
        'permission': 'users.invite',
        'scope': 'site',
      }));
      expect(key, 'admin|users.invite|site');
    });

    test('throws when a required path is missing', () {
      const k = CompositeKey(['data.missing']);
      expect(() => k.extract(_event(data: {'role': 'admin'})), throwsStateError);
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/primitives/row_key_test.dart`
Expected: FAIL — `row_key.dart` does not exist.

- [ ] **Step 3: Implement primitives**

```dart
// event_sourcing/lib/src/projections/primitives/row_key.dart
import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class RowKeyExtractor {
  const RowKeyExtractor();
  Object extract(StoredEvent event);
}

class AggregateIdKey extends RowKeyExtractor {
  const AggregateIdKey();
  @override
  Object extract(StoredEvent event) => event.aggregateId;
}

class CompositeKey extends RowKeyExtractor {
  /// Each path is dotted; the first segment is one of `data`, `metadata`,
  /// or a top-level field on the StoredEvent. The remaining segments
  /// index into the value.
  final List<String> paths;
  const CompositeKey(this.paths);

  @override
  Object extract(StoredEvent event) {
    final parts = <String>[];
    for (final path in paths) {
      final v = _resolve(event, path);
      if (v == null) {
        throw StateError(
          'CompositeKey: required path "$path" did not resolve on event '
          '${event.eventId}',
        );
      }
      parts.add(v.toString());
    }
    return parts.join('|');
  }

  static Object? _resolve(StoredEvent event, String path) {
    final segs = path.split('.');
    if (segs.isEmpty) return null;
    Object? root;
    switch (segs.first) {
      case 'data':
        root = event.data;
      case 'aggregateId':
        return event.aggregateId;
      case 'eventType':
        return event.eventType;
      default:
        root = event.data;
        // Fall through and treat full path as data.X
    }
    final rest = segs.first == 'data' ? segs.skip(1) : segs;
    Object? cur = root;
    for (final seg in rest) {
      if (cur is! Map) return null;
      if (!cur.containsKey(seg)) return null;
      cur = cur[seg];
    }
    return cur;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/primitives/row_key_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/primitives/row_key.dart \
        event_sourcing/test/projections/primitives/row_key_test.dart
git commit -m "[CUR-1317] RowKeyExtractor primitives (AggregateIdKey, CompositeKey)"
```

---

### Task 7: `RowDataExtractor` primitives

**Files:**

- Create: `event_sourcing/lib/src/projections/primitives/row_data.dart`
- Create: `event_sourcing/test/projections/primitives/row_data_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/primitives/row_data_test.dart
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _event(Map<String, Object?> data) => StoredEvent.synthetic(
      eventId: 'e-1',
      aggregateId: 'a',
      aggregateType: 'X',
      entryType: 'x',
      eventType: 'evt',
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 5, 9),
      eventHash: 'h',
      data: data,
    );

void main() {
  group('WholePayload', () {
    test('returns the entire data map', () {
      const e = WholePayload();
      final result = e.extract(_event({'a': 1, 'b': 2}));
      expect(result, {'a': 1, 'b': 2});
    });
  });

  group('PayloadField', () {
    test('returns the named field as a Map', () {
      const e = PayloadField('answers');
      final result = e.extract(_event({'answers': {'q1': 'yes'}, 'meta': 'x'}));
      expect(result, {'q1': 'yes'});
    });

    test('returns empty map when field missing', () {
      const e = PayloadField('missing');
      expect(e.extract(_event({})), <String, Object?>{});
    });

    test('throws when field present but not a Map', () {
      const e = PayloadField('answers');
      expect(() => e.extract(_event({'answers': 'not-a-map'})), throwsStateError);
    });
  });

  group('SelectedFields', () {
    test('returns a Map of just the selected fields, missing fields omitted', () {
      const e = SelectedFields(['a', 'c']);
      final result = e.extract(_event({'a': 1, 'b': 2, 'c': 3}));
      expect(result, {'a': 1, 'c': 3});
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/primitives/row_data_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement primitives**

```dart
// event_sourcing/lib/src/projections/primitives/row_data.dart
import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class RowDataExtractor {
  const RowDataExtractor();
  Map<String, Object?> extract(StoredEvent event);
}

class WholePayload extends RowDataExtractor {
  const WholePayload();
  @override
  Map<String, Object?> extract(StoredEvent event) =>
      Map<String, Object?>.unmodifiable(event.data);
}

class PayloadField extends RowDataExtractor {
  final String fieldName;
  const PayloadField(this.fieldName);

  @override
  Map<String, Object?> extract(StoredEvent event) {
    if (!event.data.containsKey(fieldName)) {
      return const <String, Object?>{};
    }
    final value = event.data[fieldName];
    if (value is! Map) {
      throw StateError(
        'PayloadField("$fieldName"): field on event ${event.eventId} '
        'is not a Map (got ${value.runtimeType})',
      );
    }
    return Map<String, Object?>.unmodifiable(Map<String, Object?>.from(value));
  }
}

class SelectedFields extends RowDataExtractor {
  final List<String> fieldNames;
  const SelectedFields(this.fieldNames);

  @override
  Map<String, Object?> extract(StoredEvent event) {
    final result = <String, Object?>{};
    for (final name in fieldNames) {
      if (event.data.containsKey(name)) {
        result[name] = event.data[name];
      }
    }
    return Map<String, Object?>.unmodifiable(result);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/primitives/row_data_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/primitives/row_data.dart \
        event_sourcing/test/projections/primitives/row_data_test.dart
git commit -m "[CUR-1317] RowDataExtractor primitives (WholePayload, PayloadField, SelectedFields)"
```

---

### Task 8: `TransformPrimitive` primitives for promoters

**Files:**

- Create: `event_sourcing/lib/src/promoters/primitives/transform.dart`
- Create: `event_sourcing/test/promoters/primitives/transform_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/promoters/primitives/transform_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('RenameField', () {
    test('renames a present field', () {
      const t = RenameField(from: 'old', to: 'new');
      final result = t.apply(const {'old': 1, 'other': 2});
      expect(result, {'new': 1, 'other': 2});
    });

    test('no-op when source absent', () {
      const t = RenameField(from: 'missing', to: 'new');
      final result = t.apply(const {'other': 2});
      expect(result, {'other': 2});
    });

    test('throws when target already present', () {
      const t = RenameField(from: 'old', to: 'new');
      expect(() => t.apply(const {'old': 1, 'new': 2}), throwsStateError);
    });
  });

  group('DefaultField', () {
    test('adds field with default when absent', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {});
      expect(result, {'x': 'd'});
    });

    test('preserves existing value when present', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {'x': 'real'});
      expect(result, {'x': 'real'});
    });

    test('present-null value is preserved (not replaced with default)', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {'x': null});
      expect(result, {'x': null});
    });
  });

  group('DropField', () {
    test('removes the named field', () {
      const t = DropField(fieldName: 'gone');
      final result = t.apply(const {'gone': 1, 'kept': 2});
      expect(result, {'kept': 2});
    });

    test('no-op when field absent', () {
      const t = DropField(fieldName: 'missing');
      final result = t.apply(const {'kept': 2});
      expect(result, {'kept': 2});
    });
  });

  group('DeriveField', () {
    test('computes new field via derivation primitive', () {
      const t = DeriveField(
        fieldName: 'echo',
        from: DottedPathLookup('source', fallback: ConstantValue('NA')),
      );
      final result = t.apply(
        const {'source': 'hello'},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(result['echo'], 'hello');
    });
  });

  group('TransformChain.applyAll', () {
    test('applies transforms in order', () {
      const chain = [
        RenameField(from: 'a', to: 'b'),
        DefaultField(fieldName: 'c', defaultValue: 3),
      ];
      final result = TransformChain.applyAll(
        chain,
        const {'a': 1},
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(result, {'b': 1, 'c': 3});
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/promoters/primitives/transform_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement primitives**

```dart
// event_sourcing/lib/src/promoters/primitives/transform.dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';

sealed class TransformPrimitive {
  const TransformPrimitive();
  Map<String, Object?> apply(
    Map<String, Object?> input, {
    DateTime? firstEventTimestamp,
  });
}

class RenameField extends TransformPrimitive {
  final String from;
  final String to;
  const RenameField({required this.from, required this.to});

  @override
  Map<String, Object?> apply(
    Map<String, Object?> input, {
    DateTime? firstEventTimestamp,
  }) {
    if (!input.containsKey(from)) return Map.unmodifiable(input);
    if (input.containsKey(to)) {
      throw StateError(
        'RenameField($from -> $to): target field "$to" already present',
      );
    }
    final next = Map<String, Object?>.from(input);
    next[to] = next.remove(from);
    return Map.unmodifiable(next);
  }
}

class DefaultField extends TransformPrimitive {
  final String fieldName;
  final Object? defaultValue;
  const DefaultField({required this.fieldName, required this.defaultValue});

  @override
  Map<String, Object?> apply(
    Map<String, Object?> input, {
    DateTime? firstEventTimestamp,
  }) {
    if (input.containsKey(fieldName)) return Map.unmodifiable(input);
    final next = Map<String, Object?>.from(input);
    next[fieldName] = defaultValue;
    return Map.unmodifiable(next);
  }
}

class DropField extends TransformPrimitive {
  final String fieldName;
  const DropField({required this.fieldName});

  @override
  Map<String, Object?> apply(
    Map<String, Object?> input, {
    DateTime? firstEventTimestamp,
  }) {
    if (!input.containsKey(fieldName)) return Map.unmodifiable(input);
    final next = Map<String, Object?>.from(input)..remove(fieldName);
    return Map.unmodifiable(next);
  }
}

class DeriveField extends TransformPrimitive {
  final String fieldName;
  final DerivedFieldComputation from;
  const DeriveField({required this.fieldName, required this.from});

  @override
  Map<String, Object?> apply(
    Map<String, Object?> input, {
    DateTime? firstEventTimestamp,
  }) {
    final next = Map<String, Object?>.from(input);
    next[fieldName] = from.resolve(
      rowState: input,
      firstEventTimestamp: firstEventTimestamp ?? DateTime.fromMillisecondsSinceEpoch(0),
    );
    return Map.unmodifiable(next);
  }
}

class TransformChain {
  static Map<String, Object?> applyAll(
    List<TransformPrimitive> chain,
    Map<String, Object?> input, {
    required DateTime firstEventTimestamp,
  }) {
    var current = input;
    for (final t in chain) {
      current = t.apply(current, firstEventTimestamp: firstEventTimestamp);
    }
    return current;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/promoters/primitives/transform_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/promoters/primitives/transform.dart \
        event_sourcing/test/promoters/primitives/transform_test.dart
git commit -m "[CUR-1317] TransformPrimitive primitives (Rename, Default, Drop, Derive)"
```

---

## Phase C — Spec types and registries

### Task 9: `ProjectionSpec` sealed types (Aggregate, Table)

**Files:**

- Create: `event_sourcing/lib/src/projections/projection_spec.dart`
- Create: `event_sourcing/lib/src/projections/subscription_filter.dart` (or modify existing if it already lives elsewhere — verify with grep)
- Create: `event_sourcing/test/projections/projection_spec_test.dart`

- [ ] **Step 1: Verify location of existing `SubscriptionFilter`**

Run: `grep -rn 'class SubscriptionFilter' event_sourcing/lib/`
Expected: existing class at `event_sourcing/lib/src/destinations/subscription_filter.dart` per the explore agent's earlier survey. Move or re-export under `event_sourcing/lib/src/projections/subscription_filter.dart` so projection and subscribe code share the same type. Keep the destinations export for the existing `Destination` consumers.

- [ ] **Step 2: Write the failing tests**

```dart
// event_sourcing/test/projections/projection_spec_test.dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('AggregateProjectionSpec', () {
    test('exposes viewName, interest, aggregateType, tombstones, derivations', () {
      final spec = AggregateProjectionSpec(
        viewName: 'diary_entries',
        aggregateType: 'DiaryEntry',
        interest: SubscriptionFilter(aggregateTypes: const {'DiaryEntry'}),
        tombstoneEventTypes: const {'tombstone'},
        derivedFields: const [
          DerivedField(
            'effective_date',
            DottedPathLookup('answers.date_of_event', fallback: FirstEventTimestamp()),
          ),
        ],
      );
      expect(spec.viewName, 'diary_entries');
      expect(spec.aggregateType, 'DiaryEntry');
      expect(spec.tombstoneEventTypes, {'tombstone'});
      expect(spec.derivedFields, hasLength(1));
    });
  });

  group('TableProjectionSpec', () {
    test('exposes insert/remove event sets, key, data extractor', () {
      final spec = TableProjectionSpec(
        viewName: 'role_permission_grants',
        interest: SubscriptionFilter(eventTypes: const {
          'permission_granted',
          'permission_revoked',
        }),
        insertEventTypes: const {'permission_granted'},
        removeEventTypes: const {'permission_revoked'},
        rowKey: const CompositeKey(['data.role', 'data.permission', 'data.scope']),
        rowData: const PayloadField('data'),
      );
      expect(spec.viewName, 'role_permission_grants');
      expect(spec.insertEventTypes, {'permission_granted'});
    });
  });
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/projection_spec_test.dart`
Expected: FAIL — `projection_spec.dart` does not exist.

- [ ] **Step 4: Implement spec types**

```dart
// event_sourcing/lib/src/projections/projection_spec.dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';

sealed class ProjectionSpec {
  const ProjectionSpec();
  String get viewName;
  SubscriptionFilter get interest;
}

class AggregateProjectionSpec extends ProjectionSpec {
  @override final String viewName;
  @override final SubscriptionFilter interest;
  final String aggregateType;
  final Set<String> tombstoneEventTypes;
  final List<DerivedField> derivedFields;

  const AggregateProjectionSpec({
    required this.viewName,
    required this.aggregateType,
    required this.interest,
    required this.tombstoneEventTypes,
    this.derivedFields = const [],
  });
}

class TableProjectionSpec extends ProjectionSpec {
  @override final String viewName;
  @override final SubscriptionFilter interest;
  final Set<String> insertEventTypes;
  final Set<String> removeEventTypes;
  final RowKeyExtractor rowKey;
  final RowDataExtractor rowData;

  const TableProjectionSpec({
    required this.viewName,
    required this.interest,
    required this.insertEventTypes,
    required this.removeEventTypes,
    required this.rowKey,
    required this.rowData,
  });
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/projection_spec_test.dart`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/projections/projection_spec.dart \
        event_sourcing/lib/src/projections/subscription_filter.dart \
        event_sourcing/test/projections/projection_spec_test.dart
git commit -m "[CUR-1317] ProjectionSpec sealed types (Aggregate, Table)"
```

---

### Task 10: `PromoterSpec` data type

**Files:**

- Create: `event_sourcing/lib/src/promoters/promoter_spec.dart`
- Create: `event_sourcing/test/promoters/promoter_spec_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// event_sourcing/test/promoters/promoter_spec_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('PromoterSpec exposes view, entry, version range, transforms', () {
    const spec = PromoterSpec(
      viewName: 'diary_entries',
      entryType: 'epistaxis_event',
      fromVersion: 1,
      toVersion: 2,
      transforms: [
        RenameField(from: 'old_name', to: 'new_name'),
        DefaultField(fieldName: 'language', defaultValue: 'en'),
      ],
    );
    expect(spec.viewName, 'diary_entries');
    expect(spec.entryType, 'epistaxis_event');
    expect(spec.fromVersion, 1);
    expect(spec.toVersion, 2);
    expect(spec.transforms, hasLength(2));
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && flutter test test/promoters/promoter_spec_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement type**

```dart
// event_sourcing/lib/src/promoters/promoter_spec.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';

class PromoterSpec {
  final String viewName;
  final String entryType;
  final int fromVersion;
  final int toVersion;
  final List<TransformPrimitive> transforms;

  const PromoterSpec({
    required this.viewName,
    required this.entryType,
    required this.fromVersion,
    required this.toVersion,
    required this.transforms,
  });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd event_sourcing && flutter test test/promoters/promoter_spec_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/promoters/promoter_spec.dart \
        event_sourcing/test/promoters/promoter_spec_test.dart
git commit -m "[CUR-1317] PromoterSpec declarative type"
```

---

### Task 11: `ProjectionRegistry` (immutable-after-open)

**Files:**

- Create: `event_sourcing/lib/src/projections/projection_registry.dart`
- Create: `event_sourcing/test/projections/projection_registry_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/projection_registry_test.dart
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:flutter_test/flutter_test.dart';

ProjectionSpec _spec(String viewName) => TableProjectionSpec(
      viewName: viewName,
      interest: SubscriptionFilter(eventTypes: const {'evt'}),
      insertEventTypes: const {'evt'},
      removeEventTypes: const {},
      rowKey: const AggregateIdKey(),
      rowData: const WholePayload(),
    );

void main() {
  group('ProjectionRegistry', () {
    test('register + lookup by viewName', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.register(_spec('b'));
      expect(reg.lookup('a')?.viewName, 'a');
      expect(reg.lookup('b')?.viewName, 'b');
      expect(reg.lookup('missing'), isNull);
    });

    test('all() returns every registered spec', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.register(_spec('b'));
      expect(reg.all().map((s) => s.viewName).toSet(), {'a', 'b'});
    });

    test('register throws on duplicate viewName', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      expect(() => reg.register(_spec('a')), throwsStateError);
    });

    test('register after seal() throws', () {
      final reg = ProjectionRegistry();
      reg.register(_spec('a'));
      reg.seal();
      expect(() => reg.register(_spec('b')), throwsStateError);
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/projection_registry_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement registry**

```dart
// event_sourcing/lib/src/projections/projection_registry.dart
import 'package:event_sourcing/src/projections/projection_spec.dart';

class ProjectionRegistry {
  final Map<String, ProjectionSpec> _byView = {};
  bool _sealed = false;

  void register(ProjectionSpec spec) {
    if (_sealed) {
      throw StateError(
        'ProjectionRegistry: cannot register "${spec.viewName}" after seal()',
      );
    }
    if (_byView.containsKey(spec.viewName)) {
      throw StateError(
        'ProjectionRegistry: duplicate registration for viewName '
        '"${spec.viewName}"',
      );
    }
    _byView[spec.viewName] = spec;
  }

  ProjectionSpec? lookup(String viewName) => _byView[viewName];

  Iterable<ProjectionSpec> all() => _byView.values;

  /// Called by EventStore.open after composition; further register() calls
  /// throw. Phase II's settings-event-driven registration is gated behind
  /// a separate substrate-level event flow that bypasses this seal.
  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/projection_registry_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/projection_registry.dart \
        event_sourcing/test/projections/projection_registry_test.dart
git commit -m "[CUR-1317] ProjectionRegistry with immutable-after-open semantics"
```

---

### Task 12: `PromoterRegistry` with chain lookup

**Files:**

- Create: `event_sourcing/lib/src/promoters/promoter_registry.dart`
- Create: `event_sourcing/test/promoters/promoter_registry_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/promoters/promoter_registry_test.dart
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

PromoterSpec _spec(int from, int to) => PromoterSpec(
      viewName: 'v',
      entryType: 't',
      fromVersion: from,
      toVersion: to,
      transforms: const [],
    );

void main() {
  group('PromoterRegistry', () {
    test('chain returns specs in order from -> to', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      reg.register(_spec(2, 3));
      reg.register(_spec(3, 4));
      final chain = reg.chain(viewName: 'v', entryType: 't', fromVersion: 1, toVersion: 4);
      expect(chain.map((s) => '${s.fromVersion}->${s.toVersion}').toList(),
          ['1->2', '2->3', '3->4']);
    });

    test('chain returns empty list when from == to', () {
      final reg = PromoterRegistry();
      final chain = reg.chain(viewName: 'v', entryType: 't', fromVersion: 1, toVersion: 1);
      expect(chain, isEmpty);
    });

    test('chain throws when a step is missing', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      // missing 2 -> 3
      reg.register(_spec(3, 4));
      expect(
        () => reg.chain(viewName: 'v', entryType: 't', fromVersion: 1, toVersion: 4),
        throwsStateError,
      );
    });

    test('register throws on duplicate (view, entry, fromVersion)', () {
      final reg = PromoterRegistry();
      reg.register(_spec(1, 2));
      expect(() => reg.register(_spec(1, 2)), throwsStateError);
    });

    test('register after seal throws', () {
      final reg = PromoterRegistry();
      reg.seal();
      expect(() => reg.register(_spec(1, 2)), throwsStateError);
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/promoters/promoter_registry_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement registry**

```dart
// event_sourcing/lib/src/promoters/promoter_registry.dart
import 'package:event_sourcing/src/promoters/promoter_spec.dart';

class PromoterRegistry {
  // Key: "viewName|entryType|fromVersion"
  final Map<String, PromoterSpec> _byKey = {};
  bool _sealed = false;

  String _key(String view, String entry, int from) => '$view|$entry|$from';

  void register(PromoterSpec spec) {
    if (_sealed) {
      throw StateError(
        'PromoterRegistry: cannot register after seal()',
      );
    }
    final k = _key(spec.viewName, spec.entryType, spec.fromVersion);
    if (_byKey.containsKey(k)) {
      throw StateError(
        'PromoterRegistry: duplicate registration for '
        '(${spec.viewName}, ${spec.entryType}, v${spec.fromVersion})',
      );
    }
    _byKey[k] = spec;
  }

  /// Returns the chain of PromoterSpecs that promotes a payload from
  /// [fromVersion] to [toVersion] for ([viewName], [entryType]). Throws
  /// when any step in the chain is unregistered.
  List<PromoterSpec> chain({
    required String viewName,
    required String entryType,
    required int fromVersion,
    required int toVersion,
  }) {
    if (fromVersion == toVersion) return const [];
    if (fromVersion > toVersion) {
      throw StateError(
        'PromoterRegistry.chain: cannot promote backward '
        '(from=$fromVersion, to=$toVersion)',
      );
    }
    final out = <PromoterSpec>[];
    var v = fromVersion;
    while (v < toVersion) {
      final spec = _byKey[_key(viewName, entryType, v)];
      if (spec == null) {
        throw StateError(
          'PromoterRegistry.chain: no PromoterSpec registered for '
          '($viewName, $entryType, v$v -> v${v + 1}). '
          'Register a spec covering this transition.',
        );
      }
      out.add(spec);
      v = spec.toVersion;
    }
    return out;
  }

  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/promoters/promoter_registry_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/promoters/promoter_registry.dart \
        event_sourcing/test/promoters/promoter_registry_test.dart
git commit -m "[CUR-1317] PromoterRegistry with chain composition"
```

---

## Phase D — Substrate fold interpreter

### Task 13: Aggregate-shape fold mechanic

**Files:**

- Create: `event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart`
- Create: `event_sourcing/test/projections/interpreter/aggregate_fold_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/interpreter/aggregate_fold_test.dart
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase(
        'af-${DateTime.now().microsecondsSinceEpoch}.db',
      ),
    );

StoredEvent _ev(String aggId, String type, Map<String, Object?> data,
        {DateTime? ts, int? sequence}) =>
    StoredEvent.synthetic(
      eventId: 'e-$aggId-$type-${ts?.microsecondsSinceEpoch ?? 0}',
      aggregateId: aggId,
      aggregateType: 'DiaryEntry',
      entryType: 'epistaxis_event',
      eventType: type,
      initiator: const UserInitiator('u'),
      clientTimestamp: ts ?? DateTime.utc(2026, 5, 9),
      eventHash: 'h',
      sequence: sequence ?? 1,
      data: data,
    );

final _spec = AggregateProjectionSpec(
  viewName: 'diary_entries',
  aggregateType: 'DiaryEntry',
  interest: SubscriptionFilter(aggregateTypes: const {'DiaryEntry'}),
  tombstoneEventTypes: const {'tombstone'},
  derivedFields: const [
    DerivedField(
      'effective_date',
      DottedPathLookup('answers.date_of_event', fallback: FirstEventTimestamp()),
    ),
  ],
);

void main() {
  group('AggregateFold.applyEvent', () {
    test('first event creates row with merged data + metadata + derivation', () async {
      final backend = await _backend();
      await backend.runInTransaction((txn) async {
        await AggregateFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          event: _ev('e1', 'finalized',
              {'answers': {'q1': 'yes', 'date_of_event': '2026-04-01'}},
              ts: DateTime.utc(2026, 5, 9)),
        );
      });
      final row = await backend.readViewRow('diary_entries', 'e1');
      expect(row?['answers'], {'q1': 'yes', 'date_of_event': '2026-04-01'});
      expect(row?['effective_date'], '2026-04-01');
      expect(row?['updatedAt'], isNotNull);
      expect(row?['latestEventId'], isNotNull);
    });

    test('second event merges into existing row (null clears, absent preserves)', () async {
      final backend = await _backend();
      await backend.runInTransaction((txn) async {
        await AggregateFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('e1', 'checkpoint', {'answers': {'q1': 'yes', 'q2': 'no'}}),
        );
        await AggregateFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('e1', 'checkpoint', {'answers': {'q2': null}}),
        );
      });
      final row = await backend.readViewRow('diary_entries', 'e1');
      expect((row?['answers'] as Map)['q1'], 'yes');     // preserved
      expect((row?['answers'] as Map)['q2'], isNull);    // cleared
    });

    test('tombstone event deletes the row', () async {
      final backend = await _backend();
      await backend.runInTransaction((txn) async {
        await AggregateFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('e1', 'finalized', {'answers': {'q1': 'yes'}}),
        );
        await AggregateFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('e1', 'tombstone', {}),
        );
      });
      final row = await backend.readViewRow('diary_entries', 'e1');
      expect(row, isNull);
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/interpreter/aggregate_fold_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement aggregate fold**

```dart
// event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart
import 'package:event_sourcing/src/projections/primitives/merge.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class AggregateFold {
  /// Applies one [event] to its aggregate row under [spec], inside [txn].
  /// Implements the AggregateProjectionSpec fold mechanics:
  /// - read row by event.aggregateId (initialState if absent),
  /// - generic merge of event.data with null-as-clear,
  /// - apply derived fields,
  /// - stamp metadata,
  /// - delete row if event.eventType is in spec.tombstoneEventTypes.
  static Future<void> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required AggregateProjectionSpec spec,
    required StoredEvent event,
  }) async {
    if (spec.tombstoneEventTypes.contains(event.eventType)) {
      await backend.deleteViewRowInTxn(txn, spec.viewName, event.aggregateId);
      return;
    }
    final priorRaw = await backend.readViewRowInTxn(txn, spec.viewName, event.aggregateId);
    final prior = priorRaw ?? const <String, Object?>{};
    final firstEventTimestamp = (prior['firstEventTimestamp'] as String?) != null
        ? DateTime.parse(prior['firstEventTimestamp'] as String)
        : event.clientTimestamp;

    final merged = Merge.applyDelta(prior, event.data);

    final next = Map<String, Object?>.from(merged);
    next['latestEventId'] = event.eventId;
    next['updatedAt'] = event.clientTimestamp.toUtc().toIso8601String();
    next['firstEventTimestamp'] = firstEventTimestamp.toUtc().toIso8601String();

    for (final df in spec.derivedFields) {
      next[df.fieldName] = df.computation.resolve(
        rowState: next,
        firstEventTimestamp: firstEventTimestamp,
      );
    }

    await backend.upsertViewRowInTxn(
      txn,
      spec.viewName,
      event.aggregateId,
      Map<String, Object?>.unmodifiable(next),
    );
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/interpreter/aggregate_fold_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart \
        event_sourcing/test/projections/interpreter/aggregate_fold_test.dart
git commit -m "[CUR-1317] AggregateProjectionSpec fold interpreter"
```

---

### Task 14: Table-shape fold mechanic

**Files:**

- Create: `event_sourcing/lib/src/projections/interpreter/table_fold.dart`
- Create: `event_sourcing/test/projections/interpreter/table_fold_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/projections/interpreter/table_fold_test.dart
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase(
        'tf-${DateTime.now().microsecondsSinceEpoch}.db',
      ),
    );

StoredEvent _ev(String type, Map<String, Object?> data) => StoredEvent.synthetic(
      eventId: 'e-$type-${data.hashCode}',
      aggregateId: '_',
      aggregateType: '_',
      entryType: type,
      eventType: type,
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 5, 9),
      eventHash: 'h',
      sequence: 1,
      data: data,
    );

final _spec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(eventTypes: const {'permission_granted', 'permission_revoked'}),
  insertEventTypes: const {'permission_granted'},
  removeEventTypes: const {'permission_revoked'},
  rowKey: const CompositeKey(['data.role', 'data.permission', 'data.scope']),
  rowData: const WholePayload(),
);

// NOTE: In this codebase, StoredEvent.data IS the event payload. The
// CompositeKey path 'data.role' resolves to event.data['role'].
// WholePayload() returns event.data verbatim.

void main() {
  group('TableFold.applyEvent', () {
    test('insert event upserts a row keyed by composite key', () async {
      final backend = await _backend();
      await backend.runInTransaction((txn) async {
        await TableFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('permission_granted', {
            'role': 'admin', 'permission': 'users.invite', 'scope': 'site',
          }),
        );
      });
      final row = await backend.readViewRow('role_permission_grants', 'admin|users.invite|site');
      expect(row, isNotNull);
      expect(row!['role'], 'admin');
    });

    test('remove event deletes the matching row', () async {
      final backend = await _backend();
      await backend.runInTransaction((txn) async {
        await TableFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('permission_granted', {
            'role': 'admin', 'permission': 'users.invite', 'scope': 'site',
          }),
        );
        await TableFold.applyEvent(
          txn: txn, backend: backend, spec: _spec,
          event: _ev('permission_revoked', {
            'role': 'admin', 'permission': 'users.invite', 'scope': 'site',
          }),
        );
      });
      final row = await backend.readViewRow('role_permission_grants', 'admin|users.invite|site');
      expect(row, isNull);
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/projections/interpreter/table_fold_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement table fold**

```dart
// event_sourcing/lib/src/projections/interpreter/table_fold.dart
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class TableFold {
  static Future<void> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required TableProjectionSpec spec,
    required StoredEvent event,
  }) async {
    if (spec.insertEventTypes.contains(event.eventType)) {
      final key = spec.rowKey.extract(event);
      final data = spec.rowData.extract(event);
      await backend.upsertViewRowInTxn(txn, spec.viewName, key.toString(), data);
      return;
    }
    if (spec.removeEventTypes.contains(event.eventType)) {
      final key = spec.rowKey.extract(event);
      await backend.deleteViewRowInTxn(txn, spec.viewName, key.toString());
      return;
    }
    // Filter narrowing should prevent reaching here; safe no-op.
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/projections/interpreter/table_fold_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/projections/interpreter/table_fold.dart \
        event_sourcing/test/projections/interpreter/table_fold_test.dart
git commit -m "[CUR-1317] TableProjectionSpec fold interpreter"
```

---

### Task 15: Promoter chain executor

**Files:**

- Create: `event_sourcing/lib/src/promoters/promoter_executor.dart`
- Create: `event_sourcing/test/promoters/promoter_executor_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/promoters/promoter_executor_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('promotes payload through chain v1 -> v3', () {
    final reg = PromoterRegistry();
    reg.register(const PromoterSpec(
      viewName: 'v', entryType: 't',
      fromVersion: 1, toVersion: 2,
      transforms: [RenameField(from: 'old', to: 'mid')],
    ));
    reg.register(const PromoterSpec(
      viewName: 'v', entryType: 't',
      fromVersion: 2, toVersion: 3,
      transforms: [RenameField(from: 'mid', to: 'final')],
    ));
    final result = PromoterExecutor.promote(
      registry: reg,
      viewName: 'v',
      entryType: 't',
      fromVersion: 1,
      toVersion: 3,
      payload: const {'old': 'value'},
      firstEventTimestamp: DateTime.utc(2026, 1, 1),
    );
    expect(result, {'final': 'value'});
  });

  test('returns input unchanged when from == to', () {
    final reg = PromoterRegistry();
    final result = PromoterExecutor.promote(
      registry: reg, viewName: 'v', entryType: 't',
      fromVersion: 2, toVersion: 2,
      payload: const {'a': 1},
      firstEventTimestamp: DateTime.utc(2026, 1, 1),
    );
    expect(result, {'a': 1});
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/promoters/promoter_executor_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement executor**

```dart
// event_sourcing/lib/src/promoters/promoter_executor.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';

class PromoterExecutor {
  static Map<String, Object?> promote({
    required PromoterRegistry registry,
    required String viewName,
    required String entryType,
    required int fromVersion,
    required int toVersion,
    required Map<String, Object?> payload,
    required DateTime firstEventTimestamp,
  }) {
    final chain = registry.chain(
      viewName: viewName, entryType: entryType,
      fromVersion: fromVersion, toVersion: toVersion,
    );
    var current = Map<String, Object?>.unmodifiable(payload);
    for (final spec in chain) {
      current = TransformChain.applyAll(
        spec.transforms, current,
        firstEventTimestamp: firstEventTimestamp,
      );
    }
    return current;
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/promoters/promoter_executor_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/promoters/promoter_executor.dart \
        event_sourcing/test/promoters/promoter_executor_test.dart
git commit -m "[CUR-1317] PromoterExecutor — chain composition + application"
```

---

### Task 16: Wire interpreter into `EventStore.append`

**Files:**

- Modify: `event_sourcing/lib/src/event_store.dart` — append now invokes ProjectionInterpreter on matching specs
- Create: `event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart`
- Create: `event_sourcing/test/event_store/append_runs_projections_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// event_sourcing/test/event_store/append_runs_projections_test.dart
import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase(
        'rp-${DateTime.now().microsecondsSinceEpoch}.db',
      ),
    );

void main() {
  test('appended event produces projection row via interpreter', () async {
    final backend = await _backend();
    final projections = ProjectionRegistry()
      ..register(AggregateProjectionSpec(
        viewName: 'diary_entries',
        aggregateType: 'DiaryEntry',
        interest: SubscriptionFilter(aggregateTypes: const {'DiaryEntry'}),
        tombstoneEventTypes: const {'tombstone'},
      ));
    final store = await EventStore.open(
      storage: backend,
      projections: projections,
      promoters: PromoterRegistry(),
    );
    await store.append(EventDraft(
      aggregateId: 'e1',
      aggregateType: 'DiaryEntry',
      entryType: 'epistaxis_event',
      eventType: 'finalized',
      data: {'answers': {'q1': 'yes'}},
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 5, 9),
    ));
    final row = await backend.readViewRow('diary_entries', 'e1');
    expect(row, isNotNull);
    expect((row!['answers'] as Map)['q1'], 'yes');
    await store.close();
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && flutter test test/event_store/append_runs_projections_test.dart`
Expected: FAIL — `EventStore.open` does not yet accept registries; interpreter does not exist.

- [ ] **Step 3: Implement `ProjectionInterpreter`**

```dart
// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class ProjectionInterpreter {
  final ProjectionRegistry registry;
  ProjectionInterpreter(this.registry);

  /// For each ProjectionSpec whose interest filter matches [event], runs
  /// the appropriate per-shape fold inside [txn]. Caller guarantees [txn]
  /// is the same transaction as the event-append write.
  Future<void> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    for (final spec in registry.all()) {
      if (!spec.interest.matches(event)) continue;
      switch (spec) {
        case AggregateProjectionSpec():
          await AggregateFold.applyEvent(
            txn: txn, backend: backend, spec: spec, event: event,
          );
        case TableProjectionSpec():
          await TableFold.applyEvent(
            txn: txn, backend: backend, spec: spec, event: event,
          );
      }
    }
  }
}
```

If `SubscriptionFilter` does not yet have a `matches(StoredEvent)` method, add it implementing the AND-combined dimension match per the spec.

- [ ] **Step 4: Wire into `EventStore.open` and `EventStore.append`**

In `event_sourcing/lib/src/event_store.dart`:

```dart
class EventStore {
  final StorageBackend _storage;
  final ProjectionInterpreter _interpreter;
  // ... other fields ...

  EventStore._({
    required StorageBackend storage,
    required ProjectionRegistry projections,
    // ... other params ...
  })  : _storage = storage,
        _interpreter = ProjectionInterpreter(projections);

  static Future<EventStore> open({
    required StorageBackend storage,
    ProjectionRegistry? projections,
    PromoterRegistry? promoters,
    bool allowDowngrade = false,
  }) async {
    final p = projections ?? ProjectionRegistry();
    final pr = promoters ?? PromoterRegistry();
    p.seal();
    pr.seal();
    final store = EventStore._(storage: storage, projections: p, /* promoters: pr */);
    // ... existing version-check bootstrap ...
    return store;
  }

  Future<StoredEvent> append(EventDraft draft) async {
    return _storage.runInTransaction((txn) async {
      // ... existing event-append logic ...
      final stored = await _appendInTxn(txn, draft);
      await _interpreter.applyEvent(txn: txn, backend: _storage, event: stored);
      return stored;
    });
  }
}
```

(Adapt to the actual existing `append` implementation; the key change is that the projection interpreter runs in the same transaction, after the event is written.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd event_sourcing && flutter test test/event_store/append_runs_projections_test.dart`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/event_store.dart \
        event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart \
        event_sourcing/lib/src/projections/subscription_filter.dart \
        event_sourcing/test/event_store/append_runs_projections_test.dart
git commit -m "[CUR-1317] ProjectionInterpreter wired into EventStore.append"
```

---

## Phase E — subscribe<T> primitive

### Task 17: `Update<T>` sealed envelope and `SubscriptionMode<T>` sealed types

**Files:**

- Create: `event_sourcing/lib/src/subscriptions/update.dart`
- Create: `event_sourcing/lib/src/subscriptions/subscription_mode.dart`
- Create: `event_sourcing/test/subscriptions/update_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/subscriptions/update_test.dart
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Update<T>', () {
    test('Snapshot carries optional value and as-of sequence', () {
      const u = Snapshot<String>(value: 'hello', sequence: 42);
      expect(u.value, 'hello');
      expect(u.sequence, 42);
    });

    test('Snapshot allows null value (for absent aggregate)', () {
      const u = Snapshot<String>(value: null, sequence: 0);
      expect(u.value, isNull);
    });

    test('Delta carries value, sequence, cause', () {
      const u = Delta<int>(value: 7, sequence: 5, cause: 'updated');
      expect(u.value, 7);
      expect(u.cause, 'updated');
    });

    test('Tombstone carries aggregateId and sequence', () {
      const u = Tombstone<int>(aggregateId: 'a1', sequence: 10);
      expect(u.aggregateId, 'a1');
      expect(u.sequence, 10);
    });

    test('Pattern matching across variants', () {
      Update<int> any = const Snapshot<int>(value: 1, sequence: 0);
      final tag = switch (any) {
        Snapshot() => 'snap',
        Delta()    => 'delta',
        Tombstone() => 'tomb',
      };
      expect(tag, 'snap');
    });
  });

  group('SubscriptionMode<T>', () {
    test('Events is a const, parameterized over StoredEvent', () {
      const e = Events();
      expect(e, isA<SubscriptionMode<StoredEvent>>());
    });

    test('AggregateMode carries viewName and mapper', () {
      final a = AggregateMode<String>(
        viewName: 'diary_entries',
        mapper: (m) => m['title'] as String,
      );
      expect(a.viewName, 'diary_entries');
      expect(a.mapper({'title': 'hi'}), 'hi');
    });
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/subscriptions/update_test.dart`
Expected: FAIL.

- [ ] **Step 3: Implement Update + SubscriptionMode**

```dart
// event_sourcing/lib/src/subscriptions/update.dart

sealed class Update<T> {
  int get sequence;
  const Update();
}

class Snapshot<T> extends Update<T> {
  final T? value;
  @override final int sequence;
  const Snapshot({required this.value, required this.sequence});
}

class Delta<T> extends Update<T> {
  final T value;
  @override final int sequence;
  final String cause;
  const Delta({required this.value, required this.sequence, required this.cause});
}

class Tombstone<T> extends Update<T> {
  final String aggregateId;
  @override final int sequence;
  const Tombstone({required this.aggregateId, required this.sequence});
}
```

```dart
// event_sourcing/lib/src/subscriptions/subscription_mode.dart
import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class SubscriptionMode<T> {
  const SubscriptionMode();
}

class Events extends SubscriptionMode<StoredEvent> {
  const Events();
}

class AggregateMode<T> extends SubscriptionMode<T> {
  final String viewName;
  final T Function(Map<String, Object?>) mapper;
  const AggregateMode({required this.viewName, required this.mapper});
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/subscriptions/update_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/subscriptions/update.dart \
        event_sourcing/lib/src/subscriptions/subscription_mode.dart \
        event_sourcing/test/subscriptions/update_test.dart
git commit -m "[CUR-1317] Update<T> sealed envelope and SubscriptionMode<T> (Events, AggregateMode)"
```

---

### Task 18: Implement `subscribe<T>` for `Events` mode

**Files:**

- Create: `event_sourcing/lib/src/subscriptions/subscription_engine.dart`
- Modify: `event_sourcing/lib/src/event_store.dart` — add `subscribe<T>` method
- Create: `event_sourcing/test/subscriptions/events_mode_test.dart`

- [ ] **Step 1: Write the failing test**

```dart
// event_sourcing/test/subscriptions/events_mode_test.dart
import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  test('Events mode delivers subsequent appends, not pre-existing history', () async {
    final backend = SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase(
        'em-${DateTime.now().microsecondsSinceEpoch}.db',
      ),
    );
    final store = await EventStore.open(
      storage: backend,
      projections: ProjectionRegistry(),
      promoters: PromoterRegistry(),
    );

    EventDraft draft(String aggId, String type) => EventDraft(
          aggregateId: aggId, aggregateType: 'X', entryType: 'x',
          eventType: type, data: const {},
          initiator: const UserInitiator('u'),
          clientTimestamp: DateTime.utc(2026, 5, 9),
        );

    await store.append(draft('a', 'before')); // history; not delivered

    final received = <StoredEvent>[];
    final sub = store.subscribe<StoredEvent>(
      SubscriptionFilter(aggregateTypes: const {'X'}),
      const Events(),
    ).listen((Update<StoredEvent> u) {
      if (u is Delta<StoredEvent>) received.add(u.value);
    });

    await store.append(draft('a', 'after1'));
    await store.append(draft('b', 'after2'));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(received.map((e) => e.eventType).toList(), ['after1', 'after2']);
    await sub.cancel();
    await store.close();
  });
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd event_sourcing && flutter test test/subscriptions/events_mode_test.dart`
Expected: FAIL — `EventStore.subscribe` does not exist.

- [ ] **Step 3: Implement `SubscriptionEngine` + `EventStore.subscribe`**

```dart
// event_sourcing/lib/src/subscriptions/subscription_engine.dart
import 'dart:async';

import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';

/// Internal substrate component owning live broadcast streams of events
/// and projection-row changes. EventStore.append publishes into this
/// engine after the storage transaction commits successfully.
class SubscriptionEngine {
  final StreamController<StoredEvent> _eventBus =
      StreamController<StoredEvent>.broadcast();
  final StreamController<_RowChange> _rowBus =
      StreamController<_RowChange>.broadcast();

  void publishEvent(StoredEvent event) => _eventBus.add(event);

  void publishRowChange({
    required String viewName,
    required String aggregateId,
    required Map<String, Object?>? value, // null = removed/tombstoned
    required int sequence,
    required String cause,
    required bool isTombstone,
  }) {
    _rowBus.add(_RowChange(
      viewName: viewName,
      aggregateId: aggregateId,
      value: value,
      sequence: sequence,
      cause: cause,
      isTombstone: isTombstone,
    ));
  }

  Stream<Update<StoredEvent>> events(SubscriptionFilter filter) {
    return _eventBus.stream.where(filter.matches).map(
      (e) => Delta<StoredEvent>(value: e, sequence: e.sequence, cause: e.eventType),
    );
  }

  Stream<_RowChange> rowChanges(String viewName) =>
      _rowBus.stream.where((c) => c.viewName == viewName);

  Future<void> close() async {
    await _eventBus.close();
    await _rowBus.close();
  }
}

class _RowChange {
  final String viewName;
  final String aggregateId;
  final Map<String, Object?>? value;
  final int sequence;
  final String cause;
  final bool isTombstone;
  _RowChange({
    required this.viewName, required this.aggregateId,
    required this.value, required this.sequence,
    required this.cause, required this.isTombstone,
  });
}
```

In `event_sourcing/lib/src/event_store.dart`:

```dart
// inside class EventStore:
final SubscriptionEngine _subs = SubscriptionEngine();

Stream<Update<T>> subscribe<T>(
  SubscriptionFilter filter,
  SubscriptionMode<T> mode,
) {
  switch (mode) {
    case Events():
      return _subs.events(filter) as Stream<Update<T>>;
    case AggregateMode<T>():
      // implemented in Task 19
      throw UnimplementedError('AggregateMode subscribe lands in Task 19');
  }
}

Future<void> close() async {
  await _subs.close();
  await _storage.close();
}
```

In `EventStore.append`, after the transaction commits, publish:

```dart
_subs.publishEvent(stored);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd event_sourcing && flutter test test/subscriptions/events_mode_test.dart`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/subscriptions/subscription_engine.dart \
        event_sourcing/lib/src/event_store.dart \
        event_sourcing/test/subscriptions/events_mode_test.dart
git commit -m "[CUR-1317] subscribe<T> Events mode (live, from-now-forward)"
```

---

### Task 19: Implement `subscribe<T>` for `AggregateMode<T>` (snapshot + deltas + tombstone)

**Files:**

- Modify: `event_sourcing/lib/src/event_store.dart` — fill in AggregateMode subscribe + publish row changes from interpreter
- Modify: `event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart` — return change record so EventStore can publish
- Create: `event_sourcing/test/subscriptions/aggregate_mode_test.dart`

- [ ] **Step 1: Write the failing tests**

```dart
// event_sourcing/test/subscriptions/aggregate_mode_test.dart
import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/subscriptions/subscription_mode.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _DiaryEntry {
  final String entryId;
  final Map<String, Object?> answers;
  _DiaryEntry({required this.entryId, required this.answers});
  static _DiaryEntry fromMap(Map<String, Object?> m) => _DiaryEntry(
        entryId: m['latestEventId'] as String? ?? '_',
        answers: (m['answers'] as Map?)?.cast<String, Object?>() ?? {},
      );
}

void main() {
  Future<EventStore> _open() async {
    final backend = SembastBackend(
      database: await newDatabaseFactoryMemory().openDatabase(
        'am-${DateTime.now().microsecondsSinceEpoch}.db',
      ),
    );
    final projections = ProjectionRegistry()
      ..register(AggregateProjectionSpec(
        viewName: 'diary_entries',
        aggregateType: 'DiaryEntry',
        interest: SubscriptionFilter(aggregateTypes: const {'DiaryEntry'}),
        tombstoneEventTypes: const {'tombstone'},
      ));
    return EventStore.open(
      storage: backend,
      projections: projections,
      promoters: PromoterRegistry(),
    );
  }

  EventDraft _draft(String aggId, String type, [Map<String, Object?>? data]) =>
      EventDraft(
        aggregateId: aggId, aggregateType: 'DiaryEntry',
        entryType: 'epistaxis_event', eventType: type,
        data: data ?? const {},
        initiator: const UserInitiator('u'),
        clientTimestamp: DateTime.utc(2026, 5, 9),
      );

  test('snapshot for not-yet-existing aggregate emits null-value Snapshot', () async {
    final store = await _open();
    final updates = <Update<_DiaryEntry?>>[];
    final sub = store.subscribe(
      SubscriptionFilter(aggregates: const {'never-created'}),
      AggregateMode<_DiaryEntry?>(
        viewName: 'diary_entries',
        mapper: (m) => m.isEmpty ? null : _DiaryEntry.fromMap(m),
      ),
    ).listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(updates.length, 1);
    expect(updates.first, isA<Snapshot<_DiaryEntry?>>());
    expect((updates.first as Snapshot).value, isNull);
    await sub.cancel();
    await store.close();
  });

  test('snapshot for existing aggregate carries current state; subsequent appends emit Delta', () async {
    final store = await _open();
    await store.append(_draft('e1', 'finalized', {'answers': {'q1': 'yes'}}));

    final updates = <Update<_DiaryEntry?>>[];
    final sub = store.subscribe(
      SubscriptionFilter(aggregates: const {'e1'}),
      AggregateMode<_DiaryEntry?>(
        viewName: 'diary_entries',
        mapper: (m) => m.isEmpty ? null : _DiaryEntry.fromMap(m),
      ),
    ).listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    // Snapshot delivered
    expect(updates.first, isA<Snapshot<_DiaryEntry?>>());
    expect((updates.first as Snapshot).value, isNotNull);

    await store.append(_draft('e1', 'checkpoint', {'answers': {'q2': 'no'}}));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.any((u) => u is Delta<_DiaryEntry?>), isTrue);
    final delta = updates.whereType<Delta<_DiaryEntry?>>().last;
    expect(delta.value!.answers, {'q1': 'yes', 'q2': 'no'});
    await sub.cancel();
    await store.close();
  });

  test('tombstone produces Tombstone update for active subscribers', () async {
    final store = await _open();
    await store.append(_draft('e1', 'finalized', {'answers': {'q1': 'yes'}}));

    final updates = <Update<_DiaryEntry?>>[];
    final sub = store.subscribe(
      SubscriptionFilter(aggregates: const {'e1'}),
      AggregateMode<_DiaryEntry?>(
        viewName: 'diary_entries',
        mapper: (m) => m.isEmpty ? null : _DiaryEntry.fromMap(m),
      ),
    ).listen(updates.add);
    await Future<void>.delayed(const Duration(milliseconds: 50));

    await store.append(_draft('e1', 'tombstone'));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(updates.any((u) => u is Tombstone<_DiaryEntry?>), isTrue);
    await sub.cancel();
    await store.close();
  });
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd event_sourcing && flutter test test/subscriptions/aggregate_mode_test.dart`
Expected: FAIL — AggregateMode is unimplemented.

- [ ] **Step 3: Modify `AggregateFold.applyEvent` to return a change record**

```dart
class AggregateFoldChange {
  final String viewName;
  final String aggregateId;
  final Map<String, Object?>? newValue; // null = tombstoned
  final int sequence;
  final String cause;
  final bool isTombstone;
  AggregateFoldChange({
    required this.viewName, required this.aggregateId,
    required this.newValue, required this.sequence,
    required this.cause, required this.isTombstone,
  });
}

// applyEvent now returns AggregateFoldChange
```

Update `TableFold.applyEvent` similarly to return a change record. The `ProjectionInterpreter.applyEvent` collects them and returns the list to the caller.

- [ ] **Step 4: Implement `AggregateMode` subscribe in `EventStore.subscribe`**

```dart
case AggregateMode<T>():
  return _subscribeAggregate(filter, mode);
```

```dart
Stream<Update<T>> _subscribeAggregate<T>(
  SubscriptionFilter filter,
  AggregateMode<T> mode,
) async* {
  // Atomic snapshot-then-attach: open a buffered live stream FIRST,
  // then read snapshot rows; stitch by sequence.
  final liveBuffer = <_RowChange>[];
  final liveSub = _subs.rowChanges(mode.viewName).listen(liveBuffer.add);
  try {
    // Snapshot read
    final aggregateIds = filter.aggregates;
    if (aggregateIds == null) {
      // All matching aggregates of the projection's aggregateType
      final rows = await _storage.findViewRows(mode.viewName);
      for (final entry in rows.entries) {
        yield Snapshot<T>(
          value: mode.mapper(entry.value),
          sequence: entry.value['sequence'] as int? ?? 0,
        );
      }
    } else {
      for (final aggId in aggregateIds) {
        final row = await _storage.readViewRow(mode.viewName, aggId);
        yield Snapshot<T>(
          value: row == null ? null : mode.mapper(row),
          sequence: (row?['sequence'] as int?) ?? 0,
        );
      }
    }

    // Drain any buffered live changes that occurred during snapshot read
    for (final change in liveBuffer) {
      final u = _changeToUpdate<T>(change, filter, mode);
      if (u != null) yield u;
    }
    liveBuffer.clear();

    // Attach to live stream proper
    await for (final change in _subs.rowChanges(mode.viewName)) {
      final u = _changeToUpdate<T>(change, filter, mode);
      if (u != null) yield u;
    }
  } finally {
    await liveSub.cancel();
  }
}

Update<T>? _changeToUpdate<T>(_RowChange c, SubscriptionFilter filter, AggregateMode<T> mode) {
  final aggSet = filter.aggregates;
  if (aggSet != null && !aggSet.contains(c.aggregateId)) return null;
  if (c.isTombstone) {
    return Tombstone<T>(aggregateId: c.aggregateId, sequence: c.sequence);
  }
  return Delta<T>(
    value: mode.mapper(c.value!),
    sequence: c.sequence,
    cause: c.cause,
  );
}
```

In `EventStore.append`, after the transaction commits, publish each change returned by the interpreter:

```dart
for (final change in changes) {
  _subs.publishRowChange(
    viewName: change.viewName,
    aggregateId: change.aggregateId,
    value: change.newValue,
    sequence: change.sequence,
    cause: change.cause,
    isTombstone: change.isTombstone,
  );
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd event_sourcing && flutter test test/subscriptions/aggregate_mode_test.dart`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add event_sourcing/lib/src/event_store.dart \
        event_sourcing/lib/src/projections/interpreter/aggregate_fold.dart \
        event_sourcing/lib/src/projections/interpreter/table_fold.dart \
        event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart \
        event_sourcing/lib/src/subscriptions/subscription_engine.dart \
        event_sourcing/test/subscriptions/aggregate_mode_test.dart
git commit -m "[CUR-1317] subscribe<T> AggregateMode<T> with snapshot + deltas + tombstone"
```

---

## Phase F — Migration of existing materializers + rebuildView

### Task 20: Migrate `RolePermissionGrants` to `TableProjectionSpec` (substrate's policy mechanism)

**Framing:** `RolePermissionGrants` is **the substrate's policy projection**, not a reference implementation. Per Architectural Commitment 4 in the spec (Permission policy is substrate code), `event_sourcing/lib/src/permissions/` ships exactly one policy mechanism in v1; alternative policy models require library extension, not app-side replacement. This task migrates the existing untyped materializer to the new declarative `TableProjectionSpec` form while preserving its substrate-mandated status.

**Files:**

- Create: `event_sourcing/lib/src/permissions/role_permission_grants_spec.dart`
- Modify: every site that constructs the old `RolePermissionGrantsMaterializer` (run `grep -rn 'RolePermissionGrantsMaterializer' event_sourcing/`)
- Delete: `event_sourcing/lib/src/permissions/role_permission_grants_materializer.dart`
- Rewrite: `event_sourcing/test/permissions/role_permission_grants_materializer_test.dart` → `event_sourcing/test/permissions/role_permission_grants_spec_test.dart`

- [ ] **Step 1: Author the spec**

```dart
// event_sourcing/lib/src/permissions/role_permission_grants_spec.dart
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';

final rolePermissionGrantsSpec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(eventTypes: const {
    'permission_granted',
    'permission_revoked',
  }),
  insertEventTypes: const {'permission_granted'},
  removeEventTypes: const {'permission_revoked'},
  rowKey: const CompositeKey(['data.role', 'data.permission', 'data.scope']),
  rowData: const WholePayload(),
);

// NOTE: StoredEvent.data IS the payload root in this codebase. CompositeKey
// path 'data.role' resolves to event.data['role']. WholePayload() returns
// the event's full data Map as the row.
```

- [ ] **Step 2: Rewrite the test against the new spec, asserting the same observable rows**

Reuse the test fixtures from the existing `role_permission_grants_materializer_test.dart`, but:

- Construct an `EventStore.open` with `ProjectionRegistry()..register(rolePermissionGrantsSpec)`
- Append events via `store.append(EventDraft(...))`
- Assert via `backend.readViewRow('role_permission_grants', composite_key_string)`
- Cover: insert, remove, no-op for unrelated events, idempotency (re-insert same row)

- [ ] **Step 3: Run the new test to verify it passes**

Run: `cd event_sourcing && flutter test test/permissions/role_permission_grants_spec_test.dart`
Expected: PASS.

- [ ] **Step 4: Replace every construction site of `RolePermissionGrantsMaterializer`**

Run: `grep -rn 'RolePermissionGrantsMaterializer' event_sourcing/lib event_sourcing/test`
For each match, swap the materializer with `..register(rolePermissionGrantsSpec)` on the `ProjectionRegistry`.

- [ ] **Step 5: Delete the old materializer + its test**

```bash
git rm event_sourcing/lib/src/permissions/role_permission_grants_materializer.dart
git rm event_sourcing/test/permissions/role_permission_grants_materializer_test.dart
```

- [ ] **Step 6: Run the full test suite to verify nothing else broke**

Run: `cd event_sourcing && flutter test`
Expected: PASS for all tests; any failures point to construction sites missed in step 4.

- [ ] **Step 7: Commit**

```bash
git add event_sourcing/lib/src/permissions/role_permission_grants_spec.dart \
        event_sourcing/test/permissions/role_permission_grants_spec_test.dart \
        event_sourcing/lib/  # for construction-site swaps
git commit -m "[CUR-1317] Migrate RolePermissionGrants to TableProjectionSpec; remove legacy materializer"
```

---

### Task 21: Delete diary-domain code from the lib

**Framing:** the 2026-05-10 audit identified that `DiaryEntry`, `DiaryEntriesMaterializer`, the `rebuildMaterializedView()` diary-specific helper, `EntryService`, and ~25 tests are kick-start extraction debt — domain-specific code that should live in `hht_diary`, not in this domain-neutral substrate. This task removes that debt outright. `hht_diary` will author its own `diaryEntriesSpec` (and any associated `PromoterSpec`s) against the lib's `ProjectionRegistry` when it adopts the new lib pin (Phase III in the README's roadmap). **No diary code is moved into a new in-lib location** — the lib gets out of the diary business entirely.

**Files (all to be deleted from this lib):**

- `event_sourcing/lib/src/storage/diary_entry.dart` — `DiaryEntry` row type
- `event_sourcing/lib/src/materialization/diary_entries_materializer.dart` — diary fold logic
- `event_sourcing/lib/src/entry_service.dart` — legacy diary write path
- `event_sourcing/lib/src/materialization/rebuild.dart` — only the `rebuildMaterializedView()` function (the parameterized `rebuildView()` in the same file is generic; it survives and is rewired in Task 23)
- `event_sourcing/test/materialization/diary_entries_materializer_test.dart`
- `event_sourcing/test/materialization/entry_promoter_test.dart` (diary-promoter-flavoured tests; the abstract `EntryPromoter` itself goes in Task 22)
- `event_sourcing/test/entry_service_test.dart` (if present; verify by grep)
- Any other diary-tainted test the audit surfaces

**Files (modify):**

- `event_sourcing/lib/event_sourcing.dart` — remove the `export 'src/storage/diary_entry.dart' show DiaryEntry;` and `export 'src/materialization/diary_entries_materializer.dart' show DiaryEntriesMaterializer;` lines (per audit findings, lines ~206 and ~285 of that file)
- `event_sourcing/lib/src/materialization/rebuild.dart` — keep `rebuildView`; delete `rebuildMaterializedView` and any helpers that only the deleted function used

- [ ] **Step 1: Confirm the deletion list against the live codebase**

Run:

```bash
grep -rn 'class DiaryEntry\b\|DiaryEntriesMaterializer\b\|EntryService\b\|rebuildMaterializedView\b' event_sourcing/lib event_sourcing/test
```

Cross-check against the audit's tainted-files list. Surface anything new (the kick-start may have left additional diary-flavoured files the audit didn't catch). If a file references `DiaryEntry` only via test fixtures and the test is exercising substrate behaviour (e.g., a storage-backend test that happens to use `DiaryEntry` as a sample row), it's a candidate for genericization in Step 4 rather than deletion.

- [ ] **Step 2: Delete the public API exports**

In `event_sourcing/lib/event_sourcing.dart`, remove the two `export` lines for `DiaryEntry` and `DiaryEntriesMaterializer`. Run `cd event_sourcing && flutter analyze` afterwards to surface every consumer that imported these symbols via the public API; those consumers are about to break (which is correct — they're either tests being rewritten in this task or hht_diary code that will resurface during Phase III adoption, not lib code).

- [ ] **Step 3: Delete the tainted lib files**

```bash
cd event_sourcing/event_sourcing
git rm lib/src/storage/diary_entry.dart \
       lib/src/materialization/diary_entries_materializer.dart \
       lib/src/entry_service.dart
```

For `lib/src/materialization/rebuild.dart`: edit to remove only the `rebuildMaterializedView()` function and any private helpers it exclusively uses; keep the file (and `rebuildView()`).

- [ ] **Step 4: Triage the ~25 diary-tainted tests**

Run: `grep -rln 'DiaryEntry\|DiaryEntriesMaterializer' event_sourcing/test`

For each tainted test, classify into one of:

- **Substrate behaviour test that happens to use diary fixtures** → rewrite using a small toy in-test materializer (the `LightsMaterializer` pattern from `event_sourcing/example/` is a good template). Example targets: storage-backend contract tests, generic materializer-behaviour tests.
- **Pure diary-domain test** → delete from this lib. It will be reauthored in `hht_diary` against the new `diaryEntriesSpec` when hht_diary adopts the new lib pin. Don't try to preserve it; greenfield discipline (see memory).
- **Test that exercises the `RolePermissionGrants` path through the diary materializer** → rewrite to exercise the same path through the substrate's projection interpreter directly, with no diary references.

For each rewrite, the rule is: the test verifies a *substrate property* (atomicity, fold determinism, hash chain, version migration, etc.), so the test should construct the smallest possible domain-neutral fixture that exercises that property. Toy fixtures > diary fixtures.

- [ ] **Step 5: Delete the pure-domain tests**

```bash
git rm event_sourcing/test/materialization/diary_entries_materializer_test.dart \
       event_sourcing/test/materialization/entry_promoter_test.dart
# plus any others identified in step 4 as pure-domain
```

- [ ] **Step 6: Commit the rewritten substrate-behaviour tests**

After Step 4's rewrites land, run `cd event_sourcing && flutter test` to confirm they pass against the substrate without any diary references.

- [ ] **Step 7: Sweep remaining call sites of deleted symbols**

```bash
grep -rn 'DiaryEntry\|DiaryEntriesMaterializer\|EntryService\|rebuildMaterializedView' event_sourcing/
```

Expected: no matches. Any matches indicate code that depended on the deleted symbols and now needs either deletion or replacement with substrate-generic equivalents.

- [ ] **Step 8: Run the full suite**

Run: `cd event_sourcing && flutter test`
Expected: PASS.

Run: `cd event_sourcing && flutter analyze`
Expected: no errors. Warnings about unused imports in `event_sourcing.dart` are acceptable if they reflect imports that became unused after the export removals; clean those up.

- [ ] **Step 9: Commit**

```bash
git add event_sourcing/
git commit -m "[CUR-1317] Delete diary-domain code from event_sourcing lib (extraction debt cleanup)"
```

The commit message should note that hht_diary will author its own spec when it adopts the new lib pin, and that the audit findings are recorded in `docs/superpowers/specs/2026-05-09-projections-and-subscribe-design.md` Section 6.

---

### Task 22: Delete the abstract `Materializer`, `EntryPromoter`, and related dead code

**Files (all to be deleted):**

- `event_sourcing/lib/src/materialization/materializer.dart`
- `event_sourcing/lib/src/materialization/entry_promoter.dart`
- `event_sourcing/lib/src/materialization/entry_type_definition_lookup.dart` (only if no longer referenced — verify)
- `event_sourcing/test/materialization/materializer_test.dart`
- `event_sourcing/test/materialization/materializer_target_version_test.dart`

Files to keep: `event_sourcing/lib/src/materialization/rebuild.dart` is rewired in Task 23, not deleted.

- [ ] **Step 1: Verify no remaining references to deleted symbols**

Run:

```bash
grep -rn 'class Materializer\b\|extends Materializer\b\|EntryPromoter\b\|DiaryEntryPromoter' event_sourcing/lib event_sourcing/test
```

Expected: only matches inside the files about to be deleted.

If any other file still references these symbols, address those before deletion.

- [ ] **Step 2: Delete the files**

```bash
git rm event_sourcing/lib/src/materialization/materializer.dart \
       event_sourcing/lib/src/materialization/entry_promoter.dart \
       event_sourcing/test/materialization/materializer_test.dart \
       event_sourcing/test/materialization/materializer_target_version_test.dart
```

If `entry_type_definition_lookup.dart` is unused: `git rm` it as well.

- [ ] **Step 3: Run full suite to verify clean removal**

Run: `cd event_sourcing && flutter test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git commit -m "[CUR-1317] Remove legacy Materializer abstract class and EntryPromoter interface"
```

---

### Task 23: Rewire `rebuildView` to interpret `ProjectionSpec`

**Files:**

- Modify: `event_sourcing/lib/src/materialization/rebuild.dart` — only the parameterized `rebuildView()` function remains by this point (the diary-specific `rebuildMaterializedView()` was deleted in Task 21)
- Modify: `event_sourcing/test/materialization/rebuild_test.dart` — rewrite against the substrate's projection interpreter using a toy in-test materializer (the file's existing diary-flavoured fixtures should have been genericized in Task 21 step 4; this task verifies that and adapts the harness to the new `rebuildView` signature)

- [ ] **Step 1: Update `rebuild_test.dart` to operate against ProjectionSpecs**

Adapt the existing tests to construct an `EventStore.open` with a populated `ProjectionRegistry`, append a known event sequence, then call `rebuildView` against a registered spec. Use a toy in-test materializer (e.g., the `LightsMaterializer`-style projection from `example/`) — no diary references should remain in the test file by this point. Assert that the resulting view rows match what the spec's fold mechanics would produce against the same event sequence. Cover: full clear-and-replay, atomicity (failure mid-rebuild rolls back), per-entryType target-version map.

- [ ] **Step 2: Implement the new `rebuildView`**

```dart
Future<int> rebuildView({
  required EventStore store,
  required String viewName,
  required Map<String, int> targetVersionByEntryType,
}) async {
  final spec = store.projections.lookup(viewName);
  if (spec == null) {
    throw StateError('rebuildView: no ProjectionSpec registered under "$viewName"');
  }
  return store.storage.runInTransaction((txn) async {
    await store.storage.clearViewInTxn(txn, viewName);
    await _writeTargetVersionsInTxn(txn, store.storage, viewName, targetVersionByEntryType);
    var processed = 0;
    final events = store.storage.readEventsInTxn(txn);
    await for (final event in events) {
      if (!spec.interest.matches(event)) continue;
      // Promote event payload via PromoterRegistry chain to the entryType's target version.
      final tgt = targetVersionByEntryType[event.entryType];
      if (tgt == null) continue;
      final promoted = PromoterExecutor.promote(
        registry: store.promoters,
        viewName: viewName,
        entryType: event.entryType,
        fromVersion: event.entryVersion,
        toVersion: tgt,
        payload: event.data,
        firstEventTimestamp: event.clientTimestamp,
      );
      // Apply via the appropriate fold interpreter against the promoted payload.
      switch (spec) {
        case AggregateProjectionSpec():
          await AggregateFold.applyEvent(
            txn: txn, backend: store.storage, spec: spec,
            event: event.copyWith(data: promoted),
          );
        case TableProjectionSpec():
          await TableFold.applyEvent(
            txn: txn, backend: store.storage, spec: spec,
            event: event.copyWith(data: promoted),
          );
      }
      processed++;
    }
    return processed;
  });
}
```

(`StoredEvent.copyWith(data: ...)` may not exist; add it as part of this task if needed.)

- [ ] **Step 3: Run rebuild tests**

Run: `cd event_sourcing && flutter test test/materialization/rebuild_test.dart`
Expected: PASS.

- [ ] **Step 4: Run full suite**

Run: `cd event_sourcing && flutter test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add event_sourcing/lib/src/materialization/rebuild.dart \
        event_sourcing/lib/src/storage/stored_event.dart \
        event_sourcing/test/materialization/rebuild_test.dart
git commit -m "[CUR-1317] Rewire rebuildView to interpret ProjectionSpec + PromoterRegistry"
```

---

## Self-review

After every task above is completed, verify against the spec:

**Spec coverage check:**

- Spec § Architectural Commitment 1 (Projections closed under events) → Tasks 9, 13, 14, 16 (substrate, no author code)
- Spec § Architectural Commitment 2 (Library primitives append-only) → Tasks 4-8 (initial primitive catalogue)
- Spec § Architectural Commitment 3 (Library version recorded in log) → Tasks 1, 2, 3
- Spec § Architectural Commitment 4 (Permission policy is substrate code) → Task 20 (RolePermissionGrants migration as substrate-mandated policy mechanism, not reference impl)
- Spec § "Closed set of shapes" → Tasks 9 (ProjectionSpec types), 13 (Aggregate fold), 14 (Table fold)
- Spec § "Per-shape fold mechanics — AggregateProjectionSpec" → Task 13
- Spec § "Per-shape fold mechanics — TableProjectionSpec" → Task 14
- Spec § "Library-supplied derivation primitives" → Task 5
- Spec § "Library-supplied row extractors" → Tasks 6, 7
- Spec § "Storage shape" → Task 0 (genericization rename) + verified by Tasks 13, 14 writing Map rows directly
- Spec § "Registration" → Tasks 11, 12
- Spec § "Specs as events (closing the loop)" → Phase II hook; not implemented in Phase I (per spec)
- Spec § Promoter model → Tasks 8, 10, 12, 15
- Spec § Subscribe primitive — signature → Task 18 (Events), Task 19 (AggregateMode)
- Spec § Subscription filter → Verified in Task 9 (existing type, possibly relocated)
- Spec § Update<T> envelope → Task 17
- Spec § Per-mode behavior → Tasks 18, 19
- Spec § Snapshot for absent and tombstoned aggregates → Task 19
- Spec § Substrate's responsibilities → Tasks 16, 18, 19
- Spec § History + live composition → Documented in subscribe<T> docs; covered by Task 18's "from-now-forward" assertion
- Spec § At-least-once → Within-process Dart Stream guarantee; cross-process is Destination's domain (out of scope)
- Spec § Library-version events → Task 1 (constants), Task 2 (scan), Task 3 (boot flow)
- Spec § Bootstrap flow → Task 3
- Spec § EventStore.open composition → Tasks 3, 16
- Spec § "What is removed (lib-level)" → legacy substrate interfaces in Tasks 21-22; diary-domain code in Task 21
- Spec § "What is migrated (stays in lib, new shape)" → Task 20 (RolePermissionGrants → TableProjectionSpec)
- Spec § "What carries over" — `StorageBackend` view-row methods after rename → Task 0
- Spec § "What hht_diary takes on" → out of scope of this lib; documented as Phase III follow-up

**Final pass:**

- [ ] Run full test suite: `cd event_sourcing && flutter test`
- [ ] Run analyzer: `cd event_sourcing && flutter analyze`
- [ ] Verify no remaining `// Implements: REQ-d{NNNNN}` annotations in the new files (only `// Implements: EVS-DEV-*` annotations belong here; legacy refs removed with deleted code).
- [ ] Verify `event_sourcing/lib/event_sourcing.dart` no longer exports any diary types (`DiaryEntry`, `DiaryEntriesMaterializer`).
- [ ] Verify `grep -rn 'DiaryEntry\|DiaryEntriesMaterializer\|EntryService' event_sourcing/lib event_sourcing/test` returns no matches.

## Out of scope (follow-up plans needed)

- **Track 8 from the spec** — migration of `event_sourcing/example/` and `event_sourcing/example_action_permissions/` from `watchEvents`/`watchView`/`watchFifo` to `subscribe<T>`. Once this plan completes, the legacy backend-layer methods can be considered for removal in a follow-up plan that also migrates the demo apps.
- **hht_diary adoption (Phase III in the README's roadmap)** — once this plan lands, hht_diary is responsible for:
  - moving `DiaryEntry` (or its successor) into its own codebase
  - authoring its `diaryEntriesSpec` (`AggregateProjectionSpec`) against the lib's `ProjectionRegistry`
  - authoring its `PromoterSpec`s for any diary entry-type schema evolution
  - re-implementing its write path on top of `EventStore.append` (replacing the deleted `EntryService.record`)
  - reauthoring the diary-specific tests against the new spec form
  - bumping the lib pin in `clinical_diary/pubspec.yaml` to a tag that includes this plan's output
- **Authoring of `EVS-DEV-*` requirements in `spec/`** for each implemented component. Per CLAUDE.md these land alongside code; this plan's task descriptions identify what each implementation satisfies, but the actual `spec/dev-*.md` files should be authored by the implementing engineer alongside each task's commit. Update this plan to add explicit "author EVS-DEV-foo" steps if reviewers prefer them surfaced in the plan rather than treated as implicit.
