# Entry-Type Version Substrate-Owned — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the substrate the sole authority over `entryTypeVersion`. Producers never choose it; the substrate stamps the registry's `registeredVersion` on every append. Ingest transparently promotes older-peer events before fold. `EventStore.open` snapshot-promotes lagging view rows on `registeredVersion` bump and refuses downgrade.

**Architecture.** (1) Drop the `entryTypeVersion` parameter from `EventStore.append`/`appendInTxn`; the substrate looks it up from the registry inside the implementation. (2) Generalize `StorageBackend.findAllEvents` with `entryType` + `clientTimestampStart`/`clientTimestampEnd` filters — foundation for snapshot promotion and richer audit-stream UX. (3) Add `lib/src/projections/snapshot_promotion.dart` with three boot-time helpers — `seedViewTargetVersions`, `assertNoEntryTypeDowngrade`, `promoteViewSnapshots` — wired into `EventStore.open` in that order. (4) Move ingest-side per-event promotion into `ProjectionInterpreter` so existing call sites (`EventStore.append`, `_ingestOneInTxn`) stay unchanged at the call point. (5) Delete `DeriveField` from the `TransformPrimitive` set so promoter chains are strictly fold-commutative (shape-changers only).

**Tech Stack.** Dart (Flutter package), sembast (in-lib reference `StorageBackend`). Test framework `flutter_test` with `sembast_memory`.

**Design source.** [`docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md`](../specs/2026-05-11-entry-type-version-substrate-owned-design.md). Read it before starting; this plan does not re-derive its decisions.

**Working directory.** All paths in this plan are relative to `event_sourcing/` (the Dart package root) unless otherwise specified. The example apps live at `event_sourcing/example/` and `event_sourcing/example_action_permissions/`.

**Test commands.** Run `flutter test test/path/to/test.dart` for one file, `flutter test test/path/` for a subdir, `flutter test` for the whole suite. `flutter analyze lib test` for static analysis. Example apps: `cd example && flutter test`, `cd example_action_permissions && flutter test`.

**Test API convention.** All view-state read methods on `StorageBackend` exist only as `*InTxn` variants (no non-transactional convenience wrappers): `readViewRowInTxn`, `readViewTargetVersionInTxn`, `readAllViewTargetVersionsInTxn`. Test code that reads view state for assertions therefore wraps the read in a `backend.transaction((txn) async { expect(await backend.readXInTxn(txn, ...), ...); });` block. `appendEvent` and the existing per-aggregate / hash-tail reads also have Txn-only variants. By contrast, `findAllEvents` has both a non-Txn variant and an `*InTxn` variant — the non-Txn form opens its own transaction internally and is the right choice for after-the-fact event-log assertions. Apply this consistently to every code block below.

---

## File map

| Path | Action | Responsibility |
|---|---|---|
| `lib/src/promoters/primitives/transform.dart` | modify | Delete the `DeriveField` class. |
| `test/promoters/primitives/transform_test.dart` | modify | Delete the `DeriveField` test group. |
| `lib/src/security/system_entry_types.dart` | modify | Add `ingest-audit` and `view_snapshot_promoted` definitions to `kSystemEntryTypes`. |
| `lib/src/storage/storage_backend.dart` | modify | Extend `findAllEvents` / `findAllEventsInTxn` signatures with `entryType`, `clientTimestampStart`, `clientTimestampEnd` filters. |
| `lib/src/storage/sembast_backend.dart` | modify | Implement the new filters in the reference impl. |
| `test/storage/find_all_events_filters_test.dart` | create | Cover the new filters (entryType, timestamp range, AND composition, existing axes unchanged). |
| `lib/src/storage/stored_event.dart` | modify | Add `StoredEvent.withData(Map<String, Object?> newData)` method. |
| `test/storage/stored_event_test.dart` | modify | Verify `withData` preserves all other fields. |
| `lib/src/event_store.dart` | modify | (a) Drop `entryTypeVersion` from `append`/`appendInTxn`; substrate looks it up. (b) Add `EntryTypeVersionDowngradeError`. (c) Wire seeding + downgrade-check + snapshot-promotion into `EventStore.open` (and `openForTest` opt-in). (d) Update `clearSecurityContext`, `applyRetentionPolicy`, raw-path callers. |
| `lib/src/actions/action_dispatcher.dart` | modify | Drop `entryTypeVersion: 1` from 2 append-call sites. |
| `lib/src/bootstrap.dart` | modify | Drop `entryTypeVersion: initDef.registeredVersion` from the registry-initialized audit append. |
| `lib/src/destinations/destination_registry.dart` | modify | Drop `entryTypeVersion: def?.registeredVersion ?? 0`. |
| `lib/src/permissions/event_seed_applier.dart` | modify | Drop `entryTypeVersion: 1`. |
| `lib/src/projections/interpreter/projection_interpreter.dart` | modify | Constructor takes `(projections, promoters, entryTypes)`. `applyEvent` detects version mismatch and applies promoter chain per-spec before fold. |
| `lib/src/projections/rebuild.dart` | modify | Use `StoredEvent.withData` instead of the private `_withData` helper. |
| `lib/src/projections/snapshot_promotion.dart` | create | `seedViewTargetVersions`, `assertNoEntryTypeDowngrade`, `promoteViewSnapshots` — the three boot-time helpers. |
| `test/projections/snapshot_promotion_test.dart` | create | Seeding (greenfield + no-op reboot + ignore-non-interest), downgrade refusal, snapshot promotion (lagging rows promoted, non-affected rows untouched, audit event emitted), mid-promotion crash recovery. |
| `test/projections/interpreter_promotion_test.dart` | create | Ingest-side per-spec promotion: peer event at older version is folded as promoted; multi-spec divergence; equal-version path untouched. |
| `test/event_store/append_stamps_registered_version_test.dart` | create | `append`/`appendInTxn` stamp `registeredVersion`; throw on unregistered entry type. |
| `example/lib/widgets/top_action_bar.dart` | modify | Drop `entryTypeVersion: 1` from the rebuild-trigger append (line ~102). |
| `example_action_permissions/lib/server/bootstrap.dart` | modify | Drop `entryTypeVersion: 1` (line ~141). |
| `lib/event_sourcing.dart` | modify | Export `EntryTypeVersionDowngradeError`. |

---

## Tasks

### Task 1: Delete `DeriveField` from the promoter primitive set

`DeriveField` has zero production callers (verified). It's the only `TransformPrimitive` that is non-commutative with the fold under the snapshot-promotion design, so the spec deletes it now (Append-Only Primitives discipline only kicks in once a primitive ships externally; this never has).

**Files:**

- Modify: `lib/src/promoters/primitives/transform.dart`
- Modify: `test/promoters/primitives/transform_test.dart`

- [ ] **Step 1: Remove the `DeriveField` test group**

In `test/promoters/primitives/transform_test.dart`, locate the `group('DeriveField', () { ... });` block (starts around line 60) and the related `import 'package:event_sourcing/src/projections/primitives/derived_field.dart';` import line if it's only used by that group. Delete the group block. If the derived_field import is only referenced by that group, delete the import too.

- [ ] **Step 2: Run the test file to verify failure (compile error in production code referenced by the test that no longer exists)**

```bash
flutter test test/promoters/primitives/transform_test.dart 2>&1 | tail -20
```

Expected: all remaining groups (`RenameField`, `DefaultField`, `DropField`, `TransformChain`) pass.

- [ ] **Step 3: Delete the `DeriveField` class from `lib/src/promoters/primitives/transform.dart`**

Remove the entire `class DeriveField extends TransformPrimitive { ... }` block (roughly lines 66–84 of the current file).

If the file's top-level `import 'package:event_sourcing/src/projections/primitives/derived_field.dart';` line becomes unused, remove it as well.

- [ ] **Step 4: Verify the package builds and the test suite is green**

```bash
flutter analyze lib test 2>&1 | tail -10
flutter test test/promoters/ 2>&1 | tail -10
```

Expected: analyzer clean (no new warnings beyond pre-existing); promoter test groups all pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/promoters/primitives/transform.dart test/promoters/primitives/transform_test.dart
git commit -m "[CUR-1317] Delete DeriveField from promoter primitive set

Zero production callers; non-commutative with the deep-merge fold which
makes snapshot-promotion-equivalent-to-replay impossible to guarantee.
The substrate's promoter primitive set is now strictly shape-changers
(Rename / Default / Drop), all fold-commutative.

See: docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md"
```

---

### Task 2: Register `ingest-audit` and `view_snapshot_promoted` system entry types

`ingest-audit` covers the two existing raw-path callers (`logRejectedBatch`, `_emitDuplicateReceivedInTxn`); `view_snapshot_promoted` is emitted by the new boot-time snapshot-promotion pass.

**Files:**

- Modify: `lib/src/security/system_entry_types.dart`
- Modify: `test/security/system_entry_types_materialize_false_test.dart` (existing test asserts the registry contents; needs to grow with the new entries)

- [ ] **Step 1: Write the failing test**

Open `test/security/system_entry_types_materialize_false_test.dart`. Locate the test that iterates `kReservedSystemEntryTypeIds` or `kSystemEntryTypes` and asserts membership. Add two new test cases (or extend an existing parameterized one):

```dart
test('kSystemEntryTypes registers ingest-audit at version 1, '
    'non-materializing', () {
  final byId = {for (final d in kSystemEntryTypes) d.id: d};
  expect(byId.containsKey('ingest-audit'), isTrue,
      reason: 'ingest-audit must be a registered system entry type so the '
          'raw-path ingest-audit callers can read registeredVersion from '
          'the registry instead of hardcoding.');
  expect(byId['ingest-audit']!.registeredVersion, 1);
  expect(byId['ingest-audit']!.materialize, isFalse);
});

test('kSystemEntryTypes registers view_snapshot_promoted at version 1, '
    'non-materializing', () {
  final byId = {for (final d in kSystemEntryTypes) d.id: d};
  expect(byId.containsKey('view_snapshot_promoted'), isTrue,
      reason: 'view_snapshot_promoted is emitted by the boot-time '
          'snapshot-promotion pass and must be in the registry to be '
          'append-stampable under the new substrate-owned version model.');
  expect(byId['view_snapshot_promoted']!.registeredVersion, 1);
  expect(byId['view_snapshot_promoted']!.materialize, isFalse);
});
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
flutter test test/security/system_entry_types_materialize_false_test.dart 2>&1 | tail -15
```

Expected: the two new tests fail because `kSystemEntryTypes` does not contain either id yet.

- [ ] **Step 3: Add the two new entries to `lib/src/security/system_entry_types.dart`**

Above `kSystemEntryTypes` declare two top-level constants alongside the existing `kSecurityContextRedactedEntryType` style:

```dart
const String kIngestAuditEntryType = 'ingest-audit';
const String kViewSnapshotPromotedEntryType = 'view_snapshot_promoted';
```

In the `kReservedSystemEntryTypeIds` list (locate near the top of the file), add both constants.

In the `kSystemEntryTypes` list (near the bottom of the file, the 12-entry `<EntryTypeDefinition>[ ... ]`), append:

```dart
  EntryTypeDefinition(
    id: kIngestAuditEntryType,
    registeredVersion: 1,
    name: 'Ingest Audit',
    materialize: false,
  ),
  EntryTypeDefinition(
    id: kViewSnapshotPromotedEntryType,
    registeredVersion: 1,
    name: 'View Snapshot Promoted',
    materialize: false,
  ),
```

- [ ] **Step 4: Run the test, verify it passes**

```bash
flutter test test/security/system_entry_types_materialize_false_test.dart 2>&1 | tail -10
```

Expected: all tests in the file pass, including the two new ones.

- [ ] **Step 5: Run the full security test dir to confirm no regressions**

```bash
flutter test test/security/ 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add lib/src/security/system_entry_types.dart test/security/system_entry_types_materialize_false_test.dart
git commit -m "[CUR-1317] Register ingest-audit and view_snapshot_promoted system entry types

ingest-audit covers the raw-path callers (logRejectedBatch and
_emitDuplicateReceivedInTxn) so they can read registeredVersion from
the registry instead of hardcoding 1. view_snapshot_promoted is the
audit event emitted by the boot-time snapshot-promotion pass landing
in a later task. Both are materialize: false."
```

---

### Task 3: Extend `findAllEvents` / `findAllEventsInTxn` with entry-type + timestamp filters

Adds three optional parameters (`entryType`, `clientTimestampStart`, `clientTimestampEnd`) to the existing read primitive on `StorageBackend`. Implemented in the sembast reference backend; test the AND composition and unchanged behavior of existing filters. This is foundation work for snapshot promotion (which needs to ask "which aggregates have ever produced an event of type X?") and for richer audit-stream UX in general.

**Files:**

- Modify: `lib/src/storage/storage_backend.dart`
- Modify: `lib/src/storage/sembast_backend.dart`
- Create: `test/storage/find_all_events_filters_test.dart`

- [ ] **Step 1: Write the failing test**

Create `test/storage/find_all_events_filters_test.dart`:

```dart
// Verifies: findAllEvents grows entryType + clientTimestamp{Start,End}
// filters. All filters compose with AND. Pre-existing parameters are
// unchanged.

import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _counter = 0;

Future<SembastBackend> _openBackend() async {
  final db = await databaseFactoryMemory.openDatabase('test_${_counter++}.db');
  return SembastBackend(db: db);
}

StoredEvent _event({
  required int seq,
  required String entryType,
  required DateTime clientTimestamp,
  String aggregateId = 'agg-1',
}) {
  return StoredEvent(
    key: 'k$seq',
    eventId: 'e$seq',
    aggregateId: aggregateId,
    aggregateType: 'note',
    entryType: entryType,
    entryTypeVersion: 1,
    libFormatVersion: 1,
    eventType: 'finalized',
    sequenceNumber: seq,
    data: <String, Object?>{},
    metadata: <String, Object?>{'provenance': <Map<String, Object?>>[]},
    initiator: <String, Object?>{'kind': 'system', 'service': 'test'},
    clientTimestamp: clientTimestamp,
    eventHash: 'h$seq',
    flowToken: null,
    previousEventHash: null,
  );
}

void main() {
  group('findAllEvents extended filters', () {
    test('entryType filter returns only matching events', () async {
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
        await b.appendEvent(txn, _event(
            seq: 2, entryType: 'lights', clientTimestamp: DateTime.utc(2026, 1, 2)));
        await b.appendEvent(txn, _event(
            seq: 3, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 3)));
      });

      final notes = await b.findAllEvents(entryType: 'note');
      expect(notes.map((e) => e.sequenceNumber).toList(), [1, 3]);

      final lights = await b.findAllEvents(entryType: 'lights');
      expect(lights.map((e) => e.sequenceNumber).toList(), [2]);
    });

    test('clientTimestampStart filter is inclusive-lower-bound', () async {
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
        await b.appendEvent(txn, _event(
            seq: 2, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 5)));
        await b.appendEvent(txn, _event(
            seq: 3, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 10)));
      });

      final later = await b.findAllEvents(
          clientTimestampStart: DateTime.utc(2026, 1, 5));
      expect(later.map((e) => e.sequenceNumber).toList(), [2, 3]);
    });

    test('clientTimestampEnd filter is inclusive-upper-bound', () async {
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
        await b.appendEvent(txn, _event(
            seq: 2, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 5)));
        await b.appendEvent(txn, _event(
            seq: 3, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 10)));
      });

      final earlier = await b.findAllEvents(
          clientTimestampEnd: DateTime.utc(2026, 1, 5));
      expect(earlier.map((e) => e.sequenceNumber).toList(), [1, 2]);
    });

    test('AND-composes entryType with timestamp range', () async {
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
        await b.appendEvent(txn, _event(
            seq: 2, entryType: 'lights', clientTimestamp: DateTime.utc(2026, 1, 3)));
        await b.appendEvent(txn, _event(
            seq: 3, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 5)));
        await b.appendEvent(txn, _event(
            seq: 4, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 10)));
      });

      final filtered = await b.findAllEvents(
        entryType: 'note',
        clientTimestampStart: DateTime.utc(2026, 1, 3),
        clientTimestampEnd: DateTime.utc(2026, 1, 7),
      );
      expect(filtered.map((e) => e.sequenceNumber).toList(), [3]);
    });

    test('existing afterSequence + limit + originator filters still work '
        'alongside the new ones', () async {
      // Smoke test: with no new filters supplied, behavior matches the
      // pre-existing findAllEvents contract.
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
        await b.appendEvent(txn, _event(
            seq: 2, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 2)));
      });

      final all = await b.findAllEvents();
      expect(all.map((e) => e.sequenceNumber).toList(), [1, 2]);
    });

    test('empty result when no events match', () async {
      final b = await _openBackend();
      await b.transaction((txn) async {
        await b.appendEvent(txn, _event(
            seq: 1, entryType: 'note', clientTimestamp: DateTime.utc(2026, 1, 1)));
      });

      final none = await b.findAllEvents(entryType: 'no_such_type');
      expect(none, isEmpty);
    });
  });
}
```

- [ ] **Step 2: Run the test, verify it fails with a compile error**

```bash
flutter test test/storage/find_all_events_filters_test.dart 2>&1 | tail -20
```

Expected: failure — the named parameters `entryType`, `clientTimestampStart`, `clientTimestampEnd` are not yet defined on `findAllEvents`.

- [ ] **Step 3: Extend the `StorageBackend` abstract signature**

In `lib/src/storage/storage_backend.dart`, modify the `findAllEvents` declaration (around line 77) to:

```dart
  /// All events, optionally sliced by `afterSequence` (exclusive) and
  /// `limit`, and optionally filtered by originator identity, entry type,
  /// and client-timestamp range. All supplied filters compose with AND.
  /// Returned in `sequence_number` order.
  ///
  /// [originatorHopId] matches `provenance[0].hopId`; [originatorIdentifier]
  /// matches `provenance[0].identifier` (existing semantics, unchanged).
  ///
  /// [entryType] matches the event's `entry_type` exactly.
  /// [clientTimestampStart] / [clientTimestampEnd] are inclusive bounds on
  /// `event.client_timestamp`.
  ///
  /// Concrete backends are expected to translate these filters to whatever
  /// query mechanism they support (indexed predicate, WHERE clause, etc.).
  // Implements: REQ-d00154-C — originator filters on findAllEvents.
  // Implements: EVS-DEV-find-all-events-extended-filters — entry-type and
  //   client-timestamp filters on findAllEvents.
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  });
```

Do the same parameter additions to `findAllEventsInTxn` (around line 103). Use identical optional-parameter shape and doc comment additions.

- [ ] **Step 4: Implement the new filters in the sembast backend**

In `lib/src/storage/sembast_backend.dart`, locate `findAllEvents` (search for `Future<List<StoredEvent>> findAllEvents`). The current impl uses a sembast `Filter` composed from the originator parameters; extend the composition.

Before the function body, after the existing originator-filter assembly, add equivalent assembly for the new parameters. The current impl filters on the event store with `Filter.and([...])`. Add:

```dart
    if (entryType != null) {
      filters.add(Filter.equals('entry_type', entryType));
    }
    if (clientTimestampStart != null) {
      filters.add(Filter.greaterThanOrEquals(
        'client_timestamp',
        clientTimestampStart.toUtc().toIso8601String(),
      ));
    }
    if (clientTimestampEnd != null) {
      filters.add(Filter.lessThanOrEquals(
        'client_timestamp',
        clientTimestampEnd.toUtc().toIso8601String(),
      ));
    }
```

(The exact local variable name `filters` may differ — match what the existing impl uses. The shape is "build up a list of Filter conditions then `Filter.and(filters)`".)

Make the same additions in the `findAllEventsInTxn` implementation in the same file.

If `client_timestamp` is stored as a non-ISO-string shape, adjust the comparison values to match the stored representation. The `StoredEvent.toMap` encoding is the source of truth.

- [ ] **Step 5: Run the test, verify it passes**

```bash
flutter test test/storage/find_all_events_filters_test.dart 2>&1 | tail -15
```

Expected: all six tests pass.

- [ ] **Step 6: Run the full storage test dir to confirm no regression**

```bash
flutter test test/storage/ 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add lib/src/storage/storage_backend.dart lib/src/storage/sembast_backend.dart test/storage/find_all_events_filters_test.dart
git commit -m "[CUR-1317] Extend findAllEvents with entryType + client-timestamp filters

Adds entryType, clientTimestampStart, clientTimestampEnd as optional
parameters to StorageBackend.findAllEvents and findAllEventsInTxn. All
filters compose with AND. The sembast reference backend translates each
to a Filter predicate; other backends translate to whatever their query
mechanism supports.

Foundation for the boot-time snapshot-promotion pass (which queries
'which aggregates have ever produced an event of type X?') and for
richer audit-stream UX (timestamp-windowed queries by entry type)."
```

---

### Task 4: Add `StoredEvent.withData(newData)` helper

`rebuild.dart` has a private file-level `_withData` helper. The same operation is needed in `ProjectionInterpreter`'s promotion path. Move it to a `withData` method on `StoredEvent` so both call sites share one implementation.

**Files:**

- Modify: `lib/src/storage/stored_event.dart`
- Modify: `lib/src/projections/rebuild.dart` (delete `_withData`, use `event.withData(...)`)
- Modify: `test/storage/stored_event_test.dart`

- [ ] **Step 1: Write the failing test**

In `test/storage/stored_event_test.dart`, add a test:

```dart
  group('StoredEvent.withData', () {
    test('replaces only the data field; all other fields preserved', () {
      final original = StoredEvent(
        key: 'k1',
        eventId: 'e1',
        aggregateId: 'agg-1',
        aggregateType: 'note',
        entryType: 'note',
        entryTypeVersion: 2,
        libFormatVersion: 1,
        eventType: 'finalized',
        sequenceNumber: 42,
        data: <String, Object?>{'old_key': 'old_value'},
        metadata: <String, Object?>{'provenance': <Map<String, Object?>>[]},
        initiator: <String, Object?>{'kind': 'system'},
        clientTimestamp: DateTime.utc(2026, 1, 1),
        eventHash: 'h1',
        flowToken: null,
        previousEventHash: null,
      );

      final promoted = original.withData(<String, Object?>{
        'new_key': 'new_value',
      });

      expect(promoted.data, {'new_key': 'new_value'});
      // All other fields preserved.
      expect(promoted.key, original.key);
      expect(promoted.eventId, original.eventId);
      expect(promoted.aggregateId, original.aggregateId);
      expect(promoted.aggregateType, original.aggregateType);
      expect(promoted.entryType, original.entryType);
      expect(promoted.entryTypeVersion, original.entryTypeVersion);
      expect(promoted.libFormatVersion, original.libFormatVersion);
      expect(promoted.eventType, original.eventType);
      expect(promoted.sequenceNumber, original.sequenceNumber);
      expect(promoted.metadata, original.metadata);
      expect(promoted.initiator, original.initiator);
      expect(promoted.clientTimestamp, original.clientTimestamp);
      expect(promoted.eventHash, original.eventHash);
      expect(promoted.flowToken, original.flowToken);
      expect(promoted.previousEventHash, original.previousEventHash);
    });
  });
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
flutter test test/storage/stored_event_test.dart 2>&1 | tail -10
```

Expected: failure — `withData` is not defined on `StoredEvent`.

- [ ] **Step 3: Add `withData` to `StoredEvent`**

In `lib/src/storage/stored_event.dart`, add a method on the class (place it after the constructor, before `fromMap`):

```dart
  /// Returns a copy of this event with [newData] replacing [data]. All
  /// other fields are preserved. Used by the substrate's promoter
  /// machinery (rebuildView and ProjectionInterpreter) to thread a
  /// promoted payload through the fold interpreters without modifying the
  /// in-memory original or rebuilding the event hash chain.
  StoredEvent withData(Map<String, Object?> newData) {
    return StoredEvent(
      key: key,
      eventId: eventId,
      aggregateId: aggregateId,
      aggregateType: aggregateType,
      entryType: entryType,
      entryTypeVersion: entryTypeVersion,
      libFormatVersion: libFormatVersion,
      eventType: eventType,
      sequenceNumber: sequenceNumber,
      data: newData,
      metadata: metadata,
      initiator: initiator,
      clientTimestamp: clientTimestamp,
      eventHash: eventHash,
      flowToken: flowToken,
      previousEventHash: previousEventHash,
    );
  }
```

- [ ] **Step 4: Replace `_withData` in `rebuild.dart`**

In `lib/src/projections/rebuild.dart`:

1. Find the call site `_withData(event, promoted)` (around line 104). Change to `event.withData(promoted)`.
2. Delete the entire `StoredEvent _withData(...)` top-level function (lines ~133–156).

- [ ] **Step 5: Run the tests**

```bash
flutter test test/storage/stored_event_test.dart test/projections/rebuild_test.dart 2>&1 | tail -10
```

Expected: both pass.

- [ ] **Step 6: Commit**

```bash
git add lib/src/storage/stored_event.dart lib/src/projections/rebuild.dart test/storage/stored_event_test.dart
git commit -m "[CUR-1317] Add StoredEvent.withData helper; collapse rebuild.dart's private _withData

The substrate's promoter machinery (rebuildView and the
ProjectionInterpreter promotion path landing next) both need to thread a
promoted payload through the fold without modifying the in-memory
original. Lift the helper to a method on StoredEvent so both call sites
share one implementation."
```

---

### Task 5: Drop `entryTypeVersion` parameter from `EventStore.append`/`appendInTxn`

The substrate now stamps `entryTypeVersion = entryTypes.byId(entryType).registeredVersion` internally. All callers that previously passed the parameter drop it. The recordMap construction inside `appendInTxn` reads from the registry. This task is large but mechanical: signature change in `event_store.dart`, then every caller updated. Tests for the stamping behavior are added in this task too.

**Files:**

- Modify: `lib/src/event_store.dart` — signatures of `append`, `appendInTxn`, internal recordMap; existing callers `clearSecurityContext`, `applyRetentionPolicy` (3 sites)
- Modify: `lib/src/actions/action_dispatcher.dart` — 2 sites (line ~239 in `appendInTxn` call, line ~327 in `append` call)
- Modify: `lib/src/bootstrap.dart` — line ~138
- Modify: `lib/src/destinations/destination_registry.dart` — line ~587
- Modify: `lib/src/permissions/event_seed_applier.dart` — line ~73
- Create: `test/event_store/append_stamps_registered_version_test.dart`

Note: example app callers and the raw-path `_appendRawInternalEventInTxn` callers are handled in later tasks (Tasks 6 and 7 respectively).

- [ ] **Step 1: Write the failing test for stamp-from-registry behavior**

Create `test/event_store/append_stamps_registered_version_test.dart`:

```dart
// Verifies: EVS-DEV-append-stamps-registered-version — EventStore.append
// stamps the registry's registeredVersion on every appended event. Callers
// no longer pass entryTypeVersion.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kCustom = EntryTypeDefinition(
  id: 'custom_type',
  registeredVersion: 7,
  name: 'Custom Type',
);

Future<EventStore> _openStore() async {
  final db =
      await databaseFactoryMemory.openDatabase('test_${_dbCounter++}.db');
  final backend = SembastBackend(db: db);
  final registry = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    registry.register(defn);
  }
  registry.register(_kCustom);
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'test',
      identifier: 'test-install',
      softwareVersion: '0.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

void main() {
  group('EventStore.append stamps registeredVersion', () {
    test('stamps registry version 7 for custom_type', () async {
      final store = await _openStore();
      final stored = await store.append(
        entryType: 'custom_type',
        aggregateId: 'agg-1',
        aggregateType: 'custom_type',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const AutomationInitiator(service: 'test'),
      );
      expect(stored, isNotNull);
      expect(stored!.entryTypeVersion, 7,
          reason: 'append must stamp registeredVersion = 7 from the registry, '
              'not a caller-supplied value (since the parameter is removed) '
              'and not a default of 1.');
    });

    test('throws on unregistered entry type (existing behavior preserved)',
        () async {
      final store = await _openStore();
      expect(
        () async => await store.append(
          entryType: 'not_registered',
          aggregateId: 'agg-1',
          aggregateType: 'whatever',
          eventType: 'finalized',
          data: const <String, Object?>{},
          initiator: const AutomationInitiator(service: 'test'),
        ),
        throwsArgumentError,
      );
    });
  });
}
```

- [ ] **Step 2: Run the test; verify the compile failure (entryTypeVersion still required)**

```bash
flutter test test/event_store/append_stamps_registered_version_test.dart 2>&1 | tail -15
```

Expected: compile error — `entryTypeVersion` is still a required parameter on `append`.

- [ ] **Step 3: Change the `append` signature**

In `lib/src/event_store.dart`, locate the `Future<StoredEvent?> append({...})` declaration (around line 446). Remove the `required int entryTypeVersion,` line.

- [ ] **Step 4: Change the `appendInTxn` signature**

In the same file, locate `Future<StoredEvent?> appendInTxn(...)` (around line 748). Remove the `required int entryTypeVersion,` line.

- [ ] **Step 5: Stamp `registeredVersion` inside `appendInTxn`**

Inside `appendInTxn`, locate where `final def = entryTypes.byId(entryType)!;` is read (around line 771). Immediately after that line, add:

```dart
    final entryTypeVersion = def.registeredVersion;
```

The existing `recordMap` construction (around line 837) that uses `'entry_type_version': entryTypeVersion,` continues to work — `entryTypeVersion` is now a local variable instead of a parameter. No other changes needed in the function body.

- [ ] **Step 6: Update `append`'s call to `appendInTxn`**

In the same file, the public `append` method delegates to `appendInTxn`. Locate the delegation (around line 466) and remove the `entryTypeVersion: entryTypeVersion,` line from the call.

- [ ] **Step 7: Update `clearSecurityContext` and `applyRetentionPolicy` internal callers**

In the same file, `clearSecurityContext` calls `appendInTxn` (around line 547). Remove the `entryTypeVersion: entryTypes.byId(kSecurityContextRedactedEntryType)!.registeredVersion,` line.

In `applyRetentionPolicy`, there are three `appendInTxn` calls (compacted, purged, retention_applied — around lines 616–700). Remove the `entryTypeVersion: entryTypes.byId(...)!.registeredVersion,` line from each.

- [ ] **Step 8: Update `action_dispatcher.dart`**

In `lib/src/actions/action_dispatcher.dart`:

- Around line 239 (inside the `executionResult.events` loop in Stage 8), remove `entryTypeVersion: 1,` from the `appendInTxn` call.
- Around line 327 (inside `_persistDenial`), remove `entryTypeVersion: 1,` from the `append` call.

Delete the now-stale "`entryTypeVersion` is hardcoded to 1 for now; consumers must register the `action_denial` entry type at version 1. When the dispatcher's host-bootstrap helper lands (plan-1 Task 22), this hardcoding gets reviewed." doc comment block on `_persistDenial` (lines ~313–319). Replace with a one-line comment if useful, or simply delete.

- [ ] **Step 9: Update `bootstrap.dart`**

In `lib/src/bootstrap.dart`, around line 138 (inside `eventStore.append` of `kEntryTypeRegistryInitializedEntryType`), remove the `entryTypeVersion: initDef.registeredVersion,` line. Also remove the now-unused `final initDef = typeRegistry.byId(kEntryTypeRegistryInitializedEntryType)!;` line on the line above. Update the surrounding doc comment to drop the `REQ-d00134-G — entryTypeVersion read from the registry` line (the behavior is now substrate-stamped, not caller-derived).

- [ ] **Step 10: Update `destination_registry.dart`**

In `lib/src/destinations/destination_registry.dart`, around line 587, remove the `entryTypeVersion: def?.registeredVersion ?? 0,` line from the `appendInTxn` call.

- [ ] **Step 11: Update `event_seed_applier.dart`**

In `lib/src/permissions/event_seed_applier.dart`, around line 73 (inside `eventStore.append`), remove the `entryTypeVersion: 1,` line.

- [ ] **Step 12: Run the test, verify it passes**

```bash
flutter test test/event_store/append_stamps_registered_version_test.dart 2>&1 | tail -15
```

Expected: both tests pass — the substrate stamps `registeredVersion=7` for `custom_type` and `ArgumentError` is still thrown for unregistered types.

- [ ] **Step 13: Run the full lib test suite to surface any uncovered call sites**

```bash
flutter analyze lib test 2>&1 | grep -E "(error|warning)" | head -20
flutter test 2>&1 | tail -20
```

Expected: analyzer surface unchanged (info-level lints only); test suite green. If any test passes `entryTypeVersion:` as a named parameter, update that test to drop it (those are leftover call-site usages from before this task).

- [ ] **Step 14: Commit**

```bash
git add lib/src/event_store.dart lib/src/actions/action_dispatcher.dart lib/src/bootstrap.dart lib/src/destinations/destination_registry.dart lib/src/permissions/event_seed_applier.dart test/event_store/append_stamps_registered_version_test.dart
git commit -m "[CUR-1317] Drop entryTypeVersion parameter from EventStore.append/appendInTxn

Substrate is now the sole authority. Append-call sites no longer pass
entryTypeVersion; appendInTxn stamps entryTypes.byId(entryType)
.registeredVersion internally. All callers in lib/src/ updated:
action_dispatcher (2 sites), bootstrap, destination_registry,
event_seed_applier, and event_store itself (clearSecurityContext,
applyRetentionPolicy x3).

Closes the 'caller hardcodes 1' / 'caller looks up registry' split:
the substrate is the single source of truth for entry-type version on
every append. Producer-side discretion is eliminated by API."
```

---

### Task 6: Drop `entryTypeVersion` from example app callers

Two final `entryTypeVersion: 1` call sites in the example apps. Mechanical follow-up to Task 5.

**Files:**

- Modify: `example/lib/widgets/top_action_bar.dart`
- Modify: `example_action_permissions/lib/server/bootstrap.dart`

- [ ] **Step 1: Update `example/`**

In `example/lib/widgets/top_action_bar.dart`, around line 102, remove the `entryTypeVersion: 1,` line from the `eventStore.append` call.

- [ ] **Step 2: Verify example app builds and tests pass**

```bash
cd example
flutter analyze lib test 2>&1 | tail -5
flutter test 2>&1 | tail -10
cd ..
```

Expected: analyzer clean; all `example` tests pass.

- [ ] **Step 3: Update `example_action_permissions/`**

In `example_action_permissions/lib/server/bootstrap.dart`, around line 141, remove the `entryTypeVersion: 1,` line from the `eventStore.append` call.

- [ ] **Step 4: Verify example_action_permissions app builds and tests pass**

```bash
cd example_action_permissions
flutter analyze lib test 2>&1 | tail -5
flutter test 2>&1 | tail -10
cd ..
```

Expected: analyzer clean; all tests pass.

- [ ] **Step 5: Commit**

```bash
git add example/lib/widgets/top_action_bar.dart example_action_permissions/lib/server/bootstrap.dart
git commit -m "[CUR-1317] Drop entryTypeVersion from example app append callers

Companion to the substrate signature change; example apps now match the
new caller shape. Verified both example apps build clean and their test
suites pass."
```

---

### Task 7: Update raw-path ingest-audit callers to read from the registry

`_appendRawInternalEventInTxn` (top-level helper in `event_store.dart`) is used by `logRejectedBatch`, `_emitDuplicateReceivedInTxn`, and `_appendLibVersionEventToBackend`. The first two now refer to a registered `ingest-audit` entry type (Task 2); their hardcoded `entryTypeVersion: 1` becomes a registry lookup. `_appendLibVersionEventToBackend` keeps its hardcoded 1 — it runs from inside `EventStore.open` before the `EventStore` exists, so it has no access to `entryTypes`. That carve-out is documented in code.

**Files:**

- Modify: `lib/src/event_store.dart`

- [ ] **Step 1: Update `logRejectedBatch`**

In `lib/src/event_store.dart`, around line 1389, locate the `_appendRawInternalEventInTxn` call inside `logRejectedBatch`. Change `entryTypeVersion: 1,` to:

```dart
        entryTypeVersion:
            entryTypes.byId(kIngestAuditEntryType)!.registeredVersion,
```

- [ ] **Step 2: Update `_emitDuplicateReceivedInTxn`**

Around line 1440, the same substitution inside `_emitDuplicateReceivedInTxn`.

- [ ] **Step 3: Verify `_appendLibVersionEventToBackend` stays as-is**

Around line 1569, the call inside `_appendLibVersionEventToBackend` still has `entryTypeVersion: 1`. This is the documented timing carve-out: this function is invoked from inside `EventStore.open` before the `EventStore` instance exists and has no access to `entryTypes`. The comment block on `_appendLibVersionEventToBackend` (around line 1541) explains this; verify the comment is accurate and update if needed to reference the substrate-stamped policy as the rule and this site as the documented exception.

Recommended comment addition:

```dart
/// Append a substrate-emitted lib_version event directly to [backend].
///
/// Bypasses [EventStore.appendInTxn] because lib_version events are
/// appended from inside [EventStore.open] BEFORE the [EventStore]
/// instance exists, so we cannot reach the registry through it. The
/// hardcoded `entryTypeVersion: 1` here is the one documented exception
/// to the substrate-stamps-registeredVersion-from-the-registry rule
/// (see EVS-DEV-append-stamps-registered-version). If
/// `kLibVersionInitializedEntryType` / `kLibVersionChangedEntryType`
/// ever bump their registeredVersion in [kSystemEntryTypes], this
/// constant must move in lockstep.
```

- [ ] **Step 4: Verify the package builds and tests pass**

```bash
flutter analyze lib 2>&1 | tail -5
flutter test 2>&1 | tail -10
```

Expected: clean; all tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/event_store.dart
git commit -m "[CUR-1317] Wire ingest-audit raw-path callers to registry-derived version

logRejectedBatch and _emitDuplicateReceivedInTxn now read
entryTypes.byId(kIngestAuditEntryType)!.registeredVersion instead of
hardcoding 1. _appendLibVersionEventToBackend keeps its hardcoded 1 as
the documented timing carve-out (runs from inside EventStore.open before
the EventStore instance exists; no registry access)."
```

---

### Task 8: Promotion-aware `ProjectionInterpreter`

`ProjectionInterpreter` gains access to `PromoterRegistry` and `EntryTypeRegistry`. When `applyEvent` encounters an event whose `entryTypeVersion` is below the current `registeredVersion`, it builds a per-spec promoted copy via `PromoterExecutor` and feeds the promoted event to the fold. The original event in the log is untouched (hash-chain integrity preserved). Existing call sites (`EventStore.append`, `_ingestOneInTxn`) keep their existing one-line `_interpreter.applyEvent(...)` invocation; the promotion is transparent.

**Files:**

- Modify: `lib/src/projections/interpreter/projection_interpreter.dart`
- Modify: `lib/src/event_store.dart` (constructor wiring of `_interpreter`)
- Create: `test/projections/interpreter_promotion_test.dart`

- [ ] **Step 1: Write the failing test**

Create `test/projections/interpreter_promotion_test.dart`:

```dart
// Verifies: EVS-DEV-ingest-promotes-before-fold — ProjectionInterpreter
// applies the promoter chain on a per-spec basis when an event's
// entryTypeVersion is below the current registeredVersion. The original
// event in the log is untouched.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kNoteEntryType = 'note';

Future<SembastBackend> _openBackend() async {
  final db =
      await databaseFactoryMemory.openDatabase('test_${_dbCounter++}.db');
  return SembastBackend(db: db);
}

StoredEvent _event({
  required int seq,
  required Map<String, Object?> data,
  int entryTypeVersion = 1,
}) {
  return StoredEvent(
    key: 'k$seq',
    eventId: 'e$seq',
    aggregateId: 'agg-1',
    aggregateType: 'note',
    entryType: _kNoteEntryType,
    entryTypeVersion: entryTypeVersion,
    libFormatVersion: 1,
    eventType: 'finalized',
    sequenceNumber: seq,
    data: data,
    metadata: <String, Object?>{'provenance': <Map<String, Object?>>[]},
    initiator: <String, Object?>{'kind': 'system'},
    clientTimestamp: DateTime.utc(2026, 1, 1),
    eventHash: 'h$seq',
    flowToken: null,
    previousEventHash: null,
  );
}

void main() {
  group('ProjectionInterpreter promotion', () {
    test('event at registered version is folded raw (no promotion)',
        () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      entryTypes.register(const EntryTypeDefinition(
        id: _kNoteEntryType,
        registeredVersion: 1,
        name: 'Note',
      ));

      final projections = ProjectionRegistry()
        ..register(const AggregateProjectionSpec(
          viewName: 'notes',
          interest: SubscriptionFilter(entryTypes: <String>[_kNoteEntryType]),
          tombstoneEventTypes: <String>{},
        ));
      final promoters = PromoterRegistry();
      final interpreter = ProjectionInterpreter(
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
      );

      await backend.transaction((txn) async {
        await interpreter.applyEvent(
          txn: txn,
          backend: backend,
          event: _event(seq: 1, data: const {'body': 'hello'}),
        );
      });

      await backend.transaction((txn) async {
        final row =
            await backend.readViewRowInTxn(txn, 'notes', 'agg-1');
        expect(row!['body'], 'hello');
      });
    });

    test('event below registered version is folded after per-view promotion',
        () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      entryTypes.register(const EntryTypeDefinition(
        id: _kNoteEntryType,
        registeredVersion: 2,
        name: 'Note',
      ));

      final projections = ProjectionRegistry()
        ..register(const AggregateProjectionSpec(
          viewName: 'notes',
          interest: SubscriptionFilter(entryTypes: <String>[_kNoteEntryType]),
          tombstoneEventTypes: <String>{},
        ));
      final promoters = PromoterRegistry()
        ..register(const PromoterSpec(
          viewName: 'notes',
          entryType: _kNoteEntryType,
          fromVersion: 1,
          toVersion: 2,
          transforms: <TransformPrimitive>[
            RenameField(from: 'body', to: 'note_body'),
          ],
        ));
      final interpreter = ProjectionInterpreter(
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
      );

      await backend.transaction((txn) async {
        await interpreter.applyEvent(
          txn: txn,
          backend: backend,
          event: _event(
            seq: 1,
            data: const {'body': 'hello'},
            entryTypeVersion: 1,
          ),
        );
      });

      final row = await backend.readViewRow('notes', 'agg-1');
      expect(row!['note_body'], 'hello',
          reason: 'the v1 event was promoted to v2 (body -> note_body) for '
              'the notes view before folding.');
      expect(row.containsKey('body'), isFalse,
          reason: 'the old name was renamed away by the promoter chain.');
    });

    test('promotion is per-spec; two specs can apply different chains '
        'to the same event', () async {
      // viewA renames body -> body_a; viewB drops body. Both match the
      // same entry type. After folding the same v1 event, viewA's row has
      // body_a; viewB's row has neither body nor body_a.
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      entryTypes.register(const EntryTypeDefinition(
        id: _kNoteEntryType,
        registeredVersion: 2,
        name: 'Note',
      ));

      final projections = ProjectionRegistry()
        ..register(const AggregateProjectionSpec(
          viewName: 'view_a',
          interest: SubscriptionFilter(entryTypes: <String>[_kNoteEntryType]),
          tombstoneEventTypes: <String>{},
        ))
        ..register(const AggregateProjectionSpec(
          viewName: 'view_b',
          interest: SubscriptionFilter(entryTypes: <String>[_kNoteEntryType]),
          tombstoneEventTypes: <String>{},
        ));
      final promoters = PromoterRegistry()
        ..register(const PromoterSpec(
          viewName: 'view_a',
          entryType: _kNoteEntryType,
          fromVersion: 1,
          toVersion: 2,
          transforms: <TransformPrimitive>[
            RenameField(from: 'body', to: 'body_a'),
          ],
        ))
        ..register(const PromoterSpec(
          viewName: 'view_b',
          entryType: _kNoteEntryType,
          fromVersion: 1,
          toVersion: 2,
          transforms: <TransformPrimitive>[
            DropField(fieldName: 'body'),
          ],
        ));
      final interpreter = ProjectionInterpreter(
        projections: projections,
        promoters: promoters,
        entryTypes: entryTypes,
      );

      await backend.transaction((txn) async {
        await interpreter.applyEvent(
          txn: txn,
          backend: backend,
          event: _event(
            seq: 1,
            data: const {'body': 'hello'},
            entryTypeVersion: 1,
          ),
        );
      });

      final rowA = await backend.readViewRow('view_a', 'agg-1');
      expect(rowA!['body_a'], 'hello');
      expect(rowA.containsKey('body'), isFalse);

      final rowB = await backend.readViewRow('view_b', 'agg-1');
      expect(rowB!.containsKey('body'), isFalse);
      expect(rowB.containsKey('body_a'), isFalse);
    });
  });
}
```

- [ ] **Step 2: Run the test; verify the compile error (constructor signature mismatch)**

```bash
flutter test test/projections/interpreter_promotion_test.dart 2>&1 | tail -15
```

Expected: compile error — `ProjectionInterpreter` constructor signature doesn't accept named parameters yet.

- [ ] **Step 3: Update `ProjectionInterpreter`**

Replace the contents of `lib/src/projections/interpreter/projection_interpreter.dart` with:

```dart
// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class ProjectionInterpreter {
  final ProjectionRegistry projections;
  final PromoterRegistry promoters;
  final EntryTypeRegistry entryTypes;

  /// Back-compat alias for callers reading `interpreter.registry`. Equal to
  /// [projections]. New code should use [projections] directly.
  ProjectionRegistry get registry => projections;

  ProjectionInterpreter({
    required this.projections,
    required this.promoters,
    required this.entryTypes,
  });

  /// Apply [event] to all matching projection specs inside [txn]. When
  /// [event.entryTypeVersion] is below the entry type's current
  /// `registeredVersion`, the substrate applies the promoter chain for
  /// each matching view in-memory before folding. The original [event]
  /// is not modified.
  // Implements: EVS-DEV-ingest-promotes-before-fold — per-spec
  //   in-memory promotion of older-version events before fold.
  Future<List<AggregateFoldChange>> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    final def = entryTypes.byId(event.entryType);
    final registeredVersion = def?.registeredVersion ?? event.entryTypeVersion;

    final changes = <AggregateFoldChange>[];
    for (final spec in projections.all()) {
      if (!spec.interest.matches(event)) continue;

      StoredEvent eventForFold = event;
      if (event.entryTypeVersion < registeredVersion) {
        final promotedData = PromoterExecutor.promote(
          registry: promoters,
          viewName: spec.viewName,
          entryType: event.entryType,
          fromVersion: event.entryTypeVersion,
          toVersion: registeredVersion,
          payload: event.data,
          firstEventTimestamp: event.clientTimestamp,
        );
        eventForFold = event.withData(promotedData);
      }

      AggregateFoldChange? change;
      switch (spec) {
        case AggregateProjectionSpec():
          change = await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: eventForFold,
          );
        case TableProjectionSpec():
          change = await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: eventForFold,
          );
      }
      if (change != null) changes.add(change);
    }
    return changes;
  }
}
```

Note the `def == null` fallback: lib_version events bypass the registry (timing carve-out documented elsewhere) and may reach the interpreter via boot-time replays. If the entry type isn't in the registry, treat the event's own version as authoritative for that event (no promotion).

- [ ] **Step 4: Update `EventStore` to construct the interpreter with the registries**

In `lib/src/event_store.dart`, locate the constructor body where `_interpreter` is initialized (around line 113). Change from:

```dart
       _interpreter = ProjectionInterpreter(
         projections ?? ProjectionRegistry(),
       ),
```

to:

```dart
       _interpreter = ProjectionInterpreter(
         projections: projections ?? ProjectionRegistry(),
         promoters: promoters ?? PromoterRegistry(),
         entryTypes: entryTypes,
       ),
```

The interpreter now has access to all three registries. The existing `projections` getter on `EventStore` continues to work (still delegates to `_interpreter.registry`, which is the back-compat alias).

- [ ] **Step 5: Run the test, verify it passes**

```bash
flutter test test/projections/interpreter_promotion_test.dart 2>&1 | tail -20
```

Expected: all three tests pass.

- [ ] **Step 6: Run the full test suite to confirm no regression**

```bash
flutter test 2>&1 | tail -10
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add lib/src/projections/interpreter/projection_interpreter.dart lib/src/event_store.dart test/projections/interpreter_promotion_test.dart
git commit -m "[CUR-1317] ProjectionInterpreter promotes per-spec on version mismatch

When an event's entryTypeVersion is below the registry's current
registeredVersion, the interpreter applies the per-view promoter chain
to a working copy of event.data before dispatching to the fold. The
original event in the log is untouched; hash-chain integrity preserved.
Promotion is per-spec — two views matching the same entry type can
register different promoter chains and produce different fold inputs.

Existing call sites (EventStore.append, _ingestOneInTxn) are unchanged;
the promotion is transparent. Local-append events never need promotion
under the substrate-stamps-registeredVersion invariant — the path is
exercised only by ingest (older-peer events) and by future replay paths."
```

---

### Task 9: `view_target_versions` seeding helper

First of three boot-time helpers. Ensures every registered `ProjectionSpec` has a `view_target_versions` row for every entry type matched by its `interest` filter; absent rows are seeded at the entry type's current `registeredVersion`. On a greenfield install this is the only thing that runs (snapshot promotion is a no-op). On subsequent boots without a registeredVersion bump this is also a no-op.

**Files:**

- Create: `lib/src/projections/snapshot_promotion.dart` (this task adds the seeding function; later tasks add downgrade + promotion to the same file)
- Create: `test/projections/snapshot_promotion_test.dart` (this task adds the seeding-related test group; later tasks extend)

- [ ] **Step 1: Write the failing test**

Create `test/projections/snapshot_promotion_test.dart`:

```dart
// Verifies: EVS-DEV-view-target-versions-seeding — EventStore.open seeds
// view_target_versions rows for every (projection viewName, entry type
// in the projection's interest) pair where no row exists, at the entry
// type's current registeredVersion.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/snapshot_promotion.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kNote = EntryTypeDefinition(
  id: 'note',
  registeredVersion: 3,
  name: 'Note',
);

const _kLights = EntryTypeDefinition(
  id: 'lights',
  registeredVersion: 1,
  name: 'Lights',
);

const _kNotesSpec = AggregateProjectionSpec(
  viewName: 'notes',
  interest: SubscriptionFilter(entryTypes: <String>['note']),
  tombstoneEventTypes: <String>{},
);

const _kLightsSpec = AggregateProjectionSpec(
  viewName: 'lights',
  interest: SubscriptionFilter(entryTypes: <String>['lights']),
  tombstoneEventTypes: <String>{},
);

Future<SembastBackend> _openBackend() async {
  final db =
      await databaseFactoryMemory.openDatabase('test_${_dbCounter++}.db');
  return SembastBackend(db: db);
}

EntryTypeRegistry _registry() {
  final r = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    r.register(defn);
  }
  r..register(_kNote)..register(_kLights);
  return r;
}

void main() {
  group('seedViewTargetVersions', () {
    test('greenfield install seeds rows at current registeredVersion '
        'for every interest-matched entry type', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()
        ..register(_kNotesSpec)
        ..register(_kLightsSpec);

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      expect(await backend.readViewTargetVersion('notes', 'note'), 3);
      expect(await backend.readViewTargetVersion('lights', 'lights'), 1);
    });

    test('does not overwrite existing rows', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      // Pre-seed at a lagging version (simulating a prior boot under an
      // older registry where note was at registeredVersion=1).
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
      });

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      expect(await backend.readViewTargetVersion('notes', 'note'), 1,
          reason: 'seedViewTargetVersions must not overwrite an existing '
              'row; that lagging value drives the snapshot-promotion pass.');
    });

    test('ignores entry types not matched by any projection interest',
        () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      // No projection cares about lights.
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      expect(await backend.readViewTargetVersion('notes', 'note'), 3);
      // lights has no projection; no row was seeded.
      expect(
        await backend.readViewTargetVersion('lights', 'lights'),
        isNull,
      );
    });
  });
}
```

- [ ] **Step 2: Run the test; verify it fails (no snapshot_promotion.dart yet)**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: compile failure — module `snapshot_promotion.dart` does not exist.

- [ ] **Step 3: Create the snapshot-promotion module with the seeding function**

Create `lib/src/projections/snapshot_promotion.dart`:

```dart
// Substrate boot-time helpers for entry-type version evolution.
//
// Three helpers live in this file, all invoked from EventStore.open in
// fixed order:
//   1. assertNoEntryTypeDowngrade — refuse boot if any entry type's
//      registeredVersion has decreased.
//   2. seedViewTargetVersions — ensure every (viewName, interest-matched
//      entry type) pair has a view_target_versions row; absent ones are
//      written at the current registeredVersion.
//   3. promoteViewSnapshots — for each (viewName, entryType) pair whose
//      stored view_target_versions value is below current
//      registeredVersion, apply the promoter chain to the affected
//      view rows (those whose history includes events of that entry
//      type) and update the row.
//
// See: docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Ensure every (registered projection viewName, entry type matched by
/// the projection's interest filter) pair has a `view_target_versions`
/// row. Absent rows are written at the entry type's current
/// `registeredVersion`. Existing rows are left untouched (they may lag
/// and drive the subsequent [promoteViewSnapshots] pass).
///
/// Runs inside the caller's transaction so the seeding and any
/// subsequent boot-time work commit atomically.
// Implements: EVS-DEV-view-target-versions-seeding.
Future<void> seedViewTargetVersions({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required EntryTypeRegistry entryTypes,
}) async {
  for (final spec in projections.all()) {
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final def = entryTypes.byId(entryType);
      if (def == null) continue; // not in registry; out of scope for seeding
      final existing =
          await backend.readViewTargetVersionInTxn(txn, spec.viewName, entryType);
      if (existing != null) continue;
      await backend.writeViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
        def.registeredVersion,
      );
    }
  }
}

/// Set of entry-type ids the filter matches by name. Returns empty
/// when the filter is "match any entry type" — in that case the seed
/// pass falls back to iterating every registered entry type, which is
/// handled by the caller passing an explicit list.
List<String> _interestEntryTypes(SubscriptionFilter interest) {
  if (interest.entryTypes.isEmpty) return const <String>[];
  return interest.entryTypes;
}
```

If `SubscriptionFilter.entryTypes` is not a list of strings, adjust the accessor name (check the existing type). The helper does what its name implies — extracts the concrete entry-type set the filter cares about.

If `StorageBackend` does not currently have `readViewTargetVersionInTxn` or `readViewTargetVersion`, check the existing API for the equivalent and adjust the call. The `view_target_versions` store is already wired (used by `rebuildView`); the read accessor exists.

- [ ] **Step 4: Verify the test now passes**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: the three seeding tests pass.

- [ ] **Step 5: Commit**

```bash
git add lib/src/projections/snapshot_promotion.dart test/projections/snapshot_promotion_test.dart
git commit -m "[CUR-1317] Add seedViewTargetVersions boot-time helper

Ensures every (viewName, entry type in the projection's interest) pair
has a view_target_versions row by the time EventStore.open returns;
absent rows are seeded at the entry type's current registeredVersion.
Existing rows are left untouched (they may lag and drive the
snapshot-promotion pass landing in a later task).

Per the spec, this is the second of three boot-time helpers; the
assertNoEntryTypeDowngrade check runs first, the snapshot-promotion
pass runs third."
```

---

### Task 10: `assertNoEntryTypeDowngrade` boot-time refusal

Runs first in the boot order. Reads `view_target_versions` across all views and refuses the boot if any registered entry type's `registeredVersion` is below the highest stored value for that entry type. No `allowDowngrade` escape hatch — Phase I has no demotion specs.

**Files:**

- Modify: `lib/src/projections/snapshot_promotion.dart` (add the function)
- Modify: `lib/src/event_store.dart` (add the `EntryTypeVersionDowngradeError` class)
- Modify: `lib/event_sourcing.dart` (export the new error type)
- Modify: `test/projections/snapshot_promotion_test.dart` (add downgrade-refusal test group)

- [ ] **Step 1: Write the failing test**

Append to `test/projections/snapshot_promotion_test.dart`, inside the top-level `void main() { ... }`:

```dart
  group('assertNoEntryTypeDowngrade', () {
    test('throws EntryTypeVersionDowngradeError when registry version '
        'is below the highest stored view_target_versions value', () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(const EntryTypeDefinition(
        id: 'note',
        registeredVersion: 1, // downgrade!
        name: 'Note',
      ));
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      // Simulate: an earlier boot had note at registeredVersion=3 and
      // promoted the view to that target.
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 3);
      });

      await expectLater(
        backend.transaction((txn) async {
          await assertNoEntryTypeDowngrade(
            txn: txn,
            backend: backend,
            projections: projections,
            entryTypes: entryTypes,
          );
        }),
        throwsA(isA<EntryTypeVersionDowngradeError>()
            .having((e) => e.entryType, 'entryType', 'note')
            .having((e) => e.fromVersion, 'fromVersion', 3)
            .having((e) => e.toVersion, 'toVersion', 1)),
      );
    });

    test('no-op when every entry type registeredVersion >= stored',
        () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
      });

      // No throw; the registry's note is at 3, stored is 1 (lag, not
      // downgrade).
      await backend.transaction((txn) async {
        await assertNoEntryTypeDowngrade(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });
    });
  });
```

Add the import at the top of the test file:

```dart
import 'package:event_sourcing/src/projections/snapshot_promotion.dart' show seedViewTargetVersions, assertNoEntryTypeDowngrade;
```

(Or import the whole module; both spellings are fine.)

- [ ] **Step 2: Run the test, verify compile failure**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: compile error — `EntryTypeVersionDowngradeError` and `assertNoEntryTypeDowngrade` undefined.

- [ ] **Step 3: Add `EntryTypeVersionDowngradeError` to `event_store.dart`**

In `lib/src/event_store.dart`, near the existing `DowngradeRefusedError` class (around line 76), add a sibling class:

```dart
/// Thrown by [EventStore.open] when any registered entry type's
/// `registeredVersion` is below the highest value recorded for that
/// entry type across the local `view_target_versions` store. The lib
/// has no `DemotionSpec` mechanism in Phase I; the only resolution is
/// to pin a lib build whose registry's `registeredVersion` is at least
/// as high as the stored target. See
/// docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md.
class EntryTypeVersionDowngradeError extends Error {
  EntryTypeVersionDowngradeError({
    required this.entryType,
    required this.fromVersion,
    required this.toVersion,
  });

  final String entryType;
  final int fromVersion;
  final int toVersion;

  @override
  String toString() =>
      'EntryTypeVersionDowngradeError: entry type "$entryType" was '
      'previously folded at registeredVersion=$fromVersion (stored in '
      'view_target_versions), but the current registry has '
      'registeredVersion=$toVersion. Phase I refuses entry-type '
      'downgrade unconditionally. Pin a lib build with '
      'registeredVersion >= $fromVersion for "$entryType".';
}
```

- [ ] **Step 4: Add `assertNoEntryTypeDowngrade` to `snapshot_promotion.dart`**

In `lib/src/projections/snapshot_promotion.dart`, add an import:

```dart
import 'package:event_sourcing/src/event_store.dart'
    show EntryTypeVersionDowngradeError;
```

And add the function:

```dart
/// Refuse the boot if any registered entry type's `registeredVersion`
/// is below the highest stored value in `view_target_versions` across
/// all views. This is a Layer 1 substrate-enforced invariant — Phase I
/// has no `DemotionSpec` mechanism.
///
/// Runs FIRST in the boot order (before [seedViewTargetVersions] and
/// before [promoteViewSnapshots]), so a downgrade fails fast without
/// the substrate touching any state.
// Implements: EVS-DEV-entry-type-downgrade-refusal.
Future<void> assertNoEntryTypeDowngrade({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required EntryTypeRegistry entryTypes,
}) async {
  // For each entry type, find the maximum stored target across all
  // views that touch that entry type. Compare against the registry.
  final maxStored = <String, int>{};
  for (final spec in projections.all()) {
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final stored = await backend.readViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
      );
      if (stored == null) continue;
      final prior = maxStored[entryType];
      if (prior == null || stored > prior) {
        maxStored[entryType] = stored;
      }
    }
  }
  for (final entry in maxStored.entries) {
    final def = entryTypes.byId(entry.key);
    if (def == null) continue;
    if (def.registeredVersion < entry.value) {
      throw EntryTypeVersionDowngradeError(
        entryType: entry.key,
        fromVersion: entry.value,
        toVersion: def.registeredVersion,
      );
    }
  }
}
```

- [ ] **Step 5: Export the new error from `lib/event_sourcing.dart`**

Find the existing `DowngradeRefusedError` export (around line 157). Add `EntryTypeVersionDowngradeError` to the same `show` list, or add an adjacent export line.

```dart
export 'src/event_store.dart'
    show
        // ...existing names...
        DowngradeRefusedError,
        EntryTypeVersionDowngradeError;
```

Also update the doc comment block above the export (around line 17) — if there's a list like "`DowngradeRefusedError` — thrown by `EventStore.open` on lib downgrade," add a sibling line for the new error.

- [ ] **Step 6: Run the test, verify it passes**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: all tests pass (seeding tests from Task 9 plus the two new downgrade tests).

- [ ] **Step 7: Commit**

```bash
git add lib/src/event_store.dart lib/src/projections/snapshot_promotion.dart lib/event_sourcing.dart test/projections/snapshot_promotion_test.dart
git commit -m "[CUR-1317] Add assertNoEntryTypeDowngrade and EntryTypeVersionDowngradeError

Refuses boot when any registered entry type's registeredVersion is
below the highest value stored in view_target_versions across all
views. Phase I has no DemotionSpec mechanism, so the refusal is
unconditional — no allowDowngrade escape. The error includes
entryType, fromVersion, and toVersion so operators can see exactly
which entry type to align."
```

---

### Task 11: `promoteViewSnapshots` snapshot promotion + audit event

Third boot-time helper. For each `(viewName, entryType)` pair whose stored `view_target_versions` lags `registeredVersion`, query the event log for distinct aggregate ids with events of that entry type, apply the promoter chain to each affected row, re-run `derivedFields` for `AggregateProjectionSpec`, write back, update `view_target_versions`, and emit one `view_snapshot_promoted` audit event per (viewName, entryType) pair.

**Files:**

- Modify: `lib/src/projections/snapshot_promotion.dart` (add `promoteViewSnapshots`)
- Modify: `test/projections/snapshot_promotion_test.dart` (add snapshot-promotion test group)

- [ ] **Step 1: Write the failing test**

Append to `test/projections/snapshot_promotion_test.dart`:

```dart
  group('promoteViewSnapshots', () {
    test('lagging view rows are promoted; non-affected rows untouched',
        () async {
      // Setup: registry has note at registeredVersion=2; view_target_versions
      // has notes/note at version 1 (lag). The promoter renames body ->
      // note_body. Existing view row reflects v1 shape (body present).
      // promoteViewSnapshots should rename body -> note_body and update
      // view_target_versions to 2.
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(const EntryTypeDefinition(
        id: 'note',
        registeredVersion: 2,
        name: 'Note',
      ));
      final projections = ProjectionRegistry()..register(_kNotesSpec);
      final promoters = PromoterRegistry()
        ..register(const PromoterSpec(
          viewName: 'notes',
          entryType: 'note',
          fromVersion: 1,
          toVersion: 2,
          transforms: <TransformPrimitive>[
            RenameField(from: 'body', to: 'note_body'),
          ],
        ));

      // Seed view_target_versions at v1 + write an existing view row with v1
      // shape.
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
        await backend.upsertViewRowInTxn(txn, 'notes', 'agg-1',
            const <String, Object?>{
          'aggregateId': 'agg-1',
          'body': 'hello',
          'sequence': 1,
        });
        // Append a real event of entry type note so the
        // findAggregateIdsWithEventType (via findAllEvents entryType filter)
        // returns agg-1.
        await backend.appendEvent(txn, _event(
          seq: 1,
          data: const {'body': 'hello'},
          entryTypeVersion: 1,
        ));
      });

      await backend.transaction((txn) async {
        await promoteViewSnapshots(
          txn: txn,
          backend: backend,
          projections: projections,
          promoters: promoters,
          entryTypes: entryTypes,
          initiator: const AutomationInitiator(service: 'test-boot'),
          now: DateTime.utc(2026, 5, 11),
        );
      });

      final row = await backend.readViewRow('notes', 'agg-1');
      expect(row!['note_body'], 'hello');
      expect(row.containsKey('body'), isFalse);
      expect(await backend.readViewTargetVersion('notes', 'note'), 2);
    });

    test('rows whose history does not include the affected entry type '
        'are untouched', () async {
      final backend = await _openBackend();
      final entryTypes = _registry(); // notes at 3, lights at 1
      final projections = ProjectionRegistry()
        ..register(_kNotesSpec)
        ..register(_kLightsSpec);
      // Promoter for notes 1->3; no row in lights/lights is affected
      // because lights's stored version equals registered.
      final promoters = PromoterRegistry()
        ..register(const PromoterSpec(
          viewName: 'notes',
          entryType: 'note',
          fromVersion: 1,
          toVersion: 3,
          transforms: <TransformPrimitive>[
            RenameField(from: 'body', to: 'note_body'),
          ],
        ));

      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
        await backend.writeViewTargetVersionInTxn(txn, 'lights', 'lights', 1);
        // notes row needs promotion; lights row should NOT be touched.
        await backend.upsertViewRowInTxn(txn, 'notes', 'agg-1',
            const <String, Object?>{'body': 'note-body'});
        await backend.upsertViewRowInTxn(txn, 'lights', 'agg-2',
            const <String, Object?>{'state': 'on'});
        // Append matching note + lights events.
        await backend.appendEvent(txn, _event(
            seq: 1, data: const {'body': 'note-body'}, entryTypeVersion: 1));
      });

      await backend.transaction((txn) async {
        await promoteViewSnapshots(
          txn: txn,
          backend: backend,
          projections: projections,
          promoters: promoters,
          entryTypes: entryTypes,
          initiator: const AutomationInitiator(service: 'test-boot'),
          now: DateTime.utc(2026, 5, 11),
        );
      });

      // notes row promoted (body -> note_body)
      final notesRow = await backend.readViewRow('notes', 'agg-1');
      expect(notesRow!['note_body'], 'note-body');
      // lights row unchanged
      final lightsRow = await backend.readViewRow('lights', 'agg-2');
      expect(lightsRow!['state'], 'on');
    });

    test('emits one view_snapshot_promoted audit event per promoted pair',
        () async {
      final backend = await _openBackend();
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(const EntryTypeDefinition(
        id: 'note',
        registeredVersion: 2,
        name: 'Note',
      ));
      final projections = ProjectionRegistry()..register(_kNotesSpec);
      final promoters = PromoterRegistry()
        ..register(const PromoterSpec(
          viewName: 'notes',
          entryType: 'note',
          fromVersion: 1,
          toVersion: 2,
          transforms: <TransformPrimitive>[
            RenameField(from: 'body', to: 'note_body'),
          ],
        ));

      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
        await backend.upsertViewRowInTxn(txn, 'notes', 'agg-1',
            const <String, Object?>{'body': 'x'});
        await backend.appendEvent(txn, _event(
            seq: 1, data: const {'body': 'x'}, entryTypeVersion: 1));
      });

      await backend.transaction((txn) async {
        await promoteViewSnapshots(
          txn: txn,
          backend: backend,
          projections: projections,
          promoters: promoters,
          entryTypes: entryTypes,
          initiator: const AutomationInitiator(service: 'test-boot'),
          now: DateTime.utc(2026, 5, 11),
        );
      });

      final all = await backend.findAllEvents(
          entryType: 'view_snapshot_promoted');
      expect(all.length, 1);
      expect(all.single.data['viewName'], 'notes');
      expect(all.single.data['entryType'], 'note');
      expect(all.single.data['fromVersion'], 1);
      expect(all.single.data['toVersion'], 2);
      expect(all.single.data['rowsPromoted'], 1);
    });
  });
}
```

(Move `_event` helper from the interpreter test file into the snapshot promotion test if needed, or define a local copy. Keep it small.)

- [ ] **Step 2: Run the test, verify compile failure**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: compile failure — `promoteViewSnapshots` undefined.

- [ ] **Step 3: Implement `promoteViewSnapshots`**

In `lib/src/projections/snapshot_promotion.dart`, add imports:

```dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/rebuild.dart' show /* may need to import the StoredEvent-based fold helpers; adjust as needed */;
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
```

(Add `Uuid` if needed for event-id generation; check imports of `event_store.dart`'s raw-internal-append code path.)

Add the function:

```dart
/// For each (viewName, entryType) pair where stored view_target_versions
/// lags the entry type's current registeredVersion, promote affected view
/// rows by applying the registered promoter chain. After each pair's
/// promotion: update view_target_versions to the current registeredVersion
/// and emit one view_snapshot_promoted audit event. Audit events are
/// appended via the raw internal-append path (substrate-internal, like
/// security_context_redacted).
///
/// Runs inside the caller's transaction so the promotion and the audit
/// commit atomically.
// Implements: EVS-DEV-snapshot-promotion-on-open.
Future<void> promoteViewSnapshots({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required Initiator initiator,
  required DateTime now,
}) async {
  for (final spec in projections.all()) {
    for (final entryType in _interestEntryTypes(spec.interest)) {
      final def = entryTypes.byId(entryType);
      if (def == null) continue;
      final stored = await backend.readViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
      );
      if (stored == null) continue; // not yet seeded; out of scope
      if (stored >= def.registeredVersion) continue; // up to date

      final chain = promoters.chain(
        viewName: spec.viewName,
        entryType: entryType,
        fromVersion: stored,
        toVersion: def.registeredVersion,
      );
      // Find affected aggregate ids — those whose history includes any
      // event of this entry type. Use the extended findAllEvents filter
      // from Task 3.
      final events = await backend.findAllEventsInTxn(
        txn,
        entryType: entryType,
      );
      final affectedAggregateIds = <String>{
        for (final e in events) e.aggregateId,
      };

      var rowsPromoted = 0;
      for (final aggregateId in affectedAggregateIds) {
        final row =
            await backend.readViewRowInTxn(txn, spec.viewName, aggregateId);
        if (row == null) continue; // tombstoned or never present
        final firstEventTimestamp =
            (row['firstEventTimestamp'] as String?) != null
                ? DateTime.parse(row['firstEventTimestamp'] as String)
                : now;
        // Apply transform chain to the row.
        var promotedRow = Map<String, Object?>.from(row);
        for (final pspec in chain) {
          promotedRow = _applyChainStep(pspec, promotedRow, firstEventTimestamp);
        }
        if (spec is AggregateProjectionSpec) {
          // Re-run derivedFields over the promoted row.
          for (final df in spec.derivedFields) {
            promotedRow[df.fieldName] = df.computation.resolve(
              rowState: promotedRow,
              firstEventTimestamp: firstEventTimestamp,
            );
          }
        }
        await backend.upsertViewRowInTxn(
          txn,
          spec.viewName,
          aggregateId,
          Map<String, Object?>.unmodifiable(promotedRow),
        );
        rowsPromoted++;
      }

      // Update view_target_versions to the new registeredVersion.
      await backend.writeViewTargetVersionInTxn(
        txn,
        spec.viewName,
        entryType,
        def.registeredVersion,
      );

      // Emit the audit event via the substrate-internal raw-append path.
      await _emitViewSnapshotPromotedAuditInTxn(
        txn: txn,
        backend: backend,
        entryTypes: entryTypes,
        initiator: initiator,
        now: now,
        viewName: spec.viewName,
        entryType: entryType,
        fromVersion: stored,
        toVersion: def.registeredVersion,
        rowsPromoted: rowsPromoted,
      );
    }
  }
}

Map<String, Object?> _applyChainStep(
  PromoterSpec pspec,
  Map<String, Object?> input,
  DateTime firstEventTimestamp,
) {
  // Equivalent to TransformChain.applyAll but inline so we can stamp the
  // firstEventTimestamp for each transform.
  var current = input;
  for (final t in pspec.transforms) {
    current = t.apply(current, firstEventTimestamp: firstEventTimestamp);
  }
  return current;
}

Future<void> _emitViewSnapshotPromotedAuditInTxn({
  required Txn txn,
  required StorageBackend backend,
  required EntryTypeRegistry entryTypes,
  required Initiator initiator,
  required DateTime now,
  required String viewName,
  required String entryType,
  required int fromVersion,
  required int toVersion,
  required int rowsPromoted,
}) async {
  // Use EventStore.appendInTxn? No — boot-time helpers must not depend
  // on an EventStore instance. The audit event is written via the same
  // raw-internal-append helper used for ingest-audit; expose the helper
  // for this use (see also _appendRawInternalEventInTxn in event_store.dart).
  //
  // Implementation note for the engineer: the cleanest plumbing is to
  // call EventStore.appendInTxn from EventStore.open AFTER the boot
  // helpers' read pass, then have the boot helpers RETURN a list of
  // pending-audit records that EventStore.open emits. That keeps the
  // helpers pure-functional (no append from inside a helper).
  //
  // For now: shape this helper to receive a callback or a list to
  // populate, OR pass the raw-append function in. Pick one when wiring
  // EventStore.open in Task 12.
  throw UnimplementedError(
      'audit emission wired in Task 12 (EventStore.open integration)');
}
```

NOTE: the audit-emission detail involves a cross-module concern — the helpers cannot reach `EventStore.appendInTxn` because they are called from inside `EventStore.open` before the instance exists. The cleanest plumbing is to have `promoteViewSnapshots` return a list of `PendingPromotionAudit` records and have the caller (`EventStore.open` in Task 12) emit them via the raw append path with access to the same `_appendRawInternalEventInTxn` already used by `_appendLibVersionEventToBackend`. Restructure `promoteViewSnapshots` to return `Future<List<PendingPromotionAudit>>` where `PendingPromotionAudit` is a value class with `(viewName, entryType, fromVersion, toVersion, rowsPromoted)` fields. Then Task 12's integration step calls this helper, gets the list, and emits the audit events post-helper.

Refactor the function accordingly. The third test in this task (audit-event emission) is asserting against the END state after Task 12 wires the audit emission — so for THIS task, either:

- Move that test into Task 12, OR
- Have the helper accept an `audit` callback parameter and exercise it via a test fake.

Cleanest is the latter; restructure the helper:

```dart
typedef AuditEmitter = Future<void> Function({
  required String viewName,
  required String entryType,
  required int fromVersion,
  required int toVersion,
  required int rowsPromoted,
});

Future<void> promoteViewSnapshots({
  required Txn txn,
  required StorageBackend backend,
  required ProjectionRegistry projections,
  required PromoterRegistry promoters,
  required EntryTypeRegistry entryTypes,
  required AuditEmitter emitAudit,
  required DateTime now,
}) async {
  // ... same body, but where _emitViewSnapshotPromotedAuditInTxn was
  // called, call:
  await emitAudit(
    viewName: spec.viewName,
    entryType: entryType,
    fromVersion: stored,
    toVersion: def.registeredVersion,
    rowsPromoted: rowsPromoted,
  );
}
```

Update the third test to construct a recording fake `AuditEmitter`:

```dart
test('emits one audit event per promoted pair (via emitAudit callback)',
    () async {
  final calls = <Map<String, Object?>>[];
  Future<void> recordingEmit({
    required String viewName,
    required String entryType,
    required int fromVersion,
    required int toVersion,
    required int rowsPromoted,
  }) async {
    calls.add({
      'viewName': viewName,
      'entryType': entryType,
      'fromVersion': fromVersion,
      'toVersion': toVersion,
      'rowsPromoted': rowsPromoted,
    });
  }

  // ... setup as before ...
  await backend.transaction((txn) async {
    await promoteViewSnapshots(
      // ...
      emitAudit: recordingEmit,
      now: DateTime.utc(2026, 5, 11),
    );
  });

  expect(calls, hasLength(1));
  expect(calls.single['viewName'], 'notes');
  expect(calls.single['entryType'], 'note');
  expect(calls.single['fromVersion'], 1);
  expect(calls.single['toVersion'], 2);
  expect(calls.single['rowsPromoted'], 1);
});
```

The first two tests in this task also need the `emitAudit` parameter — supply a no-op `(args...) async {}` callback.

- [ ] **Step 4: Run the test, verify it passes**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -10
```

Expected: all tests pass (seeding, downgrade, snapshot promotion x3).

- [ ] **Step 5: Commit**

```bash
git add lib/src/projections/snapshot_promotion.dart test/projections/snapshot_promotion_test.dart
git commit -m "[CUR-1317] Add promoteViewSnapshots boot-time helper

For each (viewName, entryType) pair where stored view_target_versions
lags the entry type's current registeredVersion, find affected aggregate
ids via findAllEvents(entryType:), apply the promoter chain to each
affected row, re-run AggregateProjectionSpec.derivedFields, write back,
and update view_target_versions. Calls an emitAudit callback once per
promoted pair; the audit emission is plumbed in Task 12 because boot
helpers run before EventStore exists."
```

---

### Task 12: Wire seeding + downgrade + promotion into `EventStore.open`

The three boot helpers run in fixed order inside `EventStore.open`, each inside its own backend transaction. The audit emission for `promoteViewSnapshots` is supplied as a callback that uses the same raw-append helper already used by `_appendLibVersionEventToBackend`. Order: lib-version check (existing) → downgrade refusal → seeding → snapshot promotion.

**Files:**

- Modify: `lib/src/event_store.dart`
- Modify: `test/projections/snapshot_promotion_test.dart` (add an end-to-end integration test exercising `EventStore.open`)

- [ ] **Step 1: Write the failing integration test**

Append to `test/projections/snapshot_promotion_test.dart`:

```dart
  group('EventStore.open integration', () {
    test('boot with a bumped registeredVersion: seeds, refuses no '
        'downgrade, snapshot-promotes, emits audit event', () async {
      // First boot at v1, append a v1 note, simulate a registry bump to v2,
      // re-open. After re-open: row is promoted, view_target_versions[notes/note]
      // = 2, and a view_snapshot_promoted audit event exists in the log.
      final db = await databaseFactoryMemory.openDatabase(
          'integration_${_dbCounter++}.db');
      final backend = SembastBackend(db: db);

      // First boot: register note at v1.
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(const EntryTypeDefinition(
          id: 'note', registeredVersion: 1, name: 'Note'));
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final store = await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
              hopId: 'test', identifier: 'test-i', softwareVersion: '0.0.0'),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        );
        await store.append(
          entryType: 'note',
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'body': 'hello'},
          initiator: const AutomationInitiator(service: 'test'),
        );
        await store.close();
      }

      // Second boot: register note at v2 plus a v1->v2 promoter (rename
      // body -> note_body).
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(const EntryTypeDefinition(
          id: 'note', registeredVersion: 2, name: 'Note'));
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final promoters = PromoterRegistry()
          ..register(const PromoterSpec(
            viewName: 'notes',
            entryType: 'note',
            fromVersion: 1,
            toVersion: 2,
            transforms: <TransformPrimitive>[
              RenameField(from: 'body', to: 'note_body'),
            ],
          ));
        final store = await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
              hopId: 'test', identifier: 'test-i', softwareVersion: '0.0.0'),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
          promoters: promoters,
        );

        // View row was promoted.
        final row = await backend.readViewRow('notes', 'agg-1');
        expect(row!['note_body'], 'hello');
        expect(row.containsKey('body'), isFalse);

        // view_target_versions now at 2.
        expect(await backend.readViewTargetVersion('notes', 'note'), 2);

        // Audit event was emitted.
        final audits = await backend.findAllEvents(
            entryType: 'view_snapshot_promoted');
        expect(audits, hasLength(1));
        expect(audits.single.data['viewName'], 'notes');
        expect(audits.single.data['fromVersion'], 1);
        expect(audits.single.data['toVersion'], 2);
        expect(audits.single.data['rowsPromoted'], 1);

        await store.close();
      }
    });

    test('boot refuses entry-type downgrade', () async {
      final db = await databaseFactoryMemory.openDatabase(
          'downgrade_${_dbCounter++}.db');
      final backend = SembastBackend(db: db);

      // First boot at v2, append an event, close.
      {
        final entryTypes = EntryTypeRegistry();
        for (final defn in kSystemEntryTypes) {
          entryTypes.register(defn);
        }
        entryTypes.register(const EntryTypeDefinition(
          id: 'note', registeredVersion: 2, name: 'Note'));
        final projections = ProjectionRegistry()..register(_kNotesSpec);
        final store = await EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
              hopId: 'test', identifier: 'test-i', softwareVersion: '0.0.0'),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        );
        await store.append(
          entryType: 'note',
          aggregateId: 'agg-1',
          aggregateType: 'note',
          eventType: 'finalized',
          data: const <String, Object?>{'body': 'x'},
          initiator: const AutomationInitiator(service: 'test'),
        );
        await store.close();
      }

      // Second boot: downgrade to v1.
      final entryTypes = EntryTypeRegistry();
      for (final defn in kSystemEntryTypes) {
        entryTypes.register(defn);
      }
      entryTypes.register(const EntryTypeDefinition(
        id: 'note', registeredVersion: 1, name: 'Note'));
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      await expectLater(
        EventStore.open(
          storage: backend,
          entryTypes: entryTypes,
          source: const Source(
              hopId: 'test', identifier: 'test-i', softwareVersion: '0.0.0'),
          securityContexts: SembastSecurityContextStore(backend: backend),
          projections: projections,
        ),
        throwsA(isA<EntryTypeVersionDowngradeError>()
            .having((e) => e.entryType, 'entryType', 'note')
            .having((e) => e.fromVersion, 'fromVersion', 2)
            .having((e) => e.toVersion, 'toVersion', 1)),
      );
    });
  });
```

- [ ] **Step 2: Run the test, verify it fails**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -20
```

Expected: failure — the helpers are defined but not wired into `EventStore.open`.

- [ ] **Step 3: Wire the helpers into `EventStore.open`**

In `lib/src/event_store.dart`, locate `EventStore.open` (around line 157). After `await _runBootVersionCheck(storage, allowDowngrade: allowDowngrade);` and after `effectiveProjections.seal()` / `effectivePromoters.seal()`, add a new boot-time pass:

```dart
    await storage.transaction((txn) async {
      await assertNoEntryTypeDowngrade(
        txn: txn,
        backend: storage,
        projections: effectiveProjections,
        entryTypes: entryTypes,
      );
      await seedViewTargetVersions(
        txn: txn,
        backend: storage,
        projections: effectiveProjections,
        entryTypes: entryTypes,
      );
      await promoteViewSnapshots(
        txn: txn,
        backend: storage,
        projections: effectiveProjections,
        promoters: effectivePromoters,
        entryTypes: entryTypes,
        now: DateTime.now().toUtc(),
        emitAudit: ({
          required String viewName,
          required String entryType,
          required int fromVersion,
          required int toVersion,
          required int rowsPromoted,
        }) async {
          await _appendViewSnapshotPromotedAuditInTxn(
            txn,
            storage,
            entryTypes,
            viewName: viewName,
            entryType: entryType,
            fromVersion: fromVersion,
            toVersion: toVersion,
            rowsPromoted: rowsPromoted,
          );
        },
      );
    });
```

Add the imports at the top of `event_store.dart`:

```dart
import 'package:event_sourcing/src/projections/snapshot_promotion.dart';
```

Add the file-level helper for the audit append at the bottom of the file (sibling to `_appendLibVersionEventToBackend`):

```dart
/// Append a substrate-emitted view_snapshot_promoted event inside [txn].
///
/// Called by [promoteViewSnapshots] (via the emitAudit callback) after a
/// (viewName, entryType) pair has been lifted to the new registered
/// version. Bypasses [EventStore.appendInTxn] because boot-time helpers
/// run before the [EventStore] instance exists; uses the same raw-internal-
/// append helper as [logRejectedBatch] and [_emitDuplicateReceivedInTxn].
Future<void> _appendViewSnapshotPromotedAuditInTxn(
  Txn txn,
  StorageBackend backend,
  EntryTypeRegistry entryTypes, {
  required String viewName,
  required String entryType,
  required int fromVersion,
  required int toVersion,
  required int rowsPromoted,
}) async {
  const uuid = Uuid();
  final now = DateTime.now().toUtc();
  final localSeq = await backend.nextSequenceNumber(txn);
  final previousTailHash = await backend.readLatestEventHash(txn);
  final provenance0 = ProvenanceEntry(
    hop: 'event_sourcing',
    receivedAt: now,
    identifier: 'event_sourcing',
    softwareVersion: LibVersion.current,
  );
  await _appendRawInternalEventInTxn(
    txn,
    backend,
    aggregateId: '_lib',
    aggregateType: '_lib',
    entryType: kViewSnapshotPromotedEntryType,
    entryTypeVersion: entryTypes
        .byId(kViewSnapshotPromotedEntryType)!
        .registeredVersion,
    eventType: 'finalized',
    data: <String, Object?>{
      'viewName': viewName,
      'entryType': entryType,
      'fromVersion': fromVersion,
      'toVersion': toVersion,
      'rowsPromoted': rowsPromoted,
    },
    initiator: const AutomationInitiator(service: 'event_sourcing'),
    provenance0: provenance0,
    localSeq: localSeq,
    previousTailHash: previousTailHash,
    uuid: uuid,
  );
}
```

If `openForTest` exists and is intended to bypass the boot-time pass (so unit tests don't pay the cost), wrap the new boot-time pass in a conditional based on a flag, or leave it always running and adjust tests as needed. Recommend always running it — its no-op behavior on greenfield is cheap.

- [ ] **Step 4: Run the test, verify it passes**

```bash
flutter test test/projections/snapshot_promotion_test.dart 2>&1 | tail -20
```

Expected: all tests in the file pass, including the new integration scenarios.

- [ ] **Step 5: Run the full test suite to catch regressions**

```bash
flutter test 2>&1 | tail -20
flutter analyze lib test 2>&1 | grep -E "(error|warning)" | head -5
```

Expected: all green; analyzer surface unchanged (info-level lints only).

- [ ] **Step 6: Run both example apps' test suites**

```bash
cd example && flutter test 2>&1 | tail -10; cd ..
cd example_action_permissions && flutter test 2>&1 | tail -10; cd ..
```

Expected: both pass (77 and 134 tests respectively, per the roadmap baseline).

- [ ] **Step 7: Commit**

```bash
git add lib/src/event_store.dart test/projections/snapshot_promotion_test.dart
git commit -m "[CUR-1317] Wire snapshot-promotion boot pass into EventStore.open

EventStore.open now runs three additional boot-time helpers in fixed
order after the existing lib-version check:

  1. assertNoEntryTypeDowngrade — throws EntryTypeVersionDowngradeError
     if any registered entry type's registeredVersion is below the
     highest stored view_target_versions value.
  2. seedViewTargetVersions — seeds missing view_target_versions rows
     at the entry type's current registeredVersion.
  3. promoteViewSnapshots — lifts lagging view rows via the registered
     promoter chains; emits one view_snapshot_promoted audit event per
     promoted (viewName, entryType) pair via the same raw-internal-append
     path used by lib_version events.

All three run inside a single backend transaction so a mid-pass crash
rolls back and retries on the next boot. Existing 984-test surface
unaffected; example apps green."
```

---

### Task 13: Final regression sweep

Run the full test surface (lib + both example apps) and verify charter-assertion alignment (CLAUDE.md remains accurate, no doc references to removed concepts).

**Files:**

- None (verification + targeted comment updates only)

- [ ] **Step 1: Full test suite**

```bash
flutter test 2>&1 | tail -10
```

Expected: 773+ tests passing, 0 failing (the lib gained several new tests across this plan; original count was 773).

- [ ] **Step 2: Example apps**

```bash
cd example && flutter test 2>&1 | tail -5; cd ..
cd example_action_permissions && flutter test 2>&1 | tail -5; cd ..
```

Expected: 77 and 134 tests respectively (unchanged baselines).

- [ ] **Step 3: Analyzer surface**

```bash
flutter analyze lib test 2>&1 | tail -3
cd example && flutter analyze lib test 2>&1 | tail -3; cd ..
cd example_action_permissions && flutter analyze lib test 2>&1 | tail -3; cd ..
```

Expected: each invocation reports info-level lints only; no new errors or warnings introduced by this work.

- [ ] **Step 4: Doc-comment sweep on touched files**

For each file modified by this plan, scan its doc comments for stale references:

- `REQ-d00141-B` ("entryTypeVersion required") — the required-parameter clause is reversed by this work. Comment block(s) around it should be updated to read "substrate stamps registeredVersion from the registry."
- `REQ-d00141-F` ("append does NOT validate entryTypeVersion") — superseded; the substrate now stamps unconditionally and `_validateAppendInputs` already rejects unregistered entry types.
- `Task 22` / `Task 23` / similar in-line task references — update to point at this plan's roadmap entry, or drop if the historical reference no longer adds context.

These are touch-as-you-go; do not author a sweeping rebinding pass — only edit comments on files this work touches.

- [ ] **Step 5: CLAUDE.md alignment check**

Open `CLAUDE.md`. Verify the Architectural Commitments section is consistent with this work. In particular:

- The "Domain-neutral lib" + "Declarative projections" commitments are unchanged. ✓
- The "Library version recorded in the log" commitment still describes lib-version events. ✓
- New: there is now a Layer 1 invariant about `entryTypeVersion` being substrate-stamped — consider adding a one-paragraph note to Architectural Commitments after "Library version recorded in the log" along the lines of:

> **Entry-type version is substrate-owned.** The substrate stamps `entryTypeVersion = entryTypes.byId(entryType).registeredVersion` on every appended event. Producers do not choose the version; ingest transparently promotes older-peer events before the fold; `EventStore.open` snapshot-promotes view rows on a `registeredVersion` bump and refuses downgrade. The registered version IS the version a projection folds at — there is no separate target. See `docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md` for the full design.

If you add this paragraph, do it in this task and commit it as a CLAUDE.md update.

- [ ] **Step 6: Commit (optional CLAUDE.md sweep)**

```bash
git add CLAUDE.md
git commit -m "[CUR-1317] CLAUDE.md: pin entry-type version as substrate-owned commitment

Adds one paragraph to Architectural Commitments capturing the Layer 1
invariant introduced by docs/superpowers/specs/2026-05-11-entry-type-version-substrate-owned-design.md:
the substrate stamps entryTypeVersion = registeredVersion on every
append; ingest promotes transparently; EventStore.open auto-promotes
lagging view rows on bump and refuses downgrade."
```

- [ ] **Step 7: Roadmap update**

In `docs/superpowers/specs/2026-05-11-roadmap.md`, locate the "Open design questions deferred from per-task reviews" section. Mark Q1 and Q2 as resolved (or move them to a "Resolved" subsection at the bottom of that section) referencing the design doc and this plan's PR. Q3 (`view_target_versions` purpose) can be likewise marked — this work confirms it's load-bearing.

Commit:

```bash
git add docs/superpowers/specs/2026-05-11-roadmap.md
git commit -m "[CUR-1317] Roadmap: mark Q1/Q2/Q3 resolved by entry-type-version-substrate-owned design"
```

---

## Self-review checklist (run before declaring complete)

After all tasks land:

- [ ] Every spec section maps to at least one task. ✓ (decisions table → Tasks 1, 2, 3, 5, 7, 8, 12; invariants → Tasks 5, 8, 10, 12; design → Tasks 4, 8, 9, 10, 11, 12; test plan → tests in Tasks 1, 3, 5, 8, 9, 10, 11, 12; out of scope → genuinely deferred)
- [ ] No `TBD` / `TODO` / placeholder language in the plan.
- [ ] Code shown in steps compiles against the spec's named types (`EntryTypeRegistry`, `PromoterRegistry`, `PromoterSpec`, `TransformPrimitive` subclasses, `ProjectionInterpreter`, `StorageBackend`).
- [ ] Method signatures consistent across tasks (`promoteViewSnapshots` parameter list defined in Task 11 matches the call site wired in Task 12).
- [ ] Test patterns match the established convention (`flutter_test`, `sembast_memory`, group/test/expect; `_dbCounter` pattern for parallel test isolation).
