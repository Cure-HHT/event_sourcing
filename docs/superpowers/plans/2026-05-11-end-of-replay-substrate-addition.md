# EndOfReplay<T> Substrate Addition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a 4th `EndOfReplay<T>` variant to the substrate's `Update<T>` sealed class, surfaced by `EventStore.subscribe<T>` between the snapshot replay phase and live delta delivery, so consumers can deterministically detect "snapshot complete; everything from here is live."

**Architecture:** Single new sealed-class variant in `event_sourcing/lib/src/subscriptions/update.dart`. Emission point is `EventStore._subscribeAggregate` in `event_sourcing/lib/src/event_store.dart`, after the buffered-change drain and before flipping the `replayDone` flag. `EndOfReplay.sequence` is the maximum sequence emitted so far in the stream (snapshot rows + drained buffered changes), or `0` if neither (empty view, no concurrent appends during snapshot read). For `Events()`-mode subscriptions, no `EndOfReplay` is emitted (no replay phase exists).

**Tech Stack:** Dart 3.x (sealed classes, exhaustive switch). Test framework: `package:test`. No new dependencies.

**Spec reference:** `spec/prd-reaction.md` § `EVS-PRD-cross-process-event-transport` Assertion A and § `EVS-PRD-view-subscriber` (the wire transport requires the four `Update<T>` variants including `EndOfReplay`); brainstorm context in `docs/superpowers/specs/2026-05-11-reaction-design.md` §"Substrate additions".

**Scope check:** This plan covers ONLY the substrate addition. It does NOT touch `reaction` or `reaction_widgets` (those are separate plans). It produces working, testable software on its own — the new variant is usable by any existing `subscribe<T>` consumer.

---

## File Structure

**Files this plan touches:**

| File | Role | Action |
|---|---|---|
| `event_sourcing/lib/src/subscriptions/update.dart` | Defines the sealed `Update<T>` hierarchy | Add `EndOfReplay<T>` variant |
| `event_sourcing/test/subscriptions/update_test.dart` | Unit tests for `Update<T>` variants | Create file (or add to existing); test the new variant |
| `event_sourcing/lib/src/event_store.dart` | `EventStore._subscribeAggregate` builds the `Stream<Update<T>>` for `AggregateMode<T>` subscriptions | Track max-sequence-seen; emit `EndOfReplay<T>` after buffer drain, before `replayDone = true` |
| `event_sourcing/test/subscriptions/end_of_replay_emission_test.dart` | Integration test for the emission contract | Create new file; cover: empty view, populated view, concurrent appends during snapshot read |
| `event_sourcing/lib/event_sourcing.dart` | Public barrel + doc comment example | Update the doc-comment switch example to include the new variant |
| `event_sourcing/example_action_permissions/lib/server/bootstrap.dart` | Demo's runtime `is Delta<StoredEvent>` listener | No code change required (`is`-checks are not exhaustive); add a one-line comment noting EndOfReplay is intentionally ignored |

**No new files in `reaction/` or `reaction_widgets/` — those packages do not exist yet and are out of scope for this plan.**

---

## Task 1: Add the `EndOfReplay<T>` variant to `Update<T>`

**Files:**

- Test: `event_sourcing/test/subscriptions/update_test.dart` (create if missing)
- Modify: `event_sourcing/lib/src/subscriptions/update.dart`

- [ ] **Step 1: Check if `update_test.dart` exists**

```bash
ls event_sourcing/test/subscriptions/update_test.dart 2>&1
```

If the file doesn't exist, create it in the next step. If it exists, append to it.

- [ ] **Step 2: Write the failing test**

Create or append to `event_sourcing/test/subscriptions/update_test.dart`:

```dart
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:test/test.dart';

void main() {
  group('EndOfReplay<T>', () {
    test('is a subtype of Update<T> with a sequence', () {
      const Update<String> marker = EndOfReplay<String>(sequence: 42);
      expect(marker, isA<Update<String>>());
      expect(marker.sequence, equals(42));
    });

    test('is exhaustively switchable alongside the other variants', () {
      const Update<String> marker = EndOfReplay<String>(sequence: 7);
      final tag = switch (marker) {
        Snapshot<String>() => 'snapshot',
        EndOfReplay<String>() => 'end_of_replay',
        Delta<String>() => 'delta',
        Tombstone<String>() => 'tombstone',
      };
      expect(tag, equals('end_of_replay'));
    });
  });
}
```

- [ ] **Step 3: Run the test to verify it fails**

```bash
cd event_sourcing && dart test test/subscriptions/update_test.dart -n 'EndOfReplay'
```

Expected: compile error or test failure with "EndOfReplay isn't a constant" / "Undefined name 'EndOfReplay'".

- [ ] **Step 4: Add the new variant**

Modify `event_sourcing/lib/src/subscriptions/update.dart`. Insert the new class between `Snapshot<T>` and `Delta<T>` (so the file's variant order matches the temporal order of emission):

```dart
sealed class Update<T> {
  int get sequence;
  const Update();
}

class Snapshot<T> extends Update<T> {
  final T? value;
  @override
  final int sequence;
  const Snapshot({required this.value, required this.sequence});
}

/// Marker emitted by `EventStore.subscribe<T>` (AggregateMode only) after
/// the initial snapshot replay completes, before any live `Delta`/`Tombstone`
/// updates flow. For consumers that need a deterministic "snapshot complete;
/// stream is now live" signal — e.g., to dismiss a loading state, take a
/// resume cursor, or transition UI from skeleton to populated.
///
/// `sequence` is the max sequence reflected in the stream so far (the
/// max across emitted Snapshots and any deltas that arrived during snapshot
/// read and were drained from the buffer), or 0 if both are empty.
///
/// Not emitted by `Events()`-mode subscriptions (no replay phase).
class EndOfReplay<T> extends Update<T> {
  @override
  final int sequence;
  const EndOfReplay({required this.sequence});
}

class Delta<T> extends Update<T> {
  final T value;
  @override
  final int sequence;
  final String cause;
  const Delta({
    required this.value,
    required this.sequence,
    required this.cause,
  });
}

class Tombstone<T> extends Update<T> {
  final String aggregateId;
  @override
  final int sequence;
  const Tombstone({required this.aggregateId, required this.sequence});
}
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
cd event_sourcing && dart test test/subscriptions/update_test.dart -n 'EndOfReplay'
```

Expected: 2 tests pass, 0 failures.

- [ ] **Step 6: Run the broader subscriptions test suite to catch unexpected breakage**

```bash
cd event_sourcing && dart test test/subscriptions/ -r expanded
```

Expected: all tests pass (the broader suite doesn't yet exercise `EndOfReplay`; failures here would indicate the variant addition broke an unrelated test, which means the variant is being implicitly switched on somewhere in test code — fix by adding the case).

- [ ] **Step 7: Commit**

```bash
git add event_sourcing/lib/src/subscriptions/update.dart event_sourcing/test/subscriptions/update_test.dart
git commit -m "[CUR-1317] Add EndOfReplay<T> variant to Update<T> sealed class

The 4th variant surfaces what was previously the implicit
snapshot-to-deltas transition inside _subscribeAggregate's _replayDone
flag pattern. Consumers now have a deterministic 'snapshot complete'
signal — useful for loading-state UX and cross-process Subscribe
transports that need a clean cursor at the snapshot/tail boundary.

This task only adds the variant. Emission from EventStore.subscribe<T>
is added in a follow-on task in the same plan.

Refs: spec/prd-reaction.md (EVS-PRD-cross-process-event-transport,
EVS-PRD-view-subscriber)."
```

---

## Task 2: Emit `EndOfReplay<T>` from `_subscribeAggregate` after the buffer drain

**Files:**

- Test: `event_sourcing/test/subscriptions/end_of_replay_emission_test.dart` (create new)
- Modify: `event_sourcing/lib/src/event_store.dart` lines 453-526 (specifically the `start()` closure inside `_subscribeAggregate`)

The contract (from this plan's Architecture section):

1. After all Snapshots are emitted AND any buffered live changes are drained, emit exactly ONE `EndOfReplay<T>` update on the stream.
2. `EndOfReplay.sequence` = max sequence emitted in the stream so far (across Snapshots + drained buffered Delta/Tombstone), or `0` if neither.
3. The emission happens BEFORE `replayDone = true` is set, so the live listener still routes via the buffer; but the buffer has just been drained, so live arrivals after the EndOfReplay go via the direct path on the next event.
4. `Events()`-mode subscriptions emit no `EndOfReplay` (they have no replay phase).

- [ ] **Step 1: Familiarize with the existing test fixtures**

```bash
ls event_sourcing/test/subscriptions/
```

Look for existing `subscribe<T>` integration tests (e.g., files that exercise `AggregateMode` end-to-end via a real `EventStore` + sembast in-memory backend). Read at least one such file fully to understand:

- How an `EventStore` is wired up for tests (likely a helper in `test/_support/` or similar)
- How `ProjectionRegistry` gets a test `AggregateProjectionSpec`
- How a row is appended such that the view picks it up

If no existing helper covers this, look at `event_sourcing/test/actions/test_support/event_store_helper.dart` (referenced earlier in this conversation) — it's the action-tests' fixture and likely re-usable.

- [ ] **Step 2: Write the failing tests**

Create `event_sourcing/test/subscriptions/end_of_replay_emission_test.dart`:

```dart
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:test/test.dart';

// NOTE: This test depends on having a test EventStore harness that:
//   - Constructs an EventStore.open with an in-memory sembast backend
//   - Allows registering an AggregateProjectionSpec for a test view name
//   - Allows direct event append for setup
// Re-use the existing test fixture (likely event_sourcing/test/actions/
// test_support/event_store_helper.dart). If the API differs, adapt the
// setup calls below to match it.

void main() {
  group('EndOfReplay<T> emission from EventStore.subscribe<T>', () {
    late EventStore store;

    setUp(() async {
      // STUB (replaced in Step 4): brings up an EventStore with one
      // registered AggregateProjectionSpec under viewName 'test_view'.
      // The spec should map aggregateType 'thing' to a single row per
      // aggregateId. See event_sourcing/test/actions/test_support/
      // event_store_helper.dart for the existing pattern.
      store = await openTestEventStoreWithThingView();
    });

    tearDown(() async {
      await store.close();
    });

    test('empty view: emits EndOfReplay(sequence: 0) before any deltas', () async {
      final updates = <Update<Map<String, Object?>>>[];
      final completer = Completer<void>();

      final sub = store.subscribe<Map<String, Object?>>(
        const SubscriptionFilter(),
        AggregateMode<Map<String, Object?>>(
          viewName: 'test_view',
          mapper: (row) => row,
        ),
      ).listen(updates.add);

      // Give the subscribe pipeline one event-loop turn to read the
      // (empty) snapshot and emit EndOfReplay.
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      expect(updates, hasLength(1));
      expect(updates.single, isA<EndOfReplay<Map<String, Object?>>>());
      expect(updates.single.sequence, equals(0));

      await sub.cancel();
      completer.complete();
    });

    test(
      'populated view: emits N Snapshots then EndOfReplay with the max '
      'snapshotted sequence',
      () async {
        // Pre-populate the view with two events, each producing a row.
        await appendThingEvent(store, aggregateId: 'a', payload: {'v': 1});
        await appendThingEvent(store, aggregateId: 'b', payload: {'v': 2});

        // The two appends produced events at sequences 1 and 2 (or
        // higher if substrate-internal events also incremented the
        // sequence — capture actual sequences via the appends' returns).
        // Capture max snapshotted sequence for assertion below:
        final maxSnapshotSeq =
            await maxSequenceInView(store, viewName: 'test_view');

        final updates = <Update<Map<String, Object?>>>[];
        final sub = store.subscribe<Map<String, Object?>>(
          const SubscriptionFilter(),
          AggregateMode<Map<String, Object?>>(
            viewName: 'test_view',
            mapper: (row) => row,
          ),
        ).listen(updates.add);

        await Future<void>.delayed(Duration.zero);
        await Future<void>.delayed(Duration.zero);

        // Expect: 2 Snapshots followed by 1 EndOfReplay, no Deltas yet.
        expect(updates, hasLength(3));
        expect(updates[0], isA<Snapshot<Map<String, Object?>>>());
        expect(updates[1], isA<Snapshot<Map<String, Object?>>>());
        expect(updates[2], isA<EndOfReplay<Map<String, Object?>>>());
        expect(updates[2].sequence, equals(maxSnapshotSeq));

        await sub.cancel();
      },
    );

    test(
      'EndOfReplay arrives before any post-subscribe Delta',
      () async {
        await appendThingEvent(store, aggregateId: 'a', payload: {'v': 1});

        final updates = <Update<Map<String, Object?>>>[];
        final sub = store.subscribe<Map<String, Object?>>(
          const SubscriptionFilter(),
          AggregateMode<Map<String, Object?>>(
            viewName: 'test_view',
            mapper: (row) => row,
          ),
        ).listen(updates.add);

        // Allow the subscribe to settle (snapshot + EndOfReplay).
        await Future<void>.delayed(Duration.zero);
        await Future<void>.delayed(Duration.zero);

        // NOW append a new event. Its Delta should be ordered AFTER
        // EndOfReplay.
        await appendThingEvent(store, aggregateId: 'c', payload: {'v': 3});
        await Future<void>.delayed(Duration.zero);
        await Future<void>.delayed(Duration.zero);

        final endOfReplayIdx =
            updates.indexWhere((u) => u is EndOfReplay<Map<String, Object?>>);
        final firstDeltaIdx =
            updates.indexWhere((u) => u is Delta<Map<String, Object?>>);

        expect(endOfReplayIdx, isNonNegative);
        expect(firstDeltaIdx, isNonNegative);
        expect(endOfReplayIdx, lessThan(firstDeltaIdx));

        await sub.cancel();
      },
    );
  });

  group('EndOfReplay<T> NOT emitted by Events() mode', () {
    late EventStore store;

    setUp(() async {
      store = await openTestEventStoreWithThingView();
    });

    tearDown(() async {
      await store.close();
    });

    test('Events() subscription emits no EndOfReplay', () async {
      final updates = <Update<StoredEvent>>[];
      final sub = store.subscribe<StoredEvent>(
        const SubscriptionFilter(),
        const Events(),
      ).listen(updates.add);

      await appendThingEvent(store, aggregateId: 'a', payload: {'v': 1});
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      expect(
        updates.whereType<EndOfReplay<StoredEvent>>(),
        isEmpty,
      );

      await sub.cancel();
    });
  });
}

// --- Test fixture helpers below. Replace these with your project's
// existing harness if it provides equivalent setup; the helpers below
// are sketches showing the SHAPE the tests above need.

Future<EventStore> openTestEventStoreWithThingView() async {
  // The exact wiring depends on the project's existing test patterns.
  // Look at event_sourcing/test/actions/test_support/event_store_helper.dart
  // for the canonical bootstrap path. The fixture should:
  //   1. Open an in-memory sembast backend
  //   2. Register one EntryTypeDefinition for entryType='thing', version 1
  //   3. Register an AggregateProjectionSpec under viewName='test_view'
  //      that materializes 'thing' events into rows keyed by aggregateId
  //   4. Call EventStore.open(...) with the above
  throw UnimplementedError(
    'Replace with actual fixture; see event_store_helper.dart',
  );
}

Future<StoredEvent?> appendThingEvent(
  EventStore store, {
  required String aggregateId,
  required Map<String, Object?> payload,
}) async {
  // Replace with the project's existing append helper, or the equivalent
  // EventStore.append(...) call. Returns the StoredEvent so the test can
  // read its sequence if needed.
  throw UnimplementedError('Replace with actual append helper');
}

Future<int> maxSequenceInView(
  EventStore store, {
  required String viewName,
}) async {
  // Walk the view rows and return the max sequence. The substrate
  // backend exposes findViewRows for this; iterate and return max
  // (row['sequence'] as int).
  throw UnimplementedError('Replace with actual view-row scan');
}
```

NOTE: the three helper functions at the bottom (`openTestEventStoreWithThingView`, `appendThingEvent`, `maxSequenceInView`) are stubs. The implementer's first sub-step here is to wire them to whatever existing test helpers the repo provides — likely re-using or extending `event_sourcing/test/actions/test_support/event_store_helper.dart`. If a suitable helper does not yet exist for the AggregateProjectionSpec setup needed, write it inline in this test file (~30-50 LOC).

- [ ] **Step 3: Run the tests to verify they fail**

```bash
cd event_sourcing && dart test test/subscriptions/end_of_replay_emission_test.dart -r expanded
```

Expected: tests fail. Until Step 5, the EndOfReplay variant is defined but never emitted, so the empty-view test fails with `expected length 1 but was 0` and the populated-view test fails with `expected length 3 but was 2`.

- [ ] **Step 4: Wire the test helpers to real fixtures**

Replace the three stub helpers at the bottom of the test file with real implementations that re-use existing patterns. Concrete steps:

1. Read `event_sourcing/test/actions/test_support/event_store_helper.dart` end-to-end.
2. If it exposes a function like `openInMemoryEventStore({projections: ...})`, use that and pass an `AggregateProjectionSpec` for `'test_view'` mapping `entryType: 'thing'` to a single row per aggregate.
3. For `appendThingEvent`, use `EventStore.append(entryType: 'thing', aggregateId: aggregateId, eventType: 'thing_changed', data: payload, ...)` matching the existing append signature in tests.
4. For `maxSequenceInView`, call `store.backend.findViewRows('test_view')` and `fold` to find the max `row['sequence'] as int`.

After this step, re-run the tests; they should still fail (variant not yet emitted) but they should COMPILE and run to actual assertion failures rather than helper `UnimplementedError`s.

- [ ] **Step 5: Implement the emission**

Modify `event_sourcing/lib/src/event_store.dart` `_subscribeAggregate` (lines 453-526). Track the max sequence seen, emit `EndOfReplay<T>` after the buffer drain, before flipping `replayDone`:

```dart
Stream<Update<T>> _subscribeAggregate<T>(
  SubscriptionFilter filter,
  AggregateMode<T> mode,
) {
  late StreamController<Update<T>> controller;
  StreamSubscription<AggregateFoldChange>? liveSub;

  Future<void> start() async {
    var replayDone = false;
    var maxSequenceSeen = 0;
    final liveBuffer = <AggregateFoldChange>[];

    liveSub = _subs.rowChanges(mode.viewName).listen((change) {
      if (controller.isClosed) return;
      if (!replayDone) {
        liveBuffer.add(change);
      } else {
        final u = _changeToUpdate<T>(change, filter, mode);
        if (u != null) {
          if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
          controller.add(u);
        }
      }
    }, onDone: () => controller.close());

    // Snapshot read
    final aggregateIds = mode.aggregates;
    if (aggregateIds == null) {
      final rows = await backend.findViewRows(mode.viewName);
      for (final row in rows) {
        if (controller.isClosed) return;
        final seq = (row['sequence'] as int?) ?? 0;
        if (seq > maxSequenceSeen) maxSequenceSeen = seq;
        controller.add(
          Snapshot<T>(value: mode.mapper(row), sequence: seq),
        );
      }
    } else {
      for (final aggId in aggregateIds) {
        if (controller.isClosed) return;
        final row = await backend.transaction(
          (txn) => backend.readViewRowInTxn(txn, mode.viewName, aggId),
        );
        final seq = (row?['sequence'] as int?) ?? 0;
        if (seq > maxSequenceSeen) maxSequenceSeen = seq;
        controller.add(
          Snapshot<T>(
            value: row == null ? null : mode.mapper(row),
            sequence: seq,
          ),
        );
      }
    }

    // Drain buffered live changes that arrived during snapshot read.
    for (final change in liveBuffer) {
      if (controller.isClosed) return;
      final u = _changeToUpdate<T>(change, filter, mode);
      if (u != null) {
        if (u.sequence > maxSequenceSeen) maxSequenceSeen = u.sequence;
        controller.add(u);
      }
    }
    liveBuffer.clear();

    // Snapshot phase + buffer drain are complete. Emit EndOfReplay BEFORE
    // flipping replayDone so the marker is ordered correctly relative to
    // any deltas that arrive after this point.
    if (!controller.isClosed) {
      controller.add(EndOfReplay<T>(sequence: maxSequenceSeen));
    }

    replayDone = true;
  }

  controller = StreamController<Update<T>>(
    onListen: () => start(),
    onCancel: () async {
      await liveSub?.cancel();
      liveSub = null;
      if (!controller.isClosed) await controller.close();
    },
  );

  return controller.stream;
}
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
cd event_sourcing && dart test test/subscriptions/end_of_replay_emission_test.dart -r expanded
```

Expected: all 4 tests pass.

- [ ] **Step 7: Run the broader subscriptions test suite**

```bash
cd event_sourcing && dart test test/subscriptions/ -r expanded
```

Expected: all tests pass. If any test that previously asserted "stream emitted exactly N updates" now fails because of the new EndOfReplay marker shifting the count, update those tests to account for the marker.

- [ ] **Step 8: Run the full event_sourcing test suite**

```bash
cd event_sourcing && dart test -r expanded
```

Expected: all tests pass. Adjust any test that fails because it counted updates from `subscribe<T>` and now gets one more (the EndOfReplay).

- [ ] **Step 9: Commit**

```bash
git add event_sourcing/lib/src/event_store.dart event_sourcing/test/subscriptions/end_of_replay_emission_test.dart
git commit -m "[CUR-1317] Emit EndOfReplay<T> from EventStore.subscribe<T> AggregateMode

After the snapshot read and buffered-change drain complete in
_subscribeAggregate, emit exactly one EndOfReplay<T> update before
flipping replayDone. The marker's sequence field is the max
sequence emitted on the stream so far (snapshots + drained buffered
deltas), or 0 if neither.

Events() mode is unchanged — no replay phase, no EndOfReplay.

Refs: spec/prd-reaction.md (EVS-PRD-cross-process-event-transport,
EVS-PRD-view-subscriber)."
```

---

## Task 3: Update the doc-comment switch example

**Files:**

- Modify: `event_sourcing/lib/event_sourcing.dart` lines 60-64 (the existing switch example)

The barrel file's doc comment shows a `switch (update)` example that currently has only Snapshot/Delta/Tombstone cases. With the new variant, an exhaustive switch needs an EndOfReplay case too — and the example should demonstrate the deterministic snapshot-complete pattern that motivates the addition.

- [ ] **Step 1: Read current state of the doc comment**

```bash
sed -n '55,70p' event_sourcing/lib/event_sourcing.dart
```

Note the exact format and indentation of the existing example.

- [ ] **Step 2: Update the example**

In `event_sourcing/lib/event_sourcing.dart`, find the existing example (around lines 60-64) and replace it with a 4-case switch:

```dart
///   switch (update) {
///     case Snapshot(:final value):    print('snapshot: $value');
///     case EndOfReplay(:final sequence):
///       print('replay complete at sequence $sequence');
///     case Delta(:final value):       print('delta: $value');
///     case Tombstone(:final aggregateId):
///       print('deleted: $aggregateId');
///   }
```

(Match the surrounding indentation/comment style; the snippet above shows the new content but not the exact column placement.)

- [ ] **Step 3: Verify the file still parses**

```bash
cd event_sourcing && dart analyze lib/event_sourcing.dart
```

Expected: no new analyzer issues.

- [ ] **Step 4: Commit**

```bash
git add event_sourcing/lib/event_sourcing.dart
git commit -m "[CUR-1317] Doc comment: include EndOfReplay in subscribe<T> example"
```

---

## Task 4: Add a clarifying comment in the example_action_permissions listener

**Files:**

- Modify: `event_sourcing/example_action_permissions/lib/server/bootstrap.dart` around line 109

The listener uses `if (update is Delta<StoredEvent>)`, which is not exhaustive — so it doesn't break with the new variant. But a future reader looking at the file may wonder what happens to EndOfReplay; a one-line comment addresses that.

- [ ] **Step 1: Read context around the listener**

```bash
sed -n '100,120p' event_sourcing/example_action_permissions/lib/server/bootstrap.dart
```

- [ ] **Step 2: Add the comment**

In `event_sourcing/example_action_permissions/lib/server/bootstrap.dart`, locate the `eventStore.subscribe<StoredEvent>(...).listen((update) { ... })` block. Add a brief comment immediately above the `if (update is Delta<StoredEvent>)` line:

```dart
.listen((update) {
  // Events()-mode subscriptions never emit Snapshot/EndOfReplay/Tombstone;
  // we only act on Delta. Any future variants are intentionally ignored.
  if (update is Delta<StoredEvent>) {
    directoryMaterializer.applyDirect(update.value.data);
  }
});
```

- [ ] **Step 3: Run example_action_permissions tests to make sure nothing broke**

```bash
cd event_sourcing/example_action_permissions && dart test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add event_sourcing/example_action_permissions/lib/server/bootstrap.dart
git commit -m "[CUR-1317] example_action_permissions: clarify Events() listener ignores non-Delta variants"
```

---

## Task 5: Final verification across the whole repo

- [ ] **Step 1: Run the full event_sourcing test suite**

```bash
cd event_sourcing && dart test -r expanded
```

Expected: all tests pass. If any test was implicitly counting on the prior 3-variant `Update<T>`, it must be updated. Look for failures with messages like "expected N but was N+1".

- [ ] **Step 2: Run the example_action_permissions test suite**

```bash
cd event_sourcing/example_action_permissions && dart test
```

Expected: all tests pass.

- [ ] **Step 3: Run the example/ test suite (the dual-pane substrate showcase)**

```bash
cd event_sourcing/example && flutter test
```

Expected: all tests pass.

- [ ] **Step 4: Run flutter analyze on event_sourcing and the two examples**

```bash
cd event_sourcing && flutter analyze
cd ../event_sourcing/example_action_permissions && flutter analyze
cd ../../event_sourcing/example && flutter analyze
```

Expected: clean (info-level lints only). If any new analyzer warnings appear (e.g., `non_exhaustive_switch_expression`), add the missing EndOfReplay case to the offending switch.

- [ ] **Step 5: If any cleanup commits were needed in Steps 1-4, commit them**

```bash
git status
# If unstaged changes exist:
git add <files>
git commit -m "[CUR-1317] Test/lint cleanup for EndOfReplay<T> addition"
```

- [ ] **Step 6: Verify the spec reference still resolves**

```bash
grep -n "EndOfReplay" spec/prd-reaction.md | head -10
```

Confirm that `spec/prd-reaction.md` references `EndOfReplay<T>` consistently with what the substrate now emits. If the spec text is more specific than the implementation (e.g., it specifies a different `sequence` semantic), revisit and align — likely by editing the spec since the implementation is now the ground truth for behavior.

---

## Self-Review Checklist (run before declaring the plan complete)

- [ ] Each `Task N` produces a working, testable, commit-shaped unit of work
- [ ] No `TODO`/`TBD`/`fill in` placeholders in any step (verify by `grep -n 'TBD\|TODO\|fill in' docs/superpowers/plans/2026-05-11-end-of-replay-substrate-addition.md`)
- [ ] Every code-changing step shows the actual code, not a description of it
- [ ] Test fixture helpers in Task 2 Step 2 are explicitly called out as stubs to be wired in Step 4 (avoids the "implementer reads ahead and doesn't notice" trap)
- [ ] Identifier consistency: `EndOfReplay`, `replayDone`, `maxSequenceSeen`, `sequence` — all spelled the same way across tasks
- [ ] Spec reference (`spec/prd-reaction.md`) checked at end, in case the spec drifts during implementation
