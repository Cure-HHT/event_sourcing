import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/view_catch_up.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/manual_timers.dart';

const _kType = 'marked_copy_note';
const _kView = 'marked_copy_notes';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'marked-copy-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

Future<EventStore> _open(
  SembastBackend backend, {
  List<AggregateProjectionSpec> projections = const [_kSpec],
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry();
  for (final spec in projections) {
    registry.register(spec);
  }
  return EventStore.openForTest(
    storage: backend,
    entryTypes: entryTypes,
    source: const Source(
      hopId: 'marked-copy-hop',
      identifier: 'marked-copy-install',
      softwareVersion: 'marked-copy-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

Future<StoredEvent> _appendNote(EventStore store, String aggregateId) async =>
    (await store.append(
      entryType: _kType,
      aggregateId: aggregateId,
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{'title': 't'},
      initiator: const UserInitiator('marked-copy-user'),
    ))!;

void main() {
  // Verifies: EVS-DEV-view-convergence/T
  group('a copy marked for deletion, or whose record is gone', () {
    // Verifies: EVS-DEV-converging-view-reads/A+H
    test('is never served as current or folded into, and a read never '
        'throws once its record is gone', () async {
      final manualTimers = ManualTimers();
      final backend = await _openBackend();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(timerFactory: manualTimers.create),
        () async {
          final store = await _open(backend);
          // Let the driver's first discovery pass settle on its idle
          // wait (blocked on the manual timer, never fired below) so
          // this test controls the copy's state without the driver's
          // own deletion racing it.
          await pumpEventQueue(times: 10);

          await _appendNote(store, 'agg-1');
          await _appendNote(store, 'agg-2');
          await _appendNote(store, 'agg-3');

          final copyId = store.copyIdOf(_kView);

          // As another instance's rebuildView or boot legitimately
          // would (EVS-DEV-view-convergence/D, U): mark the shared copy
          // for deletion, then delete part of its rows -- the window a
          // live deletion step leaves before its next step finishes.
          await backend.transaction((txn) async {
            await backend.markViewCopyForDeletionInTxn(txn, copyId);
            await backend.deleteViewCopyRowsInTxn(txn, copyId, limit: 2);
          });

          // The marked copy withholds its rows as converging.
          final duringDeletion = await store.reader.findViewRows(_kView);
          expect(
            duringDeletion.state,
            ViewConvergenceState.converging,
            reason:
                'the marked copy is never served as current, whatever '
                'rows its deletion step has left behind so far',
          );
          expect(duringDeletion.rows, isEmpty);

          // A library operation that decides from the view's rows -- the
          // authorization policy's reads, among others -- refuses with a
          // typed, transient error naming the view rather than deciding
          // from the marked copy's leftover rows.
          await store.reader.transaction((txn) async {
            await expectLater(
              () => currentViewRows(store.reader)(txn, _kView),
              throwsA(isA<ViewConvergingRefusal>()),
            );
          });

          // An append while the copy is marked does not fold into it or
          // move its watermark: the marked copy is left exactly as it
          // was, since the catch-up driver -- not an append -- is what
          // creates this instance's replacement copy.
          final beforeAppendRows = await backend.transaction(
            (txn) => backend.findViewRowsInTxn(txn, copyId),
          );
          final beforeAppendCopy = (await backend.transaction(
            backend.readViewCopiesInTxn,
          )).singleWhere((c) => c.copyId == copyId);
          await _appendNote(store, 'agg-4');
          final afterAppendRows = await backend.transaction(
            (txn) => backend.findViewRowsInTxn(txn, copyId),
          );
          final afterAppendCopy = (await backend.transaction(
            backend.readViewCopiesInTxn,
          )).singleWhere((c) => c.copyId == copyId);
          expect(
            afterAppendRows.length,
            beforeAppendRows.length,
            reason: 'the marked copy folds no further events',
          );
          expect(
            afterAppendCopy.watermark,
            beforeAppendCopy.watermark,
            reason: "the marked copy's watermark never moves",
          );

          // Finish the deletion by hand, as the driver's own next step
          // would, removing the copy's record entirely.
          await backend.transaction((txn) async {
            await backend.deleteViewCopyRowsInTxn(txn, copyId, limit: 10000);
            await backend.deleteViewCopyRecordInTxn(txn, copyId);
          });

          // No unmarked copy of the fingerprint remains: the read finds
          // none and creates one rather than throwing StateError.
          final afterRecordGone = await store.reader.findViewRows(_kView);
          expect(afterRecordGone.state, ViewConvergenceState.converging);
          expect(afterRecordGone.rows, isEmpty);

          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/Q
    test('a catch-up of a missing copy backs off on failure instead of '
        "spinning, and the view's progress reports the failure", () async {
      final backend = await _openBackend();
      final manualTimers = ManualTimers();
      var now = DateTime.utc(2026, 1, 1);
      var beginCount = 0;
      var throwOnStep = true;
      // Copy ids of every other view the store registers (built-in
      // views among them): their own idle catch-up passes run on the
      // same manual timer and would otherwise be counted as this
      // view's attempts.
      final otherCopyIds = <String>{};
      final recordedWaits = <Duration>[];
      const injected = InjectedFailure('missing-copy backoff test');
      late EventStore store;

      Timer recordingFactory(
        Duration duration,
        void Function(Timer timer) callback,
      ) {
        recordedWaits.add(duration);
        return manualTimers.create(duration, callback);
      }

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: recordingFactory,
          catchUpClock: () => now,
          onCatchUpStep: (copyId, eventId) {
            if (throwOnStep) throw injected;
          },
          onCatchUpTransactionBegin: (copyId) {
            if (!otherCopyIds.contains(copyId)) beginCount++;
          },
        ),
        () async {
          store = await _open(backend);
          // Let the initial discovery pass (every copy is current)
          // settle on its idle wait before this test starts counting
          // attempts.
          await pumpEventQueue(times: 10);
          final oldCopyId = store.copyIdOf(_kView);
          for (final copy in await backend.transaction(
            backend.readViewCopiesInTxn,
          )) {
            if (copy.copyId != oldCopyId) otherCopyIds.add(copy.copyId);
          }
          beginCount = 0;
          recordedWaits.clear();

          await _appendNote(store, 'agg-1');
          // Simulate the copy having been marked and fully deleted
          // underneath this instance (EVS-DEV-view-convergence/T Terms):
          // no unmarked copy of the fingerprint remains, so the
          // driver's next attempt has no known copy id.
          await backend.transaction((txn) async {
            await backend.deleteViewCopyRowsInTxn(txn, oldCopyId, limit: 10000);
            await backend.deleteViewCopyRecordInTxn(txn, oldCopyId);
          });

          // Wake the driver's idle wait so it notices the missing copy.
          await manualTimers.fire();
          for (var i = 0; i < 200 && beginCount < 1; i++) {
            await pumpEventQueue();
          }
          expect(
            beginCount,
            1,
            reason:
                'one attempt creates a copy, folds the one event, '
                'throws, and rolls back leaving no copy behind',
          );

          // Without the fix this spins with no backoff at all: pumping
          // the event queue alone, with no timer fire and no clock
          // advance, would drive thousands of further attempts.
          await pumpEventQueue(times: 300);
          expect(
            beginCount,
            1,
            reason:
                'nothing is due again until the backoff elapses, even '
                'though no copy id exists to key the backoff on',
          );

          // The doubling schedule itself, keyed by the fingerprint since
          // no copy id exists: not yet due at 500 ms into the 1 s backoff,
          // due once it elapses, and backed off to 2 s afterwards.
          now = now.add(const Duration(milliseconds: 500));
          await manualTimers.fire();
          await pumpEventQueue(times: 30);
          expect(
            beginCount,
            1,
            reason: "the 1 s backoff isn't due yet at 500 ms",
          );

          now = now.add(const Duration(milliseconds: 600));
          await manualTimers.fire();
          await pumpEventQueue(times: 30);
          expect(beginCount, 2, reason: 'due once the 1 s backoff elapses');

          now = now.add(const Duration(milliseconds: 1200));
          await manualTimers.fire();
          await pumpEventQueue(times: 30);
          expect(
            beginCount,
            2,
            reason: 'the next backoff doubles to 2 s, not due at 1.2 s',
          );

          now = now.add(const Duration(seconds: 1));
          await manualTimers.fire();
          await pumpEventQueue(times: 30);
          expect(
            beginCount,
            3,
            reason: 'due once the doubled 2 s backoff elapses',
          );

          final status = (await store.reader.viewProgress()).singleWhere(
            (s) => s.viewName == _kView,
          );
          expect(status.state, ViewConvergenceState.converging);
          expect(
            status.lastFailure,
            isA<InjectedFailure>(),
            reason:
                "the view's progress reports the failure recorded under "
                'the fingerprint key, even though no copy id has ever '
                'existed for it to be recorded under',
          );

          // Recovery: once the fold stops failing, the copy is created,
          // catches up and reaches tip. The stale backoff entry, recorded
          // under the fingerprint while no copy existed, must not turn
          // the driver's idle wait into a zero-duration spin afterwards.
          throwOnStep = false;
          now = now.add(const Duration(seconds: 4));
          recordedWaits.clear();
          await manualTimers.fire();
          for (var i = 0; i < 200; i++) {
            await pumpEventQueue();
            final recovered = (await store.reader.viewProgress()).singleWhere(
              (s) => s.viewName == _kView,
            );
            if (recovered.state == ViewConvergenceState.current) break;
          }
          final recoveredStatus = (await store.reader.viewProgress())
              .singleWhere((s) => s.viewName == _kView);
          expect(
            recoveredStatus.state,
            ViewConvergenceState.current,
            reason:
                'the copy is created and catches up once folding '
                'stops failing',
          );

          // A few further idle passes: none is a zero-duration wait, and
          // the driver never spins (only the other view's copy, excluded
          // above, and this now-current one are attempted).
          for (var i = 0; i < 5; i++) {
            await manualTimers.fire();
            await pumpEventQueue(times: 10);
          }
          expect(
            recordedWaits,
            everyElement(isNot(Duration.zero)),
            reason:
                'a stale backoff entry left under the fingerprint after '
                'recovery must not make every future idle wait zero',
          );

          await store.close();
        },
      );
    });
  });

  group('a backoff entry left under a key another instance makes stale', () {
    // Verifies: EVS-DEV-view-convergence/Q
    test('recorded under the fingerprint while no copy existed is cleared '
        'once another instance creates the copy, so recovery never spins '
        'with a zero-duration wait', () async {
      final backend = await _openBackend();
      final manualTimers = ManualTimers();
      var now = DateTime.utc(2026, 1, 1);
      var beginCount = 0;
      var throwOnStep = true;
      final otherCopyIds = <String>{};
      final recordedWaits = <Duration>[];
      const injected = InjectedFailure('fingerprint-then-other-instance');
      late EventStore store;

      Timer recordingFactory(
        Duration duration,
        void Function(Timer timer) callback,
      ) {
        recordedWaits.add(duration);
        return manualTimers.create(duration, callback);
      }

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: recordingFactory,
          catchUpClock: () => now,
          onCatchUpStep: (copyId, eventId) {
            if (throwOnStep) throw injected;
          },
          onCatchUpTransactionBegin: (copyId) {
            if (!otherCopyIds.contains(copyId)) beginCount++;
          },
        ),
        () async {
          store = await _open(backend);
          await pumpEventQueue(times: 10);
          final oldCopyId = store.copyIdOf(_kView);
          final fingerprint = (await backend.transaction(
            backend.readViewCopiesInTxn,
          )).singleWhere((c) => c.copyId == oldCopyId).fingerprint;
          for (final copy in await backend.transaction(
            backend.readViewCopiesInTxn,
          )) {
            if (copy.copyId != oldCopyId) otherCopyIds.add(copy.copyId);
          }
          beginCount = 0;
          recordedWaits.clear();

          await _appendNote(store, 'agg-1');
          // The copy is gone: the next attempt has no known copy id and
          // records its failure under the fingerprint.
          await backend.transaction((txn) async {
            await backend.deleteViewCopyRowsInTxn(txn, oldCopyId, limit: 10000);
            await backend.deleteViewCopyRecordInTxn(txn, oldCopyId);
          });

          await manualTimers.fire();
          for (var i = 0; i < 200 && beginCount < 1; i++) {
            await pumpEventQueue();
          }
          expect(
            beginCount,
            1,
            reason: 'one attempt creates a copy, throws and rolls back',
          );

          // Another instance creates the unmarked copy directly, standing
          // in for a concurrent build's own catch-up transaction. This
          // instance never keys another attempt by the fingerprint again.
          final otherInstanceCopyId = await backend.transaction(
            (txn) => backend.createViewCopyInTxn(txn, _kView, fingerprint, 0),
          );
          throwOnStep = false;

          // Advance the clock well past the stale fingerprint entry's
          // `nextAttemptAt` (lastFailure + kCatchUpInitialBackoff). Left
          // in place, a lingering entry would now compute a negative --
          // clamped to zero -- remaining duration; only a full removal
          // (not merely clearing `lastFailure`) keeps every subsequent
          // wait at the idle interval.
          now = now.add(kCatchUpIdleInterval * 10);
          recordedWaits.clear();
          final beginCountBeforeRecovery = beginCount;

          // Wake the driver repeatedly: every subsequent pass must key its
          // attempt by the copy the other instance created, never by the
          // now-stale fingerprint entry.
          for (var i = 0; i < 20; i++) {
            await manualTimers.fire();
            await pumpEventQueue(times: 20);
          }

          final finalStatus = (await store.reader.viewProgress()).singleWhere(
            (s) => s.viewName == _kView,
          );
          expect(finalStatus.state, ViewConvergenceState.current);
          expect(
            finalStatus.lastFailure,
            isNull,
            reason:
                'the stale fingerprint entry is cleared, not merely reset, '
                'once the copy the other instance created catches up',
          );
          expect(
            recordedWaits,
            everyElement(kCatchUpIdleInterval),
            reason:
                'the stale fingerprint entry must never make '
                "_nextWaitDuration return zero once the other instance's "
                'copy is current -- every wait after recovery is exactly '
                'the idle interval, never a shorter, clock-derived '
                'remaining duration',
          );
          expect(
            beginCount - beginCountBeforeRecovery,
            lessThanOrEqualTo(20),
            reason:
                'catch-up transaction begins stay bounded across the 20 '
                'timer fires -- one begin per fire, not a hot loop that '
                'begins several per fire with no wait between them',
          );
          expect(
            store.copyIdOf(_kView),
            otherInstanceCopyId,
            reason: "this instance caught up the other instance's copy",
          );

          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/Q
    test('recorded under a copy id that another instance deletes before '
        'this instance discovers it marked is pruned once the discovery '
        'pass notices the copy is gone', () async {
      final backend = await _openBackend();
      final manualTimers = ManualTimers();
      var now = DateTime.utc(2026, 1, 1);
      var beginCount = 0;
      var throwOnStep = true;
      // Copy ids belonging to the view under test, counted by
      // onCatchUpTransactionBegin below: seeded with oldCopyId once it is
      // known and extended with replacementCopyId once it is created.
      // Tracking membership this way, rather than excluding every copy id
      // *not* under test, stays correct regardless of which other views'
      // copies the driver also begins transactions on -- including ones
      // created, or first read, only after this test reads the copy list,
      // which an exclusion set captured at one point in time can miss.
      final trackedCopyIds = <String>{};
      final recordedWaits = <Duration>[];
      const injected = InjectedFailure('copy-id-then-deleted');
      late EventStore store;

      Timer recordingFactory(
        Duration duration,
        void Function(Timer timer) callback,
      ) {
        recordedWaits.add(duration);
        return manualTimers.create(duration, callback);
      }

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: recordingFactory,
          catchUpClock: () => now,
          onCatchUpStep: (copyId, eventId) {
            if (throwOnStep) throw injected;
          },
          onCatchUpTransactionBegin: (copyId) {
            if (trackedCopyIds.contains(copyId)) beginCount++;
          },
        ),
        () async {
          // Events appended with no projection registered for them force
          // the reopened store's catch-up driver to do real work against
          // an already-existing copy, rather than folding them at append
          // time, so the failure below is recorded under a real copy id.
          final seeder = await _open(backend, projections: const []);
          await _appendNote(seeder, 'agg-1');
          await seeder.close();

          store = await _open(backend);
          final oldCopyId = store.copyIdOf(_kView);
          // Synchronous with the copyIdOf call above -- no await lets the
          // driver's loop run in between -- so the very first begin this
          // instance's driver reports for oldCopyId is never missed.
          trackedCopyIds.add(oldCopyId);
          final fingerprint = (await backend.transaction(
            backend.readViewCopiesInTxn,
          )).singleWhere((c) => c.copyId == oldCopyId).fingerprint;

          // The copy still exists (freshly created at watermark 0) and is
          // behind the one seeded event, so the failure is recorded under
          // its own copy id, not the fingerprint.
          await manualTimers.fire();
          for (var i = 0; i < 200 && beginCount < 1; i++) {
            await pumpEventQueue();
          }
          expect(beginCount, 1);
          await pumpEventQueue(times: 10);
          expect(store.catchUpProgressOf(oldCopyId), isNotNull);

          // Another instance marks and fully deletes that copy, then
          // creates a fresh unmarked one -- as its own rebuild or a
          // replacement catch-up would -- before this instance's next
          // discovery pass notices the old copy id was ever marked.
          final replacementCopyId = await backend.transaction((txn) async {
            await backend.markViewCopyForDeletionInTxn(txn, oldCopyId);
            await backend.deleteViewCopyRowsInTxn(txn, oldCopyId, limit: 10000);
            await backend.deleteViewCopyRecordInTxn(txn, oldCopyId);
            return backend.createViewCopyInTxn(txn, _kView, fingerprint, 0);
          });
          trackedCopyIds.add(replacementCopyId);
          throwOnStep = false;

          // Advance the clock well past the old copy id's stale
          // `nextAttemptAt` (lastFailure + kCatchUpInitialBackoff). A
          // lingering, unpruned entry would now compute a negative --
          // clamped to zero -- remaining duration.
          now = now.add(kCatchUpIdleInterval * 10);
          recordedWaits.clear();
          final beginCountBeforeRecovery = beginCount;

          for (var i = 0; i < 20; i++) {
            await manualTimers.fire();
            await pumpEventQueue(times: 20);
          }

          expect(
            store.catchUpProgressOf(oldCopyId),
            isNull,
            reason:
                "the discovery pass prunes the old copy id's progress "
                'entry once it is neither a discovered copy nor a '
                'fingerprint lacking one',
          );
          final finalStatus = (await store.reader.viewProgress()).singleWhere(
            (s) => s.viewName == _kView,
          );
          expect(finalStatus.state, ViewConvergenceState.current);
          expect(finalStatus.lastFailure, isNull);
          expect(
            recordedWaits,
            everyElement(kCatchUpIdleInterval),
            reason:
                'a pruned entry must never make _nextWaitDuration return a '
                'shorter, clock-derived remaining duration -- every wait '
                'after the prune is exactly the idle interval',
          );
          expect(
            beginCount - beginCountBeforeRecovery,
            lessThanOrEqualTo(20),
            reason:
                'catch-up transaction begins stay bounded across the 20 '
                'timer fires -- one begin per fire, not a hot loop that '
                'begins several per fire with no wait between them',
          );
          expect(store.copyIdOf(_kView), replacementCopyId);

          await store.close();
        },
      );
    });
  });
}
