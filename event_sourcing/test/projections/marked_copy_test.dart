// Verifies: EVS-DEV-view-convergence/T
// a copy marked for deletion, or whose record is gone, is treated as
//   absent by both the read path and the append fold: neither serves or
//   folds into it, and the unmarked copy of the fingerprint is found or
//   created instead.
// Verifies: EVS-DEV-view-convergence/Q
// a catch-up attempt that has no unmarked copy of its fingerprint yet
//   still backs off on failure, keyed by the fingerprint, instead of
//   retrying in a tight loop.
// Verifies: EVS-DEV-converging-view-reads/A
// Verifies: EVS-DEV-converging-view-reads/H

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
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

Future<EventStore> _open(SembastBackend backend) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry()..register(_kSpec);
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
  group('a copy marked for deletion, or whose record is gone', () {
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
}
