import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:event_sourcing/src/projections/view_catch_up.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/manual_timers.dart';

const _kType = 'catch_up_note';
const _kView = 'catch_up_notes';
const _kOtherView = 'catch_up_notes_other';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kOtherSpec = AggregateProjectionSpec(
  viewName: _kOtherView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'view-catch-up-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

Future<EventStore> _open(
  SembastBackend backend, {
  EntryTypeVersion registered = const EntryTypeVersion(1, 0),
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
  PromoterRegistry? promoters,
  DateTime Function()? clock,
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      EntryTypeDefinition(
        id: _kType,
        registeredVersion: registered,
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
      hopId: 'catch-up-hop',
      identifier: 'catch-up-install',
      softwareVersion: 'catch-up-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
    promoters: promoters,
    clock: clock,
  );
}

Future<ViewCopy> _copyOf(SembastBackend backend, String viewName) async {
  final all = await backend.transaction(backend.readViewCopiesInTxn);
  return all.singleWhere((c) => c.viewName == viewName && !c.markedForDeletion);
}

/// Polls, pumping the event queue between checks, until [viewName]'s
/// unmarked copy has a watermark at or past [sequence]. Fails the test
/// after too many pumps rather than hanging.
Future<void> _waitUntilWatermarkAtLeast(
  SembastBackend backend,
  String viewName,
  int sequence,
) async {
  for (var i = 0; i < 500; i++) {
    final copy = await _copyOf(backend, viewName);
    if (copy.watermark >= sequence) return;
    await pumpEventQueue();
  }
  fail('view "$viewName" did not catch up to sequence $sequence in time');
}

Future<void> _waitUntilCopyGone(SembastBackend backend, String copyId) async {
  for (var i = 0; i < 500; i++) {
    final all = await backend.transaction(backend.readViewCopiesInTxn);
    if (all.every((c) => c.copyId != copyId)) return;
    await pumpEventQueue();
  }
  fail('copy "$copyId" was not deleted in time');
}

Future<void> _waitUntilCopyIdChanges(
  EventStore store,
  String viewName,
  String oldCopyId,
) async {
  for (var i = 0; i < 500; i++) {
    if (store.copyIdOf(viewName) != oldCopyId) return;
    await pumpEventQueue();
  }
  fail('view "$viewName" was not re-created in time');
}

Future<StoredEvent> _appendNote(EventStore store, String aggregateId) async {
  final event = await store.append(
    entryType: _kType,
    aggregateId: aggregateId,
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'title': aggregateId},
    initiator: const UserInitiator('catch-up-user'),
  );
  return event!;
}

void main() {
  group('the catch-up driver', () {
    // Verifies: EVS-DEV-view-convergence/G
    // Verifies: EVS-DEV-view-convergence/J
    // Verifies: EVS-DEV-view-convergence/K
    test('a view added over a log becomes current after open, matching a '
        'fresh rebuild', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      StoredEvent? last;
      for (var i = 0; i < 1000; i++) {
        last = await _appendNote(seeder, 'agg-${i % 50}');
      }
      await seeder.close();

      final store = await _open(backend);
      await _waitUntilWatermarkAtLeast(backend, _kView, last!.sequenceNumber);
      final beforeRebuild = (await store.reader.findViewRows(_kView)).rows;
      expect(beforeRebuild, hasLength(50));

      await rebuildView(
        store: store,
        viewName: _kView,
        deadline: DateTime.now().toUtc().add(const Duration(seconds: 20)),
      );
      final afterRebuild = (await store.reader.findViewRows(_kView)).rows;
      expect(
        {for (final r in afterRebuild) r['title']},
        {for (final r in beforeRebuild) r['title']},
        reason: "the catch-up driver's rows equal a fresh rebuild's",
      );
      await store.close();
    });

    // Verifies: EVS-DEV-view-convergence/K
    test('a new minor promotes through the chain', () async {
      final backend = await _openBackend();
      final older = await _open(backend);
      final appended = await _appendNote(older, 'agg-1');
      await older.close();

      final promoters = PromoterRegistry()
        ..register(
          const PromoterSpec(
            viewName: _kView,
            entryType: _kType,
            fromVersion: EntryTypeVersion(1, 0),
            toVersion: EntryTypeVersion(1, 1),
            transforms: [
              DefaultField(fieldName: 'promoted', defaultValue: 'defaulted'),
            ],
          ),
        )
        ..seal();

      final newer = await _open(
        backend,
        registered: const EntryTypeVersion(1, 1),
        promoters: promoters,
      );
      await _waitUntilWatermarkAtLeast(
        backend,
        _kView,
        appended.sequenceNumber,
      );
      final rows = (await newer.reader.findViewRows(_kView)).rows;
      expect(rows.single['promoted'], 'defaulted');
      await newer.close();
    });

    // Verifies: EVS-DEV-view-convergence/G
    // Verifies: EVS-DEV-view-convergence/H
    test('no catch-up transaction begins before open returns, and none after '
        'close', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      final appended = await _appendNote(seeder, 'agg-1');
      await seeder.close();

      final begins = <String>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(onCatchUpTransactionBegin: begins.add),
        () async {
          final store = await _open(backend);
          expect(
            begins,
            isEmpty,
            reason: 'no catch-up transaction begins before open returns',
          );
          await _waitUntilWatermarkAtLeast(
            backend,
            _kView,
            appended.sequenceNumber,
          );
          await store.close();
          final atClose = begins.length;
          await pumpEventQueue(times: 100);
          expect(
            begins.length,
            atClose,
            reason: 'no catch-up transaction begins after close',
          );
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/I
    test('close awaits the catch-up transaction in flight', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      await _appendNote(seeder, 'agg-1');
      await seeder.close();

      final blocker = Completer<void>();
      var stepEntered = false;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          onCatchUpStep: (copyId, eventId) async {
            stepEntered = true;
            await blocker.future;
          },
        ),
        () async {
          final store = await _open(backend);
          for (var i = 0; i < 500 && !stepEntered; i++) {
            await pumpEventQueue();
          }
          expect(stepEntered, isTrue);

          var closed = false;
          final closeFuture = store.close().then((_) => closed = true);
          await pumpEventQueue(times: 50);
          expect(
            closed,
            isFalse,
            reason: 'close awaits the transaction in flight',
          );

          blocker.complete();
          await closeFuture;
          expect(closed, isTrue);
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/N
    // Verifies: EVS-DEV-view-convergence/O
    test(
      'the 200 ms bound stops further steps, and one step always runs',
      () async {
        final backend = await _openBackend();
        final seeder = await _open(backend, projections: const []);
        StoredEvent? last;
        for (var i = 0; i < 5; i++) {
          last = await _appendNote(seeder, 'agg-$i');
        }
        await seeder.close();

        var now = DateTime.utc(2026, 1, 1);
        late String viewCopyId;
        var transactionBegins = 0;
        late EventStore store;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            catchUpClock: () => now,
            onCatchUpTransactionBegin: (copyId) {
              if (copyId == viewCopyId) transactionBegins++;
            },
            onCatchUpStep: (copyId, eventId) {
              if (copyId == viewCopyId) {
                now = now.add(const Duration(milliseconds: 201));
              }
            },
          ),
          () async {
            store = await _open(backend);
            viewCopyId = store.copyIdOf(_kView);
            await _waitUntilWatermarkAtLeast(
              backend,
              _kView,
              last!.sequenceNumber,
            );
            expect(
              transactionBegins,
              6,
              reason:
                  'the 200 ms bound stopped every transaction after one '
                  'step, so the 5 events each needed their own transaction, '
                  'plus one confirming transaction that found the tip',
            );
            await store.close();
          },
        );
      },
    );

    // Verifies: EVS-DEV-view-convergence/O
    test('a transaction performs at least one step even when the clock '
        'already reads past the 200 ms bound', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      StoredEvent? last;
      for (var i = 0; i < 3; i++) {
        last = await _appendNote(seeder, 'agg-$i');
      }
      await seeder.close();

      // Every transaction for this copy sees a clock that reads before
      // the bound on its first read (capturing the transaction's start)
      // and past the bound on every read after -- so a do-while's first
      // step always runs, and a check-then-step loop would never step
      // at all.
      final base = DateTime.utc(2026, 1, 1);
      late String viewCopyId;
      var afterArmCalls = 0;
      DateTime clock() {
        afterArmCalls++;
        return afterArmCalls <= 1
            ? base
            : base.add(const Duration(milliseconds: 300));
      }

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          catchUpClock: clock,
          onCatchUpTransactionBegin: (copyId) {
            if (copyId == viewCopyId) afterArmCalls = 0;
          },
        ),
        () async {
          final store = await _open(backend);
          viewCopyId = store.copyIdOf(_kView);
          await _waitUntilWatermarkAtLeast(
            backend,
            _kView,
            last!.sequenceNumber,
          );
          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/Q
    test('a throwing step backs off 1 s then 2 s, records the failure in '
        "the copy's progress, and other copies keep catching up", () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      StoredEvent? last;
      for (var i = 0; i < 2; i++) {
        last = await _appendNote(seeder, 'agg-$i');
      }
      await seeder.close();

      final manualTimers = ManualTimers();

      // A frozen clock: real elapsed wall time between setting a backoff
      // and computing the remaining wait would otherwise make the
      // recorded durations a few milliseconds short of exact.
      var now = DateTime.utc(2026, 1, 1);

      late String failingCopyId;
      const injected = InjectedFailure('always fails');
      late EventStore store;
      final recordedBackoffs = <Duration>[];
      final recordedWaits = <Duration>[];
      Timer recordingFactory(
        Duration duration,
        void Function(Timer timer) callback,
      ) {
        recordedWaits.add(duration);
        return manualTimers.create(duration, callback);
      }

      // Records a newly observed failure's backoff the first time this
      // sees the copy's progress reflect it -- called after every await,
      // since the first failure can land before the test ever drives the
      // clock (the driver attempts a copy as soon as it discovers it).
      void captureIfNew() {
        final progress = store.catchUpProgressOf(failingCopyId);
        if (progress == null) return;
        if (progress.consecutiveFailures != recordedBackoffs.length + 1) {
          return;
        }
        recordedBackoffs.add(
          progress.nextAttemptAt!.difference(progress.lastFailureAt!),
        );
        expect(
          progress.lastFailure,
          isA<InjectedFailure>().having(
            (f) => f.point,
            'point',
            injected.point,
          ),
          reason:
              "Q records the step's error in the copy's progress "
              '(failure ${recordedBackoffs.length})',
        );
      }

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: recordingFactory,
          catchUpClock: () => now,
          onCatchUpStep: (copyId, eventId) {
            if (copyId == failingCopyId) throw injected;
          },
        ),
        () async {
          store = await _open(
            backend,
            projections: const <ProjectionSpec>[_kSpec, _kOtherSpec],
          );
          failingCopyId = store.copyIdOf(_kView);

          for (var i = 0; i < 200 && recordedBackoffs.isEmpty; i++) {
            captureIfNew();
            if (recordedBackoffs.isEmpty) await pumpEventQueue();
          }

          // The other copy keeps catching up despite the failing one: the
          // idle wait is never delayed by the failing copy's backoff
          // (EVS-DEV-view-convergence/Q), so discovery keeps running.
          await _waitUntilWatermarkAtLeast(
            backend,
            _kOtherView,
            last!.sequenceNumber,
          );

          while (recordedBackoffs.length < 2) {
            // Advance the frozen clock past whatever backoff is in
            // effect, then fire the manual timer that was waiting on it,
            // so the driver's due-check admits the retry.
            now = now.add(const Duration(seconds: 3));
            await manualTimers.fire();
            for (var i = 0; i < 200 && recordedBackoffs.length < 2; i++) {
              captureIfNew();
              if (recordedBackoffs.length < 2) await pumpEventQueue();
            }
          }
          expect(recordedBackoffs, <Duration>[
            const Duration(seconds: 1),
            const Duration(seconds: 2),
          ], reason: 'the backoff doubles from 1 s');
          expect(
            recordedWaits,
            everyElement(predicate<Duration>((d) => d <= kCatchUpIdleInterval)),
            reason:
                'the failing copy backing off as long as 2 s never makes '
                'the driver wait longer than the idle interval before its '
                "next discovery pass, so another copy's catch-up is never "
                'delayed by it',
          );

          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-view-convergence/S
    test(
      "a marked copy's rows are deleted 500 per step, then its record",
      () async {
        final backend = await _openBackend();
        final withView = await _open(backend);
        final copyId = withView.copyIdOf(_kView);
        for (var i = 0; i < 600; i++) {
          await _appendNote(withView, 'agg-$i');
        }
        await withView.close();

        var deleteTransactions = 0;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            onCatchUpTransactionBegin: (id) {
              if (id == copyId) deleteTransactions++;
            },
          ),
          () async {
            // Reopening without the view marks its copy for deletion
            // (EVS-DEV-view-convergence/D).
            final dropped = await _open(backend, projections: const []);
            await _waitUntilCopyGone(backend, copyId);
            expect(
              deleteTransactions,
              2,
              reason: '600 rows need two steps of up to 500',
            );
            await dropped.close();
          },
        );
      },
    );

    // Verifies: EVS-DEV-view-convergence/T
    test('a copy deleted underneath the instance is re-created', () async {
      final backend = await _openBackend();
      final store = await _open(backend);
      final oldCopyId = store.copyIdOf(_kView);

      await backend.transaction((txn) async {
        await backend.deleteViewCopyRowsInTxn(txn, oldCopyId, limit: 10000);
        await backend.deleteViewCopyRecordInTxn(txn, oldCopyId);
      });

      await _waitUntilCopyIdChanges(store, _kView, oldCopyId);
      expect(store.copyIdOf(_kView), isNot(oldCopyId));

      await _appendNote(store, 'agg-after-recreate');
      final rows = (await store.reader.findViewRows(_kView)).rows;
      expect(rows, hasLength(1));
      await store.close();
    });

    // Verifies: EVS-DEV-view-convergence/M (isolate-local lock)
    test('the lock held means the transaction ends without writing', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      final appended = await _appendNote(seeder, 'agg-1');
      await seeder.close();

      final begins = <String>[];
      final manualTimers = ManualTimers();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          timerFactory: manualTimers.create,
          onCatchUpTransactionBegin: begins.add,
        ),
        () async {
          final store = await _open(backend);
          final copyId = store.copyIdOf(_kView);
          final handle = backend.drainExclusionKey(store.databaseId);
          expect(debugTryHoldViewCopyLock(handle, copyId), isTrue);

          await pumpEventQueue(times: 100);
          expect(
            begins.contains(copyId),
            isFalse,
            reason:
                'a held lock ends the attempt before it opens a '
                'transaction',
          );
          final copy = await _copyOf(backend, _kView);
          expect(copy.watermark, 0);

          debugReleaseViewCopyLock(handle, copyId);
          // The idle wait between passes runs on a manual timer here:
          // fire it a few times so the driver's next pass, now finding
          // the lock free, does not wait on real time.
          for (var i = 0; i < 5; i++) {
            await manualTimers.fire();
          }
          await _waitUntilWatermarkAtLeast(
            backend,
            _kView,
            appended.sequenceNumber,
          );
          await store.close();
        },
      );
    });
  });

  group('a catch-up fold failure', () {
    const kKeyedView = 'catch_up_keyed_notes';
    const kKeyedSpec = TableProjectionSpec(
      viewName: kKeyedView,
      interest: SubscriptionFilter(entryTypes: <String>{_kType}),
      insertEventTypes: <String>{'finalized'},
      removeEventTypes: <String>{'removed'},
      rowKey: CompositeKey(<String>['data.k']),
      rowData: WholePayload(),
    );

    Future<StoredEvent> appendKeyed(
      EventStore store,
      String aggregateId, {
      required bool keyed,
    }) async {
      final event = await store.append(
        entryType: _kType,
        aggregateId: aggregateId,
        aggregateType: 'note',
        eventType: 'finalized',
        data: keyed
            ? <String, Object?>{'k': aggregateId}
            : <String, Object?>{'title': aggregateId},
        initiator: const UserInitiator('catch-up-user'),
      );
      return event!;
    }

    // Verifies: EVS-DEV-view-convergence/Z
    // Verifies: EVS-DEV-view-convergence/O
    // Verifies: EVS-DEV-security-findings/F
    // Verifies: EVS-DEV-security-findings/S
    // Verifies: EVS-PRD-materializer/I
    test('a converging copy that cannot key one event records one fold_failed '
        'finding, passes over it, and becomes current', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      StoredEvent? first;
      StoredEvent? unkeyable;
      StoredEvent? last;
      for (var i = 0; i < 4; i++) {
        last = await appendKeyed(seeder, 'agg-$i', keyed: i != 2);
        first ??= last;
        if (i == 2) unkeyable = last;
      }
      await seeder.close();

      late String copyId;
      var transactionCount = 0;
      // (transaction index, event id) for every step this copy took,
      // across every catch-up transaction attempted on it.
      final stepLog = <(int, String)>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          onCatchUpTransactionBegin: (id) {
            if (id != copyId) return;
            transactionCount++;
          },
          onCatchUpStep: (id, eventId) {
            if (id != copyId) return;
            stepLog.add((transactionCount, eventId));
          },
        ),
        () async {
          final store = await _open(backend, projections: const [kKeyedSpec]);
          copyId = store.copyIdOf(kKeyedView);

          await _waitUntilWatermarkAtLeast(
            backend,
            kKeyedView,
            last!.sequenceNumber,
          );

          final findings = await store.reader.findAllEvents(
            entryType: kSecurityFindingEntryType,
          );
          expect(findings, hasLength(1));
          final data = findings.single.data;
          expect(data['kind'], 'fold_failed');
          final evidence = data['evidence']! as Map<String, Object?>;
          expect(evidence['view'], kKeyedView);
          expect(evidence['event_id'], unkeyable!.eventId);
          expect(evidence['reason'], 'row_key_failed');

          final rows = (await store.reader.findViewRows(kKeyedView)).rows;
          final keys = rows.map((r) => r['aggregateId']).toSet();
          expect(keys, containsAll(<String>['agg-0', 'agg-1', 'agg-3']));
          expect(keys, isNot(contains('agg-2')));

          // The transaction that met the unkeyable event wrote nothing,
          // including the steps it folded before that event: the retry
          // re-folds the log from the same watermark it started at, so
          // the first event's step is seen again in a later transaction
          // than the one that first attempted it.
          final firstEventTransactions = stepLog
              .where((e) => e.$2 == first!.eventId)
              .map((e) => e.$1)
              .toSet();
          expect(
            firstEventTransactions.length,
            greaterThan(1),
            reason:
                'the first event is re-folded by a later transaction, '
                'proving the transaction that met the unkeyable event '
                'committed none of its earlier steps',
          );
          final unkeyableEventTransactions = stepLog
              .where((e) => e.$2 == unkeyable!.eventId)
              .map((e) => e.$1)
              .toSet();
          expect(
            unkeyableEventTransactions,
            hasLength(2),
            reason:
                'the unkeyable event is met once by the transaction that '
                'ends unwritten on it, and once more by the retry, which '
                'finds the finding held and passes over it -- never a '
                'third time',
          );

          final progress = await store.reader.viewProgress();
          expect(
            progress.singleWhere((p) => p.viewName == kKeyedView).state,
            ViewConvergenceState.current,
          );

          // No back-off: a fold failure never records a copy failure.
          expect(store.catchUpProgressOf(copyId)?.lastFailure, isNull);

          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-security-findings/T
    test('a fold_failed finding event the copy cannot key is passed over at '
        'once, with no second finding authored about it', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      // A finding whose own evidence carries no top-level 'k' is itself
      // unkeyable by kKeyedSpec: recorded through the same path any
      // detection point uses, not appended directly (the reserved
      // namespace refuses a plain append).
      final findingEvent = await seeder.runTransaction(
        (txn, collector) => recordFindingInTxnForTest(
          seeder,
          txn,
          collector,
          role: FindingRole.fold,
          kind: FindingKind.foldFailed,
          evidence: <String, Object?>{
            'view': 'peer-view',
            'definition_fingerprint': 'peer-fp',
            'event_id': 'peer-event',
            'sealed_hash': 'peer-hash',
            'reason': 'row_key_failed',
          },
          aggregates: <String>['peer-agg'],
        ),
      );
      await seeder.close();

      final store = await _open(backend, projections: const [kKeyedSpec]);
      await _waitUntilWatermarkAtLeast(
        backend,
        kKeyedView,
        findingEvent!.sequenceNumber,
      );

      final findings = await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      );
      expect(
        findings,
        hasLength(1),
        reason:
            'exactly the received finding is stored; the copy passes over '
            'it without authoring a further fold_failed finding about it '
            '(EVS-DEV-security-findings/T)',
      );
      await store.close();
    });

    // Verifies: EVS-DEV-view-convergence/Z
    test("when the finding's own append fails, the copy's watermark stays "
        'before the event and no pass-over happens', () async {
      final backend = await _openBackend();
      final seeder = await _open(backend, projections: const []);
      await appendKeyed(seeder, 'agg-0', keyed: true);
      final unkeyable = await appendKeyed(seeder, 'agg-1', keyed: false);
      await seeder.close();

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(failCatchUpFoldFindingAppend: () => true),
        () async {
          final store = await _open(backend, projections: const [kKeyedSpec]);
          final copyId = store.copyIdOf(kKeyedView);

          // The finding's own append fails with a plain throw, not a fold
          // failure, so this path is logged and backed off from 1 s like
          // any other storage failure (Q): a handful of pumps is enough to
          // observe the one attempt this backoff allows within the window.
          await pumpEventQueue(times: 200);

          final copy = await _copyOf(backend, kKeyedView);
          expect(
            copy.watermark,
            lessThan(unkeyable.sequenceNumber),
            reason:
                'the finding append is injected to fail, so the copy '
                'never passes over the event it names',
          );
          expect(
            await store.reader.findAllEvents(
              entryType: kSecurityFindingEntryType,
            ),
            isEmpty,
            reason: 'the failing append leaves no finding recorded',
          );
          expect(
            store.catchUpProgressOf(copyId)?.lastFailure,
            isNotNull,
            reason:
                'unlike a fold failure, the finding append throw is logged '
                "and backed off: it does update the copy's last failure",
          );

          await store.close();
        },
      );
    });
  });
}
