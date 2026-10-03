import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _kType = 'converging_read_note';
const _kView = 'converging_read_notes';
const _kTableView = 'converging_read_notes_table';

const _kAggregateSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kTableSpec = TableProjectionSpec(
  viewName: _kTableView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// Installed as `onCatchUpStep` to keep the catch-up driver backed off
/// (EVS-DEV-view-convergence/Q) for the duration of a test that reads a
/// copy this test has rewound behind the log by hand.
void _pauseCatchUp(String copyId, String eventId) =>
    throw const InjectedFailure('paused for a converging-view-reads test');

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'converging-view-reads-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

Future<EventStore> _open(
  SembastBackend backend, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kAggregateSpec],
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
      hopId: 'converging-read-hop',
      identifier: 'converging-read-install',
      softwareVersion: 'converging-read-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
    projections: registry,
  );
}

Future<StoredEvent> _appendNote(
  EventStore store,
  String aggregateId,
  String title,
) async => (await store.append(
  entryType: _kType,
  aggregateId: aggregateId,
  aggregateType: 'note',
  eventType: 'finalized',
  data: <String, Object?>{'title': title},
  initiator: const UserInitiator('converging-read-user'),
))!;

/// Rewinds [viewName]'s current copy watermark back to [watermark], as if
/// every event after it had not yet been folded, without touching its
/// already-folded rows: a way to put a copy behind the log without a
/// second build racing this test's own reads.
Future<void> _rewindWatermark(
  EventStore store,
  SembastBackend backend,
  String viewName,
  int watermark,
) async {
  final copyId = store.copyIdOf(viewName);
  await backend.transaction(
    (txn) => backend.setViewCopyWatermarkInTxn(txn, copyId, watermark),
  );
}

void main() {
  group('converging view reads', () {
    // Verifies: EVS-DEV-converging-view-reads/B+C
    test('an aggregate view converging on one aggregate withholds its row and '
        "serves the untouched aggregate's settled row", () async {
      final backend = await _openBackend();
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
        () async {
          final store = await _open(backend);
          final settled = await _appendNote(store, 'agg-settled', 'settled');
          await _appendNote(store, 'agg-behind', 'behind');
          await _rewindWatermark(
            store,
            backend,
            _kView,
            settled.sequenceNumber,
          );

          final all = await store.reader.findViewRows(_kView);
          expect(all.state, ViewConvergenceState.converging);
          expect(all.rows, hasLength(1));
          expect(all.rows.single['aggregateId'], 'agg-settled');

          final byKey = await store.reader.readViewRowsByKeys(_kView, {
            'agg-settled',
            'agg-behind',
          });
          expect(byKey.state, ViewConvergenceState.converging);
          expect(byKey.rows['agg-settled'], isA<SettledRow>());
          expect(byKey.rows['agg-settled']!.dataOrNull!['title'], 'settled');
          expect(byKey.rows['agg-behind'], isA<PendingRow>());

          final single = await store.reader.transaction(
            (txn) => store.reader.readViewRowInTxn(txn, _kView, 'agg-behind'),
          );
          expect(single.row, isA<PendingRow>());

          final absentKey = await store.reader.readViewRowsByKeys(_kView, {
            'no-such-aggregate',
          });
          expect(absentKey.rows['no-such-aggregate'], isA<AbsentRow>());

          await store.close();
        },
      );
    });

    // Verifies: EVS-DEV-converging-view-reads/B+C
    test(
      'a converging table view returns no rows, every key pending',
      () async {
        final backend = await _openBackend();
        await runWithDeliveryTestHooks(
          const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
          () async {
            final store = await _open(
              backend,
              projections: const [_kTableSpec],
            );
            await _appendNote(store, 'agg-1', 'first');
            await _rewindWatermark(store, backend, _kTableView, 0);

            final all = await store.reader.findViewRows(_kTableView);
            expect(all.state, ViewConvergenceState.converging);
            expect(all.rows, isEmpty);

            final byKey = await store.reader.readViewRowsByKeys(_kTableView, {
              'agg-1',
            });
            expect(byKey.rows['agg-1'], isA<PendingRow>());

            await store.close();
          },
        );
      },
    );

    // Verifies: EVS-DEV-converging-view-reads/A
    test('state and rows are read in one transaction: an append queued '
        'between the state read and the row read has not landed by the '
        'time the rows are read', () async {
      final backend = await _openBackend();
      final store = await _open(backend);
      await _appendNote(store, 'agg-1', 'first');

      var hookRan = false;
      final appended = Completer<void>();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterViewStateReadBeforeRows: () async {
            if (hookRan) return;
            hookRan = true;
            // Not awaited: Sembast serializes transactions on one
            // database-wide lock, so awaiting this here -- inside the
            // read's own still-open transaction -- would deadlock.
            // Queuing it lets the read's transaction finish first.
            unawaited(
              _appendNote(
                store,
                'agg-2',
                'second',
              ).then((_) => appended.complete()),
            );
            // Give the queued append a chance to reach the lock and
            // block behind this transaction.
            await Future<void>.delayed(Duration.zero);
          },
        ),
        () async {
          final result = await store.reader.findViewRows(_kView);
          expect(result.state, ViewConvergenceState.current);
          expect(result.rows, hasLength(1));
          expect(result.rows.single['aggregateId'], 'agg-1');
        },
      );
      expect(hookRan, isTrue);
      await appended.future;
      await store.close();
    });

    // Verifies: EVS-DEV-converging-view-reads/A
    // the by-key read's state scan and its row fetch share one storage
    //   transaction too: an append queued between them, naming a key this
    //   read asked for, has not landed by the time the rows are read.
    test('readViewRowsByKeys reads state and rows in one transaction: an '
        'append queued between them has not landed by the time the rows '
        'are read', () async {
      final backend = await _openBackend();
      final store = await _open(backend);
      await _appendNote(store, 'agg-1', 'first');

      var hookRan = false;
      final appended = Completer<void>();
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          afterViewStateReadBeforeRows: () async {
            if (hookRan) return;
            hookRan = true;
            // Not awaited: Sembast serializes transactions on one
            // database-wide lock, so awaiting this here -- inside the
            // read's own still-open transaction -- would deadlock.
            // Queuing it lets the read's transaction finish first.
            unawaited(
              _appendNote(
                store,
                'agg-1',
                'second',
              ).then((_) => appended.complete()),
            );
            // Give the queued append a chance to reach the lock and
            // block behind this transaction.
            await Future<void>.delayed(Duration.zero);
          },
        ),
        () async {
          final result = await store.reader.readViewRowsByKeys(_kView, {
            'agg-1',
          });
          expect(result.state, ViewConvergenceState.current);
          expect(result.rows['agg-1']!.dataOrNull!['title'], 'first');
        },
      );
      expect(hookRan, isTrue);
      await appended.future;
      await store.close();
    });

    // Verifies: EVS-DEV-converging-view-reads/D
    test('settled rows equal a replay: once the copy catches up, its rows '
        'match what the converging read already reported as settled', () async {
      final backend = await _openBackend();
      Map<String, dynamic>? settledRowWhileConverging;
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
        () async {
          final store = await _open(backend);
          final settled = await _appendNote(store, 'agg-settled', 'settled');
          await _appendNote(store, 'agg-behind', 'behind');
          await _rewindWatermark(
            store,
            backend,
            _kView,
            settled.sequenceNumber,
          );

          final read = await store.reader.findViewRows(_kView);
          expect(read.state, ViewConvergenceState.converging);
          settledRowWhileConverging = read.rows.single;
          await store.close();
        },
      );

      // Without the pausing hook, this reopen's catch-up runs to
      // completion (the previous store's watermark was left rewound).
      final caughtUp = await _open(backend);
      await _waitUntilCurrent(caughtUp, _kView);
      final replayed = await caughtUp.reader.findViewRows(_kView);
      expect(replayed.state, ViewConvergenceState.current);
      final byAggregate = {for (final r in replayed.rows) r['aggregateId']: r};
      expect(
        byAggregate['agg-settled'],
        settledRowWhileConverging,
        reason:
            "the converging read's settled row equals the caught-up "
            "copy's row for the same aggregate",
      );
      await caughtUp.close();
    });

    // Verifies: EVS-DEV-converging-view-reads/J
    test('viewProgress reports state, watermark, log head and last failure '
        'per registered view', () async {
      final backend = await _openBackend();
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(onCatchUpStep: _pauseCatchUp),
        () async {
          final store = await _open(backend);
          final settled = await _appendNote(store, 'agg-settled', 'settled');
          final behind = await _appendNote(store, 'agg-behind', 'behind');
          await _rewindWatermark(
            store,
            backend,
            _kView,
            settled.sequenceNumber,
          );

          // Rewinding makes the copy behind, so the driver's next idle
          // pass tries a catch-up transaction on it, which the pausing
          // hook fails: wait for that failure to land in the copy's
          // progress before reading it.
          final copyId = store.copyIdOf(_kView);
          for (var i = 0; i < 200; i++) {
            if (store.catchUpProgressOf(copyId)?.lastFailure != null) break;
            await Future<void>.delayed(const Duration(milliseconds: 20));
          }

          final progress = await store.reader.viewProgress();
          final view = progress.singleWhere((p) => p.viewName == _kView);
          expect(view.state, ViewConvergenceState.converging);
          expect(view.logHead, behind.sequenceNumber);
          expect(view.watermark, settled.sequenceNumber);
          expect(view.lastFailure, isA<InjectedFailure>());
          expect(view.lastFailureAt, isNotNull);
          await store.close();
        },
      );
    });
  });
}

/// Waits until [store]'s copy of [viewName] is current, polling its
/// reader's `viewProgress`.
Future<void> _waitUntilCurrent(EventStore store, String viewName) async {
  for (var i = 0; i < 200; i++) {
    final progress = await store.reader.viewProgress();
    final view = progress.singleWhere((p) => p.viewName == viewName);
    if (view.state == ViewConvergenceState.current) return;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw StateError('$viewName did not converge in time');
}
