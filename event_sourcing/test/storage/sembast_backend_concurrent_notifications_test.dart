// Verifies: EVS-PRD-subscription/E
// a committed transaction and a rolled-back one started together on one
//   Sembast backend: the committed one's view change and queue status change
//   reach their watchers exactly once, the rolled-back one's never, in both
//   start orders.
//
// Sembast serializes transaction bodies, so the two bodies run one after
// the other in the order they start; only their futures are pending
// together. The two start orders are what make the test discriminating:
// committed-first shows a rollback does not withdraw the earlier commit's
// notification, and rolled-back-first shows the rollback's pending
// notification is discarded rather than published by the commit that runs
// after it.

import 'dart:async';

import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/fifo_entry_helpers.dart';

var _dbCounter = 0;

Future<SembastBackend> _openBackend() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'concurrent-notifications-$_dbCounter.db',
  );
  return SembastBackend(database: db);
}

/// Starts a transaction that writes through [write] on [name] and commits,
/// and a second that writes on [otherName] and throws after the write,
/// both started before either completes, in the order [committedFirst]
/// says; the backend runs their bodies in that order. Returns the live
/// emissions each watcher received after its initial snapshot.
Future<(List<T>, List<T>)> _runPair<T>({
  required SembastBackend backend,
  required Stream<T> Function(String name) watch,
  required Future<void> Function(Transaction txn, String name) write,
  required String name,
  required String otherName,
  required bool committedFirst,
}) async {
  final committedEmissions = <T>[];
  final rolledBackEmissions = <T>[];
  final subCommitted = watch(name).listen(committedEmissions.add);
  final subRolledBack = watch(otherName).listen(rolledBackEmissions.add);
  await pumpEventQueue();
  expect(committedEmissions, hasLength(1), reason: 'initial snapshot');
  expect(rolledBackEmissions, hasLength(1), reason: 'initial snapshot');

  Future<Object?> commit() => backend
      .transaction<void>((txn) => write(txn, name))
      .then<Object?>((_) => null, onError: (Object e) => e);
  Future<Object?> rollBack() => backend
      .transaction<void>((txn) async {
        await write(txn, otherName);
        throw StateError('injected rollback');
      })
      .then<Object?>((_) => null, onError: (Object e) => e);

  final outcomes = committedFirst
      ? await Future.wait<Object?>([commit(), rollBack()])
      : await Future.wait<Object?>([rollBack(), commit()]);
  expect(outcomes.whereType<StateError>(), hasLength(1));
  await pumpEventQueue();
  await subCommitted.cancel();
  await subRolledBack.cancel();
  return (
    committedEmissions.skip(1).toList(),
    rolledBackEmissions.skip(1).toList(),
  );
}

void main() {
  late SembastBackend backend;

  setUp(() async {
    backend = await _openBackend();
  });

  tearDown(() async {
    await backend.close();
  });

  group(
    'SembastBackend.watchView under a commit and a rollback started together',
    () {
      Future<void> upsert(Transaction txn, String view) =>
          backend.upsertViewRowInTxn(txn, view, 'row-1', <String, Object?>{
            'view': view,
          });

      for (final committedFirst in [true, false]) {
        test(
          'committed upsert notifies once, rolled-back upsert never '
          '(committed ${committedFirst ? 'starts first' : 'starts second'})',
          () async {
            final (committed, rolledBack) = await _runPair(
              backend: backend,
              watch: backend.watchView,
              write: upsert,
              name: 'view_a',
              otherName: 'view_b',
              committedFirst: committedFirst,
            );
            expect(committed, hasLength(1));
            expect(committed.single.single['view'], 'view_a');
            expect(rolledBack, isEmpty);
            expect(await backend.findViewRows('view_b'), isEmpty);
          },
        );
      }
    },
  );

  group('SembastBackend.watchFifo status change under a commit and a '
      'rollback started together', () {
    for (final committedFirst in [true, false]) {
      test(
        'committed status change notifies once, rolled-back one never '
        '(committed ${committedFirst ? 'starts first' : 'starts second'})',
        () async {
          final rowA = await enqueueSingle(backend, 'A', eventId: 'on-a');
          final rowB = await enqueueSingle(backend, 'B', eventId: 'on-b');
          final rowIds = <String, String>{'A': rowA.entryId, 'B': rowB.entryId};

          final (committed, rolledBack) = await _runPair<List<FifoEntry>>(
            backend: backend,
            watch: backend.watchFifo,
            write: (txn, destination) => backend.setFinalStatusTxn(
              txn,
              destination,
              rowIds[destination]!,
              FinalStatus.sent,
            ),
            name: 'A',
            otherName: 'B',
            committedFirst: committedFirst,
          );
          expect(committed, hasLength(1));
          expect(committed.single.single.finalStatus, FinalStatus.sent);
          expect(rolledBack, isEmpty);
          final entriesB = await backend.listFifoEntries('B');
          expect(entriesB.single.finalStatus, isNull);
        },
      );
    }
  });
}
