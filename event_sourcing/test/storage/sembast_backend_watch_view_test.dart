// Verifies: EVS-PRD-portability/D
// watchView is a SembastBackend-specific
//   reactive surface exposing view-store mutations; snapshot-on-subscribe +
//   re-emit-on-mutation; cross-view-name isolation.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

void main() {
  group('SembastBackend.watchView', () {
    late SembastBackend backend;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('watch-view-$dbCounter.db');
    });

    tearDown(() async {
      await backend.close();
    });

    // unknown view.
    test('watchView emits empty snapshot for unknown view', () async {
      final stream = backend.watchView('never-written');
      await expectLater(
        stream,
        emits(
          isA<List<Map<String, Object?>>>().having(
            (l) => l.length,
            'length',
            0,
          ),
        ),
      );
    });

    test('watchView emits a new snapshot on upsert', () async {
      final stream = backend.watchView('lights');
      final emissions = <List<Map<String, Object?>>>[];
      final sub = stream.listen(emissions.add);
      await Future<void>.delayed(Duration.zero); // initial empty snapshot

      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(
          txn,
          'lights',
          'red',
          <String, Object?>{'color': 'red', 'is_on': true},
        );
      });
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(
        Duration.zero,
      ); // emitSnapshot's two-microtask chain

      await sub.cancel();
      expect(emissions.length, greaterThanOrEqualTo(2));
      expect(emissions.first, isEmpty);
      expect(emissions.last, hasLength(1));
      expect(emissions.last.first['color'], 'red');
      expect(emissions.last.first['is_on'], true);
    });

    test('watchView emits a new snapshot on delete', () async {
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(
          txn,
          'lights',
          'red',
          <String, Object?>{'color': 'red'},
        );
      });

      final stream = backend.watchView('lights');
      final emissions = <List<Map<String, Object?>>>[];
      final sub = stream.listen(emissions.add);
      await Future<void>.delayed(Duration.zero);

      await backend.transaction((txn) async {
        await backend.deleteViewRowInTxn(txn, 'lights', 'red');
      });
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      await sub.cancel();
      expect(emissions.length, greaterThanOrEqualTo(2));
      expect(emissions.last, isEmpty);
    });

    test('watchView emits a new snapshot on clear', () async {
      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(
          txn,
          'lights',
          'red',
          <String, Object?>{'color': 'red'},
        );
        await backend.upsertViewRowInTxn(
          txn,
          'lights',
          'green',
          <String, Object?>{'color': 'green'},
        );
      });

      final stream = backend.watchView('lights');
      final emissions = <List<Map<String, Object?>>>[];
      final sub = stream.listen(emissions.add);
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      await backend.transaction((txn) async {
        await backend.clearViewInTxn(txn, 'lights');
      });
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      await sub.cancel();
      expect(emissions.length, greaterThanOrEqualTo(2));
      expect(emissions.first, hasLength(2));
      expect(emissions.last, isEmpty);
    });

    // emit to a watchView(A) subscriber).
    test('watchView is per-view (no cross-view noise)', () async {
      final streamA = backend.watchView('view-A');
      final emA = <List<Map<String, Object?>>>[];
      final sa = streamA.listen(emA.add);
      await Future<void>.delayed(Duration.zero);
      emA.clear();

      await backend.transaction((txn) async {
        await backend.upsertViewRowInTxn(txn, 'view-B', 'k1', <String, Object?>{
          'value': 1,
        });
      });
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      await sa.cancel();
      // Mutating view-B did not emit to view-A.
      expect(emA, isEmpty);
    });

    test('watchView closes on backend close, then throws', () async {
      final stream = backend.watchView('lights');
      final fut = expectLater(stream, emitsThrough(emitsDone));
      await backend.close();
      await fut;
      expect(() => backend.watchView('lights'), throwsStateError);
      backend = await _openBackend('watch-view-reopen-$dbCounter.db');
    });
  });
}
