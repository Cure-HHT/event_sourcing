// Verifies: EVS-PRD-portability/D — generic view-storage methods are part of
//   the StorageBackend abstraction; projection fold interpreter reads/writes
//   view rows without coupling to a specific backend.
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend(String tag) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'views-$tag-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

void main() {
  group('generic view storage on StorageBackend', () {
    test('readViewRowInTxn on missing key returns null', () async {
      final b = await _backend('missing');
      final row = await b.transaction(
        (txn) async => b.readViewRowInTxn(txn, 'test_view', 'missing'),
      );
      expect(row, isNull);
      await b.close();
    });

    test('upsert then read round-trips', () async {
      final b = await _backend('upsert');
      await b.transaction((txn) async {
        await b.upsertViewRowInTxn(txn, 'test_view', 'k1', {'a': 1, 'b': 's'});
      });
      final row = await b.transaction(
        (txn) async => b.readViewRowInTxn(txn, 'test_view', 'k1'),
      );
      expect(row, {'a': 1, 'b': 's'});
      await b.close();
    });

    test('delete removes the row; read-after-delete returns null', () async {
      final b = await _backend('delete');
      await b.transaction((txn) async {
        await b.upsertViewRowInTxn(txn, 'test_view', 'k', {'x': 1});
        await b.deleteViewRowInTxn(txn, 'test_view', 'k');
      });
      final row = await b.transaction(
        (txn) async => b.readViewRowInTxn(txn, 'test_view', 'k'),
      );
      expect(row, isNull);
      await b.close();
    });

    test('findViewRows iterates with limit and offset', () async {
      final b = await _backend('find');
      await b.transaction((txn) async {
        for (var i = 0; i < 5; i++) {
          await b.upsertViewRowInTxn(txn, 'v', 'k$i', {'i': i});
        }
      });
      final all = await b.findViewRows('v');
      expect(all, hasLength(5));
      final two = await b.findViewRows('v', limit: 2);
      expect(two, hasLength(2));
      final skip2 = await b.findViewRows('v', limit: 100, offset: 2);
      expect(skip2, hasLength(3));
      await b.close();
    });

    test('clearViewInTxn empties one view without touching others', () async {
      final b = await _backend('clear');
      await b.transaction((txn) async {
        await b.upsertViewRowInTxn(txn, 'a', 'k', {'x': 1});
        await b.upsertViewRowInTxn(txn, 'b', 'k', {'y': 2});
        await b.clearViewInTxn(txn, 'a');
      });
      expect(await b.findViewRows('a'), isEmpty);
      expect(await b.findViewRows('b'), hasLength(1));
      await b.close();
    });

    // Verifies: EVS-PRD-permissions-as-events — multi-row in-txn read is
    //   the substrate primitive that lets the scoped-permissions authorize
    //   stage enumerate user_role_scopes inside the dispatch transaction
    //   (so the authorize-stage read and the execute-stage append share
    //   one read-consistent snapshot).
    // Verifies: EVS-PRD-action-dispatch — same dispatch-transaction
    //   coherence requirement on the authorize side.
    test('findViewRowsInTxn returns rows matching column equality filter '
        'inside a transaction', () async {
      final b = await _backend('find-in-txn-where');
      await b.transaction((txn) async {
        await b.upsertViewRowInTxn(txn, 'demo', 'k1', {
          'user_id': 'U1',
          'role': 'SC',
        });
        await b.upsertViewRowInTxn(txn, 'demo', 'k2', {
          'user_id': 'U1',
          'role': 'SUP',
        });
        await b.upsertViewRowInTxn(txn, 'demo', 'k3', {
          'user_id': 'U2',
          'role': 'SC',
        });
      });

      final rows = await b.transaction(
        (txn) => b.findViewRowsInTxn(
          txn,
          'demo',
          where: {'user_id': 'U1', 'role': 'SC'},
        ),
      );
      expect(rows, hasLength(1));
      expect(rows.single['user_id'], 'U1');
      expect(rows.single['role'], 'SC');
      await b.close();
    });

    test(
      'findViewRowsInTxn with null where returns all rows (limit/offset apply)',
      () async {
        final b = await _backend('find-in-txn-null-where');
        await b.transaction((txn) async {
          for (var i = 0; i < 5; i++) {
            await b.upsertViewRowInTxn(txn, 'v', 'k$i', {'i': i});
          }
        });
        final all = await b.transaction((txn) => b.findViewRowsInTxn(txn, 'v'));
        expect(all, hasLength(5));
        final two = await b.transaction(
          (txn) => b.findViewRowsInTxn(txn, 'v', limit: 2),
        );
        expect(two, hasLength(2));
        final skip2 = await b.transaction(
          (txn) => b.findViewRowsInTxn(txn, 'v', limit: 100, offset: 2),
        );
        expect(skip2, hasLength(3));
        await b.close();
      },
    );

    test(
      'findViewRowsInTxn with empty where returns all rows (no filtering)',
      () async {
        final b = await _backend('find-in-txn-empty-where');
        await b.transaction((txn) async {
          await b.upsertViewRowInTxn(txn, 'v', 'k1', {'a': 1});
          await b.upsertViewRowInTxn(txn, 'v', 'k2', {'a': 2});
        });
        final rows = await b.transaction(
          (txn) =>
              b.findViewRowsInTxn(txn, 'v', where: const <String, Object?>{}),
        );
        expect(rows, hasLength(2));
        await b.close();
      },
    );

    test(
      'findViewRowsInTxn sees uncommitted writes from same transaction',
      () async {
        final b = await _backend('find-in-txn-coherent');
        final rows = await b.transaction((txn) async {
          await b.upsertViewRowInTxn(txn, 'v', 'k1', {'user_id': 'U1'});
          await b.upsertViewRowInTxn(txn, 'v', 'k2', {'user_id': 'U2'});
          return b.findViewRowsInTxn(txn, 'v', where: {'user_id': 'U1'});
        });
        expect(rows, hasLength(1));
        expect(rows.single['user_id'], 'U1');
        await b.close();
      },
    );

    test('viewName isolation: writing to "a" never affects "b"', () async {
      final b = await _backend('isolation');
      await b.transaction((txn) async {
        await b.upsertViewRowInTxn(txn, 'a', 'k', {'src': 'a'});
        await b.upsertViewRowInTxn(txn, 'b', 'k', {'src': 'b'});
      });
      final a = await b.transaction(
        (txn) async => b.readViewRowInTxn(txn, 'a', 'k'),
      );
      final bb = await b.transaction(
        (txn) async => b.readViewRowInTxn(txn, 'b', 'k'),
      );
      expect(a, {'src': 'a'});
      expect(bb, {'src': 'b'});
      await b.close();
    });
  });
}
