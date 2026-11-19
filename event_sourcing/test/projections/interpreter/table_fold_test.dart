// event_sourcing/test/projections/interpreter/table_fold_test.dart
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
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

StoredEvent _ev(String type, Map<String, Object?> data) =>
    StoredEvent.synthetic(
      eventId: 'e-$type-${data.hashCode}',
      aggregateId: '_',
      aggregateType: '_',
      entryType: type,
      eventType: type,
      initiator: const UserInitiator('u'),
      clientTimestamp: DateTime.utc(2026, 5, 9),
      eventHash: 'h',
      sequenceNumber: 1,
      data: data,
    );

const _spec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(
    eventTypes: {'permission_granted', 'permission_revoked'},
  ),
  insertEventTypes: {'permission_granted'},
  removeEventTypes: {'permission_revoked'},
  rowKey: CompositeKey(['data.role', 'data.permission', 'data.scope']),
  rowData: WholePayload(),
);

// NOTE: In this codebase, StoredEvent.data IS the event payload. The
// CompositeKey path 'data.role' resolves to event.data['role'].
// WholePayload() returns event.data verbatim.

void main() {
  // Verifies: EVS-PRD-materializer/A
  group('TableFold.applyEvent', () {
    test('insert event upserts a row keyed by composite key', () async {
      final backend = await _backend();
      await backend.transaction((txn) async {
        await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          integrity: const <String>[],
          copyId: 'role_permission_grants',
          event: _ev('permission_granted', {
            'role': 'admin',
            'permission': 'users.invite',
            'scope': 'site',
          }),
        );
      });
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(
          txn,
          'role_permission_grants',
          'admin|users.invite|site',
        ),
      );
      expect(row, isNotNull);
      expect(row!['role'], 'admin');
    });

    test(
      'insert stamps aggregateId + sequence into the persisted row',
      () async {
        // TableFold stamps the same `aggregateId` and `sequence` fields that
        // AggregateFold stamps, satisfying the documented view-row contract
        // (ViewBuilder's `aggregateIdOf` reads 'aggregateId' from the raw row;
        // subscribe's snapshot replay uses 'sequence' for ordering).
        final backend = await _backend();
        await backend.transaction((txn) async {
          await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            integrity: const <String>[],
            copyId: 'role_permission_grants',
            event: _ev('permission_granted', {
              'role': 'admin',
              'permission': 'users.invite',
              'scope': 'site',
            }),
          );
        });
        final row = await backend.transaction(
          (txn) async => backend.readViewRowInTxn(
            txn,
            'role_permission_grants',
            'admin|users.invite|site',
          ),
        );
        expect(row, isNotNull);
        // Payload columns preserved.
        expect(row!['role'], 'admin');
        expect(row['permission'], 'users.invite');
        // Substrate-stamped identity + ordering fields, matching AggregateFold.
        expect(
          row['aggregateId'],
          'admin|users.invite|site',
          reason: 'TableFold must stamp the row key as aggregateId',
        );
        expect(
          row['sequence'],
          1,
          reason: 'TableFold must stamp the event sequence',
        );
      },
    );

    test('insert change.newValue carries aggregateId + sequence', () async {
      // Live Delta consumers receive change.newValue over the wire; it must
      // carry the same stamped fields as the persisted row so a mapper that
      // reads `row['aggregateId']` works for both snapshot replay and live
      // deltas.
      final backend = await _backend();
      AggregateFoldChange? change;
      await backend.transaction((txn) async {
        change = (await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          integrity: const <String>[],
          copyId: 'role_permission_grants',
          event: _ev('permission_granted', {
            'role': 'editor',
            'permission': 'notes.edit',
            'scope': 'west',
          }),
        )).single;
      });
      expect(change, isNotNull);
      expect(change!.newValue, isNotNull);
      expect(change!.newValue!['aggregateId'], 'editor|notes.edit|west');
      expect(change!.newValue!['sequence'], 1);
      expect(change!.newValue!['role'], 'editor');
    });

    test('remove event deletes the matching row', () async {
      final backend = await _backend();
      await backend.transaction((txn) async {
        await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          integrity: const <String>[],
          copyId: 'role_permission_grants',
          event: _ev('permission_granted', {
            'role': 'admin',
            'permission': 'users.invite',
            'scope': 'site',
          }),
        );
        await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          integrity: const <String>[],
          copyId: 'role_permission_grants',
          event: _ev('permission_revoked', {
            'role': 'admin',
            'permission': 'users.invite',
            'scope': 'site',
          }),
        );
      });
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(
          txn,
          'role_permission_grants',
          'admin|users.invite|site',
        ),
      );
      expect(row, isNull);
    });

    test(
      'remove event on nonexistent row returns null (no spurious tombstone)',
      () async {
        final backend = await _backend();
        List<AggregateFoldChange>? result;
        await backend.transaction((txn) async {
          result = await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            integrity: const <String>[],
            copyId: 'role_permission_grants',
            event: _ev('permission_revoked', {
              'role': 'admin',
              'permission': 'users.invite',
              'scope': 'site',
            }),
          );
        });
        // Row never existed — no tombstone should be emitted.
        expect(result, isEmpty);
      },
    );
  });

  // Verifies: EVS-PRD-materializer/A+B
  // the table fold's update, match-field and listed-removal rules: an update
  //   merges its fields into the row at its key, an update or remove whose
  //   match field differs from the row's changes nothing, an update for a
  //   missing row creates none, and a listed removal deletes every row the
  //   event names in its list, skipping missing ones.
  group('TableFold update, match and listed removal', () {
    const holds = TableProjectionSpec(
      viewName: 'holds',
      interest: SubscriptionFilter(
        eventTypes: {'held', 'released', 'cleared', 'deleted'},
      ),
      insertEventTypes: {'held'},
      updateEventTypes: {'released'},
      removeEventTypes: {'cleared'},
      rowKey: CompositeKey(['data.id', 'data.key']),
      rowData: CombinedRowData([
        SelectedFields(['id', 'key', 'hold_id']),
        ListLengthField('event_count', 'event_ids'),
        EventTimestampField('held_at'),
        ConstantFields({'state': 'held'}),
      ]),
      updateData: ConstantFields({'state': 'releasing'}),
      matchField: 'hold_id',
      listedRemovals: ListedRowKeys(
        eventTypes: {'deleted'},
        listField: 'key_lists',
        paths: ['data.id', 'item.key'],
      ),
    );

    Future<List<AggregateFoldChange>> apply(
      SembastBackend backend,
      StoredEvent event,
    ) => backend.transaction(
      (txn) => TableFold.applyEvent(
        txn: txn,
        backend: backend,
        spec: holds,
        integrity: const <String>[],
        copyId: 'holds',
        event: event,
      ),
    );

    Future<Map<String, dynamic>?> row(SembastBackend backend, String key) =>
        backend.transaction(
          (txn) async => backend.readViewRowInTxn(txn, 'holds', key),
        );

    test('insert extracts combined, counted, timestamped and constant '
        'fields', () async {
      final backend = await _backend();
      await apply(
        backend,
        _ev('held', {
          'id': 'd',
          'key': 'K',
          'hold_id': 'h1',
          'event_ids': ['a', 'b'],
          'other': 'ignored',
        }),
      );
      final held = (await row(backend, 'd|K'))!;
      expect(held['hold_id'], 'h1');
      expect(held['event_count'], 2);
      expect(held['held_at'], DateTime.utc(2026, 5, 9).toIso8601String());
      expect(held['state'], 'held');
      expect(held.containsKey('other'), isFalse);
    });

    test('an update merges into a matching row only', () async {
      final backend = await _backend();
      await apply(
        backend,
        _ev('held', {
          'id': 'd',
          'key': 'K',
          'hold_id': 'h1',
          'event_ids': ['a'],
        }),
      );
      expect(
        await apply(
          backend,
          _ev('released', {'id': 'd', 'key': 'K', 'hold_id': 'h0'}),
        ),
        isEmpty,
      );
      expect((await row(backend, 'd|K'))!['state'], 'held');
      expect(
        await apply(
          backend,
          _ev('released', {'id': 'd', 'key': 'Z', 'hold_id': 'h1'}),
        ),
        isEmpty,
      );
      expect(await row(backend, 'd|Z'), isNull);
      final changes = await apply(
        backend,
        _ev('released', {'id': 'd', 'key': 'K', 'hold_id': 'h1'}),
      );
      expect(changes.single.newValue!['state'], 'releasing');
      final released = (await row(backend, 'd|K'))!;
      expect(released['state'], 'releasing');
      expect(released['event_count'], 1);
      expect(released['hold_id'], 'h1');
    });

    test('a remove deletes a matching row only', () async {
      final backend = await _backend();
      await apply(
        backend,
        _ev('held', {'id': 'd', 'key': 'K', 'hold_id': 'h1'}),
      );
      expect(
        await apply(
          backend,
          _ev('cleared', {'id': 'd', 'key': 'K', 'hold_id': 'h0'}),
        ),
        isEmpty,
      );
      expect(await row(backend, 'd|K'), isNotNull);
      final changes = await apply(
        backend,
        _ev('cleared', {'id': 'd', 'key': 'K', 'hold_id': 'h1'}),
      );
      expect(changes.single.isTombstone, isTrue);
      expect(await row(backend, 'd|K'), isNull);
    });

    test('a listed removal deletes every named row that exists', () async {
      final backend = await _backend();
      for (final key in ['A', 'B', 'C']) {
        await apply(
          backend,
          _ev('held', {'id': 'd', 'key': key, 'hold_id': 'h-$key'}),
        );
      }
      await apply(
        backend,
        _ev('held', {'id': 'e', 'key': 'A', 'hold_id': 'h-eA'}),
      );
      final changes = await apply(
        backend,
        _ev('deleted', {
          'id': 'd',
          'key_lists': [
            {'key': 'A', 'hold_id': 'h-A'},
            {'key': 'B', 'hold_id': 'h-B'},
            {'key': 'M', 'hold_id': 'h-M'},
          ],
        }),
      );
      expect(changes.map((c) => c.aggregateId), ['d|A', 'd|B']);
      expect(await row(backend, 'd|A'), isNull);
      expect(await row(backend, 'd|B'), isNull);
      expect(await row(backend, 'd|C'), isNotNull);
      expect(await row(backend, 'e|A'), isNotNull);
      expect(
        await apply(backend, _ev('deleted', {'id': 'd'})),
        isEmpty,
        reason: 'a deletion naming no list removes nothing',
      );
      expect(
        TableFold.readKeysOf(
          holds,
          _ev('deleted', {
            'id': 'd',
            'key_lists': [
              {'key': 'C', 'hold_id': 'h-C'},
            ],
          }),
        ),
        ['d|C'],
      );
    });
  });
}
