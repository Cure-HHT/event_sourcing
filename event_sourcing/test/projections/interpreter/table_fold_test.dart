// event_sourcing/test/projections/interpreter/table_fold_test.dart
//
// Verifies: EVS-PRD-materializer/A — TableFold provides the fold engine
//   that the library's materializer uses for TableProjectionSpec views.
// Verifies: EVS-PRD-materializer/B — upsert-on-insert, delete-on-remove,
//   and silent-no-op-on-missing-row are deterministic; tests confirm each.
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

final _spec = TableProjectionSpec(
  viewName: 'role_permission_grants',
  interest: SubscriptionFilter(
    eventTypes: const {'permission_granted', 'permission_revoked'},
  ),
  insertEventTypes: const {'permission_granted'},
  removeEventTypes: const {'permission_revoked'},
  rowKey: const CompositeKey(['data.role', 'data.permission', 'data.scope']),
  rowData: const WholePayload(),
);

// NOTE: In this codebase, StoredEvent.data IS the event payload. The
// CompositeKey path 'data.role' resolves to event.data['role'].
// WholePayload() returns event.data verbatim.

void main() {
  group('TableFold.applyEvent', () {
    test('insert event upserts a row keyed by composite key', () async {
      final backend = await _backend();
      await backend.transaction((txn) async {
        await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
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

    test('insert stamps aggregateId + sequence into the persisted row', () async {
      // Regression: TableFold previously persisted only the extracted
      // rowData, omitting the `aggregateId` and `sequence` fields that
      // AggregateFold stamps. That asymmetry broke every consumer that
      // followed the documented view-row contract (ViewBuilder's
      // `aggregateIdOf` / "the aggregate id is stamped into the raw
      // view-row map under the 'aggregateId' key") and zeroed the
      // sequence used by subscribe's snapshot replay.
      final backend = await _backend();
      await backend.transaction((txn) async {
        await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
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
    });

    test('insert change.newValue carries aggregateId + sequence', () async {
      // Live Delta consumers receive change.newValue over the wire; it must
      // carry the same stamped fields as the persisted row so a mapper that
      // reads `row['aggregateId']` works for both snapshot replay and live
      // deltas.
      final backend = await _backend();
      AggregateFoldChange? change;
      await backend.transaction((txn) async {
        change = await TableFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          event: _ev('permission_granted', {
            'role': 'editor',
            'permission': 'notes.edit',
            'scope': 'west',
          }),
        );
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
        AggregateFoldChange? result;
        await backend.transaction((txn) async {
          result = await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            event: _ev('permission_revoked', {
              'role': 'admin',
              'permission': 'users.invite',
              'scope': 'site',
            }),
          );
        });
        // Row never existed — no tombstone should be emitted.
        expect(result, isNull);
      },
    );
  });
}
