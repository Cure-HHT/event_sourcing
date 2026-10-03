// test/permissions/role_permission_grants_spec_test.dart
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

import 'test_support/sembast_event_store_harness.dart';

void main() {
  group('rolePermissionGrantsSpec', () {
    late EventStore eventStore;

    setUp(() async {
      eventStore = await buildInMemoryEventStore();
    });

    tearDown(() async {
      await eventStore.close();
    });

    // Verifies: EVS-PRD-permissions-as-events/A
    test(
      'permission_granted upserts view row with role/permissionName',
      () async {
        const payload = PermissionGrantedPayload(
          role: 'admin',
          permissionName: 'user.invite',
        );
        await eventStore.append(
          entryType: kRolePermissionGrantEntryType,
          aggregateType: 'role_permission_grant',
          aggregateId: 'admin:user.invite',
          eventType: 'permission_granted',
          data: payload.toJson(),
          initiator: const AutomationInitiator(service: 'test'),
        );

        // Row is stored under the aggregateId key.
        final row = await eventStore.reader.transaction(
          (txn) => eventStore.reader.readViewRowInTxn(
            txn,
            kRolePermissionGrantsView,
            'admin:user.invite',
          ),
        );
        expect(row.row, isA<SettledRow>());
        expect(row.row.dataOrNull!['role'], 'admin');
        expect(row.row.dataOrNull!['permissionName'], 'user.invite');
      },
    );

    // Verifies: EVS-PRD-permissions-as-events/A
    test('permission_revoked removes the view row', () async {
      // First insert.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        aggregateType: 'role_permission_grant',
        aggregateId: 'admin:user.invite',
        eventType: 'permission_granted',
        data: const PermissionGrantedPayload(
          role: 'admin',
          permissionName: 'user.invite',
        ).toJson(),
        initiator: const AutomationInitiator(service: 'test'),
      );

      // Then revoke.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        aggregateType: 'role_permission_grant',
        aggregateId: 'admin:user.invite',
        eventType: 'permission_revoked',
        data: const <String, Object?>{
          'role': 'admin',
          'permissionName': 'user.invite',
        },
        initiator: const AutomationInitiator(service: 'test'),
      );

      final row = await eventStore.reader.transaction(
        (txn) => eventStore.reader.readViewRowInTxn(
          txn,
          kRolePermissionGrantsView,
          'admin:user.invite',
        ),
      );
      expect(row.row, isA<AbsentRow>());
    });

    test('events with a different aggregateType are ignored (no-op)', () async {
      // An event with the same eventType but wrong aggregateType should
      // not produce a view row (the spec's interest filter includes
      // aggregateTypes: {'role_permission_grant'}).
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        aggregateType: 'some_other_aggregate_type',
        aggregateId: 'unrelated:aggregate',
        eventType: 'permission_granted',
        data: const <String, Object?>{
          'role': 'admin',
          'permissionName': 'user.invite',
        },
        initiator: const AutomationInitiator(service: 'test'),
      );

      final rows = await eventStore.reader.findViewRows(
        kRolePermissionGrantsView,
      );
      expect(rows.rows, isEmpty);
    });

    test('re-inserting the same aggregateId is idempotent (upsert)', () async {
      const aggregateId = 'admin:user.invite';
      final payload = const PermissionGrantedPayload(
        role: 'admin',
        permissionName: 'user.invite',
      ).toJson();

      // Insert twice — second append goes to the same aggregateId.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        aggregateType: 'role_permission_grant',
        aggregateId: aggregateId,
        eventType: 'permission_granted',
        data: payload,
        initiator: const AutomationInitiator(service: 'test'),
      );
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        aggregateType: 'role_permission_grant',
        aggregateId: aggregateId,
        eventType: 'permission_granted',
        data: payload,
        initiator: const AutomationInitiator(service: 'test'),
      );

      final rows = await eventStore.reader.findViewRows(
        kRolePermissionGrantsView,
      );
      // Exactly one row — upsert overwrote, no duplicates.
      expect(rows.rows.where((r) => r['role'] == 'admin').length, 1);

      final row = await eventStore.reader.transaction(
        (txn) => eventStore.reader.readViewRowInTxn(
          txn,
          kRolePermissionGrantsView,
          aggregateId,
        ),
      );
      expect(row.row, isA<SettledRow>());
      expect(row.row.dataOrNull!['permissionName'], 'user.invite');
    });
  });
}
