// test/permissions/role_permission_grants_spec_test.dart
// Verifies rolePermissionGrantsSpec: insert, remove, no-op for unrelated
// events, and idempotency (re-insert same row).
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

    test(
      'permission_granted upserts view row with role/permissionName/scope',
      () async {
        const payload = PermissionGrantedPayload(
          role: 'admin',
          permissionName: 'user.invite',
          scope: ScopeClass.global,
        );
        await eventStore.append(
          entryType: kRolePermissionGrantEntryType,
          entryTypeVersion: 1,
          aggregateType: 'role_permission_grant',
          aggregateId: 'admin:user.invite',
          eventType: 'permission_granted',
          data: payload.toJson(),
          initiator: const AutomationInitiator(service: 'test'),
        );

        // Row is stored under the aggregateId key.
        final row = await eventStore.backend.transaction(
          (txn) => eventStore.backend.readViewRowInTxn(
            txn,
            kRolePermissionGrantsView,
            'admin:user.invite',
          ),
        );
        expect(row, isNotNull);
        expect(row!['role'], 'admin');
        expect(row['permissionName'], 'user.invite');
        expect(row['scope'], 'global');
      },
    );

    test('permission_revoked removes the view row', () async {
      // First insert.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        entryTypeVersion: 1,
        aggregateType: 'role_permission_grant',
        aggregateId: 'admin:user.invite',
        eventType: 'permission_granted',
        data: const PermissionGrantedPayload(
          role: 'admin',
          permissionName: 'user.invite',
          scope: ScopeClass.global,
        ).toJson(),
        initiator: const AutomationInitiator(service: 'test'),
      );

      // Then revoke.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        entryTypeVersion: 1,
        aggregateType: 'role_permission_grant',
        aggregateId: 'admin:user.invite',
        eventType: 'permission_revoked',
        data: const <String, Object?>{
          'role': 'admin',
          'permissionName': 'user.invite',
        },
        initiator: const AutomationInitiator(service: 'test'),
      );

      final row = await eventStore.backend.transaction(
        (txn) => eventStore.backend.readViewRowInTxn(
          txn,
          kRolePermissionGrantsView,
          'admin:user.invite',
        ),
      );
      expect(row, isNull);
    });

    test('events with a different aggregateType are ignored (no-op)', () async {
      // An event with the same eventType but wrong aggregateType should
      // not produce a view row (the spec's interest filter includes
      // aggregateTypes: {'role_permission_grant'}).
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        entryTypeVersion: 1,
        aggregateType: 'some_other_aggregate_type',
        aggregateId: 'unrelated:aggregate',
        eventType: 'permission_granted',
        data: const <String, Object?>{
          'role': 'admin',
          'permissionName': 'user.invite',
          'scope': 'global',
        },
        initiator: const AutomationInitiator(service: 'test'),
      );

      final rows = await eventStore.backend.findViewRows(
        kRolePermissionGrantsView,
      );
      expect(rows, isEmpty);
    });

    test('re-inserting the same aggregateId is idempotent (upsert)', () async {
      const aggregateId = 'admin:user.invite';
      final payload = const PermissionGrantedPayload(
        role: 'admin',
        permissionName: 'user.invite',
        scope: ScopeClass.global,
      ).toJson();

      // Insert twice — second append goes to the same aggregateId.
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        entryTypeVersion: 1,
        aggregateType: 'role_permission_grant',
        aggregateId: aggregateId,
        eventType: 'permission_granted',
        data: payload,
        initiator: const AutomationInitiator(service: 'test'),
      );
      await eventStore.append(
        entryType: kRolePermissionGrantEntryType,
        entryTypeVersion: 1,
        aggregateType: 'role_permission_grant',
        aggregateId: aggregateId,
        eventType: 'permission_granted',
        data: payload,
        initiator: const AutomationInitiator(service: 'test'),
      );

      final rows = await eventStore.backend.findViewRows(
        kRolePermissionGrantsView,
      );
      // Exactly one row — upsert overwrote, no duplicates.
      expect(rows.where((r) => r['role'] == 'admin').length, 1);

      final row = await eventStore.backend.transaction(
        (txn) => eventStore.backend.readViewRowInTxn(
          txn,
          kRolePermissionGrantsView,
          aggregateId,
        ),
      );
      expect(row, isNotNull);
      expect(row!['permissionName'], 'user.invite');
    });
  });
}
