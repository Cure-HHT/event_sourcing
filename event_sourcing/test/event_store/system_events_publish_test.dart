// Verifies that all event-appending paths notify the subscription bus, not
// only the public EventStore.append path.
//
// Before the _runInTxnWithPublish fix, appendInTxn callers inside
// clearSecurityContext, applyRetentionPolicy, _emitDuplicateReceivedInTxn,
// _ingestOneInTxn, and DestinationRegistry._emitDestinationAuditInTxn did NOT
// publish to the subscription engine. A subscriber using
// SubscriptionFilter(includeSystemEvents: true) would silently miss all of
// those events.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _installUUID = 'aaaa0001-0000-4000-8000-000000000001';
const _source = Source(
  hopId: 'test',
  identifier: _installUUID,
  softwareVersion: '0.0.0-test',
);

EntryTypeDefinition _simpleDef(String id) =>
    EntryTypeDefinition(id: id, registeredVersion: 1, name: id);

Future<
  ({EventStore store, SembastBackend backend, SembastSecurityContextStore sc})
>
_setup({DateTime? now}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'syspub-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    registry.register(defn);
  }
  registry.register(_simpleDef('test_event'));
  final sc = SembastSecurityContextStore(backend: backend);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: _source,
    securityContexts: sc,
    clock: now == null ? null : () => now,
  );
  return (store: store, backend: backend, sc: sc);
}

void main() {
  group('Subscription bus receives system-event appends', () {
    // Verifies that clearSecurityContext publishes the
    // security_context_redacted event to the subscription engine.
    test(
      'clearSecurityContext publishes security_context_redacted to subscribers',
      () async {
        final fx = await _setup();
        final store = fx.store;

        final received = <StoredEvent>[];
        final sub = store
            .subscribe<StoredEvent>(
              const SubscriptionFilter(includeSystemEvents: true),
              const Events(),
            )
            .listen((u) {
              if (u is Delta<StoredEvent>) received.add(u.value);
            });

        // Append a user event with security details so a context row exists.
        final ev = await store.append(
          entryType: 'test_event',
          entryTypeVersion: 1,
          aggregateId: 'a1',
          aggregateType: 'Test',
          eventType: 'created',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u1'),
          security: const SecurityDetails(ipAddress: '1.2.3.4'),
        );

        // Clear the context; this appends a security_context_redacted event.
        await store.clearSecurityContext(
          ev!.eventId,
          reason: 'GDPR',
          redactedBy: const UserInitiator('admin'),
        );

        await Future<void>.delayed(const Duration(milliseconds: 50));

        final systemReceived = received
            .where((e) => e.entryType == kSecurityContextRedactedEntryType)
            .toList();
        expect(
          systemReceived,
          hasLength(1),
          reason:
              'clearSecurityContext must publish security_context_redacted '
              'to the subscription bus',
        );
        expect(systemReceived.first.data['subject_event_id'], ev.eventId);

        await sub.cancel();
        await store.close();
      },
    );

    // Verifies that applyRetentionPolicy publishes all three audit events
    // (compact, purge, per-sweep) to the subscription engine.
    test(
      'applyRetentionPolicy publishes retention audit events to subscribers',
      () async {
        final fixtureNow = DateTime.utc(2030, 1, 1);
        final fx = await _setup(now: fixtureNow);
        final store = fx.store;
        final sc = fx.sc;
        final backend = fx.backend;

        final received = <StoredEvent>[];
        final sub = store
            .subscribe<StoredEvent>(
              const SubscriptionFilter(includeSystemEvents: true),
              const Events(),
            )
            .listen((u) {
              if (u is Delta<StoredEvent>) received.add(u.value);
            });

        // Seed a compact-window row.
        final evCompact = await store.append(
          entryType: 'test_event',
          entryTypeVersion: 1,
          aggregateId: 'c1',
          aggregateType: 'Test',
          eventType: 'created',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u1'),
          security: const SecurityDetails(ipAddress: '10.0.0.1'),
        );
        await backend.transaction((txn) async {
          await sc.upsertInTxn(
            txn,
            EventSecurityContext(
              eventId: evCompact!.eventId,
              recordedAt: DateTime.utc(2029, 1, 1), // within compact window
              ipAddress: '10.0.0.1',
            ),
          );
        });

        // Seed a purge-window row.
        final evPurge = await store.append(
          entryType: 'test_event',
          entryTypeVersion: 1,
          aggregateId: 'p1',
          aggregateType: 'Test',
          eventType: 'created',
          data: const <String, Object?>{},
          initiator: const UserInitiator('u1'),
          security: const SecurityDetails(ipAddress: '10.0.0.2'),
        );
        await backend.transaction((txn) async {
          await sc.upsertInTxn(
            txn,
            EventSecurityContext(
              eventId: evPurge!.eventId,
              recordedAt: DateTime.utc(2020, 1, 1), // past purge cutoff
              ipAddress: '10.0.0.2',
            ),
          );
        });

        // Clear what the user-event appends emitted (they fire before subscription
        // filter is set in place). Reset the list.
        received.clear();

        await store.applyRetentionPolicy();
        await Future<void>.delayed(const Duration(milliseconds: 50));

        final compacted = received
            .where((e) => e.entryType == kSecurityContextCompactedEntryType)
            .toList();
        final purged = received
            .where((e) => e.entryType == kSecurityContextPurgedEntryType)
            .toList();
        final policyApplied = received
            .where((e) => e.entryType == kRetentionPolicyAppliedEntryType)
            .toList();

        expect(
          compacted,
          hasLength(1),
          reason: 'compact audit must be published to the subscription bus',
        );
        expect(
          purged,
          hasLength(1),
          reason: 'purge audit must be published to the subscription bus',
        );
        expect(
          policyApplied,
          hasLength(1),
          reason:
              'retention_policy_applied audit must be published to the '
              'subscription bus',
        );

        await sub.cancel();
        await store.close();
      },
    );
  });
}
