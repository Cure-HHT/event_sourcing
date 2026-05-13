// Verifies: EVS-PRD-library-charter/A
// Verifies: EVS-PRD-event-log/A/B/C/D
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

class _Fixture {
  _Fixture({
    required this.eventStore,
    required this.backend,
    required this.securityContexts,
    required this.entryTypes,
    required this.syncCalls,
  });

  final EventStore eventStore;
  final SembastBackend backend;
  final SembastSecurityContextStore securityContexts;
  final EntryTypeRegistry entryTypes;
  final List<DateTime> syncCalls;
}

// Build an AggregateProjectionSpec that matches [entryTypeIds]. Used to
// populate 'toy_view' for tests that verify view-row production.
AggregateProjectionSpec _toyViewSpec(List<String> entryTypeIds) =>
    AggregateProjectionSpec(
      viewName: 'toy_view',
      interest: SubscriptionFilter(entryTypes: entryTypeIds),
      tombstoneEventTypes: const <String>{},
    );

Future<_Fixture> _setup({
  List<EntryTypeDefinition>? defs,
  DateTime? now,

  /// When true, the toy_view ProjectionSpec is registered so appended events
  /// produce view rows. When false, no projection is registered (simulates
  /// an entry type that should not produce rows).
  bool registerProjection = true,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'es-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  // Auto-register every reserved system entry type.
  for (final defn in kSystemEntryTypes) {
    registry.register(defn);
  }
  final effectiveDefs = defs ?? [_simpleDef('epistaxis_event')];
  for (final def in effectiveDefs) {
    registry.register(def);
  }
  final securityContexts = SembastSecurityContextStore(backend: backend);
  final syncCalls = <DateTime>[];

  ProjectionRegistry? projections;
  if (registerProjection) {
    final materializableIds = effectiveDefs
        .where((d) => d.materialize)
        .map((d) => d.id)
        .toList();
    if (materializableIds.isNotEmpty) {
      projections = ProjectionRegistry()
        ..register(_toyViewSpec(materializableIds));
    }
  }

  final eventStore = await EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'mobile-device',
      identifier: 'device-1',
      softwareVersion: 'clinical_diary@1.0.0',
    ),
    securityContexts: securityContexts,
    projections: projections,
    syncCycleTrigger: () async {
      syncCalls.add(DateTime.now());
    },
    clock: now == null ? null : () => now,
  );
  return _Fixture(
    eventStore: eventStore,
    backend: backend,
    securityContexts: securityContexts,
    entryTypes: registry,
    syncCalls: syncCalls,
  );
}

EntryTypeDefinition _simpleDef(String id) =>
    EntryTypeDefinition(id: id, registeredVersion: 1, name: id);

void main() {
  group('EventStore.append', () {
    // initiator / flowToken round-tripped.
    test('returns StoredEvent with initiator + flowToken', () async {
      final fx = await _setup();
      final ev = await fx.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {
          'answers': {'severity': 'mild'},
        },
        initiator: const UserInitiator('u1'),
        flowToken: 'flow:abc',
      );
      expect(ev, isNotNull);
      expect(ev!.initiator, const UserInitiator('u1'));
      expect(ev.flowToken, 'flow:abc');
      await fx.backend.close();
    });

    test('append with security writes both rows', () async {
      final fx = await _setup();
      final ev = await fx.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {'answers': {}},
        initiator: const UserInitiator('u1'),
        security: const SecurityDetails(ipAddress: '203.0.113.7'),
      );
      final ctx = await fx.securityContexts.read(ev!.eventId);
      expect(ctx, isNotNull);
      expect(ctx!.ipAddress, '203.0.113.7');
      await fx.backend.close();
    });

    test('append without security writes only event row', () async {
      final fx = await _setup();
      final ev = await fx.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {'answers': {}},
        initiator: const UserInitiator('u1'),
      );
      expect(await fx.securityContexts.read(ev!.eventId), isNull);
      await fx.backend.close();
    });

    // interest filter produces no view row. In the declarative model this is
    // expressed by simply not registering a ProjectionSpec for that entry type.
    test('entry type not in any ProjectionSpec produces no view row', () async {
      final fx = await _setup(
        defs: [
          const EntryTypeDefinition(
            id: 'non_materialized',
            registeredVersion: 1,
            name: 'Non-Mat',
            materialize: false,
          ),
        ],
        registerProjection: false,
      );
      final ev = await fx.eventStore.append(
        entryType: 'non_materialized',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {'answers': {}},
        initiator: const UserInitiator('u1'),
      );
      expect(ev, isNotNull);
      final viewRow = await fx.backend.transaction(
        (txn) async => fx.backend.readViewRowInTxn(txn, 'toy_view', 'a'),
      );
      expect(viewRow, isNull);
      await fx.backend.close();
    });

    test('unregistered entryType throws ArgumentError before I/O', () async {
      final fx = await _setup();
      await expectLater(
        fx.eventStore.append(
          entryType: 'weather_report',
          aggregateId: 'a',
          aggregateType: 'SampleAggregate',
          eventType: 'finalized',
          data: const {'answers': {}},
          initiator: const UserInitiator('u1'),
        ),
        throwsArgumentError,
      );
      await fx.backend.close();
    });

    // it changes the hash.
    test('flow_token participates in event_hash', () async {
      final fxA = await _setup(now: DateTime.utc(2026, 4, 22));
      final evA = await fxA.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {
          'answers': {'x': 1},
        },
        initiator: const UserInitiator('u1'),
        flowToken: 'alpha',
      );
      final fxB = await _setup(now: DateTime.utc(2026, 4, 22));
      final evB = await fxB.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {
          'answers': {'x': 1},
        },
        initiator: const UserInitiator('u1'),
        flowToken: 'beta',
      );
      expect(evA!.eventHash, isNot(evB!.eventHash));
      await fxA.backend.close();
      await fxB.backend.close();
    });
  });

  group('EventStore.clearSecurityContext', () {
    // security_context_redacted event with the correct fields.
    test('deletes security row + emits redaction event', () async {
      final fx = await _setup();
      final ev = await fx.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {'answers': {}},
        initiator: const UserInitiator('u1'),
        security: const SecurityDetails(ipAddress: '1.2.3.4'),
      );
      await fx.eventStore.clearSecurityContext(
        ev!.eventId,
        reason: 'GDPR request',
        redactedBy: const UserInitiator('admin-1'),
      );
      expect(await fx.securityContexts.read(ev.eventId), isNull);

      final events = await fx.backend.findAllEvents();
      final redactionEvent = events.last;
      expect(redactionEvent.entryType, 'security_context_redacted');
      expect(redactionEvent.aggregateType, 'security_context');
      // UUID; the redacted subject's eventId moves into
      // data.subject_event_id.
      expect(redactionEvent.aggregateId, fx.eventStore.source.identifier);
      expect(redactionEvent.data['subject_event_id'], ev.eventId);
      expect(redactionEvent.initiator, const UserInitiator('admin-1'));
      expect(redactionEvent.data['reason'], 'GDPR request');
      await fx.backend.close();
    });

    test('missing eventId throws ArgumentError; no event emitted', () async {
      final fx = await _setup();
      await expectLater(
        fx.eventStore.clearSecurityContext(
          'nope',
          reason: 'oops',
          redactedBy: const UserInitiator('admin-1'),
        ),
        throwsArgumentError,
      );
      expect(await fx.backend.findAllEvents(), isEmpty);
      await fx.backend.close();
    });
  });

  group('EventStore.applyRetentionPolicy', () {
    // events. empty sweep DOES emit a per-sweep
    // retention_policy_applied audit. The audit is the timeline
    // record; the compact/purge events are gated on actual work.
    test('empty sweep emits per-sweep audit only', () async {
      final fx = await _setup(now: DateTime.utc(2030, 1, 1));
      final result = await fx.eventStore.applyRetentionPolicy();
      expect(result.compactedCount, 0);
      expect(result.purgedCount, 0);
      final events = await fx.backend.findAllEvents();
      // No compact / purge audit on a zero-effect sweep.
      expect(
        events.any((e) => e.entryType == kSecurityContextCompactedEntryType),
        isFalse,
      );
      expect(
        events.any((e) => e.entryType == kSecurityContextPurgedEntryType),
        isFalse,
      );
      // But the per-sweep retention_policy_applied audit fires
      // unconditionally.
      final perSweep = events
          .where((e) => e.entryType == kRetentionPolicyAppliedEntryType)
          .toList();
      expect(perSweep, hasLength(1));
      expect(perSweep.single.data['events_truncated'], 0);
      expect(perSweep.single.data['events_purged'], 0);
      await fx.backend.close();
    });

    // security_context_compacted event.
    test('compact sweep truncates IP and emits compacted event', () async {
      final fixtureNow = DateTime.utc(2030, 1, 1);
      final fx = await _setup(now: fixtureNow);
      // Write an event from 2020 (well past the 90-day full retention
      // window but within 90+365 so it is compacted, not purged).
      final ev = await fx.eventStore.append(
        entryType: 'epistaxis_event',
        aggregateId: 'a',
        aggregateType: 'SampleAggregate',
        eventType: 'finalized',
        data: const {'answers': {}},
        initiator: const UserInitiator('u1'),
        security: const SecurityDetails(ipAddress: '203.0.113.7'),
      );
      // Manually backdate the security row to ensure it's past
      // fullRetention.
      final backdated = EventSecurityContext(
        eventId: ev!.eventId,
        recordedAt: DateTime.utc(2029, 1, 1),
        ipAddress: '203.0.113.7',
      );
      await fx.backend.transaction((txn) async {
        await fx.securityContexts.upsertInTxn(txn, backdated);
      });

      final result = await fx.eventStore.applyRetentionPolicy();
      expect(result.compactedCount, 1);
      final events = await fx.backend.findAllEvents();
      final compacted = events.firstWhere(
        (e) => e.entryType == 'security_context_compacted',
      );
      expect(compacted.data['count'], 1);
      final ctx = await fx.securityContexts.read(ev.eventId);
      expect(ctx!.ipAddress, '203.0.113.0');
      await fx.backend.close();
    });
  });
}
