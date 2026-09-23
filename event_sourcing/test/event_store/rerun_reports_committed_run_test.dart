// Verifies: EVS-PRD-event-log/G
// when the storage layer runs a write more
//   than once before one run commits, the dispatcher's result and its
//   idempotency record, and ingestBatch's per-event outcomes, reflect only
//   the committed run: no event id, decision or outcome of the rolled-back
//   run is returned or recorded.
//
// RerunningSembastBackend runs every transaction body twice on the VM,
// rolling the first run back and committing the second, as Postgres does
// after a serialization conflict and sembast_web does after another tab
// commits first.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;
import 'package:uuid/uuid.dart';

import '../actions/fixtures/test_actions.dart'
    show AlwaysAllowPolicy, MultiEventAction, OptionalKeyAction;
import '../test_support/rerunning_sembast_backend.dart';

const _source = Source(
  hopId: 'test-server',
  identifier: 'rerun-instance-1',
  softwareVersion: 'event_sourcing_test@0.0.0',
);

EntryTypeRegistry _registry() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Action denial',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'greeting',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Greeting',
      ),
    );
}

Future<(EventStore, RerunningSembastBackend)> _openRerunningStore() async {
  final backend = await RerunningSembastBackend.openInMemory('rerun-report');
  backend.rerunEnabled = false;
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: _registry(),
    source: _source,
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
  backend
    ..rerunEnabled = true
    ..bodyRuns = 0;
  return (store, backend);
}

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'u-1',
    roles: const {'tester'},
    activeRole: 'tester',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.parse('2026-04-22T12:00:00Z'),
);

/// Denies the first authorization question it is asked and allows every
/// later one: the first run of a re-run dispatch body is denied, the
/// committed second run is permitted, as when a role is granted between the
/// two runs.
class _DenyFirstThenAllowPolicy extends AuthorizationPolicy {
  int calls = 0;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async {
    calls += 1;
    if (calls == 1) {
      return Deny(permission: permission, reason: DenyReason.notGranted);
    }
    return const Allow();
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async => EffectiveAuthorization.empty;
}

void main() {
  group('ActionDispatcher under a re-run dispatch transaction', () {
    test("returns and records only the committed run's event ids", () async {
      final (store, backend) = await _openRerunningStore();
      addTearDown(backend.close);
      final idempotency = InMemoryIdempotencyStore();
      final dispatcher = ActionDispatcher(
        registry: ActionRegistry()
          ..register(MultiEventAction())
          ..register(OptionalKeyAction()),
        authorization: const AlwaysAllowPolicy(),
        events: store,
        idempotency: idempotency,
      );

      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'multi_event',
          rawInput: <String, Object?>{'who': 'x'},
        ),
        _ctx(),
      );
      expect(backend.bodyRuns, greaterThanOrEqualTo(2));
      expect(result, isA<DispatchSuccess<Object?>>());
      final emitted = (result as DispatchSuccess<Object?>).emittedEventIds;
      final storedIds = (await backend.findAllEvents())
          .where((e) => e.entryType == 'greeting')
          .map((e) => e.eventId)
          .toList();
      expect(storedIds, hasLength(3));
      expect(emitted, storedIds);

      final keyed = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'optional_key',
          rawInput: <String, Object?>{'who': 'y'},
          idempotencyKey: 'key-1',
        ),
        _ctx(),
      );
      final keyedIds = (keyed as DispatchSuccess<Object?>).emittedEventIds;
      final allIds = (await backend.findAllEvents())
          .map((e) => e.eventId)
          .toSet();
      expect(keyedIds, hasLength(1));
      expect(allIds, containsAll(keyedIds));
      final entry = await idempotency.lookup(
        'optional_key',
        'u-1',
        'key-1',
        now: _ctx().requestStartedAt,
      );
      expect(entry, isNotNull);
      expect(entry!.emittedEventIds, keyedIds);
    });

    test(
      'a denial reached only by the rolled-back run is not returned',
      () async {
        final (store, backend) = await _openRerunningStore();
        addTearDown(backend.close);
        final policy = _DenyFirstThenAllowPolicy();
        final dispatcher = ActionDispatcher(
          registry: ActionRegistry()..register(MultiEventAction()),
          authorization: policy,
          events: store,
          idempotency: InMemoryIdempotencyStore(),
        );

        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'multi_event',
            rawInput: <String, Object?>{'who': 'x'},
          ),
          _ctx(),
        );
        expect(policy.calls, 2, reason: 'one authorization per body run');
        final stored = await backend.findAllEvents();
        expect(
          stored.where((e) => e.entryType == 'action_denial'),
          isEmpty,
          reason: 'the committed run was permitted and recorded no denial',
        );
        expect(result, isA<DispatchSuccess<Object?>>());
        expect(
          (result as DispatchSuccess<Object?>).emittedEventIds,
          stored.where((e) => e.entryType == 'greeting').map((e) => e.eventId),
        );
      },
    );
  });

  group('EventStore.ingestBatch under a re-run transaction', () {
    test('returns one outcome per subject event', () async {
      final origDb = await newDatabaseFactoryMemory().openDatabase(
        'rerun-origin-${DateTime.now().microsecondsSinceEpoch}.db',
      );
      final origBackend = SembastBackend(database: origDb);
      addTearDown(origBackend.close);
      final origin = await EventStore.openForTest(
        storage: origBackend,
        entryTypes: _registry(),
        source: const Source(
          hopId: 'mobile-device',
          identifier: 'device-1',
          softwareVersion: 'my_app@1.0.0',
        ),
        securityContexts: SembastSecurityContextStore(backend: origBackend),
      );
      final subjects = <StoredEvent>[
        for (final who in ['a', 'b', 'c'])
          (await origin.append(
            entryType: 'greeting',
            aggregateId: 'agg-$who',
            aggregateType: 'greeting',
            eventType: 'hello.said',
            data: <String, Object?>{'who': who},
            initiator: const UserInitiator('u1'),
          ))!,
      ];
      final envelope = BatchEnvelope(
        batchFormatVersion: '2',
        batchId: const Uuid().v4(),
        senderHop: 'mobile-device',
        senderIdentifier: 'device-1',
        senderSoftwareVersion: 'my_app@1.0.0',
        sentAt: DateTime.now().toUtc(),
        events: subjects
            .map((e) => Map<String, Object?>.from(e.toMap()))
            .toList(),
      );

      final (store, backend) = await _openRerunningStore();
      addTearDown(backend.close);
      final result = await store.ingestBatch(
        envelope.encode(),
        wireFormat: BatchEnvelope.wireFormat,
      );

      expect(backend.bodyRuns, 2, reason: 'the ingest body must be re-run');
      expect(result.events, hasLength(subjects.length));
      expect(
        result.events.map((o) => o.eventId),
        subjects.map((e) => e.eventId),
      );
      for (final outcome in result.events) {
        expect(outcome.outcome, IngestOutcome.ingested);
      }
    });
  });
}
