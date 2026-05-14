// Verifies: EVS-PRD-action-dispatch/A (dispatcher accepts principal-submitted actions)
// Verifies: EVS-PRD-action-dispatch/B (stages 1–10 in order: lookup, invocation_id, parse, idempotency, validate, authorize, execute, persist, record, return)
// Verifies: EVS-PRD-action-dispatch/C (every dispatched action produces a recorded denial event or DispatchSuccess with emittedEventIds)
// Verifies: EVS-PRD-action-dispatch/D (idempotency cache hit short-circuits; DispatchIdempotencyHit returned; no new events appended)
// Uses flutter_test (not package:test) because EventStore depends on
// Sembast, which requires the Flutter test binding to run in this package.
// All other tests in event_sourcing/ that touch EventStore use flutter_test
// for the same reason (see event_store_test.dart, append_versioning_test.dart,
// etc.).

import 'package:event_sourcing/event_sourcing.dart';
// AggregateIdKey and WholePayload are not re-exported from the barrel.
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/projections/primitives/row_key.dart';
import 'package:flutter_test/flutter_test.dart';

import 'fixtures/test_actions.dart'
    show
        AlwaysAllowPolicy,
        AlwaysDenyNotGrantedPolicy,
        BadExecuteAction,
        BadParseAction,
        BadValidateAction,
        HelloAction,
        MultiEventAction,
        OptionalKeyAction,
        RecordingAllowPolicy,
        RequiredKeyAction,
        ScopedPatientEditAction,
        TwoPermissionAction;
import 'test_support/event_store_helper.dart' show bootstrapTestEventStore;

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'u-1',
    roles: const {'tester'},
    activeRole: 'tester',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.parse('2026-04-22T12:00:00Z'),
);

void main() {
  late ActionRegistry registry;
  late EventStore eventStore;
  late InMemoryIdempotencyStore idempotency;
  late ActionDispatcher dispatcher;

  setUp(() async {
    registry = ActionRegistry()..register(HelloAction());
    eventStore = await bootstrapTestEventStore();
    idempotency = InMemoryIdempotencyStore();
    dispatcher = ActionDispatcher(
      registry: registry,
      authorization: const DenyAllAuthorizationPolicy.forTests(),
      events: eventStore,
      idempotency: idempotency,
    );
  });

  group('Stage 1 — lookup', () {
    test('unknown action returns DispatchUnknownAction', () async {
      final r = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'nope',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      expect(r, isA<DispatchUnknownAction<Object?>>());
      expect((r as DispatchUnknownAction<Object?>).requestedName, 'nope');
    });

    test('unknown action emits unknown_action denial event', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'nope',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'unknown_action')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.first.data['requested_name'], 'nope');
    });
  });

  group('Stage 2 — invocation_id stamping', () {
    test('every emitted event has action_invocation_id metadata', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'nope',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denial = allEvents.firstWhere(
        (e) => e.eventType == 'unknown_action',
      );
      expect(denial.metadata['action_invocation_id'], isNotNull);
      expect(denial.metadata['action_invocation_id'], isA<String>());
    });

    test('invocation_id is unique per call', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'nope',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'nope',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final ids = allEvents
          .where((e) => e.eventType == 'unknown_action')
          .map((e) => e.metadata['action_invocation_id'] as String)
          .toSet();
      expect(ids, hasLength(2));
    });
  });

  group('Stage 3 — parse', () {
    setUp(() {
      registry.register(BadParseAction());
    });

    test('parseInput failure returns DispatchParseDenied', () async {
      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_parse',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchParseDenied<Object?>>());
    });

    test('parseInput failure emits parse_denied event', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_parse',
          rawInput: <String, Object?>{},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'parse_denied')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.first.data['action_name'], 'bad_parse');
    });
  });

  group('Stage 4 — idempotency check', () {
    setUp(() {
      registry.register(RequiredKeyAction());
    });

    test(
      'idempotency.required without key returns DispatchParseDenied',
      () async {
        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{},
            // idempotencyKey intentionally omitted
          ),
          _ctx(),
        );
        expect(result, isA<DispatchParseDenied<Object?>>());
        final error = (result as DispatchParseDenied<Object?>).error;
        expect(error.toString(), contains('idempotency'));
      },
    );

    test(
      'missing required key emits parse_denied before parseInput runs',
      () async {
        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{},
          ),
          _ctx(),
        );
        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'parse_denied')
            .toList();
        expect(denials, hasLength(1));
        // RequiredKeyAction inherits HelloAction.parseInput which throws a
        // TypeError when 'who' is missing. MissingIdempotencyKeyError must
        // fire first, so the error_class must be MissingIdempotencyKeyError.
        expect(denials.first.data['error_class'], 'MissingIdempotencyKeyError');
      },
    );

    test(
      'idempotency hit short-circuits and returns DispatchIdempotencyHit',
      () async {
        // Pre-populate the idempotency store for (requires_key, u-1, k1).
        await idempotency.record(
          actionName: 'requires_key',
          principalId: 'u-1',
          key: 'k1',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['prior-event-id-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
        );

        final eventCountBefore =
            (await eventStore.backend.findAllEvents()).length;

        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{'who': 'tester'},
            idempotencyKey: 'k1',
          ),
          _ctx(),
        );

        expect(result, isA<DispatchIdempotencyHit<Object?>>());
        final hit = result as DispatchIdempotencyHit<Object?>;
        expect(hit.priorEmittedEventIds, contains('prior-event-id-1'));

        // No new events must have been appended.
        final eventCountAfter =
            (await eventStore.backend.findAllEvents()).length;
        expect(eventCountAfter, equals(eventCountBefore));
      },
    );

    test(
      'idempotency.none ignores supplied key — no store record created',
      () async {
        // HelloAction has Idempotency.none. Dispatch with a key; Stage 6
        // (authorize) now runs and DenyAllAuthorizationPolicy denies it —
        // the important thing is no idempotency record is created.
        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
            idempotencyKey: 'should-be-ignored',
          ),
          _ctx(),
        );

        final entry = await idempotency.lookup(
          'hello',
          'u-1',
          'should-be-ignored',
        );
        expect(entry, isNull);
      },
    );
  });

  group('Stage 5 — validate', () {
    setUp(() {
      registry.register(BadValidateAction());
    });

    test('validate failure returns DispatchValidationDenied', () async {
      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_validate',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchValidationDenied<Object?>>());
    });

    test('validate failure emits validation_denied event', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_validate',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'validation_denied')
          .toList();
      expect(denials, hasLength(1));
      expect(
        denials.first.data['error_class'],
        StateError('').runtimeType.toString(),
      );
    });
  });

  // Helper: dispatcher that always allows authorization.
  ActionDispatcher makeAllowDispatcher(
    ActionRegistry reg,
    EventStore es,
    InMemoryIdempotencyStore idm,
  ) => ActionDispatcher(
    registry: reg,
    authorization: const AlwaysAllowPolicy(),
    events: es,
    idempotency: idm,
  );

  group('Stage 6 — authorize', () {
    test('authz denial returns DispatchAuthorizationDenied', () async {
      // Default dispatcher uses DenyAllAuthorizationPolicy.forTests().
      // HelloAction declares permission test.hello which will be denied.
      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchAuthorizationDenied<Object?>>());
      final denied = result as DispatchAuthorizationDenied<Object?>;
      expect(denied.permission.name, 'test.hello');
    });

    test('authz denial emits authorization_denied event', () async {
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'authorization_denied')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.first.data['permission_denied'], 'test.hello');
      // DenyAllAuthorizationPolicy always denies with DenyReason.notGranted.
      expect(denials.first.data['deny_reason'], 'notGranted');
    });

    test(
      'first denial short-circuits — only one authorization_denied event emitted',
      () async {
        registry.register(TwoPermissionAction());
        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'two_perms',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'authorization_denied')
            .toList();
        expect(denials, hasLength(1));
      },
    );

    test(
      'all-Allow falls through all stages and returns DispatchSuccess',
      () async {
        final allowDispatcher = ActionDispatcher(
          registry: registry,
          authorization: const AlwaysAllowPolicy(),
          events: eventStore,
          idempotency: idempotency,
        );
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());
      },
    );
  });

  group('Stage 6 — authorize scope resolution', () {
    // Verifies: EVS-PRD-action-dispatch/B (dispatcher resolves scope via
    // Action.scopeFor for scoped permissions; class-mismatch / null /
    // TotalWildcardScope deny with DenyReason.scopeUnresolvable)
    // Verifies: EVS-PRD-action-dispatch/C (authorization_denied stamps
    // the resolved scope onto data['scope'] when non-null)

    test(
      'scopeFor returns class-matching BoundScope → Allow falls through',
      () async {
        registry.register(ScopedPatientEditAction());
        final policy = RecordingAllowPolicy();
        final allowDispatcher = ActionDispatcher(
          registry: registry,
          authorization: policy,
          events: eventStore,
          idempotency: idempotency,
        );

        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-123'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        // Policy received the resolved scope.
        expect(policy.calls, hasLength(1));
        final (_, scope) = policy.calls.single;
        expect(scope, isA<BoundScope>());
        expect((scope! as BoundScope).class_, 'patient');
        expect((scope as BoundScope).value, 'p-123');
      },
    );

    test('scopeFor returns null → Deny(scopeUnresolvable)', () async {
      registry.register(const ScopedPatientEditAction(forceNull: true));
      final allowDispatcher = makeAllowDispatcher(
        registry,
        eventStore,
        idempotency,
      );

      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'patient_edit',
          rawInput: <String, Object?>{'patient_id': 'p-123'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchAuthorizationDenied<Object?>>());

      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'authorization_denied')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.first.data['deny_reason'], 'scopeUnresolvable');
      // No scope to stamp when resolution returned null.
      expect(denials.first.data.containsKey('scope'), isFalse);
    });

    test(
      'scopeFor returns class-mismatched BoundScope → Deny(scopeUnresolvable)',
      () async {
        registry.register(
          ScopedPatientEditAction(
            scopeOverride: const BoundScope(
              class_: 'patient_group',
              value: 'pg-7',
            ),
          ),
        );
        final allowDispatcher = makeAllowDispatcher(
          registry,
          eventStore,
          idempotency,
        );

        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-123'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchAuthorizationDenied<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'authorization_denied')
            .toList();
        expect(denials, hasLength(1));
        expect(denials.first.data['deny_reason'], 'scopeUnresolvable');
        // The class-mismatched scope IS stamped (it's a real ScopeValue,
        // just not one this permission's scopeClass accepts).
        final scope = denials.first.data['scope'] as Map<String, Object?>;
        expect(scope['class'], 'patient_group');
        expect(scope['value'], 'pg-7');
      },
    );

    test(
      'scopeFor returns TotalWildcardScope → Deny(scopeUnresolvable)',
      () async {
        registry.register(
          ScopedPatientEditAction(scopeOverride: const TotalWildcardScope()),
        );
        final allowDispatcher = makeAllowDispatcher(
          registry,
          eventStore,
          idempotency,
        );

        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-123'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchAuthorizationDenied<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'authorization_denied')
            .toList();
        expect(denials, hasLength(1));
        expect(denials.first.data['deny_reason'], 'scopeUnresolvable');
        // TotalWildcardScope IS stamped (toJson returns
        // `{"wildcard_class": true}`) — it's a real ScopeValue, just one
        // the dispatcher refuses because it carries no class_ to match.
        final scope = denials.first.data['scope'] as Map<String, Object?>;
        expect(scope['wildcard_class'], isTrue);
      },
    );

    test(
      'class-matched scope is stamped onto policy-deny authorization_denied event',
      () async {
        registry.register(ScopedPatientEditAction());
        final denyDispatcher = ActionDispatcher(
          registry: registry,
          authorization: const AlwaysDenyNotGrantedPolicy(),
          events: eventStore,
          idempotency: idempotency,
        );

        final result = await denyDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'patient_edit',
            rawInput: <String, Object?>{'patient_id': 'p-42'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchAuthorizationDenied<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'authorization_denied')
            .toList();
        expect(denials, hasLength(1));
        expect(denials.first.data['permission_denied'], 'patient.edit');
        expect(denials.first.data['deny_reason'], 'notGranted');
        final scope = denials.first.data['scope'] as Map<String, Object?>;
        expect(scope['class'], 'patient');
        expect(scope['value'], 'p-42');
      },
    );
  });

  group('Stage 7 — execute', () {
    late ActionDispatcher allowDispatcher;

    setUp(() {
      registry.register(BadExecuteAction());
      allowDispatcher = makeAllowDispatcher(registry, eventStore, idempotency);
    });

    test('execute throw returns DispatchExecutionFailed', () async {
      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_execute',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchExecutionFailed<Object?>>());
    });

    test('execute throw emits execution_failed denial event', () async {
      await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_execute',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'execution_failed')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.first.data['action_name'], 'bad_execute');
    });

    test(
      'execute success persists events and returns DispatchSuccess',
      () async {
        // Stage 8 persists the event; Stage 9-10 complete and return success.
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final greetings = allEvents
            .where((e) => e.eventType == 'hello.said')
            .toList();
        expect(greetings, hasLength(1));
        expect(greetings.first.data['who'], 'world');
      },
    );
  });

  group('Stage 8 — atomic persist', () {
    late ActionDispatcher allowDispatcher;

    setUp(() {
      registry.register(MultiEventAction());
      allowDispatcher = makeAllowDispatcher(registry, eventStore, idempotency);
    });

    test(
      'each emitted event has action_invocation_id and action_name in metadata',
      () async {
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'stamp-test'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final greetings = allEvents
            .where((e) => e.eventType == 'hello.said')
            .toList();
        expect(greetings, hasLength(1));

        final meta = greetings.first.metadata;
        expect(meta['action_invocation_id'], isA<String>());
        expect(
          meta['action_invocation_id'] as String,
          matches(
            RegExp(
              r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
            ),
          ),
        );
        expect(meta['action_name'], 'hello');
      },
    );

    test(
      'multi-event execute persists all atomically with same action_invocation_id',
      () async {
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'multi_event',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        final allEvents = await eventStore.backend.findAllEvents();
        final greetings = allEvents
            .where((e) => e.eventType == 'hello.said')
            .toList();
        expect(greetings, hasLength(3));

        // All three events must carry the same action_invocation_id.
        final invIds = greetings
            .map((e) => e.metadata['action_invocation_id'] as String)
            .toSet();
        expect(invIds, hasLength(1));

        // All three must have action_name stamped.
        for (final e in greetings) {
          expect(e.metadata['action_name'], 'multi_event');
        }
      },
    );

    // TODO(CUR-1192): Add fault-injection test for Stage 8 rollback-on-persist-failure.
    //   This requires a seam in StorageBackend to inject mid-transaction failures.
    //   Without such a seam the rollback semantic is verified only by code inspection
    //   and the Sembast transaction contract. Track as follow-up.
  });

  // Regression coverage for the EventStore.appendInTxn projection-interpreter
  // gap surfaced in CUR-1331 Task 25: ActionDispatcher Stage 8 appends action
  // events via appendInTxn, and prior to the fix the projection interpreter
  // was NOT invoked, so views were stale until something else (e.g., a
  // subscriber bridge) re-fed the event through the public append. The fix
  // moves _interpreter.applyEvent inside appendInTxn so views materialize in
  // the same transaction as the append.
  group('Stage 8 — projection materialization inside dispatch tx', () {
    test('view row is present immediately after dispatch returns', () async {
      // Register a TableProjectionSpec that watches the test action's
      // `hello.said` events and writes one row per (aggregateId).
      final projections = ProjectionRegistry()
        ..register(
          TableProjectionSpec(
            viewName: 'greetings_view',
            interest: const SubscriptionFilter(
              eventTypes: <String>{'hello.said'},
            ),
            insertEventTypes: const <String>{'hello.said'},
            removeEventTypes: const <String>{},
            rowKey: const AggregateIdKey(),
            rowData: const WholePayload(),
          ),
        );

      final store = await bootstrapTestEventStore(projections: projections);
      final reg = ActionRegistry()..register(HelloAction());
      final idem = InMemoryIdempotencyStore();
      final allowDispatcher = ActionDispatcher(
        registry: reg,
        authorization: const AlwaysAllowPolicy(),
        events: store,
        idempotency: idem,
      );

      // HelloAction emits an EventDraft with aggregateId
      // 'greeting-${who}' (see fixtures/test_actions.dart). After
      // dispatch returns, the view row for that aggregate MUST already
      // be present — proving the projection ran inside the dispatch
      // transaction, not via some post-commit subscriber path.
      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'in-tx-world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());

      final row = await store.backend.transaction(
        (txn) => store.backend.readViewRowInTxn(
          txn,
          'greetings_view',
          'greeting-in-tx-world',
        ),
      );
      expect(
        row,
        isNotNull,
        reason:
            'appendInTxn must run the projection interpreter so action-'
            'emitted events update views inside the dispatch transaction',
      );
      expect(row!['who'], 'in-tx-world');
    });
  });

  group('Stages 9+10 — record idempotency + return success', () {
    late ActionDispatcher allowDispatcher;

    setUp(() {
      registry
        ..register(OptionalKeyAction())
        ..register(RequiredKeyAction())
        ..register(MultiEventAction());
      allowDispatcher = makeAllowDispatcher(registry, eventStore, idempotency);
    });

    test(
      'success path returns DispatchSuccess with result and emittedEventIds',
      () async {
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());
        final success = result as DispatchSuccess<Object?>;
        expect(success.result, equals('Hello, world'));
        // HelloAction emits exactly one event.
        expect(success.emittedEventIds, hasLength(1));
      },
    );

    test(
      'Idempotency.optional + key records entry; lookup returns entry with matching emittedEventIds',
      () async {
        final ctx = _ctx();
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'optional_key',
            rawInput: <String, Object?>{'who': 'optional-world'},
            idempotencyKey: 'k1',
          ),
          ctx,
        );
        expect(result, isA<DispatchSuccess<Object?>>());
        final success = result as DispatchSuccess<Object?>;

        // Pass now = requestStartedAt so the entry is not expired at lookup time.
        final entry = await idempotency.lookup(
          'optional_key',
          'u-1',
          'k1',
          now: ctx.requestStartedAt,
        );
        expect(entry, isNotNull);
        expect(entry!.emittedEventIds, equals(success.emittedEventIds));
      },
    );

    test(
      'Idempotency.required + key records entry; lookup returns entry with matching emittedEventIds',
      () async {
        final ctx = _ctx();
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{'who': 'required-world'},
            idempotencyKey: 'k2',
          ),
          ctx,
        );
        expect(result, isA<DispatchSuccess<Object?>>());
        final success = result as DispatchSuccess<Object?>;

        // Pass now = requestStartedAt so the entry is not expired at lookup time.
        final entry = await idempotency.lookup(
          'requires_key',
          'u-1',
          'k2',
          now: ctx.requestStartedAt,
        );
        expect(entry, isNotNull);
        expect(entry!.emittedEventIds, equals(success.emittedEventIds));
      },
    );

    test(
      'Idempotency.none + key supplied → no idempotency entry recorded',
      () async {
        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'no-record'},
            idempotencyKey: 'ignored-key',
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        // HelloAction has Idempotency.none; key must be silently ignored —
        // no entry written to the store.
        final entry = await idempotency.lookup('hello', 'u-1', 'ignored-key');
        expect(entry, isNull);
      },
    );
  });

  // Verifies: EVS-PRD-action-dispatch/B (authorize stage's policy reads
  // and Stage 8's event appends share one storage transaction, so a
  // role/scope revocation committed between authorize and append cannot
  // mid-flight invalidate an authorized dispatch.)
  group('Stage 6–8 — authorize+execute share one transaction', () {
    test(
      'policy.isPermitted receives a non-null Txn injected by the dispatcher',
      () async {
        final policy = RecordingAllowPolicy();
        final d = ActionDispatcher(
          registry: registry,
          authorization: policy,
          events: eventStore,
          idempotency: idempotency,
        );
        final result = await d.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'world'},
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        expect(policy.txns, hasLength(1));
        expect(
          policy.txns.single,
          isNotNull,
          reason:
              'dispatcher must inject its active runTransaction Txn into '
              'policy.isPermitted so authorize+execute share a snapshot',
        );
      },
    );

    test('two dispatches receive distinct Txn instances (each dispatch opens '
        'its own backend transaction)', () async {
      final policy = RecordingAllowPolicy();
      final d = ActionDispatcher(
        registry: registry,
        authorization: policy,
        events: eventStore,
        idempotency: idempotency,
      );
      await d.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'one'},
        ),
        _ctx(),
      );
      await d.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'two'},
        ),
        _ctx(),
      );

      expect(policy.txns, hasLength(2));
      expect(policy.txns[0], isNotNull);
      expect(policy.txns[1], isNotNull);
      expect(
        identical(policy.txns[0], policy.txns[1]),
        isFalse,
        reason: 'distinct dispatches must open distinct transactions',
      );
    });

    test('both isPermitted calls within one dispatch (TwoPermissionAction) '
        'receive the SAME Txn instance — proves the policy reads share one '
        'snapshot across the action\'s permission iteration', () async {
      registry.register(TwoPermissionAction());
      final policy = RecordingAllowPolicy();
      final d = ActionDispatcher(
        registry: registry,
        authorization: policy,
        events: eventStore,
        idempotency: idempotency,
      );

      final result = await d.dispatch(
        const ActionSubmission(
          actionName: 'two_perms',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());

      expect(policy.txns, hasLength(2));
      expect(policy.txns[0], isNotNull);
      expect(
        identical(policy.txns[0], policy.txns[1]),
        isTrue,
        reason:
            'authorize-stage policy reads for multiple permissions in '
            'one dispatch must share the dispatcher\'s active txn',
      );
    });

    test('authorize-stage denial commits the authorization_denied event in '
        'the dispatch tx (success path: tx is committed even though no '
        'execute-result events were appended)', () async {
      // Default `dispatcher` uses DenyAllAuthorizationPolicy.forTests();
      // HelloAction declares test.hello which gets denied.
      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchAuthorizationDenied<Object?>>());

      // The denial event must be durably persisted (i.e. the tx was
      // committed with just the denial in it, not rolled back).
      final allEvents = await eventStore.backend.findAllEvents();
      final denials = allEvents
          .where((e) => e.eventType == 'authorization_denied')
          .toList();
      expect(denials, hasLength(1));
      expect(denials.single.data['permission_denied'], 'test.hello');
    });

    test('execute-stage failure rolls the dispatch tx back, then emits the '
        'execution_failed denial in its own (post-rollback) append', () async {
      registry.register(BadExecuteAction());
      final allowDispatcher = ActionDispatcher(
        registry: registry,
        authorization: const AlwaysAllowPolicy(),
        events: eventStore,
        idempotency: idempotency,
      );
      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'bad_execute',
          rawInput: <String, Object?>{'who': 'world'},
        ),
        _ctx(),
      );
      expect(result, isA<DispatchExecutionFailed<Object?>>());

      // The dispatch tx rolled back, so no greeting events; the
      // execution_failed denial was appended in its own tx.
      final allEvents = await eventStore.backend.findAllEvents();
      final greetings = allEvents
          .where((e) => e.eventType == 'hello.said')
          .toList();
      expect(greetings, isEmpty);
      final denials = allEvents
          .where((e) => e.eventType == 'execution_failed')
          .toList();
      expect(denials, hasLength(1));
    });
  });
}
