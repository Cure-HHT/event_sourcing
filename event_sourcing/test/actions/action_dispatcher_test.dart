// Verifies: EVS-PRD-action-dispatch/C (every dispatched action produces a recorded denial event or DispatchSuccess with emittedEventIds)
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
    // Verifies: EVS-PRD-action-dispatch/A (dispatcher accepts principal-submitted actions and processes each through the dispatch flow)
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

    // Verifies: EVS-PRD-action-dispatch/B (stages in order: Stage 4 idempotency key check precedes Stage 3 parseInput; missing required key emits parse_denied before parse runs)
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

    // Verifies: EVS-PRD-action-dispatch/D (idempotency cache hit short-circuits; DispatchIdempotencyHit returned; no new events appended)
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

    // Verifies: EVS-PRD-action-dispatch/D (same key + matching rawInput → DispatchIdempotencyHit; no new events appended)
    test(
      'idempotency content match (same key, same rawInput) returns DispatchIdempotencyHit; no new events',
      () async {
        await idempotency.record(
          actionName: 'requires_key',
          principalId: 'u-1',
          key: 'k-match',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['prior-event-id-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          // canonicalize({'who': 'tester'}) = '{"who":"tester"}'
          rawInputCanonicalJson: '{"who":"tester"}',
        );

        final eventCountBefore =
            (await eventStore.backend.findAllEvents()).length;

        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{'who': 'tester'},
            idempotencyKey: 'k-match',
          ),
          _ctx(),
        );

        expect(result, isA<DispatchIdempotencyHit<Object?>>());
        final hit = result as DispatchIdempotencyHit<Object?>;
        expect(hit.priorEmittedEventIds, contains('prior-event-id-1'));

        // No idempotency_mismatch event emitted.
        final allEvents = await eventStore.backend.findAllEvents();
        expect(
          allEvents.where((e) => e.eventType == 'idempotency_mismatch'),
          isEmpty,
        );
        expect(allEvents, hasLength(eventCountBefore));
      },
    );

    // Verifies: EVS-PRD-action-dispatch/E (same key + different rawInput → DispatchIdempotencyMismatch denial; idempotency_mismatch event emitted with hashed inputs)
    test(
      'idempotency content mismatch (same key, different rawInput) emits '
      'idempotency_mismatch denial and returns DispatchIdempotencyMismatch',
      () async {
        await idempotency.record(
          actionName: 'requires_key',
          principalId: 'u-1',
          key: 'k-mismatch',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['prior-event-id-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          rawInputCanonicalJson: '{"who":"original"}',
        );

        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{'who': 'DIFFERENT'},
            idempotencyKey: 'k-mismatch',
          ),
          _ctx(),
        );

        expect(result, isA<DispatchIdempotencyMismatch<Object?>>());
        final mismatch = result as DispatchIdempotencyMismatch<Object?>;
        expect(mismatch.actionName, 'requires_key');
        expect(mismatch.idempotencyKey, 'k-mismatch');
        // SHA-256 hex is 64 lowercase hex chars.
        expect(mismatch.cachedRawInputHash, hasLength(64));
        expect(mismatch.submittedRawInputHash, hasLength(64));
        expect(
          mismatch.cachedRawInputHash,
          isNot(equals(mismatch.submittedRawInputHash)),
        );

        // Exactly one idempotency_mismatch denial event was appended.
        final allEvents = await eventStore.backend.findAllEvents();
        final denials = allEvents
            .where((e) => e.eventType == 'idempotency_mismatch')
            .toList();
        expect(denials, hasLength(1));
        expect(denials.single.data['action_name'], 'requires_key');
        expect(denials.single.data['idempotency_key'], 'k-mismatch');
        expect(
          denials.single.data['cached_raw_input_hash'],
          mismatch.cachedRawInputHash,
        );
        expect(
          denials.single.data['submitted_raw_input_hash'],
          mismatch.submittedRawInputHash,
        );
        // The denial event carries the dispatcher-generated invocation id
        // (proves it went through the standard denial path).
        expect(denials.single.metadata['action_invocation_id'], isA<String>());
      },
    );

    // Verifies: EVS-PRD-action-dispatch/D (JCS canonicalization prevents false mismatches on key-order differences → same content still yields DispatchIdempotencyHit)
    test(
      'idempotency mismatch is keyed on canonical JSON, not raw dict order',
      () async {
        // canonicalize({'a': 1, 'b': 2}) == canonicalize({'b': 2, 'a': 1})
        await idempotency.record(
          actionName: 'requires_key',
          principalId: 'u-1',
          key: 'k-order',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['evt-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          rawInputCanonicalJson: '{"a":1,"b":2,"who":"tester"}',
        );

        final result = await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            // Map literal order varies from what was stored, but JCS
            // sorts keys so canonical form is the same.
            rawInput: <String, Object?>{'b': 2, 'who': 'tester', 'a': 1},
            idempotencyKey: 'k-order',
          ),
          _ctx(),
        );
        expect(result, isA<DispatchIdempotencyHit<Object?>>());
      },
    );

    // Verifies: EVS-PRD-action-dispatch/D (legacy entries without rawInputCanonicalJson fall back to cache-hit semantics; no false mismatch raised)
    test('legacy cache entry without rawInputCanonicalJson returns '
        'DispatchIdempotencyHit (forward-compat, no false mismatch)', () async {
      await idempotency.record(
        actionName: 'requires_key',
        principalId: 'u-1',
        key: 'k-legacy',
        resultJson: const <String, dynamic>{'cached': true},
        emittedEventIds: const ['prior-evt'],
        expiresAt: DateTime.now().add(const Duration(hours: 1)),
        // rawInputCanonicalJson intentionally omitted (legacy row).
      );

      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'requires_key',
          rawInput: <String, Object?>{'who': 'anything-goes'},
          idempotencyKey: 'k-legacy',
        ),
        _ctx(),
      );

      expect(result, isA<DispatchIdempotencyHit<Object?>>());
      final allEvents = await eventStore.backend.findAllEvents();
      expect(
        allEvents.where((e) => e.eventType == 'idempotency_mismatch'),
        isEmpty,
      );
    });

    // Verifies: EVS-PRD-action-dispatch/D (mismatch check is per (actionName, principalId, key); different key → cache miss, no idempotency_mismatch event)
    test(
      'different idempotency keys do not interfere — distinct cache slots',
      () async {
        await idempotency.record(
          actionName: 'requires_key',
          principalId: 'u-1',
          key: 'k-aaa',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['evt-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          rawInputCanonicalJson: '{"who":"original"}',
        );

        // Different key + different content → cache miss → no
        // idempotency_mismatch denial. (The action's authorization is
        // denied by DenyAllAuthorizationPolicy.forTests, which is fine
        // — we only assert here that no idempotency_mismatch fired.)
        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'requires_key',
            rawInput: <String, Object?>{'who': 'OTHER'},
            idempotencyKey: 'k-bbb',
          ),
          _ctx(),
        );

        final allEvents = await eventStore.backend.findAllEvents();
        expect(
          allEvents.where((e) => e.eventType == 'idempotency_mismatch'),
          isEmpty,
        );
      },
    );

    // Verifies: EVS-PRD-action-dispatch/E (Idempotency.optional + no key → Stage 4 bypassed entirely; no idempotency_mismatch fired)
    test(
      'idempotency.optional with no key bypasses mismatch detection',
      () async {
        registry.register(OptionalKeyAction());

        await idempotency.record(
          actionName: 'optional_key',
          principalId: 'u-1',
          key: 'k-opt',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['evt-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          rawInputCanonicalJson: '{"who":"original"}',
        );

        // Submit with no idempotencyKey; mismatch path must not fire
        // (authorize is denied by the default policy, which is fine).
        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'optional_key',
            rawInput: <String, Object?>{'who': 'TOTALLY-DIFFERENT'},
          ),
          _ctx(),
        );

        final allEvents = await eventStore.backend.findAllEvents();
        expect(
          allEvents.where((e) => e.eventType == 'idempotency_mismatch'),
          isEmpty,
        );
      },
    );

    // Verifies: EVS-PRD-action-dispatch/E (Idempotency.none + key supplied → Stage 4 bypassed; no idempotency_mismatch fired)
    test(
      'idempotency.none with key supplied bypasses mismatch detection',
      () async {
        await idempotency.record(
          actionName: 'hello',
          principalId: 'u-1',
          key: 'k-none',
          resultJson: const <String, dynamic>{'cached': true},
          emittedEventIds: const ['evt-1'],
          expiresAt: DateTime.now().add(const Duration(hours: 1)),
          rawInputCanonicalJson: '{"who":"original"}',
        );

        await dispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'TOTALLY-DIFFERENT'},
            idempotencyKey: 'k-none',
          ),
          _ctx(),
        );

        final allEvents = await eventStore.backend.findAllEvents();
        expect(
          allEvents.where((e) => e.eventType == 'idempotency_mismatch'),
          isEmpty,
        );
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

    // Verifies: EVS-PRD-action-dispatch/A (dispatcher accepts principal-submitted actions and processes through the full dispatch flow)
    // Verifies: EVS-PRD-action-dispatch/B (stages 1–10 in order: all-Allow falls through all stages to DispatchSuccess)
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
    // Verifies: EVS-DEV-scope-unresolvable-denial/A (dispatcher invokes scopeFor before opening the dispatch transaction; class-matching BoundScope → Allow)
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

    // Verifies: EVS-DEV-scope-unresolvable-denial/B (scopeFor null → authorization_denied with scopeUnresolvable; no scope stamped)
    // Verifies: EVS-PRD-scoped-permissions/E (scopeUnresolvable denial when action supplies no scope value)
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

    // Verifies: EVS-DEV-scope-unresolvable-denial/D (scopeFor class-mismatched BoundScope → authorization_denied with scopeUnresolvable; scope stamped)
    // Verifies: EVS-DEV-scope-unresolvable-denial/E (offending scope value stamped on denial for class-mismatch case)
    // Verifies: EVS-PRD-scoped-permissions/E (scopeUnresolvable denial for class-mismatch)
    // Verifies: EVS-PRD-scoped-permissions/I (mismatched scope value stamped onto authorization_denied)
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

    // Verifies: EVS-DEV-scope-unresolvable-denial/C (scopeFor TotalWildcardScope → authorization_denied with scopeUnresolvable; scope stamped)
    // Verifies: EVS-DEV-scope-unresolvable-denial/E (offending scope value stamped on denial for TotalWildcardScope case)
    // Verifies: EVS-PRD-scoped-permissions/E (scopeUnresolvable denial for TotalWildcardScope)
    // Verifies: EVS-PRD-scoped-permissions/I (TotalWildcardScope value stamped onto authorization_denied)
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

    // Verifies: EVS-PRD-scoped-permissions/I (class-matched resolved scope value stamped onto authorization_denied when policy denies)
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

    // Verifies: EVS-PRD-action-dispatch/B (execute stage is part of the dispatch pipeline; execute throw → DispatchExecutionFailed returned)
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

    // Verifies: EVS-PRD-action-dispatch/A (dispatcher accepts actions and processes all the way through to DispatchSuccess)
    // Verifies: EVS-PRD-action-dispatch/B (all stages complete in order for a successful dispatch)
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

  // Stage 8 appends action events via appendInTxn, and the projection
  // interpreter runs inside that same transaction so view rows are
  // materialized atomically with the append.
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

    // Verifies: EVS-PRD-action-dispatch/D (Stage 9 records idempotency entry when key is present; lookup returns matching entry)
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

    // Verifies: EVS-PRD-action-dispatch/D (Stage 9 records the idempotency entry for future cache lookups)
    // Verifies: EVS-PRD-action-dispatch/E (Stage 9 stamps canonical JSON on the entry so subsequent retries can be content-compared)
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
        // JCS sorts keys and uses no whitespace.
        expect(entry.rawInputCanonicalJson, '{"who":"required-world"}');
      },
    );

    // Verifies: EVS-PRD-action-dispatch/D (Stage 9→Stage 4 round-trip: first dispatch records canonical JSON; second dispatch hits cache with no mismatch)
    test('Stage 9 -> Stage 4 round-trip: recorded canonical JSON drives the '
        'next dispatch into the cache-hit branch (no mismatch)', () async {
      // Use DateTime.now() for the context (not the fixed _ctx()
      // timestamp) so the recorded entry's expiresAt is in the future
      // relative to the lookup's now=DateTime.now().
      final ctx = ActionContext(
        principal: Principal.user(
          userId: 'u-1',
          roles: const {'tester'},
          activeRole: 'tester',
        ),
        security: const SecurityDetails(),
        requestStartedAt: DateTime.now(),
      );
      final first = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'requires_key',
          rawInput: <String, Object?>{'who': 'roundtrip', 'q': 7},
          idempotencyKey: 'k-rt',
        ),
        ctx,
      );
      expect(first, isA<DispatchSuccess<Object?>>());
      final firstSuccess = first as DispatchSuccess<Object?>;

      final eventsBefore = (await eventStore.backend.findAllEvents()).length;

      // Re-submit identically; expect cache hit, no new events.
      final second = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'requires_key',
          // Map literal order intentionally swapped — JCS canonicalizes
          // the key order, so this is still the same content.
          rawInput: <String, Object?>{'q': 7, 'who': 'roundtrip'},
          idempotencyKey: 'k-rt',
        ),
        ctx,
      );
      expect(second, isA<DispatchIdempotencyHit<Object?>>());
      final hit = second as DispatchIdempotencyHit<Object?>;
      expect(hit.priorEmittedEventIds, firstSuccess.emittedEventIds);

      final eventsAfter = (await eventStore.backend.findAllEvents()).length;
      expect(eventsAfter, eventsBefore);
    });

    // Verifies: EVS-PRD-action-dispatch/D (Idempotency.none ignores supplied key; no entry written; future retries cannot get a false hit)
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

  // Verifies: EVS-DEV-transactional-authorize-execute/A (one storage transaction for authorize + execute + persist; policy receives the active Transaction)
  // Verifies: EVS-PRD-scoped-permissions/H (authorize-stage policy reads and execute-stage appends share one storage transaction)
  group('Stage 6–8 — authorize+execute share one transaction', () {
    test(
      'policy.isPermitted receives a non-null Transaction injected by the dispatcher',
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
              'dispatcher must inject its active runTransaction Transaction into '
              'policy.isPermitted so authorize+execute share a snapshot',
        );
      },
    );

    // Verifies: EVS-DEV-transactional-authorize-execute/A (each dispatch opens its own storage transaction)
    // Verifies: EVS-PRD-scoped-permissions/H (each dispatch opens its own isolated storage transaction — transactions are not shared across dispatches)
    test(
      'two dispatches receive distinct Transaction instances (each dispatch opens '
      'its own backend transaction)',
      () async {
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
      },
    );

    // Verifies: EVS-DEV-transactional-authorize-execute/A (both isPermitted calls in one dispatch share the same Transaction snapshot)
    // Verifies: EVS-PRD-scoped-permissions/H (multiple policy reads within one dispatch share one storage transaction)
    test('both isPermitted calls within one dispatch (TwoPermissionAction) '
        'receive the SAME Transaction instance — proves the policy reads share one '
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

    // Verifies: EVS-DEV-transactional-authorize-execute/B (authorize-stage denial commits authorization_denied in the same dispatch transaction)
    // Verifies: EVS-PRD-scoped-permissions/H (denial event committed atomically inside the dispatch transaction)
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

    // Verifies: EVS-DEV-transactional-authorize-execute/C (execute throws → dispatch tx rolled back; execution_failed emitted in a separate append)
    // Verifies: EVS-PRD-scoped-permissions/H (dispatch tx is rolled back on execute failure; separate append for denial event)
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
