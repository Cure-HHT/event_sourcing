// Verifies: EVS-DEV-flow-token/A — dispatcher accepts an optional opaque
//   correlation token on submission (null and non-null; the field is a
//   plain nullable String with no validation constraints).
// Verifies: EVS-DEV-flow-token/B — token is threaded onto every emitted
//   event, including denial events (authorization denied, unknown-action
//   denial), as well as success events.
// Verifies: EVS-DEV-flow-token/C — token is preserved unchanged when the
//   event is ingested by another deployment (the ingest leg re-reads the
//   stored event from a second backend and asserts flowToken == original).
// Verifies: EVS-DEV-flow-token/D — token is opaque: the substrate stores
//   and returns any valid UTF-8 string byte-identically, without parsing
//   or interpreting its contents.
//
// NOTE on assertion D's consumer obligation:
//   The requirement also states "SHALL NOT embed cleartext OTP, recovery,
//   or session tokens" in the flowToken field. That is a *consumer*
//   obligation the substrate cannot enforce — the substrate has no schema
//   for the token's content. The test below covers the substrate-side half
//   (byte-identical round-trip for an intentionally weird string); the
//   consumer obligation is documented here for traceability only.
//
// Uses flutter_test (not package:test) because EventStore depends on
// Sembast, which requires the Flutter test binding to run in this package.
// All tests in event_sourcing/ that touch EventStore use flutter_test for
// the same reason.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import 'fixtures/test_actions.dart' show AlwaysAllowPolicy, HelloAction;
import 'test_support/event_store_helper.dart' show bootstrapTestEventStore;

// ---------------------------------------------------------------------------
// Helper: build a context with an authorized principal.
// Mirrors _ctx() in action_dispatcher_test.dart.
// ---------------------------------------------------------------------------

ActionContext _ctx() => ActionContext(
  principal: Principal.user(
    userId: 'u-1',
    roles: const {'tester'},
    activeRole: 'tester',
  ),
  security: const SecurityDetails(),
  requestStartedAt: DateTime.parse('2026-04-22T12:00:00Z'),
);

// ---------------------------------------------------------------------------
// Helper: open a second bare EventStore for the ingest leg of test C.
// Registers the same entry types that bootstrapTestEventStore registers so
// that the ingested event (entryType='action_denial' or 'greeting') is
// accepted by the second store's registry.
// ---------------------------------------------------------------------------

var _dbCounter = 0;

Future<({EventStore store, SembastBackend backend})> _openSecondStore() async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'flow-token-ingest-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final def in kSystemEntryTypes) {
    registry.register(def);
  }
  registry
    ..register(
      const EntryTypeDefinition(
        id: 'action_denial',
        registeredVersion: 1,
        name: 'Action denial',
        isMaterialized: false,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'greeting',
        registeredVersion: 1,
        name: 'Greeting',
        isMaterialized: false,
      ),
    );
  final securityContexts = SembastSecurityContextStore(backend: backend);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'second-server',
      identifier: 'second-instance-1',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    securityContexts: securityContexts,
  );
  return (store: store, backend: backend);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('flow correlation token', () {
    late EventStore eventStore;
    late ActionDispatcher dispatcher;
    late ActionDispatcher allowDispatcher;

    setUp(() async {
      // Register HelloAction (success) only. The default dispatcher uses
      // DenyAllAuthorizationPolicy.forTests(), so dispatching 'hello' with
      // the default dispatcher produces an authorization_denied event.
      // The allowDispatcher uses AlwaysAllowPolicy to reach success events.
      final registry = ActionRegistry()..register(HelloAction());
      eventStore = await bootstrapTestEventStore();
      final idempotency = InMemoryIdempotencyStore();

      // Default dispatcher: DenyAllAuthorizationPolicy → denial events.
      dispatcher = ActionDispatcher(
        registry: registry,
        authorization: const DenyAllAuthorizationPolicy.forTests(),
        events: eventStore,
        idempotency: idempotency,
      );

      // Allow dispatcher: AlwaysAllowPolicy → success events.
      allowDispatcher = ActionDispatcher(
        registry: registry,
        authorization: const AlwaysAllowPolicy(),
        events: eventStore,
        idempotency: idempotency,
      );
    });

    // -----------------------------------------------------------------------
    // A: ActionSubmission accepts an optional opaque flowToken field.
    // -----------------------------------------------------------------------

    test('A: submission accepts a null flowToken (default)', () {
      const sub = ActionSubmission(
        actionName: 'hello',
        rawInput: <String, Object?>{},
      );
      expect(sub.flowToken, isNull);
    });

    test('A: submission accepts a non-null flowToken', () {
      const sub = ActionSubmission(
        actionName: 'hello',
        rawInput: <String, Object?>{},
        flowToken: 'flow-123',
      );
      expect(sub.flowToken, 'flow-123');
    });

    // -----------------------------------------------------------------------
    // B: token threaded onto success events.
    // -----------------------------------------------------------------------

    test('B: success event carries the flowToken', () async {
      // Verifies: EVS-DEV-flow-token/B
      const token = 'flow-success-42';
      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
          flowToken: token,
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());

      final events = await eventStore.backend.findAllEvents(
        entryType: 'greeting',
      );
      expect(events, isNotEmpty);
      for (final e in events) {
        expect(
          e.flowToken,
          token,
          reason:
              'every success event emitted by this dispatch must carry '
              'flowToken=$token; got ${e.flowToken} on event ${e.eventId}',
        );
      }
    });

    test('B: no flowToken → success events have null flowToken', () async {
      final result = await allowDispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'no-token'},
          // flowToken intentionally omitted
        ),
        _ctx(),
      );
      expect(result, isA<DispatchSuccess<Object?>>());

      final events = await eventStore.backend.findAllEvents(
        entryType: 'greeting',
      );
      expect(events, isNotEmpty);
      for (final e in events) {
        expect(e.flowToken, isNull);
      }
    });

    // -----------------------------------------------------------------------
    // B: token threaded onto denial events (authorization_denied path).
    // -----------------------------------------------------------------------

    test('B: authorization_denied event carries the flowToken', () async {
      // Verifies: EVS-DEV-flow-token/B — denial events carry the token.
      // The default dispatcher uses DenyAllAuthorizationPolicy.forTests()
      // so dispatching 'hello' produces an authorization_denied denial.
      const token = 'flow-denied-99';
      final result = await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'hello',
          rawInput: <String, Object?>{'who': 'world'},
          flowToken: token,
        ),
        _ctx(),
      );
      expect(result, isA<DispatchAuthorizationDenied<Object?>>());

      final denials = await eventStore.backend.findAllEvents(
        entryType: 'action_denial',
      );
      expect(denials, isNotEmpty);
      // Every denial emitted in this dispatch must carry the token.
      final authDenials = denials
          .where((e) => e.eventType == 'authorization_denied')
          .toList();
      expect(authDenials, isNotEmpty);
      for (final e in authDenials) {
        expect(
          e.flowToken,
          token,
          reason:
              'authorization_denied denial event must carry flowToken=$token',
        );
      }
    });

    test('B: unknown_action denial event carries the flowToken', () async {
      // Verifies: EVS-DEV-flow-token/B — the earliest possible denial
      // (Stage 1: action not found) also carries the token.
      const token = 'flow-unknown-action';
      await dispatcher.dispatch(
        const ActionSubmission(
          actionName: 'no_such_action',
          rawInput: <String, Object?>{},
          flowToken: token,
        ),
        _ctx(),
      );

      final denials = await eventStore.backend.findAllEvents(
        entryType: 'action_denial',
      );
      final unknownDenials = denials
          .where((e) => e.eventType == 'unknown_action')
          .toList();
      expect(unknownDenials, hasLength(1));
      expect(
        unknownDenials.single.flowToken,
        token,
        reason: 'unknown_action denial event must carry flowToken=$token',
      );
    });

    // -----------------------------------------------------------------------
    // C/D: arbitrary opaque token round-trips byte-identically through
    // persistence AND survives ingest into a second backend unchanged.
    // -----------------------------------------------------------------------

    test(
      'C/D: an arbitrary opaque token is preserved byte-identically '
      'through persistence and ingest into a second deployment',
      () async {
        // Verifies: EVS-DEV-flow-token/C — token preserved across ingest
        // Verifies: EVS-DEV-flow-token/D — token is opaque: substrate
        //   stores/returns it byte-identically without parsing
        //
        // The token below is intentionally pathological: it contains URL
        // meta-chars, embedded JSON, whitespace, and a non-ASCII code point.
        // The substrate has no schema for the token content and must pass it
        // through untouched.
        const weird = 'a/b+c=  {"not":"json-to-the-substrate"} é';

        final result = await allowDispatcher.dispatch(
          const ActionSubmission(
            actionName: 'hello',
            rawInput: <String, Object?>{'who': 'opacity-test'},
            flowToken: weird,
          ),
          _ctx(),
        );
        expect(result, isA<DispatchSuccess<Object?>>());

        // D: round-trip through Sembast storage — token must be identical.
        final stored = await eventStore.backend.findAllEvents(
          entryType: 'greeting',
        );
        expect(stored, isNotEmpty);
        final evt = stored.single;
        expect(
          evt.flowToken,
          weird,
          reason:
              'flowToken must survive Sembast persistence byte-identically; '
              'got "${evt.flowToken}"',
        );

        // C: ingest the stored event into a SECOND backend and assert the
        // token is preserved unchanged. The substrate's ingest path records
        // the event verbatim (identity fields preserved per EVS-PRD-ingest/B);
        // flow_token is an identity-level field on StoredEvent and must survive.
        final second = await _openSecondStore();
        try {
          final outcome = await second.store.ingestEvent(evt);
          expect(outcome.outcome, IngestOutcome.ingested);

          final ingestedEvents = await second.backend.findAllEvents(
            entryType: 'greeting',
          );
          expect(ingestedEvents, hasLength(1));
          expect(
            ingestedEvents.single.flowToken,
            weird,
            reason:
                'flowToken must survive ingest into a second backend '
                'byte-identically; got "${ingestedEvents.single.flowToken}"',
          );
        } finally {
          // Close the EventStore (not just its backend) so the subscription
          // engine is released alongside storage. EventStore.close() closes
          // _subs then backend.
          await second.store.close();
        }
      },
    );
  });
}
