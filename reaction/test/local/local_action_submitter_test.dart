// Verifies: EVS-PRD-action-submitter — LocalActionSubmitter honors
// the ActionSubmitter interface, surfaces dispatch outcomes as
// DispatchResult variants, and throws TransportException on
// unauthenticated submissions.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/local/local_action_submitter.dart';

import 'test_support/reaction_test_harness.dart';

/// Minimal stub AuthSession for tests.
class _StubAuthSession implements AuthSession {
  _StubAuthSession(this._principal);

  final Principal? _principal;

  @override
  AuthStatus get current {
    final p = _principal;
    return p == null ? const NotAuthenticated() : Authenticated(principal: p);
  }

  @override
  Principal? get principal => _principal;

  @override
  Stream<AuthStatus> get stream => const Stream.empty();

  @override
  void setCredential(String? credential) => throw UnimplementedError();

  @override
  Future<void> dispose() async {}
}

void main() {
  group('LocalActionSubmitter', () {
    late ReactionTestHarness harness;
    late _StubAuthSession authSession;
    late LocalActionSubmitter submitter;

    // Alice has 'greeter' as her active role — matches the seeded grant.
    const alice = Principal.user(
      userId: 'alice',
      roles: {'greeter'},
      activeRole: 'greeter',
    );

    setUp(() async {
      harness = await ReactionTestHarness.open();
      await _grantSayHelloToGreeterRole(harness);

      authSession = _StubAuthSession(alice);
      submitter = LocalActionSubmitter(
        dispatcher: harness.dispatcher,
        authSession: authSession,
      );
    });

    tearDown(() async {
      await harness.close();
    });

    test('successful submission returns DispatchSuccess', () async {
      final result = await submitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'World'},
          idempotencyKey: 'key-1',
        ),
      );
      expect(result, isA<DispatchSuccess<Object?>>());
      expect(
        (result as DispatchSuccess<Object?>).emittedEventIds,
        hasLength(1),
      );
    });

    test('parse failure returns DispatchParseDenied', () async {
      final result = await submitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {}, // missing 'name' key
          idempotencyKey: 'key-parse',
        ),
      );
      expect(result, isA<DispatchParseDenied<Object?>>());
    });

    test('authorization denial returns DispatchAuthorizationDenied', () async {
      // Bob has no active role — not granted say_hello.
      const bob = Principal.user(
        userId: 'bob',
        roles: {},
        activeRole: 'nobody',
      );
      authSession = _StubAuthSession(bob);
      submitter = LocalActionSubmitter(
        dispatcher: harness.dispatcher,
        authSession: authSession,
      );

      final result = await submitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'World'},
          idempotencyKey: 'key-deny',
        ),
      );
      expect(result, isA<DispatchAuthorizationDenied<Object?>>());
    });

    test('idempotent replay returns DispatchIdempotencyHit', () async {
      final r1 = await submitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'Once'},
          idempotencyKey: 'key-idem',
        ),
      );
      final r2 = await submitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'Once'},
          idempotencyKey: 'key-idem',
        ),
      );
      expect(r1, isA<DispatchSuccess<Object?>>());
      expect(r2, isA<DispatchIdempotencyHit<Object?>>());
      expect(
        (r2 as DispatchIdempotencyHit<Object?>).priorEmittedEventIds,
        equals((r1 as DispatchSuccess<Object?>).emittedEventIds),
      );
    });

    test(
      'submission without authenticated Principal throws TransportException',
      () async {
        authSession = _StubAuthSession(null);
        submitter = LocalActionSubmitter(
          dispatcher: harness.dispatcher,
          authSession: authSession,
        );
        await expectLater(
          () => submitter.submit(
            const ActionSubmission(
              actionName: 'say_hello',
              rawInput: {'name': 'World'},
            ),
          ),
          throwsA(isA<TransportException>()),
        );
      },
    );
  });
}

/// Seeds the harness's role-permission matrix with 'say_hello' granted to the
/// 'greeter' role.
///
/// Uses [EventSeedApplier] directly (no YAML round-trip needed in tests).
/// [SayHelloAction.permissions] declares `Permission('say_hello',
/// scope: ScopeClass.global)` — that's the declared set passed to apply().
Future<void> _grantSayHelloToGreeterRole(ReactionTestHarness harness) async {
  const sayHelloPerm = Permission('say_hello', scope: ScopeClass.global);
  const seed = PermissionSeed(
    roles: {'greeter'},
    grants: {
      'greeter': {'say_hello'},
    },
  );
  final applier = EventSeedApplier(
    eventStore: harness.eventStore,
    seedInitiator: const AutomationInitiator(service: 'reaction_test_seed'),
  );
  await applier.apply(seed, {sayHelloPerm});
}
