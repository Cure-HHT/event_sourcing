// Verifies: EVS-PRD-permission-snapshot-source/B/E —
// LocalPermissionSource derives the snapshot from the substrate's
// permissions projections via AuthorizationPolicy.effectivePermissionsFor
// (B), and re-fetches + re-emits when the active Principal changes via
// setActivePrincipal (E). Also exercises the current/stream getters,
// dispose, and the snapshot-on-listen contract documented in the
// interface.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/local/local_permission_source.dart';

import 'test_support/reaction_test_harness.dart';

void main() {
  group('LocalPermissionSource', () {
    late ReactionTestHarness harness;
    late LocalPermissionSource source;

    // Define the helper-permission for assertions.
    const sayHello = Permission('say_hello');

    setUp(() async {
      harness = await ReactionTestHarness.open();
      // Seed: 'greeter' role has 'say_hello'.
      await _grantSayHelloToGreeterRole(harness);
      // Seed: alice and bob hold the roles they claim below. The substrate
      // (and therefore LocalPermissionSource via effectivePermissionsFor)
      // refuses to honour an activeRole claim without a user_role_scopes
      // membership row.
      await harness.seedRoleAssigned(userId: 'alice', role: 'greeter');
      await harness.seedRoleAssigned(userId: 'bob', role: 'nobody');

      source = LocalPermissionSource(
        eventStore: harness.eventStore,
        policy: harness.dispatcher.authorization,
      );
    });

    tearDown(() async {
      await source.dispose();
      await harness.close();
    });

    test('current is null before setActivePrincipal', () {
      expect(source.current, isNull);
    });

    test(
      'after setActivePrincipal(alice/greeter), current reflects greeter grants',
      () async {
        final alice = Principal.user(
          userId: 'alice',
          roles: const {'greeter'},
          activeRole: 'greeter',
        );
        source.setActivePrincipal(alice);

        // Allow async recompute to settle.
        await Future<void>.delayed(const Duration(milliseconds: 100));

        final snapshot = source.current;
        expect(snapshot, isNotNull);
        expect(snapshot!.activeRole, equals('greeter'));
        expect(snapshot.rolePermissions, contains(sayHello));
      },
    );

    test('scopeAssignments surfaces on the snapshot', () async {
      // alice was seeded with role 'greeter' via seedRoleAssigned, which
      // defaults to a TotalWildcardScope. After effectivePermissionsFor
      // resolves, the snapshot SHOULD expose that assignment so UI code
      // can pre-filter scoped lists without a separate fetch.
      final alice = Principal.user(
        userId: 'alice',
        roles: const {'greeter'},
        activeRole: 'greeter',
      );
      source.setActivePrincipal(alice);
      await Future<void>.delayed(const Duration(milliseconds: 100));

      final snapshot = source.current;
      expect(snapshot, isNotNull);
      expect(
        snapshot!.scopeAssignments,
        isNotEmpty,
        reason: 'alice/greeter was seeded with a scope assignment',
      );
    });

    test('setActivePrincipal(null) clears current to null', () async {
      final alice = Principal.user(
        userId: 'alice',
        roles: const {'greeter'},
        activeRole: 'greeter',
      );
      source.setActivePrincipal(alice);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(source.current, isNotNull);

      source.setActivePrincipal(null);
      // null-clear is synchronous; no need to wait.
      expect(source.current, isNull);
    });

    test('stream emits when active principal changes', () async {
      final events = <EffectiveAuthorization?>[];
      final sub = source.stream.listen(events.add);

      // Alice has 'greeter' — should get a non-null snapshot with say_hello.
      source.setActivePrincipal(
        Principal.user(
          userId: 'alice',
          roles: const {'greeter'},
          activeRole: 'greeter',
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 100));

      // Bob has 'nobody' role — no grants; snapshot has empty grants.
      source.setActivePrincipal(
        Principal.user(
          userId: 'bob',
          roles: const {'nobody'},
          activeRole: 'nobody',
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 100));

      // Explicit null-clear.
      source.setActivePrincipal(null);
      await Future<void>.delayed(const Duration(milliseconds: 50));

      // Expect at least 3 events.
      expect(events.length, greaterThanOrEqualTo(3));
      // alice's event: role='greeter', has say_hello.
      final aliceEvt = events.firstWhere(
        (e) => e != null && e.rolePermissions.contains(sayHello),
        orElse: () => null,
      );
      expect(aliceEvt, isNotNull);
      expect(aliceEvt!.activeRole, equals('greeter'));
      // null-clear event is present.
      expect(events.last, isNull);

      await sub.cancel();
    });

    test(
      'stream emits current value to a late subscriber (snapshot-on-listen)',
      () async {
        final alice = Principal.user(
          userId: 'alice',
          roles: const {'greeter'},
          activeRole: 'greeter',
        );
        source.setActivePrincipal(alice);
        await Future<void>.delayed(const Duration(milliseconds: 100));

        // Late subscriber comes after current is already set.
        final firstEvent = source.stream.first;
        await expectLater(
          firstEvent,
          completion(
            predicate<EffectiveAuthorization?>(
              (s) => s != null && s.rolePermissions.contains(sayHello),
            ),
          ),
        );
      },
    );

    test('principal claiming a role without user_role_scopes membership '
        'gets null snapshot (divergence-closing parity with Remote)', () async {
      // Carol claims the 'greeter' role — which has the say_hello grant —
      // but no role_assigned event has been appended for (carol, greeter).
      // The substrate's effectivePermissionsFor returns
      // EffectiveAuthorization.empty in that case; LocalPermissionSource
      // must surface that as a null snapshot (not a snapshot leaking the
      // role's permissions to a user who doesn't actually hold the role).
      final carol = Principal.user(
        userId: 'carol',
        roles: const {'greeter'},
        activeRole: 'greeter',
      );
      source.setActivePrincipal(carol);

      await Future<void>.delayed(const Duration(milliseconds: 100));

      expect(source.current, isNull);
    });
  });
}

/// Seeds the harness's role-permission matrix with 'say_hello' granted to the
/// 'greeter' role. Copied from local_action_submitter_test.dart.
Future<void> _grantSayHelloToGreeterRole(ReactionTestHarness harness) async {
  const sayHelloPerm = Permission('say_hello');
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
