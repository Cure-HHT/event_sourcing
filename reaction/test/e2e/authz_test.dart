// reaction/test/e2e/authz_test.dart
// Verifies: EVS-PRD-cross-process-event-transport/E (per-sub authz)
// + mid-session permission-change handling (force-logout + stale_data).
//
// Status: ships two tests expanded today:
//   1) view-level deny at subscribe time, and
//   2) role_unassigned mid-subscription → server closes WS with 4003 →
//      client RemoteAuthSession flips to Expired.
// The remaining scenarios are kept as skipped scaffolds:
//   - row-level scope narrowing requires CUR-1331 scoped-permission
//     fixture wiring on the harness;
//   - permission_revoked closing-all-affected-users is identical
//     server-side to role_unassigned (same _forceLogout path) but
//     requires policy fixtures to seed the right (role, perm) state;
//   - stale_data scenarios (role_assigned, permission_granted,
//     containment) require client-side stale_data envelope handling
//     which is not yet wired through the RemoteScope.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  // --- Subscribe-time authorization ---

  test(
    'subscribe to view without view-level perm gets subscription_denied',
    () async {
      final h = await ReactionRemoteTestHarness.open();
      addTearDown(h.close);

      // Deliberately do NOT seed `view:notes_today`. The harness's
      // TrustingAuthValidator surfaces every credential as activeRole
      // 'install', but TableBackedAuthorizationPolicy will refuse the
      // view-permission check because no grant row exists.
      h.scope.authSession.setCredential('alice');
      await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

      final stream = h.scope.viewSource.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      );

      // The subscription handler should emit a subscription_denied
      // envelope, which RemoteConnection._onMessage translates into a
      // stream error of the form 'subscription_denied: <reason>'.
      Object? caught;
      try {
        await stream.first.timeout(const Duration(seconds: 2));
      } catch (e) {
        caught = e;
      }
      expect(caught, isNotNull);
      expect(caught.toString(), contains('subscription_denied'));
      expect(caught.toString(), contains('view_permission_denied'));
    },
  );

  test(
    'row-level scope narrows aggregates',
    () async {
      // Seed Principal has scope on [a1, a2]; subscribe with
      // aggregates: [a1, a2, a3]; expect only a1, a2 rows.
    },
    skip: 'expand with CUR-1331 scoped-permission fixtures',
  );

  // --- Mid-session AuthzWatcher behavior ---
  //
  // The server-side AuthzWatcher is already wired (see
  // reaction/lib/src/server/authz_watcher.dart); these tests are
  // blocked on the client-side surfacing of WS close-code 4003 into
  // RemoteAuthSession.onAuthRejected, plus stale_data envelope
  // handling on the client.

  test('role_unassigned mid-subscription closes WS with 4003', () async {
    final h = await ReactionRemoteTestHarness.open();
    addTearDown(h.close);

    // Seed view perm + role membership + authenticate. The substrate now
    // requires a `user_role_scopes` row for (alice, install) before any
    // permission check authorizes. We seed with the same scope the
    // role_unassigned appended below targets, so that unassign actually
    // removes alice's only membership row (proving the AuthzWatcher
    // reacts to the projection becoming empty).
    await h.grantPermission(role: 'install', permission: 'view:notes_today');
    await h.assignRole(
      userId: 'alice',
      role: 'install',
      scope: const BoundScope(class_: 'site', value: 'A'),
    );
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

    // Open subscription and await EOR — this round-trips through the
    // WS handler and registers alice's connection in the
    // WsConnectionRegistry so the AuthzWatcher can find it later.
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );
    final updates = <Update<Map<String, Object?>>>[];
    final errors = <Object>[];
    final sub = stream.listen(updates.add, onError: errors.add);
    // Wait for the snapshot/EOR to settle.
    await Future<void>.delayed(const Duration(milliseconds: 200));

    // Append role_unassigned for alice. AuthzWatcher matches on
    // aggregateType=user_role_scope + eventType=role_unassigned and
    // closes alice's WS connection with 4003.
    const userId = 'alice';
    const role = 'install';
    const scope = BoundScope(class_: 'site', value: 'A');
    await h.substrate.eventStore.append(
      entryType: 'user_role_scope',
      aggregateType: 'user_role_scope',
      aggregateId: roleAssignmentAggregateId(
        userId: userId,
        role: role,
        scope: scope,
      ),
      eventType: 'role_unassigned',
      data: const RoleUnassignedPayload(
        userId: userId,
        role: role,
        scope: scope,
      ).toJson(),
      initiator: const AutomationInitiator(service: 'test'),
    );

    // Wait for the WS close-frame to arrive on the client and for the
    // RemoteConnection.onAuthClose callback (wired in RemoteScope) to
    // flip RemoteAuthSession to Expired.
    await h.scope.authSession.stream
        .firstWhere((s) => s is Expired)
        .timeout(const Duration(seconds: 2));
    // Pump once more so the error-out-subs loop inside _onWsClosed
    // delivers 'wire_disconnected' onto the subscription stream
    // (onAuthClose is fired first, then the loop runs).
    await Future<void>.delayed(const Duration(milliseconds: 50));

    expect(h.scope.authSession.current, isA<Expired>());
    // Subscription is wire-errored by the same _onWsClosed path.
    expect(
      errors.any((e) => e.toString().contains('wire_disconnected')),
      isTrue,
    );

    await sub.cancel();
  });

  test(
    'after force-logout, re-login + submit denies via substrate membership check',
    () async {
      // Pins the full reaction round-trip the substrate trust-model fix
      // (62b2bcc) was responding to: a user submits an action successfully,
      // an admin revokes their role mid-session, the AuthzWatcher closes
      // their WS, the user re-logs-in as the same identity, and the
      // substrate's membership gate denies the second submit. Without this
      // test, a regression in either the policy's user_role_scopes lookup
      // or the AuthzWatcher's projection trigger could silently re-
      // introduce the original bug.
      final h = await ReactionRemoteTestHarness.open();
      addTearDown(h.close);

      // Seed install->say_hello permission + (alice, install) membership.
      // The harness's TrustingAuthValidator surfaces any non-empty
      // credential as activeRole='install'.
      await h.grantPermission(role: 'install', permission: 'say_hello');
      await h.assignRole(userId: 'alice', role: 'install');

      // First login: succeeds.
      h.scope.authSession.setCredential('alice');
      await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

      // First submit: dispatch authorizes (alice holds 'install', role
      // grants 'say_hello').
      final firstResult = await h.scope.actionSubmitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'alice'},
        ),
      );
      expect(firstResult, isA<DispatchSuccess<Object?>>());

      // Open a subscription so alice's WS connection is registered in the
      // server's WsConnectionRegistry — without this the AuthzWatcher has
      // nothing to force-close. Use the same view-permission seeded
      // elsewhere in this file for consistency.
      await h.grantPermission(role: 'install', permission: 'view:notes_today');
      final stream = h.scope.viewSource.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      );
      final sub = stream.listen((_) {}, onError: (_) {});
      // Wait for snapshot/EOR to settle.
      await Future<void>.delayed(const Duration(milliseconds: 200));

      // Revoke alice's role via the substrate, mirroring what an
      // /admin/revoke route would do in a real app.
      await h.substrate.eventStore.append(
        entryType: 'user_role_scope',
        aggregateType: 'user_role_scope',
        aggregateId: roleAssignmentAggregateId(
          userId: 'alice',
          role: 'install',
          scope: const TotalWildcardScope(),
        ),
        eventType: 'role_unassigned',
        data: const RoleUnassignedPayload(
          userId: 'alice',
          role: 'install',
          scope: TotalWildcardScope(),
        ).toJson(),
        initiator: const AutomationInitiator(service: 'test-revoke'),
      );

      // AuthzWatcher closes alice's WS; client RemoteAuthSession flips
      // to Expired.
      await h.scope.authSession.stream
          .firstWhere((s) => s is Expired)
          .timeout(const Duration(seconds: 2));
      await sub.cancel();

      // Re-login as alice. The TrustingAuthValidator still accepts the
      // credential (the substrate's role_unassigned is invisible to it),
      // and authSession transitions back to Authenticated.
      h.scope.authSession.setCredential('alice');
      await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

      // Second submit: the substrate's membership gate (no user_role_scopes
      // row for (alice, install)) returns DispatchAuthorizationDenied. This
      // is the exact path the trust-model fix wired up.
      final secondResult = await h.scope.actionSubmitter.submit(
        const ActionSubmission(
          actionName: 'say_hello',
          rawInput: {'name': 'alice after revoke'},
        ),
      );
      expect(secondResult, isA<DispatchAuthorizationDenied<Object?>>());
      expect(
        (secondResult as DispatchAuthorizationDenied<Object?>).permission.name,
        equals('say_hello'),
      );
    },
  );

  test(
    'permission_revoked from held role closes all affected users',
    () async {
      // Open subs as alice (role X) and bob (role X). Append
      // permission_revoked(role: X, perm: ...). Assert: both WS
      // connections close with 4003.
    },
    skip: 'expand with CUR-1331 fixtures + client-side 4003 close-frame wiring',
  );

  test(
    'role_assigned mid-subscription sends stale_data envelope',
    () async {
      // Open subscription as alice (role X). Append role_assigned
      // adding alice to role Y with scope. Assert: client receives
      // stale_data with reason: role_assigned; subscription stays open.
    },
    skip: 'expand with CUR-1331 fixtures + client-side stale_data handling',
  );

  test(
    'permission_granted to held role sends stale_data',
    () async {
      // Open subscription as alice (role X). Append permission_granted
      // to role X. Assert: client receives stale_data with
      // reason: permission_added.
    },
    skip: 'expand with CUR-1331 fixtures + client-side stale_data handling',
  );

  test(
    'containment change does NOT emit stale_data by default',
    () async {
      // Without watchContainment(...): open subscription, mutate
      // patient_site_index, assert no stale_data is sent.
    },
    skip: 'expand with CUR-1331 fixtures + client-side stale_data handling',
  );

  test(
    'watchContainment(...) opt-in emits stale_data on projection change',
    () async {
      // Call harness.reaction.watchContainment('patient_site_index').
      // Open subscription. Mutate patient_site_index row. Assert:
      // client receives stale_data with reason: containment_changed.
    },
    skip: 'expand with CUR-1331 fixtures + client-side stale_data handling',
  );
}
