// reaction/test/e2e/authz_test.dart
// Verifies: EVS-PRD-cross-process-event-transport/E (per-sub authz)
// + mid-session permission-change handling (force-logout + stale_data).
//
// Status: this file ships test 1 expanded (view-level deny). The
// remaining scenarios are kept as skipped scaffolds:
//   - row-level scope narrowing requires CUR-1331 scoped-permission
//     fixture wiring on the harness;
//   - force-logout / stale_data scenarios additionally require the
//     RemoteConnection client to surface the 4003 'permissions_changed'
//     close-frame back to RemoteAuthSession (today
//     RemoteConnection._onWsClosed errors all subs with
//     'wire_disconnected' regardless of close code, and there is no
//     auth-session callback wired through RemoteScope). Closing that
//     gap is impl work outside Task 32; tracking as a follow-up.
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

  test(
    'role_unassigned mid-subscription closes WS with 4003',
    () async {
      // Open subscription as alice. Append role_unassigned for alice.
      // Assert: WS closes with code 4003 within 200ms; AuthSession
      // flips to Expired.
    },
    skip: 'expand with CUR-1331 fixtures + client-side 4003 close-frame wiring',
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
