// reaction/test/e2e/permission_test.dart
// Verifies: EVS-PRD-permission-source/C/E
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('permission snapshot fetched on Authenticated', () async {
    // alice must actually hold her claimed activeRole ('install') for the
    // policy to return a non-null EffectiveAuthorization — without a
    // user_role_scopes row the policy returns EffectiveAuthorization.empty
    // (activeRole == ''), which both Local and Remote map to current=null
    // (closed-under-events: the activeRole claim is verified, not trusted).
    // Seed the assignment, then bounce the credential to force a re-fetch
    // on the next Authenticated transition.
    await h.assignRole(userId: 'alice', role: 'install');
    final naFuture = h.scope.authSession.stream.firstWhere(
      (s) => s is NotAuthenticated,
    );
    h.scope.authSession.setCredential(null);
    await naFuture;
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

    final snap = await h.scope.permissionSource.stream.firstWhere(
      (s) => s != null,
    );
    expect(snap, isNotNull);
    expect(snap!.activeRole, 'install');
  });

  test('granted permissions appear in the snapshot', () async {
    // Seed the grant + role membership after the initial setUp fetch,
    // then bounce the credential to force a re-fetch and observe the new
    // grant in the refreshed snapshot. The substrate requires a
    // `user_role_scopes` row for (alice, install) before
    // effectivePermissionsFor returns anything for the activeRole.
    // Subscribe to the auth stream BEFORE setCredential(null), because
    // that path is synchronous — a listener attached after the call
    // will miss the NotAuthenticated transition.
    await h.grantPermission(role: 'install', permission: 'say_hello');
    await h.assignRole(userId: 'alice', role: 'install');
    final naFuture = h.scope.authSession.stream.firstWhere(
      (s) => s is NotAuthenticated,
    );
    h.scope.authSession.setCredential(null);
    await naFuture;
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);

    final snap = await h.scope.permissionSource.stream.firstWhere(
      (s) => s != null && s.rolePermissions.isNotEmpty,
    );
    expect(snap!.activeRole, 'install');
    expect(snap.rolePermissions.any((p) => p.name == 'say_hello'), isTrue);
  });
}
