// reaction/example/test/e2e/reactive_permissions_test.dart
//
// Tier-2 scenarios for MID-SESSION permission changes across clients —
// the reactive-security flows running end to end against the shipped
// `reaction_example` demo with its real scoped permissions and
// AuthorizationWatcher.
//
// Scenarios:
//   S4  admin grant mid-session -> stale_data -> client permission view
//       refreshes -> previously-denied write now succeeds (no re-login)
//   S5  admin revoke mid-session -> WS force-closed 4003 -> client Expired
//   S6  unknown bearer is denied both the read path and the write path

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';

import 'multiclient_harness.dart';

/// True if [auth] carries a BoundScope(workspace, [value]) assignment.
bool hasWorkspaceScope(EffectiveAuthorization? auth, String value) {
  if (auth == null) return false;
  return auth.scopeAssignments.any((a) {
    final s = a.scope;
    return s is BoundScope && s.class_ == 'workspace' && s.value == value;
  });
}

void main() {
  late DemoServer server;

  setUp(() async {
    server = await DemoServer.start();
  });
  tearDown(() => server.stop());

  test('S4: an admin grant mid-session refreshes the client permission view '
      'and unblocks a previously-denied write, with no re-login', () async {
    final alice = await server.login('alice'); // editor @ west only
    // Subscribe so alice has a live WS the AuthorizationWatcher can signal.
    final aliceView = NotesObserver(alice, label: 'alice');
    await pumpUntil(
      () => aliceView.sawEndOfReplay,
      describe: () => 'alice reaches EndOfReplay',
    );

    // Baseline: alice cannot write to east, and her cached permission
    // view shows no east assignment.
    final before = await submitNote(
      alice,
      workspace: 'east',
      title: 'x-east-denied',
    );
    expect(before, isA<DispatchAuthorizationDenied<Object?>>());
    expect(hasWorkspaceScope(alice.permissionSource.current, 'east'), isFalse);

    // carol (admin) grants alice editor @ east.
    final carol = await server.login('carol');
    final assign = await assignRole(
      carol,
      userId: 'alice',
      role: 'editor',
      workspace: 'east',
    );
    expect(assign, isA<DispatchSuccess<Object?>>());

    // The AuthorizationWatcher pushes stale_data to alice's WS; RemoteScope
    // re-fetches /permissions/snapshot; her cached view gains east.
    await pumpUntil(
      () => hasWorkspaceScope(alice.permissionSource.current, 'east'),
      describe: () =>
          'alice permission view refreshes to include east '
          '(current=${alice.permissionSource.current?.scopeAssignments})',
    );

    // And the substrate now authorizes the previously-denied write —
    // proving closed-under-events on the server independent of the
    // client cache.
    final after = await submitNote(
      alice,
      workspace: 'east',
      title: 'x-east-ok',
    );
    expect(after, isA<DispatchSuccess<Object?>>());

    await aliceView.cancel();
  });

  test('S5: an admin revoke mid-session force-closes the client WS (4003) '
      'and flips its auth session to Expired', () async {
    final alice = await server.login('alice');
    // Open a subscription so alice is registered in the WS registry.
    final aliceView = NotesObserver(alice, label: 'alice');
    await pumpUntil(
      () => aliceView.sawEndOfReplay,
      describe: () => 'alice reaches EndOfReplay',
    );

    // Revoke alice's seeded (editor, west) assignment via the demo's
    // no-auth escape hatch. AuthorizationWatcher sees role_unassigned for a
    // role alice holds and closes her WS with 4003.
    final res = await http.post(
      server.baseUrl.replace(
        path: '/admin/revoke',
        queryParameters: {'user': 'alice'},
      ),
    );
    expect(res.statusCode, 200, reason: '/admin/revoke -> ${res.body}');

    // The 4003 close may flip the session to Expired before we attach the
    // listener below. authSession.stream is broadcast and does not replay
    // its current value, so check `current` first and only await a
    // subsequent transition if it has not already happened. (The check and
    // the firstWhere attach with no await between them, so no Expired
    // emission can slip through the gap.)
    if (alice.authSession.current is! Expired) {
      await alice.authSession.stream
          .firstWhere((s) => s is Expired)
          .timeout(
            const Duration(seconds: 5),
            onTimeout: () => throw StateError('alice never expired'),
          );
    }
    expect(alice.authSession.current, isA<Expired>());

    await aliceView.cancel();
  });

  test(
    'S6: an unknown bearer is denied both the read path and the write path',
    () async {
      final zoe = server.client();
      zoe.authSession.setCredential('zoe'); // not a seeded user

      // Read path: subscribing to notes_today is refused with a
      // subscription_denied(view_permission_denied) envelope, surfaced as
      // a stream error.
      final stream = zoe.viewSource.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      );
      Object? readError;
      try {
        await stream.first.timeout(const Duration(seconds: 3));
      } on Object catch (e) {
        readError = e;
      }
      expect(readError, isNotNull, reason: 'expected a subscription error');
      expect(readError.toString(), contains('subscription_denied'));

      // Write path: submitting an action is denied. Depending on how the
      // anonymous principal surfaces, this is either a server-side
      // authorization denial or a transport-level rejection — both are
      // "denied", neither is a success.
      Object? writeOutcome;
      try {
        writeOutcome = await submitNote(zoe, workspace: 'west', title: 'nope');
      } on Object catch (e) {
        writeOutcome = e;
      }
      expect(
        writeOutcome,
        isNot(isA<DispatchSuccess<Object?>>()),
        reason: 'unknown bearer must never write successfully',
      );
    },
  );
}
