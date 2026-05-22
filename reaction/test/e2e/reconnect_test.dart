// reaction/test/e2e/reconnect_test.dart
// Verifies baseline RemoteConnection drop behavior:
//   - When the server-side WS closes, all in-flight subscriptions
//     surface a 'wire_disconnected' error on the client stream.
//   - The next viewSource.watch() lazily re-establishes the WS
//     connection and replays the view's current state from snapshots.
//
// Note: This is the BASELINE behavior documented in
// `RemoteConnection._onWsClosed` (errors all subs; reconnect on next
// `openSubscription`). Full exponential-backoff auto-reconnect of
// existing subs is deferred.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    // Seed install -> view:notes_today permission so the subscription
    // handler's view-level authz check allows the subscribe. Mirrors
    // view_test.dart's setUp.
    await h.grantPermission(role: 'install', permission: 'view:notes_today');
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test(
    'server-side WS drop errors in-flight subs; next watch re-replays',
    () async {
      // (1) Pre-populate two notes so we can verify the replay shape.
      final initiator = UserPrincipal(
        userId: 'alice',
        roles: const {'install'},
        activeRole: 'install',
      ).toInitiator();
      await h.substrate.eventStore.append(
        aggregateType: 'note',
        aggregateId: 'note-1',
        entryType: 'note',
        eventType: 'note_updated',
        data: const {'title': 'first'},
        initiator: initiator,
      );
      await h.substrate.eventStore.append(
        aggregateType: 'note',
        aggregateId: 'note-2',
        entryType: 'note',
        eventType: 'note_updated',
        data: const {'title': 'second'},
        initiator: initiator,
      );

      // (2) First subscription: drain Snapshot x 2 -> EndOfReplay.
      final stream1 = h.scope.viewSource.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      );
      final firstError = Completer<Object>();
      final firstReplay = <Update<Map<String, Object?>>>[];
      final sub1 = stream1.listen(
        firstReplay.add,
        onError: (Object e) {
          if (!firstError.isCompleted) firstError.complete(e);
        },
      );
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(firstReplay.whereType<Snapshot<Map<String, Object?>>>().length, 2);
      expect(
        firstReplay.whereType<EndOfReplay<Map<String, Object?>>>().length,
        1,
      );

      // (3) Drop the server-side WS by closing every channel the
      // ws_connection_registry holds for alice. This is the same
      // sink-close mechanism the AuthzWatcher uses for force-logout,
      // minus the 4003 code (we want a generic drop, not a
      // permissions_changed signal that would also flip AuthSession to
      // Expired).
      final channels = h.reaction.connectionRegistry
          .channelsFor('alice')
          .toList();
      expect(
        channels,
        hasLength(1),
        reason: 'one WS connection should be registered for alice',
      );
      for (final ch in channels) {
        await ch.sink.close();
      }

      // (4) The in-flight subscription should surface
      // 'wire_disconnected' as a stream error (baseline behavior in
      // RemoteConnection._onWsClosed).
      final err = await firstError.future.timeout(const Duration(seconds: 2));
      expect(err.toString(), contains('wire_disconnected'));
      await sub1.cancel();

      // (5) Fresh watch should lazily reconnect and re-replay from
      // snapshots.
      final stream2 = h.scope.viewSource.watch<Map<String, Object?>>(
        viewName: 'notes_today',
        mapper: (m) => m,
      );
      final secondReplay = <Update<Map<String, Object?>>>[];
      final sub2 = stream2.listen(secondReplay.add);
      await Future<void>.delayed(const Duration(milliseconds: 400));
      expect(
        secondReplay.whereType<Snapshot<Map<String, Object?>>>().length,
        2,
        reason: 'reconnect should replay both seeded notes',
      );
      expect(
        secondReplay.whereType<EndOfReplay<Map<String, Object?>>>().length,
        1,
      );
      await sub2.cancel();
    },
  );
}
