// reaction/test/e2e/reconnect_test.dart
//
// Verifies: EVS-PRD-cross-process-event-transport/H — on non-auth WS
//   drops, RemoteConnection auto-reconnects with exponential backoff
//   and re-issues every active subscribe. The substrate's snapshot-
//   then-deltas semantics replays a fresh `Snapshot x N -> EndOfReplay
//   -> live` on the existing client-side stream (per `spec/reaction-
//   remote.md` Section 3 "Reconnect strategy (v1 baseline: refetch)").

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
    // Substrate-verified role membership: seed alice -> install.
    await h.assignRole(userId: 'alice', role: 'install');
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('server-side WS drop triggers auto-reconnect; existing sub re-replays '
      'in place', () async {
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

    // (2) Open one long-lived subscription and drain its initial
    // Snapshot x 2 -> EndOfReplay.
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );
    final received = <Update<Map<String, Object?>>>[];
    var errored = false;
    final sub = stream.listen(
      received.add,
      onError: (Object _) => errored = true,
    );
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(received.whereType<Snapshot<Map<String, Object?>>>().length, 2);
    expect(received.whereType<EndOfReplay<Map<String, Object?>>>().length, 1);

    // (3) Drop the server-side WS by closing every channel the
    // ws_connection_registry holds for alice. This is the same
    // sink-close mechanism the AuthzWatcher uses for force-logout,
    // minus the 4003 code (we want a generic drop, not a
    // permissions_changed signal that would also flip AuthSession to
    // Expired). Per `EVS-PRD-cross-process-event-transport`-H, this
    // is exactly the case the auto-reconnect loop covers.
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

    // (4) Wait for the auto-reconnect loop to fire (default backoff:
    // 250ms initial). The existing stream remains open and receives
    // a fresh Snapshot x 2 -> EndOfReplay on top of the originals.
    await Future<void>.delayed(const Duration(milliseconds: 800));

    expect(
      errored,
      isFalse,
      reason:
          'auto-reconnect should keep the existing subscription stream '
          'open; the wire_disconnected error path is reserved for the '
          'auth-rejected close codes (4001/4003) and retry-exhausted.',
    );
    expect(
      received.whereType<Snapshot<Map<String, Object?>>>().length,
      4,
      reason:
          'each re-issued subscribe replays both seeded notes; the '
          'original 2 + 2 from the reconnect = 4 total snapshots.',
    );
    expect(
      received.whereType<EndOfReplay<Map<String, Object?>>>().length,
      2,
      reason: 'one EndOfReplay per subscribe (initial + reconnect).',
    );

    await sub.cancel();
  });
}
