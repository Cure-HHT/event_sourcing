// reaction/test/e2e/view_test.dart
// Verifies: EVS-PRD-view-subscriber/C/D, EVS-PRD-cross-process-event-transport/A-D
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
    // Seed install -> view:notes_today permission so the subscription
    // handler's view-level authz check (policy.isPermitted on
    // Permission('view:notes_today')) allows the subscribe. The harness's
    // TrustingAuthValidator surfaces every credential as activeRole
    // 'install'; TableBackedAuthorizationPolicy still requires a matching
    // grant row.
    await h.grantPermission(role: 'install', permission: 'view:notes_today');
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
  });
  tearDown(() => h.close());

  test('subscribe receives EndOfReplay when view is empty', () async {
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );
    final first = await stream.firstWhere((u) => u is EndOfReplay);
    expect(first, isA<EndOfReplay<Map<String, Object?>>>());
  });

  test('subscribe receives Snapshot x N -> EOR -> Delta sequence', () async {
    // (1) Pre-populate with N=2 notes via direct substrate append.
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

    // (2) Subscribe.
    final stream = h.scope.viewSource.watch<Map<String, Object?>>(
      viewName: 'notes_today',
      mapper: (m) => m,
    );

    // (3) Collect snapshots until EOR, then expect a future delta.
    final replay = <Update<Map<String, Object?>>>[];
    final sub = stream.listen(replay.add);
    await Future<void>.delayed(const Duration(milliseconds: 200));
    final snaps = replay.whereType<Snapshot<Map<String, Object?>>>().toList();
    final eor = replay.whereType<EndOfReplay<Map<String, Object?>>>().toList();
    expect(snaps, hasLength(2));
    expect(eor, hasLength(1));

    // (4) Append a third note; expect a Delta after EOR.
    await h.substrate.eventStore.append(
      aggregateType: 'note',
      aggregateId: 'note-3',
      entryType: 'note',
      eventType: 'note_updated',
      data: const {'title': 'third'},
      initiator: initiator,
    );
    await Future<void>.delayed(const Duration(milliseconds: 200));
    expect(replay.last, isA<Delta<Map<String, Object?>>>());
    await sub.cancel();
  });

  test('mapper transforms rows client-side', () async {
    await h.substrate.eventStore.append(
      aggregateType: 'note',
      aggregateId: 'note-1',
      entryType: 'note',
      eventType: 'note_updated',
      data: const {'title': 'hello'},
      initiator: UserPrincipal(
        userId: 'alice',
        roles: const {'install'},
        activeRole: 'install',
      ).toInitiator(),
    );
    final stream = h.scope.viewSource.watch<String>(
      viewName: 'notes_today',
      mapper: (m) => m['title'] as String,
    );
    final snap =
        await stream.firstWhere((u) => u is Snapshot<String>)
            as Snapshot<String>;
    expect(snap.value, 'hello');
  });
}
