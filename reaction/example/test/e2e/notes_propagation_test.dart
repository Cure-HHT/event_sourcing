// reaction/example/test/e2e/notes_propagation_test.dart
//
// Tier-2 scenarios: multiple clients on one server, the reactive view
// path, and the write-side scope perimeter. Driven entirely through the
// real `RemoteScope` client against the real `reaction_example` server
// over HTTP + WebSocket.
//
// Scenarios in this file:
//   S1  cross-client live propagation + ordering + convergence
//   S2  late joiner gets a snapshot of prior state, then live deltas
//   S3  write-side scope perimeter holds independently per client

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'multiclient_harness.dart';

void main() {
  late DemoServer server;

  setUp(() async {
    server = await DemoServer.start();
  });
  tearDown(() => server.stop());

  test('S1: a note written by one client propagates to every subscriber, '
      'in order, and all clients converge to the same set', () async {
    // Three clients on one server: alice (editor-west), bob
    // (editor-east), dave (viewer). All three subscribe to notes_today.
    final alice = await server.login('alice');
    final bob = await server.login('bob');
    final dave = await server.login('dave');

    final aliceView = NotesObserver(alice, label: 'alice');
    final bobView = NotesObserver(bob, label: 'bob');
    final daveView = NotesObserver(dave, label: 'dave');

    // Let the (empty) replay settle so subsequent notes are LIVE deltas.
    await pumpUntil(
      () =>
          aliceView.sawEndOfReplay &&
          bobView.sawEndOfReplay &&
          daveView.sawEndOfReplay,
      describe: () => 'all observers reach EndOfReplay',
    );

    // carol (admin, any workspace) writes three notes in order.
    final carol = await server.login('carol');
    for (final t in const ['n1', 'n2', 'n3']) {
      final r = await submitNote(carol, workspace: 'west', title: t);
      expect(r, isA<DispatchSuccess<Object?>>(), reason: 'write $t');
    }

    // Every subscriber converges to the same three notes...
    const expected = {'n1', 'n2', 'n3'};
    await aliceView.waitForTitles(expected);
    await bobView.waitForTitles(expected);
    await daveView.waitForTitles(expected);

    // ...and observed them as LIVE deltas in log order (no reordering).
    expect(aliceView.liveDeltaTitles, ['n1', 'n2', 'n3']);
    expect(bobView.liveDeltaTitles, ['n1', 'n2', 'n3']);
    expect(daveView.liveDeltaTitles, ['n1', 'n2', 'n3']);

    // No errors surfaced on any subscription.
    expect(aliceView.errors, isEmpty);
    expect(bobView.errors, isEmpty);
    expect(daveView.errors, isEmpty);

    await aliceView.cancel();
    await bobView.cancel();
    await daveView.cancel();
  });

  test('S2: a late-joining client replays prior notes as a snapshot, '
      'then receives subsequent writes as live deltas', () async {
    final carol = await server.login('carol');

    // Two notes exist BEFORE the late joiner subscribes.
    expect(
      await submitNote(carol, workspace: 'west', title: 'early-1'),
      isA<DispatchSuccess<Object?>>(),
    );
    expect(
      await submitNote(carol, workspace: 'east', title: 'early-2'),
      isA<DispatchSuccess<Object?>>(),
    );

    // Late joiner subscribes and should replay both as a snapshot.
    final dave = await server.login('dave');
    final daveView = NotesObserver(dave, label: 'dave-late');
    await daveView.waitForTitles({'early-1', 'early-2'});
    await pumpUntil(
      () => daveView.sawEndOfReplay,
      describe: () => 'late joiner reaches EndOfReplay',
    );
    expect(daveView.snapshotTitles.toSet(), {'early-1', 'early-2'});
    expect(
      daveView.liveDeltaTitles,
      isEmpty,
      reason: 'prior notes arrive as snapshot, not live deltas',
    );

    // A subsequent write arrives as a LIVE delta.
    expect(
      await submitNote(carol, workspace: 'west', title: 'late-live'),
      isA<DispatchSuccess<Object?>>(),
    );
    await daveView.waitForTitles({'late-live'});
    expect(daveView.liveDeltaTitles, ['late-live']);

    await daveView.cancel();
  });

  test(
    'S3: the write-side scope perimeter holds independently per client',
    () async {
      final alice = await server.login('alice'); // editor @ west
      final bob = await server.login('bob'); // editor @ east

      // alice: west allowed, east denied.
      expect(
        await submitNote(alice, workspace: 'west', title: 'a-west'),
        isA<DispatchSuccess<Object?>>(),
      );
      final aliceEast = await submitNote(
        alice,
        workspace: 'east',
        title: 'a-east',
      );
      expect(aliceEast, isA<DispatchAuthorizationDenied<Object?>>());
      expect(
        (aliceEast as DispatchAuthorizationDenied<Object?>).permission.name,
        'submit_note',
      );

      // bob: east allowed, west denied — the mirror image.
      expect(
        await submitNote(bob, workspace: 'east', title: 'b-east'),
        isA<DispatchSuccess<Object?>>(),
      );
      final bobWest = await submitNote(bob, workspace: 'west', title: 'b-west');
      expect(bobWest, isA<DispatchAuthorizationDenied<Object?>>());
    },
  );
}
