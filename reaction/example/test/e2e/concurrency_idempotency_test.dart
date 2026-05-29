// reaction/example/test/e2e/concurrency_idempotency_test.dart
//
// Tier-2 scenarios for concurrent multi-client writes and the
// per-principal idempotency contract — both observed end to end through
// the reactive view on a separate client.
//
// Scenarios:
//   S7  concurrent writes from three clients all land exactly once, with
//       no loss and no duplicate delivery, and a fourth client converges
//       to the union
//   S8  idempotency is keyed per (action, principal, key): a replay is a
//       cache hit (no new event), a same-key/different-input is a
//       mismatch denial (no new event), and another principal's reuse of
//       the same key is independent

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

  test('S7: concurrent writes from multiple clients all land exactly once '
      'and every subscriber converges to the union', () async {
    final alice = await server.login('alice'); // west
    final bob = await server.login('bob'); // east
    final carol = await server.login('carol'); // any
    final dave = await server.login('dave'); // observer

    final view = NotesObserver(dave, label: 'dave');
    await pumpUntil(
      () => view.sawEndOfReplay,
      describe: () => 'observer reaches EndOfReplay',
    );

    const perClient = 8;
    final expected = <String>{};
    final futures = <Future<DispatchResult<Object?>>>[];
    for (var i = 0; i < perClient; i++) {
      expected.add('alice-$i');
      expected.add('bob-$i');
      expected.add('carol-$i');
      futures
        ..add(submitNote(alice, workspace: 'west', title: 'alice-$i'))
        ..add(submitNote(bob, workspace: 'east', title: 'bob-$i'))
        ..add(submitNote(carol, workspace: 'west', title: 'carol-$i'));
    }
    final results = await Future.wait(futures);

    // Every concurrent dispatch succeeded.
    expect(
      results.every((r) => r is DispatchSuccess<Object?>),
      isTrue,
      reason: 'all concurrent writes should succeed',
    );

    // The observer converges to the full union...
    await view.waitForTitles(expected);
    // ...with NO lost and NO duplicated live deliveries.
    expect(
      view.liveDeltaTitles.length,
      expected.length,
      reason: 'each note delivered exactly once',
    );
    expect(
      view.liveDeltaTitles.toSet().length,
      view.liveDeltaTitles.length,
      reason: 'no duplicate delta delivery',
    );
    expect(view.titles, expected);
    expect(view.errors, isEmpty);

    await view.cancel();
  });

  test('S8: idempotency is keyed per (action, principal, key)', () async {
    final alice = await server.login('alice'); // west
    final carol = await server.login('carol'); // any
    final dave = await server.login('dave'); // observer
    final view = NotesObserver(dave, label: 'dave');
    await pumpUntil(
      () => view.sawEndOfReplay,
      describe: () => 'observer reaches EndOfReplay',
    );

    const key = 'K1';

    // (1) First submit under K1 creates a note.
    final first = await submitNote(
      alice,
      workspace: 'west',
      title: 'idem-A',
      idempotencyKey: key,
    );
    expect(first, isA<DispatchSuccess<Object?>>());

    // (2) Replay: same key, same input -> cache hit, no new event.
    final replay = await submitNote(
      alice,
      workspace: 'west',
      title: 'idem-A',
      idempotencyKey: key,
    );
    expect(replay, isA<DispatchIdempotencyHit<Object?>>());

    // (3) Same key, DIFFERENT input -> mismatch denial, no new event.
    final mismatch = await submitNote(
      alice,
      workspace: 'west',
      title: 'idem-A-changed',
      idempotencyKey: key,
    );
    expect(mismatch, isA<DispatchIdempotencyMismatch<Object?>>());

    // (4) A DIFFERENT principal reusing the same key is independent.
    final crossUser = await submitNote(
      carol,
      workspace: 'west',
      title: 'idem-carol',
      idempotencyKey: key,
    );
    expect(crossUser, isA<DispatchSuccess<Object?>>());

    // The view reflects exactly the two real notes; the replay added
    // nothing and the mismatch was rejected.
    await view.waitForTitles({'idem-A', 'idem-carol'});
    // Give any erroneous extra delta a chance to arrive, then assert
    // the mismatch title never materialized.
    await pumpUntil(
      () => view.titles.length >= 2,
      describe: () => 'two notes present',
    );
    expect(
      view.titles.contains('idem-A-changed'),
      isFalse,
      reason: 'mismatch must not create an event',
    );
    expect(view.titles, {'idem-A', 'idem-carol'});

    await view.cancel();
  });
}
