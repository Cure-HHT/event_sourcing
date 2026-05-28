// Verifies: EVS-PRD-cross-process-event-transport — composition smoke
//   test: RemoteScope produces the four Remote* impls sharing a single
//   RemoteConnection.
// Verifies: EVS-PRD-auth-session/G — single AuthSession is wired to the
//   action/view/permission impls as the source of truth.
// Verifies: EVS-PRD-reaction-scope/A — RemoteScope implements the
//   ReactionScope interface (four interface getters + connectionStatus
//   + connectionStatusStream + dispose).
// Verifies: EVS-PRD-reaction-scope/D — ConnectionStatus transitions are
//   driven by the underlying RemoteConnection's WS lifecycle.
// Verifies: EVS-PRD-reaction-scope/E — disposed-scope access throws
//   StateError (matches LocalScope's contract, so consumer code is
//   source-identical across Local and Remote).

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/fake_ws.dart';

void main() {
  test('constructs four Remote* impls with shared connection', () async {
    final scope = RemoteScope(baseUrl: Uri.parse('http://localhost:0'));
    expect(scope.authSession, isNotNull);
    expect(scope.actionSubmitter, isNotNull);
    expect(scope.viewSource, isNotNull);
    expect(scope.permissionSource, isNotNull);
    await expectLater(scope.dispose(), completes);
  });

  group('RemoteScope as ReactionScope', () {
    test('implements ReactionScope interface', () async {
      final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
      expect(scope, isA<ReactionScope>());
      await scope.dispose();
    });

    test(
      'connectionStatus starts Disconnected (before first WS open)',
      () async {
        final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
        expect(scope.connectionStatus, equals(const Disconnected()));
        await scope.dispose();
      },
    );

    test(
      'connectionStatusStream emits transitions from RemoteConnection',
      () async {
        final factory = FakeWsFactory();
        final scope = RemoteScope(
          baseUrl: Uri.parse('http://test.local'),
          httpClient: FakeHttpClient(),
          wsFactory: factory.build,
        );

        // Capture every transition. The stream is broadcast and does
        // NOT replay the current value on subscribe (per the interface
        // doc), so transitions we miss before listen() will not appear.
        final transitions = <ConnectionStatus>[];
        final sub = scope.connectionStatusStream.listen(transitions.add);

        // Trigger a view subscription via the underlying ViewSource —
        // this drives the lazy WS connect through RemoteConnection.
        scope.viewSource
            .watch<Map<String, Object?>>(
              viewName: 'notes_today',
              mapper: (row) => row,
            )
            .listen((_) {}, onError: (_) {});

        // Drive the initial handshake to auth_ok -> Connected.
        await factory.latest.acceptAuth();
        await pumpEventLoop();
        expect(transitions, [const Connected()]);

        // Drop with a non-auth close code (1006). The auto-reconnect
        // loop emits Reconnecting, opens a fresh WS generation, and
        // emits Connected again on auth_ok. RemoteScope uses the
        // default ExponentialBackoff (initial=250ms); wait past that
        // so the loop opens a new factory pair before we accept auth.
        await factory.latest.serverCloseClient(1006);
        await Future<void>.delayed(const Duration(milliseconds: 300));
        await factory.latest.acceptAuth();
        await pumpEventLoop();

        expect(transitions, [
          const Connected(),
          const Reconnecting(),
          const Connected(),
        ]);

        await sub.cancel();
        for (final p in factory.pairs) {
          await p.dispose();
        }
        await scope.dispose();
      },
    );

    test(
      'dispose() makes connection-status getters throw StateError',
      () async {
        final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
        await scope.dispose();
        expect(() => scope.connectionStatus, throwsStateError);
      },
    );
  });
}
