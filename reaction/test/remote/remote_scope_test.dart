import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:reaction/src/wire/principal_codec.dart';

import 'test_support/fake_ws.dart';

/// HTTP client that answers GET /me with a fixed [Principal] (driving
/// RemoteAuthSession to Authenticated) and answers
/// /permissions/snapshot with a fixed sequence of responses, repeating
/// the last one once the sequence is exhausted. Used to drive an
/// initial 200, a 503 view_converging, and a widened 200 through the
/// same connection.
class _SequencedSnapshotClient extends http.BaseClient {
  _SequencedSnapshotClient(this._principal, this._snapshotResponses);

  final Principal _principal;
  final List<http.StreamedResponse Function()> _snapshotResponses;
  int callCount = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    if (request.url.path == '/me') {
      return http.StreamedResponse(
        Stream.value(
          utf8.encode(jsonEncode(PrincipalCodec.encode(_principal))),
        ),
        200,
      );
    }
    final index = callCount < _snapshotResponses.length
        ? callCount
        : _snapshotResponses.length - 1;
    callCount++;
    return _snapshotResponses[index]();
  }
}

http.StreamedResponse _okResponse(EffectiveAuthorization auth) =>
    http.StreamedResponse(
      Stream.value(
        utf8.encode(jsonEncode(EffectiveAuthorizationCodec.encode(auth))),
      ),
      200,
    );

http.StreamedResponse _convergingResponse() => http.StreamedResponse(
  Stream.value(
    utf8.encode(
      jsonEncode({'error': 'view_converging', 'view': 'user_role_scopes'}),
    ),
  ),
  503,
);

void main() {
  // Verifies: EVS-PRD-reaction-scope/A+D
  test('constructs four Remote* impls with shared connection', () async {
    final scope = RemoteScope(baseUrl: Uri.parse('http://localhost:0'));
    expect(scope.authSession, isNotNull);
    expect(scope.actionSubmitter, isNotNull);
    expect(scope.viewSource, isNotNull);
    expect(scope.permissionSource, isNotNull);
    await expectLater(scope.dispose(), completes);
  });

  test('actionSubmitterMaxConvergingRetries reaches the RemoteActionSubmitter '
      'it builds (default off)', () async {
    final defaultScope = RemoteScope(baseUrl: Uri.parse('http://localhost:0'));
    expect(
      (defaultScope.actionSubmitter as RemoteActionSubmitter)
          .maxConvergingRetries,
      0,
    );
    await defaultScope.dispose();

    final optedInScope = RemoteScope(
      baseUrl: Uri.parse('http://localhost:0'),
      actionSubmitterMaxConvergingRetries: 3,
    );
    expect(
      (optedInScope.actionSubmitter as RemoteActionSubmitter)
          .maxConvergingRetries,
      3,
    );
    await optedInScope.dispose();
  });

  group('RemoteScope as ReactionScope', () {
    // Verifies: EVS-PRD-reaction-scope/A
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

    // Verifies: EVS-PRD-reaction-scope/D
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

    // Verifies: EVS-PRD-auth-session/G
    test(
      // Verifies: EVS-PRD-cross-process-event-transport/L
      'stale_data refresh retries a 503 view_converging '
      'without another stale_data envelope',
      () async {
        final before = EffectiveAuthorization(
          activeRole: 'clinician',
          rolePermissions: {const Permission('view:patient_diary')},
          scopeAssignments: const <ScopeAssignment>[],
        );
        final after = EffectiveAuthorization(
          activeRole: 'clinician',
          rolePermissions: {
            const Permission('view:patient_diary'),
            const Permission('edit:patient_diary'),
          },
          scopeAssignments: const <ScopeAssignment>[],
        );
        final principal = UserPrincipal(
          userId: 'u1',
          roles: const {'clinician'},
          activeRole: 'clinician',
        );
        final httpClient = _SequencedSnapshotClient(principal, [
          () => _okResponse(before),
          _convergingResponse,
          () => _okResponse(after),
        ]);
        final factory = FakeWsFactory();
        final scope = RemoteScope(
          baseUrl: Uri.parse('http://test.local'),
          httpClient: httpClient,
          wsFactory: factory.build,
        );
        addTearDown(scope.dispose);

        final snapshots = <EffectiveAuthorization?>[];
        final sub = scope.permissionSource.stream.listen(snapshots.add);
        addTearDown(sub.cancel);

        // Drive the WS handshake so the transport becomes Connected...
        scope.viewSource
            .watch<Map<String, Object?>>(
              viewName: 'notes_today',
              mapper: (row) => row,
            )
            .listen((_) {}, onError: (_) {});
        await factory.latest.acceptAuth();
        await pumpEventLoop();

        // ...then drive the AuthSession itself to Authenticated (GET /me),
        // which triggers the initial snapshot fetch (call #1, 200).
        scope.authSession.setCredential('token');
        await pumpEventLoop();

        expect(httpClient.callCount, 1);
        expect(snapshots.last?.rolePermissions, before.rolePermissions);

        // The server pushes a stale_data envelope. RemoteScope's handler
        // calls RemotePermissionSource.refresh(), which hits the 503
        // (call #2). The refusal must not be swallowed outright: it has
        // to schedule its own retry so the client converges on the
        // widened snapshot without waiting for another stale_data
        // envelope or the view's own next catch-up (there is none —
        // RemotePermissionSource is not itself a subscribed view).
        factory.latest.serverSide.sink.add(
          jsonEncode({'type': 'stale_data', 'reason': 'permission_added'}),
        );
        await pumpEventLoop();

        // Wait past the retry's backoff (base delay 200ms) for call #3.
        await Future<void>.delayed(const Duration(milliseconds: 500));

        expect(
          httpClient.callCount,
          3,
          reason: 'the retry must fire without another stale_data envelope',
        );
        expect(snapshots.last?.rolePermissions, after.rolePermissions);

        for (final p in factory.pairs) {
          await p.dispose();
        }
      },
    );

    // Verifies: EVS-PRD-reaction-scope/E
    test('dispose() makes all interface and connection-status getters '
        'throw StateError', () async {
      final scope = RemoteScope(baseUrl: Uri.parse('http://test.local'));
      await scope.dispose();
      // All six accessors must throw post-dispose (parity with
      // LocalScope's interface-getter test, plus the two
      // connection-status getters specific to ReactionScope).
      expect(() => scope.authSession, throwsStateError);
      expect(() => scope.actionSubmitter, throwsStateError);
      expect(() => scope.viewSource, throwsStateError);
      expect(() => scope.permissionSource, throwsStateError);
      expect(() => scope.connectionStatus, throwsStateError);
      expect(() => scope.connectionStatusStream, throwsStateError);
    });
  });
}
