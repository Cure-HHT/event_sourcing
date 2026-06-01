// Verifies: EVS-PRD-cross-process-event-transport/H — RemoteScope.reconnect()
//   and RemoteConnection.reconnect() manually trigger the same re-auth +
//   re-issue path as the auto-reconnect loop, re-authenticating with the
//   CURRENT credential and re-issuing every active subscribe.

import 'dart:async';
import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_connection.dart';

import 'test_support/fake_ws.dart';

void main() {
  group('RemoteConnection.reconnect()', () {
    test('no-op when no socket is open', () async {
      // No subscription opened yet -> no WS opened; reconnect() should
      // return without throwing.
      final factory = FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: factory.build,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 3,
          multiplier: 2,
        ),
      );
      await conn.reconnect(); // should not throw
      expect(factory.pairs, isEmpty, reason: 'no WS opened');
      await conn.dispose();
    });

    test('no-op after dispose', () async {
      final factory = FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: factory.build,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 3,
          multiplier: 2,
        ),
      );
      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});
      await factory.latest.acceptAuth();
      await pumpEventLoop();
      await conn.dispose();

      // After dispose, reconnect() should be a no-op.
      await conn.reconnect(); // should not throw
      // Only the one WS generation was opened.
      expect(factory.pairs, hasLength(1));

      for (final p in factory.pairs) {
        await p.dispose();
      }
    });

    test(
      're-sends AuthMessage and re-issues active subscribe on reconnect',
      () async {
        final factory = FakeWsFactory();
        final conn = RemoteConnection(
          baseUrl: Uri.parse('http://test.local'),
          httpClient: FakeHttpClient(),
          wsFactory: factory.build,
          reconnectBackoff: const ExponentialBackoff(
            initial: Duration(milliseconds: 1),
            maxAttempts: 3,
            multiplier: 2,
          ),
        );

        conn
            .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
            .listen((_) {}, onError: (_) {});

        // Drive initial connect + auth.
        await factory.latest.acceptAuth();
        await pumpEventLoop();
        expect(factory.pairs, hasLength(1));

        // Sanity: first generation sent one auth + one subscribe.
        final firstGen = factory.pairs.first;
        expect(
          firstGen.sentMessages.where((m) => m.contains('"type":"auth"')),
          hasLength(1),
        );
        expect(
          firstGen.sentMessages.where((m) => m.contains('"type":"subscribe"')),
          hasLength(1),
        );

        // Call reconnect(); the loop opens a fresh WS generation.
        unawaited(conn.reconnect());

        // Accept auth on the new generation so the loop can complete.
        await Future<void>.delayed(const Duration(milliseconds: 10));
        await factory.latest.acceptAuth();
        await pumpEventLoop();

        // A second WS generation was opened.
        expect(factory.pairs, hasLength(2));
        final secondGen = factory.pairs.last;

        // Re-auth was sent on the new generation.
        expect(
          secondGen.sentMessages.where((m) => m.contains('"type":"auth"')),
          hasLength(1),
          reason: 're-authentication sent on reconnect',
        );

        // The subscription was re-issued on the new generation.
        final resentSubscribes = secondGen.sentMessages
            .where((m) => m.contains('"type":"subscribe"'))
            .toList();
        expect(
          resentSubscribes,
          hasLength(1),
          reason: 'active subscribe re-issued on reconnect',
        );
        final subId =
            (jsonDecode(resentSubscribes.first)
                as Map<String, Object?>)['subscriptionId'];
        expect(subId, 'sub-1');

        for (final p in factory.pairs) {
          await p.dispose();
        }
        await conn.dispose();
      },
    );

    test(
      'late close echo on old channel does not clobber new generation (Case B)',
      () async {
        // Regression test: before the _wsStreamSub fix, the old channel's
        // real close-handshake echo could arrive AFTER reconnect() had
        // established a new _channel, firing a second _onWsClosed() that
        // nulled the NEW channel's _authComplete and forced a wasted retry.
        //
        // We simulate the echo by manually closing the OLD FakeWsPair's
        // server-to-client controller AFTER reconnect has produced a new
        // generation. If the bug were present the new channel's _authComplete
        // would be clobbered and the subscription would error.
        final factory = FakeWsFactory();
        final conn = RemoteConnection(
          baseUrl: Uri.parse('http://test.local'),
          httpClient: FakeHttpClient(),
          wsFactory: factory.build,
          reconnectBackoff: const ExponentialBackoff(
            initial: Duration(milliseconds: 1),
            maxAttempts: 3,
            multiplier: 2,
          ),
        );

        final errors = <Object>[];
        conn
            .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
            .listen((_) {}, onError: errors.add);

        // First generation auth.
        await factory.latest.acceptAuth();
        await pumpEventLoop();
        final firstGen = factory.pairs.first;
        expect(factory.pairs, hasLength(1));

        // Start reconnect and let the reconnect loop open a new generation.
        unawaited(conn.reconnect());
        await Future<void>.delayed(const Duration(milliseconds: 10));
        await factory.latest.acceptAuth();
        await pumpEventLoop();
        expect(factory.pairs, hasLength(2), reason: 'new WS generation opened');

        // NOW fire the stale close echo on the OLD generation — simulates
        // the real-WS close-handshake arriving late.
        await firstGen.serverCloseClient(1000);
        await pumpEventLoop();

        // The new generation must still be alive: no errors on the sub,
        // and the connection is Connected.
        expect(
          errors,
          isEmpty,
          reason:
              'stale close echo on old channel must not error the subscription',
        );
        expect(
          conn.connectionStatus,
          isA<Connected>(),
          reason:
              'stale close echo must not move the new connection out of Connected',
        );

        for (final p in factory.pairs) {
          await p.dispose();
        }
        await conn.dispose();
      },
    );

    test('re-auths with the CURRENT credential after setCredential', () async {
      final factory = FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: factory.build,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 3,
          multiplier: 2,
        ),
      );

      conn.setCredential('alice');
      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});

      await factory.latest.acceptAuth();
      await pumpEventLoop();

      // First gen auth carried 'alice'.
      final firstGenAuth =
          jsonDecode(
                factory.pairs.first.sentMessages.firstWhere(
                  (m) => m.contains('"type":"auth"'),
                ),
              )
              as Map<String, Object?>;
      expect(firstGenAuth['credential'], 'alice');

      // Switch credential (e.g. active-role switch issues a new token).
      conn.setCredential('alice-as-admin');

      // Reconnect with the new credential.
      unawaited(conn.reconnect());
      await Future<void>.delayed(const Duration(milliseconds: 10));
      await factory.latest.acceptAuth();
      await pumpEventLoop();

      expect(factory.pairs, hasLength(2));
      final secondGenAuth =
          jsonDecode(
                factory.pairs.last.sentMessages.firstWhere(
                  (m) => m.contains('"type":"auth"'),
                ),
              )
              as Map<String, Object?>;
      expect(
        secondGenAuth['credential'],
        'alice-as-admin',
        reason: 're-auth uses the credential set AFTER setCredential()',
      );

      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });
  });

  group('RemoteScope.reconnect()', () {
    test('delegates to connection; re-auth + re-issue on active sub', () async {
      final factory = FakeWsFactory();
      final scope = RemoteScope(
        baseUrl: Uri.parse('http://test.local'),
        wsFactory: factory.build,
        httpClient: FakeHttpClient(),
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 3,
          multiplier: 2,
        ),
      );

      // Watch a view to register a live subscription (drives lazy WS connect).
      final sub = scope.viewSource
          .watch<Map<String, Object?>>(
            viewName: 'notes_today',
            mapper: (m) => m,
          )
          .listen((_) {}, onError: (_) {});

      // Drive auth_ok on the first generation.
      await factory.latest.acceptAuth();
      await pumpEventLoop();
      expect(factory.pairs, hasLength(1));

      // Call reconnect() via RemoteScope.
      unawaited(scope.reconnect());
      await Future<void>.delayed(const Duration(milliseconds: 10));
      await factory.latest.acceptAuth();
      await pumpEventLoop();

      expect(
        factory.pairs,
        hasLength(2),
        reason: 'scope.reconnect() opened a second WS generation',
      );
      // Re-auth was sent on the new generation.
      expect(
        factory.pairs.last.sentMessages.where(
          (m) => m.contains('"type":"auth"'),
        ),
        hasLength(1),
        reason: 'RemoteScope.reconnect() re-authenticates',
      );
      // Subscribe was re-issued.
      expect(
        factory.pairs.last.sentMessages.where(
          (m) => m.contains('"type":"subscribe"'),
        ),
        hasLength(greaterThanOrEqualTo(1)),
        reason: 'RemoteScope.reconnect() re-issues live subscribes',
      );

      await sub.cancel();
      for (final p in factory.pairs) {
        await p.dispose();
      }
      await scope.dispose();
    });

    test('throws StateError after dispose', () async {
      final scope = RemoteScope(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
      );
      await scope.dispose();
      expect(() => scope.reconnect(), throwsStateError);
    });
  });
}
