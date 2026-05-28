// Verifies: EVS-PRD-cross-process-event-transport/H — auto-reconnect
//   with exponential backoff on non-auth WS drops; re-issues every
//   active subscribe on successful reconnect; transitions to Disconnected
//   after maxAttempts; 4001/4003 carve-outs do NOT enter the cycle.
// Verifies: EVS-PRD-cross-process-event-transport/I — ConnectionStatus
//   transitions are driven by observable WS lifecycle events (initial
//   open success, drop, reconnect success, retry-exhausted), not by
//   synthesized pings or polling.

import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_connection.dart';

import 'test_support/fake_ws.dart';

void main() {
  group('RemoteConnection auto-reconnect', () {
    test('on non-auth WS close, transitions Reconnecting then Connected '
        'on success', () async {
      final transitions = <ConnectionStatus>[];
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
      conn.onConnectionStatusChanged = transitions.add;

      // Open a subscription; the connection lazily connects.
      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});

      // Drive the initial handshake to auth_ok -> Connected.
      await factory.latest.acceptAuth();
      await pumpEventLoop();
      expect(transitions, [const Connected()]);

      // Server drops the connection with a non-auth close code (1006).
      await factory.latest.serverCloseClient(1006);

      // The reconnect loop awakes after `backoff.initial` (1ms), opens
      // a fresh channel (factory.latest now points at it), and re-issues
      // the subscribe. Accept auth on the new generation so the loop
      // can complete.
      await Future<void>.delayed(const Duration(milliseconds: 5));
      await factory.latest.acceptAuth();
      await pumpEventLoop();

      expect(transitions, [
        const Connected(),
        const Reconnecting(),
        const Connected(),
      ]);

      // Cleanup before dispose (don't strand the open StreamController).
      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });

    test('on retry-exhausted, transitions to Disconnected', () async {
      final transitions = <ConnectionStatus>[];
      // Allow the FIRST connect to succeed (so we have a subscription
      // registered when the WS drops), then force every retry to fail.
      final factory = FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: FakeHttpClient(),
        wsFactory: factory.build,
        reconnectBackoff: const ExponentialBackoff(
          initial: Duration(milliseconds: 1),
          maxAttempts: 2,
          multiplier: 2,
        ),
      );
      conn.onConnectionStatusChanged = transitions.add;

      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});

      await factory.latest.acceptAuth();
      await pumpEventLoop();
      expect(transitions.last, const Connected());

      // Flip the factory into failConnects mode for the reconnect attempts.
      factory.failConnects = true;
      await factory.latest.serverCloseClient(1006);

      // maxAttempts=2 * (1ms + 2ms backoff) plus some slack -> pump
      // ~50ms total.
      await pumpEventLoop();

      expect(transitions.last, const Disconnected());

      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });

    test('4001 auth_rejected does NOT enter Reconnecting cycle', () async {
      final transitions = <ConnectionStatus>[];
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
      conn.onConnectionStatusChanged = transitions.add;

      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});

      await factory.latest.acceptAuth();
      await pumpEventLoop();
      expect(transitions, [const Connected()]);
      transitions.clear();

      // Server force-closes with 4001 auth_rejected.
      await factory.latest.serverCloseClient(4001);
      await pumpEventLoop();

      // The carve-out path: NO Reconnecting transition, NO subsequent
      // Connected. The auth handler is invoked separately
      // (covered by remote_connection_test.dart's onAuthClose tests).
      expect(transitions, isEmpty);
      // Belt-and-braces: confirm no fresh WS generation was built.
      expect(factory.pairs, hasLength(1));

      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });

    test(
      '4003 permissions_changed does NOT enter Reconnecting cycle',
      () async {
        final transitions = <ConnectionStatus>[];
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
        conn.onConnectionStatusChanged = transitions.add;

        conn
            .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
            .listen((_) {}, onError: (_) {});
        await factory.latest.acceptAuth();
        await pumpEventLoop();
        transitions.clear();

        await factory.latest.serverCloseClient(4003);
        await pumpEventLoop();

        // Same carve-out as 4001: no Reconnecting cycle, no fresh WS open.
        expect(transitions, isEmpty);
        expect(factory.pairs, hasLength(1));

        for (final p in factory.pairs) {
          await p.dispose();
        }
        await conn.dispose();
      },
    );

    test('on successful reconnect, re-issues every active subscribe', () async {
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

      // Open two subscriptions on the first WS generation.
      conn
          .openSubscription(subscriptionId: 'sub-A', viewName: 'view_a')
          .listen((_) {}, onError: (_) {});
      conn
          .openSubscription(subscriptionId: 'sub-B', viewName: 'view_b')
          .listen((_) {}, onError: (_) {});

      await factory.latest.acceptAuth();
      await pumpEventLoop();

      // Sanity: the first-generation server saw exactly one auth +
      // two subscribes (subscribe order is map-iteration-order; we just
      // count the subscribe envelopes).
      final firstGenSubscribes = factory.pairs.first.sentMessages
          .where((m) => m.contains('"type":"subscribe"'))
          .toList();
      expect(firstGenSubscribes, hasLength(2));

      // Drop + reconnect.
      await factory.latest.serverCloseClient(1006);
      await Future<void>.delayed(const Duration(milliseconds: 5));
      await factory.latest.acceptAuth();
      await pumpEventLoop();

      // Second generation: one fresh auth + both subscribes re-issued.
      expect(factory.pairs, hasLength(2));
      final secondGenInbound = factory.pairs.last.sentMessages;
      expect(
        secondGenInbound.where((m) => m.contains('"type":"auth"')).toList(),
        hasLength(1),
        reason: 'reconnect resends the AuthMsg',
      );
      final resentSubscribes = secondGenInbound
          .where((m) => m.contains('"type":"subscribe"'))
          .toList();
      expect(
        resentSubscribes,
        hasLength(2),
        reason: 'both active subscribes are re-issued on reconnect',
      );
      // Both subscriptionIds appear in the resent set (order-independent).
      final resentSubIds = resentSubscribes
          .map((m) => jsonDecode(m) as Map<String, Object?>)
          .map((m) => m['subscriptionId'] as String)
          .toSet();
      expect(resentSubIds, {'sub-A', 'sub-B'});

      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });
  });
}
