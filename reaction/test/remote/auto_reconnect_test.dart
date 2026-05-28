// Verifies: EVS-PRD-cross-process-event-transport/H — auto-reconnect
//   with exponential backoff on non-auth WS drops; re-issues every
//   active subscribe on successful reconnect; transitions to Disconnected
//   after maxAttempts; 4001/4003 carve-outs do NOT enter the cycle.
// Verifies: EVS-PRD-cross-process-event-transport/I — ConnectionStatus
//   transitions are driven by observable WS lifecycle events (initial
//   open success, drop, reconnect success, retry-exhausted), not by
//   synthesized pings or polling.

import 'dart:async';
import 'dart:convert';

import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Stub HTTP client; the auto-reconnect tests only exercise the WS path.
class _FakeHttpClient extends http.BaseClient {
  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async =>
      http.StreamedResponse(Stream.value(<int>[]), 200);
}

/// Factory of in-process WS channel pairs, designed to model real WS
/// lifecycle for the auto-reconnect tests. Each call to [build] returns
/// a fresh client-side [WebSocketChannel] and a controllable server-
/// side handle ([_FakeWsPair]); the factory itself records every pair
/// so the test can interact with the current generation.
///
/// In `failConnects: true` mode, each newly-built pair immediately
/// closes the client side with code 1006 (abnormal closure) before any
/// auth handshake can complete, simulating an unreachable server.
/// Otherwise, the test drives the server-side `acceptAuth()` once the
/// client has flushed its initial auth message.
class _FakeWsFactory {
  _FakeWsFactory({this.failConnects = false});

  bool failConnects;
  final List<_FakeWsPair> pairs = [];

  WebSocketChannel build(Uri _) {
    final pair = _FakeWsPair();
    pairs.add(pair);
    if (failConnects) {
      // Simulate "server unreachable": close the client side immediately
      // with 1006, after a microtask so the listener gets wired first.
      scheduleMicrotask(() => pair.serverCloseClient(1006));
    }
    return pair.clientSide;
  }

  /// The most-recently-built pair (i.e. the connection the client is
  /// currently using). null if none yet built.
  _FakeWsPair get latest => pairs.last;
}

/// In-process pair of [WebSocketChannel]s: anything the client's sink
/// emits arrives on `serverInbound`, and `serverSink.add` flows into
/// the client's stream. Modelled on `_Pair` in `remote_connection_test.dart`;
/// the difference is this version is reusable across multiple
/// generations (the factory builds a fresh pair on every reconnect).
class _FakeWsPair {
  _FakeWsPair() {
    serverSide = _MemChannel(
      stream: _clientToServer.stream,
      rawSink: _serverToClient.sink,
      closeCell: _serverCloseCell,
    );
    clientSide = _MemChannel(
      stream: _serverToClient.stream,
      rawSink: _clientToServer.sink,
      closeCell: _clientCloseCell,
    );
    // Buffer everything the client sends so tests can assert against
    // it after the fact (the production code may flush before the test
    // attaches a listener).
    _serverInboundSub = serverSide.stream.listen(
      (raw) => sentMessages.add(raw as String),
      onDone: _serverInboundDone.complete,
    );
  }

  final StreamController<Object?> _clientToServer = StreamController<Object?>();
  final StreamController<Object?> _serverToClient = StreamController<Object?>();
  final _CloseCodeCell _clientCloseCell = _CloseCodeCell();
  final _CloseCodeCell _serverCloseCell = _CloseCodeCell();
  late final _MemChannel serverSide;
  late final _MemChannel clientSide;
  late final StreamSubscription<dynamic> _serverInboundSub;
  final Completer<void> _serverInboundDone = Completer<void>();

  /// Every JSON message the client has sent on this connection.
  final List<String> sentMessages = [];

  /// Server-initiated close with a specific close code, surfacing on
  /// `clientSide.closeCode` (matching what
  /// `WebSocketChannel.closeCode` would report from a real WS close
  /// frame).
  Future<void> serverCloseClient(int code) async {
    _clientCloseCell.code = code;
    await _serverToClient.close();
  }

  /// Server-side accept: parse the first client message as an auth
  /// envelope and send back the `auth_ok` ack the production code
  /// awaits before flushing queued subscribes.
  Future<void> acceptAuth({String principalId = 'alice'}) async {
    // Wait until the client has actually sent its auth message.
    while (sentMessages.isEmpty) {
      await Future<void>.delayed(Duration.zero);
    }
    final first = jsonDecode(sentMessages.first) as Map<String, Object?>;
    expect(first['type'], 'auth', reason: 'first client message is auth');
    serverSide.sink.add(
      jsonEncode({'type': 'auth_ok', 'principalId': principalId}),
    );
  }

  Future<void> dispose() async {
    await _serverInboundSub.cancel();
    if (!_clientToServer.isClosed) await _clientToServer.close();
    if (!_serverToClient.isClosed) await _serverToClient.close();
  }
}

class _CloseCodeCell {
  int? code;
}

class _MemChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  _MemChannel({
    required Stream<Object?> stream,
    required StreamSink<Object?> rawSink,
    required _CloseCodeCell closeCell,
  }) : _stream = stream,
       _closeCell = closeCell,
       sink = _MemSink(rawSink);

  final Stream<Object?> _stream;
  final _CloseCodeCell _closeCell;

  @override
  Stream<dynamic> get stream => _stream;

  @override
  final WebSocketSink sink;

  @override
  int? get closeCode => _closeCell.code;

  @override
  String? get closeReason => null;

  @override
  String? get protocol => null;

  @override
  Future<void> get ready => Future<void>.value();
}

class _MemSink implements WebSocketSink {
  _MemSink(this._inner);

  final StreamSink<Object?> _inner;

  @override
  void add(Object? event) => _inner.add(event);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _inner.addError(error, stackTrace);

  @override
  Future<dynamic> addStream(Stream<Object?> stream) => _inner.addStream(stream);

  @override
  Future<dynamic> close([int? closeCode, String? closeReason]) =>
      _inner.close();

  @override
  Future<dynamic> get done => _inner.done;
}

/// Helper: settles every pending microtask + scheduled timer so the
/// reconnect loop's `Future.delayed` (with ms-scale intervals) drains
/// without burning test wall-clock. Repeats a handful of times because
/// each reconnect attempt yields control multiple times across the
/// loop -> _connect -> auth handshake chain.
Future<void> _pump({int iterations = 20}) async {
  for (var i = 0; i < iterations; i++) {
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

void main() {
  group('RemoteConnection auto-reconnect', () {
    test('on non-auth WS close, transitions Reconnecting then Connected '
        'on success', () async {
      final transitions = <ConnectionStatus>[];
      final factory = _FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: _FakeHttpClient(),
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
      await _pump();
      expect(transitions, [const Connected()]);

      // Server drops the connection with a non-auth close code (1006).
      await factory.latest.serverCloseClient(1006);

      // The reconnect loop awakes after `backoff.initial` (1ms), opens
      // a fresh channel (factory.latest now points at it), and re-issues
      // the subscribe. Accept auth on the new generation so the loop
      // can complete.
      await Future<void>.delayed(const Duration(milliseconds: 5));
      await factory.latest.acceptAuth();
      await _pump();

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
      final factory = _FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: _FakeHttpClient(),
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
      await _pump();
      expect(transitions.last, const Connected());

      // Flip the factory into failConnects mode for the reconnect attempts.
      factory.failConnects = true;
      await factory.latest.serverCloseClient(1006);

      // maxAttempts=2 * (1ms + 2ms backoff) plus some slack -> pump
      // ~50ms total.
      await _pump();

      expect(transitions.last, const Disconnected());

      for (final p in factory.pairs) {
        await p.dispose();
      }
      await conn.dispose();
    });

    test('4001 auth_rejected does NOT enter Reconnecting cycle', () async {
      final transitions = <ConnectionStatus>[];
      final factory = _FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: _FakeHttpClient(),
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
      await _pump();
      expect(transitions, [const Connected()]);
      transitions.clear();

      // Server force-closes with 4001 auth_rejected.
      await factory.latest.serverCloseClient(4001);
      await _pump();

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
        final factory = _FakeWsFactory();
        final conn = RemoteConnection(
          baseUrl: Uri.parse('http://test.local'),
          httpClient: _FakeHttpClient(),
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
        await _pump();
        transitions.clear();

        await factory.latest.serverCloseClient(4003);
        await _pump();

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
      final factory = _FakeWsFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://test.local'),
        httpClient: _FakeHttpClient(),
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
      await _pump();

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
      await _pump();

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
