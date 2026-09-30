// Verifies: EVS-PRD-cross-process-event-transport/A
// Update<T> envelope
//   round-trip through openSubscription.
// Verifies: EVS-PRD-cross-process-event-transport/B
// sequence +
//   subscriptionId carried on every routed envelope.
// Verifies: EVS-PRD-cross-process-event-transport/D
// multiplex multiple
//   subscriptions over a single WebSocket.
// Verifies: EVS-PRD-cross-process-event-transport/F
// bearer credential
//   injection on HTTP POST + WS auth message.
// Verifies: EVS-PRD-cross-process-event-transport/K
// a view_converging
//   error frame naming a subscriptionId surfaces a typed
//   ViewConvergingRefusal on that subscription's stream.
// Verifies: EVS-PRD-cross-process-event-transport/L
// a subscription
//   refused with view_converging recovers from its caller's single
//   openSubscription() call, with no bound on the number of retries;
//   cancelling the subscription stops the retries.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

class _FakeHttpClient extends http.BaseClient {
  http.BaseRequest? lastRequest;
  http.Response? response;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    lastRequest = request;
    return http.StreamedResponse(
      Stream.value(response?.bodyBytes ?? <int>[]),
      response?.statusCode ?? 200,
    );
  }
}

/// In-process pair of [WebSocketChannel]s for tests: anything the
/// `clientSide` sink emits arrives on `serverSide.stream`, and vice
/// versa. The two `_MemChannel` halves share a `_CloseCodeCell`, so a
/// `clientSide.sink.close(code, _)` on either end (or a test-driven
/// `setClientCloseCode(code)`) becomes observable as
/// `clientSide.closeCode` from the perspective of the side under test.
class _Pair {
  _Pair() {
    _clientToServer = StreamController<Object?>();
    _serverToClient = StreamController<Object?>();
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
  }

  late final StreamController<Object?> _clientToServer;
  late final StreamController<Object?> _serverToClient;
  final _CloseCodeCell _clientCloseCell = _CloseCodeCell();
  final _CloseCodeCell _serverCloseCell = _CloseCodeCell();
  late final _MemChannel serverSide;
  late final _MemChannel clientSide;

  /// Simulate the server-side closing the WS with a specific code,
  /// surfacing it on `clientSide.closeCode` and ending its stream.
  Future<void> serverCloseClient(int code) async {
    _clientCloseCell.code = code;
    await _serverToClient.close();
  }

  Future<void> close() async {
    await _clientToServer.close();
    await _serverToClient.close();
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

void main() {
  test('credential round-trips through setCredential/credential', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.credential, isNull);
    conn.setCredential('alice');
    expect(conn.credential, 'alice');
    conn.setCredential(null);
    expect(conn.credential, isNull);
  });

  test('HTTP requests include bearer header from credential', () async {
    final client = _FakeHttpClient()
      ..response = http.Response(
        '{"kind":"user","userId":"u","activeRole":"i"}',
        200,
      );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    )..setCredential('alice');
    await conn.httpGet(Uri.parse('http://localhost:1234/me'));
    expect(client.lastRequest!.headers['Authorization'], 'Bearer alice');
  });

  test('HTTP requests omit auth header when credential is null', () async {
    final client = _FakeHttpClient()..response = http.Response('ok', 200);
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    await conn.httpGet(Uri.parse('http://localhost:1234/healthz'));
    expect(client.lastRequest!.headers.containsKey('Authorization'), isFalse);
  });

  test('wsUrl derives ws scheme from http baseUrl', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:1234'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.wsUrl.scheme, 'ws');
    expect(conn.wsUrl.path, '/subscriptions');
  });

  test('wsUrl derives wss scheme from https baseUrl', () {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('https://api.example.com'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => throw UnimplementedError(),
    );
    expect(conn.wsUrl.scheme, 'wss');
    expect(conn.wsUrl.path, '/subscriptions');
  });

  test(
    'openSubscription returns a stream that receives routed envelopes',
    () async {
      final pair = _Pair();
      addTearDown(pair.close);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
      )..setCredential('alice');

      // Drain server-inbound traffic so the connection's dispose() can
      // close its sink without hanging on an undrained listener.
      pair.serverSide.stream.listen((_) {});

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );

      // Subscribe before the events arrive.
      final firstFuture = stream.first;

      // Wait a microtask so _ensureConnected/_connect/_sendClient flush.
      await Future<void>.delayed(Duration.zero);

      // Simulate server response: auth_ok then a snapshot envelope.
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'snapshot',
          'subscriptionId': 'sub-1',
          'sequence': 1,
          'value': {'aggregateId': 'a-1', 'k': 'v'},
        }),
      );

      final first = await firstFuture;
      expect(first, isA<Snapshot<Map<String, Object?>>>());
      await conn.dispose();
    },
  );

  test(
    'SubscribeMsg is not sent until auth_ok arrives (slow validator)',
    () async {
      final pair = _Pair();
      addTearDown(pair.close);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
      )..setCredential('alice');

      // Record the order/types of messages the server receives. With a
      // slow validator the server holds off on auth_ok; the client must
      // NOT push a subscribe in the meantime — sending one before auth_ok
      // would be an auth-protocol violation.
      final serverInbound = <String>[];
      pair.serverSide.stream.listen((raw) {
        final type = (jsonDecode(raw as String) as Map)['type'] as String?;
        if (type != null) serverInbound.add(type);
      });

      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});

      // Let the auth message flush and give the client ample time to (if
      // it were buggy) race ahead with a subscribe before auth_ok.
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(serverInbound, [
        'auth',
      ], reason: 'subscribe must not precede auth_ok');

      // Now the (slow) validator completes: server acks auth.
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(serverInbound, [
        'auth',
        'subscribe',
      ], reason: 'subscribe flushes only after auth_ok');
      await conn.dispose();
    },
  );

  test('handshake failure (ws drops before auth_ok) errors the subscription '
      'instead of hanging', () async {
    final pair = _Pair();
    addTearDown(pair.close);
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: _FakeHttpClient(),
      wsFactory: (_) => pair.clientSide,
    )..setCredential('alice');
    pair.serverSide.stream.listen((_) {});

    final errored = Completer<Object>();
    conn
        .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
        .listen(
          (_) {},
          onError: (Object e) {
            if (!errored.isCompleted) errored.complete(e);
          },
        );
    await Future<void>.delayed(Duration.zero);

    // Server closes with 4001 before ever sending auth_ok.
    await pair.serverCloseClient(4001);

    final err = await errored.future.timeout(const Duration(seconds: 2));
    expect(err, isNotNull);
    await conn.dispose();
  });

  group('_onWsClosed close-code routing', () {
    for (final code in [4001, 4003]) {
      test('WS close with code $code invokes onAuthClose', () async {
        final pair = _Pair();
        addTearDown(pair.close);
        var authCloseCount = 0;
        final conn =
            RemoteConnection(
                baseUrl: Uri.parse('http://localhost:0'),
                httpClient: _FakeHttpClient(),
                wsFactory: (_) => pair.clientSide,
              )
              ..onAuthClose = () {
                authCloseCount++;
              }
              ..setCredential('alice');

        // Drain server-inbound traffic so the connection's flush
        // doesn't hang on an undrained listener.
        pair.serverSide.stream.listen((_) {});

        // Open a sub to drive _ensureConnected -> _connect, which
        // attaches the onDone handler that fires _onWsClosed.
        // Listen with onError to absorb the 'wire_disconnected' error
        // that _onWsClosed forwards into every open sub controller;
        // otherwise the error escapes as an uncaught exception.
        conn
            .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
            .listen((_) {}, onError: (_) {});
        await Future<void>.delayed(Duration.zero);

        // Simulate server-initiated close with the auth-related code.
        await pair.serverCloseClient(code);
        await Future<void>.delayed(const Duration(milliseconds: 10));

        expect(authCloseCount, 1);
        await conn.dispose();
      });
    }

    test('stale_data envelope invokes onStaleData with reason', () async {
      final pair = _Pair();
      addTearDown(pair.close);
      final reasons = <String?>[];
      final conn =
          RemoteConnection(
              baseUrl: Uri.parse('http://localhost:0'),
              httpClient: _FakeHttpClient(),
              wsFactory: (_) => pair.clientSide,
            )
            ..onStaleData = reasons.add
            ..setCredential('alice');

      pair.serverSide.stream.listen((_) {});
      // Open a sub to drive _ensureConnected -> _connect (which wires
      // _onMessage).
      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});
      await Future<void>.delayed(Duration.zero);

      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      pair.serverSide.sink.add(
        jsonEncode({'type': 'stale_data', 'reason': 'role_assigned'}),
      );
      pair.serverSide.sink.add(jsonEncode({'type': 'stale_data'}));
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(reasons, ['role_assigned', null]);
      await conn.dispose();
    });

    test('WS close with code 1000 does NOT invoke onAuthClose', () async {
      final pair = _Pair();
      addTearDown(pair.close);
      var authCloseCount = 0;
      final conn =
          RemoteConnection(
              baseUrl: Uri.parse('http://localhost:0'),
              httpClient: _FakeHttpClient(),
              wsFactory: (_) => pair.clientSide,
            )
            ..onAuthClose = () {
              authCloseCount++;
            }
            ..setCredential('alice');

      pair.serverSide.stream.listen((_) {});
      conn
          .openSubscription(subscriptionId: 'sub-1', viewName: 'notes_today')
          .listen((_) {}, onError: (_) {});
      await Future<void>.delayed(Duration.zero);

      // Normal close (1000): not an auth-revocation signal.
      await pair.serverCloseClient(1000);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(authCloseCount, 0);
      await conn.dispose();
    });
  });

  group('view_converging subscription refusal', () {
    test('a view_converging error frame surfaces a typed ViewConvergingRefusal '
        'on that subscription naming the view, not a generic string', () async {
      // Verifies: EVS-PRD-cross-process-event-transport/K
      // R13: the refusal carries the subscriptionId so
      // RemoteConnection can route it to the refused subscription's
      // stream as a typed, transient error rather than dropping it
      // as unaddressed or rendering it indistinguishably from a
      // permission denial.
      final pair = _Pair();
      addTearDown(pair.close);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
      )..setCredential('alice');

      pair.serverSide.stream.listen((_) {});

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );
      final errorFuture = stream.first.then<Object?>(
        (v) => v,
        onError: (Object e) => e,
      );

      await Future<void>.delayed(Duration.zero);
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      await Future<void>.delayed(Duration.zero);
      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'error',
          'code': 'view_converging',
          'message': 'notes_today',
          'subscriptionId': 'sub-1',
        }),
      );

      final result = await errorFuture;
      expect(result, isA<ViewConvergingRefusal>());
      expect((result as ViewConvergingRefusal).viewName, 'notes_today');

      await conn.dispose();
    });
  });

  group('view_converging subscription recovery', () {
    test('a subscription refused with view_converging N times then served '
        'delivers its data with no new request from the caller, surfacing '
        'the converging condition meanwhile', () async {
      // Verifies: EVS-PRD-cross-process-event-transport/K
      // Verifies: EVS-PRD-cross-process-event-transport/L
      final pair = _Pair();
      addTearDown(pair.close);
      var subscribeCount = 0;
      final serverSub = pair.serverSide.stream.listen((raw) {
        final json = jsonDecode(raw as String) as Map<String, Object?>;
        if (json['type'] == 'subscribe') subscribeCount++;
      });
      addTearDown(serverSub.cancel);

      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
        convergingRetryScheduler: _immediateTimer,
      )..setCredential('alice');

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );
      final errors = <Object>[];
      Update<Map<String, Object?>>? received;
      final sub = stream.listen((u) => received = u, onError: errors.add);
      addTearDown(sub.cancel);

      await Future<void>.delayed(Duration.zero);
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      await Future<void>.delayed(Duration.zero);
      expect(subscribeCount, 1);

      // N view_converging refusals, N above the old bounded-retry
      // count (5) that this behaviour must no longer cap at.
      const n = 8;
      for (var i = 0; i < n; i++) {
        pair.serverSide.sink.add(
          jsonEncode({
            'type': 'error',
            'code': 'view_converging',
            'message': 'notes_today',
            'subscriptionId': 'sub-1',
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }

      expect(
        subscribeCount,
        n + 1,
        reason:
            'each refusal must re-issue the subscribe on its own; the '
            'test never called openSubscription again',
      );
      expect(errors, hasLength(n));
      expect(errors.every((e) => e is ViewConvergingRefusal), isTrue);

      // The server finally serves it.
      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'snapshot',
          'subscriptionId': 'sub-1',
          'sequence': 1,
          'value': {'aggregateId': 'a-1', 'k': 'v'},
        }),
      );
      await Future<void>.delayed(Duration.zero);

      expect(received, isA<Snapshot<Map<String, Object?>>>());

      await conn.dispose();
    });

    test(
      'cancelling the subscription stops further converging retries',
      () async {
        // Verifies: EVS-PRD-cross-process-event-transport/L
        final pair = _Pair();
        addTearDown(pair.close);
        var subscribeCount = 0;
        final serverSub = pair.serverSide.stream.listen((raw) {
          final json = jsonDecode(raw as String) as Map<String, Object?>;
          if (json['type'] == 'subscribe') subscribeCount++;
        });
        addTearDown(serverSub.cancel);

        final factory = _CapturingTimerFactory();
        final conn = RemoteConnection(
          baseUrl: Uri.parse('http://localhost:0'),
          httpClient: _FakeHttpClient(),
          wsFactory: (_) => pair.clientSide,
          convergingRetryScheduler: factory.schedule,
        )..setCredential('alice');

        final stream = conn.openSubscription(
          subscriptionId: 'sub-1',
          viewName: 'notes_today',
        );
        final sub = stream.listen((_) {}, onError: (_) {});

        await Future<void>.delayed(Duration.zero);
        pair.serverSide.sink.add(
          jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
        );
        await Future<void>.delayed(Duration.zero);
        expect(subscribeCount, 1);

        pair.serverSide.sink.add(
          jsonEncode({
            'type': 'error',
            'code': 'view_converging',
            'message': 'notes_today',
            'subscriptionId': 'sub-1',
          }),
        );
        await Future<void>.delayed(Duration.zero);

        final scheduledTimer = factory.lastTimer;
        expect(scheduledTimer, isNotNull);
        expect(scheduledTimer!.isActive, isTrue);

        await sub.cancel();
        await Future<void>.delayed(Duration.zero);

        expect(
          scheduledTimer.isActive,
          isFalse,
          reason:
              'cancelling the subscription must cancel its pending '
              'converging retry timer',
        );

        // Belt-and-suspenders: even if the timer fired anyway, the
        // subscription is gone from the registry so no resend happens.
        factory.lastCallback!();
        await Future<void>.delayed(const Duration(milliseconds: 5));
        expect(subscribeCount, 1);

        await conn.dispose();
      },
    );

    test('dispose() cancels a pending converging re-subscribe: no subscribe '
        'frame is sent after dispose', () async {
      // Verifies: EVS-PRD-cross-process-event-transport/L
      final pair = _Pair();
      addTearDown(pair.close);
      var subscribeCount = 0;
      final serverSub = pair.serverSide.stream.listen((raw) {
        final json = jsonDecode(raw as String) as Map<String, Object?>;
        if (json['type'] == 'subscribe') subscribeCount++;
      });
      addTearDown(serverSub.cancel);

      final factory = _CapturingTimerFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
        convergingRetryScheduler: factory.schedule,
      )..setCredential('alice');

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );
      final sub = stream.listen((_) {}, onError: (_) {});
      addTearDown(sub.cancel);

      await Future<void>.delayed(Duration.zero);
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      await Future<void>.delayed(Duration.zero);
      expect(subscribeCount, 1);

      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'error',
          'code': 'view_converging',
          'message': 'notes_today',
          'subscriptionId': 'sub-1',
        }),
      );
      await Future<void>.delayed(Duration.zero);

      final scheduledTimer = factory.lastTimer;
      expect(scheduledTimer, isNotNull);
      expect(scheduledTimer!.isActive, isTrue);

      await conn.dispose();

      expect(
        scheduledTimer.isActive,
        isFalse,
        reason: 'dispose() must cancel the pending converging retry timer',
      );

      // Belt-and-suspenders: even if the timer fired anyway, dispose()
      // has already torn down the subscription registry, so no resend
      // happens.
      factory.lastCallback!();
      await Future<void>.delayed(const Duration(milliseconds: 5));
      expect(subscribeCount, 1);
    });

    test('a 4001 auth-rejected close cancels a pending converging retry '
        'timer instead of reconnecting with the rejected credential', () async {
      // Verifies: EVS-PRD-cross-process-event-transport/L
      // R: the auth-rejected/permissions-changed carve-out from
      // auto-reconnect (H) must also stop a converging retry from
      // reconnecting blindly with the same bad credential.
      final pair = _Pair();
      addTearDown(pair.close);
      var subscribeCount = 0;
      final serverSub = pair.serverSide.stream.listen((raw) {
        final json = jsonDecode(raw as String) as Map<String, Object?>;
        if (json['type'] == 'subscribe') subscribeCount++;
      });
      addTearDown(serverSub.cancel);

      final factory = _CapturingTimerFactory();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FakeHttpClient(),
        wsFactory: (_) => pair.clientSide,
        convergingRetryScheduler: factory.schedule,
      )..setCredential('alice');

      final stream = conn.openSubscription(
        subscriptionId: 'sub-1',
        viewName: 'notes_today',
      );
      final sub = stream.listen((_) {}, onError: (_) {});
      addTearDown(sub.cancel);

      await Future<void>.delayed(Duration.zero);
      pair.serverSide.sink.add(
        jsonEncode({'type': 'auth_ok', 'principalId': 'alice'}),
      );
      await Future<void>.delayed(Duration.zero);
      expect(subscribeCount, 1);

      pair.serverSide.sink.add(
        jsonEncode({
          'type': 'error',
          'code': 'view_converging',
          'message': 'notes_today',
          'subscriptionId': 'sub-1',
        }),
      );
      await Future<void>.delayed(Duration.zero);

      final scheduledTimer = factory.lastTimer;
      expect(scheduledTimer, isNotNull);
      expect(scheduledTimer!.isActive, isTrue);

      await pair.serverCloseClient(4001);
      await Future<void>.delayed(Duration.zero);

      expect(
        scheduledTimer.isActive,
        isFalse,
        reason:
            'a 4001 close must cancel the pending converging retry '
            'timer so it never reconnects with the rejected credential',
      );
      // Belt-and-suspenders: even if the timer had fired anyway (e.g. it
      // fires in the same event turn as the close, before `_onWsClosed`
      // cancels it), the callback's own authed-channel guard must keep
      // it from opening a fresh connection with the rejected credential.
      factory.lastCallback!();
      await Future<void>.delayed(const Duration(milliseconds: 5));
      expect(subscribeCount, 1);

      await conn.dispose();
    });
  });
}

/// A [Timer]-factory seam that fires every scheduled callback on the
/// next microtask instead of waiting out a real delay, so retry tests
/// run fast without fake_async.
Timer _immediateTimer(Duration duration, void Function() callback) {
  return Timer(Duration.zero, callback);
}

/// A [Timer]-factory seam that records every scheduled delay and
/// callback without ever invoking one automatically, so a test can
/// assert the chosen backoff and that nothing fires after a
/// cancellation point.
class _CapturingTimerFactory {
  final List<Duration> delays = [];
  void Function()? lastCallback;
  _FakeTimer? lastTimer;

  Timer schedule(Duration duration, void Function() callback) {
    delays.add(duration);
    lastCallback = callback;
    final timer = _FakeTimer();
    lastTimer = timer;
    return timer;
  }
}

/// A [Timer] double that never actually fires: [isActive] flips to
/// `false` only when [cancel] is called, so a test can assert the
/// production code cancelled it rather than merely relying on a
/// stale-generation guard inside the callback.
class _FakeTimer implements Timer {
  bool _active = true;

  @override
  bool get isActive => _active;

  @override
  void cancel() => _active = false;

  @override
  int get tick => 0;
}
