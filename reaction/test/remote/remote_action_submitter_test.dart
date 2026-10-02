import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/reaction.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/wire/principal_codec.dart';

class _Client extends http.BaseClient {
  _Client(this._respond);
  final http.Response Function(http.BaseRequest) _respond;
  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    final r = _respond(req);
    return http.StreamedResponse(Stream.value(r.bodyBytes), r.statusCode);
  }
}

/// An [http.BaseClient] that answers `/me` with a fixed principal body
/// and every `/actions` POST from a scripted list of responses (one
/// per call; the last entry repeats once exhausted), recording each
/// `/actions` request body so a test can assert the idempotency key
/// stayed unchanged across retries.
class _ScriptedActionsClient extends http.BaseClient {
  _ScriptedActionsClient({required this.principalBody, required this.script});
  final String principalBody;
  final List<http.Response> script;
  final List<String> requestBodies = [];
  int actionsCallCount = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest req) async {
    if (req.url.path == '/me') {
      return http.StreamedResponse(
        Stream.value(utf8.encode(principalBody)),
        200,
      );
    }
    requestBodies.add(utf8.decode((req as http.Request).bodyBytes));
    final r =
        script[actionsCallCount < script.length
            ? actionsCallCount
            : script.length - 1];
    actionsCallCount++;
    return http.StreamedResponse(Stream.value(r.bodyBytes), r.statusCode);
  }
}

http.Response _convergingResponse(String view) =>
    http.Response(jsonEncode({'error': 'view_converging', 'view': view}), 503);

http.Response _successResponse() => http.Response(
  jsonEncode({
    'type': 'success',
    'result': null,
    'emittedEventIds': <String>[],
  }),
  200,
);

/// A [Timer]-factory seam that fires every scheduled callback on the
/// next microtask instead of waiting out a real delay, mirroring
/// RemotePermissionSource's test seam so retry tests run fast.
Timer _immediateTimer(Duration duration, void Function() callback) {
  return Timer(Duration.zero, callback);
}

/// A [Timer]-factory seam that records the scheduled delay and callback
/// without ever invoking one automatically, so a test can dispose the
/// submitter mid-wait and assert the pending retry never fires (mirrors
/// `remote_connection_test.dart`'s `_CapturingTimerFactory`).
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
/// production code cancelled it rather than merely relying on some
/// other guard inside the callback.
class _FakeTimer implements Timer {
  bool _active = true;

  @override
  bool get isActive => _active;

  @override
  void cancel() => _active = false;

  @override
  int get tick => 0;
}

Future<RemoteAuthSession> _authenticatedSession(RemoteConnection conn) async {
  final auth = RemoteAuthSession(connection: conn)..setCredential('alice');
  await Future<void>.delayed(const Duration(milliseconds: 20));
  return auth;
}

String _principalBody() => jsonEncode(
  PrincipalCodec.encode(
    UserPrincipal(userId: 'alice', roles: {'install'}, activeRole: 'install'),
  ),
);

void main() {
  test('throws TransportException when not authenticated', () async {
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: _Client((_) => http.Response('', 200)),
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = RemoteAuthSession(connection: conn);
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
    );
    await expectLater(
      () => submitter.submit(
        const ActionSubmission(actionName: 'x', rawInput: {}),
      ),
      throwsA(isA<TransportException>()),
    );
  });

  test(
    '401 on /actions throws TransportException and flips to Expired',
    () async {
      final principalBody = jsonEncode(
        PrincipalCodec.encode(
          UserPrincipal(
            userId: 'alice',
            roles: {'install'},
            activeRole: 'install',
          ),
        ),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://x:1'),
        httpClient: _Client(
          (req) => req.url.path == '/me'
              ? http.Response(principalBody, 200)
              : http.Response('', 401),
        ),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final auth = RemoteAuthSession(connection: conn)..setCredential('alice');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(auth.current, isA<Authenticated>());
      final submitter = RemoteActionSubmitter(
        connection: conn,
        authSession: auth,
      );
      await expectLater(
        () => submitter.submit(
          const ActionSubmission(actionName: 'x', rawInput: {}),
        ),
        throwsA(isA<TransportException>()),
      );
      expect(auth.current, isA<Expired>());
    },
  );

  test('503 view_converging on /actions throws a typed ViewConvergingRefusal '
      'naming the view, with no retry by default', () async {
    // Verifies: EVS-PRD-cross-process-event-transport/K
    final client = _ScriptedActionsClient(
      principalBody: _principalBody(),
      script: [_convergingResponse('user_role_scopes')],
    );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = await _authenticatedSession(conn);
    expect(auth.current, isA<Authenticated>());
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
    );
    await expectLater(
      () => submitter.submit(
        const ActionSubmission(actionName: 'x', rawInput: {}),
      ),
      throwsA(
        isA<ViewConvergingRefusal>().having(
          (e) => e.viewName,
          'viewName',
          'user_role_scopes',
        ),
      ),
    );
    // Default is no retry: exactly one /actions request.
    expect(client.actionsCallCount, 1);
  });

  // Verifies: EVS-PRD-action-submitter/C
  test('opt-in retry re-sends the unchanged submission and succeeds once '
      'the server stops refusing', () async {
    // Verifies: EVS-PRD-cross-process-event-transport/K
    final client = _ScriptedActionsClient(
      principalBody: _principalBody(),
      script: [
        _convergingResponse('user_role_scopes'),
        _convergingResponse('user_role_scopes'),
        _successResponse(),
      ],
    );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = await _authenticatedSession(conn);
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
      maxConvergingRetries: 5,
      scheduleRetry: _immediateTimer,
    );
    final seen = <ViewConvergingRefusal>[];
    final sub = submitter.convergingStream.listen(seen.add);
    final result = await submitter.submit(
      const ActionSubmission(
        actionName: 'x',
        rawInput: {},
        idempotencyKey: 'k1',
      ),
    );
    await sub.cancel();
    expect(result, isA<DispatchSuccess<Object?>>());
    expect(client.actionsCallCount, 3);
    // Same idempotency key on every re-send.
    for (final body in client.requestBodies) {
      final decoded = jsonDecode(body) as Map<String, Object?>;
      expect(decoded['idempotencyKey'], 'k1');
    }
    // The typed converging condition was surfaced while it waited.
    expect(seen, hasLength(2));
    expect(seen.every((r) => r.viewName == 'user_role_scopes'), isTrue);
  });

  test('opt-in retry stops at the bound and delivers the typed transient '
      'refusal', () async {
    // Verifies: EVS-PRD-cross-process-event-transport/K
    final client = _ScriptedActionsClient(
      principalBody: _principalBody(),
      script: [_convergingResponse('user_role_scopes')],
    );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = await _authenticatedSession(conn);
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
      maxConvergingRetries: 2,
      scheduleRetry: _immediateTimer,
    );
    await expectLater(
      () => submitter.submit(
        const ActionSubmission(actionName: 'x', rawInput: {}),
      ),
      throwsA(isA<ViewConvergingRefusal>()),
    );
    // Initial attempt + 2 bounded retries = 3 requests, then no more.
    expect(client.actionsCallCount, 3);
  });

  test('dispose() during a pending converging-retry wait stops the loop: '
      'the in-flight submit throws the refusal and no further request is '
      'sent', () async {
    // Verifies: EVS-PRD-cross-process-event-transport/K
    final client = _ScriptedActionsClient(
      principalBody: _principalBody(),
      script: [_convergingResponse('user_role_scopes')],
    );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://x:1'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final auth = await _authenticatedSession(conn);
    final factory = _CapturingTimerFactory();
    final submitter = RemoteActionSubmitter(
      connection: conn,
      authSession: auth,
      maxConvergingRetries: 5,
      scheduleRetry: factory.schedule,
    );

    final future = submitter.submit(
      const ActionSubmission(actionName: 'x', rawInput: {}),
    );
    // Listen before dispose() so the eventual error has a listener
    // (otherwise the zone flags it as an uncaught async error).
    final expectation = expectLater(
      future,
      throwsA(isA<ViewConvergingRefusal>()),
    );

    // The submission met the 503, scheduled its backoff wait, and is
    // now parked in `await _wait(...)` — exactly the window dispose()
    // must interrupt.
    await Future<void>.delayed(Duration.zero);
    expect(factory.lastTimer, isNotNull);
    expect(client.actionsCallCount, 1);

    await submitter.dispose();

    expect(
      factory.lastTimer!.isActive,
      isFalse,
      reason: 'dispose() must cancel the pending converging-retry timer',
    );
    await expectation;
    expect(
      client.actionsCallCount,
      1,
      reason: 'dispose() must stop the loop before it re-sends',
    );

    // Belt-and-suspenders: even if the real timer fired anyway (e.g. in
    // the same event turn as dispose(), before cancellation takes
    // effect), no further request must follow.
    factory.lastCallback!();
    await Future<void>.delayed(const Duration(milliseconds: 5));
    expect(client.actionsCallCount, 1);
  });

  // The 'submit and decode DispatchResult' happy path is exercised in
  // the e2e suite where a full substrate response is available.
}
