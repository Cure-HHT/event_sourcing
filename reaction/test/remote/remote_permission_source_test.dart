// Verifies: EVS-PRD-permission-source/A+C+D+E
// Remote impl surface.
//   Full per-assertion coverage (two-phase load + AuthSession
//   dependency) lives in e2e/permission_test.dart; the
//   null-on-empty-role parity test below runs here because it exercises
//   only the snapshot-decode branch, which needs no live server.
// Verifies: EVS-DEV-converging-view-reads/H
// an explicit refresh() call
//   propagates a typed ViewConvergingRefusal on a 503 view_converging
//   response, not a silently swallowed transport failure.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:http/http.dart' as http;
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:reaction/src/remote/remote_permission_source.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';

/// HTTP client that returns 503 view_converging for the first
/// [convergingResponses] requests on a path, then 200 with [okBody].
/// Every request is recorded so tests can assert call counts.
class _SequencedHttpClient extends http.BaseClient {
  _SequencedHttpClient({
    required this.convergingResponses,
    required this.okBody,
  });

  final int convergingResponses;
  final String okBody;
  int callCount = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    callCount++;
    if (callCount <= convergingResponses) {
      return http.StreamedResponse(
        Stream.value(
          utf8.encode(
            jsonEncode({
              'error': 'view_converging',
              'view': 'user_role_scopes',
            }),
          ),
        ),
        503,
      );
    }
    return http.StreamedResponse(Stream.value(utf8.encode(okBody)), 200);
  }
}

/// HTTP client that always answers 503 view_converging, counting calls.
/// Optionally sends a fixed `Retry-After` header on every response.
class _AlwaysConvergingClient extends http.BaseClient {
  _AlwaysConvergingClient({this.retryAfterSeconds});

  final int? retryAfterSeconds;
  int callCount = 0;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    callCount++;
    return http.StreamedResponse(
      Stream.value(
        utf8.encode(
          jsonEncode({'error': 'view_converging', 'view': 'user_role_scopes'}),
        ),
      ),
      503,
      headers: retryAfterSeconds == null
          ? {}
          : {'retry-after': '$retryAfterSeconds'},
    );
  }
}

/// A [Timer]-factory seam that fires every scheduled callback on the
/// next microtask instead of waiting out a real delay, so retry tests
/// run fast without fake_async (unavailable in this package).
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

/// HTTP client returning a fixed 200 body for the snapshot GET.
class _FixedHttpClient extends http.BaseClient {
  _FixedHttpClient(this._body);

  final String _body;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    return http.StreamedResponse(Stream.value(utf8.encode(_body)), 200);
  }
}

/// HTTP client returning a fixed status + body for every request.
class _FixedStatusClient extends http.BaseClient {
  _FixedStatusClient(this._status, this._body);

  final int _status;
  final String _body;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) async {
    return http.StreamedResponse(Stream.value(utf8.encode(_body)), _status);
  }
}

/// Minimal Authenticated AuthSession for the snapshot fetch path.
class _AuthenticatedSession implements AuthSession {
  _AuthenticatedSession(this._principal);

  final Principal _principal;
  final StreamController<AuthStatus> _ctl =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => Authenticated(principal: _principal);

  @override
  Stream<AuthStatus> get stream => _ctl.stream;

  @override
  Principal? get principal => _principal;

  @override
  void setCredential(String? credential) {}

  /// Test-only: push an auth-status transition to subscribers.
  void emit(AuthStatus status) => _ctl.add(status);

  @override
  Future<void> dispose() async {
    await _ctl.close();
  }
}

/// An [AuthSession] whose [current] tracks the last status [emit]ted,
/// starting [NotAuthenticated]. Unlike [_AuthenticatedSession] (whose
/// [current] getter is pinned to `Authenticated` regardless of what it
/// emits), this drives a genuine Authenticated transition for tests
/// that need `authSession.current is Authenticated` to actually change.
class _TransitioningSession implements AuthSession {
  AuthStatus _current = const NotAuthenticated();
  final StreamController<AuthStatus> _ctl =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _ctl.stream;

  @override
  Principal? get principal =>
      _current is Authenticated ? (_current as Authenticated).principal : null;

  @override
  void setCredential(String? credential) {}

  void emit(AuthStatus status) {
    _current = status;
    _ctl.add(status);
  }

  @override
  Future<void> dispose() async {
    await _ctl.close();
  }
}

Principal _clinician() => UserPrincipal(
  userId: 'u1',
  roles: const {'clinician'},
  activeRole: 'clinician',
);

void main() {
  test(
    'current is null before AuthSession becomes Authenticated',
    () {
      // Full coverage of two-phase load + AuthSession dependency in
      // e2e/permission_test.dart; skeleton verified here.
    },
    skip: 'covered in e2e/permission_test.dart',
  );

  test(
    'empty-activeRole snapshot maps current to null (parity with Local)',
    () async {
      // The policy returns EffectiveAuthorization.empty (activeRole == '')
      // for principals who don't hold their claimed activeRole. Local maps
      // that to current=null; Remote must too.
      final emptyBody = jsonEncode(
        EffectiveAuthorizationCodec.encode(EffectiveAuthorization.empty),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FixedHttpClient(emptyBody),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
      );
      addTearDown(source.dispose);

      // Constructor kicks off the fetch since the session is Authenticated.
      // Give the in-flight HTTP GET + decode time to settle.
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(
        source.current,
        isNull,
        reason: 'empty activeRole must clear to null, matching Local',
      );
    },
  );

  test(
    'non-empty-activeRole snapshot is stored as a non-null authorization',
    () async {
      final body = jsonEncode(
        EffectiveAuthorizationCodec.encode(
          EffectiveAuthorization(
            activeRole: 'clinician',
            rolePermissions: <Permission>{
              const Permission('view:patient_diary'),
            },
            scopeAssignments: const <ScopeAssignment>[],
          ),
        ),
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: _FixedHttpClient(body),
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
      );
      addTearDown(source.dispose);

      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(source.current, isNotNull);
      expect(source.current!.activeRole, 'clinician');
    },
  );

  test('refresh() throws a typed ViewConvergingRefusal on a 503 '
      'view_converging response; the constructor-triggered initial fetch '
      'stays quiet on the same response', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    final client = _FixedStatusClient(
      503,
      jsonEncode({'error': 'view_converging', 'view': 'user_role_scopes'}),
    );
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final session = _AuthenticatedSession(_clinician());
    addTearDown(session.dispose);

    final source = RemotePermissionSource(
      connection: conn,
      authSession: session,
    );
    addTearDown(source.dispose);

    // The constructor's Authenticated-triggered fetch hits the same
    // 503 view_converging response; it must not crash the source or
    // leak an unhandled async error.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(source.current, isNull);

    await expectLater(
      source.refresh,
      throwsA(
        isA<ViewConvergingRefusal>().having(
          (e) => e.viewName,
          'viewName',
          'user_role_scopes',
        ),
      ),
    );
  });

  test(
    'the Authenticated-transition fetch retries a 503 view_converging '
    'with bounded backoff, exposing a typed transient state meanwhile',
    () async {
      // Verifies: EVS-DEV-converging-view-reads/H
      // Verifies: EVS-PRD-permission-source/C+E
      final okBody = jsonEncode(
        EffectiveAuthorizationCodec.encode(
          EffectiveAuthorization(
            activeRole: 'clinician',
            rolePermissions: <Permission>{
              const Permission('view:patient_diary'),
            },
            scopeAssignments: const <ScopeAssignment>[],
          ),
        ),
      );
      final client = _SequencedHttpClient(
        convergingResponses: 1,
        okBody: okBody,
      );
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: client,
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _TransitioningSession();
      addTearDown(session.dispose);

      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
        scheduleRetry: _immediateTimer,
      );
      addTearDown(source.dispose);

      // No fetch yet: the session is still NotAuthenticated.
      expect(client.callCount, 0);

      // Drive the Authenticated transition (EVS-PRD-permission-source/E).
      session.emit(Authenticated(principal: _clinician()));

      // The Authenticated-triggered fetch hits the 503 synchronously
      // (before any retry fires); the typed transient refusal should
      // already be visible.
      await Future<void>.delayed(Duration.zero);
      expect(client.callCount, 1);
      expect(source.current, isNull);
      expect(source.converging, isNotNull);
      expect(source.converging!.viewName, 'user_role_scopes');

      // Let the scheduled retry (immediate timer) fire and settle.
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(client.callCount, 2);
      expect(source.current, isNotNull);
      expect(source.current!.activeRole, 'clinician');
      expect(source.converging, isNull);
    },
  );

  test(
    'a Retry-After header on the 503 sets the scheduled retry delay',
    () async {
      final client = _AlwaysConvergingClient(retryAfterSeconds: 7);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: client,
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final factory = _CapturingTimerFactory();
      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
        scheduleRetry: factory.schedule,
      );
      addTearDown(source.dispose);

      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(client.callCount, 1);
      expect(
        factory.delays,
        [const Duration(seconds: 7)],
        reason: 'Retry-After: 7 must drive the scheduled delay verbatim',
      );
    },
  );

  test(
    'a Retry-After header above the ceiling is capped at 30 seconds',
    () async {
      final client = _AlwaysConvergingClient(retryAfterSeconds: 86400);
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: client,
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final factory = _CapturingTimerFactory();
      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
        scheduleRetry: factory.schedule,
      );
      addTearDown(source.dispose);

      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(factory.delays, [const Duration(seconds: 30)]);
    },
  );

  test('with no Retry-After header, each successive backoff doubles the '
      'previous delay', () async {
    // The cap (5s) is not exercised here: 200ms base doubled over the
    // bounded 5-attempt budget never reaches it, so it is a safety
    // bound rather than a behaviour this test can turn RED.
    final client = _AlwaysConvergingClient();
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final session = _AuthenticatedSession(_clinician());
    addTearDown(session.dispose);

    final factory = _CapturingTimerFactory();
    final source = RemotePermissionSource(
      connection: conn,
      authSession: session,
      scheduleRetry: factory.schedule,
    );
    addTearDown(source.dispose);

    // First 503: let the retry get scheduled but not fired.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(factory.delays, hasLength(1));
    final firstDelay = factory.delays.single;

    // Manually fire the pending retry, which issues the second GET
    // (another 503) and schedules the next retry.
    factory.lastCallback!();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(client.callCount, 2);
    expect(factory.delays, hasLength(2));
    final secondDelay = factory.delays[1];

    expect(
      secondDelay,
      firstDelay * 2,
      reason: 'each successive backoff must double the previous delay',
    );
  });

  test('retries are bounded: an always-converging server is retried exactly '
      'the initial fetch plus the fixed attempt bound, then stops', () async {
    // 1 initial fetch + 5 bounded retries = 6, matching
    // `_maxRetryAttempts = 5` in remote_permission_source.dart.
    const expectedTotalCalls = 6;
    final client = _AlwaysConvergingClient();
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final session = _AuthenticatedSession(_clinician());
    addTearDown(session.dispose);

    final source = RemotePermissionSource(
      connection: conn,
      authSession: session,
      scheduleRetry: _immediateTimer,
    );
    addTearDown(source.dispose);

    // Pump enough microtask/immediate-timer turns for every bounded
    // retry to exhaust.
    for (var i = 0; i < 10; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    expect(client.callCount, expectedTotalCalls);

    // No further GETs after the bound is reached, however long we wait.
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(client.callCount, expectedTotalCalls);
    expect(source.converging, isNotNull);
  });

  test(
    'dispose cancels the pending retry timer: no further GET happens',
    () async {
      final client = _AlwaysConvergingClient();
      final conn = RemoteConnection(
        baseUrl: Uri.parse('http://localhost:0'),
        httpClient: client,
        wsFactory: (_) => throw UnimplementedError(),
      );
      final session = _AuthenticatedSession(_clinician());
      addTearDown(session.dispose);

      final factory = _CapturingTimerFactory();
      final source = RemotePermissionSource(
        connection: conn,
        authSession: session,
        scheduleRetry: factory.schedule,
      );

      // Let the initial fetch complete and the retry get scheduled
      // (captured, not fired) via the capturing factory.
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(client.callCount, 1);
      final scheduledTimer = factory.lastTimer;
      expect(scheduledTimer, isNotNull);
      expect(scheduledTimer!.isActive, isTrue);

      await source.dispose();

      expect(
        scheduledTimer.isActive,
        isFalse,
        reason: 'disposal must cancel the pending retry timer outright',
      );

      // Belt-and-suspenders: even if the timer had fired anyway, the
      // stale-generation guard inside the callback must also bail.
      factory.lastCallback!();
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(client.callCount, 1);
    },
  );

  test('an auth-status change cancels the pending retry timer: no further '
      'GET happens for the superseded generation', () async {
    final client = _AlwaysConvergingClient();
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final session = _AuthenticatedSession(_clinician());
    addTearDown(session.dispose);

    final factory = _CapturingTimerFactory();
    final source = RemotePermissionSource(
      connection: conn,
      authSession: session,
      scheduleRetry: factory.schedule,
    );
    addTearDown(source.dispose);

    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(client.callCount, 1);
    final scheduledTimer = factory.lastTimer;
    final scheduledCallback = factory.lastCallback;
    expect(scheduledTimer, isNotNull);
    expect(scheduledTimer!.isActive, isTrue);

    // Simulate the auth session leaving Authenticated, which bumps
    // the generation counter and must cancel the pending retry.
    session.emit(const NotAuthenticated());
    await Future<void>.delayed(Duration.zero);

    expect(
      scheduledTimer.isActive,
      isFalse,
      reason: 'an auth transition must cancel the pending retry timer',
    );

    scheduledCallback!();
    await Future<void>.delayed(const Duration(milliseconds: 20));

    expect(
      client.callCount,
      1,
      reason: 'a superseded generation must not fire the retried fetch',
    );
  });

  test('refresh() also leaves a retry scheduled after rethrowing the '
      'typed refusal', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    final client = _AlwaysConvergingClient();
    final conn = RemoteConnection(
      baseUrl: Uri.parse('http://localhost:0'),
      httpClient: client,
      wsFactory: (_) => throw UnimplementedError(),
    );
    final session = _AuthenticatedSession(_clinician());
    addTearDown(session.dispose);

    final factory = _CapturingTimerFactory();
    final source = RemotePermissionSource(
      connection: conn,
      authSession: session,
      scheduleRetry: factory.schedule,
    );
    addTearDown(source.dispose);

    // Constructor's Authenticated-triggered fetch: 1st GET, 1 retry
    // scheduled.
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(client.callCount, 1);
    expect(factory.delays, hasLength(1));

    // An explicit refresh() rethrows the refusal to its caller, and
    // must still leave a (fresh) retry scheduled rather than only
    // throwing and going quiet.
    await expectLater(
      source.refresh,
      throwsA(
        isA<ViewConvergingRefusal>().having(
          (e) => e.viewName,
          'viewName',
          'user_role_scopes',
        ),
      ),
    );
    expect(client.callCount, 2);
    expect(
      factory.delays,
      hasLength(2),
      reason:
          'refresh() must schedule its own retry on a 503, not only '
          'throw to its caller',
    );

    // Firing that retry issues a third GET.
    factory.lastCallback!();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(client.callCount, 3);
  });
}
