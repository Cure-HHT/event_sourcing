// Verifies: EVS-PRD-subscription remote-server side — WS subscription
// state machine accepts AuthMsg first and emits AuthOkMsg; per-subscribe
// view-level authorization rejects with subscription_denied on policy
// deny. Snapshot/Delta relay covered in Phase 4 e2e tests where the
// full substrate is exercised.

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// In-process pair of [WebSocketChannel]s for tests: anything the
/// `clientSide` sink emits arrives on `serverSide.stream`, and vice
/// versa.
class _Pair {
  _Pair() {
    _clientToServer = StreamController<Object?>();
    _serverToClient = StreamController<Object?>();
    serverSide = _MemChannel(
      stream: _clientToServer.stream,
      rawSink: _serverToClient.sink,
    );
    clientSide = _MemChannel(
      stream: _serverToClient.stream,
      rawSink: _clientToServer.sink,
    );
  }

  late final StreamController<Object?> _clientToServer;
  late final StreamController<Object?> _serverToClient;
  late final WebSocketChannel serverSide;
  late final WebSocketChannel clientSide;

  Future<void> close() async {
    await _clientToServer.close();
    await _serverToClient.close();
  }
}

class _MemChannel extends StreamChannelMixin<dynamic>
    implements WebSocketChannel {
  _MemChannel({
    required Stream<Object?> stream,
    required StreamSink<Object?> rawSink,
  }) : _stream = stream,
       sink = _MemSink(rawSink);

  final Stream<Object?> _stream;

  @override
  Stream<dynamic> get stream => _stream;

  @override
  final WebSocketSink sink;

  @override
  int? get closeCode => null;

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

/// Stub [EventStore] that returns a controlled [Update] stream from
/// [subscribe]. All other surface methods throw — the handler must not
/// touch them.
class _StubEventStore implements EventStore {
  final StreamController<Update<Map<String, Object?>>> _ctl =
      StreamController<Update<Map<String, Object?>>>.broadcast();

  void push(Update<Map<String, Object?>> u) => _ctl.add(u);

  @override
  Stream<Update<T>> subscribe<T>(
    SubscriptionFilter filter,
    SubscriptionMode<T> mode,
  ) {
    return _ctl.stream.cast<Update<T>>();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _StubPolicy implements AuthorizationPolicy {
  // `effective` is a stub knob retained for follow-up row-level tests;
  // unused in this slice's two tests.
  // ignore: unused_element_parameter
  _StubPolicy({this.allow = true, this.effective});

  final bool allow;
  final EffectiveAuthorization? effective;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async => allow
      ? const Allow()
      : Deny(permission: permission, reason: DenyReason.notGranted);

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async =>
      effective ??
      const EffectiveAuthorization(
        activeRole: 'install',
        rolePermissions: <Permission>{},
        scopeAssignments: <ScopeAssignment>[],
      );
}

void main() {
  late PrincipalAuthValidator validator;
  late _StubEventStore store;
  late _StubPolicy policy;
  late ViewScopeRegistry viewScopes;

  setUp(() {
    validator = TrustingAuthValidator(defaultActiveRole: 'install');
    store = _StubEventStore();
    policy = _StubPolicy();
    viewScopes = ViewScopeRegistry();
  });

  Future<Map<String, Object?>> recvJson(
    WebSocketChannel channel, {
    int n = 1,
  }) async {
    final received = await channel.stream
        .map((e) => jsonDecode(e as String) as Map<String, Object?>)
        .take(n)
        .toList();
    return received.last;
  }

  test('auth_ok on valid first message', () async {
    final pair = _Pair();
    addTearDown(pair.close);
    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: policy,
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => 'view:$v',
    );

    pair.clientSide.sink.add(
      jsonEncode({'type': 'auth', 'credential': 'alice'}),
    );

    final res = await recvJson(pair.clientSide);
    expect(res['type'], 'auth_ok');
    expect(res['principalId'], 'alice');
  });

  test('subscription_denied when view-level perm fails', () async {
    final pair = _Pair();
    addTearDown(pair.close);
    policy = _StubPolicy(allow: false);
    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: policy,
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => 'view:$v',
    );

    pair.clientSide.sink.add(
      jsonEncode({'type': 'auth', 'credential': 'alice'}),
    );
    pair.clientSide.sink.add(
      jsonEncode({
        'type': 'subscribe',
        'subscriptionId': 'sub-1',
        'viewName': 'audit_log',
      }),
    );

    final res = await recvJson(pair.clientSide, n: 2);
    expect(res['type'], 'subscription_denied');
    expect(res['reason'], 'view_permission_denied');
  });
}
