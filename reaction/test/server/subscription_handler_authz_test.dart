// Verifies: EVS-PRD-cross-process-event-transport/E — row-level
//   narrowing of a subscription via the view's scope binding. These
//   tests exercise the REAL _expandAssignments path: they assert the
//   AggregateMode.aggregates set the handler computes from a principal's
//   EffectiveAuthorization.scopeAssignments, and prove that scope
//   assignments for an UNRELATED scope class do not over-grant (the
//   read path mirrors the write-path policy's class-and-ancestry
//   matching).

import 'dart:async';
import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:reaction/src/server/subscription_handler.dart';
import 'package:reaction/src/server/validators/trusting_auth_validator.dart';
import 'package:reaction/src/server/view_scope_registry.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// In-process pair of [WebSocketChannel]s (mirrors the harness in
/// subscription_handler_test.dart).
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

/// Stub [EventStore] that records the [AggregateMode.aggregates] set the
/// handler computes, so tests can assert row-level narrowing.
class _CapturingEventStore implements EventStore {
  final StreamController<Update<Map<String, Object?>>> _ctl =
      StreamController<Update<Map<String, Object?>>>.broadcast();

  /// Captured allow-set from the most recent subscribe call. `null`
  /// means "unrestricted" (no narrowing).
  Set<String>? capturedAggregates;
  bool subscribed = false;

  @override
  Stream<Update<T>> subscribe<T>(
    SubscriptionFilter filter,
    SubscriptionMode<T> mode,
  ) {
    subscribed = true;
    if (mode is AggregateMode<T>) {
      capturedAggregates = mode.aggregates;
    }
    return _ctl.stream.cast<Update<T>>();
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

/// Policy stub that returns a fixed EffectiveAuthorization (so the
/// row-level expansion path consumes a controlled set of scope
/// assignments) and always allows the view-level check.
class _ScopePolicy implements AuthorizationPolicy {
  _ScopePolicy(this._assignments);

  final List<ScopeAssignment> _assignments;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async => const Allow();

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async => EffectiveAuthorization(
    activeRole: 'clinician',
    rolePermissions: const <Permission>{},
    scopeAssignments: _assignments,
  );
}

/// A registry with patient contained in site, so isAncestor('site',
/// 'patient') is true. Uses a stub projection descriptor for the
/// containment column validation.
ScopeClassRegistry _siteOverPatientRegistry() => ScopeClassRegistry(
  classes: const [
    ScopeClassSpec(name: 'site'),
    ScopeClassSpec(
      name: 'patient',
      containedIn: ContainmentReference(
        parentClass: 'site',
        projection: 'patient_site',
        keyColumn: 'patient_id',
        parentColumn: 'site_id',
      ),
    ),
  ],
  projectionLookup: (_) => _StubDescriptor(),
);

class _StubDescriptor implements ScopeProjectionDescriptor {
  @override
  Set<String> get columns => {'patient_id', 'site_id'};
}

void main() {
  late WsConnectionRegistry connectionRegistry;
  late PrincipalAuthValidator validator;

  setUp(() {
    connectionRegistry = WsConnectionRegistry();
    validator = TrustingAuthValidator(defaultActiveRole: 'clinician');
  });

  /// Drives the handshake + a single subscribe to the patient-scoped
  /// view, then returns the captured allow-set. Waits for the
  /// subscription to actually open.
  Future<Set<String>?> runAndCapture({
    required List<ScopeAssignment> assignments,
    ScopeClassRegistry? scopeClassRegistry,
  }) async {
    final pair = _Pair();
    addTearDown(pair.close);
    final store = _CapturingEventStore();
    final viewScopes = ViewScopeRegistry()
      ..register(
        viewName: 'patient_diary',
        scopeClass: 'patient',
        // 1:1: a patient scope value IS the patient aggregate id.
        aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
      );

    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: _ScopePolicy(assignments),
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => null, // skip view-level gate
      connectionRegistry: connectionRegistry,
      scopeClassRegistry: scopeClassRegistry,
    );

    // Drain server->client messages so the write chain never stalls and
    // auth_ok / subscription envelopes are consumed (the client doesn't
    // assert on them here).
    pair.clientSide.stream.listen((_) {});

    pair.clientSide.sink.add(
      jsonEncode({'type': 'auth', 'credential': 'alice'}),
    );
    // Let the server process auth (set _principal + emit auth_ok) before
    // sending subscribe, so this test exercises the row-narrowing path
    // rather than the subscribe-before-auth race (covered separately).
    await Future<void>.delayed(const Duration(milliseconds: 20));
    pair.clientSide.sink.add(
      jsonEncode({
        'type': 'subscribe',
        'subscriptionId': 'sub-1',
        'viewName': 'patient_diary',
      }),
    );

    // Wait until the subscribe is processed (auth_ok arrives first, then
    // subscribe is handled). Poll the store flag.
    final deadline = DateTime.now().add(const Duration(seconds: 2));
    while (!store.subscribed && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    expect(store.subscribed, isTrue, reason: 'subscription never opened');
    return store.capturedAggregates;
  }

  test(
    'ValueWildcardScope(site) on a patient view (no registry) NARROWS — '
    'not unrestricted; yields zero aggregates from that assignment',
    () async {
      final captured = await runAndCapture(
        assignments: const [
          ScopeAssignment(scope: ValueWildcardScope(class_: 'site')),
        ],
      );
      // The cross-class wildcard must NOT short-circuit to unrestricted.
      expect(captured, isNotNull);
      expect(captured, isEmpty);
    },
  );

  test('ValueWildcardScope(patient) on a patient view (exact match) is '
      'unrestricted', () async {
    final captured = await runAndCapture(
      assignments: const [
        ScopeAssignment(scope: ValueWildcardScope(class_: 'patient')),
      ],
    );
    expect(captured, isNull); // unrestricted
  });

  test(
    'ValueWildcardScope(site) on a patient view WITH registry where '
    'patient is contained in site is unrestricted (write-path parity)',
    () async {
      final captured = await runAndCapture(
        assignments: const [
          ScopeAssignment(scope: ValueWildcardScope(class_: 'site')),
        ],
        scopeClassRegistry: _siteOverPatientRegistry(),
      );
      expect(captured, isNull); // ancestor wildcard => unrestricted
    },
  );

  test('BoundScope(site, X) on a patient view does NOT authorize patient '
      'aggregate X (class mismatch filtered)', () async {
    final captured = await runAndCapture(
      assignments: const [
        ScopeAssignment(
          scope: BoundScope(class_: 'site', value: 'X'),
        ),
      ],
      // Even with a registry that knows site is an ancestor, the
      // ancestor-BoundScope containment expansion is deferred, so the
      // patient row 'X' must NOT be granted.
      scopeClassRegistry: _siteOverPatientRegistry(),
    );
    expect(captured, isNotNull);
    expect(captured, isEmpty);
  });

  test(
    'BoundScope(patient, p1) on a patient view authorizes exactly p1',
    () async {
      final captured = await runAndCapture(
        assignments: const [
          ScopeAssignment(
            scope: BoundScope(class_: 'patient', value: 'p1'),
          ),
        ],
      );
      expect(captured, {'p1'});
    },
  );
}
