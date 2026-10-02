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

  /// Closes the stream every subscription was handed.
  Future<void> closeUpdates() => _ctl.close();

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

/// Policy stub whose view-level check (`isPermitted`) throws, mirroring
/// `TableBackedAuthorizationPolicy` reading a converging
/// `user_role_scopes` view while deciding the view-level gate.
class _ThrowingIsPermittedPolicy implements AuthorizationPolicy {
  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async => throw const ViewConvergingRefusal('user_role_scopes');

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async => throw StateError('not reached: view-level gate throws first');
}

class _StubDescriptor implements ScopeProjectionDescriptor {
  @override
  Set<String> get columns => {'participant_id', 'site_id'};
}

void main() {
  late WsConnectionRegistry connectionRegistry;
  late PrincipalAuthValidator validator;

  setUp(() {
    connectionRegistry = WsConnectionRegistry();
    validator = TrustingAuthValidator(defaultActiveRole: 'investigator');
  });

  ScopeClassRegistry participantInSite() => ScopeClassRegistry(
    classes: const [
      ScopeClassSpec(name: 'site'),
      ScopeClassSpec(
        name: 'participant',
        containedIn: ContainmentReference(
          parentClass: 'site',
          projection: 'participant_site_index',
          keyColumn: 'participant_id',
          parentColumn: 'site_id',
        ),
      ),
    ],
    projectionLookup: (_) => _StubDescriptor(),
  );

  // Verifies: EVS-PRD-cross-process-event-transport/E
  test(
    'site BoundScope on participant view expands to its participants',
    () async {
      final pair = _Pair();
      addTearDown(pair.close);
      final store = _CapturingEventStore();
      addTearDown(store.closeUpdates);
      final viewScopes = ViewScopeRegistry()
        ..register(
          viewName: 'participants',
          scopeClass: 'participant',
          aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
        );

      BoundScope? capturedAssignment;
      String? capturedTargetClass;

      runSubscriptionHandler(
        channel: pair.serverSide,
        validator: validator,
        eventStore: store,
        policy: _ScopePolicy(const [
          ScopeAssignment(
            scope: BoundScope(class_: 'site', value: 'site-A'),
          ),
        ]),
        viewScopes: viewScopes,
        viewPermissionNamer: (v) => null,
        connectionRegistry: connectionRegistry,
        scopeClassRegistry: participantInSite(),
        expandDescendants: (assignment, targetClass) async {
          capturedAssignment = assignment;
          capturedTargetClass = targetClass;
          return {'P-1', 'P-2'};
        },
      );

      pair.clientSide.stream.listen((_) {});
      pair.clientSide.sink.add(
        jsonEncode({'type': 'auth', 'credential': 'dr'}),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));
      pair.clientSide.sink.add(
        jsonEncode({
          'type': 'subscribe',
          'subscriptionId': 'sub-1',
          'viewName': 'participants',
        }),
      );

      final deadline = DateTime.now().add(const Duration(seconds: 2));
      while (!store.subscribed && DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(store.subscribed, isTrue);
      expect(store.capturedAggregates, equals({'P-1', 'P-2'}));
      expect(
        capturedAssignment,
        const BoundScope(class_: 'site', value: 'site-A'),
      );
      expect(capturedTargetClass, 'participant');
    },
  );

  test('a converging containment view refuses the subscription with an '
      'ErrorMsg naming the view rather than subscribing or crashing', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    final pair = _Pair();
    addTearDown(pair.close);
    final store = _CapturingEventStore();
    addTearDown(store.closeUpdates);
    final viewScopes = ViewScopeRegistry()
      ..register(
        viewName: 'participants',
        scopeClass: 'participant',
        aggregateIdResolver: (sv) => sv is BoundScope ? sv.value : null,
      );

    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: _ScopePolicy(const [
        ScopeAssignment(
          scope: BoundScope(class_: 'site', value: 'site-A'),
        ),
      ]),
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => null,
      connectionRegistry: connectionRegistry,
      scopeClassRegistry: participantInSite(),
      expandDescendants: (assignment, targetClass) async =>
          throw const ViewConvergingRefusal('participant_site_index'),
    );

    final messages = <Map<String, Object?>>[];
    pair.clientSide.stream.listen(
      (raw) => messages.add(jsonDecode(raw as String) as Map<String, Object?>),
    );
    pair.clientSide.sink.add(jsonEncode({'type': 'auth', 'credential': 'dr'}));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    pair.clientSide.sink.add(
      jsonEncode({
        'type': 'subscribe',
        'subscriptionId': 'sub-1',
        'viewName': 'participants',
      }),
    );

    final deadline = DateTime.now().add(const Duration(seconds: 2));
    while (messages.length < 2 && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }

    expect(store.subscribed, isFalse);
    final errorMsg = messages.singleWhere((m) => m['type'] == 'error');
    expect(errorMsg['code'], 'view_converging');
    expect(errorMsg['message'], 'participant_site_index');
    // Verifies: EVS-DEV-converging-view-reads/H
    // The refusal names the subscriptionId it refuses so a remote
    // client can route it to that subscription's stream rather than
    // dropping it as unaddressed (R13).
    expect(errorMsg['subscriptionId'], 'sub-1');
  });

  test('a converging user_role_scopes view surfacing from the view-level '
      'permission gate refuses the subscription with an ErrorMsg instead '
      'of reaching the substrate subscribe or crashing', () async {
    // Verifies: EVS-DEV-converging-view-reads/H
    final pair = _Pair();
    addTearDown(pair.close);
    final store = _CapturingEventStore();
    addTearDown(store.closeUpdates);
    final viewScopes = ViewScopeRegistry();

    runSubscriptionHandler(
      channel: pair.serverSide,
      validator: validator,
      eventStore: store,
      policy: _ThrowingIsPermittedPolicy(),
      viewScopes: viewScopes,
      viewPermissionNamer: (v) => 'view.$v.read',
      connectionRegistry: connectionRegistry,
    );

    final messages = <Map<String, Object?>>[];
    pair.clientSide.stream.listen(
      (raw) => messages.add(jsonDecode(raw as String) as Map<String, Object?>),
    );
    pair.clientSide.sink.add(jsonEncode({'type': 'auth', 'credential': 'dr'}));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    pair.clientSide.sink.add(
      jsonEncode({
        'type': 'subscribe',
        'subscriptionId': 'sub-1',
        'viewName': 'participants',
      }),
    );

    final deadline = DateTime.now().add(const Duration(seconds: 2));
    while (messages.length < 2 && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }

    expect(store.subscribed, isFalse);
    final errorMsg = messages.singleWhere((m) => m['type'] == 'error');
    expect(errorMsg['code'], 'view_converging');
    expect(errorMsg['message'], 'user_role_scopes');
    expect(errorMsg['subscriptionId'], 'sub-1');
  });
}
