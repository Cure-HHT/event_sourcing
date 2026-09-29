// AuthorizationWatcher's permission_revoked/permission_granted fan-out
// reads the effective role of every connected user through the
// substrate's authorization policy. That read refuses with
// ViewConvergingRefusal while the role-assignment or permission-grant
// view is converging for this instance (EVS-DEV-converging-view-reads/H).
// The watcher fails closed on a narrowing signal and keeps notifying on
// an expanding one, and never lets the unawaited fan-out's error escape
// uncaught.
//
// Verifies: EVS-DEV-converging-view-reads/H
// the watcher's own
//   decisions (force-logout, stale_data) treat a ViewConvergingRefusal
//   from the policy as fail-closed on a narrowing event and as an
//   over-notify on an expanding one, per-user, without aborting the
//   fan-out for later users or leaking an uncaught async error.

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:logging/logging.dart';
import 'package:reaction/src/server/authorization_watcher.dart';
import 'package:reaction/src/server/ws_connection_registry.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import '../local/test_support/reaction_test_harness.dart';

/// Policy fake standing in for `TableBackedAuthorizationPolicy`: returns
/// the principal's active role as-is for every user except the
/// configured `throwFor` userIds, for which it throws
/// [ViewConvergingRefusal] the way the real policy does while its role
/// or permission view converges, and the configured `throwOtherFor`
/// userIds, for which it throws an arbitrary non-refusal error (e.g. a
/// storage error) the way an unrelated backend failure would surface.
class _FakePolicy extends AuthorizationPolicy {
  _FakePolicy({this.throwFor = const {}, this.throwOtherFor = const {}});

  final Set<String> throwFor;
  final Set<String> throwOtherFor;
  final List<String> queried = [];

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) {
    throw UnimplementedError('not exercised by these tests');
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async {
    final p = principal as UserPrincipal;
    queried.add(p.userId);
    if (throwFor.contains(p.userId)) {
      throw const ViewConvergingRefusal('user_role_scopes');
    }
    if (throwOtherFor.contains(p.userId)) {
      throw StateError('backend unavailable for ${p.userId}');
    }
    return EffectiveAuthorization(
      activeRole: p.activeRole,
      rolePermissions: const <Permission>{},
      scopeAssignments: const [],
    );
  }
}

/// Records close-code/reason and every stale_data envelope sent to it,
/// standing in for a live WS connection in `WsConnectionRegistry`.
class _RecordingChannel implements WebSocketChannel {
  final List<String> messages = [];
  int? _closeCode;
  String? _closeReason;

  @override
  int? get closeCode => _closeCode;

  @override
  String? get closeReason => _closeReason;

  @override
  WebSocketSink get sink => _RecordingSink(this);

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError(invocation.memberName.toString());
}

class _RecordingSink implements WebSocketSink {
  _RecordingSink(this._channel);
  final _RecordingChannel _channel;

  @override
  void add(Object? event) => _channel.messages.add(event! as String);

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    _channel._closeCode = closeCode;
    _channel._closeReason = closeReason;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) =>
      throw UnimplementedError(invocation.memberName.toString());
}

void main() {
  late ReactionTestHarness h;
  late WsConnectionRegistry registry;

  setUp(() async {
    h = await ReactionTestHarness.open();
    registry = WsConnectionRegistry();
  });

  tearDown(() async {
    await h.close();
  });

  Future<void> appendPermissionEvent(String eventType, String role) {
    return h.eventStore.append(
      entryType: 'role_permission_grant',
      aggregateType: 'role_permission_grant',
      aggregateId: '$role:say_hello',
      eventType: eventType,
      data: eventType == 'permission_granted'
          ? PermissionGrantedPayload(
              role: role,
              permissionName: 'say_hello',
            ).toJson()
          : PermissionRevokedPayload(
              role: role,
              permissionName: 'say_hello',
            ).toJson(),
      initiator: const AutomationInitiator(service: 'authz_watcher_test'),
    );
  }

  test('permission_revoked force-logs-out a user whose role view is '
      'converging and keeps closing every later connected user', () async {
    final alice = _RecordingChannel();
    final bob = _RecordingChannel();
    final carol = _RecordingChannel();
    registry
      ..register('alice', alice)
      ..register('bob', bob)
      ..register('carol', carol);

    final policy = _FakePolicy(throwFor: {'alice'});
    final watcher = AuthorizationWatcher(
      eventStore: h.eventStore,
      connectionRegistry: registry,
      policy: policy,
    );
    await watcher.start();
    addTearDown(watcher.stop);

    final errors = <Object>[];
    await runZonedGuarded(() async {
      await appendPermissionEvent('permission_revoked', 'install');
      for (var i = 0; i < 20 && policy.queried.length < 3; i++) {
        await pumpEventQueue();
      }
    }, (error, stack) => errors.add(error));

    expect(
      errors,
      isEmpty,
      reason: 'no uncaught async error escapes the unawaited fan-out',
    );
    expect(
      alice.closeCode,
      4003,
      reason: "fail closed: alice's role could not be confirmed",
    );
    expect(bob.closeCode, 4003);
    expect(carol.closeCode, 4003);
  });

  test(
    'permission_revoked fails closed on a non-refusal error too, '
    'forcing that user out and continuing the loop, logged at severe',
    () async {
      final alice = _RecordingChannel();
      final bob = _RecordingChannel();
      final carol = _RecordingChannel();
      registry
        ..register('alice', alice)
        ..register('bob', bob)
        ..register('carol', carol);

      final policy = _FakePolicy(throwOtherFor: {'bob'});
      final watcher = AuthorizationWatcher(
        eventStore: h.eventStore,
        connectionRegistry: registry,
        policy: policy,
      );
      await watcher.start();
      addTearDown(watcher.stop);

      final records = <LogRecord>[];
      final logSub = Logger.root.onRecord.listen(records.add);
      addTearDown(logSub.cancel);
      final previousLevel = Logger.root.level;
      Logger.root.level = Level.ALL;
      addTearDown(() => Logger.root.level = previousLevel);

      final errors = <Object>[];
      await runZonedGuarded(() async {
        await appendPermissionEvent('permission_revoked', 'install');
        for (var i = 0; i < 20 && policy.queried.length < 3; i++) {
          await pumpEventQueue();
        }
      }, (error, stack) => errors.add(error));

      expect(
        errors,
        isEmpty,
        reason: 'no uncaught async error escapes the unawaited fan-out',
      );
      expect(alice.closeCode, 4003);
      expect(
        bob.closeCode,
        4003,
        reason:
            "fail closed: bob's role could not be determined "
            'because the policy threw an unexpected error',
      );
      expect(
        carol.closeCode,
        4003,
        reason: 'the loop continues past the failing user',
      );
      expect(
        records.any(
          (r) =>
              r.level == Level.SEVERE &&
              r.loggerName == 'reaction.authorization_watcher',
        ),
        isTrue,
        reason:
            'the non-refusal error is logged at severe through '
            'the package logger',
      );
    },
  );

  test('permission_granted sends stale_data even to a user whose role '
      'view is converging (over-notify is safe)', () async {
    final alice = _RecordingChannel();
    final bob = _RecordingChannel();
    registry
      ..register('alice', alice)
      ..register('bob', bob);

    final policy = _FakePolicy(throwFor: {'alice'});
    final watcher = AuthorizationWatcher(
      eventStore: h.eventStore,
      connectionRegistry: registry,
      policy: policy,
    );
    await watcher.start();
    addTearDown(watcher.stop);

    final errors = <Object>[];
    await runZonedGuarded(() async {
      await appendPermissionEvent('permission_granted', 'install');
      for (var i = 0; i < 20 && policy.queried.length < 2; i++) {
        await pumpEventQueue();
      }
    }, (error, stack) => errors.add(error));

    expect(errors, isEmpty);
    expect(
      alice.closeCode,
      isNull,
      reason: 'an expanding event never force-closes',
    );
    expect(
      alice.messages,
      isNotEmpty,
      reason: 'over-notify: alice still gets stale_data',
    );
    expect(bob.messages, isNotEmpty);
  });
}
