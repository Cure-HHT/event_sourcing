// Verifies: EVS-PRD-reaction-widget-contract/H

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

void main() {
  group('FakeReaction', () {
    test('implements ReactionScope', () {
      final fake = FakeReaction();
      expect(fake, isA<ReactionScope>());
    });

    test('driveAuthStatus emits to authSession.stream', () async {
      final fake = FakeReaction();
      final received = <AuthStatus>[];
      final sub = fake.authSession.stream.listen(received.add);

      fake.driveAuthStatus(const NotAuthenticated());
      fake.driveAuthStatus(
        Authenticated(
          principal: Principal.user(
            userId: 'u',
            activeRole: 'r',
            roles: const {'r'},
          ),
        ),
      );
      await pumpEventQueue();

      expect(received.length, 2);
      expect(received[0], isA<NotAuthenticated>());
      expect(received[1], isA<Authenticated>());
      expect(fake.authSession.current, isA<Authenticated>());
      expect(fake.authSession.principal, isA<UserPrincipal>());
      await sub.cancel();
    });

    test('driveConnectionStatus emits to connectionStatusStream', () async {
      final fake = FakeReaction();
      final received = <ConnectionStatus>[];
      final sub = fake.connectionStatusStream.listen(received.add);

      fake.driveConnectionStatus(const Reconnecting());
      fake.driveConnectionStatus(const Connected());
      await pumpEventQueue();

      expect(received, [const Reconnecting(), const Connected()]);
      expect(fake.connectionStatus, const Connected());
      await sub.cancel();
    });

    test('actionSubmitter returns queued DispatchResult', () async {
      final fake = FakeReaction();
      const result = DispatchResult<Object?>.success('ok', <String>[]);
      fake.queueDispatchResult(result);

      final got = await fake.actionSubmitter.submit(
        const ActionSubmission(actionName: 'noop', rawInput: {}),
      );
      expect(got, same(result));
      expect(fake.submittedActions, hasLength(1));
      expect(fake.submittedActions.first.actionName, 'noop');
    });

    test('actionSubmitter throws when no result queued', () async {
      final fake = FakeReaction();
      expect(
        () => fake.actionSubmitter.submit(
          const ActionSubmission(actionName: 'noop', rawInput: {}),
        ),
        throwsStateError,
      );
    });

    test(
      'viewSource emits queued Update<T> events to active subscribers',
      () async {
        final fake = FakeReaction();
        final stream = fake.viewSource.watch<Map<String, Object?>>(
          viewName: 'v',
          mapper: (m) => m,
        );
        final received = <Update<Map<String, Object?>>>[];
        final sub = stream.listen(received.add);
        await pumpEventQueue();

        fake.emitViewUpdate<Map<String, Object?>>(
          'v',
          const Snapshot<Map<String, Object?>>(value: {'id': 1}, sequence: 1),
        );
        fake.emitViewUpdate<Map<String, Object?>>(
          'v',
          const EndOfReplay<Map<String, Object?>>(sequence: 1),
        );
        await pumpEventQueue();

        expect(received.length, 2);
        expect(received[0], isA<Snapshot<Map<String, Object?>>>());
        expect(received[1], isA<EndOfReplay<Map<String, Object?>>>());
        await sub.cancel();
      },
    );

    test('permissionSource.current and stream are drivable', () async {
      final fake = FakeReaction();
      expect(fake.permissionSource.current, isNull);

      final received = <EffectiveAuthorization?>[];
      final sub = fake.permissionSource.stream.listen(received.add);

      const auth = EffectiveAuthorization(
        activeRole: 'install',
        rolePermissions: <Permission>{},
        scopeAssignments: <ScopeAssignment>[],
      );
      fake.drivePermission(auth);
      await pumpEventQueue();

      expect(fake.permissionSource.current, same(auth));
      expect(received, hasLength(1));
      expect(received.first, same(auth));
      await sub.cancel();
    });

    test('dispose() is idempotent', () async {
      final fake = FakeReaction();
      await fake.dispose();
      await fake.dispose(); // should not throw
    });

    test('post-dispose access throws StateError', () async {
      final fake = FakeReaction();
      await fake.dispose();
      expect(() => fake.authSession, throwsStateError);
      expect(() => fake.actionSubmitter, throwsStateError);
      expect(() => fake.viewSource, throwsStateError);
      expect(() => fake.permissionSource, throwsStateError);
      expect(() => fake.connectionStatus, throwsStateError);
      expect(() => fake.connectionStatusStream, throwsStateError);
      expect(
        () => fake.driveAuthStatus(const NotAuthenticated()),
        throwsStateError,
      );
      expect(
        () => fake.driveConnectionStatus(const Connected()),
        throwsStateError,
      );
      expect(() => fake.drivePermission(null), throwsStateError);
      expect(
        () => fake.queueDispatchResult(
          const DispatchResult<Object?>.success('ok', <String>[]),
        ),
        throwsStateError,
      );
      expect(
        () => fake.queueDispatchResultFuture(
          Future.value(const DispatchResult<Object?>.success('ok', <String>[])),
        ),
        throwsStateError,
      );
      expect(
        () => fake.emitViewUpdate<Map<String, Object?>>(
          'v',
          const EndOfReplay<Map<String, Object?>>(sequence: 1),
        ),
        throwsStateError,
      );
      expect(() => fake.submittedActions, throwsStateError);
    });
  });

  group('pumpReactionWidget', () {
    testWidgets('mounts a widget with FakeReaction in scope', (tester) async {
      final fake = FakeReaction();
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: Builder(
          builder: (ctx) {
            final scope = ReActionScope.of(ctx);
            expect(scope, same(fake));
            return const SizedBox.shrink();
          },
        ),
      );
    });
  });
}
