// Verifies: EVS-PRD-reaction-widget-contract/G (error-sink sub-clause)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/material.dart';
import 'package:flutter/scheduler.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

/// Settle the test after emitting a stream event.
///
/// A broadcast `StreamController.add` dispatches to listeners on a
/// microtask; the listener schedules a post-frame callback; that
/// callback runs at the END of the next frame.
///
/// `tester.pump()` schedules a frame; the microtask that dispatches the
/// stream event runs before the frame starts; the listener's
/// `addPostFrameCallback` registers against the frame in flight; the
/// callback then runs at frame-end. A second pump settles anything the
/// callback scheduled.
Future<void> _settle(WidgetTester tester) async {
  await tester.pump();
  await tester.pump();
}

void main() {
  group('ReActionErrorListener', () {
    testWidgets('fires onAuthExpired when AuthSession transitions to Expired', (
      tester,
    ) async {
      final fake = FakeReaction();
      var expiredCount = 0;
      var notAuthCount = 0;

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ReActionErrorListener(
          onAuthExpired: (_) => expiredCount++,
          onAuthNotAuthenticated: (_) => notAuthCount++,
          child: const Text('CHILD'),
        ),
      );

      // Authenticated -> no-op (no error transition).
      fake.driveAuthStatus(
        const Authenticated(principal: AnonymousPrincipal()),
      );
      await _settle(tester);
      expect(expiredCount, 0);
      expect(notAuthCount, 0);

      // Expired -> onAuthExpired fires once.
      fake.driveAuthStatus(const Expired());
      await _settle(tester);
      expect(expiredCount, 1);
      expect(notAuthCount, 0);

      // NotAuthenticated -> onAuthNotAuthenticated fires once.
      fake.driveAuthStatus(const NotAuthenticated());
      await _settle(tester);
      expect(expiredCount, 1);
      expect(notAuthCount, 1);
    });

    testWidgets('fires onTransport callbacks on Reconnecting/Disconnected', (
      tester,
    ) async {
      final fake = FakeReaction();
      var reconnectingCount = 0;
      var disconnectedCount = 0;

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ReActionErrorListener(
          onTransportReconnecting: (_) => reconnectingCount++,
          onTransportDisconnected: (_) => disconnectedCount++,
          child: const Text('CHILD'),
        ),
      );

      // Connected -> no-op.
      fake.driveConnectionStatus(const Connected());
      await _settle(tester);
      expect(reconnectingCount, 0);
      expect(disconnectedCount, 0);

      // Reconnecting -> onTransportReconnecting fires.
      fake.driveConnectionStatus(const Reconnecting());
      await _settle(tester);
      expect(reconnectingCount, 1);
      expect(disconnectedCount, 0);

      // Disconnected -> onTransportDisconnected fires.
      fake.driveConnectionStatus(const Disconnected());
      await _settle(tester);
      expect(reconnectingCount, 1);
      expect(disconnectedCount, 1);
    });

    testWidgets('does not rebuild child on auth/transport transitions', (
      tester,
    ) async {
      final fake = FakeReaction();
      var childBuilds = 0;

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ReActionErrorListener(
          onAuthExpired: (_) {},
          onTransportDisconnected: (_) {},
          child: Builder(
            builder: (ctx) {
              childBuilds++;
              return const Text('CHILD');
            },
          ),
        ),
      );

      final buildsAfterMount = childBuilds;
      expect(find.text('CHILD'), findsOneWidget);

      // Drive every observable transition; the child must not rebuild.
      fake.driveAuthStatus(const Expired());
      fake.driveAuthStatus(const NotAuthenticated());
      fake.driveConnectionStatus(const Reconnecting());
      fake.driveConnectionStatus(const Disconnected());
      await _settle(tester);

      expect(
        childBuilds,
        buildsAfterMount,
        reason:
            'ReActionErrorListener MUST NOT rebuild the child subtree in '
            'response to auth or transport transitions '
            '(EVS-PRD-reaction-widget-contract/G error-sink sub-clause).',
      );
    });

    testWidgets(
      'callbacks fire post-frame (safe for Navigator-style BuildContext use)',
      (tester) async {
        final fake = FakeReaction();
        SchedulerPhase? phaseAtCallback;
        var navigatorWasResolvable = false;

        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ReActionErrorListener(
            onAuthExpired: (ctx) {
              // `addPostFrameCallback` invokes its callback in
              // `SchedulerPhase.postFrameCallbacks` — AFTER the
              // transient/persistent phases that run during
              // build/layout/paint. Capturing the phase is the
              // substrate-level signal that the callback ran from the
              // post-frame queue rather than from inside the stream
              // listener (which would observe `transientCallbacks` or
              // `persistentCallbacks`, never `postFrameCallbacks`).
              phaseAtCallback = SchedulerBinding.instance.schedulerPhase;
              // And Navigator-style lookups must succeed; this is the
              // canonical "BuildContext safe for navigation" check.
              navigatorWasResolvable = Navigator.maybeOf(ctx) != null;
            },
            child: const Text('CHILD'),
          ),
        );

        fake.driveAuthStatus(const Expired());
        await _settle(tester);

        expect(
          phaseAtCallback,
          SchedulerPhase.postFrameCallbacks,
          reason:
              'Callback must run from addPostFrameCallback, which fires '
              'in SchedulerPhase.postFrameCallbacks — not from inside '
              'the stream listener (which would run in persistent/'
              'transient callback phase).',
        );
        expect(
          navigatorWasResolvable,
          isTrue,
          reason:
              'BuildContext passed to the callback must be valid for '
              'Navigator-style lookups (the post-frame guarantee).',
        );
      },
    );
  });
}
