// Verifies: EVS-PRD-reaction-widget-contract/C, /E, /G

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

ActionSubmission _sub({String? idempotencyKey}) => ActionSubmission(
  actionName: 'noop',
  rawInput: const <String, Object?>{},
  idempotencyKey: idempotencyKey,
);

/// Deterministic key generator that emits `key-1`, `key-2`, ... so
/// tests can assert idempotency-key lifetime behaviour without
/// involving real UUIDs.
class _CountingKeyGen implements IdempotencyKeyGenerator {
  int _n = 0;
  @override
  String generate() => 'key-${++_n}';
}

void main() {
  group('ActionBuilder', () {
    testWidgets('starts in Idle state', (tester) async {
      final fake = FakeReaction();
      late ActionState observed;

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: _sub,
          builder: (ctx, state, submit) {
            observed = state;
            return const SizedBox.shrink();
          },
        ),
      );

      expect(observed, isA<Idle>());
    });

    testWidgets('transitions Idle -> Submitting -> Success', (tester) async {
      final fake = FakeReaction();
      // Use a Completer so we can observe the Submitting state distinctly
      // from the Success state (Future.value resolves on a microtask
      // before the first pump can render Submitting in isolation).
      const result = DispatchResult<Object?>.success('ok', <String>[]);
      final completer = Completer<DispatchResult<Object?>>();
      fake.queueDispatchResultFuture(completer.future);

      final observed = <ActionState>[];
      late void Function() triggerSubmit;

      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: _sub,
          builder: (ctx, state, submit) {
            observed.add(state);
            triggerSubmit = submit;
            return const SizedBox.shrink();
          },
        ),
      );

      expect(observed.single, isA<Idle>());
      triggerSubmit();
      await tester.pump();
      expect(observed.last, isA<Submitting>());

      completer.complete(result);
      await tester.pumpAndSettle();

      expect(observed[0], isA<Idle>());
      expect(observed.any((s) => s is Submitting), isTrue);
      expect(observed.last, isA<Success>());
      expect((observed.last as Success).result, same(result));
    });

    testWidgets('retry during Submitting reuses the same idempotency key', (
      tester,
    ) async {
      final fake = FakeReaction();
      // Hold both submissions in flight; complete after both fire.
      final c1 = Completer<DispatchResult<Object?>>();
      final c2 = Completer<DispatchResult<Object?>>();
      fake.queueDispatchResultFuture(c1.future);
      fake.queueDispatchResultFuture(c2.future);

      late void Function() triggerSubmit;
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: _sub,
          idempotencyKeyGenerator: _CountingKeyGen(),
          builder: (ctx, state, submit) {
            triggerSubmit = submit;
            return const SizedBox.shrink();
          },
        ),
      );

      triggerSubmit();
      await tester.pump();
      triggerSubmit();
      await tester.pump();

      expect(fake.submittedActions, hasLength(2));
      expect(
        fake.submittedActions[0].idempotencyKey,
        fake.submittedActions[1].idempotencyKey,
        reason:
            'Retry during Submitting MUST reuse the cached idempotency key '
            '(EVS-PRD-reaction-widget-contract/E).',
      );
      // Drain the in-flight futures so the test does not leak completers.
      c1.complete(const DispatchResult<Object?>.success('a', <String>[]));
      c2.complete(const DispatchResult<Object?>.success('b', <String>[]));
      await tester.pumpAndSettle();
    });

    testWidgets('fresh key generated after terminal state', (tester) async {
      final fake = FakeReaction();
      fake.queueDispatchResult(
        const DispatchResult<Object?>.success('a', <String>[]),
      );
      fake.queueDispatchResult(
        const DispatchResult<Object?>.success('b', <String>[]),
      );

      late void Function() triggerSubmit;
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: _sub,
          idempotencyKeyGenerator: _CountingKeyGen(),
          builder: (ctx, state, submit) {
            triggerSubmit = submit;
            return const SizedBox.shrink();
          },
        ),
      );

      triggerSubmit();
      await tester.pumpAndSettle();
      triggerSubmit();
      await tester.pumpAndSettle();

      expect(fake.submittedActions, hasLength(2));
      expect(
        fake.submittedActions[0].idempotencyKey,
        isNot(equals(fake.submittedActions[1].idempotencyKey)),
        reason:
            'A fresh submit after a terminal state MUST mint a new key '
            '(EVS-PRD-reaction-widget-contract/E).',
      );
    });

    testWidgets('consumer-supplied idempotencyKey overrides generation', (
      tester,
    ) async {
      final fake = FakeReaction();
      fake.queueDispatchResult(
        const DispatchResult<Object?>.success('ok', <String>[]),
      );

      late void Function() triggerSubmit;
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: () => _sub(idempotencyKey: 'fixed-key'),
          idempotencyKeyGenerator: _CountingKeyGen(),
          builder: (ctx, state, submit) {
            triggerSubmit = submit;
            return const SizedBox.shrink();
          },
        ),
      );

      triggerSubmit();
      await tester.pumpAndSettle();

      expect(fake.submittedActions.single.idempotencyKey, 'fixed-key');
    });

    testWidgets(
      'dispose during Submitting does not throw / does not callback',
      (tester) async {
        final fake = FakeReaction();
        final completer = Completer<DispatchResult<Object?>>();
        fake.queueDispatchResultFuture(completer.future);

        var builderCalls = 0;
        late void Function() triggerSubmit;
        await pumpReactionWidget(
          tester,
          fake: fake,
          child: ActionBuilder(
            submissionFactory: _sub,
            builder: (ctx, state, submit) {
              builderCalls++;
              triggerSubmit = submit;
              return const SizedBox.shrink();
            },
          ),
        );

        triggerSubmit();
        await tester.pump(); // Submitting build flushes.
        final buildsAtUnmount = builderCalls;

        // Unmount the widget by pumping a tree without ActionBuilder.
        await tester.pumpWidget(const SizedBox.shrink());

        // Complete the in-flight submission AFTER the widget is gone.
        // The state's .then() should silently no-op (mounted == false);
        // any setState would assert-throw.
        completer.complete(
          const DispatchResult<Object?>.success('late', <String>[]),
        );
        await tester.pumpAndSettle();

        expect(
          builderCalls,
          buildsAtUnmount,
          reason: 'Builder MUST NOT be invoked after dispose.',
        );
        expect(
          tester.takeException(),
          isNull,
          reason: 'No setState-on-unmounted exception expected.',
        );
      },
    );

    testWidgets('renders nothing on its own (headless)', (tester) async {
      final fake = FakeReaction();
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ActionBuilder(
          submissionFactory: _sub,
          builder: (ctx, state, submit) => const Text('CUSTOM'),
        ),
      );

      expect(find.text('CUSTOM'), findsOneWidget);
    });
  });
}
