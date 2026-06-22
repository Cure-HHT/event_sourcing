// Verifies: EVS-PRD-reaction-widget-contract/D/G

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

typedef _Row = Map<String, Object?>;

_Row _row(String id, {String? title}) => <String, Object?>{
  'aggregateId': id,
  'title': ?title,
};

/// Settle the test after emitting stream events.
///
/// A broadcast `StreamController.add` schedules listener dispatch on a
/// microtask; the listener's callback then runs synchronously. We pump
/// twice to drain any queued microtasks and any frame they scheduled.
Future<void> _settleStream(WidgetTester tester) async {
  await tester.pump();
  await tester.pump();
}

void main() {
  testWidgets('fires onUpdate without rebuilding child', (tester) async {
    final fake = FakeReaction();
    final updates = <Update<_Row>>[];
    var childBuilds = 0;

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: ViewListener<_Row>(
        viewName: 'v',
        mapper: (m) => m,
        onUpdate: (ctx, u) => updates.add(u),
        child: Builder(
          builder: (ctx) {
            childBuilds++;
            return const SizedBox.shrink();
          },
        ),
      ),
    );

    final buildsAfterMount = childBuilds;
    expect(updates, isEmpty);

    fake.emitViewUpdate<_Row>(
      'v',
      Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 1),
    );
    fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 1));
    await _settleStream(tester);

    expect(updates, hasLength(2));
    expect(updates[0], isA<Snapshot<_Row>>());
    expect(updates[1], isA<EndOfReplay<_Row>>());
    expect(
      childBuilds,
      buildsAfterMount,
      reason:
          'ViewListener MUST NOT rebuild the child subtree in response '
          'to view updates (EVS-PRD-reaction-widget-contract/D).',
    );
  });

  testWidgets('cancels subscription on dispose', (tester) async {
    final fake = FakeReaction();
    final updates = <Update<_Row>>[];

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: ViewListener<_Row>(
        viewName: 'v',
        mapper: (m) => m,
        onUpdate: (ctx, u) => updates.add(u),
        child: const SizedBox.shrink(),
      ),
    );

    fake.emitViewUpdate<_Row>(
      'v',
      Snapshot<_Row>(value: _row('a'), sequence: 1),
    );
    await _settleStream(tester);
    expect(updates, hasLength(1));

    // Unmount.
    await tester.pumpWidget(const SizedBox.shrink());

    final countAtUnmount = updates.length;

    // Drive updates that would, if the subscription were still active,
    // grow updates and/or fire onUpdate against an unmounted state.
    fake.emitViewUpdate<_Row>(
      'v',
      Snapshot<_Row>(value: _row('b'), sequence: 2),
    );
    fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
    await tester.pumpAndSettle();

    expect(
      updates.length,
      countAtUnmount,
      reason: 'onUpdate MUST NOT fire after dispose.',
    );
    expect(
      tester.takeException(),
      isNull,
      reason: 'No setState-on-unmounted or stream-after-cancel exception.',
    );
  });

  testWidgets('child renders without decoration (headless)', (tester) async {
    final fake = FakeReaction();

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: ViewListener<_Row>(
        viewName: 'v',
        mapper: (m) => m,
        onUpdate: (_, _) {},
        child: const Text('CUSTOM', textDirection: TextDirection.ltr),
      ),
    );

    expect(
      find.text('CUSTOM'),
      findsOneWidget,
      reason:
          'ViewListener MUST render only its child, with no added '
          'decoration (EVS-PRD-reaction-widget-contract/G).',
    );
  });
}
