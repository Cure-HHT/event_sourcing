// Verifies: EVS-PRD-reaction-widget-contract/A/B

import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

void main() {
  testWidgets('threads ReactionScope down the tree', (tester) async {
    final fake = FakeReaction();
    late ReactionScope captured;

    await pumpReactionWidget(
      tester,
      fake: fake,
      child: Builder(
        builder: (ctx) {
          captured = ReActionScope.of(ctx);
          return const SizedBox.shrink();
        },
      ),
    );

    expect(captured, same(fake));
  });

  testWidgets('ReActionScope.of throws helpfully if not in tree', (
    tester,
  ) async {
    Object? error;
    await tester.pumpWidget(
      Builder(
        builder: (ctx) {
          try {
            ReActionScope.of(ctx);
          } catch (e) {
            error = e;
          }
          return const SizedBox.shrink();
        },
      ),
    );
    expect(error, isNotNull);
    expect(error.toString(), contains('ReActionScope'));
  });

  testWidgets('rebuilds dependents when scope reference changes', (
    tester,
  ) async {
    final f1 = FakeReaction();
    final f2 = FakeReaction();
    int builds = 0;

    Widget wrap(ReactionScope s) => ReActionScope(
      scope: s,
      child: Builder(
        builder: (ctx) {
          ReActionScope.of(ctx);
          builds++;
          return const SizedBox.shrink();
        },
      ),
    );

    await tester.pumpWidget(wrap(f1));
    expect(builds, 1);
    await tester.pumpWidget(wrap(f2));
    expect(builds, 2);
  });
}
