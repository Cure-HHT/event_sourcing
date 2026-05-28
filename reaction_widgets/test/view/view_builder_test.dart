// Verifies: EVS-PRD-reaction-widget-contract/C, /G, /I, /J

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
// Hide ViewState.Disconnected so the bare `Disconnected` identifier in
// this file refers to the transport-side ConnectionStatus variant. The
// rendering-state Disconnected from reaction_widgets is referenced via
// the [vs] prefix.
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart' as vs show Disconnected;
import 'package:reaction_widgets/reaction_widgets.dart' hide Disconnected;

typedef _Row = Map<String, Object?>;

_Row _row(String id, {String? title}) => <String, Object?>{
  'aggregateId': id,
  'title': ?title,
};

String _aggregateIdOf(_Row r) => r['aggregateId']! as String;

/// Settle the test after emitting stream events.
///
/// A broadcast `StreamController.add` schedules listener dispatch on a
/// microtask; the listener's `setState` then schedules a rebuild on a
/// frame. A single `tester.pump()` may run only one of those phases,
/// depending on the timing of the await-chain — so we pump twice (and
/// then once more for any setState fired during the second build) to
/// reliably observe the builder rebuild that the emission caused.
Future<void> _settleStream(WidgetTester tester) async {
  await tester.pump();
  await tester.pump();
}

/// Pump a [ViewBuilder] under test, recording every [ViewState] the
/// builder sees so transitions are inspectable post-hoc.
Future<List<ViewState<_Row>>> _pumpRecording(
  WidgetTester tester, {
  required FakeReaction fake,
  required String viewName,
  bool progressive = false,
}) async {
  final transitions = <ViewState<_Row>>[];
  await pumpReactionWidget(
    tester,
    fake: fake,
    child: ViewBuilder<_Row>(
      viewName: viewName,
      mapper: (m) => m,
      aggregateIdOf: _aggregateIdOf,
      progressive: progressive,
      builder: (ctx, state) {
        transitions.add(state);
        return const SizedBox.shrink();
      },
    ),
  );
  return transitions;
}

void main() {
  group('ViewBuilder (default mode)', () {
    testWidgets('starts Loading; Ready arrives only after EndOfReplay', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      expect(transitions.last, isA<Loading<_Row>>());

      // Pre-EndOfReplay snapshots: default mode keeps state at Loading.
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('b', title: 'B'), sequence: 2),
      );
      await _settleStream(tester);
      expect(
        transitions.last,
        isA<Loading<_Row>>(),
        reason:
            'Default mode MUST NOT surface Ready before EndOfReplay '
            '(EVS-PRD-reaction-widget-contract/I).',
      );

      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());
      final ready = transitions.last as Ready<_Row>;
      expect(ready.rows.length, 2);
      expect(ready.rows.map(_aggregateIdOf), containsAll(<String>['a', 'b']));
    });

    testWidgets('Delta after Ready updates rows in place', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 1));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());

      // Delta for existing aggregate id: row replaced in place.
      fake.emitViewUpdate<_Row>(
        'v',
        Delta<_Row>(
          value: _row('a', title: 'A-updated'),
          sequence: 2,
          cause: 'evt',
        ),
      );
      await _settleStream(tester);
      final ready = transitions.last as Ready<_Row>;
      expect(ready.rows, hasLength(1));
      expect(ready.rows.single['title'], 'A-updated');
    });

    testWidgets('Tombstone removes the matching row from Ready', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('b'), sequence: 2),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
      await _settleStream(tester);
      expect((transitions.last as Ready<_Row>).rows, hasLength(2));

      fake.emitViewUpdate<_Row>(
        'v',
        const Tombstone<_Row>(aggregateId: 'a', sequence: 3),
      );
      await _settleStream(tester);

      final ready = transitions.last as Ready<_Row>;
      expect(ready.rows, hasLength(1));
      expect(_aggregateIdOf(ready.rows.single), 'b');
    });

    testWidgets('ConnectionStatus.Reconnecting -> Disconnected retains rows', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 1));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());

      fake.driveConnectionStatus(const Reconnecting());
      await _settleStream(tester);

      expect(transitions.last, isA<vs.Disconnected<_Row>>());
      final dc = transitions.last as vs.Disconnected<_Row>;
      expect(dc.lastRows, hasLength(1));
      expect(_aggregateIdOf(dc.lastRows.single), 'a');
      expect(dc.error, isA<Reconnecting>());
    });

    testWidgets('Connected after Disconnected re-enters Loading then Ready', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 1));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());

      fake.driveConnectionStatus(const Reconnecting());
      await _settleStream(tester);
      expect(transitions.last, isA<vs.Disconnected<_Row>>());

      fake.driveConnectionStatus(const Connected());
      await _settleStream(tester);
      expect(
        transitions.last,
        isA<Loading<_Row>>(),
        reason: 'Reconnect MUST reset to Loading before fresh EndOfReplay.',
      );

      // Fresh replay arrives.
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a', title: 'fresh'), sequence: 2),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());
      expect((transitions.last as Ready<_Row>).rows.single['title'], 'fresh');
    });

    testWidgets('cancels subscriptions on dispose', (tester) async {
      final fake = FakeReaction();
      var builderCalls = 0;
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ViewBuilder<_Row>(
          viewName: 'v',
          mapper: (m) => m,
          aggregateIdOf: _aggregateIdOf,
          builder: (ctx, state) {
            builderCalls++;
            return const SizedBox.shrink();
          },
        ),
      );

      final callsAtUnmount = builderCalls;

      // Unmount the widget by pumping a tree without ViewBuilder.
      await tester.pumpWidget(const SizedBox.shrink());

      // Drive updates that would, if the subscription were still active,
      // trigger setState on an unmounted state object (asserting).
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 1));
      fake.driveConnectionStatus(const Reconnecting());
      await tester.pumpAndSettle();

      expect(
        builderCalls,
        callsAtUnmount,
        reason: 'Builder MUST NOT be invoked after dispose.',
      );
      expect(
        tester.takeException(),
        isNull,
        reason: 'No setState-on-unmounted exception expected.',
      );
    });
  });

  group('ViewBuilder (progressive mode)', () {
    testWidgets('progressive mode emits Ready during snapshot replay', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
        progressive: true,
      );

      expect(transitions.last, isA<Loading<_Row>>());

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      await _settleStream(tester);
      expect(
        transitions.last,
        isA<Ready<_Row>>(),
        reason:
            'Progressive mode MUST surface partial rows before EndOfReplay '
            '(EVS-PRD-reaction-widget-contract/J).',
      );
      expect((transitions.last as Ready<_Row>).rows, hasLength(1));

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('b'), sequence: 2),
      );
      await _settleStream(tester);
      expect((transitions.last as Ready<_Row>).rows, hasLength(2));

      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
      await _settleStream(tester);
      expect(transitions.last, isA<Ready<_Row>>());
      expect((transitions.last as Ready<_Row>).rows, hasLength(2));
    });
  });
}
