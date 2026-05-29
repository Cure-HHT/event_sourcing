// Verifies: EVS-PRD-reaction-widget-contract/C, /G, /I, /J

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

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

    testWidgets(
      'default mode buffers a pre-EndOfReplay Delta/Tombstone and stays '
      'Loading until EndOfReplay',
      (tester) async {
        // Pins the consistent-snapshot invariant
        // (EVS-PRD-reaction-widget-contract/I): in default mode the
        // builder MUST NOT surface Ready before EndOfReplay — not even
        // when the pre-replay updates are Deltas/Tombstones rather than
        // Snapshots. The off-contract row is still accumulated, so it
        // appears once EndOfReplay arrives. (Coverage gap noted during
        // the 2026-05-29 Tier-2 investigation: the existing pre-EOR test
        // only drove Snapshots.)
        final fake = FakeReaction();
        final transitions = await _pumpRecording(
          tester,
          fake: fake,
          viewName: 'v',
        );
        expect(transitions.last, isA<Loading<_Row>>());

        // A Delta before EndOfReplay: buffered, NOT surfaced.
        fake.emitViewUpdate<_Row>(
          'v',
          Delta<_Row>(
            value: _row('a', title: 'A'),
            sequence: 1,
            cause: 'evt',
          ),
        );
        await _settleStream(tester);
        expect(
          transitions.last,
          isA<Loading<_Row>>(),
          reason:
              'default mode MUST NOT surface Ready on a pre-EndOfReplay '
              'Delta',
        );

        // A Tombstone before EndOfReplay: also no Ready.
        fake.emitViewUpdate<_Row>(
          'v',
          const Tombstone<_Row>(aggregateId: 'ghost', sequence: 2),
        );
        await _settleStream(tester);
        expect(
          transitions.last,
          isA<Loading<_Row>>(),
          reason:
              'default mode MUST NOT surface Ready on a pre-EndOfReplay '
              'Tombstone',
        );

        // EndOfReplay finally promotes to Ready, surfacing the buffered row.
        fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
        await _settleStream(tester);
        expect(transitions.last, isA<Ready<_Row>>());
        final ready = transitions.last as Ready<_Row>;
        expect(ready.rows, hasLength(1));
        expect(ready.rows.single['title'], 'A');
      },
    );

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

    testWidgets('Snapshot with null value skips accumulation', (tester) async {
      // Substrate emits Snapshot(value: null) for an explicitly-listed-but-
      // empty aggregate (the aggregate id was named in the subscription
      // filter but no rows currently exist for it). ViewBuilder's _onUpdate
      // must skip accumulation for that case rather than crash or insert a
      // null/spurious row.
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake: fake,
        viewName: 'v',
      );

      // First snapshot has null value -> skipped.
      fake.emitViewUpdate<_Row>(
        'v',
        const Snapshot<_Row>(value: null, sequence: 1),
      );
      // Second snapshot has a real row -> accumulated.
      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 2),
      );
      fake.emitViewUpdate<_Row>('v', const EndOfReplay<_Row>(sequence: 2));
      await _settleStream(tester);

      expect(transitions.last, isA<Ready<_Row>>());
      final ready = transitions.last as Ready<_Row>;
      expect(
        ready.rows,
        hasLength(1),
        reason:
            'Snapshot(value: null) MUST be skipped; only the row from the '
            'second non-null snapshot should be accumulated.',
      );
      expect(_aggregateIdOf(ready.rows.single), 'a');
    });

    testWidgets('ConnectionStatus.Reconnecting -> Stale retains rows', (
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

      expect(transitions.last, isA<Stale<_Row>>());
      final dc = transitions.last as Stale<_Row>;
      expect(dc.lastRows, hasLength(1));
      expect(_aggregateIdOf(dc.lastRows.single), 'a');
      expect(dc.error, isA<Reconnecting>());
    });

    testWidgets('Connected after Stale re-enters Loading then Ready', (
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
      expect(transitions.last, isA<Stale<_Row>>());

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

    testWidgets(
      'progressive mode emits Ready on Delta during snapshot replay',
      (tester) async {
        final fake = FakeReaction();
        final transitions = await _pumpRecording(
          tester,
          fake: fake,
          viewName: 'v',
          progressive: true,
        );

        // Snapshot installs row 'a'.
        fake.emitViewUpdate<_Row>(
          'v',
          Snapshot<_Row>(value: _row('a', title: 'A'), sequence: 1),
        );
        await _settleStream(tester);
        expect(transitions.last, isA<Ready<_Row>>());
        expect((transitions.last as Ready<_Row>).rows.single['title'], 'A');

        // Delta during pre-EndOfReplay: progressive mode MUST surface
        // the updated row immediately, not buffer until EndOfReplay
        // (parity with Snapshot behaviour under progressive=true).
        fake.emitViewUpdate<_Row>(
          'v',
          Delta<_Row>(
            value: _row('a', title: 'A-updated'),
            sequence: 2,
            cause: 'evt',
          ),
        );
        await _settleStream(tester);
        expect(transitions.last, isA<Ready<_Row>>());
        expect(
          (transitions.last as Ready<_Row>).rows.single['title'],
          'A-updated',
          reason:
              'Progressive mode MUST emit Ready on Delta during replay '
              '(EVS-PRD-reaction-widget-contract/J).',
        );

        // Tombstone during pre-EndOfReplay: same parity expectation.
        fake.emitViewUpdate<_Row>(
          'v',
          const Tombstone<_Row>(aggregateId: 'a', sequence: 3),
        );
        await _settleStream(tester);
        expect(transitions.last, isA<Ready<_Row>>());
        expect(
          (transitions.last as Ready<_Row>).rows,
          isEmpty,
          reason:
              'Progressive mode MUST emit Ready on Tombstone during replay '
              '(EVS-PRD-reaction-widget-contract/J).',
        );
      },
    );
  });
}
