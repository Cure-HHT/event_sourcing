import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';
import 'package:reaction_widgets_testing/reaction_widgets_testing.dart';

typedef _Row = Map<String, Object?>;

_Row _row(String id) => <String, Object?>{'aggregateId': id};

String _aggregateIdOf(_Row r) => r['aggregateId']! as String;

Future<void> _settle(WidgetTester tester) async {
  await tester.pump();
  await tester.pump();
}

Future<List<ViewState<_Row>>> _pumpRecording(
  WidgetTester tester,
  FakeReaction fake, {
  bool isProgressive = false,
}) async {
  final transitions = <ViewState<_Row>>[];
  await pumpReactionWidget(
    tester,
    fake: fake,
    child: ViewBuilder<_Row>(
      viewName: 'v',
      mapper: (m) => m,
      aggregateIdOf: _aggregateIdOf,
      isProgressive: isProgressive,
      builder: (ctx, state) {
        transitions.add(state);
        return const SizedBox.shrink();
      },
    ),
  );
  return transitions;
}

void _replay(FakeReaction fake, List<String> ids, int sequence) {
  for (final id in ids) {
    fake.emitViewUpdate<_Row>(
      'v',
      Snapshot<_Row>(value: _row(id), sequence: sequence),
    );
  }
  fake.emitViewUpdate<_Row>(
    'v',
    EndOfReplay<_Row>(sequence: sequence, state: ViewConvergenceState.current),
  );
}

List<String> _ids(ViewState<_Row> s) =>
    (s as Ready<_Row>).rows.map(_aggregateIdOf).toList();

const _denial = SubscriptionDenied(
  viewName: 'v',
  reason: SubscriptionDenyReason.viewPermissionDenied,
);

void main() {
  // Verifies: EVS-PRD-reaction-widget-contract/I
  group('ViewBuilder subscription errors', () {
    // Verifies: EVS-PRD-reaction-widget-contract/M
    testWidgets('a denial surfaces Rejected carrying the typed denial, and '
        'nothing is left uncaught', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(tester, fake);

      fake.emitViewError('v', _denial);
      await _settle(tester);

      expect(tester.takeException(), isNull);
      expect(
        transitions.last,
        isA<Rejected<_Row>>().having((s) => s.denial, 'denial', same(_denial)),
      );
    });

    // Verifies: EVS-PRD-reaction-widget-contract/N
    testWidgets('Rejected is terminal: a reconnect and later updates leave it '
        'in place', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(tester, fake);

      fake.emitViewError('v', _denial);
      await _settle(tester);
      fake.driveConnectionStatus(const Reconnecting());
      await _settle(tester);
      fake.driveConnectionStatus(const Connected());
      await _settle(tester);
      _replay(fake, ['a'], 1);
      await _settle(tester);

      expect(transitions.last, isA<Rejected<_Row>>());
    });

    // Verifies: EVS-PRD-reaction-widget-contract/M
    testWidgets('a converging refusal surfaces Converging naming the refused '
        'view, then Loading and Ready as the recovered rows arrive', (
      tester,
    ) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(tester, fake);

      fake.emitViewError('v', const ViewConvergingRefusal('v_index'));
      await _settle(tester);

      expect(tester.takeException(), isNull);
      expect(
        transitions.last,
        isA<Converging<_Row>>().having(
          (s) => s.viewName,
          'viewName',
          'v_index',
        ),
      );

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      await _settle(tester);
      expect(transitions.last, isA<Loading<_Row>>());

      fake.emitViewUpdate<_Row>(
        'v',
        const EndOfReplay<_Row>(
          sequence: 1,
          state: ViewConvergenceState.current,
        ),
      );
      await _settle(tester);
      expect(_ids(transitions.last), ['a']);
    });

    testWidgets('repeated refusals stay Converging; the recovered replay '
        'replaces the rows held before the refusal', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(tester, fake);

      _replay(fake, ['a'], 1);
      await _settle(tester);
      expect(_ids(transitions.last), ['a']);

      for (var i = 0; i < 3; i++) {
        fake.emitViewError('v', const ViewConvergingRefusal('v'));
        await _settle(tester);
        expect(transitions.last, isA<Converging<_Row>>());
      }

      _replay(fake, ['b'], 2);
      await _settle(tester);
      expect(tester.takeException(), isNull);
      expect(_ids(transitions.last), ['b']);
    });

    testWidgets('progressive mode leaves Converging for Ready on the first '
        'recovered row', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(
        tester,
        fake,
        isProgressive: true,
      );

      fake.emitViewError('v', const ViewConvergingRefusal('v'));
      await _settle(tester);
      expect(transitions.last, isA<Converging<_Row>>());

      fake.emitViewUpdate<_Row>(
        'v',
        Snapshot<_Row>(value: _row('a'), sequence: 1),
      );
      await _settle(tester);
      expect(_ids(transitions.last), ['a']);
    });

    // Verifies: EVS-PRD-reaction-widget-contract/M
    testWidgets('any other error surfaces Errored carrying the error and its '
        'stack trace, terminal thereafter', (tester) async {
      final fake = FakeReaction();
      final transitions = await _pumpRecording(tester, fake);

      final error = StateError('transport broke');
      final trace = StackTrace.current;
      fake.emitViewError('v', error, trace);
      await _settle(tester);

      expect(tester.takeException(), isNull);
      expect(
        transitions.last,
        isA<Errored<_Row>>()
            .having((s) => s.error, 'error', same(error))
            .having((s) => s.stackTrace, 'stackTrace', same(trace)),
      );

      fake.driveConnectionStatus(const Reconnecting());
      await _settle(tester);
      _replay(fake, ['a'], 1);
      await _settle(tester);
      expect(transitions.last, isA<Errored<_Row>>());
    });
  });

  // Verifies: EVS-PRD-reaction-widget-contract/K
  group('semanticIdentifier tokens for error states', () {
    Future<String?> tokenAfter(
      WidgetTester tester,
      void Function(FakeReaction) drive,
    ) async {
      final handle = tester.ensureSemantics();
      final fake = FakeReaction();
      await pumpReactionWidget(
        tester,
        fake: fake,
        child: ViewBuilder<_Row>(
          viewName: 'v',
          semanticIdentifier: 'view',
          mapper: (m) => m,
          aggregateIdOf: _aggregateIdOf,
          builder: (ctx, state) => const SizedBox(key: ValueKey('leaf')),
        ),
      );
      drive(fake);
      await _settle(tester);
      final value = tester
          .getSemantics(find.byKey(const ValueKey('leaf')))
          .value;
      handle.dispose();
      return value;
    }

    testWidgets('converging', (tester) async {
      expect(
        await tokenAfter(
          tester,
          (f) => f.emitViewError('v', const ViewConvergingRefusal('v')),
        ),
        'converging',
      );
    });

    testWidgets('rejected', (tester) async {
      expect(
        await tokenAfter(tester, (f) => f.emitViewError('v', _denial)),
        'rejected',
      );
    });

    testWidgets('errored', (tester) async {
      expect(
        await tokenAfter(tester, (f) => f.emitViewError('v', StateError('x'))),
        'errored',
      );
    });
  });
}
