import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

const _denial = SubscriptionDenied(
  viewName: 'v',
  reason: SubscriptionDenyReason.viewPermissionDenied,
);

void main() {
  // Verifies: EVS-PRD-reaction-widget-contract/I
  group('ViewState', () {
    test('six sealed variants instantiate at the declared type', () {
      const ViewState<int> a = Loading<int>();
      const ViewState<int> b = Ready<int>([1, 2, 3]);
      const ViewState<int> c = Stale<int>([1], 'err');
      const ViewState<int> d = Converging<int>('v');
      const ViewState<int> e = Rejected<int>(_denial);
      final ViewState<int> f = Errored<int>('boom', StackTrace.empty);

      expect(a, isA<Loading<int>>());
      expect(b, isA<Ready<int>>());
      expect(c, isA<Stale<int>>());
      expect(d, isA<Converging<int>>());
      expect(e, isA<Rejected<int>>());
      expect(f, isA<Errored<int>>());
    });

    test('Converging names the view; Rejected carries the denial; Errored '
        'carries the error and its stack trace', () {
      expect(const Converging<int>('v').viewName, 'v');
      expect(const Rejected<int>(_denial).denial, same(_denial));
      final errored = Errored<int>('boom', StackTrace.empty);
      expect(errored.error, 'boom');
      expect(errored.stackTrace, same(StackTrace.empty));
    });

    test('Ready carries rows; Stale retains lastRows + error', () {
      const rows = <int>[1, 2, 3];
      expect(const Ready<int>(rows).rows, equals(rows));
      expect(const Stale<int>(rows, 'err').lastRows, equals(rows));
      expect(const Stale<int>(rows, 'err').connectionStatus, equals('err'));
    });

    test('exhaustive switch compiles and dispatches correctly', () {
      String label(ViewState<int> s) => switch (s) {
        Loading<int>() => 'loading',
        Ready<int>(:final rows) => 'ready(${rows.length})',
        Stale<int>(:final lastRows) => 'stale(${lastRows.length})',
        Converging<int>(:final viewName) => 'converging($viewName)',
        Rejected<int>(:final denial) => 'rejected(${denial.viewName})',
        Errored<int>(:final error) => 'errored($error)',
      };

      expect(label(const Loading()), 'loading');
      expect(label(const Ready([1, 2])), 'ready(2)');
      expect(label(const Stale([1], 'e')), 'stale(1)');
      expect(label(const Converging('v')), 'converging(v)');
      expect(label(const Rejected(_denial)), 'rejected(v)');
      expect(label(Errored('x', StackTrace.empty)), 'errored(x)');
    });
  });
}
