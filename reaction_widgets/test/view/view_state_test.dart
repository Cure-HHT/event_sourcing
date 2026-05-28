// Verifies: EVS-PRD-reaction-widget-contract/I

import 'package:flutter_test/flutter_test.dart';
import 'package:reaction_widgets/reaction_widgets.dart';

void main() {
  group('ViewState', () {
    test('three sealed variants instantiate at the declared type', () {
      const ViewState<int> a = Loading<int>();
      const ViewState<int> b = Ready<int>([1, 2, 3]);
      const ViewState<int> c = Disconnected<int>([1], 'err');

      expect(a, isA<Loading<int>>());
      expect(b, isA<Ready<int>>());
      expect(c, isA<Disconnected<int>>());
    });

    test('Ready carries rows; Disconnected retains lastRows + error', () {
      const rows = <int>[1, 2, 3];
      expect(const Ready<int>(rows).rows, equals(rows));
      expect(const Disconnected<int>(rows, 'err').lastRows, equals(rows));
      expect(const Disconnected<int>(rows, 'err').error, equals('err'));
    });

    test('exhaustive switch compiles and dispatches correctly', () {
      String label(ViewState<int> s) => switch (s) {
        Loading<int>() => 'loading',
        Ready<int>(:final rows) => 'ready(${rows.length})',
        Disconnected<int>(:final lastRows) =>
          'disconnected(${lastRows.length})',
      };

      expect(label(const Loading()), 'loading');
      expect(label(const Ready([1, 2])), 'ready(2)');
      expect(label(const Disconnected([1], 'e')), 'disconnected(1)');
    });
  });
}
