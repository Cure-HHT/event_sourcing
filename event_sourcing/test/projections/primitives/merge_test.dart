// event_sourcing/test/projections/primitives/merge_test.dart
import 'package:event_sourcing/src/projections/primitives/merge.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('Merge.applyDelta', () {
    test('present-non-null overwrites prior', () {
      final result = Merge.applyDelta(const {'a': 1, 'b': 2}, const {'a': 10});
      expect(result, {'a': 10, 'b': 2});
    });

    test('absent key preserves prior', () {
      final result = Merge.applyDelta(const {'a': 1, 'b': 2}, const {'a': 10});
      expect(result['b'], 2);
    });

    test('present-null clears prior', () {
      final result = Merge.applyDelta(
        const {'a': 1, 'b': 2},
        const {'a': null},
      );
      expect(result.containsKey('a'), isTrue);
      expect(result['a'], isNull);
    });

    test('returns unmodifiable map', () {
      final result = Merge.applyDelta(const {'a': 1}, const {'b': 2});
      expect(() => result['c'] = 3, throwsUnsupportedError);
    });

    test('empty delta returns equivalent of prior', () {
      final result = Merge.applyDelta(const {'a': 1}, const {});
      expect(result, {'a': 1});
    });

    test('empty prior returns equivalent of delta', () {
      final result = Merge.applyDelta(const {}, const {'a': 1});
      expect(result, {'a': 1});
    });
  });
}
