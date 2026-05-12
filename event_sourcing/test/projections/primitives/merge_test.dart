// event_sourcing/test/projections/primitives/merge_test.dart
//
// Verifies: EVS-PRD-materializer/A — Merge.applyDelta and
//   Merge.applyDeepDelta are library-supplied materializer primitives.
// Verifies: EVS-PRD-materializer/B — both methods are pure functions; tests
//   confirm deterministic behavior for null-as-clear, absent-preserves,
//   nested-map recursion, defensive Map<dynamic,dynamic> branch, and
//   empty-delta/prior edge cases.
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

  group('Merge.applyDeepDelta', () {
    test('top-level scalar: present-non-null overwrites prior', () {
      final result = Merge.applyDeepDelta(
        const {'a': 1, 'b': 2},
        const {'a': 10},
      );
      expect(result, {'a': 10, 'b': 2});
    });

    test('top-level scalar: absent key preserves prior', () {
      final result = Merge.applyDeepDelta(
        const {'a': 1, 'b': 2},
        const {'a': 10},
      );
      expect(result['b'], 2);
    });

    test('top-level scalar: null clears key (null-as-clear)', () {
      final result = Merge.applyDeepDelta(
        const {'a': 1, 'b': 2},
        const {'a': null},
      );
      expect(result.containsKey('a'), isTrue);
      expect(result['a'], isNull);
    });

    test('nested map: inner keys absent from delta are preserved', () {
      final result = Merge.applyDeepDelta(
        const {
          'answers': {'q1': 'yes', 'q2': 'no'},
        },
        const {
          'answers': {'q2': null},
        },
      );
      final answers = result['answers'] as Map<String, Object?>;
      expect(answers['q1'], 'yes'); // preserved
      expect(answers['q2'], isNull); // cleared
    });

    test('nested map: inner non-null overwrites', () {
      final result = Merge.applyDeepDelta(
        const {
          'answers': {'q1': 'yes'},
        },
        const {
          'answers': {'q1': 'no'},
        },
      );
      final answers = result['answers'] as Map<String, Object?>;
      expect(answers['q1'], 'no');
    });

    test('nested map at depth >= 2 recurses correctly', () {
      final result = Merge.applyDeepDelta(
        const {
          'a': {
            'b': {'c': 'old', 'd': 'keep'},
          },
        },
        const {
          'a': {
            'b': {'c': 'new'},
          },
        },
      );
      final b =
          ((result['a'] as Map<String, Object?>)['b'] as Map<String, Object?>);
      expect(b['c'], 'new');
      expect(b['d'], 'keep');
    });

    test('empty delta returns copy of prior', () {
      final result = Merge.applyDeepDelta(const {'a': 1}, const {});
      expect(result, {'a': 1});
    });

    test('empty prior returns copy of delta', () {
      final result = Merge.applyDeepDelta(const {}, const {'a': 1});
      expect(result, {'a': 1});
    });

    test(
      'defensive branch: decoded-from-JSON Map<dynamic,dynamic> is handled',
      () {
        // Simulate what Sembast / json.decode produces at runtime.
        // The static type says Map<String, Object?> but the runtime
        // value may be Map<dynamic, dynamic>.
        final priorRaw = <dynamic, dynamic>{
          'nested': <dynamic, dynamic>{'x': 1, 'y': 2},
        };
        final deltaRaw = <dynamic, dynamic>{
          'nested': <dynamic, dynamic>{'x': 99},
        };
        final prior = Map<String, Object?>.from(priorRaw);
        final delta = Map<String, Object?>.from(deltaRaw);
        final result = Merge.applyDeepDelta(prior, delta);
        final nested = result['nested'] as Map<String, Object?>;
        expect(nested['x'], 99);
        expect(nested['y'], 2);
      },
    );
  });
}
