// event_sourcing/test/projections/primitives/derived_field_test.dart
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('DottedPathLookup', () {
    test('resolves a single segment', () {
      const lookup = DottedPathLookup('answers', fallback: ConstantValue(null));
      final value = lookup.resolve(
        rowState: const {
          'answers': {'date': '2026-05-09'},
        },
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, {'date': '2026-05-09'});
    });

    test('resolves a dotted path', () {
      const lookup = DottedPathLookup(
        'answers.date',
        fallback: ConstantValue(null),
      );
      final value = lookup.resolve(
        rowState: const {
          'answers': {'date': '2026-05-09'},
        },
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, '2026-05-09');
    });

    test('falls back when path resolves to non-Map mid-traversal', () {
      const lookup = DottedPathLookup(
        'answers.date.year',
        fallback: ConstantValue('NA'),
      );
      final value = lookup.resolve(
        rowState: const {
          'answers': {'date': '2026-05-09'},
        },
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, 'NA');
    });

    test('falls back when key absent', () {
      const lookup = DottedPathLookup(
        'missing.key',
        fallback: ConstantValue('NA'),
      );
      final value = lookup.resolve(
        rowState: const {
          'answers': {'date': 'x'},
        },
        firstEventTimestamp: DateTime.utc(2026, 1, 1),
      );
      expect(value, 'NA');
    });

    test('FirstEventTimestamp fallback returns ISO8601 string', () {
      const lookup = DottedPathLookup(
        'missing',
        fallback: FirstEventTimestamp(),
      );
      final value = lookup.resolve(
        rowState: const {},
        firstEventTimestamp: DateTime.utc(2026, 5, 9, 12, 0, 0),
      );
      expect(value, '2026-05-09T12:00:00.000Z');
    });
  });
}
