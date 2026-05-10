// event_sourcing/test/promoters/primitives/transform_test.dart
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('RenameField', () {
    test('renames a present field', () {
      const t = RenameField(from: 'old', to: 'new');
      final result = t.apply(const {'old': 1, 'other': 2});
      expect(result, {'new': 1, 'other': 2});
    });

    test('no-op when source absent', () {
      const t = RenameField(from: 'missing', to: 'new');
      final result = t.apply(const {'other': 2});
      expect(result, {'other': 2});
    });

    test('throws when target already present', () {
      const t = RenameField(from: 'old', to: 'new');
      expect(() => t.apply(const {'old': 1, 'new': 2}), throwsStateError);
    });
  });

  group('DefaultField', () {
    test('adds field with default when absent', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {});
      expect(result, {'x': 'd'});
    });

    test('preserves existing value when present', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {'x': 'real'});
      expect(result, {'x': 'real'});
    });

    test('present-null value is preserved (not replaced with default)', () {
      const t = DefaultField(fieldName: 'x', defaultValue: 'd');
      final result = t.apply(const {'x': null});
      expect(result, {'x': null});
    });
  });

  group('DropField', () {
    test('removes the named field', () {
      const t = DropField(fieldName: 'gone');
      final result = t.apply(const {'gone': 1, 'kept': 2});
      expect(result, {'kept': 2});
    });

    test('no-op when field absent', () {
      const t = DropField(fieldName: 'missing');
      final result = t.apply(const {'kept': 2});
      expect(result, {'kept': 2});
    });
  });

  group('DeriveField', () {
    test('computes new field via derivation primitive', () {
      const t = DeriveField(
        fieldName: 'echo',
        from: DottedPathLookup('source', fallback: ConstantValue('NA')),
      );
      final result = t.apply(const {
        'source': 'hello',
      }, firstEventTimestamp: DateTime.utc(2026, 1, 1));
      expect(result['echo'], 'hello');
    });
  });

  group('TransformChain.applyAll', () {
    test('applies transforms in order', () {
      const chain = [
        RenameField(from: 'a', to: 'b'),
        DefaultField(fieldName: 'c', defaultValue: 3),
      ];
      final result = TransformChain.applyAll(chain, const {
        'a': 1,
      }, firstEventTimestamp: DateTime.utc(2026, 1, 1));
      expect(result, {'b': 1, 'c': 3});
    });
  });
}
