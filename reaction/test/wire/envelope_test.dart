import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/wire/envelope.dart';

void main() {
  group('readType', () {
    test('returns the type field', () {
      expect(readType({'type': 'snapshot'}), 'snapshot');
    });
    test('throws on missing type', () {
      expect(() => readType({}), throwsA(isA<FormatException>()));
    });
    test('throws on non-string type', () {
      expect(() => readType({'type': 42}), throwsA(isA<FormatException>()));
    });
  });

  group('requireString', () {
    test('returns the string field', () {
      expect(requireString({'k': 'v'}, 'k'), 'v');
    });
    test('throws on missing key', () {
      expect(() => requireString({}, 'k'), throwsA(isA<FormatException>()));
    });
    test('throws on non-string', () {
      expect(
        () => requireString({'k': 1}, 'k'),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
