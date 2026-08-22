// Verifies: EVS-PRD-reaction-widget-contract/E
// UuidIdempotencyKeyGenerator emits UUID v4 keys (the format the
// widget library is required to use), and the IdempotencyKeyGenerator
// interface admits deterministic stub replacements for tests
// (supporting the consumer-override path in assertion E).
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/state/idempotency_key_generator.dart';

void main() {
  group('UuidIdempotencyKeyGenerator', () {
    test('produces a UUID v4 format string', () {
      final gen = UuidIdempotencyKeyGenerator();
      final key = gen.generate();
      // UUID v4: 8-4-4-4-12 hex, with version nibble 4 and variant
      // nibble 8/9/a/b.
      final pattern = RegExp(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
      );
      expect(pattern.hasMatch(key), isTrue, reason: 'got: $key');
    });

    test('successive calls produce distinct keys', () {
      final gen = UuidIdempotencyKeyGenerator();
      final keys = <String>{for (var i = 0; i < 100; i++) gen.generate()};
      expect(keys.length, equals(100));
    });
  });

  group('IdempotencyKeyGenerator (interface)', () {
    test('can be replaced with a deterministic stub for tests', () {
      final stub = _StubIdempotencyKeyGenerator(['key-1', 'key-2', 'key-3']);
      expect(stub.generate(), equals('key-1'));
      expect(stub.generate(), equals('key-2'));
      expect(stub.generate(), equals('key-3'));
    });
  });
}

class _StubIdempotencyKeyGenerator implements IdempotencyKeyGenerator {
  final List<String> _keys;
  int _i = 0;
  _StubIdempotencyKeyGenerator(this._keys);

  @override
  String generate() => _keys[_i++];
}
