import 'package:event_sourcing/src/storage/record_characters.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  // Verifies: EVS-DEV-event-record/L
  group('recordFieldWithNulCharacter', () {
    test('returns null for a record free of U+0000', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{
          'event_id': 'e-1',
          'data': <String, Object?>{
            'note': 'ordinary',
            'items': <Object?>['a', 'b'],
          },
        }),
        isNull,
      );
    });

    test('names the top-level field of a U+0000 in a direct value', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{
          'event_id': 'e-1',
          'data': <String, Object?>{'note': 'x\u0000y'},
        }),
        'data',
      );
    });

    test('names the top-level field of a U+0000 in a nested map value', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{
          'event_id': 'e-1',
          'data': <String, Object?>{
            'nested': <String, Object?>{'note': 'x\u0000y'},
          },
        }),
        'data',
      );
    });

    test('names the top-level field of a U+0000 in a nested map key', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{
          'event_id': 'e-1',
          'data': <String, Object?>{
            'nested': <String, Object?>{'k\u0000ey': 1},
          },
        }),
        'data',
      );
    });

    test('names the top-level field of a U+0000 inside a list', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{
          'event_id': 'e-1',
          'data': <String, Object?>{
            'items': <Object?>['a', 'x\u0000y'],
          },
        }),
        'data',
      );
    });

    test('names the top-level field when the top-level key itself carries '
        'U+0000', () {
      expect(
        recordFieldWithNulCharacter(<String, Object?>{'ev\u0000ent_id': 'e'}),
        'ev\u0000ent_id',
      );
    });
  });
}
