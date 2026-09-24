import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('EntryTypeVersion', () {
    // Verifies: EVS-DEV-version-compatibility/A
    test('equality and hashCode follow major and minor', () {
      expect(const EntryTypeVersion(1, 2), const EntryTypeVersion(1, 2));
      expect(
        const EntryTypeVersion(1, 2).hashCode,
        const EntryTypeVersion(1, 2).hashCode,
      );
      expect(const EntryTypeVersion(1, 2), isNot(const EntryTypeVersion(1, 3)));
      expect(const EntryTypeVersion(1, 2), isNot(const EntryTypeVersion(2, 2)));
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('orders by major, then minor', () {
      final sorted = <EntryTypeVersion>[
        const EntryTypeVersion(2, 0),
        const EntryTypeVersion(1, 10),
        const EntryTypeVersion(1, 2),
        const EntryTypeVersion(3, 1),
      ]..sort();
      expect(sorted, const <EntryTypeVersion>[
        EntryTypeVersion(1, 2),
        EntryTypeVersion(1, 10),
        EntryTypeVersion(2, 0),
        EntryTypeVersion(3, 1),
      ]);
      expect(
        const EntryTypeVersion(1, 9) < const EntryTypeVersion(2, 0),
        isTrue,
      );
      expect(
        const EntryTypeVersion(2, 0) > const EntryTypeVersion(1, 9),
        isTrue,
      );
      expect(
        const EntryTypeVersion(1, 1) <= const EntryTypeVersion(1, 1),
        isTrue,
      );
      expect(
        const EntryTypeVersion(1, 1) >= const EntryTypeVersion(1, 2),
        isFalse,
      );
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('versions are compatible exactly when their majors are equal', () {
      expect(
        const EntryTypeVersion(
          1,
          0,
        ).isCompatibleWith(const EntryTypeVersion(1, 7)),
        isTrue,
      );
      expect(
        const EntryTypeVersion(
          1,
          7,
        ).isCompatibleWith(const EntryTypeVersion(2, 0)),
        isFalse,
      );
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('nextMinor raises the minor within the major', () {
      expect(
        const EntryTypeVersion(1, 3).nextMinor,
        const EntryTypeVersion(1, 4),
      );
      expect(const EntryTypeVersion(1, 3).toString(), '1.3');
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('JSON round-trips', () {
      const v = EntryTypeVersion(4, 11);
      expect(v.toJson(), <String, Object?>{'major': 4, 'minor': 11});
      expect(EntryTypeVersion.fromJson(v.toJson()), v);
      // A map whose key and value types are not narrowed (as a storage
      // engine hands one back) parses too.
      expect(
        EntryTypeVersion.fromJson(<dynamic, dynamic>{'major': 4, 'minor': 11}),
        v,
      );
    });

    // Verifies: EVS-DEV-version-compatibility/A
    test('fromJson refuses malformed values, naming the key', () {
      final cases = <String, (Object?, String)>{
        'missing major': (<String, Object?>{'minor': 0}, 'major'),
        'missing minor': (<String, Object?>{'major': 1}, 'minor'),
        'string major': (<String, Object?>{'major': '1', 'minor': 0}, 'major'),
        'string minor': (<String, Object?>{'major': 1, 'minor': '0'}, 'minor'),
        'double minor': (<String, Object?>{'major': 1, 'minor': 0.5}, 'minor'),
        'negative minor': (<String, Object?>{'major': 1, 'minor': -1}, 'minor'),
        'zero major': (<String, Object?>{'major': 0, 'minor': 0}, 'major'),
        'not a map': (1, 'major'),
        'an extra key': (
          <String, Object?>{'major': 1, 'minor': 0, 'patch': 0},
          'patch',
        ),
      };
      for (final entry in cases.entries) {
        final (value, key) = entry.value;
        expect(
          () => EntryTypeVersion.fromJson(value),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains(key),
            ),
          ),
          reason: entry.key,
        );
      }
    });
  });

  group('DataFormatVersion', () {
    // Verifies: EVS-DEV-version-compatibility/C
    test('equality, ordering, compatibility and nextMinor', () {
      expect(const DataFormatVersion(2, 0), const DataFormatVersion(2, 0));
      expect(
        const DataFormatVersion(2, 0).hashCode,
        const DataFormatVersion(2, 0).hashCode,
      );
      expect(
        const DataFormatVersion(2, 0),
        isNot(const DataFormatVersion(2, 1)),
      );
      expect(
        const DataFormatVersion(2, 9).compareTo(const DataFormatVersion(3, 0)),
        lessThan(0),
      );
      expect(
        const DataFormatVersion(
          2,
          0,
        ).isCompatibleWith(const DataFormatVersion(2, 7)),
        isTrue,
      );
      expect(
        const DataFormatVersion(
          2,
          0,
        ).isCompatibleWith(const DataFormatVersion(1, 0)),
        isFalse,
      );
      expect(
        const DataFormatVersion(2, 0).nextMinor,
        const DataFormatVersion(2, 1),
      );
      expect(const DataFormatVersion(2, 1).toString(), '2.1');
    });

    // Verifies: EVS-DEV-version-compatibility/C
    test('a data-format version is never equal to an entry-type version', () {
      // The two types are distinct so that one cannot stand in for the other.
      expect(
        const DataFormatVersion(1, 0) ==
            (const EntryTypeVersion(1, 0) as Object),
        isFalse,
      );
    });

    // Verifies: EVS-DEV-version-compatibility/C
    test('JSON round-trips and refuses malformed values, naming the key', () {
      const v = DataFormatVersion(2, 3);
      expect(DataFormatVersion.fromJson(v.toJson()), v);
      final cases = <String, (Object?, String)>{
        'missing major': (<String, Object?>{'minor': 0}, 'major'),
        'string minor': (<String, Object?>{'major': 2, 'minor': 'x'}, 'minor'),
        'negative minor': (<String, Object?>{'major': 2, 'minor': -3}, 'minor'),
        'zero major': (<String, Object?>{'major': 0, 'minor': 1}, 'major'),
        'integer': (2, 'major'),
        'an extra key': (
          <String, Object?>{'major': 2, 'minor': 0, 'patch': 1},
          'patch',
        ),
      };
      for (final entry in cases.entries) {
        final (value, key) = entry.value;
        expect(
          () => DataFormatVersion.fromJson(value),
          throwsA(
            isA<FormatException>().having(
              (e) => e.message,
              'message',
              contains(key),
            ),
          ),
          reason: entry.key,
        );
      }
    });
  });
}
