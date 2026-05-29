// Verifies: EVS-PRD-event-log/A
// Verifies: EVS-DEV-append-stamps-registered-version/A/B/C
import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:flutter_test/flutter_test.dart';

Map<String, Object?> _validJson() => <String, Object?>{
  'id': 'epistaxis_event',
  'registered_version': 1,
  'name': 'Nosebleed',
};

void main() {
  group('EntryTypeDefinition', () {
    test('constructs with all required fields; getters round-trip', () {
      const def = EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: 1,
        name: 'Nosebleed',
      );

      expect(def.id, 'epistaxis_event');
      expect(def.registeredVersion, 1);
      expect(def.name, 'Nosebleed');
      expect(def.isMaterialized, isTrue);
    });

    test('toJson emits snake_case keys for every field', () {
      const def = EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: 1,
        name: 'Nosebleed',
      );

      expect(def.toJson(), {
        'id': 'epistaxis_event',
        'registered_version': 1,
        'name': 'Nosebleed',
        'materialize': true,
      });
    });

    // Verifies: round-trip preserves all fields.
    test('toJson/fromJson round-trip preserves all fields', () {
      const def = EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: 2,
        name: 'Nosebleed',
        isMaterialized: false,
      );

      final roundTripped = EntryTypeDefinition.fromJson(def.toJson());
      expect(roundTripped, equals(def));
    });

    group('fromJson validation', () {
      test('missing id throws FormatException', () {
        final bad = _validJson()..remove('id');
        expect(() => EntryTypeDefinition.fromJson(bad), throwsFormatException);
      });

      test('missing registered_version throws FormatException', () {
        final bad = _validJson()..remove('registered_version');
        expect(() => EntryTypeDefinition.fromJson(bad), throwsFormatException);
      });

      test('missing name throws FormatException', () {
        final bad = _validJson()..remove('name');
        expect(() => EntryTypeDefinition.fromJson(bad), throwsFormatException);
      });

      // Verifies: absent optional field defaults to true.
      test('absent "materialize" in JSON defaults to true', () {
        final def = EntryTypeDefinition.fromJson(_validJson());
        expect(def.isMaterialized, isTrue);
      });
    });

    group('value equality', () {
      // Verifies: equal fields produce equal entries.
      test('equal fields produce equal entries with equal hashCodes', () {
        const a = EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: 1,
          name: 'Nosebleed',
        );
        const b = EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: 1,
          name: 'Nosebleed',
        );

        expect(a, equals(b));
        expect(a.hashCode, b.hashCode);
      });

      test('any field difference breaks equality', () {
        const base = EntryTypeDefinition(
          id: 'epistaxis_event',
          registeredVersion: 1,
          name: 'Nosebleed',
        );

        expect(
          base,
          isNot(
            equals(
              const EntryTypeDefinition(
                id: 'nose_hht_survey',
                registeredVersion: 1,
                name: 'Nosebleed',
              ),
            ),
          ),
        );
        expect(
          base,
          isNot(
            equals(
              const EntryTypeDefinition(
                id: 'epistaxis_event',
                registeredVersion: 2,
                name: 'Nosebleed',
              ),
            ),
          ),
        );
        expect(
          base,
          isNot(
            equals(
              const EntryTypeDefinition(
                id: 'epistaxis_event',
                registeredVersion: 1,
                name: 'Different Name',
              ),
            ),
          ),
        );
      });
    });
  });

  group('materialize flag', () {
    test('defaults to true', () {
      const def = EntryTypeDefinition(id: 'x', registeredVersion: 1, name: 'X');
      expect(def.isMaterialized, isTrue);
    });

    test('false round-trips through JSON', () {
      const def = EntryTypeDefinition(
        id: 'x',
        registeredVersion: 1,
        name: 'X',
        isMaterialized: false,
      );
      expect(def.isMaterialized, isFalse);
      final map = def.toJson();
      expect(map['materialize'], isFalse);
      final roundTripped = EntryTypeDefinition.fromJson(map);
      expect(roundTripped.isMaterialized, isFalse);
    });

    test('absent "materialize" in JSON defaults to true', () {
      final def = EntryTypeDefinition.fromJson(<String, Object?>{
        'id': 'x',
        'registered_version': 1,
        'name': 'X',
      });
      expect(def.isMaterialized, isTrue);
    });

    test('non-bool "materialize" is rejected', () {
      expect(
        () => EntryTypeDefinition.fromJson(<String, Object?>{
          'id': 'x',
          'registered_version': 1,
          'name': 'X',
          'materialize': 'yes',
        }),
        throwsFormatException,
      );
    });

    test('materialize participates in equality', () {
      const a = EntryTypeDefinition(id: 'x', registeredVersion: 1, name: 'X');
      const b = EntryTypeDefinition(
        id: 'x',
        registeredVersion: 1,
        name: 'X',
        isMaterialized: false,
      );
      expect(a, isNot(b));
    });
  });
}
