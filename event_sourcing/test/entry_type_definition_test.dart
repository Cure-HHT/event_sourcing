import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:flutter_test/flutter_test.dart';

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
                id: 'nose_symptom_survey',
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
    // An entry type that does not declare otherwise is materialized;
    // reserved audit types opt out explicitly with isMaterialized: false.
    test('defaults to true', () {
      const def = EntryTypeDefinition(id: 'x', registeredVersion: 1, name: 'X');
      expect(def.isMaterialized, isTrue);
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
