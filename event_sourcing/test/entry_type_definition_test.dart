import 'package:event_sourcing/src/entry_type_definition.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('EntryTypeDefinition', () {
    test('constructs with all required fields; getters round-trip', () {
      const def = EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Nosebleed',
      );

      expect(def.id, 'epistaxis_event');
      expect(def.registeredVersion, const EntryTypeVersion(1, 0));
      expect(def.name, 'Nosebleed');
    });
  });
}
