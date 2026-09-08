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
    });
  });
}
