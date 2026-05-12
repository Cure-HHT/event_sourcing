// test/permissions/yaml_seed_loader_test.dart
import 'package:event_sourcing/src/permissions/yaml_seed_loader.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('YamlSeedLoader', () {
    test('parses well-formed seed', () {
      const yaml = '''
roles:
  - patient
  - investigator
  - admin

grants:
  patient:
    - patient.diary.submit
    - patient.consent.sign
  investigator:
    - patient.read
  admin: []
''';
      final seed = YamlSeedLoader().loadFromString(yaml);
      expect(seed.roles, <String>{'patient', 'investigator', 'admin'});
      expect(seed.grants['patient'], <String>{
        'patient.diary.submit',
        'patient.consent.sign',
      });
      expect(seed.grants['investigator'], <String>{'patient.read'});
      expect(seed.grants['admin'], isEmpty);
    });

    test('throws on missing roles key', () {
      const yaml = 'grants: {}';
      expect(
        () => YamlSeedLoader().loadFromString(yaml),
        throwsA(isA<FormatException>()),
      );
    });

    test('throws on missing grants key', () {
      const yaml = 'roles: [admin]';
      expect(
        () => YamlSeedLoader().loadFromString(yaml),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
