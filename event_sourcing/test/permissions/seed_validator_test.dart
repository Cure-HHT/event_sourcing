// test/permissions/seed_validator_test.dart
// Verifies: EVS-PRD-permissions-as-events/A — SeedValidator rejects seeds
// that reference undeclared permissions, roles absent from the grants map,
// or names containing reserved characters, ensuring that only well-formed
// grants are admitted to the event log.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('SeedValidator', () {
    final declared = <Permission>{
      const Permission('user.invite', scope: ScopeClass.global),
      const Permission('patient.read', scope: ScopeClass.global),
    };

    test('SeedValid for clean seed', () {
      const seed = PermissionSeed(
        roles: <String>{'admin', 'investigator'},
        grants: <String, Set<String>>{
          'admin': <String>{'user.invite'},
          'investigator': <String>{'patient.read'},
        },
      );
      expect(SeedValidator().validate(seed, declared), isA<SeedValid>());
    });

    test('rejects unknown permission name (typo)', () {
      const seed = PermissionSeed(
        roles: <String>{'admin'},
        grants: <String, Set<String>>{
          'admin': <String>{'user.inivte'}, // typo
        },
      );
      final result = SeedValidator().validate(seed, declared);
      expect(result, isA<SeedInvalid>());
      expect((result as SeedInvalid).errors.first, contains('user.inivte'));
    });

    test('rejects grant key absent from roles list', () {
      const seed = PermissionSeed(
        roles: <String>{'admin'},
        grants: <String, Set<String>>{
          'admin': <String>{'user.invite'},
          'patient': <String>{'patient.read'}, // patient not in roles
        },
      );
      expect(SeedValidator().validate(seed, declared), isA<SeedInvalid>());
    });

    test('rejects role missing from grants', () {
      const seed = PermissionSeed(
        roles: <String>{'admin', 'patient'},
        grants: <String, Set<String>>{
          'admin': <String>{'user.invite'},
          // patient missing from grants
        },
      );
      expect(SeedValidator().validate(seed, declared), isA<SeedInvalid>());
    });

    test('rejects role name containing colon', () {
      const seed = PermissionSeed(
        roles: <String>{'a:b'},
        grants: <String, Set<String>>{'a:b': <String>{}},
      );
      expect(SeedValidator().validate(seed, declared), isA<SeedInvalid>());
    });

    test('rejects permission name containing colon', () {
      final declared2 = <Permission>{
        const Permission('user:invite', scope: ScopeClass.global),
      };
      const seed = PermissionSeed(
        roles: <String>{'admin'},
        grants: <String, Set<String>>{
          'admin': <String>{'user:invite'},
        },
      );
      expect(SeedValidator().validate(seed, declared2), isA<SeedInvalid>());
    });
  });
}
