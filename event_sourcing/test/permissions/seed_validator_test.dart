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
      const Permission('user.invite'),
      const Permission('patient.read'),
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
      final declared2 = <Permission>{const Permission('user:invite')};
      const seed = PermissionSeed(
        roles: <String>{'admin'},
        grants: <String, Set<String>>{
          'admin': <String>{'user:invite'},
        },
      );
      expect(SeedValidator().validate(seed, declared2), isA<SeedInvalid>());
    });

    test('rejects seed referencing a permission whose scopeClass is not '
        'registered in ScopeClassRegistry', () {
      final declared = <Permission>{
        const Permission('patient.edit', scopeClass: 'patient'),
      };
      final registry = ScopeClassRegistry(
        classes: const [ScopeClassSpec(name: 'site')], // 'patient' absent
        projectionLookup: (_) => null,
      );
      const seed = PermissionSeed(
        roles: <String>{'SC'},
        grants: <String, Set<String>>{
          'SC': <String>{'patient.edit'},
        },
      );
      final result = SeedValidator().validate(
        seed,
        declared,
        scopeClassRegistry: registry,
      );
      expect(result, isA<SeedInvalid>());
      expect((result as SeedInvalid).errors.single, contains('patient'));
    });

    test('accepts seed where all referenced scopeClasses are registered', () {
      final declared = <Permission>{
        const Permission('patient.edit', scopeClass: 'patient'),
      };
      final registry = ScopeClassRegistry(
        classes: const [
          ScopeClassSpec(name: 'site'),
          ScopeClassSpec(
            name: 'patient',
            containedIn: ContainmentReference(
              parentClass: 'site',
              projection: 'patient_site',
              keyColumn: 'patient_id',
              parentColumn: 'site_id',
            ),
          ),
        ],
        projectionLookup: (_) => const _MinimalDescriptor(),
      );
      const seed = PermissionSeed(
        roles: <String>{'SC'},
        grants: <String, Set<String>>{
          'SC': <String>{'patient.edit'},
        },
      );
      final result = SeedValidator().validate(
        seed,
        declared,
        scopeClassRegistry: registry,
      );
      expect(result, isA<SeedValid>());
    });

    test('unscoped permissions are not checked against the registry', () {
      // When permission has scopeClass == null, the registry is irrelevant.
      final declared = <Permission>{const Permission('report.generate')};
      final registry = ScopeClassRegistry(
        classes: const [],
        projectionLookup: (_) => null,
      );
      const seed = PermissionSeed(
        roles: <String>{'SC'},
        grants: <String, Set<String>>{
          'SC': <String>{'report.generate'},
        },
      );
      final result = SeedValidator().validate(
        seed,
        declared,
        scopeClassRegistry: registry,
      );
      expect(result, isA<SeedValid>());
    });
  });
}

/// Minimal projection descriptor whose columns cover the test
/// `ContainmentReference` (so `ScopeClassRegistry` accepts the registration).
class _MinimalDescriptor implements ScopeProjectionDescriptor {
  const _MinimalDescriptor();
  @override
  Set<String> get columns => const {'patient_id', 'site_id'};
}
