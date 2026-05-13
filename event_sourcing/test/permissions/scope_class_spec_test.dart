// Verifies: EVS-PRD-permissions-as-events (scope-class registration shape)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClassSpec', () {
    test('top-level class has no containment', () {
      const spec = ScopeClassSpec(name: 'site');
      expect(spec.name, 'site');
      expect(spec.containedIn, isNull);
    });

    test('contained class carries a ContainmentRef', () {
      const spec = ScopeClassSpec(
        name: 'patient',
        containedIn: ContainmentRef(
          parentClass: 'site',
          projection: 'patient_site_index',
          keyColumn: 'patient_id',
          parentColumn: 'site_id',
        ),
      );
      expect(spec.containedIn?.parentClass, 'site');
      expect(spec.containedIn?.projection, 'patient_site_index');
      expect(spec.containedIn?.keyColumn, 'patient_id');
      expect(spec.containedIn?.parentColumn, 'site_id');
    });

    test('constructor rejects empty name', () {
      expect(() => ScopeClassSpec(name: ''), throwsA(isA<AssertionError>()));
    });

    test('ContainmentRef rejects empty fields', () {
      expect(
        () => ContainmentRef(
          parentClass: '',
          projection: 'p',
          keyColumn: 'k',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
          parentClass: 'p',
          projection: '',
          keyColumn: 'k',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
          parentClass: 'p',
          projection: 'p',
          keyColumn: '',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentRef(
          parentClass: 'p',
          projection: 'p',
          keyColumn: 'k',
          parentColumn: '',
        ),
        throwsA(isA<AssertionError>()),
      );
    });

    test('equality compares all fields', () {
      const a = ScopeClassSpec(name: 'site');
      const b = ScopeClassSpec(name: 'site');
      const c = ScopeClassSpec(name: 'patient');
      expect(a, equals(b));
      expect(a, isNot(equals(c)));
    });
  });
}
