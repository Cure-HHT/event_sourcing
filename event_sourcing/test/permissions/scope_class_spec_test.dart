// Verifies: EVS-PRD-permissions-as-events
// (scope-class registration shape)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClassSpec', () {
    test('top-level class has no containment', () {
      const spec = ScopeClassSpec(name: 'site');
      expect(spec.name, 'site');
      expect(spec.containedIn, isNull);
    });

    test('contained class carries a ContainmentReference', () {
      const spec = ScopeClassSpec(
        name: 'patient',
        containedIn: ContainmentReference(
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

    test('ContainmentReference rejects empty fields', () {
      expect(
        () => ContainmentReference(
          parentClass: '',
          projection: 'p',
          keyColumn: 'k',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentReference(
          parentClass: 'p',
          projection: '',
          keyColumn: 'k',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentReference(
          parentClass: 'p',
          projection: 'p',
          keyColumn: '',
          parentColumn: 'pc',
        ),
        throwsA(isA<AssertionError>()),
      );
      expect(
        () => ContainmentReference(
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

    test('ContainmentReference equality compares all fields', () {
      const r1 = ContainmentReference(
        parentClass: 'site',
        projection: 'idx',
        keyColumn: 'k',
        parentColumn: 'p',
      );
      const r2 = ContainmentReference(
        parentClass: 'site',
        projection: 'idx',
        keyColumn: 'k',
        parentColumn: 'p',
      );
      const r3 = ContainmentReference(
        parentClass: 'other',
        projection: 'idx',
        keyColumn: 'k',
        parentColumn: 'p',
      );
      expect(r1, equals(r2));
      expect(r1, isNot(equals(r3)));
      expect(r1.hashCode, equals(r2.hashCode));
    });

    test('ScopeClassSpec equality accounts for containedIn', () {
      const ref = ContainmentReference(
        parentClass: 'site',
        projection: 'idx',
        keyColumn: 'k',
        parentColumn: 'p',
      );
      const withRef = ScopeClassSpec(name: 'patient', containedIn: ref);
      const withoutRef = ScopeClassSpec(name: 'patient');
      expect(withRef, isNot(equals(withoutRef)));
    });
  });
}
