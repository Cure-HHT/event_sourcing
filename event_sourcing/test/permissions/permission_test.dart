// Verifies: EVS-PRD-permissions-as-events (Permission carries optional scopeClass identifier; legacy ScopeClass enum removed)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('Permission', () {
    test('unscoped permission has null scopeClass', () {
      const p = Permission('users.provision');
      expect(p.scopeClass, isNull);
      expect(p.name, 'users.provision');
    });

    test('scoped permission carries scopeClass identifier', () {
      const p = Permission('patient.edit', scopeClass: 'patient');
      expect(p.scopeClass, 'patient');
    });

    test('equality is by name only', () {
      const a = Permission('p');
      const b = Permission('p', scopeClass: 'site');
      expect(a, equals(b));
    });

    test('checked() rejects empty name', () {
      expect(() => Permission.checked(''), throwsArgumentError);
      expect(() => Permission.checked('   '), throwsArgumentError);
    });
  });
}
