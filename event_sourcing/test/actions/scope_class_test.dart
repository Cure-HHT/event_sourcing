// Verifies: EVS-PRD-action-dispatch/B (ScopeClass has exactly three values used in the authorize stage's session-context precondition check)
// Verifies: EVS-PRD-permissions-as-events/B (stable enum names are the wire format for scope in permission-grant events)

import 'package:event_sourcing/src/actions/scope_class.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeClass', () {
    test('closed set of three values', () {
      expect(ScopeClass.values, hasLength(3));
      expect(ScopeClass.values.toSet(), {
        ScopeClass.global,
        ScopeClass.site,
        ScopeClass.self,
      });
    });

    test('enum names are stable wire format', () {
      expect(ScopeClass.global.name, 'global');
      expect(ScopeClass.site.name, 'site');
      expect(ScopeClass.self.name, 'self');
    });
  });
}
