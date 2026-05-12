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
