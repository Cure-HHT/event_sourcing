import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeValue', () {
    // Verifies: EVS-DEV-scope-value-json/A+D+E
    test('BoundScope round-trips through JSON', () {
      const v = BoundScope(class_: 'site', value: 'A');
      expect(v.toJson(), {'class': 'site', 'value': 'A'});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    // Verifies: EVS-DEV-scope-value-json/B+D+E
    test('ValueWildcardScope round-trips through JSON', () {
      const v = ValueWildcardScope(class_: 'site');
      expect(v.toJson(), {'class': 'site', 'wildcard_value': true});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    // Verifies: EVS-DEV-scope-value-json/C+D+E
    test('TotalWildcardScope round-trips through JSON', () {
      const v = TotalWildcardScope();
      expect(v.toJson(), {'wildcard_class': true});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    test('BoundScope and ValueWildcardScope with same class are unequal', () {
      expect(
        const BoundScope(class_: 'site', value: 'A'),
        isNot(equals(const ValueWildcardScope(class_: 'site'))),
      );
    });

    // Verifies: EVS-DEV-scope-value-json/D
    test(
      'fromJson rejects ambiguous objects (both value and wildcard_value)',
      () {
        expect(
          () => ScopeValue.fromJson({
            'class': 'site',
            'value': 'A',
            'wildcard_value': true,
          }),
          throwsA(isA<FormatException>()),
        );
      },
    );

    // Verifies: EVS-DEV-scope-value-json/D
    test('fromJson rejects total_wildcard combined with class', () {
      expect(
        () => ScopeValue.fromJson({'wildcard_class': true, 'class': 'site'}),
        throwsA(isA<FormatException>()),
      );
    });

    // Verifies: EVS-DEV-scope-value-json/D
    test('fromJson rejects empty object', () {
      expect(
        () => ScopeValue.fromJson(<String, Object?>{}),
        throwsA(isA<FormatException>()),
      );
    });

    // Verifies: EVS-DEV-scope-value-json/D
    test('fromJson rejects bound shape with empty value', () {
      expect(
        () => ScopeValue.fromJson({'class': 'site', 'value': ''}),
        throwsA(isA<FormatException>()),
      );
    });

    // Verifies: EVS-DEV-scope-value-json/D
    test('fromJson rejects wildcard_class with non-true value', () {
      expect(
        () => ScopeValue.fromJson({'wildcard_class': false}),
        throwsA(isA<FormatException>()),
      );
      expect(
        () => ScopeValue.fromJson({'wildcard_class': 'yes'}),
        throwsA(isA<FormatException>()),
      );
    });

    // Verifies: EVS-DEV-scope-value-json/D
    test('fromJson rejects wildcard_value with non-true value', () {
      expect(
        () => ScopeValue.fromJson({'class': 'site', 'wildcard_value': false}),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
