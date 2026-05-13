// Verifies: EVS-PRD-permissions-as-events (scope-value shape pinned by spec/scoped-permissions.md)

import 'package:event_sourcing/event_sourcing.dart';
import 'package:test/test.dart';

void main() {
  group('ScopeValue', () {
    test('BoundScope round-trips through JSON', () {
      const v = BoundScope(class_: 'site', value: 'A');
      expect(v.toJson(), {'class': 'site', 'value': 'A'});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

    test('ValueWildcardScope round-trips through JSON', () {
      const v = ValueWildcardScope(class_: 'site');
      expect(v.toJson(), {'class': 'site', 'wildcard_value': true});
      expect(ScopeValue.fromJson(v.toJson()), equals(v));
    });

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

    test('fromJson rejects total_wildcard combined with class', () {
      expect(
        () => ScopeValue.fromJson({'wildcard_class': true, 'class': 'site'}),
        throwsA(isA<FormatException>()),
      );
    });

    test('fromJson rejects empty object', () {
      expect(
        () => ScopeValue.fromJson(<String, Object?>{}),
        throwsA(isA<FormatException>()),
      );
    });

    test('fromJson rejects bound shape with empty value', () {
      expect(
        () => ScopeValue.fromJson({'class': 'site', 'value': ''}),
        throwsA(isA<FormatException>()),
      );
    });

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

    test('fromJson rejects wildcard_value with non-true value', () {
      expect(
        () => ScopeValue.fromJson({'class': 'site', 'wildcard_value': false}),
        throwsA(isA<FormatException>()),
      );
    });
  });
}
