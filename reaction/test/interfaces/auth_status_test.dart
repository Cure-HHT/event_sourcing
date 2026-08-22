// Verifies: EVS-PRD-auth-session/B
// AuthStatus is a sealed type
// with exactly three variants (Authenticated, NotAuthenticated,
// Expired).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/auth_session.dart';

void main() {
  group('AuthStatus', () {
    test('Authenticated carries a Principal', () {
      final p = Principal.user(
        userId: 'user-1',
        roles: const {'viewer'},
        activeRole: 'viewer',
      );
      final status = Authenticated(principal: p);
      expect(status.principal, equals(p));
      expect(status, isA<AuthStatus>());
    });

    test('NotAuthenticated is a const value', () {
      const a = NotAuthenticated();
      const b = NotAuthenticated();
      expect(identical(a, b), isTrue);
      expect(a, isA<AuthStatus>());
    });

    test('Expired is a const value', () {
      const a = Expired();
      const b = Expired();
      expect(identical(a, b), isTrue);
      expect(a, isA<AuthStatus>());
    });

    test('exhaustive switch across the three variants', () {
      AuthStatus status = const NotAuthenticated();
      final tag = switch (status) {
        Authenticated() => 'authd',
        NotAuthenticated() => 'unauth',
        Expired() => 'expired',
      };
      expect(tag, equals('unauth'));
    });
  });
}
