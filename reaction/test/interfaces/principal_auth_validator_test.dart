// Verifies: EVS-PRD-auth-session — PrincipalAuthValidator interface
// contract: authenticate() returns the Principal on success or
// throws AuthenticationDenied on rejection.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';

class _StubValidator implements PrincipalAuthValidator {
  final Map<String, Principal> _accepts;
  _StubValidator(this._accepts);

  @override
  Future<Principal> authenticate(String credential) async {
    final p = _accepts[credential];
    if (p == null) throw const AuthenticationDenied('unknown credential');
    return p;
  }
}

void main() {
  group('PrincipalAuthValidator contract', () {
    test('accepts a known credential and returns the Principal', () async {
      final validator = _StubValidator({
        'token-a': const Principal.user(
          userId: 'user-a',
          roles: {},
          activeRole: 'viewer',
        ),
      });
      final p = await validator.authenticate('token-a');
      expect(p.id, equals('user-a'));
    });

    test('throws AuthenticationDenied on unknown credential', () async {
      final validator = _StubValidator(const {});
      expect(
        () => validator.authenticate('bogus'),
        throwsA(isA<AuthenticationDenied>()),
      );
    });

    test('AuthenticationDenied carries a reason message', () {
      const e = AuthenticationDenied('jwt expired at 2025-01-01');
      expect(e.message, contains('jwt expired'));
      expect(e.toString(), contains('jwt expired'));
    });
  });
}
