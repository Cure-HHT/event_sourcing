import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

void main() {
  // Verifies: EVS-PRD-auth-session/C+F
  test('accepts non-empty credential as Principal userId', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'install');
    final p = await v.authenticate('user-123');
    expect(p, isA<UserPrincipal>());
    final up = p as UserPrincipal;
    expect(up.userId, 'user-123');
    expect(up.activeRole, 'install');
    expect(up.roles, {'install'});
  });

  test('uses configured defaultActiveRole', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'StudyCoordinator');
    final p = await v.authenticate('user-x') as UserPrincipal;
    expect(p.activeRole, 'StudyCoordinator');
    expect(p.roles, {'StudyCoordinator'});
  });

  // Verifies: EVS-PRD-auth-session/C
  test('rejects empty credential with AuthenticationDenied', () async {
    final v = TrustingAuthValidator(defaultActiveRole: 'install');
    await expectLater(
      () => v.authenticate(''),
      throwsA(isA<AuthenticationDenied>()),
    );
  });
}
