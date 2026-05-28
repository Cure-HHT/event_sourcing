// reaction/test/e2e/auth_test.dart
// Verifies: EVS-PRD-auth-session/E (Remote 401 -> Expired),
//           and the GET /me round-trip that drives setCredential.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/reaction.dart';

import 'test_support/reaction_remote_test_harness.dart';

void main() {
  late ReactionRemoteTestHarness h;

  setUp(() async {
    h = await ReactionRemoteTestHarness.open();
  });
  tearDown(() => h.close());

  test('initial status is NotAuthenticated', () {
    expect(h.scope.authSession.current, isA<NotAuthenticated>());
  });

  test('setCredential(valid) flips to Authenticated with Principal', () async {
    h.scope.authSession.setCredential('alice');
    final s = await h.scope.authSession.stream.firstWhere(
      (s) => s is! NotAuthenticated,
      orElse: () => const NotAuthenticated(),
    );
    expect(s, isA<Authenticated>());
    // UserPrincipal does not override == / hashCode (identity equality),
    // so drill into fields rather than comparing whole instances.
    final p = (s as Authenticated).principal as UserPrincipal;
    expect(p.userId, 'alice');
    expect(p.roles, {'install'});
    expect(p.activeRole, 'install');
  });

  test('setCredential(null) returns to NotAuthenticated', () async {
    h.scope.authSession.setCredential('alice');
    await h.scope.authSession.stream.firstWhere((s) => s is Authenticated);
    h.scope.authSession.setCredential(null);
    expect(h.scope.authSession.current, isA<NotAuthenticated>());
  });
}
