// Verifies: EVS-PRD-auth-session/A/B/G
// LocalAuthSession honors
// the AuthSession interface (A: current/stream/setCredential/
// principal), the AuthStatus sealed-type variants exposed via state
// transitions (B), and the rule that the active Principal flows
// through to consumers via `session.principal` (G).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reaction/src/interfaces/auth_session.dart';
import 'package:reaction/src/local/local_auth_session.dart';

void main() {
  group('LocalAuthSession', () {
    late LocalAuthSession session;

    setUp(() {
      session = LocalAuthSession();
    });

    tearDown(() async {
      await session.dispose();
    });

    test('starts NotAuthenticated', () {
      expect(session.current, isA<NotAuthenticated>());
      expect(session.principal, isNull);
    });

    test(
      'setCredential(non-null) transitions to Authenticated with the install UUID as userId',
      () {
        session.setCredential('install-uuid-123');
        expect(session.current, isA<Authenticated>());
        final p = session.principal;
        expect(p, isNotNull);
        expect(p!.id, equals('install-uuid-123'));
      },
    );

    test('setCredential(null) transitions back to NotAuthenticated', () {
      session
        ..setCredential('install-uuid-123')
        ..setCredential(null);
      expect(session.current, isA<NotAuthenticated>());
      expect(session.principal, isNull);
    });

    test('stream emits on every status change', () async {
      final events = <AuthStatus>[];
      final sub = session.stream.listen(events.add);

      session
        ..setCredential('a')
        ..setCredential('b')
        ..setCredential(null);

      // Allow microtasks to drain.
      await Future<void>.delayed(Duration.zero);
      await Future<void>.delayed(Duration.zero);

      expect(events.length, equals(3));
      expect(events[0], isA<Authenticated>());
      expect((events[0] as Authenticated).principal.id, equals('a'));
      expect(events[1], isA<Authenticated>());
      expect((events[1] as Authenticated).principal.id, equals('b'));
      expect(events[2], isA<NotAuthenticated>());

      await sub.cancel();
    });

    test('defaultActiveRole can be overridden', () {
      final adminSession = LocalAuthSession(defaultActiveRole: 'admin')
        ..setCredential('alice');
      final principal = adminSession.principal as UserPrincipal;
      expect(principal.activeRole, equals('admin'));
    });
  });
}
