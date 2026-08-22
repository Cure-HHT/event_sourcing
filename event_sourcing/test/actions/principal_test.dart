// Verifies: EVS-PRD-action-dispatch/A
// (Principal sealed type: UserPrincipal and AnonymousPrincipal carry caller identity through every stage)
// Verifies: EVS-PRD-action-dispatch/C
// (toInitiator() stamps the Principal onto emitted events for the audit trail)
// Verifies: EVS-PRD-library-charter/H
// (Principal is accepted on faith from the caller — the acknowledged-unaudited trust input)

import 'package:event_sourcing/event_sourcing.dart'
    show UserInitiator, AnonymousInitiator;
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:test/test.dart';

void main() {
  group('Principal', () {
    group('UserPrincipal', () {
      test('id returns userId', () {
        final p = Principal.user(
          userId: 'u-1',
          roles: const {'Investigator'},
          activeRole: 'Investigator',
        );
        expect(p, isA<UserPrincipal>());
        expect(p.id, 'u-1');
      });

      test('toInitiator returns UserInitiator carrying userId', () {
        final p = Principal.user(
          userId: 'u-7',
          roles: const {'X'},
          activeRole: 'X',
        );
        final init = p.toInitiator();
        expect(init, isA<UserInitiator>());
        expect((init as UserInitiator).userId, 'u-7');
      });

      test('multi-role users pick their activeRole', () {
        final p = Principal.user(
          userId: 'u-1',
          roles: const {'Investigator', 'Analyst'},
          activeRole: 'Analyst',
        );
        expect((p as UserPrincipal).roles, hasLength(2));
        expect(p.activeRole, 'Analyst');
      });

      test('asserts non-empty userId', () {
        // Cannot use const here: Dart evaluates const asserts at compile time
        // and throws a compile-time error rather than a runtime AssertionError.
        // We use a non-const local variable to defer evaluation to runtime.
        final emptyId = ''; // ignore: prefer_const_declarations
        expect(
          () => Principal.user(
            userId: emptyId,
            roles: const {'X'},
            activeRole: 'X',
          ),
          throwsA(isA<AssertionError>()),
        );
      });

      test('asserts non-empty activeRole', () {
        // Non-const local defers assert eval to runtime — see prior test.
        final emptyRole = ''; // ignore: prefer_const_declarations
        expect(
          () => Principal.user(
            userId: 'u-1',
            roles: const {'X'},
            activeRole: emptyRole,
          ),
          throwsA(isA<AssertionError>()),
        );
      });

      test('asserts activeRole is in roles', () {
        // Non-const local defers assert eval to runtime — see prior test.
        final activeRole = 'Guest'; // ignore: prefer_const_declarations
        expect(
          () => Principal.user(
            userId: 'u-1',
            roles: const {'Admin'},
            activeRole: activeRole,
          ),
          throwsA(isA<AssertionError>()),
        );
      });
    });

    group('AnonymousPrincipal', () {
      test('id returns "anon:<ip>" when ip provided', () {
        const p = Principal.anonymous(ipAddress: '1.2.3.4');
        expect(p.id, 'anon:1.2.3.4');
      });

      test('id returns "anon:unknown" when ip absent', () {
        const p = Principal.anonymous();
        expect(p.id, 'anon:unknown');
      });

      test('toInitiator returns AnonymousInitiator', () {
        const p = Principal.anonymous(ipAddress: '5.6.7.8');
        final init = p.toInitiator();
        expect(init, isA<AnonymousInitiator>());
      });
    });
  });
}
