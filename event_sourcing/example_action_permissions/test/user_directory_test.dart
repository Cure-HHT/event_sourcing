// test/user_directory_test.dart
// Verifies: EVS-PRD-permissions-as-events/B
import 'package:action_permissions_demo/server/user_directory.dart';
import 'package:event_sourcing/event_sourcing.dart'
    show AnonymousPrincipal, UserPrincipal;
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('UserDirectory', () {
    test('resolve returns AnonymousPrincipal when userId is null', () {
      final dir = UserDirectory();
      expect(dir.resolve(null), isA<AnonymousPrincipal>());
    });

    test('resolve returns AnonymousPrincipal when userId is unknown', () {
      final dir = UserDirectory();
      expect(dir.resolve('not-a-user'), isA<AnonymousPrincipal>());
    });

    test('resolve returns UserPrincipal for known userId with role+site', () {
      final dir = UserDirectory();
      dir.upsert(
        userId: 'green-user-1',
        role: 'GreenTeam',
        activeSite: 'green-workspace',
      );
      final p = dir.resolve('green-user-1');
      expect(p, isA<UserPrincipal>());
      final user = p as UserPrincipal;
      expect(user.userId, 'green-user-1');
      expect(user.activeRole, 'GreenTeam');
      expect(user.roles, <String>{'GreenTeam'});
      // UserPrincipal carries identity/role only; the demo exposes
      // activeSite via the directory record.
      expect(dir.siteFor('green-user-1'), 'green-workspace');
    });

    test('siteFor returns null when upserted without an activeSite', () {
      final dir = UserDirectory();
      dir.upsert(userId: 'admin-user', role: 'Admin', activeSite: null);
      final p = dir.resolve('admin-user') as UserPrincipal;
      expect(p.userId, 'admin-user');
      expect(dir.siteFor('admin-user'), isNull);
    });

    test('listEntries returns alphabetically sorted snapshot', () {
      final dir = UserDirectory();
      dir.upsert(
        userId: 'b-user',
        role: 'BlueTeam',
        activeSite: 'blue-workspace',
      );
      dir.upsert(userId: 'a-user', role: 'Admin', activeSite: null);
      expect(dir.listEntries().map((e) => e.userId).toList(), <String>[
        'a-user',
        'b-user',
      ]);
    });

    test('contains is true after upsert, false otherwise', () {
      final dir = UserDirectory();
      expect(dir.contains('x'), isFalse);
      dir.upsert(userId: 'x', role: 'Admin', activeSite: null);
      expect(dir.contains('x'), isTrue);
    });
  });
}
