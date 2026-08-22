// test/permissions/permission_revoked_payload_test.dart
// Verifies: EVS-PRD-permissions-as-events/A
// the permission_revoked event
// payload round-trips faithfully through JSON, confirming that revocation
// events can be durably recorded in and replayed from the event log.
import 'package:event_sourcing/src/permissions/permission_revoked_payload.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('PermissionRevokedPayload', () {
    test('round-trips through JSON', () {
      const payload = PermissionRevokedPayload(
        role: 'admin',
        permissionName: 'user.invite',
      );
      final parsed = PermissionRevokedPayload.fromJson(payload.toJson());
      expect(parsed.role, 'admin');
      expect(parsed.permissionName, 'user.invite');
    });

    test('equality on all fields', () {
      const a = PermissionRevokedPayload(role: 'r', permissionName: 'p');
      const b = PermissionRevokedPayload(role: 'r', permissionName: 'p');
      const c = PermissionRevokedPayload(role: 'r', permissionName: 'q');
      expect(a, equals(b));
      expect(a, isNot(equals(c)));
    });
  });
}
