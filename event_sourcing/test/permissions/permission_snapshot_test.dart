// test/permissions/permission_snapshot_test.dart
// Verifies: EVS-PRD-permissions-as-events/C — PermissionSnapshot round-trips
// faithfully through JSON, confirming that log-derived permission state can
// be serialized and restored without loss.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('PermissionSnapshot', () {
    test('round-trips through JSON', () {
      final snap = PermissionSnapshot(
        role: 'admin',
        grants: <Permission>{
          const Permission('user.invite'),
          const Permission('site.manage'),
        },
        issuedAt: DateTime.utc(2026, 5, 6),
      );
      final json = snap.toJson();
      final parsed = PermissionSnapshot.fromJson(json);
      expect(parsed.role, 'admin');
      expect(parsed.grants.length, 2);
      expect(parsed.grants.any((p) => p.name == 'user.invite'), isTrue);
      expect(parsed.grants.any((p) => p.name == 'site.manage'), isTrue);
      expect(parsed.issuedAt, DateTime.utc(2026, 5, 6));
    });
  });
}
