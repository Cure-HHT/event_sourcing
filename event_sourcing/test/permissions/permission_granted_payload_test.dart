// test/permissions/permission_granted_payload_test.dart
// Verifies: EVS-PRD-permissions-as-events/A
// the permission_granted event
// payload round-trips faithfully through JSON, confirming that grant events
// can be durably recorded in and replayed from the event log.
import 'package:event_sourcing/src/permissions/permission_granted_payload.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('PermissionGrantedPayload', () {
    test('toJson/fromJson round-trips', () {
      const p = PermissionGrantedPayload(
        role: 'SC',
        permissionName: 'patient.edit',
      );
      final j = p.toJson();
      expect(j, {'role': 'SC', 'permissionName': 'patient.edit'});
      expect(PermissionGrantedPayload.fromJson(j), equals(p));
    });

    test('fromJson throws on missing role', () {
      expect(
        () => PermissionGrantedPayload.fromJson({'permissionName': 'x'}),
        throwsA(anyOf(isA<TypeError>(), isA<FormatException>())),
      );
    });
  });
}
