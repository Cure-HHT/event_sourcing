// test/permissions/permission_granted_payload_test.dart
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
        () => PermissionGrantedPayload.fromJson(const {'permissionName': 'x'}),
        throwsA(anyOf(isA<TypeError>(), isA<FormatException>())),
      );
    });
  });
}
