// lib/src/permissions/permission_granted_payload.dart
// Implements: EVS-PRD-permissions-as-events/A
// payload for the
// permission_granted event type, which records the grant as an immutable
// log entry. Scope class lives on the registered Permission definition,
// not in the per-grant payload.

import 'package:meta/meta.dart';

@immutable
class PermissionGrantedPayload {
  const PermissionGrantedPayload({
    required this.role,
    required this.permissionName,
  });

  factory PermissionGrantedPayload.fromJson(Map<String, Object?> json) {
    return PermissionGrantedPayload(
      role: json['role']! as String,
      permissionName: json['permissionName']! as String,
    );
  }

  final String role;
  final String permissionName;

  Map<String, Object?> toJson() => <String, Object?>{
    'role': role,
    'permissionName': permissionName,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is PermissionGrantedPayload &&
          role == other.role &&
          permissionName == other.permissionName;

  @override
  int get hashCode => Object.hash(role, permissionName);
}
