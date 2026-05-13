// lib/src/permissions/permission_revoked_payload.dart
// Implements: EVS-PRD-permissions-as-events/A — payload for the
// permission_revoked event type, which records permission revocations as
// immutable log entries alongside all other application state changes.

import 'package:meta/meta.dart';

@immutable
class PermissionRevokedPayload {
  const PermissionRevokedPayload({
    required this.role,
    required this.permissionName,
  });

  factory PermissionRevokedPayload.fromJson(Map<String, Object?> json) {
    return PermissionRevokedPayload(
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
      other is PermissionRevokedPayload &&
          role == other.role &&
          permissionName == other.permissionName;

  @override
  int get hashCode => Object.hash(role, permissionName);
}
