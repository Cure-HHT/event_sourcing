// lib/src/permissions/permission_snapshot.dart
// Implements: EVS-PRD-permissions-as-events/C — a serializable snapshot of
// the permission state for a single role, derived from the event-log
// projection; enables reconstructability without a live StorageBackend.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
class PermissionSnapshot {
  const PermissionSnapshot({
    required this.role,
    required this.grants,
    required this.issuedAt,
  });

  factory PermissionSnapshot.fromJson(Map<String, Object?> json) {
    final grantsList = json['grants']! as List<Object?>;
    final grants = grantsList.map((g) {
      final m = g! as Map<Object?, Object?>;
      return Permission(m['name']! as String);
    }).toSet();
    return PermissionSnapshot(
      role: json['role']! as String,
      grants: grants,
      issuedAt: DateTime.parse(json['issuedAt']! as String),
    );
  }

  final String role;
  final Set<Permission> grants;
  final DateTime issuedAt;

  Map<String, Object?> toJson() => <String, Object?>{
    'role': role,
    'grants': grants.map((p) => <String, Object?>{'name': p.name}).toList(),
    'issuedAt': issuedAt.toIso8601String(),
  };
}
