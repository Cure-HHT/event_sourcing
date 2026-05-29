// Implements: EVS-PRD-cross-process-event-transport/A — JSON codec for
//   EffectiveAuthorization wire envelopes.
// Implements: EVS-PRD-permission-source/C — wire shape of the
//   GET /permissions/snapshot response that RemotePermissionSource
//   consumes.

import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for the substrate's [EffectiveAuthorization] — the
/// permissions-projection materialised by
/// `AuthorizationPolicy.effectivePermissionsFor`. Carries the active
/// role + permissions granted to that role + the user's scope
/// assignments under that role.
///
/// Wire shape:
///   {"activeRole": "...",
///    "rolePermissions": [{"name": "...", "scopeClass": "..." | null}, ...],
///    "scopeAssignments": [{"scope": <ScopeValue.toJson()>}, ...]}
///
/// `rolePermissions` is sorted by permission name on the wire for
/// deterministic output. Scope values reuse the substrate's
/// `ScopeValue.toJson` / `ScopeValue.fromJson` — the codec does not
/// duplicate that contract.
class EffectiveAuthorizationCodec {
  const EffectiveAuthorizationCodec._();

  static Map<String, Object?> encode(EffectiveAuthorization authorization) {
    final perms = authorization.rolePermissions.toList()
      ..sort((a, b) => a.name.compareTo(b.name));
    return {
      'activeRole': authorization.activeRole,
      'rolePermissions': perms.map(_encodePermission).toList(),
      'scopeAssignments': authorization.scopeAssignments
          .map(_encodeScopeAssignment)
          .toList(),
    };
  }

  static EffectiveAuthorization decode(Map<String, Object?> json) {
    return EffectiveAuthorization(
      activeRole: requireString(json, 'activeRole'),
      rolePermissions: (json['rolePermissions']! as List)
          .cast<Map<String, Object?>>()
          .map(_decodePermission)
          .toSet(),
      scopeAssignments: (json['scopeAssignments']! as List)
          .cast<Map<String, Object?>>()
          .map(_decodeScopeAssignment)
          .toList(),
    );
  }

  static Map<String, Object?> _encodePermission(Permission p) => {
    'name': p.name,
    'scopeClass': p.scopeClass,
  };

  static Permission _decodePermission(Map<String, Object?> j) => Permission(
    requireString(j, 'name'),
    scopeClass: readString(j, 'scopeClass'),
  );

  static Map<String, Object?> _encodeScopeAssignment(ScopeAssignment a) => {
    'scope': a.scope.toJson(),
  };

  static ScopeAssignment _decodeScopeAssignment(Map<String, Object?> j) =>
      ScopeAssignment(scope: ScopeValue.fromJson(requireMap(j, 'scope')));
}
