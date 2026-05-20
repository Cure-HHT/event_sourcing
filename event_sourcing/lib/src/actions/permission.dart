// Implements: EVS-PRD-action-dispatch/B (Permission is the unit checked by the authorize stage; Action.permissions declares what is required)
// Implements: EVS-PRD-permissions-as-events/A (Permission names are the subject of permission-grant events in the same log)
// Implements: EVS-PRD-permissions-as-events/B (AuthorizationPolicy.isPermitted receives Permission; evaluates from event-derived projections)

/// A named permission, by convention `<aggregate>.<verb>` (e.g.
/// `user.invite`, `patient.enroll`). Used by `Action.permissions` to
/// declare what the action requires; used by `AuthorizationPolicy` to
/// decide whether a principal may execute it.
///
/// [scopeClass] (optional) is the name of a registered `ScopeClassSpec`
/// when the permission is scoped (e.g., `patient.edit` is `scopeClass:
/// 'patient'`). Null means unscoped: the permission applies globally and
/// no `ScopeValue` is required at dispatch.
class Permission {
  const Permission(this.name, {this.scopeClass})
    : assert(name != '', 'name must not be empty');

  /// Throws `ArgumentError` if `name` is empty or whitespace-only.
  factory Permission.checked(String name, {String? scopeClass}) {
    if (name.trim().isEmpty) {
      throw ArgumentError.value(
        name,
        'name',
        'must not be empty or whitespace',
      );
    }
    return Permission(name, scopeClass: scopeClass);
  }

  final String name;

  /// The registered ScopeClassSpec name this permission is scoped to,
  /// or null if the permission is unscoped.
  final String? scopeClass;

  @override
  bool operator ==(Object other) => other is Permission && other.name == name;

  @override
  int get hashCode => name.hashCode;

  @override
  String toString() => scopeClass == null
      ? 'Permission($name)'
      : 'Permission($name, scoped: $scopeClass)';
}
