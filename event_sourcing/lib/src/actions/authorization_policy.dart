// Implements: EVS-PRD-action-dispatch/B (authorize stage pluggable interface)
// Implements: EVS-PRD-permissions-as-events/B (concrete impls evaluate decisions from event-derived projections only)
// Implements: EVS-PRD-library-charter/H (trust-boundary interface: AuthorizationPolicy is the named, registered policy surface)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/effective_authorization.dart';

/// Pluggable authorization decision-maker. Concrete impls live in the
/// permissions module within `event_sourcing`
/// (`TableBackedAuthorizationPolicy` over storage; `FailSafeAuthorizationPolicy`
/// for boot-failure).
abstract class AuthorizationPolicy {
  const AuthorizationPolicy();

  /// Decide whether [principal] may exercise [permission] against the
  /// optional [scopeValue]. The dispatcher guarantees scopeValue is
  /// non-null iff permission.scopeClass is non-null; impls may assert
  /// this invariant.
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue,
  );

  /// Materials for client-side UI gating and app-side scope-aware
  /// queries: the active role's permission set + the user's scope
  /// assignments under that role.
  Future<EffectiveAuthorization> effectivePermissionsFor(Principal principal);
}
