// Implements: EVS-PRD-action-dispatch/B (authorize stage pluggable interface)
// Implements: EVS-PRD-permissions-as-events/B (concrete impls evaluate decisions from event-derived projections only)
// Implements: EVS-PRD-permissions-as-events/D (the AuthorizationPolicy interface and its concrete impls live in library code; apps do not subclass it to inject alternative Allow/Deny logic — closed-under-events requires the decision function itself to be part of the substrate)
// Implements: EVS-PRD-library-charter/H (trust-boundary interface: AuthorizationPolicy is the named, registered policy surface)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/effective_authorization.dart';
import 'package:event_sourcing/src/storage/txn.dart';

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
  ///
  /// When [txn] is non-null, the policy's projection reads MUST run
  /// inside that transaction (and therefore against the same storage
  /// read-snapshot the caller is using for subsequent writes). When
  /// [txn] is null, the policy opens its own transaction. The
  /// dispatcher passes its active txn so that authorize + execute share
  /// a snapshot — a revocation committed mid-flight cannot invalidate
  /// an authorized dispatch.
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  });

  /// Materials for client-side UI gating and app-side scope-aware
  /// queries: the active role's permission set + the user's scope
  /// assignments under that role.
  ///
  /// [txn] has the same semantics as on [isPermitted]: non-null = run
  /// reads in the caller's transaction; null = open one internally.
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  });
}
