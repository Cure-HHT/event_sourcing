// Implements: EVS-PRD-action-dispatch/B (authorize stage pluggable interface: isPermitted called once per declared permission)
// Implements: EVS-PRD-permissions-as-events/B (concrete impls evaluate decisions from event-derived projections only)
// Implements: EVS-PRD-library-charter/H (trust-boundary interface: AuthorizationPolicy is the named, registered policy surface)

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';

/// Pluggable authorization decision-maker. Concrete impls live in the
/// permissions module within `event_sourcing`
/// (`TableBackedAuthorizationPolicy` over a `RoleMatrixReader`;
/// `FailSafeAuthorizationPolicy` for the boot-failure case).
abstract class AuthorizationPolicy {
  const AuthorizationPolicy();

  /// Decide whether [principal] may exercise [permission].
  ///
  /// The dispatcher's authorize stage calls this once per permission
  /// declared by the action being dispatched. The first non-Allow
  /// short-circuits and produces an `authorization_denied` denial event.
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
  );

  /// The exercisable permission set for [principal] right now (filtered
  /// by session-context preconditions).
  ///
  /// Hosts call this once per session start to construct a
  /// `PermissionSnapshot` for client delivery.
  Future<Set<Permission>> permissionsFor(Principal principal);
}
