// lib/src/permissions/fail_safe_authorization_policy.dart
// Implements: EVS-PRD-permissions-as-events/B — safe fallback implementation
// of AuthorizationPolicy used when bootstrap validation fails; denies all
// requests with DenyReason.notGranted so that no authorization decision
// is made from a corrupt or incomplete event-derived projection.
//
// TODO(Task 18): Task 17 made the signature compatible with the
// reshaped AuthorizationPolicy interface so the substrate compiles;
// Task 18 owns the spec-aligned rewrite and the matching test refresh.

import 'package:event_sourcing/event_sourcing.dart';

class FailSafeAuthorizationPolicy implements AuthorizationPolicy {
  const FailSafeAuthorizationPolicy(this.bootstrapErrors);
  final List<String> bootstrapErrors;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission perm,
    ScopeValue? scopeValue,
  ) async {
    return Deny(permission: perm, reason: DenyReason.notGranted);
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal,
  ) async => EffectiveAuthorization.empty;
}
