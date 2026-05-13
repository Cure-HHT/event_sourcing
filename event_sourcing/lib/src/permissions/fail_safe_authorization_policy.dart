// lib/src/permissions/fail_safe_authorization_policy.dart
// Implements: EVS-PRD-permissions-as-events/B — safe fallback implementation
// of AuthorizationPolicy used when bootstrap validation fails; denies all
// requests with DenyReason.notGranted so that no authorization decision
// is made from a corrupt or incomplete event-derived projection.

import 'package:event_sourcing/event_sourcing.dart';

class FailSafeAuthorizationPolicy implements AuthorizationPolicy {
  const FailSafeAuthorizationPolicy(this.bootstrapErrors);
  final List<String> bootstrapErrors;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission perm,
  ) async {
    return Deny(permission: perm, reason: DenyReason.notGranted);
  }

  @override
  Future<Set<Permission>> permissionsFor(Principal principal) async {
    return const <Permission>{};
  }
}
