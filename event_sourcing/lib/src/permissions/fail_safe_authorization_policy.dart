// Implements: EVS-PRD-action-dispatch/B
// (fail-safe policy denies everything
//   when bootstrap fails; preserves the closed-set of authorize outcomes)
// Implements: EVS-PRD-permissions-as-events/B
// (no decisions consult any
//   authority outside the log; the empty result is the only safe answer
//   when projections are unavailable)
// Implements: EVS-DEV-bootstrap-action-permissions/B
// the
//   FailSafeAuthorizationPolicy that PolicyFailSafe wraps: every
//   isPermitted call denies with DenyReason.notGranted; every
//   effectivePermissionsFor call returns EffectiveAuthorization.empty.

import 'package:event_sourcing/event_sourcing.dart';

class FailSafeAuthorizationPolicy implements AuthorizationPolicy {
  const FailSafeAuthorizationPolicy();

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Transaction? txn,
  }) async => Deny(permission: permission, reason: DenyReason.notGranted);

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Transaction? txn,
  }) async => EffectiveAuthorization.empty;
}
