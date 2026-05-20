// Implements: EVS-PRD-action-dispatch/B (satisfies the AuthorizationPolicy interface contract required by the authorize stage)
// Note: DenyAllAuthorizationPolicy is a test fixture and bootstrap placeholder — not the production policy.
// Production deployments wire TableBackedAuthorizationPolicy (EVS-PRD-permissions-as-events/B).

import 'package:event_sourcing/src/actions/authorization_decision.dart';
import 'package:event_sourcing/src/actions/authorization_policy.dart';
import 'package:event_sourcing/src/actions/permission.dart';
import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/actions/scope_value.dart';
import 'package:event_sourcing/src/permissions/effective_authorization.dart';
import 'package:event_sourcing/src/storage/txn.dart';

/// Authorization policy that denies every request. Useful for unit
/// tests of the dispatcher's authorize stage and as a temporary
/// placeholder during early app bootstrap.
///
/// The default constructor logs a warning on every call (signal that
/// production should NOT be using this). Use [DenyAllAuthorizationPolicy.forTests]
/// to suppress the warning in unit tests.
class DenyAllAuthorizationPolicy extends AuthorizationPolicy {
  const DenyAllAuthorizationPolicy() : _suppressWarning = false;

  const DenyAllAuthorizationPolicy.forTests() : _suppressWarning = true;

  final bool _suppressWarning;

  @override
  Future<AuthorizationDecision> isPermitted(
    Principal principal,
    Permission permission,
    ScopeValue? scopeValue, {
    Txn? txn,
  }) async {
    if (!_suppressWarning) {
      // ignore: avoid_print
      print(
        'WARNING: DenyAllAuthorizationPolicy.isPermitted called in '
        'production mode (use TableBackedAuthorizationPolicy from the '
        "event_sourcing package's permissions module, or another "
        'concrete policy)',
      );
    }
    return Deny(permission: permission, reason: DenyReason.notGranted);
  }

  @override
  Future<EffectiveAuthorization> effectivePermissionsFor(
    Principal principal, {
    Txn? txn,
  }) async => EffectiveAuthorization.empty;
}
