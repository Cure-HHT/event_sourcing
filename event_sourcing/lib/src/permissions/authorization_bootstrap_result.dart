// lib/src/permissions/authorization_bootstrap_result.dart
// Implements: EVS-PRD-permissions-as-events/A
// sealed result type for the
//   permission bootstrap flow; PolicyReady carries a policy built from the
//   event log; PolicyFailSafe is returned when seed validation fails,
//   preventing malformed grants from entering the log.
// Implements: EVS-PRD-permissions-as-events/B
// the policy exposed by
//   PolicyReady is always backed by the event-log projection; PolicyFailSafe
//   denies everything rather than consulting any external authority when the
//   event-derived projection is unavailable (with the carried errors).
// Implements: EVS-DEV-bootstrap-action-permissions/B/C
// sealed return
//   shape: PolicyFailSafe on validation failure, PolicyReady on success.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

@immutable
sealed class AuthorizationBootstrapResult {
  const AuthorizationBootstrapResult();
  AuthorizationPolicy get policy;
  bool get isReady;
  List<String> get errors;
}

final class PolicyReady extends AuthorizationBootstrapResult {
  const PolicyReady(this._policy);
  final AuthorizationPolicy _policy;

  @override
  AuthorizationPolicy get policy => _policy;
  @override
  bool get isReady => true;
  @override
  List<String> get errors => const <String>[];
}

final class PolicyFailSafe extends AuthorizationBootstrapResult {
  const PolicyFailSafe(this._errors);
  final List<String> _errors;

  @override
  AuthorizationPolicy get policy => const FailSafeAuthorizationPolicy();
  @override
  bool get isReady => false;
  @override
  List<String> get errors => _errors;
}
