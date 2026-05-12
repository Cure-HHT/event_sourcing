// IMPLEMENTS REQUIREMENTS:
//   and passed through every stage.

import 'package:event_sourcing/src/actions/principal.dart';
import 'package:event_sourcing/src/security/security_details.dart';

/// Carries the per-dispatch caller context: who, with what security
/// telemetry, when. Passed to `Action.validate` and `Action.execute`.
class ActionContext {
  const ActionContext({
    required this.principal,
    required this.security,
    required this.requestStartedAt,
  });

  final Principal principal;
  final SecurityDetails security;
  final DateTime requestStartedAt;
}
