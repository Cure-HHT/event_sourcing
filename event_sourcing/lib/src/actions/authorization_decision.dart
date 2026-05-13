// Implements: EVS-PRD-action-dispatch/B (authorize stage outcome type: Allow falls through; Deny short-circuits to denial event)
// Implements: EVS-PRD-action-dispatch/C (Deny carries permission + reason so dispatcher can record authorization_denied)
// Implements: EVS-PRD-permissions-as-events/B (authorization evaluated solely from event-derived projections; this sealed type carries the decision)

import 'package:event_sourcing/src/actions/permission.dart';
import 'package:meta/meta.dart';

/// The outcome of an `AuthorizationPolicy.isPermitted` call.
//
// Sealed: every consumer-side switch must exhaustively handle Allow and
// Deny. Adding a third variant is a deliberate code-plus-REQ change.
@immutable
sealed class AuthorizationDecision {
  const AuthorizationDecision();
}

/// The principal is permitted to exercise the permission.
final class Allow extends AuthorizationDecision {
  const Allow();
}

/// The principal is NOT permitted. Carries the denied permission and
/// the reason class so the dispatcher can construct the right denial
/// event payload.
final class Deny extends AuthorizationDecision {
  const Deny({required this.permission, required this.reason});

  final Permission permission;
  final DenyReason reason;
}

/// Why a [Deny] decision was returned.
//
// Closed enum. Adding a value here is a deliberate code-plus-REQ change.
enum DenyReason {
  /// Principal's active role doesn't carry the permission, OR no scope
  /// assignment under that role covers the requested scope (including
  /// the fail-closed containment-miss case).
  notGranted,

  /// `Action.scopeFor` returned null for a scoped permission, or
  /// returned a `ScopeValue` whose class does not match the permission's
  /// declared `scopeClass`. A programmer-bug surface.
  scopeUnresolvable,
}
