// Implements: EVS-PRD-auth-session/C/D — defines the
// PrincipalAuthValidator interface (C: authenticate(String) returns
// the Principal or throws AuthenticationDenied) and the credential-
// format-opaque rule (D: format selection delegated to the validator).
import 'package:event_sourcing/event_sourcing.dart';

/// Validates an opaque credential string from the wire (HTTP header
/// `X-Principal-Auth-Credential` or WS handshake `auth` field) and
/// returns the authenticated [Principal]. Implementations decide what
/// shape the credential takes (JWT, opaque session token, dev
/// shortcut, etc.) — `reaction` is agnostic.
///
/// Throws [AuthenticationDenied] on rejection. Server-side handlers
/// translate this to a wire-level rejection (HTTP 401, WS auth-rejected
/// close frame).
///
/// Reference impls shipped with `reaction`:
///
/// - `TrustingAuthValidator` — dev/test only; accepts any non-empty
///   credential verbatim as `Principal.id`. Loud "DO NOT USE IN
///   PRODUCTION" docstring.
/// - Optional `JwtAuthValidator` — verifies a JWT against a configured
///   public key/issuer. (Deferred; the pluggable seam is in place.)
///
/// This interface lives in the client-side `reaction` package because
/// it is shared with the server-side `reaction` server module;
/// dependency direction is one-way (server depends on reaction;
/// reaction does not depend on the server).
abstract interface class PrincipalAuthValidator {
  /// Validate [credential] and return the authenticated [Principal].
  /// Throws [AuthenticationDenied] on rejection.
  Future<Principal> authenticate(String credential);
}

/// Thrown by [PrincipalAuthValidator.authenticate] when a credential
/// is rejected. The [message] is for server-side logging only — do not
/// surface it raw to clients, as it may leak validator internals.
class AuthenticationDenied implements Exception {
  final String message;
  const AuthenticationDenied(this.message);

  @override
  String toString() => 'AuthenticationDenied: $message';
}
