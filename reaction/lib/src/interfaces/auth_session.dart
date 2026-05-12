import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Auth status surfaced by [AuthSession]. Sealed; consumers can switch
/// exhaustively to drive UI (e.g., show login screen on
/// [NotAuthenticated] or [Expired], show app on [Authenticated]).
sealed class AuthStatus {
  const AuthStatus();
}

/// Current credential is valid; [principal] is the validated identity
/// the substrate's `ActionDispatcher`, `PermissionSource`, and audit log
/// will use.
class Authenticated extends AuthStatus {
  final Principal principal;
  const Authenticated({required this.principal});
}

/// No credential is set, or the credential was rejected and never
/// transitioned to [Authenticated]. App should route to login.
class NotAuthenticated extends AuthStatus {
  const NotAuthenticated();
}

/// A previously-valid credential has expired (server returned 401, WS
/// returned an auth-rejected close frame, or local clock detected
/// `exp` passing). App should route to re-auth without discarding
/// app-side state.
class Expired extends AuthStatus {
  const Expired();
}

/// Holds the application's identity credential and exposes the
/// authenticated [Principal] for downstream consumers.
///
/// Implementations:
/// - `LocalAuthSession` (in-process): holds a [Principal] directly; no
///   credential lifecycle (mobile-install case).
/// - `RemoteAuthSession` (cross-process; Plan B-remote): holds a
///   bearer-token string; transitions to [Expired] on auth failures
///   from the wire.
///
/// The active [principal] flows into all other reaction interfaces
/// ([ActionSubmitter] submissions, [PermissionSource] scope) via the
/// `ReActionScope` InheritedWidget (in `reaction_widgets`) or
/// directly when consumers wire components manually.
abstract interface class AuthSession {
  /// Current status.
  AuthStatus get current;

  /// Emits whenever [current] changes (credential set/cleared/expired).
  Stream<AuthStatus> get stream;

  /// Set the credential (after login or token refresh). Pass `null` to
  /// clear (logout). Local impls treat the string as a `Principal.id`
  /// directly; Remote impls treat it as an opaque bearer token.
  void setCredential(String? credential);

  /// Convenience: the current authenticated Principal, or `null` if
  /// not currently [Authenticated].
  Principal? get principal;

  /// Release any underlying resources (e.g., the status stream's
  /// internal controller). After [dispose], the AuthSession is no
  /// longer usable.
  Future<void> dispose();
}
