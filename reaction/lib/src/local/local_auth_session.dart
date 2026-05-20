// Implements: EVS-PRD-auth-session/A/B/G — in-process AuthSession
// impl: realizes the interface (A), flips between Authenticated and
// NotAuthenticated of the AuthStatus sealed type (B; never reaches
// Expired in the in-process case), and exposes the active Principal
// as the source of truth for downstream interfaces (G). Credential
// string used directly as UserPrincipal.userId (mobile-install case).
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import 'package:reaction/src/interfaces/auth_session.dart';

/// In-process [AuthSession]. The credential string is treated as a
/// [UserPrincipal.userId] directly — there is no credential validation
/// (mobile-install case has no JWT).
///
/// The constructed [UserPrincipal] has `roles: {defaultActiveRole}`
/// and `activeRole: defaultActiveRole`. The `UserPrincipal` invariant
/// requires `activeRole` to be a member of `roles`; the session does
/// not know any other roles. Substrate-side permission events drive
/// grant lookups; consumers configure the role name via the
/// constructor.
///
/// Status flips between [Authenticated] and [NotAuthenticated]; never
/// [Expired] (no credential expiration concept in-process — the
/// mobile-install case does not have a bearer-token lifetime).
class LocalAuthSession implements AuthSession {
  /// Creates a [LocalAuthSession].
  ///
  /// [defaultActiveRole] is the `activeRole` assigned to all
  /// [UserPrincipal]s created by [setCredential]. Defaults to `'install'`
  /// for the mobile-install case; override for tests or alternative
  /// deployment shapes.
  LocalAuthSession({this.defaultActiveRole = 'install'});

  /// The activeRole assigned to authenticated principals.
  final String defaultActiveRole;

  AuthStatus _current = const NotAuthenticated();
  final StreamController<AuthStatus> _controller =
      StreamController<AuthStatus>.broadcast();

  @override
  AuthStatus get current => _current;

  @override
  Stream<AuthStatus> get stream => _controller.stream;

  @override
  Principal? get principal {
    final c = _current;
    return c is Authenticated ? c.principal : null;
  }

  @override
  void setCredential(String? credential) {
    final newStatus = credential == null
        ? const NotAuthenticated()
        : Authenticated(
            principal: Principal.user(
              userId: credential,
              roles: {defaultActiveRole},
              activeRole: defaultActiveRole,
            ),
          );
    _current = newStatus;
    if (!_controller.isClosed) _controller.add(newStatus);
  }

  @override
  Future<void> dispose() async {
    if (!_controller.isClosed) await _controller.close();
  }
}
