// reaction/example/lib/server/role_aware_auth_validator.dart
//
// PrincipalAuthValidator for the example: reads `user_role_scopes`
// at authenticate-time and surfaces the user's HIGHEST-PRIVILEGED role
// (admin > editor > viewer) as their activeRole.
//
// This is what a production validator looks like: TrustingAuthValidator
// is fine for the "any-credential-accepted, single-role-everywhere" demo
// case, but the multi-user example here has each seeded user holding a
// DIFFERENT role — and the only source of truth for "what roles does
// alice hold today" is the substrate's event log. The auth boundary
// (identity) trusts the bearer; the role binding flows out of the
// closed-under-events trust model.
//
// Anyone with a non-empty bearer authenticates as a UserPrincipal with
// that userId; if they have NO role_assigned events in the log they
// authenticate as a user with an empty role set — but UserPrincipal
// requires `activeRole` to be in `roles`, so we fall back to
// AnonymousPrincipal in that case. Wrong-credential / unknown-user gets
// AuthenticationDenied if the credential is empty (same as Trusting),
// otherwise it proceeds as anonymous.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';

/// Per the demo's design: highest-privilege wins.
const List<String> _rolePriority = <String>['admin', 'editor', 'viewer'];

/// Authenticates by reading `user_role_scopes` for the bearer's userId
/// and picking the most-privileged role they currently hold. Untrusted
/// for production: identity is still trust-on-supply (no JWT, no
/// signature). The example uses this to teach how a real consumer's
/// validator would consult the substrate's event log for role data.
class RoleAwareTrustingValidator implements PrincipalAuthValidator {
  RoleAwareTrustingValidator({required this.backend});

  final StorageBackend backend;

  @override
  Future<Principal> authenticate(String credential) async {
    if (credential.isEmpty) {
      throw const AuthenticationDenied('empty credential');
    }
    final rows = await backend.findViewRows('user_role_scopes');
    final userRoles = <String>{
      for (final row in rows)
        if (row['user_id'] == credential) row['role']! as String,
    };
    if (userRoles.isEmpty) {
      // Identity is fine, but no role is bound to this user — return
      // anonymous so the substrate's authorize stage denies every
      // permission check (closed-under-events trust model).
      return const AnonymousPrincipal();
    }
    final activeRole = _rolePriority.firstWhere(
      userRoles.contains,
      orElse: () => userRoles.first,
    );
    return UserPrincipal(
      userId: credential,
      roles: userRoles,
      activeRole: activeRole,
    );
  }
}
