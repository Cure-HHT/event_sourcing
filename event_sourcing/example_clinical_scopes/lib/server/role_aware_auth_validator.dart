// example_clinical_scopes/lib/server/role_aware_auth_validator.dart
//
// PrincipalAuthValidator for the clinical-scopes demo. Copied from
// `reaction/example/lib/server/role_aware_auth_validator.dart` with the
// role-priority list adapted to this demo's roles. Reads
// `user_role_scopes` at authenticate-time and surfaces the user's
// HIGHEST-PRIVILEGED role (Admin > Overseer > Investigator) as their
// activeRole.
//
// This is what a production validator looks like: TrustingAuthValidator
// is fine for the "any-credential-accepted, single-role-everywhere" demo
// case, but each seeded user here holds a DIFFERENT role — and the only
// source of truth for "what roles does dr-investigator hold today" is the
// substrate's event log. The auth boundary (identity) trusts the bearer;
// the role binding flows out of the closed-under-events trust model.
//
// A user with NO role_assigned events authenticates as an
// AnonymousPrincipal so the substrate's authorize stage denies every
// permission check.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/reaction.dart';

/// Per the demo's design: highest-privilege wins.
const List<String> _rolePriority = <String>[
  'Admin',
  'Overseer',
  'Investigator',
];

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
