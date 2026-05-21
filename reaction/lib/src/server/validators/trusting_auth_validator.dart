// Implements: EVS-PRD-auth-session/F — TrustingAuthValidator reference impl.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';

/// Reference [PrincipalAuthValidator] for DEV/TEST USE ONLY.
///
/// Accepts any non-empty credential string verbatim as the authenticated
/// `Principal.userId`. Performs NO cryptographic validation — anyone with
/// a non-empty string is "authenticated" as a user with that ID.
///
/// **DO NOT USE IN PRODUCTION.** Production deployments must mount their
/// own validator (Firebase, Auth0, linking-code, etc.) at composition
/// time. This impl exists so the reference test harness and the in-repo
/// examples can stand up a server without authentication scaffolding.
///
/// Returns a [UserPrincipal] with:
/// - `userId = credential`
/// - `roles = {defaultActiveRole}` — the single role the user holds
/// - `activeRole = defaultActiveRole` — the role they act under
class TrustingAuthValidator implements PrincipalAuthValidator {
  TrustingAuthValidator({required this.defaultActiveRole});

  final String defaultActiveRole;

  @override
  Future<Principal> authenticate(String credential) async {
    if (credential.isEmpty) {
      throw const AuthenticationDenied('empty credential');
    }
    return UserPrincipal(
      userId: credential,
      roles: {defaultActiveRole},
      activeRole: defaultActiveRole,
    );
  }
}
