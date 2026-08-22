// Implements: EVS-PRD-auth-session/C
// invokes
//   PrincipalAuthValidator.authenticate on the bearer credential and
//   attaches the resulting Principal to the request context (or surfaces
//   AuthenticationDenied as 401).
// Implements: EVS-PRD-auth-session/E
// HTTP 401 on credential failure
//   is the wire signal the RemoteAuthSession maps to AuthStatus.Expired.
// Implements: EVS-PRD-cross-process-event-transport/F
// bearer-credential
//   carriage on HTTP routes.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/principal_auth_validator.dart';
import 'package:shelf/shelf.dart';

const String _kPrincipalContextKey = 'reaction.principal';

/// Shelf middleware that reads `Authorization: Bearer <credential>`,
/// validates it with the supplied [PrincipalAuthValidator], and
/// attaches the resulting Principal to the request context under
/// the key consumed by [principalFromContext].
///
/// On missing/non-Bearer Authorization: returns 401.
/// On AuthenticationDenied: returns 401.
/// On other exception: returns 500.
Middleware authMiddleware(PrincipalAuthValidator validator) {
  return (Handler inner) {
    return (Request request) async {
      final header = request.headers['Authorization'];
      if (header == null || !header.startsWith('Bearer ')) {
        return Response(401);
      }
      final credential = header.substring('Bearer '.length);
      try {
        final principal = await validator.authenticate(credential);
        return inner(
          request.change(context: {_kPrincipalContextKey: principal}),
        );
      } on AuthenticationDenied {
        return Response(401);
      } catch (_) {
        return Response(500);
      }
    };
  };
}

/// Read the Principal attached by [authMiddleware].
Principal? principalFromContext(Request request) =>
    request.context[_kPrincipalContextKey] as Principal?;
