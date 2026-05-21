import 'dart:convert';

import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/principal_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for GET /me. Returns the Principal attached by the auth
/// middleware. The Remote client uses this to validate a credential
/// and obtain the Principal for AuthStatus.Authenticated.
Handler meRouteHandler() {
  return (Request request) {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    return Response.ok(
      jsonEncode(PrincipalCodec.encode(principal)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
