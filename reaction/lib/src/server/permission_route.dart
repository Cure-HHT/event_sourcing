// Implements: EVS-PRD-permission-source/C
// server side of the
//   /permissions/snapshot HTTP route that RemotePermissionSource fetches
//   on every Authenticated transition.
// Implements: EVS-PRD-permissions-as-events/B
// evaluates the snapshot
//   via AuthorizationPolicy.effectivePermissionsFor (which reads only
//   from substrate event-derived projections).

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/server/view_converging_response.dart';
import 'package:reaction/src/wire/effective_authorization_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for GET /permissions/snapshot. Returns the
/// EffectiveAuthorization for the Principal attached by the auth
/// middleware.
Handler permissionSnapshotHandler({required AuthorizationPolicy policy}) {
  return (Request request) async {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    final EffectiveAuthorization auth;
    try {
      auth = await policy.effectivePermissionsFor(principal);
    } on ViewConvergingRefusal catch (e) {
      // Implements: EVS-DEV-converging-view-reads/H
      // a converging-view refusal reading the role-assignment or
      //   permission-grant views answers a typed, transient 503
      //   naming the view, so RemotePermissionSource can decode it
      //   instead of seeing an untyped 500.
      return viewConvergingResponse(e);
    }
    return Response.ok(
      jsonEncode(EffectiveAuthorizationCodec.encode(auth)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
