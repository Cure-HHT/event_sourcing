// Implements: EVS-DEV-converging-view-reads/H
// Shared 503 response shape for a ViewConvergingRefusal surfaced by any
// server route that decides from a view's rows (action dispatch, the
// permission snapshot route): a typed, transient refusal naming the
// view, with a Retry-After header, so RemoteActionSubmitter and
// RemotePermissionSource decode it instead of seeing an untyped 500 or
// an opaque transport failure.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:shelf/shelf.dart';

Response viewConvergingResponse(ViewConvergingRefusal e) => Response(
  503,
  body: jsonEncode({'error': 'view_converging', 'view': e.viewName}),
  headers: {'Content-Type': 'application/json', 'Retry-After': '1'},
);
