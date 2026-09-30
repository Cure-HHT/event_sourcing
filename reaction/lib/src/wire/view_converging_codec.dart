// Implements: EVS-PRD-cross-process-event-transport/K
// Decodes the 503 view_converging response body that action_route.dart
// and permission_route.dart send for a ViewConvergingRefusal, so every
// HTTP client of those routes delivers the same typed, transient
// condition naming the view rather than treating the status code as
// an opaque transport failure.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';

/// Decodes a `{"error": "view_converging", "view": "<name>"}` response
/// body into a [ViewConvergingRefusal] naming that view. Returns `null`
/// when the body is not that shape, so a caller can fall back to a
/// generic transport error for any other 503.
ViewConvergingRefusal? decodeViewConvergingBody(String body) {
  final Object? decoded;
  try {
    decoded = jsonDecode(body);
  } on FormatException {
    return null;
  }
  if (decoded is! Map<String, Object?>) return null;
  if (decoded['error'] != 'view_converging') return null;
  final view = decoded['view'];
  if (view is! String) return null;
  return ViewConvergingRefusal(view);
}
