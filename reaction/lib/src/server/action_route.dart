// Implements: EVS-PRD-cross-process-event-transport/F — bearer-credential
//   carriage is enforced upstream by auth_middleware (or the consumer's
//   own middleware); this handler reads the validated Principal from
//   the request context and dispatches against the in-process substrate.
// Implements: EVS-PRD-action-submitter/C — server side of the HTTP POST
//   /actions round-trip that RemoteActionSubmitter consumes.

import 'dart:convert';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/server/auth_middleware.dart';
import 'package:reaction/src/wire/action_submission_codec.dart';
import 'package:reaction/src/wire/dispatch_result_codec.dart';
import 'package:shelf/shelf.dart';

/// Handler for POST /actions. Expects the caller-side
/// [authMiddleware] (or an equivalent) to have attached a [Principal]
/// to the request context. JSON-decodes the body into an
/// [ActionSubmission], builds an [ActionContext] (with default empty
/// [SecurityDetails] — production deployments wrap this handler in
/// middleware that populates IP / user-agent / etc.), dispatches via
/// the supplied [ActionDispatcher], and JSON-encodes the resulting
/// [DispatchResult] into a 200 response.
///
/// 400 on a malformed JSON body or a shape that fails
/// [ActionSubmissionCodec.decode]. 500 if no Principal is attached
/// (middleware misconfiguration — a configured deployment never
/// reaches this branch).
///
/// The substrate's [ActionDispatcher.dispatch] returns
/// `DispatchResult<Object?>`; this handler is bound to the same
/// generic. The dispatcher decides Success vs any of the Denied
/// variants — the handler simply forwards the codec output.
Handler actionHandler({
  required ActionDispatcher dispatcher,
  DateTime Function()? now,
}) {
  final clock = now ?? DateTime.now;
  return (Request request) async {
    final principal = principalFromContext(request);
    if (principal == null) {
      return Response(500, body: 'no Principal in context');
    }
    final Map<String, Object?> json;
    try {
      json = jsonDecode(await request.readAsString()) as Map<String, Object?>;
    } catch (_) {
      return Response(400, body: 'malformed json');
    }
    final ActionSubmission submission;
    try {
      submission = ActionSubmissionCodec.decode(json);
    } on FormatException catch (e) {
      return Response(400, body: 'malformed submission: ${e.message}');
    }
    final ctx = ActionContext(
      principal: principal,
      security: const SecurityDetails(),
      requestStartedAt: clock(),
    );
    final result = await dispatcher.dispatch(submission, ctx);
    return Response.ok(
      jsonEncode(DispatchResultCodec.encode(result)),
      headers: {'Content-Type': 'application/json'},
    );
  };
}
