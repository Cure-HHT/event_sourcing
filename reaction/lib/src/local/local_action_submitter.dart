// Implements: EVS-PRD-action-submitter/B — LocalActionSubmitter
// delegates submit() to an in-process ActionDispatcher.dispatch.
// Builds ActionContext from the wired-in AuthSession's active
// Principal (per EVS-PRD-auth-session-G).
import 'package:event_sourcing/event_sourcing.dart';

import 'package:reaction/src/interfaces/action_submitter.dart';
import 'package:reaction/src/interfaces/auth_session.dart';

/// In-process [ActionSubmitter] impl: delegates directly to the
/// substrate's [ActionDispatcher]. Builds [ActionContext] from the
/// configured [AuthSession]'s active [Principal].
///
/// If the [AuthSession] is not in [Authenticated] state, submissions
/// throw [TransportException] — application code should route to login
/// before calling `submit`.
class LocalActionSubmitter implements ActionSubmitter {
  LocalActionSubmitter({
    required this.dispatcher,
    required this.authSession,
    DateTime Function()? now,
  }) : _now = now ?? DateTime.now;

  final ActionDispatcher dispatcher;
  final AuthSession authSession;
  final DateTime Function() _now;

  @override
  Future<DispatchResult<Object?>> submit(ActionSubmission submission) async {
    final principal = authSession.principal;
    if (principal == null) {
      throw const TransportException('not authenticated');
    }
    final ctx = ActionContext(
      principal: principal,
      security: const SecurityDetails(),
      requestStartedAt: _now(),
    );
    return dispatcher.dispatch(submission, ctx);
  }
}
