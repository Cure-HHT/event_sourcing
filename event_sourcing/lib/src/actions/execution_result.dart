// Implements: EVS-PRD-action-dispatch/B (execute stage return type: carries events for atomic persistence in Stage 8)
// Implements: EVS-PRD-action-dispatch/C (events list fed into the atomic persist stage that records the success outcome)

import 'package:event_sourcing/src/event_draft.dart';
import 'package:event_sourcing/src/security/security_details.dart';

/// What an `Action.execute` returns to the dispatcher.
///
/// `events` is the (possibly empty) list of [EventDraft]s to persist
/// atomically. `securityDetailsOverride`, when non-null, replaces
/// `ActionContext.security` for all events written by this dispatch
/// (rare; default behavior is to use ctx.security).
//
// persists `events` in one transaction.
class ExecutionResult<TResult> {
  const ExecutionResult({
    required this.result,
    required this.events,
    this.securityDetailsOverride,
  });

  final TResult result;
  final List<EventDraft> events;
  final SecurityDetails? securityDetailsOverride;
}
