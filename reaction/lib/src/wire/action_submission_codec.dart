// Implements: EVS-PRD-cross-process-event-transport/A — JSON codec for
//   ActionSubmission wire envelopes; round-trip preserves all fields.
// Implements: EVS-PRD-action-submitter/C — wire shape consumed by
//   POST /actions.

import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for [ActionSubmission].
///
/// Wire shape:
///   {"actionName": "...",
///    "rawInput": { ... },
///    "idempotencyKey": "..." (optional),
///    "flowToken": "..." (optional)}
class ActionSubmissionCodec {
  const ActionSubmissionCodec._();

  static Map<String, Object?> encode(ActionSubmission submission) {
    final out = <String, Object?>{
      'actionName': submission.actionName,
      'rawInput': submission.rawInput,
    };
    if (submission.idempotencyKey != null)
      out['idempotencyKey'] = submission.idempotencyKey;
    if (submission.flowToken != null) out['flowToken'] = submission.flowToken;
    return out;
  }

  static ActionSubmission decode(Map<String, Object?> json) {
    return ActionSubmission(
      actionName: requireString(json, 'actionName'),
      rawInput: requireMap(json, 'rawInput'),
      idempotencyKey: readString(json, 'idempotencyKey'),
      flowToken: readString(json, 'flowToken'),
    );
  }
}
