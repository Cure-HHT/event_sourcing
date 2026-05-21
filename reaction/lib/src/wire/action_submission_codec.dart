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

  static Map<String, Object?> encode(ActionSubmission s) {
    final out = <String, Object?>{
      'actionName': s.actionName,
      'rawInput': s.rawInput,
    };
    if (s.idempotencyKey != null) out['idempotencyKey'] = s.idempotencyKey;
    if (s.flowToken != null) out['flowToken'] = s.flowToken;
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
