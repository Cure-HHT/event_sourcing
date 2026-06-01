// Implements: EVS-PRD-action-submitter/A — non-widget dispatch path that
// mints an idempotency key when the submission lacks one, then delegates to
// the pass-through ActionSubmitter (so actions declaring Idempotency.required
// are not parse-denied for programmatic/sequential callers).
import 'package:event_sourcing/event_sourcing.dart';

import '../interfaces/action_submitter.dart';
import 'idempotency_key_generator.dart';

/// Non-widget action dispatch: the programmatic analog of `ActionBuilder`'s
/// submit path. Mints an idempotency key (via [IdempotencyKeyGenerator]) when
/// the submission lacks one, then delegates to the [ActionSubmitter]. A
/// consumer-supplied key is passed through unchanged. Use for
/// sequential/programmatic dispatch where there is no per-action widget.
class ActionClient {
  ActionClient(this._submitter, {IdempotencyKeyGenerator? keyGenerator})
    : _keyGen = keyGenerator ?? UuidIdempotencyKeyGenerator();

  final ActionSubmitter _submitter;
  final IdempotencyKeyGenerator _keyGen;

  Future<DispatchResult<Object?>> submit(ActionSubmission submission) {
    final keyed = submission.idempotencyKey != null
        ? submission
        : withGeneratedKey(submission, _keyGen.generate());
    return _submitter.submit(keyed);
  }

  /// Copy of [submission] carrying [key]. Single implementation of
  /// submission-keying, reused by `ActionBuilder` so the widget keeps its
  /// retry-stable cached key.
  static ActionSubmission withGeneratedKey(
    ActionSubmission submission,
    String key,
  ) => ActionSubmission(
    actionName: submission.actionName,
    rawInput: submission.rawInput,
    idempotencyKey: key,
    flowToken: submission.flowToken,
  );
}
