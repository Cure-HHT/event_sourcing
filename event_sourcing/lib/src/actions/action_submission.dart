/// The complete input to one [ActionDispatcher.dispatch] call.
///
/// Bundles the action name, raw input, and optional idempotency/flow
/// correlation fields. The substrate's [ActionContext] is passed as a
/// SEPARATE argument to `dispatch`, not carried on this submission —
/// the caller owns Principal construction and timing.
///
/// Symmetric with [DispatchResult] on the output side.
class ActionSubmission {
  /// Registered name of the action to dispatch (e.g. `'submit_note'`).
  /// Matches what `ActionRegistry.lookup` accepts.
  final String actionName;

  /// The raw input the dispatcher's Stage 3 (parse) consumes. Shape is
  /// per-action and verified by the registered action's `parseInput`.
  final Map<String, Object?> rawInput;

  /// Idempotency key per the registered action's `IdempotencyPolicy`.
  /// `null` is valid when the action's policy is `none` or `optional`.
  /// For policy `required`, omitting the key causes the dispatcher to
  /// return a `parse_denied` outcome (Stage pre-3 precondition check;
  /// see REQ-d00170-B in action_dispatcher.dart's docs).
  final String? idempotencyKey;

  /// Optional cross-action correlation token. The dispatcher stamps it
  /// onto every emitted event's metadata so downstream audit can trace
  /// related actions across a single user flow.
  final String? flowToken;

  const ActionSubmission({
    required this.actionName,
    required this.rawInput,
    this.idempotencyKey,
    this.flowToken,
  });
}
