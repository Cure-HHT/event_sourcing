import 'package:event_sourcing/event_sourcing.dart';

/// A request to dispatch one action. Bundles the dispatcher's call
/// arguments into a single value so the [ActionSubmitter] contract is
/// uniform across in-process and remote transports.
///
/// The submitter implementation constructs the substrate's
/// `ActionContext` from its co-mounted [AuthSession] (Local) or from
/// the server-side validated credential (Remote) — the active
/// [Principal] is NOT carried on this struct. The credential header
/// for Remote submissions is attached by the [ActionSubmitter] impl
/// itself, not threaded through this submission.
class ActionSubmission {
  /// The registered name of the action to dispatch (e.g. `'submit_note'`).
  /// Matches what `ActionRegistry.lookup` accepts.
  final String actionName;

  /// The raw input the dispatcher's Stage 3 (parse) consumes. Shape is
  /// per-action and verified by the registered action's `parseInput`.
  final Map<String, Object?> rawInput;

  /// Idempotency key per the registered action's `IdempotencyPolicy`.
  /// `null` is valid when the action's policy is `none` or `optional`.
  /// For policy `required`, omitting the key causes the dispatcher to
  /// return a `parse_denied` outcome.
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

/// Submits an [ActionSubmission] for dispatch through the substrate's
/// parse → validate → authorize → execute → record pipeline and returns
/// the [DispatchResult].
///
/// Two impls ship with `reaction`:
///
/// - `LocalActionSubmitter` (in-process): delegates to
///   `ActionDispatcher.dispatch`. Returns the resulting [DispatchResult]
///   directly.
/// - `RemoteActionSubmitter` (cross-process; Plan B-remote): HTTP POST
///   to `/actions/{actionName}`; deserializes [DispatchResult] from the
///   response.
///
/// Idempotency key generation is the WIDGET-layer's concern (handled by
/// `ActionBuilder` in `reaction_widgets`). The submitter itself is a
/// pass-through: whatever key the caller put in
/// [ActionSubmission.idempotencyKey] is what the dispatcher sees.
abstract interface class ActionSubmitter {
  /// Submit an action. Returns when the dispatch pipeline has
  /// completed (Success with emitted events, or one of the denial
  /// variants).
  ///
  /// May throw a [TransportException] subclass on transport-level
  /// failures (network down for Remote; argument programming errors
  /// for both). Application-level denials surface as [DispatchResult]
  /// denial variants, NOT as thrown exceptions.
  Future<DispatchResult<Object?>> submit(ActionSubmission submission);
}

/// Transport-level failures (network unavailable, malformed wire
/// response, etc.). Not used for dispatch-level denials — those flow
/// through [DispatchResult]'s denial variants.
class TransportException implements Exception {
  final String message;
  final Object? cause;
  const TransportException(this.message, {this.cause});

  @override
  String toString() => cause == null
      ? 'TransportException: $message'
      : 'TransportException: $message (cause: $cause)';
}
