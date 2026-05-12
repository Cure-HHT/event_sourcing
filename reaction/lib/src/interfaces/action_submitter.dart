import 'package:event_sourcing/event_sourcing.dart';

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
///   to `/actions/{actionType}`; deserializes [DispatchResult] from the
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
  Future<DispatchResult> submit(ActionSubmission submission);
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
