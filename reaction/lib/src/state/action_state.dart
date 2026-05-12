import 'package:event_sourcing/event_sourcing.dart';

/// Widget-side submission state machine. Used by `ActionBuilder` in
/// `reaction_widgets` to drive a button's idle/loading/result/error UI.
///
/// Lifecycle:
///
/// ```text
/// Idle --submit()--> Submitting --+--> Success(DispatchResult)
///                                  +--> Denied(reason)
///                                  +--> Failed(error, stackTrace)
/// ```
///
/// After any terminal state, the widget can return to Idle (e.g., on
/// next button press) at the widget's discretion.
sealed class ActionState {
  const ActionState();

  /// Convenience constructors mirroring the sealed variants. Lets
  /// downstream code write `ActionState.idle()` without picking which
  /// subclass to import.
  const factory ActionState.idle() = Idle;
  const factory ActionState.submitting() = Submitting;
  const factory ActionState.success(DispatchResult<Object?> result) = Success;
  const factory ActionState.denied(String reason) = Denied;
  factory ActionState.failed(Object error, StackTrace stackTrace) = Failed;
}

/// The submission has not been initiated, or has been reset after a
/// terminal state. Button is enabled (subject to permission gating).
class Idle extends ActionState {
  const Idle();
}

/// The submission is in flight. Button is disabled; loading indicator
/// shown.
class Submitting extends ActionState {
  const Submitting();
}

/// The submission completed successfully and the substrate emitted
/// events.
class Success extends ActionState {
  const Success(this.result);
  final DispatchResult<Object?> result;
}

/// The submission was denied by the dispatcher (parse failure,
/// validation failure, authorization denied, execution failed, or
/// idempotency conflict). [reason] is a human-readable summary for
/// the UI; full denial details are available via the [DispatchResult]
/// passed to the widget builder (when applicable).
class Denied extends ActionState {
  const Denied(this.reason);
  final String reason;
}

/// The submission failed with an unexpected error (transport,
/// programming error, etc.). Distinct from [Denied] (which is a
/// pipeline outcome) — [Failed] is a thrown exception.
class Failed extends ActionState {
  Failed(this.error, this.stackTrace);
  final Object error;
  final StackTrace stackTrace;
}
