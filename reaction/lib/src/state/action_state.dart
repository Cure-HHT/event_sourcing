// Implements: EVS-PRD-reaction-widget-contract/C
// ActionState is
// the sealed state machine the widget library's ActionBuilder
// primitive exposes to caller-supplied builders for rendering
// (Idle/Submitting/Success/Denied/Failed).
import 'package:event_sourcing/event_sourcing.dart';

/// Widget-side submission state machine. Used by `ActionBuilder` in
/// `reaction_widgets` to drive a button's idle/loading/result/error UI.
///
/// Lifecycle:
///
/// ```text
/// Idle --submit()--> Submitting --+--> Success(DispatchResult)
///                                  +--> Denied(DispatchResult)
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
  const factory ActionState.denied(DispatchResult<Object?> result) = Denied;
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

/// The submission was denied by the dispatcher (parse failure, validation
/// failure, authorization denied, execution failed, unknown action, or
/// idempotency mismatch). Carries the full [DispatchResult] so widget
/// consumers can switch on specific denial variants for structured UX
/// (e.g., "you need permission X" vs "validation failed: ..."). The
/// [reason] getter is a derived human-readable summary suitable for a
/// default toast/snackbar message.
class Denied extends ActionState {
  const Denied(this.result);
  final DispatchResult<Object?> result;

  /// Human-readable one-line summary of the denial, derived from the
  /// concrete [DispatchResult] variant.
  String get reason => switch (result) {
    DispatchSuccess() => 'unexpected success classified as denial',
    DispatchUnknownAction(:final requestedName) =>
      'unknown action: "$requestedName"',
    DispatchParseDenied(:final error) => 'parse error: $error',
    DispatchValidationDenied(:final error) => 'validation failed: $error',
    DispatchAuthorizationDenied(:final permission) =>
      'you need permission "${permission.name}"',
    DispatchExecutionFailed(:final error) => 'execution failed: $error',
    DispatchIdempotencyHit() =>
      'unexpected idempotency hit classified as denial',
    DispatchIdempotencyMismatch() =>
      'idempotency mismatch — same key, different payload',
  };
}

/// The submission failed with an unexpected error (transport,
/// programming error, etc.). Distinct from [Denied] (which is a
/// pipeline outcome) — [Failed] is a thrown exception.
class Failed extends ActionState {
  Failed(this.error, this.stackTrace);
  final Object error;
  final StackTrace stackTrace;
}
