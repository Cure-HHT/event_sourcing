// Implements: EVS-PRD-reaction-widget-contract/I

import 'package:meta/meta.dart';

/// View-subscription rendering state exposed by `ViewBuilder`.
///
/// Three variants, exhaustive:
///
/// - [Loading]      — pre-`EndOfReplay`; no rows yet (or progressive mode
///                    disabled).
/// - [Ready]        — post-`EndOfReplay`; rows are live and current.
/// - [Disconnected] — transport disconnected; `lastRows` retained for UX
///                    continuity. Transition is driven by the composed
///                    `ReactionScope`'s authoritative `ConnectionStatus`
///                    per `EVS-PRD-reaction-widget-contract`-I — NOT by
///                    inference from subscription-stream liveness.
///
/// Note: this `Disconnected` is the **rendering-state** variant. It is
/// distinct from `reaction`'s `ConnectionStatus.Disconnected` (transport
/// liveness). Consumer code that imports both `package:reaction` and
/// `package:reaction_widgets` may need a `hide` clause to disambiguate.
@immutable
sealed class ViewState<T> {
  const ViewState();
}

/// Pre-`EndOfReplay`: no rows surfaced yet (default mode).
class Loading<T> extends ViewState<T> {
  const Loading();
}

/// Post-`EndOfReplay`: rows are live and current.
class Ready<T> extends ViewState<T> {
  const Ready(this.rows);

  /// Current row set. Order is the order in which rows were observed
  /// during snapshot replay + live deltas (`ViewBuilder` does not sort).
  final List<T> rows;
}

/// Transport disconnected. `lastRows` is the most recently rendered row
/// set before the transport dropped, retained so apps can render
/// "stale data with reconnecting banner" rather than blanking.
///
/// `error` carries the triggering `ConnectionStatus` (typically
/// `Reconnecting` or `ConnectionStatus.Disconnected`) for the builder's
/// information.
class Disconnected<T> extends ViewState<T> {
  const Disconnected(this.lastRows, this.error);

  final List<T> lastRows;
  final Object error;
}
