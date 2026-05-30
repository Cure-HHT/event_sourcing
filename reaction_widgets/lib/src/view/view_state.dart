// Implements: EVS-PRD-reaction-widget-contract/I

import 'package:meta/meta.dart';

/// View-subscription rendering state exposed by `ViewBuilder`.
///
/// Three variants, exhaustive:
///
/// - [Loading] — pre-`EndOfReplay`; no rows yet (or isProgressive mode
///   disabled).
/// - [Ready]   — post-`EndOfReplay`; rows are live and current.
/// - [Stale]   — transport disconnected; `lastRows` retained for UX
///   continuity. Transition is driven by the composed `ReactionScope`'s
///   authoritative `ConnectionStatus` per
///   `EVS-PRD-reaction-widget-contract`-I — NOT by inference from
///   subscription-stream liveness.
///
/// The `Stale` variant is named for what it IS at the rendering layer
/// (a stale-data surface) rather than echoing `reaction`'s transport-
/// layer term `ConnectionStatus.Disconnected`. Picking distinct names
/// keeps consumer code free of `hide`-clause workarounds when both
/// `ViewBuilder` and any `ConnectionStatus`-aware widget are imported
/// in the same library.
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

/// Transport disconnected (the data on screen is now stale). `lastRows`
/// is the most recently rendered row set before the transport dropped,
/// retained so apps can render "stale data with reconnecting banner"
/// rather than blanking.
///
/// `connectionStatus` carries the triggering `ConnectionStatus` (typically
/// `Reconnecting` or `ConnectionStatus.Disconnected`) for the builder's
/// information.
class Stale<T> extends ViewState<T> {
  const Stale(this.lastRows, this.connectionStatus);

  final List<T> lastRows;
  final Object connectionStatus;
}
