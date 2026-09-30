// Implements: EVS-PRD-reaction-widget-contract/I

import 'package:meta/meta.dart';
import 'package:reaction/reaction.dart' show SubscriptionDenied;

/// View-subscription rendering state exposed by `ViewBuilder`.
///
/// Six variants, exhaustive:
///
/// - [Loading]    — pre-`EndOfReplay`; no rows yet (or isProgressive mode
///   disabled).
/// - [Ready]      — post-`EndOfReplay`; rows are live and current.
/// - [Stale]      — transport disconnected; `lastRows` retained for UX
///   continuity. Transition is driven by the composed `ReactionScope`'s
///   authoritative `ConnectionStatus` per
///   `EVS-PRD-reaction-widget-contract`-I — NOT by inference from
///   subscription-stream liveness.
/// - [Converging] — the subscription was refused because a view it reads
///   is converging; transient, the subscription recovers on its own.
/// - [Rejected]   — the subscription was denied; terminal.
/// - [Errored]    — the subscription failed with any other error; terminal.
///
/// Every error a `ViewBuilder`'s subscription reports lands in one of the
/// last three, so none is left uncaught.
///
/// The variants are named for what they ARE at the rendering layer
/// rather than echoing terms `package:reaction` exports (`Disconnected`,
/// `Denied`, `Failed`). Picking distinct names keeps consumer code free
/// of `hide`-clause workarounds when both `ViewBuilder` and
/// `ConnectionStatus`- or `ActionState`-aware code are imported in the
/// same library.
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

/// The subscription was refused because the view named by [viewName] (the
/// subscribed view, or one the server reads to scope it) is converging
/// after a deploy. Transient: the view source re-issues the subscription
/// without a new request, and the state returns to [Loading] or [Ready]
/// as the recovered subscription's rows arrive. No rows are held: the
/// recovered subscription replays the view from the start.
class Converging<T> extends ViewState<T> {
  const Converging(this.viewName);

  /// The converging view the refusal named.
  final String viewName;
}

/// The subscription was denied (`denial` names the view and the reason).
/// Terminal: no reconnect re-issues a denied subscription, so the state
/// stays until the `ViewBuilder` is rebuilt with a new key.
class Rejected<T> extends ViewState<T> {
  const Rejected(this.denial);

  final SubscriptionDenied denial;
}

/// The subscription failed with [error], one that is neither a converging
/// refusal nor a denial. Terminal, like [Rejected].
class Errored<T> extends ViewState<T> {
  const Errored(this.error, this.stackTrace);

  final Object error;
  final StackTrace stackTrace;
}
