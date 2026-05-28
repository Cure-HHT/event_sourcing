// Implements: EVS-PRD-reaction-scope/B

/// Transport-connection liveness as observed by a [ReactionScope].
///
/// Three variants, exhaustive:
///
/// - [Connected]    — transport is up; subscriptions are live.
/// - [Reconnecting] — transport dropped; client is attempting to
///                    reconnect (subscriptions stalled until success).
/// - [Disconnected] — transport is down and the reconnect policy has
///                    given up; consumer should surface an actionable
///                    error.
///
/// [LocalScope] reports [Connected] for its entire lifetime
/// (in-process has no transport to lose). [RemoteScope] drives
/// transitions from the underlying WS lifecycle.
sealed class ConnectionStatus {
  const ConnectionStatus();
}

class Connected extends ConnectionStatus {
  const Connected();
  @override
  bool operator ==(Object other) => other is Connected;
  @override
  int get hashCode => (Connected).hashCode;
  @override
  String toString() => 'ConnectionStatus.Connected';
}

class Reconnecting extends ConnectionStatus {
  const Reconnecting();
  @override
  bool operator ==(Object other) => other is Reconnecting;
  @override
  int get hashCode => (Reconnecting).hashCode;
  @override
  String toString() => 'ConnectionStatus.Reconnecting';
}

class Disconnected extends ConnectionStatus {
  const Disconnected();
  @override
  bool operator ==(Object other) => other is Disconnected;
  @override
  int get hashCode => (Disconnected).hashCode;
  @override
  String toString() => 'ConnectionStatus.Disconnected';
}
