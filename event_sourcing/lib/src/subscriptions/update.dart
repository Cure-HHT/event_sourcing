// Implements: EVS-PRD-subscription/A
// (Update<T> sealed envelope carries the
//   Snapshot/EndOfReplay/Pending/Delta/Tombstone update shapes for both
//   mode kinds)
// Implements: EVS-PRD-subscription/B
// (Delta and Tombstone variants are the
//   reactive delivery types emitted as new events are ingested)
// Implements: EVS-PRD-subscription/C
// (sequence field on every variant
//   preserves log order; consumers can rely on monotonic sequence numbers)
import 'package:event_sourcing/src/projections/view_read.dart'
    show ViewConvergenceState;

sealed class Update<T> {
  const Update();
  int get sequence;
}

class Snapshot<T> extends Update<T> {
  const Snapshot({required this.value, required this.sequence});
  final T? value;
  @override
  final int sequence;
}

/// Marker emitted by `EventStore.subscribe<T>` (AggregateMode only) after a
/// replay of the view's rows completes: once after the initial snapshot
/// replay, before any live `Delta`/`Tombstone` update flows, and again
/// whenever a converging copy becomes current, after the subscription
/// redelivers the row of every named aggregate (or every row it would
/// snapshot, when it named none) as a `Snapshot` -- the settled ones
/// unchanged from what they already held, the formerly pending ones
/// replacing their earlier `Pending`. For consumers that need a
/// deterministic "replay complete; stream reflects [state]" signal — e.g.,
/// to dismiss a loading state, take a resume cursor, or transition UI from
/// skeleton to populated.
///
/// `state` is the view's convergence state as of this replay
/// (EVS-DEV-view-convergence): `converging` when the marker ends the
/// initial replay of a copy still catching up, `current` otherwise.
///
/// `sequence` is the max sequence reflected in the stream so far (the max
/// across emitted `Snapshot`s and any deltas that arrived during the replay
/// and were drained from the buffer), or 0 if both are empty.
///
/// Not emitted by `Events()`-mode subscriptions (no replay phase).
// Implements: EVS-DEV-converging-view-reads/E
// Implements: EVS-DEV-converging-view-reads/G
class EndOfReplay<T> extends Update<T> {
  const EndOfReplay({required this.sequence, required this.state});
  @override
  final int sequence;
  final ViewConvergenceState state;
}

/// Delivered by an `AggregateMode` subscription in place of a `Snapshot`,
/// for a named aggregate whose row a converging view's copy cannot yet
/// confirm settled (EVS-DEV-converging-view-reads/E). Distinct from
/// `Snapshot(value: null, ...)`: a `Pending` update carries no claim about
/// the aggregate's row, settled or absent, only that it is not yet known.
///
/// `sequence` is always 0: a pending aggregate's row carries no known
/// sequence to preserve log order by.
class Pending<T> extends Update<T> {
  const Pending({required this.aggregateId});
  final String aggregateId;
  @override
  int get sequence => 0;
}

class Delta<T> extends Update<T> {
  const Delta({
    required this.value,
    required this.sequence,
    required this.cause,
  });
  final T value;
  @override
  final int sequence;
  final String cause;
}

class Tombstone<T> extends Update<T> {
  const Tombstone({required this.aggregateId, required this.sequence});
  final String aggregateId;
  @override
  final int sequence;
}
