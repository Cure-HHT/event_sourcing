// Implements: EVS-PRD-subscription/A
// (Update<T> sealed envelope carries the
//   Snapshot/EndOfReplay/Delta/Tombstone update shapes for both mode kinds)
// Implements: EVS-PRD-subscription/B
// (Delta and Tombstone variants are the
//   reactive delivery types emitted as new events are ingested)
// Implements: EVS-PRD-subscription/C
// (sequence field on every variant
//   preserves log order; consumers can rely on monotonic sequence numbers)
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

/// Marker emitted by `EventStore.subscribe<T>` (AggregateMode only) after
/// the initial snapshot replay completes, before any live `Delta`/`Tombstone`
/// updates flow. For consumers that need a deterministic "snapshot complete;
/// stream is now live" signal — e.g., to dismiss a loading state, take a
/// resume cursor, or transition UI from skeleton to populated.
///
/// `sequence` is the max sequence reflected in the stream so far (the
/// max across emitted Snapshots and any deltas that arrived during snapshot
/// read and were drained from the buffer), or 0 if both are empty.
///
/// Not emitted by `Events()`-mode subscriptions (no replay phase).
class EndOfReplay<T> extends Update<T> {
  const EndOfReplay({required this.sequence});
  @override
  final int sequence;
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
