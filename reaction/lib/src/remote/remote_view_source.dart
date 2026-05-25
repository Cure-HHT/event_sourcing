// Implements: EVS-PRD-view-subscriber/C — RemoteViewSource consumes the
//   cross-process wire (via RemoteConnection.openSubscription) and
//   applies the consumer-supplied row mapper client-side, mapping
//   incoming Update<Map<String, Object?>> envelopes to Update<T>.
// Implements: EVS-PRD-cross-process-event-transport/A — drives Update<T>
//   envelope codec round-trip through the WS.
// Implements: EVS-PRD-cross-process-event-transport/B — each opened
//   subscription is keyed by a client-chosen UUID v4 subscriptionId,
//   carried on every envelope.
// Implements: EVS-PRD-cross-process-event-transport/D — multiple
//   concurrent watch() calls share one RemoteConnection's WS.
// Implements: EVS-PRD-cross-process-event-transport/G — no server-side
//   mapping; the wire ships Map<String, Object?> rows and the consumer
//   applies their mapper here, preserving Layer-2 invariance with
//   LocalViewSource.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:reaction/src/interfaces/view_source.dart';
import 'package:reaction/src/remote/remote_connection.dart';
import 'package:uuid/uuid.dart';

class RemoteViewSource implements ViewSource {
  RemoteViewSource({required this.connection}) : _uuid = const Uuid();

  final RemoteConnection connection;
  final Uuid _uuid;

  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    final subscriptionId = _uuid.v4();
    return connection
        .openSubscription(
          subscriptionId: subscriptionId,
          viewName: viewName,
          filter: filter,
          aggregates: aggregates,
        )
        .map(_mapUpdate<T>(mapper));
  }

  Update<T> Function(Update<Map<String, Object?>>) _mapUpdate<T>(
    T Function(Map<String, Object?>) mapper,
  ) {
    return (u) => switch (u) {
      Snapshot<Map<String, Object?>>() => Snapshot<T>(
        // Snapshot.value is nullable on the substrate; preserve null.
        value: u.value == null ? null : mapper(u.value!),
        sequence: u.sequence,
      ),
      Delta<Map<String, Object?>>() => Delta<T>(
        value: mapper(u.value),
        sequence: u.sequence,
        cause: u.cause,
      ),
      Tombstone<Map<String, Object?>>() => Tombstone<T>(
        aggregateId: u.aggregateId,
        sequence: u.sequence,
      ),
      EndOfReplay<Map<String, Object?>>() => EndOfReplay<T>(
        sequence: u.sequence,
      ),
    };
  }
}
