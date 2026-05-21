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
