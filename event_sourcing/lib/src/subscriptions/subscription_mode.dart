import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class SubscriptionMode<T> {
  const SubscriptionMode();
}

class Events extends SubscriptionMode<StoredEvent> {
  const Events();
}

class AggregateMode<T> extends SubscriptionMode<T> {
  final String viewName;
  final T Function(Map<String, Object?>) mapper;

  /// Optional allow-list of aggregate IDs. When non-null, only snapshots and
  /// deltas for these aggregate IDs are delivered. When null, all aggregates
  /// in [viewName] are included.
  ///
  /// Consumed by `EventStore._subscribeAggregate` when building the initial
  /// snapshot and filtering live deltas. Not consulted by
  /// `SubscriptionFilter.matches`.
  final Set<String>? aggregates;

  const AggregateMode({
    required this.viewName,
    required this.mapper,
    this.aggregates,
  });
}
