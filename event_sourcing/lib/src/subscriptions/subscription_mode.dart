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
  const AggregateMode({required this.viewName, required this.mapper});
}
