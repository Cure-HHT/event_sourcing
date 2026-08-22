// Implements: EVS-PRD-view-subscriber/B
// LocalViewSource delegates
// to EventStore.subscribe<T> with AggregateMode<T>.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

import 'package:reaction/src/interfaces/view_source.dart';

/// In-process [ViewSource] impl: delegates to
/// `EventStore.subscribe&lt;T&gt;(filter, AggregateMode(viewName, mapper,
/// aggregates))`. No transport involved.
class LocalViewSource implements ViewSource {
  const LocalViewSource({required this.eventStore});

  final EventStore eventStore;

  @override
  Stream<Update<T>> watch<T>({
    required String viewName,
    required T Function(Map<String, Object?>) mapper,
    SubscriptionFilter? filter,
    Set<String>? aggregates,
  }) {
    return eventStore.subscribe<T>(
      filter ?? const SubscriptionFilter(),
      AggregateMode<T>(
        viewName: viewName,
        mapper: mapper,
        aggregates: aggregates,
      ),
    );
  }
}
