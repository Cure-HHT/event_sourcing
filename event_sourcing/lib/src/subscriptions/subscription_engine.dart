// Implements: EVS-PRD-subscription/A
// (events() applies SubscriptionFilter to
//   the broadcast stream; rowChanges() scopes to the requested viewName)
// Implements: EVS-PRD-subscription/B
// (broadcast StreamController publishes
//   immediately after EventStore.append commits; delivery is reactive)
// Implements: EVS-PRD-subscription/C
// (stream emission order mirrors append
//   order; no reordering occurs between publishEvent and listener delivery)
import 'dart:async';

import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';
import 'package:meta/meta.dart' show internal;

/// Internal substrate component owning live broadcast streams of events
/// and projection-row changes. EventStore.append publishes into this
/// engine after the storage transaction commits successfully.
class SubscriptionEngine {
  final StreamController<StoredEvent> _eventBus =
      StreamController<StoredEvent>.broadcast();
  final StreamController<AggregateFoldChange> _rowBus =
      StreamController<AggregateFoldChange>.broadcast();
  final StreamController<String> _caughtUpBus =
      StreamController<String>.broadcast();

  @internal
  void publishEvent(StoredEvent event) => _eventBus.add(event);

  @internal
  void publishRowChange(AggregateFoldChange change) => _rowBus.add(change);

  /// Published, after commit, once a view's copy is found current at the
  /// end of a catch-up transaction (EVS-DEV-converging-view-reads/G): a
  /// signal for a live `AggregateMode` subscription on [viewName] to
  /// re-read the view and redeliver, never a claim about the rows.
  @internal
  void publishViewCaughtUp(String viewName) => _caughtUpBus.add(viewName);

  Stream<Update<StoredEvent>> events(SubscriptionFilter filter) {
    return _eventBus.stream
        .where(filter.matches)
        .map(
          (e) => Delta<StoredEvent>(
            value: e,
            sequence: e.sequenceNumber,
            cause: e.eventType,
          ),
        );
  }

  Stream<AggregateFoldChange> rowChanges(String viewName) =>
      _rowBus.stream.where((c) => c.viewName == viewName);

  /// Fires each time [viewName]'s copy is found current at the end of a
  /// catch-up transaction, while this listener is attached.
  @internal
  Stream<String> viewCaughtUp(String viewName) =>
      _caughtUpBus.stream.where((v) => v == viewName);

  Future<void> close() async {
    await _eventBus.close();
    await _rowBus.close();
    await _caughtUpBus.close();
  }
}
