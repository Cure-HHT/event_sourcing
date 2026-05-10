import 'dart:async';

import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/subscriptions/update.dart';

/// Internal substrate component owning live broadcast streams of events
/// and projection-row changes. EventStore.append publishes into this
/// engine after the storage transaction commits successfully.
class SubscriptionEngine {
  final StreamController<StoredEvent> _eventBus =
      StreamController<StoredEvent>.broadcast();
  final StreamController<RowChange> _rowBus =
      StreamController<RowChange>.broadcast();

  void publishEvent(StoredEvent event) => _eventBus.add(event);

  void publishRowChange({
    required String viewName,
    required String aggregateId,
    required Map<String, Object?>? value, // null = removed/tombstoned
    required int sequence,
    required String cause,
    required bool isTombstone,
  }) {
    _rowBus.add(
      RowChange(
        viewName: viewName,
        aggregateId: aggregateId,
        value: value,
        sequence: sequence,
        cause: cause,
        isTombstone: isTombstone,
      ),
    );
  }

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

  Stream<RowChange> rowChanges(String viewName) =>
      _rowBus.stream.where((c) => c.viewName == viewName);

  Future<void> close() async {
    await _eventBus.close();
    await _rowBus.close();
  }
}

class RowChange {
  final String viewName;
  final String aggregateId;
  final Map<String, Object?>? value;
  final int sequence;
  final String cause;
  final bool isTombstone;
  RowChange({
    required this.viewName,
    required this.aggregateId,
    required this.value,
    required this.sequence,
    required this.cause,
    required this.isTombstone,
  });
}
