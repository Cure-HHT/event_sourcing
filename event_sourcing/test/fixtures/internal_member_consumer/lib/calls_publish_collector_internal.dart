import 'package:event_sourcing/event_sourcing.dart';

/// Publishes through the collector a transaction body receives.
Future<void> publishDirectly(EventStore store, StoredEvent event) =>
    store.runTransaction((txn, collector) async {
      collector
        ..add(event)
        ..addRowChanges(const []);
    });
