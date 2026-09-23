import 'package:event_sourcing/event_sourcing.dart';

/// Reaches the raw sembast transaction inside a library transaction.
Future<void> unwrapInside(EventStore store, SembastBackend backend) =>
    store.runTransaction((txn, collector) async {
      backend.unwrapSembastTxn(txn);
    });
