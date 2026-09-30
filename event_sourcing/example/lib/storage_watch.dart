import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';

/// Live reads of one pane's storage for the demo's panels, through what the
/// event store hands the application: its [StorageReader] and its event
/// subscription.
class StorageWatch {
  StorageWatch(
    this.eventStore, {
    this.queuePollInterval = const Duration(milliseconds: 250),
  });

  /// The pane's event store.
  final EventStore eventStore;

  /// How often [queue] re-reads a destination's queue. A drain records its
  /// outcomes without appending an event, so the queue is polled.
  final Duration queuePollInterval;

  /// The reads of the pane's storage.
  StorageReader get reader => eventStore.reader;

  /// Every event the store commits after the call, user and system alike.
  Stream<StoredEvent> events() => eventStore
      .subscribe<StoredEvent>(
        const SubscriptionFilter(includeSystemEvents: true),
        const Events(),
      )
      .map(
        (update) => switch (update) {
          Delta<StoredEvent>(:final value) => value,
          _ => throw StateError('an event subscription delivers deltas'),
        },
      );

  /// The rows of [destinationId]'s queue: once when listened to, then each
  /// time they change, read every [queuePollInterval] and after every
  /// committed event.
  Stream<List<FifoEntry>> queue(String destinationId) {
    late final StreamController<List<FifoEntry>> controller;
    Timer? timer;
    StreamSubscription<StoredEvent>? events;
    List<FifoEntry>? last;
    var reading = false;

    Future<void> read() async {
      if (reading) return;
      reading = true;
      try {
        final rows = await reader.listFifoEntries(destinationId);
        if (controller.isClosed) return;
        if (last != null && _sameRows(last!, rows)) return;
        last = rows;
        controller.add(rows);
      } on Object catch (e, st) {
        if (!controller.isClosed) controller.addError(e, st);
      } finally {
        reading = false;
      }
    }

    controller = StreamController<List<FifoEntry>>(
      onListen: () {
        unawaited(read());
        timer = Timer.periodic(queuePollInterval, (_) => unawaited(read()));
        events = this.events().listen((_) => unawaited(read()));
      },
      onCancel: () async {
        timer?.cancel();
        await events?.cancel();
        await controller.close();
      },
    );
    return controller.stream;
  }

  static bool _sameRows(List<FifoEntry> a, List<FifoEntry> b) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
}
