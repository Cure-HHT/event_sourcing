// A demo destination that delivers each batch to the server's log, so a
// running server visibly drains. A demo fault injection can make its next
// send a permanent refusal, which wedges the queue head and shows the wedge
// path.

import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';

/// The id the demo server registers its [LogDestination] under.
const String logDestinationId = 'server_log';

/// Delivers every batch to [sink] (the server log), one line per batch.
///
/// It receives the demo's own events and the library's system events
/// (`includeSystemEvents: true`), so the log shows a note as it is
/// delivered and, after an operator action, the halt, wedge and recovery
/// events the library appends. [refuseNext] simulates a receiver refusal:
/// the next send returns a permanent refusal, the drainer wedges the queue
/// head, and delivery stays halted until an operator recovers it.
class LogDestination extends Destination {
  LogDestination({required this.sink, this.id = logDestinationId});

  @override
  final String id;

  /// Where delivered batches are written.
  final void Function(String line) sink;

  bool _refuseNext = false;

  /// Makes the next send of this process's destination a permanent refusal
  /// (a demo fault injection). Only the process that drains the database
  /// sends; the demo's route arms it only there.
  void refuseNext() => _refuseNext = true;

  /// Whether the next send is a refusal.
  bool get refusesNext => _refuseNext;

  @override
  SubscriptionFilter get filter => const SubscriptionFilter(
    entryTypes: <String>{
      'help_request',
      'demo_note',
      'green_button_press',
      'blue_button_press',
      'red_alarm',
    },
    includeSystemEvents: true,
  );

  @override
  String get wireFormat => 'demo-log-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.length < 10;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async {
    if (batch.isEmpty) {
      throw ArgumentError.value(batch, 'batch', 'must not be empty');
    }
    final summary = <String, Object?>{
      'events': <Object?>[
        for (final e in batch)
          <String, Object?>{
            'sequence_number': e.sequenceNumber,
            'entry_type': e.entryType,
            'event_type': e.eventType,
            'aggregate_id': e.aggregateId,
          },
      ],
    };
    return WirePayload(
      bytes: Uint8List.fromList(utf8.encode(jsonEncode(summary))),
      contentType: 'application/json',
      transformVersion: 'demo-log-v1',
    );
  }

  @override
  Future<SendResult> send(WirePayload payload) async {
    if (_refuseNext) {
      _refuseNext = false;
      sink('delivery: $id refused a batch (injected fault)');
      return const SendPermanent(error: 'refused by an injected fault');
    }
    sink('delivery: $id <- ${utf8.decode(payload.bytes)}');
    return const SendOk();
  }
}
