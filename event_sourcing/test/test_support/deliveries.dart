// Test support for the two ways a test puts received events into an event
// store: a native delivery presented to the store's receiver endpoint, on a
// channel whose numbers and links this file keeps per receiving store; and
// the ingest seam, which handles one event outside any delivery, for tests
// whose subject is how the ingest path handles a single record.
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    as internal
    show ingestEventForTest;
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show SenderSuccessionData;

/// Handles [event] as the ingest path handles one received record, in a
/// transaction of its own and outside any delivery. The seam for tests of
/// per-record ingest handling; production code admits events only in
/// deliveries (`EVS-PRD-ingest/G`).
Future<PerEventIngestOutcome> ingestEventForTest(
  EventStore store,
  StoredEvent event,
) => internal.ingestEventForTest(store, event);

/// The channel a test delivery travels on: from [senderDatabaseId] to the
/// destination [destinationId] of registration [registrationId], at
/// [generation].
DeliveryChannel testChannel(
  String senderDatabaseId, {
  String destinationId = 'test-destination',
  String registrationId = 'test-registration',
  int generation = 1,
}) => DeliveryChannel(
  senderDatabaseId: senderDatabaseId,
  destinationId: destinationId,
  registrationId: registrationId,
  generation: generation,
);

/// One delivery a test presented, with the receiver's answer.
final class TestDelivery {
  const TestDelivery({
    required this.envelope,
    required this.bytes,
    required this.response,
  });

  /// The delivery as sealed.
  final DeliveryEnvelope envelope;

  /// The bytes presented to the endpoint.
  final Uint8List bytes;

  /// The receiver's answer.
  final ReceiverResponse response;

  /// Whether the receiver accepted the delivery (not a re-presentation).
  bool get accepted =>
      response is ReceiverAcknowledgement &&
      (response as ReceiverAcknowledgement).outcome ==
          AcknowledgementOutcome.accepted;
}

/// The sender's record of each channel, per receiving store: the number and
/// hash of the last delivery the receiver acknowledged on it.
final Expando<Map<DeliveryChannel, DeliveryRecord>> _records =
    Expando<Map<DeliveryChannel, DeliveryRecord>>('test delivery records');

/// The database identity [record]'s originator provenance entry names.
String _originatorOf(Map<String, Object?> record) {
  final metadata = record['metadata'];
  final provenance = metadata is Map ? metadata['provenance'] : null;
  if (provenance is List && provenance.isNotEmpty) {
    final first = provenance.first;
    final id = first is Map ? first['database_id'] : null;
    if (id is String) return id;
  }
  throw ArgumentError.value(
    record['event_id'],
    'record',
    'names no originating database; pass a channel',
  );
}

/// Seals [records] as the next delivery on [channel] (by default the
/// channel from the first record's originating database) to [store] and
/// presents it to the store's receiver endpoint, authenticated for
/// [senderDatabaseIds] (by default the channel's sender).
///
/// The delivery is numbered one above the last delivery [store]
/// acknowledged on the channel through this helper and linked to its hash;
/// an acknowledgement moves that record to the one the receiver returns.
/// [number] and [link] override the numbering, for a test of an
/// out-of-sequence delivery.
Future<TestDelivery> deliverTo(
  EventStore store,
  List<Map<String, Object?>> records, {
  DeliveryChannel? channel,
  Set<String>? senderDatabaseIds,
  Map<String, Object?> attributes = const <String, Object?>{},
  int? number,
  String? link,
  String? batchId,
}) async {
  final c = channel ?? testChannel(_originatorOf(records.first));
  final byChannel = _records[store] ??= <DeliveryChannel, DeliveryRecord>{};
  final last = byChannel[c] ?? DeliveryRecord.none;
  final deliveryNumber = number ?? last.deliveryNumber + 1;
  final envelope = DeliveryEnvelope.seal(
    batchId:
        batchId ??
        'test-delivery-${c.senderDatabaseId}-${c.generation}-$deliveryNumber',
    senderHop: 'test-sender-hop',
    senderIdentifier: 'test-sender',
    senderSoftwareVersion: 'test-sender@1.0.0',
    sentAt: DateTime.utc(2026, 9, 1, 12),
    channel: c,
    deliveryNumber: deliveryNumber,
    previousDeliveryHash: number == null && link == null
        ? last.deliveryHash
        : link,
    events: records,
    attributes: attributes,
  );
  final bytes = envelope.encode();
  final response = await store.receiverEndpoint.accept(
    bytes,
    senderDatabaseIds: senderDatabaseIds ?? _defaultSenderIds(c, records),
  );
  if (response is ReceiverAcknowledgement) byChannel[c] = response.record;
  return TestDelivery(envelope: envelope, bytes: bytes, response: response);
}

/// The sender identities [deliverTo] authenticates a delivery for when the
/// caller names none: the channel's sender, plus, for a succession-event
/// record among [records], the identities it names as successor and
/// predecessor, so a test that hands a succession record to a helper that
/// defaults its authentication is not refused for the predecessor it
/// carries but does not otherwise mention.
Set<String> _defaultSenderIds(
  DeliveryChannel channel,
  List<Map<String, Object?>> records,
) {
  final ids = <String>{channel.senderDatabaseId};
  for (final record in records) {
    if (record['entry_type'] != kDestinationSenderSucceededEntryType) {
      continue;
    }
    final data = record['data'];
    if (data is! Map<String, Object?>) continue;
    try {
      final succession = SenderSuccessionData.fromJson(data);
      ids
        ..add(succession.databaseId)
        ..add(succession.predecessorDatabaseId);
    } on FormatException {
      // Left to ingest's own malformed-record handling.
    }
  }
  return ids;
}

/// [deliverTo] for events: delivers each event's stored record.
Future<TestDelivery> deliverEventsTo(
  EventStore store,
  List<StoredEvent> events, {
  DeliveryChannel? channel,
  Set<String>? senderDatabaseIds,
}) => deliverTo(
  store,
  <Map<String, Object?>>[
    for (final e in events) Map<String, Object?>.from(e.toMap()),
  ],
  channel: channel,
  senderDatabaseIds: senderDatabaseIds,
);

/// The batch context the last provenance entry of [event] carries, or null.
BatchContext? _batchContextOf(StoredEvent event) {
  final provenance = event.metadata['provenance'];
  if (provenance is! List || provenance.isEmpty) return null;
  final last = provenance.last;
  final context = last is Map ? last['batch_context'] : null;
  return context is Map
      ? BatchContext.fromJson(Map<String, Object?>.from(context))
      : null;
}

/// Whether [value], a finding's evidence or a part of it, carries one of
/// [names] anywhere.
bool _carries(Object? value, Set<Object?> names) => switch (value) {
  final String s => names.contains(s),
  final Map<Object?, Object?> m => m.values.any((v) => _carries(v, names)),
  final List<Object?> l => l.any((v) => _carries(v, names)),
  _ => false,
};

/// Whether [finding] is about [record]: its evidence carries the record's
/// identifier or event hash, or its aggregates name the record's aggregate.
bool _isAbout(Map<String, Object?> finding, Map<String, Object?> record) =>
    _carries(finding['evidence'], <Object?>{
      record['event_id'],
      record['event_hash'],
    }) ||
    ((finding['aggregates'] as List?)?.contains(record['aggregate_id']) ??
        false);

/// The outcome of each record of [delivery] as [store]'s log records it,
/// in the delivery's order:
///
/// - [IngestOutcome.duplicate]: a `ingest.duplicate_received` audit whose
///   batch context names the delivery at the record's position;
/// - [IngestOutcome.ingested]: an event with the record's identifier whose
///   last provenance entry's batch context names the delivery at that
///   position, or [IngestOutcome.ingestedWithFinding] when a security
///   finding this database authored is about the record (its evidence
///   carries the record's identifier or event hash, or its aggregates name
///   the record's aggregate);
/// - [IngestOutcome.keptInFinding]: neither.
///
/// Findings are matched against all [store] holds, so a test that needs
/// the findings of one delivery compares counts before and after it.
Future<List<IngestOutcome>> recordOutcomes(
  EventStore store,
  TestDelivery delivery,
) async {
  final batchId = delivery.envelope.batchId;
  final duplicates = <int>{
    for (final e in await store.reader.findAllEvents(entryType: 'ingest-audit'))
      if (e.eventType == 'ingest.duplicate_received')
        if (_batchContextOf(e) case final c? when c.batchId == batchId)
          c.batchPosition,
  };
  final findings = <Map<String, Object?>>[
    for (final e in await store.reader.findAllEvents(
      entryType: kSecurityFindingEntryType,
    ))
      if ((e.metadata['provenance']! as List).length == 1)
        Map<String, Object?>.from(e.data),
  ];
  final records = delivery.envelope.events;
  final outcomes = <IngestOutcome>[];
  for (var i = 0; i < records.length; i++) {
    if (duplicates.contains(i)) {
      outcomes.add(IngestOutcome.duplicate);
      continue;
    }
    final id = records[i]['event_id'];
    final held = id is String ? await store.reader.findEventById(id) : null;
    final context = held == null ? null : _batchContextOf(held);
    if (context != null &&
        context.batchId == batchId &&
        context.batchPosition == i) {
      outcomes.add(
        findings.any((f) => _isAbout(f, records[i]))
            ? IngestOutcome.ingestedWithFinding
            : IngestOutcome.ingested,
      );
    } else {
      outcomes.add(IngestOutcome.keptInFinding);
    }
  }
  return outcomes;
}

/// Delivers [records] to [store] in order, each run of consecutive records
/// of one originating database as one delivery on that database's
/// channel, so that no record is foreign to its channel unless a relay's
/// entry follows its originator's. Returns the deliveries in order.
Future<List<TestDelivery>> deliverByOriginator(
  EventStore store,
  List<Map<String, Object?>> records,
) async {
  final deliveries = <TestDelivery>[];
  var run = <Map<String, Object?>>[];
  String? runOriginator;
  for (final record in records) {
    final originator = _originatorOf(record);
    if (run.isNotEmpty && originator != runOriginator) {
      deliveries.add(await deliverTo(store, run));
      run = <Map<String, Object?>>[];
    }
    runOriginator = originator;
    run.add(record);
  }
  if (run.isNotEmpty) deliveries.add(await deliverTo(store, run));
  return deliveries;
}

/// Thrown by [deliverEventsOrThrow] when the receiver refuses the delivery.
final class TestDeliveryRefused implements Exception {
  const TestDeliveryRefused(this.refusal);

  /// The receiver's refusal.
  final ReceiverRefusal refusal;

  @override
  String toString() =>
      'TestDeliveryRefused(${refusal.refusal.wire}, ${refusal.reason}, '
      '${refusal.refusedEventId})';
}

/// [deliverEventsTo], throwing [TestDeliveryRefused] when the receiver
/// refuses the delivery; for a test that runs one scenario over both
/// ingest paths and expects a refusal from each.
Future<TestDelivery> deliverEventsOrThrow(
  EventStore store,
  List<StoredEvent> events, {
  DeliveryChannel? channel,
}) async {
  final delivery = await deliverEventsTo(store, events, channel: channel);
  final response = delivery.response;
  if (response is ReceiverRefusal) throw TestDeliveryRefused(response);
  return delivery;
}
