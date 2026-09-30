import 'package:event_sourcing/event_sourcing.dart';
import 'package:uuid/uuid.dart';

/// Helper for the "Ingest sample batch" demo button on `top_action_bar.dart`.
///
/// Builds synthetic events that pretend to come from a different device
/// (`remote-mobile-1`), and delivers them as native deliveries
/// (`esd/batch@3`) on that device's delivery channel to an event store's
/// [EventStore.receiverEndpoint], numbering each delivery one above the
/// last one the receiver acknowledged and linking it to its hash. The
/// receiver stamps a receiver `ProvenanceEntry` (with
/// `origin_sequence_number` carrying the wire-supplied seq) and reassigns
/// a fresh local `sequence_number`.
///
/// The event is sealed as an originator seals it: its `event_hash` is
/// `canonicalEventHash` of the record, which the receiver recomputes; when
/// it differs, the receiver stores the event as received and records a
/// `hash_mismatch` security finding about it.
class SyntheticSender {
  SyntheticSender({
    this.senderHop = 'remote-mobile-1',
    this.senderIdentifier = 'remote-device-uuid-demo',
    this.senderSoftwareVersion = 'remote-diary@1.0.0',
    this.senderDatabaseId = 'remote-database-demo',
    String? registrationId,
  }) : registrationId = registrationId ?? 'synthetic-${_uuid.v4()}';

  final String senderHop;
  final String senderIdentifier;
  final String senderSoftwareVersion;

  /// The identity of the sender's database, which its provenance entry
  /// names as the database that authored the event.
  final String senderDatabaseId;

  /// The registration of the sender's channel: by default one of its own,
  /// so that each sender starts at delivery 1 on a channel no receiver has
  /// seen, whatever an earlier sender delivered to the same receiver.
  final String registrationId;

  static const _uuid = Uuid();

  /// The delivery channel the synthetic sender delivers on.
  DeliveryChannel get channel => DeliveryChannel(
    senderDatabaseId: senderDatabaseId,
    destinationId: 'synthetic',
    registrationId: registrationId,
    generation: 1,
  );

  /// The receiver's record of [channel], as its last acknowledgement
  /// returned it.
  DeliveryRecord _record = DeliveryRecord.none;

  /// Seals [events] as the next delivery on [channel]: numbered one above
  /// the receiver's last acknowledged record and linked to its hash.
  DeliveryEnvelope nextDelivery(List<Map<String, Object?>> events) {
    final now = DateTime.now().toUtc();
    return DeliveryEnvelope.seal(
      batchId: 'demo-ingest-${now.microsecondsSinceEpoch}',
      senderHop: senderHop,
      senderIdentifier: senderIdentifier,
      senderSoftwareVersion: senderSoftwareVersion,
      sentAt: now,
      channel: channel,
      deliveryNumber: _record.deliveryNumber + 1,
      previousDeliveryHash: _record.deliveryHash,
      events: events,
    );
  }

  /// Delivers one synthetic event ([buildEvent]) to [store]'s receiver
  /// endpoint as the next delivery on [channel], and returns the
  /// receiver's answer. An acknowledgement moves the sender's copy of the
  /// receiver's record to the one it carries.
  Future<ReceiverResponse> deliverOne(EventStore store) async {
    final delivery = nextDelivery(<Map<String, Object?>>[buildEvent()]);
    final answer = await store.receiverEndpoint.accept(
      delivery.encode(),
      senderDatabaseIds: <String>{senderDatabaseId},
    );
    if (answer is ReceiverAcknowledgement) _record = answer.record;
    return answer;
  }

  /// Construct one synthetic event record as its originator seals it.
  ///
  /// The synthetic event is shaped like a "demo_note" finalized append on
  /// the originator: a single origin `ProvenanceEntry` with
  /// `received_at = now`, the sender's identifier, software version and
  /// database identity, and this library's version,
  /// `sequence_number = originSequenceNumber` (defaults to 1001 — high
  /// enough to be visually distinguishable from local sequence numbers
  /// in the demo), the causal object of the aggregate's first version, and
  /// the canonical hash of the record as its `event_hash`.
  Map<String, Object?> buildEvent({
    int originSequenceNumber = 1001,
    String aggregateId = 'remote-aggregate-1',
    String entryType = 'demo_note',
    String aggregateType = 'Note',
    String userId = 'remote-user-1',
    Map<String, Object?>? answers,
  }) {
    final now = DateTime.now().toUtc();
    // Build the origin provenance entry as raw JSON. The library
    // re-exports `BatchContext` but not `ProvenanceEntry`; rather than
    // pull `package:provenance` in as a direct dep on the example
    // (just to round-trip a six-field map), the helper writes the
    // snake_case shape inline. `ProvenanceEntry.fromJson` (called
    // by the receiver) parses this back.
    final originEntry = <String, Object?>{
      'hop': senderHop,
      'received_at': now.toIso8601String(),
      'identifier': senderIdentifier,
      'software_version': senderSoftwareVersion,
      'database_id': senderDatabaseId,
      'library_version': LibVersion.version,
    };
    final eventId = _uuid.v4();
    final eventMap = <String, Object?>{
      'event_id': eventId,
      'aggregate_id': aggregateId,
      'aggregate_type': aggregateType,
      'entry_type': entryType,
      'entry_type_version': const EntryTypeVersion(1, 0).toJson(),
      'lib_format_version': LibVersion.dataFormat.toJson(),
      'event_type': 'finalized',
      'sequence_number': originSequenceNumber,
      'data': <String, Object?>{
        'answers':
            answers ??
            <String, Object?>{
              'title': 'remote note',
              'body': 'ingested from $senderHop at ${now.toIso8601String()}',
              'date': now.toIso8601String(),
            },
      },
      'metadata': <String, Object?>{
        'change_reason': 'initial',
        'provenance': <Map<String, Object?>>[originEntry],
      },
      'initiator': UserInitiator(userId).toJson(),
      'flow_token': null,
      'client_timestamp': now.toIso8601String(),
      'previous_event_hash': null,
      'causal': CausalRecord(
        kind: CausalKind.version,
        eligible: true,
        parents: const <CausalRef>[],
      ).toJson(),
    };
    eventMap['event_hash'] = canonicalEventHash(eventMap);
    return eventMap;
  }
}
