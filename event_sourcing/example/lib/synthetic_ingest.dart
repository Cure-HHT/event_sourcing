import 'package:event_sourcing/event_sourcing.dart';
import 'package:uuid/uuid.dart';

/// Helper for the "Ingest sample batch" demo button on `top_action_bar.dart`.
///
/// Builds a minimal, well-formed `esd/batch@2` envelope carrying ONE
/// synthetic event that pretends to come from a different device
/// (`remote-mobile-1`). The resulting envelope is fed to
/// `EventStore.ingestBatch`, which stamps a receiver `ProvenanceEntry`
/// (with `origin_sequence_number` carrying the wire-supplied seq) and
/// reassigns a fresh local `sequence_number`.
///
/// The event is sealed as an originator seals it: its `event_hash` is
/// `canonicalEventHash` of the record, which the receiver recomputes and
/// refuses the event (`IngestChainBroken`) when it differs.
class SyntheticBatchBuilder {
  SyntheticBatchBuilder({
    this.senderHop = 'remote-mobile-1',
    this.senderIdentifier = 'remote-device-uuid-demo',
    this.senderSoftwareVersion = 'remote-diary@1.0.0',
    this.senderDatabaseId = 'remote-database-demo',
  });

  final String senderHop;
  final String senderIdentifier;
  final String senderSoftwareVersion;

  /// The identity of the sender's database, which its provenance entry
  /// names as the database that authored the event.
  final String senderDatabaseId;

  static const _uuid = Uuid();

  /// Construct a one-event `BatchEnvelope` ready for
  /// `eventStore.ingestBatch(envelope.encode(), wireFormat: 'esd/batch@2')`.
  ///
  /// The synthetic event is shaped like a "demo_note" finalized append on
  /// the originator: a single origin `ProvenanceEntry` with
  /// `received_at = now`, the sender's identifier, software version and
  /// database identity, and this library's version,
  /// `sequence_number = originSequenceNumber` (defaults to 1001 — high
  /// enough to be visually distinguishable from local sequence numbers
  /// in the demo), the causal object of the aggregate's first version, and
  /// the canonical hash of the record as its `event_hash`.
  BatchEnvelope buildSingleEventBatch({
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
    // inside `ingestBatch`) parses this back.
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
    return BatchEnvelope(
      batchFormatVersion: BatchEnvelope.currentBatchFormatVersion,
      batchId: 'demo-ingest-${now.millisecondsSinceEpoch}',
      senderHop: senderHop,
      senderIdentifier: senderIdentifier,
      senderSoftwareVersion: senderSoftwareVersion,
      sentAt: now,
      events: <Map<String, Object?>>[eventMap],
    );
  }
}
