// Implements: EVS-PRD-hash-chain-integrity/A
// the hash is derived from the canonical form of the event's content.
// Implements: EVS-DEV-version-compatibility/J
// the entry-type version and the data-format version are part of that
//   content, so rewriting either breaks the event's hash and the chain.
import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';

/// The hash of one event record: the SHA-256, as lowercase hex, of the JCS
/// canonical form of the record's identity fields -- its id, aggregate id,
/// entry type, entry-type version, data-format version, event type,
/// sequence number, data, initiator, flow token, client timestamp, previous
/// event hash and metadata (provenance included).
///
/// [recordMap] is an event record in its stored shape (`StoredEvent.toMap`);
/// any `event_hash` key in it is ignored. Every append path, the ingest
/// re-stamp and Chain 1 verification derive an event's hash here, so a
/// hash computed at one of them reproduces at every other.
String canonicalEventHash(Map<String, Object?> recordMap) {
  final hashInput = <String, Object?>{
    'event_id': recordMap['event_id'],
    'aggregate_id': recordMap['aggregate_id'],
    'entry_type': recordMap['entry_type'],
    'entry_type_version': recordMap['entry_type_version'],
    'lib_format_version': recordMap['lib_format_version'],
    'event_type': recordMap['event_type'],
    'sequence_number': recordMap['sequence_number'],
    'data': recordMap['data'],
    'initiator': recordMap['initiator'],
    'flow_token': recordMap['flow_token'],
    'client_timestamp': recordMap['client_timestamp'],
    'previous_event_hash': recordMap['previous_event_hash'],
    'metadata': recordMap['metadata'],
  };
  return sha256.convert(canonicalizeBytes(hashInput)).toString();
}
