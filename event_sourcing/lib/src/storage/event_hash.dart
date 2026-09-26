// Implements: EVS-PRD-hash-chain-integrity/A
// the hash is derived from the canonical form of the event's content.
// Implements: EVS-DEV-version-compatibility/J
// the entry-type version and the data-format version are part of that
//   content, so rewriting either breaks the event's hash and the chain.
// Implements: EVS-DEV-event-record/K
// the event's causal object is part of that content.
// Implements: EVS-DEV-event-record/I
// the library_version of every provenance entry is part of that content,
//   in the metadata the hash covers.
import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';

/// The hash of one event record: the SHA-256, as lowercase hex, of the JCS
/// canonical form of the record's identity fields -- its id, aggregate id,
/// entry type, entry-type version, data-format version, event type,
/// sequence number, data, initiator, flow token, client timestamp, previous
/// event hash, causal object and metadata (provenance included).
///
/// [recordMap] is an event record in its stored shape (`StoredEvent.toMap`);
/// any `event_hash` key in it is ignored, and so is `aggregate_type`, which
/// the hash does not cover. Each field is hashed as the record holds it:
/// `client_timestamp` as its string, `initiator` and `causal` as their maps
/// with every key they carry; a record without `causal` hashes it as null.
/// Every append path, the ingest re-stamp and Chain 1 verification derive
/// an event's hash here, and `StoredEvent.fromMap`
/// keeps every hashed field as the record spelled it, so a hash computed at
/// one of them reproduces at every other. A sender that builds records by
/// hand seals each one here after its last change; ingest recomputes the
/// hash over the record exactly as it arrived.
///
/// The hash is an unkeyed SHA-256: it detects a change made without
/// recomputing the hash, not one whose author recomputes it.
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
    'causal': recordMap['causal'],
    'metadata': recordMap['metadata'],
  };
  return sha256.convert(canonicalizeBytes(hashInput)).toString();
}
