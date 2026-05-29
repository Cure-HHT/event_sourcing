// Implements: EVS-PRD-ingest/A — ingest path existence (wire-format codec)
// Implements: EVS-PRD-ingest/B — upstream identity preserved (BatchEnvelope
//   carries originator events verbatim; receiver adds its own provenance hop
//   inside ingestBatch, not here)
// Implements: EVS-PRD-hash-chain-integrity/D — encoding uses JCS canonical
//   form so hash values are reproducible across observers

import 'dart:convert';
import 'dart:typed_data';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:event_sourcing/src/ingest/ingest_errors.dart';

/// The library's canonical batch envelope. Exactly one format version is
/// supported: `"1"` (identifier `"esd/batch@1"`).
class BatchEnvelope {
  const BatchEnvelope({
    required this.batchFormatVersion,
    required this.batchId,
    required this.senderHop,
    required this.senderIdentifier,
    required this.senderSoftwareVersion,
    required this.sentAt,
    required this.events,
  });

  /// Parse wire bytes as a canonical envelope. Throws [IngestDecodeFailure]
  /// on any malformedness.
  factory BatchEnvelope.decode(Uint8List bytes) {
    final Object? decoded;
    try {
      decoded = jsonDecode(utf8.decode(bytes));
    } catch (e) {
      throw IngestDecodeFailure('not valid UTF-8 JSON: $e');
    }
    if (decoded is! Map<String, Object?>) {
      throw const IngestDecodeFailure('envelope must be a JSON object');
    }
    final versionRaw = decoded['batch_format_version'];
    if (versionRaw != '1') {
      throw IngestDecodeFailure(
        'unsupported batch_format_version: got ${versionRaw ?? "(missing)"}; '
        'expected "1"',
      );
    }
    final version = versionRaw as String;
    final batchId = _requireString(decoded, 'batch_id');
    final senderHop = _requireString(decoded, 'sender_hop');
    final senderIdentifier = _requireString(decoded, 'sender_identifier');
    final senderSoftwareVersion = _requireString(
      decoded,
      'sender_software_version',
    );
    final sentAtStr = _requireString(decoded, 'sent_at');
    final DateTime sentAt;
    try {
      sentAt = DateTime.parse(sentAtStr);
    } catch (e) {
      throw IngestDecodeFailure('sent_at not parseable: $e');
    }
    final eventsRaw = decoded['events'];
    if (eventsRaw is! List) {
      throw const IngestDecodeFailure('events must be a JSON array');
    }
    final events = <Map<String, Object?>>[];
    for (var i = 0; i < eventsRaw.length; i++) {
      final e = eventsRaw[i];
      if (e is! Map<String, Object?>) {
        throw IngestDecodeFailure('events[$i] must be a JSON object');
      }
      events.add(Map<String, Object?>.from(e));
    }
    return BatchEnvelope(
      batchFormatVersion: version,
      batchId: batchId,
      senderHop: senderHop,
      senderIdentifier: senderIdentifier,
      senderSoftwareVersion: senderSoftwareVersion,
      sentAt: sentAt,
      events: events,
    );
  }

  /// Canonical identifier for this format.
  static const String wireFormat = 'esd/batch@1';

  /// Canonical `batch_format_version` value carried inside an
  /// `esd/batch@1` envelope. Held as a static constant (distinct name
  /// from the instance field [BatchEnvelope.batchFormatVersion]) so
  /// callers minting a fresh envelope (e.g. `fillBatch` building a
  /// native `BatchEnvelopeMetadata`) and the decoder share one source
  /// of truth for the version string.
  static const String currentBatchFormatVersion = '1';

  final String batchFormatVersion;
  final String batchId;
  final String senderHop;
  final String senderIdentifier;
  final String senderSoftwareVersion;
  final DateTime sentAt;

  /// Raw StoredEvent JSON. Callers decode each map into `StoredEvent` using
  /// `StoredEvent.fromMap` inside the ingest flow.
  final List<Map<String, Object?>> events;

  /// JCS-canonicalize this envelope into wire bytes.
  Uint8List encode() {
    final map = <String, Object?>{
      'batch_format_version': batchFormatVersion,
      'batch_id': batchId,
      'sender_hop': senderHop,
      'sender_identifier': senderIdentifier,
      'sender_software_version': senderSoftwareVersion,
      'sent_at': sentAt.toIso8601String(),
      'events': events,
    };
    return Uint8List.fromList(canonicalizeBytes(map));
  }
}

String _requireString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw IngestDecodeFailure('missing or non-string "$key"');
  }
  return value;
}
