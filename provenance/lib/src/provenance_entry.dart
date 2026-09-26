// Implements: EVS-PRD-provenance/A
// (immutable ProvenanceEntry value
//   type with hop, receivedAt, identifier, softwareVersion, transformVersion
//   and optional ingest/receiver fields)
// Implements: EVS-PRD-provenance/C
// (JSON serialization and
//   deserialization without loss of information)
// Implements: EVS-PRD-provenance/E
// (a decoded entry re-encodes exactly its decoded keys and values)

import 'package:provenance/src/batch_context.dart';
import 'package:provenance/src/iso8601_instant.dart';
import 'package:provenance/src/provenance_delivery.dart';

/// One hop's attribution in a cross-system event's chain-of-custody.
///
/// Each hop that receives the event (originating device, relay server,
/// controlling server) appends one `ProvenanceEntry` to
/// `event.metadata.provenance`. The class is pure data; the
/// append-and-don't-mutate invariants live in the `appendHop()` helper.
///
/// `receivedAt` is an instant; `fromJson` reads `received_at` with
/// [parseIso8601Instant], refusing a timestamp without an explicit offset,
/// outside the four-digit years or with a calendar field out of range, so
/// every host reads the same instant from it and the ALCOA+
/// *Contemporaneous* guarantee holds across the audit chain.
///
/// `identifier` and `softwareVersion` shape rules (-D, -E) are
/// **permanent caller obligations**, not deferred validation: the source of
/// each hop — originator, relay, controller — is the only place that
/// knows which shape applies, so there is no hop-ingress validator that can
/// take ownership. The type documents the contract; callers construct
/// conforming values.
///
// received_at, identifier, software_version, and optional transform_version.
// received_at timestamp form enforced at the JSON boundary;
// identifier and software_version shapes are caller obligations by design.
// arrival_hash, previous_ingest_hash, ingest_sequence_number, batch_context,
// origin_sequence_number, delivery.
class ProvenanceEntry {
  const ProvenanceEntry({
    required this.hop,
    required this.receivedAt,
    required this.identifier,
    required this.softwareVersion,
    this.transformVersion,
    this.arrivalHash,
    this.previousIngestHash,
    this.ingestSequenceNumber,
    this.batchContext,
    this.originSequenceNumber,
    this.libraryVersion,
    this.databaseId,
    this.delivery,
  }) : _source = null;

  const ProvenanceEntry._decoded({
    required this.hop,
    required this.receivedAt,
    required this.identifier,
    required this.softwareVersion,
    required this.transformVersion,
    required this.arrivalHash,
    required this.previousIngestHash,
    required this.ingestSequenceNumber,
    required this.batchContext,
    required this.originSequenceNumber,
    required this.libraryVersion,
    required this.databaseId,
    required this.delivery,
    required Map<String, Object?> source,
  }) : _source = source;

  // missing any required field, with wrong types, or with a received_at that
  // parseIso8601Instant refuses. An offsetless string would be read as local
  // time, and an out-of-range field rolled over to another instant, breaking
  // the ALCOA+ Contemporaneous guarantee in a cross-system audit chain.
  // Implements: EVS-DEV-event-record/C
  // received_at is read in the shared timestamp form, and refused naming
  //   the field otherwise.
  factory ProvenanceEntry.fromJson(Map<String, Object?> json) {
    final hop = _requireString(json, 'hop');
    final receivedAtRaw = _requireString(json, 'received_at');
    final identifier = _requireString(json, 'identifier');
    final softwareVersion = _requireString(json, 'software_version');
    final transformVersionRaw = json['transform_version'];
    if (transformVersionRaw != null && transformVersionRaw is! String) {
      throw const FormatException(
        'ProvenanceEntry: "transform_version" must be a String when present',
      );
    }
    final DateTime receivedAt;
    try {
      receivedAt = parseIso8601Instant(receivedAtRaw);
    } on FormatException catch (e) {
      throw FormatException(
        'ProvenanceEntry: "received_at" is not an ISO 8601 date-time with a '
        'four-digit year, calendar fields in range and an explicit offset: '
        '${e.message}',
      );
    }
    final arrivalHash = _optionalString(json, 'arrival_hash');
    final previousIngestHash = _optionalString(json, 'previous_ingest_hash');
    final ingestSequenceNumber = _optionalInt(json, 'ingest_sequence_number');
    final originSequenceNumber = _optionalInt(json, 'origin_sequence_number');
    final libraryVersion = _optionalString(json, 'library_version');
    final databaseId = _optionalString(json, 'database_id');
    final batchContextRaw = json['batch_context'];
    BatchContext? batchContext;
    if (batchContextRaw != null) {
      if (batchContextRaw is! Map<String, Object?>) {
        throw const FormatException(
          'ProvenanceEntry: "batch_context" must be an object when present',
        );
      }
      batchContext = BatchContext.fromJson(batchContextRaw);
    }
    final deliveryRaw = json['delivery'];
    ProvenanceDelivery? delivery;
    if (deliveryRaw != null) {
      if (deliveryRaw is! Map<String, Object?>) {
        throw const FormatException(
          'ProvenanceEntry: "delivery" must be an object when present',
        );
      }
      delivery = ProvenanceDelivery.fromJson(deliveryRaw);
    }
    return ProvenanceEntry._decoded(
      hop: hop,
      receivedAt: receivedAt,
      identifier: identifier,
      softwareVersion: softwareVersion,
      transformVersion: transformVersionRaw as String?,
      arrivalHash: arrivalHash,
      previousIngestHash: previousIngestHash,
      ingestSequenceNumber: ingestSequenceNumber,
      batchContext: batchContext,
      originSequenceNumber: originSequenceNumber,
      libraryVersion: libraryVersion,
      databaseId: databaseId,
      delivery: delivery,
      source: _deepCopy(json) as Map<String, Object?>,
    );
  }

  final String hop;

  /// The instant this hop received the event.
  ///
  /// Parsed from the `received_at` string by [parseIso8601Instant], which
  /// UTC-normalizes the offsetful ISO 8601 timestamp: the absolute instant
  /// is preserved but the original offset is not retained on this field.
  /// A decoded entry's `toJson()` re-emits the string as it was decoded; an
  /// entry built with the constructor emits a `Z`-suffixed UTC string via
  /// `toIso8601String()`.
  final DateTime receivedAt;
  final String identifier;
  final String softwareVersion;
  final String? transformVersion;

  // event as received at this hop.
  final String? arrivalHash;

  // at this hop, forming a per-hop hash chain.
  final String? previousIngestHash;

  // ingested at this hop, starting at 0.
  final int? ingestSequenceNumber;

  // received as part of an ingestBatch call.
  final BatchContext? batchContext;

  // the receiver-hop entry. Receivers reassign a fresh local sequence_number
  // to the stored event so that origin and ingested events share one event
  // store keyed by one monotone counter; this field carries the wire-supplied
  // value so Chain 1 reconstruction can recover the originator's identity-
  // field set. Null on originator entries.
  final int? originSequenceNumber;

  /// The version of the library that stamped this entry, when a library
  /// stamped it.
  final String? libraryVersion;

  /// The identity of the database that stamped this entry, when a library
  /// stamped it.
  final String? databaseId;

  /// The delivery the stamping receiver ingested the event from, on a
  /// receiver-hop entry of an event ingested from a native delivery; null
  /// otherwise.
  final ProvenanceDelivery? delivery;

  /// The JSON this entry was decoded from, deep-copied at decode; null for
  /// an entry built with the constructor.
  final Map<String, Object?>? _source;

  // A decoded entry encodes exactly the keys it was decoded from, each value
  // unchanged (received_at keeps its original spelling), so a decode and
  // re-encode preserves the bytes its sender hashed. An entry built with the
  // constructor encodes its fields with snake_case keys, received_at as a
  // `Z`-suffixed UTC string and transform_version always present; the other
  // optional fields are omitted when null.
  Map<String, Object?> toJson() {
    final source = _source;
    if (source != null) return _deepCopy(source) as Map<String, Object?>;
    return _encodeFields();
  }

  Map<String, Object?> _encodeFields() => <String, Object?>{
    'hop': hop,
    'received_at': receivedAt.toIso8601String(),
    'identifier': identifier,
    'software_version': softwareVersion,
    'transform_version': transformVersion,
    if (arrivalHash != null) 'arrival_hash': arrivalHash,
    if (previousIngestHash != null) 'previous_ingest_hash': previousIngestHash,
    if (ingestSequenceNumber != null)
      'ingest_sequence_number': ingestSequenceNumber,
    if (batchContext != null) 'batch_context': batchContext!.toJson(),
    if (originSequenceNumber != null)
      'origin_sequence_number': originSequenceNumber,
    if (libraryVersion != null) 'library_version': libraryVersion,
    if (databaseId != null) 'database_id': databaseId,
    if (delivery != null) 'delivery': delivery!.toJson(),
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ProvenanceEntry &&
          hop == other.hop &&
          receivedAt == other.receivedAt &&
          identifier == other.identifier &&
          softwareVersion == other.softwareVersion &&
          transformVersion == other.transformVersion &&
          arrivalHash == other.arrivalHash &&
          previousIngestHash == other.previousIngestHash &&
          ingestSequenceNumber == other.ingestSequenceNumber &&
          batchContext == other.batchContext &&
          originSequenceNumber == other.originSequenceNumber &&
          libraryVersion == other.libraryVersion &&
          databaseId == other.databaseId &&
          delivery == other.delivery;

  @override
  int get hashCode => Object.hash(
    hop,
    receivedAt,
    identifier,
    softwareVersion,
    transformVersion,
    arrivalHash,
    previousIngestHash,
    ingestSequenceNumber,
    batchContext,
    originSequenceNumber,
    libraryVersion,
    databaseId,
    delivery,
  );

  @override
  String toString() =>
      'ProvenanceEntry('
      'hop: $hop, '
      'receivedAt: ${receivedAt.toIso8601String()}, '
      'identifier: $identifier, '
      'softwareVersion: $softwareVersion, '
      'transformVersion: $transformVersion, '
      'arrivalHash: $arrivalHash, '
      'previousIngestHash: $previousIngestHash, '
      'ingestSequenceNumber: $ingestSequenceNumber, '
      'batchContext: $batchContext, '
      'originSequenceNumber: $originSequenceNumber, '
      'libraryVersion: $libraryVersion, '
      'databaseId: $databaseId, '
      'delivery: $delivery)';
}

String _requireString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw FormatException('ProvenanceEntry: missing or non-string "$key"');
  }
  return value;
}

String? _optionalString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is! String) {
    throw FormatException(
      'ProvenanceEntry: "$key" must be a String when present',
    );
  }
  return value;
}

int? _optionalInt(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is! int) {
    throw FormatException(
      'ProvenanceEntry: "$key" must be an int when present',
    );
  }
  return value;
}

/// Copies a JSON value with fresh maps and lists at every level, so the
/// copy shares no mutable structure with the original.
Object? _deepCopy(Object? value) {
  if (value is Map<String, Object?>) {
    return <String, Object?>{
      for (final e in value.entries) e.key: _deepCopy(e.value),
    };
  }
  if (value is Map) {
    return <Object?, Object?>{
      for (final e in value.entries) e.key: _deepCopy(e.value),
    };
  }
  if (value is List) {
    return <Object?>[for (final v in value) _deepCopy(v)];
  }
  return value;
}
