import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:meta/meta.dart' show visibleForTesting;
import 'package:provenance/provenance.dart';

/// Represents a stored event with all fields populated.
///
/// Pure data — no Sembast or Flutter dependency on its shape — so it can
/// travel through the `StorageBackend` contract without leaking backend
/// details into the abstraction. Lives in `lib/src/storage/` alongside the
/// other storage value types (`FifoEntry`, etc.).
// the ingesting server is the sole authority on its own timestamp.
// stamped by the substrate from EntryTypeDefinition.registeredVersion on every
// local append; preserved verbatim on ingested events.
// stamped by the lib from currentLibFormatVersion.
// top-level user_id remains on StoredEvent.
// event record.
class StoredEvent {
  const StoredEvent({
    required this.key,
    required this.eventId,
    required this.aggregateId,
    required this.aggregateType,
    required this.entryType,
    required this.entryTypeVersion,
    required this.libFormatVersion,
    required this.eventType,
    required this.sequenceNumber,
    required this.data,
    required this.metadata,
    required this.initiator,
    required this.clientTimestamp,
    required this.eventHash,
    this.flowToken,
    this.previousEventHash,
  });

  /// Create from a database record map.
  ///
  /// Every required field is explicitly type-checked via an `is!` guard and
  /// a thrown [FormatException] naming the offending key. A malformed event
  /// record surfaces as a typed error rather than a generic `CastError` or
  /// `TypeError` at an unrelated call site, keeping diagnosis focused on the
  /// actual bad field.
  factory StoredEvent.fromMap(Map<String, Object?> map, int key) {
    final eventId = _requireString(map, 'event_id');
    final aggregateId = _requireString(map, 'aggregate_id');
    final aggregateType = _requireString(map, 'aggregate_type');
    final entryType = _requireString(map, 'entry_type');
    final entryTypeVersion = _requireInt(map, 'entry_type_version');
    final libFormatVersion = _requireInt(map, 'lib_format_version');
    final eventType = _requireString(map, 'event_type');
    final sequenceNumber = _requireInt(map, 'sequence_number');
    final data = _requireMap(map, 'data');
    final metadataRaw = map['metadata'];
    if (metadataRaw != null && metadataRaw is! Map) {
      throw const FormatException(
        'StoredEvent: "metadata" must be a Map when present',
      );
    }
    final metadata = metadataRaw == null
        ? <String, dynamic>{}
        : Map<String, dynamic>.from(metadataRaw as Map);

    final initiatorRaw = map['initiator'];
    if (initiatorRaw is! Map) {
      throw const FormatException(
        'StoredEvent: missing or non-map "initiator"',
      );
    }
    final initiator = Initiator.fromJson(
      Map<String, dynamic>.from(initiatorRaw),
    );

    final flowTokenRaw = map['flow_token'];
    if (flowTokenRaw != null && flowTokenRaw is! String) {
      throw const FormatException(
        'StoredEvent: "flow_token" must be a String when present',
      );
    }

    final clientTimestamp = _requireDateTime(map, 'client_timestamp');
    final eventHash = _requireString(map, 'event_hash');

    final previousHashRaw = map['previous_event_hash'];
    if (previousHashRaw != null && previousHashRaw is! String) {
      throw const FormatException(
        'StoredEvent: "previous_event_hash" must be a String when present',
      );
    }
    return StoredEvent(
      key: key,
      eventId: eventId,
      aggregateId: aggregateId,
      aggregateType: aggregateType,
      entryType: entryType,
      entryTypeVersion: entryTypeVersion,
      libFormatVersion: libFormatVersion,
      eventType: eventType,
      sequenceNumber: sequenceNumber,
      data: Map<String, dynamic>.from(data),
      metadata: metadata,
      initiator: initiator,
      flowToken: flowTokenRaw as String?,
      clientTimestamp: clientTimestamp,
      eventHash: eventHash,
      previousEventHash: previousHashRaw as String?,
    );
  }

  /// Test-only factory for constructing a `StoredEvent` with caller-
  /// supplied fields — no real hash chain, no sequence bookkeeping.
  /// Downstream packages' in-memory `StorageBackend` doubles use this
  /// to seed events without re-implementing hash chaining.
  @visibleForTesting
  factory StoredEvent.synthetic({
    required String eventId,
    required String aggregateId,
    required String entryType,
    required Initiator initiator,
    required DateTime clientTimestamp,
    required String eventHash,
    int key = 0,
    String aggregateType = '_test',
    String eventType = 'finalized',
    int sequenceNumber = 0,
    Map<String, dynamic>? data,
    Map<String, dynamic>? metadata,
    String? flowToken,
    String? previousEventHash,
    int entryTypeVersion = 1,
    int libFormatVersion = 1,
  }) => StoredEvent(
    key: key,
    eventId: eventId,
    aggregateId: aggregateId,
    aggregateType: aggregateType,
    entryType: entryType,
    entryTypeVersion: entryTypeVersion,
    libFormatVersion: libFormatVersion,
    eventType: eventType,
    sequenceNumber: sequenceNumber,
    data: data ?? const <String, dynamic>{},
    metadata: metadata ?? const <String, dynamic>{},
    initiator: initiator,
    flowToken: flowToken,
    clientTimestamp: clientTimestamp,
    eventHash: eventHash,
    previousEventHash: previousEventHash,
  );

  /// Storage shape version the current lib build produces. Stamped on every
  /// event by `EventStore.append` and propagated over the wire. Receivers
  /// reject events whose `lib_format_version > currentLibFormatVersion` per
  ///
  static const int currentLibFormatVersion = 1;

  /// Database key.
  final int key;

  /// Unique event ID (UUID v4).
  final String eventId;

  /// ID of the aggregate this event belongs to.
  final String aggregateId;

  /// Type of aggregate (e.g., 'Order', 'Invoice').
  final String aggregateType;

  /// Structural kind of the entry within its aggregate type (e.g.,
  /// 'order_placed', 'invoice_paid'). First-class
  final String entryType;

  /// Application schema version under which this event was authored.
  ///
  /// Stamped by the substrate from `EntryTypeDefinition.registeredVersion`
  /// on every local append (the caller does not choose it). Preserved
  /// verbatim on ingested events — reflects the originating install's
  /// registry at the time of append. Ingest-side promotion (in
  /// `ProjectionInterpreter`) and boot-time snapshot promotion (in
  /// `EventStore.open`) read this field to decide whether the event needs
  /// to be lifted to a newer version before fold.
  final int entryTypeVersion;

  /// Storage shape version this event was persisted with. Stamped by the
  /// lib from [currentLibFormatVersion] on every append.
  final int libFormatVersion;

  /// User-intent discriminator for the event: 'finalized' | 'checkpoint' |
  /// 'tombstone'.
  final String eventType;

  /// Monotonically increasing sequence number.
  final int sequenceNumber;

  /// Event payload data (JSON).
  final Map<String, dynamic> data;

  /// Additional metadata; typically carries `change_reason` and
  /// `provenance[]`.
  final Map<String, dynamic> metadata;

  /// Actor that initiated this event. Replaces the Phase-4.3 top-level
  /// `userId` field.
  final Initiator initiator;

  /// Client-side timestamp when event was created.
  final DateTime clientTimestamp;

  /// Correlation token linking events that belong to the same multi-step
  /// business flow (e.g., `invite:ABC123`). Nullable; the library does not
  /// enforce format.
  final String? flowToken;

  /// SHA-256 hash of event for tamper detection.
  final String eventHash;

  /// Hash of previous event (for chain integrity).
  final String? previousEventHash;

  /// First `ProvenanceEntry` in this event's chain — the originator's hop.
  ///
  /// Materialized from `metadata['provenance'][0]` on each access. Convenience
  /// accessor for cross-hop discrimination logic (e.g.
  /// `EventStore.isLocallyOriginated`) and for read-side queries that
  /// project on originator identity. Throws `StateError` when the
  /// provenance list is missing, non-list, or empty:  requires
  /// every event to carry at least one provenance entry, so an absent or
  /// empty list indicates corrupted or malformed data and surfacing it
  /// loudly is the right behavior.
  // StateError on empty/missing provenance per the assertion contract.
  ProvenanceEntry get originatorHop {
    final raw = metadata['provenance'];
    if (raw is! List || raw.isEmpty) {
      throw StateError(
        'StoredEvent has empty or missing provenance; expected at least the '
        'originator entry',
      );
    }
    final first = raw.first;
    if (first is! Map) {
      throw StateError(
        'StoredEvent provenance[0] is not a Map; cannot decode originator hop',
      );
    }
    return ProvenanceEntry.fromJson(Map<String, Object?>.from(first));
  }

  /// Returns a copy of this event with [newData] replacing [data]. All
  /// other fields are preserved. Used by the substrate's promoter
  /// machinery (rebuildView and ProjectionInterpreter) to thread a
  /// promoted payload through the fold interpreters without modifying the
  /// in-memory original or rebuilding the event hash chain.
  StoredEvent withData(Map<String, Object?> newData) {
    return StoredEvent(
      key: key,
      eventId: eventId,
      aggregateId: aggregateId,
      aggregateType: aggregateType,
      entryType: entryType,
      entryTypeVersion: entryTypeVersion,
      libFormatVersion: libFormatVersion,
      eventType: eventType,
      sequenceNumber: sequenceNumber,
      data: newData,
      metadata: metadata,
      initiator: initiator,
      clientTimestamp: clientTimestamp,
      eventHash: eventHash,
      flowToken: flowToken,
      previousEventHash: previousEventHash,
    );
  }

  /// Convert to a map for storage/serialization.
  Map<String, dynamic> toMap() {
    return {
      'event_id': eventId,
      'aggregate_id': aggregateId,
      'aggregate_type': aggregateType,
      'entry_type': entryType,
      'entry_type_version': entryTypeVersion,
      'lib_format_version': libFormatVersion,
      'event_type': eventType,
      'sequence_number': sequenceNumber,
      'data': data,
      'metadata': metadata,
      'initiator': initiator.toJson(),
      'flow_token': flowToken,
      'client_timestamp': clientTimestamp.toIso8601String(),
      'event_hash': eventHash,
      'previous_event_hash': previousEventHash,
    };
  }

  /// Convert to JSON for API calls.
  Map<String, dynamic> toJson() => toMap();

  @override
  String toString() {
    return 'StoredEvent(eventId: $eventId, entryType: $entryType, '
        'eventType: $eventType, seq: $sequenceNumber)';
  }
}

String _requireString(Map<String, Object?> map, String key) {
  final value = map[key];
  if (value is! String) {
    throw FormatException('StoredEvent: missing or non-string "$key"');
  }
  return value;
}

int _requireInt(Map<String, Object?> map, String key) {
  final value = map[key];
  if (value is! int) {
    throw FormatException('StoredEvent: missing or non-int "$key"');
  }
  return value;
}

Map<Object?, Object?> _requireMap(Map<String, Object?> map, String key) {
  final value = map[key];
  if (value is! Map) {
    throw FormatException('StoredEvent: missing or non-map "$key"');
  }
  return value;
}

DateTime _requireDateTime(Map<String, Object?> map, String key) {
  final value = map[key];
  if (value is! String) {
    throw FormatException(
      'StoredEvent: missing or non-string "$key" (expected ISO 8601)',
    );
  }
  try {
    return DateTime.parse(value);
  } on FormatException catch (e) {
    throw FormatException(
      'StoredEvent: "$key" is not a valid ISO 8601 string: ${e.message}',
    );
  }
}
