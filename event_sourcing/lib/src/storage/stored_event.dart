import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/record_timestamp.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:provenance/provenance.dart';

/// Represents a stored event with all fields populated.
///
/// Pure data — no Sembast or Flutter dependency on its shape — so it can
/// travel through the `StorageBackend` contract without leaking backend
/// details into the abstraction. Lives in `lib/src/storage/` alongside the
/// other storage value types (`FifoEntry`, etc.).
// Implements: EVS-PRD-event-log/A
// immutable event record; fields are all
//   final; once stored, no field is modified by the substrate.
// Implements: EVS-DEV-flow-token/D
// flow_token is stored as an opaque String, type-checked only (never parsed or interpreted by the substrate).
// Implements: EVS-PRD-event-log/B
// carries sequenceNumber for total ordering.
// Implements: EVS-DEV-version-compatibility/A+C
// entryTypeVersion and
//   libFormatVersion are major.minor values, stored and read as
//   {major, minor} objects; a malformed one is a FormatException.
// Implements: EVS-PRD-portability/C
// pure Dart value type; no platform
//   dependency; serialises identically on every Dart-supported runtime.
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
  }) : _clientTimestampText = null,
       _initiatorJson = null,
       _entryTypeVersionJson = null,
       _libFormatVersionJson = null,
       _unknownFields = null;

  const StoredEvent._parsed({
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
    required this.flowToken,
    required this.previousEventHash,
    required String? clientTimestampText,
    required Map<String, Object?>? initiatorJson,
    required Map<String, Object?>? entryTypeVersionJson,
    required Map<String, Object?>? libFormatVersionJson,
    required Map<String, Object?>? unknownFields,
  }) : _clientTimestampText = clientTimestampText,
       _initiatorJson = initiatorJson,
       _entryTypeVersionJson = entryTypeVersionJson,
       _libFormatVersionJson = libFormatVersionJson,
       _unknownFields = unknownFields;

  /// Create from a database record map.
  ///
  /// Every field the event hash covers (`canonicalEventHash`) reads back
  /// from [toMap] as it stands in [map]: `client_timestamp` keeps its
  /// string as written (whatever its fraction digits or offset spelling),
  /// and `initiator`, `entry_type_version` and `lib_format_version` keep
  /// their maps, including keys and absent optional keys that [Initiator]
  /// and the version types do not model. So a record parsed here hashes as
  /// it did before parsing, and the copy a backend stores and a relay
  /// forwards still hashes to the `event_hash` it carries. A top-level key
  /// this build does not read is kept too, and [toMap] writes it back.
  ///
  /// `client_timestamp` must be a date-time with a four-digit year,
  /// calendar fields within their ranges, and an explicit offset (`Z` or
  /// `+/-HH[:]MM`); any other value is a [FormatException] naming it.
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
    final entryTypeVersion = _requireVersion(
      map,
      'entry_type_version',
      EntryTypeVersion.fromJson,
    );
    final libFormatVersion = _requireVersion(
      map,
      'lib_format_version',
      DataFormatVersion.fromJson,
    );
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
    return StoredEvent._parsed(
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
      clientTimestampText: map['client_timestamp']! as String,
      initiatorJson: Map<String, Object?>.unmodifiable(
        Map<String, Object?>.from(initiatorRaw),
      ),
      entryTypeVersionJson: Map<String, Object?>.unmodifiable(
        Map<String, Object?>.from(map['entry_type_version']! as Map),
      ),
      libFormatVersionJson: Map<String, Object?>.unmodifiable(
        Map<String, Object?>.from(map['lib_format_version']! as Map),
      ),
      unknownFields: map.keys.every(recordKeys.contains)
          ? null
          : Map<String, Object?>.unmodifiable(<String, Object?>{
              for (final entry in map.entries)
                if (!recordKeys.contains(entry.key)) entry.key: entry.value,
            }),
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
    EntryTypeVersion entryTypeVersion = const EntryTypeVersion(1, 0),
    DataFormatVersion libFormatVersion = LibVersion.dataFormat,
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

  /// The top-level keys of a record that this build reads.
  @internal
  static const Set<String> recordKeys = <String>{
    'event_id',
    'aggregate_id',
    'aggregate_type',
    'entry_type',
    'entry_type_version',
    'lib_format_version',
    'event_type',
    'sequence_number',
    'data',
    'metadata',
    'initiator',
    'flow_token',
    'client_timestamp',
    'event_hash',
    'previous_event_hash',
  };

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

  /// Entry-type version under which this event was authored.
  ///
  /// Stamped by the library from `EntryTypeDefinition.registeredVersion`
  /// on every local append (the caller does not choose it). Preserved
  /// verbatim on ingested events -- it reflects the originating install's
  /// registry at the time of append. The projection interpreter and
  /// `rebuildView` read it to decide whether the event is promoted before
  /// the fold, and ingest reads it to refuse a higher major.
  final EntryTypeVersion entryTypeVersion;

  /// Data-format version of the build that appended this event. Stamped by
  /// the library from `LibVersion.dataFormat` on every append; ingest
  /// refuses an event whose data-format major differs from the receiver's.
  final DataFormatVersion libFormatVersion;

  /// Discriminator for what the event records within its entry type. For a
  /// user entry type the appender chooses it (for example 'finalized',
  /// 'checkpoint' or 'tombstone'), and a projection spec's event-type sets
  /// key on it. For a reserved system entry type the library sets it per
  /// kind; each kind of destination audit event carries its own, exported
  /// as `kDestinationRegisteredEventType`,
  /// `kDestinationStartDateSetEventType`, `kDestinationEndDateSetEventType`,
  /// `kDestinationDeletedEventType` and
  /// `kDestinationWedgeRecoveredEventType`.
  final String eventType;

  /// Monotonically increasing sequence number.
  final int sequenceNumber;

  /// Event payload data (JSON).
  final Map<String, dynamic> data;

  /// Additional metadata; typically carries `change_reason` and
  /// `provenance[]`.
  final Map<String, dynamic> metadata;

  /// Actor that initiated this event.
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

  /// The `client_timestamp` string of the record this event was parsed
  /// from; null for an event built with the constructor, whose [toMap]
  /// writes [clientTimestamp] in UTC with `toIso8601String`.
  final String? _clientTimestampText;

  /// The `initiator` map of the record this event was parsed from; null for
  /// an event built with the constructor, whose [toMap] writes
  /// [initiator]'s `toJson`.
  final Map<String, Object?>? _initiatorJson;

  /// The `entry_type_version` and `lib_format_version` maps of the record
  /// this event was parsed from; null for an event built with the
  /// constructor, whose [toMap] writes the versions' `toJson`.
  final Map<String, Object?>? _entryTypeVersionJson;
  final Map<String, Object?>? _libFormatVersionJson;

  /// The top-level keys of the record this event was parsed from that this
  /// build does not read, with their values; null when there are none.
  final Map<String, Object?>? _unknownFields;

  /// Throws [FormatException] naming `client_timestamp` when the timestamp
  /// [toMap] writes is not one a record may carry (see
  /// [StoredEvent.fromMap]). An event parsed from a record always passes;
  /// one built with the constructor fails when its [clientTimestamp] lies
  /// outside the four-digit years.
  @internal
  void requireRecordTimestamp() {
    if (_clientTimestampText != null) return;
    final text = clientTimestamp.toUtc().toIso8601String();
    try {
      parseRecordTimestamp(text);
    } on FormatException catch (e) {
      throw FormatException(
        'StoredEvent: "client_timestamp" is not a timestamp a record may '
        'carry: ${e.message}',
      );
    }
  }

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
    return StoredEvent._parsed(
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
      clientTimestampText: _clientTimestampText,
      initiatorJson: _initiatorJson,
      entryTypeVersionJson: _entryTypeVersionJson,
      libFormatVersionJson: _libFormatVersionJson,
      unknownFields: _unknownFields,
    );
  }

  /// Convert to a map for storage/serialization. For an event parsed with
  /// [StoredEvent.fromMap], `client_timestamp`, `initiator`,
  /// `entry_type_version` and `lib_format_version` are written as the
  /// parsed record held them, and so is every top-level key of that record
  /// this build does not read.
  Map<String, dynamic> toMap() {
    return {
      ...?_unknownFields,
      'event_id': eventId,
      'aggregate_id': aggregateId,
      'aggregate_type': aggregateType,
      'entry_type': entryType,
      'entry_type_version': _entryTypeVersionJson == null
          ? entryTypeVersion.toJson()
          : Map<String, Object?>.of(_entryTypeVersionJson),
      'lib_format_version': _libFormatVersionJson == null
          ? libFormatVersion.toJson()
          : Map<String, Object?>.of(_libFormatVersionJson),
      'event_type': eventType,
      'sequence_number': sequenceNumber,
      'data': data,
      'metadata': metadata,
      'initiator': _initiatorJson == null
          ? initiator.toJson()
          : Map<String, Object?>.of(_initiatorJson),
      'flow_token': flowToken,
      'client_timestamp':
          _clientTimestampText ?? clientTimestamp.toUtc().toIso8601String(),
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

/// Parses the version stored under [key] with [parse], naming [key] in the
/// [FormatException] a missing or malformed value throws. An integer is the
/// version shape of a data format before `2.0`, which this build does not
/// read, and the message says so.
T _requireVersion<T>(
  Map<String, Object?> map,
  String key,
  T Function(Object? json) parse,
) {
  final value = map[key];
  if (value == null) {
    throw FormatException('StoredEvent: missing "$key"');
  }
  if (value is int) {
    throw FormatException(
      'StoredEvent: "$key" is the integer $value, a version shape this '
      'data format does not read; the record was written by a build of an '
      'earlier data format',
    );
  }
  try {
    return parse(value);
  } on FormatException catch (e) {
    throw FormatException('StoredEvent: "$key": ${e.message}');
  }
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
    return parseRecordTimestamp(value);
  } on FormatException catch (e) {
    throw FormatException(
      'StoredEvent: "$key" is not a timestamp a record may carry: '
      '${e.message}',
    );
  }
}
