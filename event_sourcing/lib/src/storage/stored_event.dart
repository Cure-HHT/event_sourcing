import 'package:event_sourcing/src/causal_record.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
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
    this.causal,
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
    required this.causal,
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
  /// `client_timestamp`, and the `received_at` of every entry of the
  /// metadata's `provenance` list that carries one, must be a date-time with a four-digit
  /// year, calendar fields within their ranges, and an explicit offset (`Z`
  /// or `+/-HH[:]MM`), the form [parseIso8601Instant] reads; any other value
  /// is a [FormatException] naming the field.
  ///
  /// Every entry of that `provenance` list that is a map must carry
  /// `database_id` and `library_version` as non-empty strings, and the
  /// record must carry a `causal` object of exactly the shape
  /// [CausalRecord.fromJson] reads; any other record is a [FormatException]
  /// naming the field.
  ///
  /// Every required field is explicitly type-checked via an `is!` guard and
  /// a thrown [FormatException] naming the offending key. A malformed event
  /// record surfaces as a typed error rather than a generic `CastError` or
  /// `TypeError` at an unrelated call site, keeping diagnosis focused on the
  /// actual bad field.
  // Implements: EVS-DEV-event-record/H
  // a record any of whose provenance entries lacks database_id or
  //   library_version, or carries one that is not a non-empty string, does
  //   not parse, naming the field.
  // Implements: EVS-DEV-causal-parents/B
  // a record with no causal object of the exact shape does not parse,
  //   naming the field.
  // Implements: EVS-DEV-event-record/G
  // every provenance entry, and every key of each entry, is kept exactly as
  //   the record carries it.
  // Implements: EVS-DEV-event-record/J
  // the causal object is kept exactly as the record carries it.
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
    _requireProvenanceEntries(metadata);
    final eventHash = _requireString(map, 'event_hash');

    final previousHashRaw = map['previous_event_hash'];
    if (previousHashRaw != null && previousHashRaw is! String) {
      throw const FormatException(
        'StoredEvent: "previous_event_hash" must be a String when present',
      );
    }
    final causalRaw = map['causal'];
    if (causalRaw == null) {
      throw const FormatException('StoredEvent: missing "causal"');
    }
    final CausalRecord causal;
    try {
      causal = CausalRecord.fromJson(causalRaw);
    } on FormatException catch (e) {
      throw FormatException('StoredEvent: "causal": ${e.message}');
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
      causal: causal,
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
  /// to seed events without re-implementing hash chaining. [causal]
  /// defaults to an eligible version with no parents.
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
    CausalRecord? causal,
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
    causal:
        causal ??
        CausalRecord(
          kind: CausalKind.version,
          eligible: true,
          parents: const <CausalRef>[],
        ),
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
    'causal',
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

  /// The event's `causal` object: its kind, its eligibility to be named as
  /// a parent, the versions of its aggregate it follows and, for a
  /// reconciliation, the skip events it closes. The library stamps it on
  /// every append and keeps it unchanged on every other path; the event
  /// hash covers it. Every event parsed from a record carries one; null
  /// only for an event built with the constructor without one, which no
  /// append, read or ingest admits ([requireWellFormedRecord]).
  final CausalRecord? causal;

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

  /// Throws [FormatException] naming the field when the record [toMap]
  /// writes is not one [StoredEvent.fromMap] reads: its `client_timestamp`,
  /// or the `received_at` of an entry of the metadata's `provenance` list,
  /// is not a timestamp a record may carry; an entry of that list lacks
  /// `database_id` or `library_version`, or carries one that is not a
  /// non-empty string; or the event carries no `causal` object. An event
  /// parsed from a record passes unless its metadata was changed since; one
  /// built with the constructor fails when any of these does not hold.
  // Implements: EVS-DEV-event-record/H
  // every append refuses, naming the field, and ingestEvent stores no event
  //   for, an event whose provenance entry lacks database_id or
  //   library_version.
  // Implements: EVS-DEV-causal-parents/B
  // every append refuses, naming the field, and ingestEvent stores no event
  //   for, an event that carries no causal object.
  @internal
  void requireWellFormedRecord() {
    if (causal == null) {
      throw const FormatException('StoredEvent: missing "causal"');
    }
    if (_clientTimestampText == null) {
      final text = clientTimestamp.toUtc().toIso8601String();
      try {
        parseIso8601Instant(text);
      } on FormatException catch (e) {
        throw FormatException(
          'StoredEvent: "client_timestamp" is not a timestamp a record may '
          'carry: ${e.message}',
        );
      }
    }
    _requireProvenanceEntries(metadata);
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

  /// The entries of this copy's `provenance` list, first to last. Throws
  /// [StateError] when the list is missing, empty, or holds an entry that
  /// is not an object.
  List<Map<String, Object?>> get _provenanceEntries {
    final raw = metadata['provenance'];
    if (raw is! List || raw.isEmpty) {
      throw StateError(
        'StoredEvent $eventId has empty or missing provenance; expected at '
        'least the originator entry',
      );
    }
    return <Map<String, Object?>>[
      for (final (index, entry) in raw.indexed)
        if (entry is Map<String, Object?>)
          entry
        else
          throw StateError(
            'StoredEvent $eventId provenance[$index] is not an object',
          ),
    ];
  }

  /// The identity of the database that authored this event: the
  /// `database_id` of its first provenance entry, the same at every holder.
  /// Throws [StateError] when that entry records none.
  // Implements: EVS-DEV-chain-verification/A
  // the originating database is read from the first provenance entry.
  String get originatingDatabaseId {
    final value = _provenanceEntries.first['database_id'];
    if (value is! String || value.isEmpty) {
      throw StateError(
        'StoredEvent $eventId: its originator entry records no database_id',
      );
    }
    return value;
  }

  /// The hash this event's originating database sealed it under, the same
  /// at every holder: the copy's [eventHash] when its provenance holds one
  /// entry, otherwise the `arrival_hash` of its second entry. Throws
  /// [StateError] when that entry records none.
  // Implements: EVS-DEV-chain-verification/A
  // the sealed hash is the event hash of a one-entry copy, otherwise the
  //   arrival hash of the second entry.
  String get sealedHash {
    final entries = _provenanceEntries;
    if (entries.length == 1) return eventHash;
    final value = entries[1]['arrival_hash'];
    if (value is! String) {
      throw StateError(
        'StoredEvent $eventId: its first receiver entry records no '
        'arrival_hash',
      );
    }
    return value;
  }

  /// The local sequence number this event's originating database stored it
  /// under, the same at every holder: the copy's [sequenceNumber] when its
  /// provenance holds one entry, otherwise the `origin_sequence_number` of
  /// its second entry. Throws [StateError] when that entry records none.
  // Implements: EVS-DEV-chain-verification/A
  // the origin position is the sequence number of a one-entry copy,
  //   otherwise the origin sequence number of the second entry.
  int get originPosition {
    final entries = _provenanceEntries;
    if (entries.length == 1) return sequenceNumber;
    final value = entries[1]['origin_sequence_number'];
    if (value is! int) {
      throw StateError(
        'StoredEvent $eventId: its first receiver entry records no '
        'origin_sequence_number',
      );
    }
    return value;
  }

  /// True when this copy is held as authored by the database [databaseId]:
  /// its provenance holds exactly one entry, and that entry names
  /// [databaseId]. A copy whose provenance is missing or malformed is not.
  bool isHeldAsAuthoredBy(String databaseId) {
    final raw = metadata['provenance'];
    if (raw is! List || raw.length != 1) return false;
    final entry = raw.single;
    return entry is Map && entry['database_id'] == databaseId;
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
      causal: causal,
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
  /// this build does not read. `causal` is written as [causal] holds it,
  /// and only when the event carries one.
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
      if (causal != null) 'causal': Map<String, Object?>.of(causal!.toJson()),
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
    return parseIso8601Instant(value);
  } on FormatException catch (e) {
    throw FormatException(
      'StoredEvent: "$key" is not a timestamp a record may carry: '
      '${e.message}',
    );
  }
}

/// Throws [FormatException] naming the field and the entry when an entry of
/// [metadata]'s `provenance` list is a map that lacks `database_id` or
/// `library_version`, or carries one that is not a non-empty string, or
/// carries a `received_at` that is not a string in the timestamp form
/// [parseIso8601Instant] reads. The shape of the list otherwise (a missing
/// or non-list `provenance`, an entry that is not a map or lacks
/// `received_at`) is left to the chain checks and [ProvenanceEntry.fromJson],
/// which read them.
// Implements: EVS-DEV-event-record/C
// a record carrying a provenance received_at outside the timestamp form is
//   malformed, naming the field.
// Implements: EVS-DEV-event-record/H
// a record any of whose provenance entries lacks database_id or
//   library_version, or carries one that is not a non-empty string, is
//   malformed, naming the field.
void _requireProvenanceEntries(Map<String, dynamic> metadata) {
  final provenance = metadata['provenance'];
  if (provenance is! List) return;
  for (var i = 0; i < provenance.length; i++) {
    final entry = provenance[i];
    if (entry is! Map) continue;
    for (final field in const <String>['database_id', 'library_version']) {
      final value = entry[field];
      if (value is! String || value.isEmpty) {
        throw FormatException(
          'StoredEvent: provenance[$i] has a missing, empty or non-string '
          '"$field"',
        );
      }
    }
    final receivedAt = entry['received_at'];
    if (receivedAt == null) continue;
    if (receivedAt is! String) {
      throw FormatException(
        'StoredEvent: provenance[$i] has a non-string "received_at" '
        '(expected ISO 8601)',
      );
    }
    try {
      parseIso8601Instant(receivedAt);
    } on FormatException catch (e) {
      throw FormatException(
        'StoredEvent: provenance[$i] "received_at" is not a timestamp a '
        'record may carry: ${e.message}',
      );
    }
  }
}
