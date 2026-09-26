import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:meta/meta.dart' show immutable;

/// One entry of the chain index: what the index holds for one stored event.
///
/// The backend writes the entry in the transaction that stores the event,
/// deriving every field from the stored copy ([ChainIndexEntry.of]), and
/// the library reads entries back by sealed hash, by predecessor, by origin
/// position and as the latest event a database holds as authored, instead
/// of scanning the log.
///
/// A field the stored copy does not yield is null: [originatingDatabaseId]
/// when the first provenance entry names no database, [sealedHash] and
/// [originPosition] when the provenance is missing or its second entry
/// records no arrival hash or origin position, and [heldAsAuthoredBy] when
/// the provenance does not hold exactly one entry naming a database.
// Implements: EVS-DEV-chain-verification/N
// the entry the chain index holds for each held event: its originating
//   database, sealed hash, origin position and previous_event_hash, beside
//   the database for which the copy counts as held as authored.
@immutable
final class ChainIndexEntry {
  /// An entry for the event [eventId] stored at [sequenceNumber].
  const ChainIndexEntry({
    required this.sequenceNumber,
    required this.eventId,
    required this.originatingDatabaseId,
    required this.sealedHash,
    required this.originPosition,
    required this.previousEventHash,
    required this.heldAsAuthoredBy,
  });

  /// The entry the chain index holds for [event], a stored copy: its
  /// originating database from the first provenance entry, its sealed hash
  /// and origin position from the copy itself when its provenance holds
  /// one entry and from the second entry otherwise, and its predecessor
  /// hash. Reads what the copy carries and never throws; a field the copy
  /// does not yield is null.
  // Implements: EVS-DEV-chain-verification/A
  // the originating database, the sealed hash and the origin position are
  //   read from the copy's provenance as the terms define them.
  factory ChainIndexEntry.of(StoredEvent event) {
    final raw = event.metadata['provenance'];
    final entries = raw is List ? raw : const <Object?>[];
    final first = entries.isEmpty ? null : entries.first;
    final second = entries.length < 2 ? null : entries[1];
    final originDb = first is Map ? first['database_id'] : null;
    final originatingDatabaseId = originDb is String && originDb.isNotEmpty
        ? originDb
        : null;
    String? sealedHash;
    int? originPosition;
    if (entries.length == 1) {
      sealedHash = event.eventHash;
      originPosition = event.sequenceNumber;
    } else if (second is Map) {
      final arrival = second['arrival_hash'];
      final position = second['origin_sequence_number'];
      sealedHash = arrival is String ? arrival : null;
      originPosition = position is int ? position : null;
    }
    return ChainIndexEntry(
      sequenceNumber: event.sequenceNumber,
      eventId: event.eventId,
      originatingDatabaseId: originatingDatabaseId,
      sealedHash: sealedHash,
      originPosition: originPosition,
      previousEventHash: event.previousEventHash,
      heldAsAuthoredBy: entries.length == 1 ? originatingDatabaseId : null,
    );
  }

  /// Decodes the persisted form [toJson] writes. Throws [FormatException]
  /// on a missing or mistyped field.
  factory ChainIndexEntry.fromJson(Map<String, Object?> json) {
    T? field<T>(String key, {required bool nullable}) {
      final value = json[key];
      if (value is T) return value;
      if (value == null && nullable) return null;
      throw FormatException('ChainIndexEntry: "$key" is missing or mistyped');
    }

    return ChainIndexEntry(
      sequenceNumber: field<int>('sequence_number', nullable: false)!,
      eventId: field<String>('event_id', nullable: false)!,
      originatingDatabaseId: field<String>(
        'origin_database_id',
        nullable: true,
      ),
      sealedHash: field<String>('sealed_hash', nullable: true),
      originPosition: field<int>('origin_position', nullable: true),
      previousEventHash: field<String>('previous_event_hash', nullable: true),
      heldAsAuthoredBy: field<String>('held_as_authored_by', nullable: true),
    );
  }

  /// The local sequence number the holding database stored the event at.
  final int sequenceNumber;

  /// The event's identifier.
  final String eventId;

  /// The database identity the event's first provenance entry records.
  final String? originatingDatabaseId;

  /// The hash the originating database sealed the event under.
  final String? sealedHash;

  /// The local sequence number the originating database stored the event
  /// at.
  final int? originPosition;

  /// The event's `previous_event_hash`: the sealed hash of the event its
  /// originating database authored before it, or null for the first.
  final String? previousEventHash;

  /// The database that holds this copy as authored when it is the holding
  /// database: the originating database of a copy whose provenance holds
  /// exactly one entry, and null for any other copy.
  final String? heldAsAuthoredBy;

  /// The persisted form, which [ChainIndexEntry.fromJson] decodes.
  Map<String, Object?> toJson() => <String, Object?>{
    'sequence_number': sequenceNumber,
    'event_id': eventId,
    'origin_database_id': originatingDatabaseId,
    'sealed_hash': sealedHash,
    'origin_position': originPosition,
    'previous_event_hash': previousEventHash,
    'held_as_authored_by': heldAsAuthoredBy,
  };

  @override
  bool operator ==(Object other) =>
      other is ChainIndexEntry &&
      other.sequenceNumber == sequenceNumber &&
      other.eventId == eventId &&
      other.originatingDatabaseId == originatingDatabaseId &&
      other.sealedHash == sealedHash &&
      other.originPosition == originPosition &&
      other.previousEventHash == previousEventHash &&
      other.heldAsAuthoredBy == heldAsAuthoredBy;

  @override
  int get hashCode => Object.hash(
    sequenceNumber,
    eventId,
    originatingDatabaseId,
    sealedHash,
    originPosition,
    previousEventHash,
    heldAsAuthoredBy,
  );

  @override
  String toString() =>
      'ChainIndexEntry(seq: $sequenceNumber, eventId: $eventId, '
      'origin: $originatingDatabaseId@$originPosition, sealed: $sealedHash, '
      'previous: $previousEventHash, heldAsAuthoredBy: $heldAsAuthoredBy)';
}
