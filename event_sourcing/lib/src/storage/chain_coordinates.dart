import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:meta/meta.dart' show immutable, internal;

/// Where a stored copy of an event sits in the chains: its local sequence
/// number, its originating database, the hash that database sealed it
/// under, the position that database stored it at, the predecessor hash it
/// carries, and the database for which the copy counts as held as authored.
///
/// Computed from the stored copy alone ([ChainCoordinates.of]); the library
/// keeps no record of it. A field the copy does not yield is null:
/// [originatingDatabaseId] when the first provenance entry names no
/// database, [sealedHash] and [originPosition] when the provenance is
/// missing or its second entry records no arrival hash or origin position,
/// and [heldAsAuthoredBy] when the provenance does not hold exactly one
/// entry naming a database.
@internal
@immutable
final class ChainCoordinates {
  /// The coordinates of the event [eventId] stored at [sequenceNumber].
  const ChainCoordinates({
    required this.sequenceNumber,
    required this.eventId,
    required this.originatingDatabaseId,
    required this.sealedHash,
    required this.originPosition,
    required this.previousEventHash,
    required this.heldAsAuthoredBy,
  });

  /// The coordinates of [event], a stored copy. Never throws.
  factory ChainCoordinates.of(StoredEvent event) => ChainCoordinates.fromFields(
    sequenceNumber: event.sequenceNumber,
    eventId: event.eventId,
    eventHash: event.eventHash,
    previousEventHash: event.previousEventHash,
    provenance: event.metadata['provenance'],
  );

  /// The coordinates of the stored copy whose record carries these fields:
  /// its local [sequenceNumber], [eventId], stored [eventHash],
  /// [previousEventHash] and the raw `metadata.provenance` value
  /// [provenance]. Reads what the fields carry and never throws.
  // Implements: EVS-DEV-chain-verification/A
  // the originating database is read from the first provenance entry, and
  //   the sealed hash and the origin position from the copy itself when its
  //   provenance holds one entry and from the second entry otherwise.
  factory ChainCoordinates.fromFields({
    required int sequenceNumber,
    required String eventId,
    required String eventHash,
    required String? previousEventHash,
    required Object? provenance,
  }) {
    final entries = provenance is List ? provenance : const <Object?>[];
    final first = entries.isEmpty ? null : entries.first;
    final second = entries.length < 2 ? null : entries[1];
    final originDb = first is Map ? first['database_id'] : null;
    final originatingDatabaseId = originDb is String && originDb.isNotEmpty
        ? originDb
        : null;
    String? sealedHash;
    int? originPosition;
    if (entries.length == 1) {
      sealedHash = eventHash;
      originPosition = sequenceNumber;
    } else if (second is Map) {
      final arrival = second['arrival_hash'];
      final position = second['origin_sequence_number'];
      sealedHash = arrival is String ? arrival : null;
      originPosition = position is int ? position : null;
    }
    return ChainCoordinates(
      sequenceNumber: sequenceNumber,
      eventId: eventId,
      originatingDatabaseId: originatingDatabaseId,
      sealedHash: sealedHash,
      originPosition: originPosition,
      previousEventHash: previousEventHash,
      heldAsAuthoredBy: entries.length == 1 ? originatingDatabaseId : null,
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

  /// The database the copy's sole provenance entry names, or null when its
  /// provenance does not hold exactly one entry. The holding database holds
  /// the copy as authored exactly when this is its own identity.
  final String? heldAsAuthoredBy;

  @override
  bool operator ==(Object other) =>
      other is ChainCoordinates &&
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
      'ChainCoordinates(seq: $sequenceNumber, eventId: $eventId, '
      'origin: $originatingDatabaseId@$originPosition, sealed: $sealedHash, '
      'previous: $previousEventHash, heldAsAuthoredBy: $heldAsAuthoredBy)';
}
