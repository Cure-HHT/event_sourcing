// Implements: EVS-PRD-ingest/A
// ingest path existence; these exceptions are
//   the typed error surface of the ingest path
// Implements: EVS-PRD-ingest/D
// IngestChainBroken reports hash-chain
//   verification failure at the ingest boundary
// Implements: EVS-PRD-ingest/F
// IngestIdentityMismatch is thrown (not
//   silently duplicated) when a re-presented event's hash differs, preserving
//   idempotency semantics (identical re-presentations are safe; divergent
//   re-presentations are rejected)

import 'package:event_sourcing/src/ingest/chain_verdict.dart';
import 'package:event_sourcing/src/versions.dart';

/// Thrown by `EventStore.ingestBatch` / `ingestEvent` / `BatchEnvelope.decode`
/// when the input bytes cannot be parsed as a well-formed `esd/batch@2`
/// envelope (malformed JSON, wrong shape, unsupported format version,
/// missing required fields).
class IngestDecodeFailure implements Exception {
  const IngestDecodeFailure(this.message);
  final String message;
  @override
  String toString() => 'IngestDecodeFailure: $message';
}

/// Thrown by `ingestBatch` / `ingestEvent`, before any write, when an
/// incoming event's Chain 1 does not verify: its `event_hash` is not the
/// canonical hash of the record it carries
/// ([ChainFailureKind.eventHashMismatch]), some hop's `arrival_hash` does not
/// match the hash the prior state would produce
/// ([ChainFailureKind.arrivalHashMismatch]), or it carries no provenance
/// ([ChainFailureKind.provenanceMissing]). `ingestBatch` refuses the whole
/// batch.
class IngestChainBroken implements Exception {
  const IngestChainBroken({
    required this.eventId,
    required this.kind,
    required this.hopIndex,
    required this.expectedHash,
    required this.actualHash,
  });

  /// The refused event.
  final String eventId;

  /// Which link failed.
  final ChainFailureKind kind;

  /// The `provenance[]` index of the failing hop: for
  /// [ChainFailureKind.eventHashMismatch], the last hop, whose hash
  /// `event_hash` is; -1 when provenance is missing.
  final int hopIndex;

  /// The hash the event states: its `event_hash`, or the hop's
  /// `arrival_hash`.
  final String expectedHash;

  /// The hash recomputed from the event's content.
  final String actualHash;
  @override
  String toString() =>
      'IngestChainBroken(eventId: $eventId, kind: ${kind.name}, '
      'hopIndex: $hopIndex, expected: $expectedHash, actual: $actualHash)';
}

/// Thrown by `ingestBatch` / `ingestEvent` when an incoming event's
/// `event_id` matches an already-stored event but the incoming wire
/// `event_hash` differs from the stored copy's
/// `provenance[thisHop].arrival_hash` (i.e., the two copies are NOT
/// byte-identical).
class IngestIdentityMismatch implements Exception {
  const IngestIdentityMismatch({
    required this.eventId,
    required this.incomingHash,
    required this.storedArrivalHash,
  });
  final String eventId;
  final String incomingHash;
  final String storedArrivalHash;
  @override
  String toString() =>
      'IngestIdentityMismatch(eventId: $eventId, incoming: $incomingHash, '
      'storedArrival: $storedArrivalHash)';
}

/// Thrown by `EventStore.ingestBatch` and `EventStore.ingestEvent` when an
/// incoming event's data-format major differs from the receiver's
/// (`LibVersion.dataFormat`). The receiver reads no other data-format
/// major, so it refuses the event before any write, and `ingestBatch`
/// refuses the whole batch. Operator action: run builds of one data-format
/// major on both sides.
class IngestDataFormatIncompatible implements Exception {
  const IngestDataFormatIncompatible({
    required this.eventId,
    required this.wireFormat,
    required this.receiverFormat,
  });

  /// The refused event.
  final String eventId;

  /// The data-format version the event carries.
  final DataFormatVersion wireFormat;

  /// The receiver's data-format version.
  final DataFormatVersion receiverFormat;

  @override
  String toString() =>
      'IngestDataFormatIncompatible(event_id: $eventId, '
      'wire: $wireFormat, receiver: $receiverFormat)';
}

/// Thrown by `EventStore.ingestBatch` and `EventStore.ingestEvent` when an
/// incoming event's entry-type major is above the major the receiver
/// registers for its entry type. The receiver refuses the event before any
/// write, and `ingestBatch` refuses the whole batch. An event of the
/// registered major is accepted at any minor. Operator action: upgrade the
/// receiver's entry-type registry to the event's major.
class IngestEntryTypeVersionAhead implements Exception {
  const IngestEntryTypeVersionAhead({
    required this.eventId,
    required this.entryType,
    required this.wireVersion,
    required this.receiverVersion,
  });
  final String eventId;
  final String entryType;

  /// The entry-type version the event carries.
  final EntryTypeVersion wireVersion;

  /// The version the receiver registers for [entryType].
  final EntryTypeVersion receiverVersion;
  @override
  String toString() =>
      'IngestEntryTypeVersionAhead(event_id: $eventId, entry_type: $entryType, '
      'wire: $wireVersion, receiver: $receiverVersion)';
}

/// Thrown by `EventStore.ingestBatch` and `EventStore.ingestEvent` when an
/// incoming event is of a lower entry-type version than the receiver
/// registers and the receiver's promoter steps for a view the event folds
/// into do not lead from the event's version to the registered one: a
/// lower major with no major step registered from it, or a version past
/// the start of that major step. The receiver refuses the event before any
/// write, and `ingestBatch` refuses the whole batch. Operator action:
/// register the missing promoter step for [viewName], or stop the peer
/// sending the event's major.
class IngestEntryTypeVersionUnpromotable implements Exception {
  const IngestEntryTypeVersionUnpromotable({
    required this.eventId,
    required this.entryType,
    required this.viewName,
    required this.wireVersion,
    required this.receiverVersion,
    required this.reason,
  });
  final String eventId;
  final String entryType;

  /// The view whose promoter chain has no path from [wireVersion].
  final String viewName;

  /// The entry-type version the event carries.
  final EntryTypeVersion wireVersion;

  /// The version the receiver registers for [entryType].
  final EntryTypeVersion receiverVersion;

  /// Why the chain has no path, as the promoter registry states it.
  final String reason;
  @override
  String toString() =>
      'IngestEntryTypeVersionUnpromotable(event_id: $eventId, entry_type: '
      '$entryType, view: $viewName, wire: $wireVersion, receiver: '
      '$receiverVersion): $reason';
}

/// Why ingest refused an event of a reserved system entry type
/// ([IngestReservedEventRefused]).
enum ReservedEventRefusal {
  /// The event's aggregate type is not the one the library appends its
  /// entry type with, or its event type is not one of those.
  shapeMismatch,

  /// The event is a destination audit event naming the receiver's own
  /// database identity (`data.database_id` equals the receiver's
  /// `EventStore.databaseId`), and the receiver does not hold it.
  namesReceiverDatabase,

  /// The event is a destination audit event whose destination identifier
  /// (`data.id`) or database identity (`data.database_id`) is missing, not a
  /// string, empty, or contains `|`.
  malformed,
}

/// Thrown by `EventStore.ingestBatch` and `EventStore.ingestEvent` when an
/// incoming event of a reserved system entry type is not one the library
/// appends. The receiver refuses the event before any write, and
/// `ingestBatch` refuses the whole batch.
///
/// The library declares, for every reserved entry type, the one aggregate
/// type and the event types it appends that entry type with, fixed within a
/// data-format major, and the shape of every destination audit event (a
/// destination identifier and the identity of the database that appended
/// it, each a non-empty string without `|`). An event outside them
/// ([ReservedEventRefusal.shapeMismatch], [ReservedEventRefusal.malformed])
/// is not an event any library of the receiver's data-format major
/// appended, whoever forwarded it.
///
/// A destination audit event naming the receiver's own database that the
/// receiver does not hold ([ReservedEventRefusal.namesReceiverDatabase]) is
/// refused too: it is a forgery, the trace of a durability failure of the
/// receiver's storage, or an audit the receiver appended and lost when its
/// database was restored from a backup, which a peer forwards back to it. A
/// peer reaches the receiver with the receiver's own events only through a
/// destination that forwards them back, and such a destination fails without
/// any restore: an own event the receiver still holds is refused as
/// [IngestIdentityMismatch]. The fix is that destination's filter: leave
/// out the events that originated at the receiver, then recover the
/// destination's wedged head, which rebuilds its pending items under the new
/// filter.
///
/// Operator action otherwise: find the peer build or the process that
/// produced the event; a peer running the library forwards only events in
/// the declared shapes.
class IngestReservedEventRefused implements Exception {
  const IngestReservedEventRefused({
    required this.eventId,
    required this.entryType,
    required this.reason,
  });

  /// The refused event.
  final String eventId;

  /// The reserved entry type the event carries.
  final String entryType;

  /// Why the event was refused.
  final ReservedEventRefusal reason;

  @override
  String toString() =>
      'IngestReservedEventRefused(event_id: $eventId, entry_type: '
      '$entryType, reason: ${reason.name})';
}
