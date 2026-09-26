// Implements: EVS-PRD-ingest/A
// ingest path existence; these exceptions are
//   the typed error surface of the ingest path

import 'package:event_sourcing/src/versions.dart';

/// Thrown by `EventStore.ingestBatch` and `BatchEnvelope.decode` when the
/// input bytes cannot be parsed as a well-formed `esd/batch@2` envelope
/// (malformed JSON, wrong shape, unsupported format version, missing
/// required fields). A record inside a well-formed envelope that the library
/// cannot store as an event is kept in a security finding instead.
class IngestDecodeFailure implements Exception {
  const IngestDecodeFailure(this.message);
  final String message;
  @override
  String toString() => 'IngestDecodeFailure: $message';
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
