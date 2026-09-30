// Implements: EVS-PRD-ingest/A
// ingest path existence; these exceptions are
//   the typed error surface of the ingest path

import 'package:event_sourcing/src/versions.dart';

/// Thrown by `DeliveryEnvelope.decode` when the input bytes cannot be
/// parsed as a well-formed delivery envelope (malformed JSON, wrong shape,
/// unsupported format version, missing required fields). A record inside a
/// well-formed envelope that the library cannot store as an event is kept
/// in a security finding instead.
///
/// [reason] names the refusal: one of [formatUnsupported],
/// [attributesNotObject], [noEvents] and [malformed]. It is the reason a
/// receiver's `rejected` refusal carries.
// Implements: EVS-DEV-delivery-receiver/A
// a batch that is not in the native batch format, one whose attributes is
//   not an object and one that carries no event are refused, each by its
//   own name.
class IngestDecodeFailure implements Exception {
  const IngestDecodeFailure(this.message, {this.reason = malformed});

  /// The batch is not in the format the decoder reads.
  static const String formatUnsupported = 'batch_format_unsupported';

  /// The batch's `attributes` is not a JSON object.
  static const String attributesNotObject = 'attributes_not_object';

  /// The batch carries no event.
  static const String noEvents = 'batch_empty';

  /// Every other malformedness: bytes that are not UTF-8 JSON, a missing,
  /// extra or mistyped field.
  static const String malformed = 'batch_malformed';

  final String message;

  /// The name of the refusal.
  final String reason;

  @override
  String toString() => 'IngestDecodeFailure($reason): $message';
}

/// Thrown by the receiver endpoint's delivery accept path when an incoming
/// event's data-format major differs from the receiver's
/// (`LibVersion.dataFormat`). The receiver reads no other data-format
/// major, so it refuses the event before any write, and a delivery
/// carrying such an event is refused whole. Operator action: run builds of
/// one data-format major on both sides.
class IngestDataFormatIncompatible implements Exception {
  const IngestDataFormatIncompatible({
    required this.eventId,
    required this.wireFormat,
    required this.receiverFormat,
  });

  /// The reason a receiver's `rejected` refusal names for this refusal.
  static const String refusalReason = 'data_format_incompatible';

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

/// Thrown by the receiver endpoint's delivery accept path when an incoming
/// event's entry-type major is above the major the receiver registers for
/// its entry type. The receiver refuses the event before any write, and a
/// delivery carrying such an event is refused whole. An event of the
/// registered major is accepted at any minor. Operator action: upgrade the
/// receiver's entry-type registry to the event's major.
class IngestEntryTypeVersionAhead implements Exception {
  const IngestEntryTypeVersionAhead({
    required this.eventId,
    required this.entryType,
    required this.wireVersion,
    required this.receiverVersion,
  });

  /// The reason a receiver's `rejected` refusal names for this refusal.
  static const String refusalReason = 'entry_type_version_ahead';
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

/// Thrown by the receiver endpoint's delivery accept path when an incoming
/// event is of a lower entry-type version than the receiver registers and
/// the receiver's promoter steps for a view the event folds into do not
/// lead from the event's version to the registered one: a lower major
/// with no major step registered from it, or a version past the start of
/// that major step. The receiver refuses the event before any write, and a
/// delivery carrying such an event is refused whole. Operator action:
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

  /// The reason a receiver's `rejected` refusal names for this refusal.
  static const String refusalReason = 'entry_type_version_unpromotable';
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

/// A delivery or a pull refused because the caller may not act for the
/// sending database it names: [senderDatabaseId] is not in the set of
/// sender database identities the deployment's authentication states for
/// the caller. Thrown before any read of a channel and any write; it
/// carries no receiver record, and the deployment's transport answers it as
/// an authentication refusal.
class DeliveryAuthenticationRefused implements Exception {
  const DeliveryAuthenticationRefused({required this.senderDatabaseId});

  /// The sending database the delivery's channel, or the pull, names.
  final String senderDatabaseId;

  @override
  String toString() =>
      'DeliveryAuthenticationRefused: the caller may not act for '
      'sender database $senderDatabaseId';
}

/// Thrown by `EventStore.restoreFromReceiver` before anything is stored,
/// naming one of the restore's refusals: the successor's log already holds
/// an authored event of an application entry type, the successor's log
/// already holds a succession event it authored, the named predecessor is
/// the successor's own identity, the receiver lists no channel for the
/// named predecessor, or a pull answered that it could not serve a
/// delivery the restore asked for. The two log-holds refusals are checked
/// before the restore pulls anything and checked again inside the
/// transaction that would store the restore, before any write, so a
/// disqualifying event appended between the two checks still refuses the
/// restore.
// Implements: EVS-DEV-sender-succession/H
// the restore refuses, before storing anything, into a successor whose
//   log holds an authored application event or an authored succession
//   event, one naming the successor's own identity, one the receiver
//   lists no channel for, and one whose pull cannot serve a delivery
//   asked for; the log-holds checks run again inside the storing
//   transaction.
// Implements: EVS-PRD-delivery-channel/R
// the restore refuses, before storing anything, into a database that has
//   authored an event of an application entry type.
class SuccessionRestoreRefused implements Exception {
  const SuccessionRestoreRefused(this.reason, this.message);

  /// The successor's log already holds an authored event of an
  /// application (non-reserved) entry type.
  static const String applicationEventAuthored = 'application_event_authored';

  /// The successor's log already holds a succession event it authored.
  static const String successionAlreadyAuthored = 'succession_already_authored';

  /// The named predecessor is the successor's own database identity.
  static const String predecessorIsSelf = 'predecessor_is_self';

  /// The receiver lists no channel for the named predecessor.
  static const String noChannelListed = 'no_channel_listed';

  /// A pull answered that it could not serve a delivery the restore asked
  /// for: `unservableDeliveryNumber` was set, or it served fewer
  /// deliveries than asked.
  static const String deliveryUnservable = 'delivery_unservable';

  /// The name of the refusal: one of the constants above.
  final String reason;

  /// A human-readable description of the refusal.
  final String message;

  @override
  String toString() => 'SuccessionRestoreRefused($reason): $message';
}
