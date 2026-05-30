import 'package:collection/collection.dart';
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';

/// Inclusive pair of sequence numbers drawn from the events in a batch
/// FIFO row. `firstSeq` is the minimum `sequence_number` across the batch
/// and `lastSeq` is the maximum; for a single-event batch they are equal.
///
/// Declared as a Dart 3 record typedef so callers read the pair positionally
/// (or via the named fields) without needing a dedicated class. Cursor-
/// advancement math in the drain/fill-batch paths uses `lastSeq` as the
/// inclusive upper bound of the batch.
typedef SequenceRange = ({int firstSeq, int lastSeq});

/// One row in a destination's FIFO store — a batch of one or more events
/// transformed together into a single wire-ready payload.
///
/// Each row is a transformed copy of a contiguous slice of the event log
/// destined for a specific synchronization target. Rows are appended in
/// strict order on write and are never reordered. `finalStatus` is
/// nullable; `null` means "not-yet-terminal" (drain may attempt the
/// row). Once delivered they are marked `FinalStatus.sent`; on
/// permanent failure they are marked `FinalStatus.wedged`; rows excised
/// by a trail sweep are marked `FinalStatus.tombstoned`. All non-null
/// terminal states are retained forever as send-log / audit records.
///
/// `eventIds` is a non-empty `List<String>`, `sequenceRange` is an
/// `(firstSeq, lastSeq)` record, and `wirePayload` is one payload for
/// the whole batch (no per-event payload is stored).
// Implements: EVS-PRD-portability/C — pure Dart value type; serialises
//   identically on every Dart-supported runtime; no platform dependency.
// Implements: EVS-PRD-portability/D — part of the platform-agnostic
//   StorageBackend abstraction layer (FIFO persistence).
// final_status is nullable (null means not-yet-terminal; non-null
// values are one of {sent, wedged, tombstoned}).
// wireFormat == "esd/batch@1" (native), in which case envelopeMetadata
// is non-null and drain reconstructs the wire bytes from
// envelopeMetadata + event_ids-resolved events. For 3rd-party rows
// (any other wireFormat) wirePayload is non-null and envelopeMetadata
// is null.
class FifoEntry {
  FifoEntry({
    required this.entryId,
    required this.eventIds,
    required this.sequenceRange,
    required this.sequenceInQueue,
    required this.wireFormat,
    required this.transformVersion,
    required this.enqueuedAt,
    required this.attempts,
    required this.finalStatus,
    required this.sentAt,
    this.wirePayload,
    this.envelopeMetadata,
  }) {
    // Explicit ArgumentError rather than assert so the invariant is
    // enforced in release builds too, not just debug.
    if (eventIds.isEmpty) {
      throw ArgumentError.value(
        eventIds,
        'eventIds',
        'FifoEntry.eventIds must be non-empty',
      );
    }
    // sequence numbers; the pair MUST be ordered (firstSeq <= lastSeq).
    if (sequenceRange.firstSeq > sequenceRange.lastSeq) {
      throw ArgumentError.value(
        sequenceRange,
        'sequenceRange',
        'sequenceRange.firstSeq (${sequenceRange.firstSeq}) must be '
            '<= lastSeq (${sequenceRange.lastSeq})',
      );
    }
  }

  /// Decode from snake_case JSON. `wirePayload`, `attempts`, and
  /// `eventIds` are wrapped unmodifiable so downstream callers cannot
  /// mutate the record in place. `wire_payload` MAY be null (native
  /// `esd/batch@1` rows store envelope_metadata instead).
  /// `envelope_metadata` MAY be null (3rd-party rows). Throws
  /// [FormatException] on missing or wrong-typed fields, or when
  /// `event_ids` is empty.
  factory FifoEntry.fromJson(Map<String, Object?> json) {
    final entryId = json['entry_id'];
    if (entryId is! String) {
      throw const FormatException(
        'FifoEntry: missing or non-string "entry_id"',
      );
    }
    final eventIdsRaw = json['event_ids'];
    if (eventIdsRaw is! List) {
      throw const FormatException('FifoEntry: missing or non-List "event_ids"');
    }
    if (eventIdsRaw.isEmpty) {
      throw const FormatException('FifoEntry: "event_ids" must be non-empty');
    }
    final eventIds = <String>[];
    for (final e in eventIdsRaw) {
      if (e is! String) {
        throw const FormatException(
          'FifoEntry: every element of "event_ids" must be a String',
        );
      }
      eventIds.add(e);
    }
    final eventIdRangeRaw = json['event_id_range'];
    if (eventIdRangeRaw is! Map) {
      throw const FormatException(
        'FifoEntry: missing or non-Map "event_id_range"',
      );
    }
    final firstSeq = eventIdRangeRaw['first_seq'];
    if (firstSeq is! int) {
      throw const FormatException(
        'FifoEntry: "event_id_range.first_seq" must be an int',
      );
    }
    final lastSeq = eventIdRangeRaw['last_seq'];
    if (lastSeq is! int) {
      throw const FormatException(
        'FifoEntry: "event_id_range.last_seq" must be an int',
      );
    }
    final seqInQueue = json['sequence_in_queue'];
    if (seqInQueue is! int) {
      throw const FormatException(
        'FifoEntry: missing or non-int "sequence_in_queue"',
      );
    }
    // null for native `esd/batch@1` rows; non-null and a Map on 3rd-party rows. Reject
    // any other shape.
    final wirePayloadRaw = json['wire_payload'];
    if (wirePayloadRaw != null && wirePayloadRaw is! Map) {
      throw const FormatException(
        'FifoEntry: "wire_payload" must be a Map or null',
      );
    }
    final wireFormat = json['wire_format'];
    if (wireFormat is! String) {
      throw const FormatException(
        'FifoEntry: missing or non-string "wire_format"',
      );
    }
    final transformVersionRaw = json['transform_version'];
    if (transformVersionRaw != null && transformVersionRaw is! String) {
      throw const FormatException(
        'FifoEntry: "transform_version" must be a String when present',
      );
    }
    final enqueuedAtRaw = json['enqueued_at'];
    if (enqueuedAtRaw is! String) {
      throw const FormatException(
        'FifoEntry: missing or non-string "enqueued_at"',
      );
    }
    final attemptsRaw = json['attempts'];
    if (attemptsRaw is! List) {
      throw const FormatException('FifoEntry: missing or non-List "attempts"');
    }
    final finalStatusRaw = json['final_status'];
    if (finalStatusRaw != null && finalStatusRaw is! String) {
      throw const FormatException(
        'FifoEntry: "final_status" must be a String or null',
      );
    }
    final finalStatus = finalStatusRaw == null
        ? null
        : FinalStatus.fromJson(finalStatusRaw as String);
    final sentAtRaw = json['sent_at'];
    if (sentAtRaw != null && sentAtRaw is! String) {
      throw const FormatException(
        'FifoEntry: "sent_at" must be a String when present',
      );
    }
    // Non-null iff wireFormat == "esd/batch@1".
    final envelopeMetadataRaw = json['envelope_metadata'];
    if (envelopeMetadataRaw != null && envelopeMetadataRaw is! Map) {
      throw const FormatException(
        'FifoEntry: "envelope_metadata" must be a Map or null',
      );
    }

    final attempts = List<AttemptResult>.unmodifiable(
      attemptsRaw.map(
        (e) => AttemptResult.fromJson(Map<String, Object?>.from(e as Map)),
      ),
    );
    return FifoEntry(
      entryId: entryId,
      eventIds: List<String>.unmodifiable(eventIds),
      sequenceRange: (firstSeq: firstSeq, lastSeq: lastSeq),
      sequenceInQueue: seqInQueue,
      wirePayload: wirePayloadRaw == null
          ? null
          : Map<String, Object?>.unmodifiable(
              Map<String, Object?>.from(wirePayloadRaw as Map),
            ),
      wireFormat: wireFormat,
      transformVersion: transformVersionRaw as String?,
      enqueuedAt: DateTime.parse(enqueuedAtRaw),
      attempts: attempts,
      finalStatus: finalStatus,
      sentAt: sentAtRaw == null ? null : DateTime.parse(sentAtRaw as String),
      envelopeMetadata: envelopeMetadataRaw == null
          ? null
          : BatchEnvelopeMetadata.fromMap(
              Map<String, Object?>.from(envelopeMetadataRaw as Map),
            ),
    );
  }

  /// Stable per-row identifier used by `markFinal`, `appendAttempt`,
  /// `tombstoneAndRefill`, and operator diagnostics. Generated as a v4
  /// UUID at enqueue time and never reused across rows; two FIFO rows
  /// (of any `final_status`, including tombstoned archive rows) never
  /// share an `entryId`. The identifier has no relationship to the
  /// events the row carries — callers that need to correlate against
  /// events should use `eventIds` or `sequenceRange` instead.
  final String entryId;

  /// Event_ids of every event included in this batch row, in the order they
  /// were batched. Always non-empty — enforced at
  /// construction and rechecked on `fromJson`. Preserved for audit and for
  /// idempotent redelivery.
  final List<String> eventIds;

  /// Inclusive `(first_seq, last_seq)` pair drawn from the sequence_numbers
  /// of the events in this batch. Used for cursor advancement math in the
  /// drain and fill-batch paths — `lastSeq` is the upper bound of the batch
  /// on the event log. For a single-event batch, `firstSeq == lastSeq`.
  final SequenceRange sequenceRange;

  /// Insertion-order position in this FIFO; monotonic per destination.
  final int sequenceInQueue;

  /// Transformed wire payload ready to hand to `destination.send()`. One
  /// payload covers every event in the batch; per-event
  /// wire payloads are NOT stored. Null when `wireFormat == "esd/batch@1"`
  /// (native rows reconstruct bytes at drain time from
  /// [envelopeMetadata] + event_ids-resolved events);
  /// non-null otherwise.
  final Map<String, Object?>? wirePayload;

  /// Wire-format discriminator (e.g., `"json-v1"`, `"fhir-r4"`).
  final String wireFormat;

  /// Version of the transform that produced `wirePayload`; null for
  /// pass-through (identity transform).
  final String? transformVersion;

  /// When the entry was appended to the FIFO (equal to the enclosing
  /// write transaction commit instant for all practical purposes).
  final DateTime enqueuedAt;

  /// Historical send attempts; grows, never shrinks; retained forever per
  ///
  final List<AttemptResult> attempts;

  /// Terminal state of this entry. `null` on enqueue and while the row is
  /// still a drain candidate; moves to `sent`, `wedged`, or `tombstoned`
  /// on a terminal transition. Non-null terminal values are retained
  /// forever as audit records.
  final FinalStatus? finalStatus;

  /// When the entry was marked `sent`; null while pre-terminal, wedged,
  /// or tombstoned.
  final DateTime? sentAt;

  /// Envelope identity for native (`esd/batch@1`) FIFO rows. Carries the
  /// `batchFormatVersion`, `batchId`, sender identity (`senderHop`,
  /// `senderIdentifier`, `senderSoftwareVersion`), and `sentAt` of the
  /// `BatchEnvelope` parsed at enqueue time. Drain combines this with
  /// `eventIds`-resolved events to re-encode the wire bytes
  /// deterministically (RFC 8785 JCS) on each send attempt. Non-null
  /// iff `wireFormat == "esd/batch@1"`; null for 3rd-party rows.
  final BatchEnvelopeMetadata? envelopeMetadata;

  /// Encode to snake_case JSON. Optional fields emit explicit null.
  Map<String, Object?> toJson() => <String, Object?>{
    'entry_id': entryId,
    'event_ids': eventIds,
    'event_id_range': <String, Object?>{
      'first_seq': sequenceRange.firstSeq,
      'last_seq': sequenceRange.lastSeq,
    },
    'sequence_in_queue': sequenceInQueue,
    'wire_payload': wirePayload,
    'wire_format': wireFormat,
    'transform_version': transformVersion,
    'enqueued_at': enqueuedAt.toIso8601String(),
    'attempts': attempts.map((a) => a.toJson()).toList(),
    'final_status': finalStatus?.toJson(),
    'sent_at': sentAt?.toIso8601String(),
    'envelope_metadata': envelopeMetadata?.toMap(),
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is FifoEntry &&
          entryId == other.entryId &&
          const ListEquality<String>().equals(eventIds, other.eventIds) &&
          sequenceRange == other.sequenceRange &&
          sequenceInQueue == other.sequenceInQueue &&
          _deepEquals.equals(wirePayload, other.wirePayload) &&
          wireFormat == other.wireFormat &&
          transformVersion == other.transformVersion &&
          enqueuedAt == other.enqueuedAt &&
          const ListEquality<AttemptResult>().equals(
            attempts,
            other.attempts,
          ) &&
          finalStatus == other.finalStatus &&
          sentAt == other.sentAt &&
          envelopeMetadata == other.envelopeMetadata;

  @override
  int get hashCode => Object.hash(
    entryId,
    const ListEquality<String>().hash(eventIds),
    sequenceRange,
    sequenceInQueue,
    _deepEquals.hash(wirePayload),
    wireFormat,
    transformVersion,
    enqueuedAt,
    const ListEquality<AttemptResult>().hash(attempts),
    finalStatus,
    sentAt,
    envelopeMetadata,
  );

  @override
  String toString() {
    final envelopeBit = envelopeMetadata == null
        ? ''
        : ', envelopeMetadata: $envelopeMetadata';
    return 'FifoEntry(entryId: $entryId, eventIds: $eventIds, '
        'sequenceRange: (firstSeq: ${sequenceRange.firstSeq}, '
        'lastSeq: ${sequenceRange.lastSeq}), '
        'sequenceInQueue: $sequenceInQueue, wireFormat: $wireFormat, '
        'finalStatus: $finalStatus, attempts: ${attempts.length}'
        '$envelopeBit)';
  }
}

const DeepCollectionEquality _deepEquals = DeepCollectionEquality();
