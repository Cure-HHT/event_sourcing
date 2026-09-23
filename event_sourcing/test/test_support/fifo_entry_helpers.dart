import 'dart:convert';
import 'dart:typed_data';

import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/versions.dart';

/// Build a minimal `StoredEvent` fixture with the given id and sequence
/// number. Tests that need a batch input to `StorageBackend.enqueueFifoTxn`
/// construct one via `[storedEventFixture(...)]`.
StoredEvent storedEventFixture({
  required String eventId,
  required int sequenceNumber,
  String aggregateId = 'agg-1',
  String entryType = 'epistaxis_event',
  String eventType = 'finalized',
}) => StoredEvent(
  key: 0,
  eventId: eventId,
  aggregateId: aggregateId,
  aggregateType: 'note',
  entryType: entryType,
  entryTypeVersion: const EntryTypeVersion(1, 0),
  libFormatVersion: const DataFormatVersion(2, 0),
  eventType: eventType,
  sequenceNumber: sequenceNumber,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026, 4, 22),
  eventHash: 'hash-$eventId',
);

/// Build a `WirePayload` whose bytes encode [payload] as JSON. The
/// `StorageBackend.enqueueFifoTxn` requires a JSON-object
/// payload so it can persist the decoded map into the FIFO row.
WirePayload wirePayloadJson(
  Map<String, Object?> payload, {
  String contentType = 'json-v1',
  String? transformVersion = 'json-v1',
}) => WirePayload(
  bytes: Uint8List.fromList(utf8.encode(jsonEncode(payload))),
  contentType: contentType,
  transformVersion: transformVersion,
);

/// Convenience: enqueue a single-event batch through the batch-aware
/// `StorageBackend.enqueueFifoTxn`. Wraps [eventId] + [sequenceNumber] in a
/// one-element batch and a JSON-encoded wire payload; returns the
/// persisted `FifoEntry`.
Future<FifoEntry> enqueueSingle(
  StorageBackend backend,
  String destinationId, {
  required String eventId,
  int sequenceNumber = 1,
  Map<String, Object?>? wirePayload,
  String wireFormat = 'json-v1',
  String? transformVersion = 'json-v1',
}) => backend.transaction(
  (txn) => backend.enqueueFifoTxn(
    txn,
    destinationId,
    [storedEventFixture(eventId: eventId, sequenceNumber: sequenceNumber)],
    wirePayload: wirePayloadJson(
      wirePayload ?? const <String, Object?>{'ok': true},
      contentType: wireFormat,
      transformVersion: transformVersion,
    ),
  ),
);

FifoEntry singleEventFifoEntry({
  required String entryId,
  required String eventId,
  required int sequenceNumber,
  required int sequenceInQueue,
  Map<String, Object?>? wirePayload,
  String wireFormat = 'json-v1',
  String? transformVersion,
  DateTime? enqueuedAt,
  List<AttemptResult>? attempts,
  FinalStatus? finalStatus,
  DateTime? sentAt,
}) => FifoEntry(
  entryId: entryId,
  eventIds: [eventId],
  sequenceRange: (firstSeq: sequenceNumber, lastSeq: sequenceNumber),
  sequenceInQueue: sequenceInQueue,
  wirePayload: wirePayload ?? const {'ok': true},
  wireFormat: wireFormat,
  transformVersion: transformVersion,
  enqueuedAt: enqueuedAt ?? DateTime.utc(2026, 4, 22),
  attempts: attempts ?? const [],
  finalStatus: finalStatus,
  sentAt: sentAt,
);

/// Set a queue item's `final_status` through the storage contract, in its
/// own transaction. Backend-level tests use it on a bare backend; a
/// `wedged -> tombstoned` step must follow a `null -> wedged` one.
Future<void> setStatusForTest(
  StorageBackend backend,
  String destinationId,
  String entryId,
  FinalStatus status,
) => backend.transaction(
  (txn) => backend.setFinalStatusTxn(txn, destinationId, entryId, status),
);

/// Mark a pending item `sent` through the storage contract.
Future<void> seedSentRowForTest(
  StorageBackend backend,
  String destinationId,
  String entryId,
) => setStatusForTest(backend, destinationId, entryId, FinalStatus.sent);

/// Record [attempt] on a pending item through the storage contract.
Future<void> appendAttemptForTest(
  StorageBackend backend,
  String destinationId,
  String entryId,
  AttemptResult attempt,
) => backend.transaction(
  (txn) => backend.appendAttemptTxn(txn, destinationId, entryId, attempt),
);

/// Write a destination's fill cursor through the storage contract.
Future<void> writeFillCursorForTest(
  StorageBackend backend,
  String destinationId,
  int sequenceNumber,
) => backend.transaction(
  (txn) => backend.writeFillCursorTxn(txn, destinationId, sequenceNumber),
);
