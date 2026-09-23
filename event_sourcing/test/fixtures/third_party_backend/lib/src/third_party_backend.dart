import 'package:event_sourcing/event_sourcing.dart';
import 'package:meta/meta.dart';

/// A third-party backend written against the barrel alone, in a package of
/// its own. It overrides every member of the contract and marks each
/// override of an internal member `@internal`, so a package that depends on
/// this one and calls such a member through this concrete type is reported.
/// Overriding an internal member is not a use of it, and annotating the
/// override is not an error, so this file analyzes clean. The annotations
/// sit in `lib/src/`: `@internal` on a declaration in a public library is
/// itself a diagnostic.
class ThirdPartyBackend extends StorageBackend {
  const ThirdPartyBackend();

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) =>
      throw UnimplementedError();

  @internal
  @override
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event) =>
      throw UnimplementedError();

  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) =>
      throw UnimplementedError();

  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) => throw UnimplementedError();

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) => throw UnimplementedError();

  @override
  Future<String?> readLatestEventHash(Transaction txn) =>
      throw UnimplementedError();

  @override
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<int> nextSequenceNumber(Transaction txn) => throw UnimplementedError();

  @override
  Future<int> readSequenceCounter() => throw UnimplementedError();

  @override
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> deleteViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  ) => throw UnimplementedError();

  @override
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  }) => throw UnimplementedError();

  @override
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  ) => throw UnimplementedError();

  @override
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearViewInTxn(Transaction txn, String viewName) =>
      throw UnimplementedError();

  @override
  Future<int?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
    int targetVersion,
  ) => throw UnimplementedError();

  @override
  Future<Map<String, int>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearViewTargetVersionsInTxn(Transaction txn, String viewName) =>
      throw UnimplementedError();

  @internal
  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<FifoEntry> enqueueFifoTxn(
    Transaction txn,
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => throw UnimplementedError();

  @override
  Future<FifoEntry?> readFifoHead(String destinationId) =>
      throw UnimplementedError();

  @override
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  ) => throw UnimplementedError();

  @override
  Future<bool> hasFifoWedged() => throw UnimplementedError();

  @override
  Future<List<WedgedFifoSummary>> wedgedFifos() => throw UnimplementedError();

  @override
  Future<int> readSchemaVersion() => throw UnimplementedError();

  @internal
  @override
  Future<void> writeSchemaVersion(Transaction txn, int version) =>
      throw UnimplementedError();

  @override
  Future<int> readFillCursor(String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeFillCursor(String destinationId, int sequenceNumber) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeFillCursorTxn(
    Transaction txn,
    String destinationId,
    int sequenceNumber,
  ) => throw UnimplementedError();

  @override
  Future<StoredEvent?> findEventByIdInTxn(Transaction txn, String eventId) =>
      throw UnimplementedError();

  @override
  Future<StoredEvent?> findEventById(String eventId) =>
      throw UnimplementedError();

  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeSchedule(
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> deleteFifoStoreTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  ) => throw UnimplementedError();

  @override
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes}) =>
      throw UnimplementedError();

  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => throw UnimplementedError();

  @override
  Future<void> close() => throw UnimplementedError();
}
