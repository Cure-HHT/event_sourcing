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
  Future<EntryTypeVersion?> readViewTargetVersionInTxn(
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
    EntryTypeVersion targetVersion,
  ) => throw UnimplementedError();

  @override
  Future<Map<String, EntryTypeVersion>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearViewTargetVersionsInTxn(Transaction txn, String viewName) =>
      throw UnimplementedError();

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
  Future<void> appendAttemptTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<FifoEntry?> readFifoHeadTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<int> readFillCursorTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<DestinationSchedule?> readScheduleTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @override
  Future<Map<String, DestinationSchedule>> listSchedules() =>
      throw UnimplementedError();

  @internal
  @override
  Future<Map<String, DestinationSchedule>> listSchedulesTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<QueueRetirement> retireQueueTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<ReplayRequest?> readReplayRequestTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeReplayRequestTxn(
    Transaction txn,
    String destinationId,
    ReplayRequest request,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearReplayRequestTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<WedgeRecord?> readWedgeRecordTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeWedgeRecordTxn(
    Transaction txn,
    String destinationId,
    WedgeRecord record,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearWedgeRecordTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<HaltRequest?> readHaltRequestTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeHaltRequestTxn(
    Transaction txn,
    String destinationId,
    HaltRequest request,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearHaltRequestTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<SendFence?> readSendFenceTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeSendFenceTxn(
    Transaction txn,
    String destinationId,
    SendFence fence,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearSendFenceTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeRegistryCheckTxn(Transaction txn, RegistryCheck check) =>
      throw UnimplementedError();

  @internal
  @override
  Future<RegistryCheck?> readRegistryCheckTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<String?> readDatabaseIdTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<String> readOrCreateDatabaseIdTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeBootCheckTxn(Transaction txn, BootCheck check) =>
      throw UnimplementedError();

  @internal
  @override
  Future<BootCheck?> readBootCheckTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<T> bootTransaction<T>(Future<T> Function(Transaction txn) body) =>
      transaction(body);

  @internal
  @override
  Future<GenerationRegistration> registerGeneration(
    GenerationDescriptor descriptor,
  ) async => const _SingleProcessRegistration();

  @internal
  @override
  Object drainExclusionKey(String databaseId) => throw UnimplementedError();

  @internal
  @override
  Future<DrainLock> tryAcquireDrainLock({required String databaseId}) =>
      throw UnimplementedError();

  @internal
  @override
  DrainLockRequest requestDrainLock({
    required String databaseId,
    required Duration retryInterval,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<int?> readDrainEpochTxn(Transaction txn) => throw UnimplementedError();

  @internal
  @override
  Future<DrainerDeclaration?> readDrainerDeclarationTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeDrainerDeclarationTxn(
    Transaction txn,
    DrainerDeclaration declaration,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<DrainHeartbeat?> readDrainHeartbeatTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeDrainHeartbeatTxn(
    Transaction txn,
    DrainHeartbeat heartbeat,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<RefillGuard?> readRefillGuardTxn(
    Transaction txn,
    String destinationId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> writeRefillGuardTxn(
    Transaction txn,
    String destinationId,
    RefillGuard guard,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearRefillGuardTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();

  @internal
  @override
  Future<GenerationRecord?> readDataGenerationTxn(Transaction txn) =>
      throw UnimplementedError();

  @internal
  @override
  Future<void> writeDataGenerationTxn(
    Transaction txn,
    GenerationRecord record,
  ) => throw UnimplementedError();

  @internal
  @override
  Stream<StoredEvent> readEventsReverseInTxn(
    Transaction txn, {
    Set<String>? eventTypes,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<ChainIndexEntry?> readLatestHeldAsAuthoredInTxn(
    Transaction txn,
    String databaseId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<CausalRef?> readLatestEligibleVersionInTxn(
    Transaction txn,
    String aggregateId,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<List<ChainIndexEntry>> findChainIndexBySealedHashInTxn(
    Transaction txn,
    String sealedHash,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<List<ChainIndexEntry>> findChainIndexByPredecessorInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required String? previousEventHash,
  }) => throw UnimplementedError();

  @internal
  @override
  Future<List<ChainIndexEntry>> findChainIndexByOriginPositionInTxn(
    Transaction txn, {
    required String originatingDatabaseId,
    required int originPosition,
  }) => throw UnimplementedError();

  @override
  Future<Map<String, EntryTypeVersion>> readViewTargetsForEntryTypeInTxn(
    Transaction txn,
    String entryType,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> markViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => throw UnimplementedError();

  @override
  Future<bool> readViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> clearViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
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
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) =>
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
    FinalStatus status,
  ) => throw UnimplementedError();

  @internal
  @override
  Future<TrailSweepResult> deleteNullRowsAfterSequenceInQueueTxn(
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

/// The registration of a backend used by one process: it holds nothing. A
/// backend several processes share implements the generation guard
/// instead.
final class _SingleProcessRegistration extends GenerationRegistration {
  const _SingleProcessRegistration();

  @override
  bool get isLost => false;

  @internal
  @override
  Future<void> recordInTxn(Transaction txn) async {}

  @override
  Future<void> completeBoot() async {}

  @override
  Future<void> release() async {}
}
