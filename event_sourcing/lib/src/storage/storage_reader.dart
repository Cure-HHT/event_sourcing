import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:event_sourcing/src/verification/chain_verification_verdict.dart';
import 'package:event_sourcing/src/versions.dart';

/// Reads of the storage an event store runs on: the event log, the views,
/// the destination queues and schedules, and the security-context audit.
///
/// The event store hands it out as `EventStore.reader`. It is an object of
/// its own, not the storage backend, and declares no member that writes;
/// each read returns what the backend's read of the same name returns.
///
/// [transaction] runs its body in one transaction for reads only. The
/// handle the body receives is accepted by this reader's `...InTxn` reads
/// while the body runs, and refused, with [StateError], by every append of
/// the event store and after the body returns. The reads also accept a
/// handle the event store's `runTransaction` passes its body, while that
/// body runs, so a decision read and the append it guards share one
/// transaction. A handle of any other event store or reader is refused
/// with [StateError].
// Implements: EVS-DEV-storage-capability/E
// the storage reader is an interface of reads only; the object the event
//   store hands out implements it alone.
abstract interface class StorageReader {
  /// Runs [body] in one transaction for reads only, and returns its result.
  /// On Postgres the transaction runs `READ ONLY` at the database. As with
  /// the backend's transactions, [body] may run more than once before one
  /// run completes.
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body);

  /// Events for one aggregate, in sequence order.
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId);

  /// [findEventsForAggregate] inside [txn].
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  );

  /// Events of the log, in sequence order, filtered by the arguments.
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  });

  /// [findAllEvents] inside [txn].
  Future<List<StoredEvent>> findAllEventsInTxn(
    Transaction txn, {
    int? afterSequence,
    int? limit,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  });

  /// The hash of the latest event of the log, or null for an empty log.
  Future<String?> readLatestEventHash(Transaction txn);

  /// The sequence number of the latest event of the log.
  Future<int> readSequenceCounter();

  /// One event by its id, or null.
  Future<StoredEvent?> findEventById(String eventId);

  /// [findEventById] inside [txn].
  Future<StoredEvent?> findEventByIdInTxn(Transaction txn, String eventId);

  /// The events of the log, newest first.
  Stream<StoredEvent> readEventsReverse({Set<String>? eventTypes});

  /// One row of [viewName] by its key, or null.
  Future<Map<String, dynamic>?> readViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
  );

  /// The rows of [viewName].
  Future<List<Map<String, dynamic>>> findViewRows(
    String viewName, {
    int? limit,
    int? offset,
  });

  /// The rows of [viewName] whose keys are in [keys], by key.
  Future<Map<String, Map<String, dynamic>>> readViewRowsByKeys(
    String viewName,
    Set<String> keys,
  );

  /// The rows of [viewName] inside [txn], optionally only those whose
  /// fields equal [where].
  Future<List<Map<String, dynamic>>> findViewRowsInTxn(
    Transaction txn,
    String viewName, {
    Map<String, Object?>? where,
    int? limit,
    int? offset,
  });

  /// The version [viewName] holds [entryType]'s rows at, or null.
  Future<EntryTypeVersion?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  );

  /// Every entry type's version [viewName] holds its rows at.
  Future<Map<String, EntryTypeVersion>> readAllViewTargetVersionsInTxn(
    Transaction txn,
    String viewName,
  );

  /// Every view's version of [entryType]'s rows, by view name.
  Future<Map<String, EntryTypeVersion>> readViewTargetsForEntryTypeInTxn(
    Transaction txn,
    String entryType,
  );

  /// Whether [viewName]'s rows of [entryType] are marked behind.
  Future<bool> readViewTargetBehindInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  );

  /// The head of [destinationId]'s queue, or null.
  Future<FifoEntry?> readFifoHead(String destinationId);

  /// The items of [destinationId]'s queue, in queue order.
  Future<List<FifoEntry>> listFifoEntries(
    String destinationId, {
    int? afterSequenceInQueue,
    int? limit,
  });

  /// One item of [destinationId]'s queue, or null.
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId);

  /// Whether any queue head is wedged.
  Future<bool> hasFifoWedged();

  /// The wedged queue heads.
  Future<List<WedgedFifoSummary>> wedgedFifos();

  /// The storage schema version.
  Future<int> readSchemaVersion();

  /// The fill position of [destinationId], or -1.
  Future<int> readFillCursor(String destinationId);

  /// The stored schedule of [destinationId], or null.
  Future<DestinationSchedule?> readSchedule(String destinationId);

  /// Every stored destination schedule, by destination id.
  Future<Map<String, DestinationSchedule>> listSchedules();

  /// The verdict `EventStore.verifyChains` returns over the local
  /// sequence numbers [from] to [to], computed from the log alone and
  /// recording nothing: the reads hold no transaction an append waits for.
  /// Throws [ArgumentError], before reading any event, for a negative bound
  /// or a lower bound above the upper.
  Future<ChainVerificationVerdict> verifyChains({int? from, int? to});

  /// The events joined with their security contexts, filtered and paged
  /// as the backend's `queryAudit` documents.
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  });
}
