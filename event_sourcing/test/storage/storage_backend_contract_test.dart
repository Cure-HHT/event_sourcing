// Verifies: EVS-PRD-portability/D
// StorageBackend interface is implementable
//   by any concrete backend. The general transaction-contract behaviors
//   (commit, rollback, Transaction-after-body, sequential-commit) are lifted into
//   the backend-agnostic harness in `storage_backend_conformance.dart` and
//   exercised against every concrete backend.
//
// What remains in this file: one test that exercises the *fake* in-memory
// backend, asserting the implementation choice this fake makes about
// nested transactions. The abstract contract is silent on whether a
// backend must support nested transactions; this test documents the
// fake's chosen behavior (reject) so a future second concrete backend
// has a known starting point. The matching SembastBackend behavior is
// covered by the implementation's own integration tests, not here.
import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/destination_schedule.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/append_result.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/fifo_entry.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/wedged_fifo_summary.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('StorageBackend contract — fake-only', () {
    late _InMemoryBackend backend;

    setUp(() {
      backend = _InMemoryBackend();
    });

    // The abstract contract is silent on whether a backend must support
    // nested transactions. This fake backend rejects them (matches Sembast,
    // which does not support re-entrant transactions either). Documenting
    // the behavior here so a second concrete backend has a known starting
    // point: if nested transactions become legal later, either relax this
    // test or make it implementation-specific.
    test('nested transaction on this fake throws', () async {
      await expectLater(
        backend.transaction((outer) async {
          await backend.transaction((inner) async {});
        }),
        throwsStateError,
      );
    });
  });
}

// -------- Fake backend --------

/// Minimal in-memory backend used only by the nested-transaction test.
///
/// Implements just enough behavior to construct a Transaction and reject nested
/// transactions. All other methods throw [UnimplementedError] — the
/// abstract StorageBackend contract is covered by the backend-agnostic
/// conformance harness exercising concrete implementations.
class _InMemoryBackend extends StorageBackend {
  /// Committed state: event_id -> stored event.
  final Map<String, StoredEvent> _events = <String, StoredEvent>{};

  /// Non-null while a [transaction] body is running; mutations inside the
  /// body go here and are promoted to [_events] only on commit.
  Map<String, StoredEvent>? _staged;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) async {
    if (_staged != null) {
      throw StateError('Nested transactions are not supported by this fake');
    }
    _staged = Map<String, StoredEvent>.from(_events);
    final txn = _InMemoryTxn(this);
    try {
      final result = await body(txn);
      _events
        ..clear()
        ..addAll(_staged!);
      return result;
    } finally {
      _staged = null;
      txn._invalidate();
    }
  }

  @override
  Future<AppendResult> appendEvent(Transaction txn, StoredEvent event) async {
    _assertOwnValid(txn)._check();
    _staged![event.eventId] = event;
    return AppendResult(
      sequenceNumber: event.sequenceNumber,
      eventHash: event.eventHash,
    );
  }

  @override
  Future<List<StoredEvent>> findAllEvents({
    int? afterSequence,
    int? limit,
    String? originatorHopId,
    String? originatorIdentifier,
    String? entryType,
    DateTime? clientTimestampStart,
    DateTime? clientTimestampEnd,
  }) async => throw UnimplementedError();

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
  }) async => throw UnimplementedError();

  _InMemoryTxn _assertOwnValid(Transaction txn) {
    if (txn is! _InMemoryTxn || txn._backend != this) {
      throw StateError('Transaction does not belong to this backend');
    }
    return txn;
  }

  // The remaining methods are not exercised by the fake-only test.
  @override
  Future<List<StoredEvent>> findEventsForAggregate(String aggregateId) =>
      throw UnimplementedError();
  @override
  Future<List<StoredEvent>> findEventsForAggregateInTxn(
    Transaction txn,
    String aggregateId,
  ) => throw UnimplementedError();
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
  @override
  Future<void> upsertViewRowInTxn(
    Transaction txn,
    String viewName,
    String key,
    Map<String, dynamic> row,
  ) => throw UnimplementedError();
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
  @override
  Future<void> clearViewInTxn(Transaction txn, String viewName) =>
      throw UnimplementedError();
  @override
  Future<int?> readViewTargetVersionInTxn(
    Transaction txn,
    String viewName,
    String entryType,
  ) => throw UnimplementedError();
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
  @override
  Future<void> clearViewTargetVersionsInTxn(Transaction txn, String viewName) =>
      throw UnimplementedError();
  @override
  Future<FifoEntry> enqueueFifo(
    String destinationId,
    List<StoredEvent> batch, {
    WirePayload? wirePayload,
    BatchEnvelopeMetadata? nativeEnvelope,
  }) => throw UnimplementedError();
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
  @override
  Future<void> appendAttempt(
    String destinationId,
    String entryId,
    AttemptResult attempt,
  ) => throw UnimplementedError();
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
  @override
  Future<void> writeSchemaVersion(Transaction txn, int version) =>
      throw UnimplementedError();
  @override
  Future<int> readFillCursor(String destinationId) =>
      throw UnimplementedError();
  @override
  Future<void> writeFillCursor(String destinationId, int sequenceNumber) =>
      throw UnimplementedError();
  @override
  Future<void> writeFillCursorTxn(
    Transaction txn,
    String destinationId,
    int sequenceNumber,
  ) => throw UnimplementedError();
  @override
  Future<DestinationSchedule?> readSchedule(String destinationId) =>
      throw UnimplementedError();
  @override
  Future<void> writeSchedule(
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError();
  @override
  Future<void> writeScheduleTxn(
    Transaction txn,
    String destinationId,
    DestinationSchedule schedule,
  ) => throw UnimplementedError();
  @override
  Future<void> deleteScheduleTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();
  @override
  Future<void> deleteFifoStoreTxn(Transaction txn, String destinationId) =>
      throw UnimplementedError();
  @override
  Future<FifoEntry?> readFifoRow(String destinationId, String entryId) =>
      throw UnimplementedError();
  @override
  Future<void> setFinalStatusTxn(
    Transaction txn,
    String destinationId,
    String entryId,
    FinalStatus? status,
  ) => throw UnimplementedError();
  @override
  Future<int> deleteNullRowsAfterSequenceInQueueTxn(
    Transaction txn,
    String destinationId,
    int afterSequenceInQueue,
  ) => throw UnimplementedError();
  @override
  Future<StoredEvent?> findEventByIdInTxn(Transaction txn, String eventId) =>
      throw UnimplementedError();
  @override
  Future<StoredEvent?> findEventById(String eventId) =>
      throw UnimplementedError();
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
  }) async => const PagedAudit(rows: <AuditRow>[]);
  @override
  Future<void> close() async {}
}

class _InMemoryTxn extends Transaction {
  _InMemoryTxn(this._backend);
  final _InMemoryBackend _backend;
  bool _valid = true;

  void _invalidate() {
    _valid = false;
  }

  void _check() {
    if (!_valid) {
      throw StateError('Transaction used outside its transaction() body');
    }
  }
}
