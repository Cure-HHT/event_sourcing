part of '../storage/sembast_backend.dart';

/// Sembast-backed `SecurityContextStore`. Maintains one sembast store
/// (`security_context`) keyed on `event_id`. Cross-store reads (the
/// security_context + events join) live on the backend via
/// [SembastBackend.queryAudit]; this store's [queryAudit] is a thin
/// delegator.
// Implements: EVS-PRD-event-log/A
// all mutations accept a caller-supplied
//   `Transaction` so they commit atomically with the event-log row they describe.
// Implements: EVS-PRD-regulatory-alignment
// `findUnredactedOlderThanInTxn`
//   and `findOlderThanInTxn` drive the retention compact/purge sweeps that
//   satisfy ALCOA+ Enduring / §11.10(c) protection-of-records obligations.

class SembastSecurityContextStore extends MutableSecurityContextStore {
  /// A store over [backend]: the event store builds one over the storage
  /// it opens, and an application builds one for a backend it constructed
  /// and names as application-supplied storage.
  SembastSecurityContextStore({required SembastBackend backend})
    : _backend = backend;

  final SembastBackend _backend;

  final StoreRef<String, Map<String, Object?>> _store = stringMapStoreFactory
      .store('security_context');

  @override
  Future<EventSecurityContext?> read(String eventId) {
    return _backend.transaction((txn) => readInTxn(txn, eventId));
  }

  @override
  Future<EventSecurityContext?> readInTxn(
    Transaction txn,
    String eventId,
  ) async {
    final sembastTxn = _castTxn(txn);
    final raw = await _store.record(eventId).get(sembastTxn);
    if (raw == null) return null;
    return EventSecurityContext.fromJson(Map<String, Object?>.from(raw));
  }

  @internal
  @override
  Future<void> writeInTxn(Transaction txn, EventSecurityContext row) async {
    final sembastTxn = _castTxn(txn);
    await _store.record(row.eventId).put(sembastTxn, row.toJson());
  }

  @internal
  @override
  Future<void> upsertInTxn(Transaction txn, EventSecurityContext row) =>
      writeInTxn(txn, row);

  @internal
  @override
  Future<void> deleteInTxn(Transaction txn, String eventId) async {
    final sembastTxn = _castTxn(txn);
    await _store.record(eventId).delete(sembastTxn);
  }

  @override
  Future<List<EventSecurityContext>> findUnredactedOlderThanInTxn(
    Transaction txn,
    DateTime cutoff,
  ) async {
    final sembastTxn = _castTxn(txn);
    final finder = Finder(
      filter: Filter.and([
        Filter.isNull('redacted_at'),
        recordedAtNotAfter(cutoff),
      ]),
    );
    final records = await _store.find(sembastTxn, finder: finder);
    return records
        .map(
          (r) =>
              EventSecurityContext.fromJson(Map<String, Object?>.from(r.value)),
        )
        .toList();
  }

  @override
  Future<List<EventSecurityContext>> findOlderThanInTxn(
    Transaction txn,
    DateTime cutoff,
  ) async {
    final sembastTxn = _castTxn(txn);
    final finder = Finder(filter: recordedAtNotAfter(cutoff));
    final records = await _store.find(sembastTxn, finder: finder);
    return records
        .map(
          (r) =>
              EventSecurityContext.fromJson(Map<String, Object?>.from(r.value)),
        )
        .toList();
  }

  // Cross-store reads (security_context + events join) live on the backend
  // so consumers cannot reach past the abstraction to perform their own joins.
  @override
  Future<PagedAudit> queryAudit({
    Initiator? initiator,
    String? flowToken,
    String? ipAddress,
    DateTime? from,
    DateTime? to,
    int limit = 50,
    String? cursor,
  }) => _backend.queryAudit(
    initiator: initiator,
    flowToken: flowToken,
    ipAddress: ipAddress,
    from: from,
    to: to,
    limit: limit,
    cursor: cursor,
  );

  sembast.Transaction _castTxn(Transaction txn) {
    // Unwrap via the backend's transaction() — test-side txns passed in
    // must have been produced by _backend.transaction(). We can't access
    // the private _SembastTxn directly, so the convention is to use the
    // txn via the backend's view methods. This concrete store is paired
    // with SembastBackend-produced transactions.
    return _backend._unwrapSembastTxn(txn);
  }
}
