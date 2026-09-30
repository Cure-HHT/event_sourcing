// The row I/O of the one fold step every writer of a copy's rows shares
// (ProjectionInterpreter.foldIntoView): an append reads and writes the
// copy's rows straight through the backend; a catch-up transaction reads
// and writes them through a buffer that serves the step's own earlier
// writes and flushes them in batches. The fold's computation is the same
// code either way; only where its reads come from and when its writes
// reach storage differ.
//
// Implements: EVS-DEV-view-convergence/K
// a catch-up transaction folds each event through the fold step an append
//   uses; the buffer it folds through answers every read with the row the
//   same writes, made one at a time, would have left in storage.
import 'package:event_sourcing/src/projections/interpreter/fold_failure.dart'
    show FoldFailure;
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart' show internal;

/// The reads and writes of one copy's rows a fold step makes.
@internal
abstract interface class ViewRowAccess {
  /// The row at [key], or null when the copy holds none.
  Future<Map<String, dynamic>?> readRow(String key);

  /// Writes [row] at [key], leaving the row's producer index as it is.
  Future<void> upsertRow(String key, Map<String, dynamic> row);

  /// Writes [row] at [key] of a table view, indexing it under
  /// [sourceAggregateId] (`StorageBackend.upsertTableViewRowInTxn`).
  Future<void> upsertTableRow(
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  });

  /// Deletes the row at [key]; a no-op when the copy holds none.
  Future<void> deleteRow(String key);

  /// The rows of a table view [sourceAggregateId] produced
  /// (`StorageBackend.findTableRowsBySourceAggregateInTxn`).
  Future<List<Map<String, dynamic>>> findTableRowsBySourceAggregate(
    String sourceAggregateId,
  );
}

/// Reads and writes a copy's rows straight through [backend] in [txn]: the
/// row access of an append's fold.
@internal
final class DirectViewRowAccess implements ViewRowAccess {
  DirectViewRowAccess(this.backend, this.txn, this.copyId);

  final StorageBackend backend;
  final Transaction txn;
  final String copyId;

  @override
  Future<Map<String, dynamic>?> readRow(String key) =>
      backend.readViewRowInTxn(txn, copyId, key);

  @override
  Future<void> upsertRow(String key, Map<String, dynamic> row) =>
      backend.upsertViewRowInTxn(txn, copyId, key, row);

  @override
  Future<void> upsertTableRow(
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  }) => backend.upsertTableViewRowInTxn(
    txn,
    copyId,
    key,
    row,
    sourceAggregateId: sourceAggregateId,
  );

  @override
  Future<void> deleteRow(String key) =>
      backend.deleteViewRowInTxn(txn, copyId, key);

  @override
  Future<List<Map<String, dynamic>>> findTableRowsBySourceAggregate(
    String sourceAggregateId,
  ) => backend.findTableRowsBySourceAggregateInTxn(
    txn,
    copyId,
    sourceAggregateId,
  );
}

/// What a [BufferedViewRowAccess] knows of one key: the row it holds (null
/// when absent) and, when a write not yet flushed changed it, how.
final class _Entry {
  _Entry(this.row, {this.dirty = false, this.source = const _SourceKept()});

  Map<String, dynamic>? row;

  /// A write not yet flushed changed the row.
  bool dirty;

  /// The row's producer as the writes not yet flushed leave it.
  _Source source;

  _Entry copy() => _Entry(row, dirty: dirty, source: source);
}

/// The producer index entry of a buffered row.
sealed class _Source {
  const _Source();
}

/// Unchanged by the writes not yet flushed: whatever storage holds.
final class _SourceKept extends _Source {
  const _SourceKept();
}

/// Set by a write not yet flushed: [aggregateId], or none (a deleted row, or
/// a row a generic upsert re-created after a delete).
final class _SourceSet extends _Source {
  const _SourceSet(this.aggregateId);
  final String? aggregateId;
}

/// Reads and writes a copy's rows through an in-memory buffer inside one
/// catch-up transaction: a read is served from the buffer's own writes, or
/// the rows [prefetch] loaded, before it falls through to [backend]; a
/// write changes only the buffer until [flush] writes every changed row
/// with at most three statements (`StorageBackend.upsertViewRowsInTxn`,
/// `upsertTableViewRowsInTxn`, `deleteViewRowsInTxn`).
///
/// [beginEvent] and [discardEvent] bracket one event's fold, so a fold
/// that fails part-way leaves the buffer exactly as the event found it --
/// the keep-no-write of the failed fold a savepoint gives the append path.
@internal
final class BufferedViewRowAccess implements ViewRowAccess {
  BufferedViewRowAccess(this.backend, this.txn, this.copyId);

  final StorageBackend backend;
  final Transaction txn;
  final String copyId;

  final Map<String, _Entry> _entries = <String, _Entry>{};
  final Set<String> _dirty = <String>{};

  /// The entries the current event's fold changed, as they were before its
  /// first change of each, for [discardEvent] to restore. A null value
  /// records a key the buffer did not hold.
  final Map<String, _Entry?> _undo = <String, _Entry?>{};

  /// How many rows hold writes not yet flushed.
  int get pendingWrites => _dirty.length;

  /// Loads, in one read, every row of [keys] the buffer does not yet know.
  Future<void> prefetch(Iterable<String> keys) async {
    final missing = <String>{
      for (final key in keys)
        if (!_entries.containsKey(key)) key,
    };
    if (missing.isEmpty) return;
    final rows = await backend.readViewRowsByKeysInTxn(txn, copyId, missing);
    for (final key in missing) {
      _entries[key] = _Entry(rows[key]);
    }
  }

  /// Opens one event's fold: its writes can be discarded until the next
  /// [beginEvent].
  void beginEvent() => _undo.clear();

  /// Restores the buffer to what it held at the last [beginEvent], keeping
  /// no write of the fold that failed.
  void discardEvent() {
    for (final MapEntry(:key, value: before) in _undo.entries) {
      if (before == null) {
        _entries.remove(key);
      } else {
        _entries[key] = before;
      }
      if (before != null && before.dirty) {
        _dirty.add(key);
      } else {
        _dirty.remove(key);
      }
    }
    _undo.clear();
  }

  Future<_Entry> _load(String key) async {
    final held = _entries[key];
    if (held != null) return held;
    final row = await backend.readViewRowInTxn(txn, copyId, key);
    return _entries[key] = _Entry(row);
  }

  /// Records [key]'s entry as it stands before the current event's first
  /// change of it.
  void _remember(String key) {
    if (_undo.containsKey(key)) return;
    _undo[key] = _entries[key]?.copy();
  }

  void _write(String key, Map<String, dynamic>? row, _Source source) {
    _remember(key);
    _entries[key] = _Entry(row, dirty: true, source: source);
    _dirty.add(key);
  }

  @override
  Future<Map<String, dynamic>?> readRow(String key) async =>
      (await _load(key)).row;

  @override
  Future<void> upsertRow(String key, Map<String, dynamic> row) async {
    // A generic upsert leaves the producer as it stands: the one the
    // buffer set, or, for a row the buffer deleted (its producer set to
    // none), none; otherwise whatever storage holds.
    final before = _entries[key];
    final source = before == null
        ? const _SourceKept()
        : (before.row == null && before.dirty
              ? const _SourceSet(null)
              : before.source);
    _write(key, row, source);
  }

  @override
  Future<void> upsertTableRow(
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  }) async {
    _write(key, row, _SourceSet(sourceAggregateId));
  }

  @override
  Future<void> deleteRow(String key) async {
    _write(key, null, const _SourceSet(null));
  }

  @override
  Future<List<Map<String, dynamic>>> findTableRowsBySourceAggregate(
    String sourceAggregateId,
  ) async {
    // The producer index lives in storage: the writes not yet flushed go
    // there first, so the lookup sees them. Rare: only the refresh of the
    // marks a held security finding changes reaches this.
    await flush();
    return backend.findTableRowsBySourceAggregateInTxn(
      txn,
      copyId,
      sourceAggregateId,
    );
  }

  /// Writes every row changed since the last flush: the deleted rows
  /// through one delete, the rows a table upsert (or a delete before a
  /// re-creation) gave a producer through the batched table upsert, and
  /// the rest through the batched generic upsert -- inside a savepoint
  /// ([StorageBackend.runInSavepointInTxn]), so a write the server rejects
  /// for its value surfaces as [RowWriteRejected] and leaves [txn] usable
  /// for its caller to end. A flushed write can no longer be discarded by
  /// [discardEvent].
  Future<void> flush() async {
    _undo.clear();
    if (_dirty.isEmpty) return;
    await backend.runInSavepointInTxn(txn, _writePending);
  }

  Future<void> _writePending() async {
    final generic = <String, Map<String, dynamic>>{};
    final table = <String, ({Map<String, dynamic> row, String? source})>{};
    final deleted = <String>[];
    for (final key in _dirty) {
      final entry = _entries[key]!;
      final row = entry.row;
      if (row == null) {
        deleted.add(key);
      } else {
        switch (entry.source) {
          case _SourceKept():
            generic[key] = row;
          case _SourceSet(:final aggregateId):
            table[key] = (row: row, source: aggregateId);
        }
      }
      entry
        ..dirty = false
        ..source = const _SourceKept();
    }
    _dirty.clear();
    if (deleted.isNotEmpty) {
      await backend.deleteViewRowsInTxn(txn, copyId, deleted);
    }
    if (table.isNotEmpty) {
      await backend.upsertTableViewRowsInTxn(txn, copyId, table);
    }
    if (generic.isNotEmpty) {
      await backend.upsertViewRowsInTxn(txn, copyId, generic);
    }
  }
}

/// Runs [fold] for one event through [rows], discarding every write it
/// made to the buffer when it throws a [FoldFailure] -- or anything else --
/// before rethrowing.
@internal
Future<T> foldBuffered<T>(
  BufferedViewRowAccess rows,
  Future<T> Function() fold,
) async {
  rows.beginEvent();
  try {
    return await fold();
  } catch (_) {
    rows.discardEvent();
    rethrow;
  }
}
