// Types and the shared scan a consumer-facing view read uses to report a
// view's convergence state alongside its rows (EVS-DEV-converging-view-reads).
//
// Implements: EVS-DEV-converging-view-reads/A
// a read of all rows, a read by key and readViewRowInTxn each read the
//   copy's state and its rows in one storage transaction and return the
//   state with the rows, via ViewRowsRead / ViewRowsByKeyRead / ViewRowRead.
// Implements: EVS-DEV-converging-view-reads/B
// a converging read withholds every row it cannot confirm settled: an
//   aggregate view withholds rows of aggregates an unfolded event or
//   finding might reach; a table view, which has no settled row while it
//   converges, withholds all of them.
// Implements: EVS-DEV-converging-view-reads/C
// a by-key or single-key read reports an unconfirmed key as PendingRow,
//   distinct from SettledRow and AbsentRow.
// Implements: EVS-DEV-converging-view-reads/D
// the settled rows a read reports are exactly the backend's stored rows
//   for keys the scan does not find touched by an unfolded event or
//   finding past the watermark, which is what the fold step already wrote
//   under the instance's registered definitions (EVS-DEV-view-convergence/K).
// Implements: EVS-DEV-converging-view-reads/J
// ViewCopyStatus lets a caller read, per registered view, its state and
//   its copy's progress (watermark, log head, last failure).
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kSecurityFindingEntryType, kSecurityFindingRecordedEventType;
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/storage/view_copy.dart';
import 'package:meta/meta.dart' show internal;

/// Whether a view's copy is current for the instance in a transaction, or
/// converging (EVS-DEV-view-convergence Terms).
enum ViewConvergenceState { current, converging }

/// One row a read reports: a settled row, a confirmed-absent key, or a key
/// the read cannot yet confirm settled.
sealed class ViewRow {
  const ViewRow();
}

/// A settled row: the row a replay of the log produces for its key, as far
/// as the copy's watermark shows.
final class SettledRow extends ViewRow {
  const SettledRow(this.data);
  final Map<String, dynamic> data;
}

/// The key names no row: the view is current, or the key is confirmed
/// settled and its row does not exist.
final class AbsentRow extends ViewRow {
  const AbsentRow();
}

/// The key's row is not confirmed settled: the log holds an unfolded event
/// or security finding past the copy's watermark that the read cannot rule
/// out as reaching it (EVS-DEV-converging-view-reads/C).
final class PendingRow extends ViewRow {
  const PendingRow();
}

/// The result of a read of a view's rows: [state] and [rows], read in one
/// storage transaction (EVS-DEV-converging-view-reads/A).
class ViewRowsRead {
  const ViewRowsRead({required this.state, required this.rows});
  final ViewConvergenceState state;
  final List<Map<String, dynamic>> rows;
}

/// The result of a read of a view's rows by key: [state] and one
/// [ViewRow] per requested key.
class ViewRowsByKeyRead {
  const ViewRowsByKeyRead({required this.state, required this.rows});
  final ViewConvergenceState state;
  final Map<String, ViewRow> rows;
}

/// The result of a read of one row by key.
class ViewRowRead {
  const ViewRowRead({required this.state, required this.row});
  final ViewConvergenceState state;
  final ViewRow row;
}

/// A registered view's convergence state and its copy's progress
/// (EVS-DEV-converging-view-reads/J).
class ViewCopyStatus {
  const ViewCopyStatus({
    required this.viewName,
    required this.state,
    required this.watermark,
    required this.logHead,
    required this.lastFailure,
    required this.lastFailureAt,
  });

  final String viewName;
  final ViewConvergenceState state;

  /// The log position through which the copy has folded every event its
  /// definition folds.
  final int watermark;

  /// The latest position of the log, as the read's transaction sees it.
  final int logHead;

  /// The error the copy's last failed catch-up transaction threw, if any.
  final Object? lastFailure;
  final DateTime? lastFailureAt;
}

/// Thrown by a library operation that decides from a view's rows -- the
/// authorization policy's reads of the role-assignment, permission-grant
/// and containment views among them -- when the view is converging for the
/// instance in the transaction in which it would read those rows.
/// Transient: the operation appends no event on this refusal, and the same
/// submission succeeds once the named view is current.
// Implements: EVS-DEV-converging-view-reads/H
final class ViewConvergingRefusal implements Exception {
  const ViewConvergingRefusal(this.viewName);

  /// The view whose copy was converging for the instance.
  final String viewName;

  @override
  String toString() =>
      'ViewConvergingRefusal: "$viewName" is converging for this instance';
}

/// Thrown by a permission bootstrap or permission-seed operation, or by
/// `rebuildView`, when a caller-supplied deadline passes before every named
/// view becomes current for the instance. Names each such view still
/// converging and its copy's progress.
// Implements: EVS-DEV-converging-view-reads/I
// Implements: EVS-DEV-view-convergence/V
final class ViewConvergenceTimeout implements Exception {
  const ViewConvergenceTimeout(this.converging);

  /// The status of each view still converging when the deadline passed.
  final List<ViewCopyStatus> converging;

  @override
  String toString() =>
      'ViewConvergenceTimeout: still converging: '
      '${converging.map((s) => '${s.viewName} (watermark ${s.watermark}/'
          '${s.logHead})').join(', ')}';
}

/// Convenience accessors for tests and callers that only care about a
/// settled row's data.
extension ViewRowData on ViewRow {
  /// The row's data when this is a [SettledRow], else null.
  Map<String, dynamic>? get dataOrNull => switch (this) {
    SettledRow(:final data) => data,
    AbsentRow() || PendingRow() => null,
  };
}

@internal
bool isSecurityFindingEvent(StoredEvent event) =>
    event.entryType == kSecurityFindingEntryType &&
    event.eventType == kSecurityFindingRecordedEventType;

/// What [scanViewCurrency] finds scanning the log past a copy's watermark.
@internal
class ViewCurrencyScan {
  const ViewCurrencyScan({
    required this.state,
    required this.logHead,
    required this.unsettledAggregateIds,
    required this.allUnsettled,
  });

  final ViewConvergenceState state;

  /// The latest position of the log, as the scan's transaction sees it.
  final int logHead;

  /// For an aggregate view: the aggregate ids an interest-matching event
  /// past the watermark names. Empty, and meaningless, for a table view.
  final Set<String> unsettledAggregateIds;

  /// Set once a security finding lies past the watermark: the fold a
  /// finding changes can reach any row the view already folded
  /// (EVS-PRD-materializer/D-E), so a read withholds every row rather
  /// than compute finding reachability itself (EVS-DEV-converging-view-reads
  /// Rationale, "a backend that cannot cheaply tell which aggregates lie
  /// past the watermark may report every requested key pending").
  final bool allUnsettled;
}

/// Scans the log past [copy]'s watermark, inside [txn], for the events
/// [spec]'s definition folds (EVS-DEV-view-convergence Terms): an event
/// [spec]'s interest matches, or any security finding.
// Implements: EVS-DEV-converging-view-reads/B
// Implements: EVS-DEV-converging-view-reads/D
@internal
Future<ViewCurrencyScan> scanViewCurrency({
  required Transaction txn,
  required StorageBackend backend,
  required ProjectionSpec spec,
  required ViewCopy copy,
}) async {
  var after = copy.watermark;
  const chunkSize = 500;
  var anyFolding = false;
  var allUnsettled = false;
  final unsettled = <String>{};
  while (true) {
    final chunk = await backend.findAllEventsInTxn(
      txn,
      afterSequence: after,
      limit: chunkSize,
    );
    if (chunk.isEmpty) break;
    for (final e in chunk) {
      if (spec.interest.matches(e)) {
        anyFolding = true;
        unsettled.add(e.aggregateId);
      }
      if (isSecurityFindingEvent(e)) {
        anyFolding = true;
        allUnsettled = true;
      }
    }
    after = chunk.last.sequenceNumber;
    if (chunk.length < chunkSize) break;
  }
  return ViewCurrencyScan(
    state: anyFolding
        ? ViewConvergenceState.converging
        : ViewConvergenceState.current,
    logHead: after,
    unsettledAggregateIds: unsettled,
    allUnsettled: allUnsettled,
  );
}
