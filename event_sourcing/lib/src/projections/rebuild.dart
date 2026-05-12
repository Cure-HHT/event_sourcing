// Implements: EVS-PRD-materializer/A — rebuildView is the library-supplied
//   helper that replays the event log to reconstruct a single view from
//   scratch; it is part of the library's materializer surface.
// Implements: EVS-PRD-materializer/B — rebuild is deterministic and
//   idempotent: the same log + same targetVersionByEntryType always produces
//   identical view rows.
// Implements: EVS-PRD-materializer/C (partial) — the strict-superset check
//   and explicit targetVersionByEntryType map ensure the rebuild's scope is
//   fully specified and auditable; rebuild does not silently shrink the set
//   of entry types the view covers.
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';

/// Chunk size for the streaming read of the event log during a rebuild.
///
/// Bounds the per-iteration working set to a fixed number of [StoredEvent]s
/// regardless of total log size. Chosen to amortize find-query overhead while
/// keeping peak memory modest on mobile and tolerable on server-scale logs.
const int _rebuildChunkSize = 500;

/// Rebuild exactly one view by replaying the event log through the registered
/// [ProjectionSpec] for [viewName] on [store]. Clears the view AND the view's
/// `view_target_versions` rows, writes the supplied [targetVersionByEntryType],
/// then applies the promoter chain (from [store.promoters]) and dispatches to
/// the appropriate fold interpreter ([AggregateFold] / [TableFold]) for every
/// event whose entry type is in [targetVersionByEntryType] and whose
/// [store.projections] spec's `interest` matches. Runs in one backend
/// transaction.
///
/// Strict-superset rule: every entry-type already present in the stored
/// `view_target_versions` for [viewName] MUST appear in
/// [targetVersionByEntryType]; otherwise [ArgumentError] is thrown before
/// any clear or write. New entry types may be added (superset). An event
/// in the log whose `entry_type` is not in [targetVersionByEntryType] is
/// skipped (it is not subject to this view's fold).
///
/// Returns the number of events processed. Idempotent — running twice on
/// the same log with the same map produces the same view rows.
//   ProjectionSpec replay; strict-superset target-version map.
//   atomically with the view rebuild.
Future<int> rebuildView({
  required EventStore store,
  required String viewName,
  required Map<String, int> targetVersionByEntryType,
}) async {
  final spec = store.projections.lookup(viewName);
  if (spec == null) {
    throw StateError(
      'rebuildView: no ProjectionSpec registered under "$viewName" in '
      'store.projections. Register the spec before calling rebuildView.',
    );
  }
  final backend = store.backend;
  return backend.transaction<int>((txn) async {
    // Strict-superset check BEFORE any destructive write.
    final existing = await backend.readAllViewTargetVersionsInTxn(
      txn,
      viewName,
    );
    for (final entry in existing.entries) {
      if (!targetVersionByEntryType.containsKey(entry.key)) {
        throw ArgumentError(
          'rebuildView: targetVersionByEntryType is not a strict superset '
          'of the existing view_target_versions for view '
          '"$viewName". Missing existing entry type '
          '"${entry.key}" (stored target ${entry.value}). '
          'Partial rebuilds are not allowed; supply every existing entry '
          'type plus any new ones.',
        );
      }
    }

    await backend.clearViewInTxn(txn, viewName);
    await backend.clearViewTargetVersionsInTxn(txn, viewName);
    for (final e in targetVersionByEntryType.entries) {
      await backend.writeViewTargetVersionInTxn(txn, viewName, e.key, e.value);
    }

    var processed = 0;
    int? lastSeq;
    while (true) {
      final chunk = await backend.findAllEventsInTxn(
        txn,
        afterSequence: lastSeq,
        limit: _rebuildChunkSize,
      );
      if (chunk.isEmpty) break;

      for (final event in chunk) {
        if (!spec.interest.matches(event)) continue;
        final tgt = targetVersionByEntryType[event.entryType];
        if (tgt == null) continue;

        final promoted = PromoterExecutor.promote(
          registry: store.promoters,
          viewName: viewName,
          entryType: event.entryType,
          fromVersion: event.entryTypeVersion,
          toVersion: tgt,
          payload: event.data,
        );

        // Rebuild applies the promoted payload via a synthetic event whose
        // data field is replaced with the promoted map. We use copyWith-style
        // construction since StoredEvent is immutable.
        final promotedEvent = event.withData(promoted);

        switch (spec) {
          case AggregateProjectionSpec():
            await AggregateFold.applyEvent(
              txn: txn,
              backend: backend,
              spec: spec,
              event: promotedEvent,
            );
          case TableProjectionSpec():
            await TableFold.applyEvent(
              txn: txn,
              backend: backend,
              spec: spec,
              event: promotedEvent,
            );
        }
        processed++;
      }

      if (chunk.length < _rebuildChunkSize) break;
      lastSeq = chunk.last.sequenceNumber;
    }

    return processed;
  });
}
