// Implements: EVS-PRD-materializer/A
// rebuildView is the library-supplied
//   helper that replays the event log to reconstruct a single view from
//   scratch; it is part of the library's materializer surface.
// Implements: EVS-PRD-materializer/B
// rebuild is deterministic and
//   idempotent: the same log + same targetVersionByEntryType always produces
//   identical view rows, outstanding-finding marks included.
// Implements: EVS-PRD-materializer/G
// the rebuild folds every event of the log through the fold step, so the
//   view folds each security finding whatever its interest, in log order.
// Implements: EVS-PRD-materializer/C
// (partial) — the strict-superset check
//   and explicit targetVersionByEntryType map ensure the rebuild's scope is
//   fully specified and auditable; rebuild does not silently shrink the set
//   of entry types the view covers.

part of '../event_store.dart';

/// Chunk size for the streaming read of the event log during a rebuild.
///
/// Bounds the per-iteration working set to a fixed number of [StoredEvent]s
/// regardless of total log size. Chosen to amortize find-query overhead while
/// keeping peak memory modest on mobile and tolerable on server-scale logs.

const int _rebuildChunkSize = 500;

// Implements: EVS-PRD-destinations/K
// the rebuild writes only target versions
//   derived from the entry-type registry, so it writes nothing that the log
//   and the registered versions do not determine.
/// Rebuild exactly one view by replaying the event log through the registered
/// [ProjectionSpec] for [viewName] on [store]. Clears the view AND the view's
/// `view_target_versions` rows, writes the supplied [targetVersionByEntryType],
/// then folds, through the projection interpreter's fold step (promotion
/// through `store.promoters`, then the aggregate or table fold), every
/// event whose entry type is in [targetVersionByEntryType] and whose
/// `store.projections` spec's `interest` matches. Runs in one backend
/// transaction.
///
/// Strict-superset rule: every entry-type already present in the stored
/// `view_target_versions` for [viewName] MUST appear in
/// [targetVersionByEntryType]; otherwise [ArgumentError] is thrown before
/// any clear or write. New entry types may be added (superset). An event
/// in the log whose `entry_type` is not in [targetVersionByEntryType] is
/// skipped (it is not subject to this view's fold).
///
/// Every target in [targetVersionByEntryType] SHALL be the registered
/// version of a registered entry type: an unregistered entry type, or a
/// target that differs from `store.entryTypes.byId(id).registeredVersion`,
/// throws [ArgumentError] before any clear or write. The rebuilt rows are
/// therefore the rows the library's fold derives from the log under the
/// registered versions.
///
/// The rebuild does not notify live subscribers: an `AggregateMode`
/// subscription keeps the rows it last received until the next append
/// changes them.
///
/// Returns the number of events processed. Idempotent — running twice on
/// the same log with the same map produces the same view rows.
Future<int> rebuildView({
  required EventStore store,
  required String viewName,
  required Map<String, EntryTypeVersion> targetVersionByEntryType,
}) async {
  refuseCallFromBootProgressObserver('rebuildView');
  final spec = store.projections.lookup(viewName);
  if (spec == null) {
    throw StateError(
      'rebuildView: no ProjectionSpec registered under "$viewName" in '
      'store.projections. Register the spec before calling rebuildView.',
    );
  }
  for (final entry in targetVersionByEntryType.entries) {
    final def = store.entryTypes.byId(entry.key);
    if (def == null) {
      throw ArgumentError(
        'rebuildView: targetVersionByEntryType names entry type '
        '"${entry.key}", which is not registered in store.entryTypes.',
      );
    }
    if (def.registeredVersion != entry.value) {
      throw ArgumentError(
        'rebuildView: target ${entry.value} for entry type "${entry.key}" '
        'differs from its registered version ${def.registeredVersion}. A '
        'rebuild folds every entry type at its registered version.',
      );
    }
  }
  final backend = store._backend;
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

    await IntegrityMarks.beginReplay(txn, backend);
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
        final tgt = spec.interest.matches(event)
            ? targetVersionByEntryType[event.entryType]
            : null;

        // The fold step of the projection interpreter, under the target:
        // a lower version is promoted, each default decided against the
        // row being rebuilt; an equal or higher minor folds unchanged; a
        // higher major throws, rolling the rebuild back. An event the view
        // does not fold still folds into its outstanding-finding marks.
        await ProjectionInterpreter.foldIntoView(
          txn: txn,
          backend: backend,
          spec: spec,
          promoters: store.promoters,
          event: event,
          version: tgt,
        );
        if (tgt != null) processed++;
      }

      if (chunk.length < _rebuildChunkSize) break;
      lastSeq = chunk.last.sequenceNumber;
    }

    return processed;
  });
}
