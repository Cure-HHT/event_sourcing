// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
//
// Implements: EVS-PRD-materializer/A — ProjectionInterpreter is the central
//   dispatch loop that drives the library's materializer: for each incoming
//   event it iterates registered specs and invokes the appropriate fold.
// Implements: EVS-PRD-materializer/B — determinism is preserved: the same
//   event dispatched to the same specs in the same order yields the same
//   fold changes; no non-deterministic inputs (wall clock, random, I/O
//   outside the backend transaction) are introduced here.
// Implements: EVS-DEV-ingest-promotes-before-fold/A — applies the per-view
//   promoter chain to any event whose entryTypeVersion is below
//   registeredVersion, before dispatching to the fold.
// Implements: EVS-DEV-ingest-promotes-before-fold/B — promotion operates on
//   an in-memory event.withData(...) copy; the original StoredEvent is not
//   modified.
// Implements: EVS-DEV-ingest-promotes-before-fold/C — two views matching the
//   same entry type receive independently-computed promoted payloads (per-spec
//   loop; promoter chain lookup is keyed by (viewName, entryType)).
// Implements: EVS-DEV-ingest-promotes-before-fold/D — when entryTypeVersion
//   equals registeredVersion the promoter branch is skipped entirely.
import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/promoters/promoter_executor.dart';
import 'package:event_sourcing/src/promoters/promoter_registry.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class ProjectionInterpreter {
  final ProjectionRegistry projections;
  final PromoterRegistry promoters;
  final EntryTypeRegistry entryTypes;

  /// Alias for [projections]; prefer [projections] for new call sites.
  ProjectionRegistry get registry => projections;

  ProjectionInterpreter({
    required this.projections,
    required this.promoters,
    required this.entryTypes,
  });

  /// Apply [event] to all matching projection specs inside [txn]. When
  /// [event.entryTypeVersion] is below the entry type's current
  /// `registeredVersion`, the substrate applies the promoter chain for
  /// each matching view in-memory before folding. The original [event]
  /// is not modified; only an in-memory working copy is promoted.
  ///
  /// Returns the list of [AggregateFoldChange] records from every spec
  /// that produced a change; null results (e.g. tombstone of non-existent
  /// row) are excluded. The caller uses this list for post-commit subscriber
  /// notification via [SubscriptionEngine.publishRowChange].
  Future<List<AggregateFoldChange>> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    // Resolve the entry type's current registered version. The `def == null`
    // fallback handles boot-time replays of lib_version events whose entry
    // types are not (re-)registered through `EntryTypeRegistry` — for those,
    // treat the event's own version as authoritative so no promotion runs.
    final def = entryTypes.byId(event.entryType);
    final registeredVersion = def?.registeredVersion ?? event.entryTypeVersion;

    final changes = <AggregateFoldChange>[];
    for (final spec in projections.all()) {
      if (!spec.interest.matches(event)) continue;

      StoredEvent eventForFold = event;
      if (event.entryTypeVersion < registeredVersion) {
        final promotedData = PromoterExecutor.promote(
          registry: promoters,
          viewName: spec.viewName,
          entryType: event.entryType,
          fromVersion: event.entryTypeVersion,
          toVersion: registeredVersion,
          payload: event.data,
        );
        eventForFold = event.withData(promotedData);
      }

      AggregateFoldChange? change;
      switch (spec) {
        case AggregateProjectionSpec():
          change = await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: eventForFold,
          );
        case TableProjectionSpec():
          change = await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: eventForFold,
          );
      }
      if (change != null) changes.add(change);
    }
    return changes;
  }
}
