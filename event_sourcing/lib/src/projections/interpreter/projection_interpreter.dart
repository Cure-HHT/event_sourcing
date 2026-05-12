// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
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

  /// Back-compat alias for callers reading `interpreter.registry`. Equal to
  /// [projections]. New code should use [projections] directly.
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
  // Implements: EVS-DEV-ingest-promotes-before-fold — per-spec
  //   in-memory promotion of older-version events before fold.
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
          firstEventTimestamp: event.clientTimestamp,
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
