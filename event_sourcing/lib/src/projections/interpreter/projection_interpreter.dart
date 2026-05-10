// event_sourcing/lib/src/projections/interpreter/projection_interpreter.dart
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/interpreter/table_fold.dart';
import 'package:event_sourcing/src/projections/projection_registry.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/txn.dart';

class ProjectionInterpreter {
  final ProjectionRegistry registry;
  ProjectionInterpreter(this.registry);

  /// Apply [event] to all matching projection specs inside [txn].
  /// Returns the list of [AggregateFoldChange] records from every spec
  /// that produced a change; null results (e.g. tombstone of non-existent
  /// row) are excluded. The caller uses this list for post-commit subscriber
  /// notification via [SubscriptionEngine.publishRowChange].
  Future<List<AggregateFoldChange>> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    final changes = <AggregateFoldChange>[];
    for (final spec in registry.all()) {
      if (!spec.interest.matches(event)) continue;
      AggregateFoldChange? change;
      switch (spec) {
        case AggregateProjectionSpec():
          change = await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: event,
          );
        case TableProjectionSpec():
          change = await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: event,
          );
      }
      if (change != null) changes.add(change);
    }
    return changes;
  }
}
