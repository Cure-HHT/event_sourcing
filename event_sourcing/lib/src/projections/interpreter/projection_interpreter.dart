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

  Future<void> applyEvent({
    required Txn txn,
    required StorageBackend backend,
    required StoredEvent event,
  }) async {
    for (final spec in registry.all()) {
      if (!spec.interest.matches(event)) continue;
      switch (spec) {
        case AggregateProjectionSpec():
          await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: event,
          );
        case TableProjectionSpec():
          await TableFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: spec,
            event: event,
          );
      }
    }
  }
}
