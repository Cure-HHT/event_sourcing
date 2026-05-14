// lib/server/demo_state_projection.dart
// IMPLEMENTS REQUIREMENTS:
//   inspector pane.
//
// PollingDemoStateProjection re-queries the event store, matrix view,
// directory, and idempotency cache on each call. CUR-1154 Phase 4.12's
// reactive read layer (watchEvents/watchFifo) is the future swap target.

import 'package:action_permissions_demo/server/bootstrap.dart';
import 'package:action_permissions_demo/server/inspect_snapshot.dart';
import 'package:action_permissions_demo/shared/wire_types.dart';

abstract class DemoStateProjection {
  Future<InspectSnapshot> snapshot();
}

class PollingDemoStateProjection implements DemoStateProjection {
  PollingDemoStateProjection({
    required this.components,
    this.lastTraceProvider,
  });

  final DemoServerComponents components;
  final DispatchTrace? Function()? lastTraceProvider;

  @override
  Future<InspectSnapshot> snapshot() async {
    final eventsList = await collectEventSummaries(
      components.eventStore,
      components.directory,
      limit: 200,
    );
    final matrix = await collectMatrixGrants(components.eventStore);
    final directory = components.directory.listEntries();
    final idem = await collectIdempotencyEntries(components.idempotencyStore);
    return InspectSnapshot(
      events: eventsList,
      matrixGrants: matrix,
      directory: directory,
      idempotency: idem,
      lastDispatchTrace: lastTraceProvider?.call(),
    );
  }
}
