// Verifies: EVS-PRD-storage-barrier/B
// Verifies: EVS-PRD-storage-barrier/C
// Verifies: EVS-PRD-storage-barrier/D
// Verifies: EVS-PRD-storage-barrier/E
// Verifies: EVS-DEV-storage-capability/C
// Verifies: EVS-DEV-storage-capability/F
//
// Runs the run-time barrier scenarios of run_time_barrier_conformance.dart
// on Sembast databases the library opens in memory: no handed-out object
// answers a writing, reserved-appending, publishing or handle-yielding
// member dynamically or downcasts to a writing type, and the public
// operations keep working.
import 'package:event_sourcing/event_sourcing.dart';

import 'run_time_barrier_conformance.dart';

var _counter = 0;

void main() {
  runRunTimeBarrierScenarios(
    freshStorage: () async => SembastStorage.memory(
      'barrier-${DateTime.now().microsecondsSinceEpoch}-${_counter++}.db',
    ),
    backendLabel: 'sembast (memory)',
    expectsIdempotencyStore: false,
    deleteStorage: (storage) =>
        deleteSembastDatabase(storage as SembastStorage),
  );
}
