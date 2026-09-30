// The catch-up equivalence check (test_support/catch_up_equivalence_
// conformance.dart) on an in-memory Sembast backend.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart' show pumpEventQueue;
import 'package:sembast/sembast_memory.dart';

import '../test_support/catch_up_equivalence_conformance.dart';

var _dbCounter = 0;

void main() {
  runCatchUpEquivalenceConformance(
    openBackend: () async {
      _dbCounter += 1;
      final db = await newDatabaseFactoryMemory().openDatabase(
        'catch-up-equivalence-$_dbCounter.db',
      );
      return SembastBackend(database: db);
    },
    opener: (backend) =>
        ({required entryTypes, required projections, required promoters}) =>
            EventStore.openForTest(
              storage: backend,
              entryTypes: entryTypes,
              source: const Source(
                hopId: 'equivalence-hop',
                identifier: 'equivalence-install',
                softwareVersion: 'equivalence-test',
              ),
              securityContexts: SembastSecurityContextStore(
                backend: backend as SembastBackend,
              ),
              projections: projections,
              promoters: promoters,
            ),
    settle: pumpEventQueue,
  );
}
