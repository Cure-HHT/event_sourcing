// The candidate read and the log-head read of SembastBackend, held to the
// backend-agnostic conformance.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;

import '../test_support/candidate_read_conformance.dart';

var _dbCounter = 0;

void main() {
  group('SembastBackend candidate reads', () {
    runCandidateReadConformance(
      openBackend: () async {
        _dbCounter += 1;
        return SembastBackend(
          database: await newDatabaseFactoryMemory().openDatabase(
            'sembast-candidate-reads-$_dbCounter.db',
          ),
        );
      },
      openStore: (backend) => EventStore.openForTest(
        storage: backend,
        entryTypes: candidateReadEntryTypes(),
        source: const Source(
          hopId: 'candidate-hop',
          identifier: 'candidate-install',
          softwareVersion: 'candidate-test',
        ),
        securityContexts: SembastSecurityContextStore(
          backend: backend as SembastBackend,
        ),
        projections: ProjectionRegistry(),
      ),
    );
  });
}
