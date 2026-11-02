// The candidate read and the log-head read of SembastBackend, held to the
// backend-agnostic conformance.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart'
    show findCandidateEventsByScan, readLogHeadByReverseRead;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart'
    show Database, newDatabaseFactoryMemory;

import '../test_support/candidate_read_conformance.dart';

/// A [SembastBackend] answering the candidate and log-head reads as the
/// storage contract's defaults do, which a backend that overrides neither
/// inherits.
class _DefaultReadsBackend extends SembastBackend {
  _DefaultReadsBackend({required super.database});

  @override
  Future<List<StoredEvent>> findCandidateEventsInTxn(
    Transaction txn, {
    required int afterSequence,
    required int throughSequence,
    required int limit,
    Set<String>? entryTypes,
    Set<String>? eventTypes,
    Set<String>? aggregateTypes,
  }) => findCandidateEventsByScan(
    this,
    txn,
    afterSequence: afterSequence,
    throughSequence: throughSequence,
    limit: limit,
    entryTypes: entryTypes,
    eventTypes: eventTypes,
    aggregateTypes: aggregateTypes,
  );

  @override
  Future<int> readLogHeadInTxn(Transaction txn) =>
      readLogHeadByReverseRead(this, txn);
}

var _dbCounter = 0;

Future<Database> _database() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase(
    'sembast-candidate-reads-$_dbCounter.db',
  );
}

ProjectionRegistry _registry(List<ProjectionSpec> specs) {
  final registry = ProjectionRegistry();
  specs.forEach(registry.register);
  return registry;
}

Future<EventStore> _openStore(StorageBackend backend) => _openWith(
  backend,
  entryTypes: candidateReadEntryTypes(),
  projections: const <ProjectionSpec>[],
);

Future<EventStore> _openWith(
  StorageBackend backend, {
  required EntryTypeRegistry entryTypes,
  required List<ProjectionSpec> projections,
}) => EventStore.openForTest(
  storage: backend,
  entryTypes: entryTypes,
  source: const Source(
    hopId: 'candidate-hop',
    identifier: 'candidate-install',
    softwareVersion: 'candidate-test',
  ),
  securityContexts: SembastSecurityContextStore(
    backend: backend as SembastBackend,
  ),
  projections: _registry(projections),
);

void main() {
  group('SembastBackend candidate reads', () {
    runCandidateReadConformance(
      openBackend: () async => SembastBackend(database: await _database()),
      openStore: _openStore,
    );
  });

  group('SembastBackend candidate windows', () {
    runCandidateWindowBoundaryConformance(
      openBackend: () async => SembastBackend(database: await _database()),
      openStore: _openWith,
    );
  });

  group("the storage contract's default candidate reads", () {
    runCandidateReadConformance(
      openBackend: () async =>
          _DefaultReadsBackend(database: await _database()),
      openStore: _openStore,
    );
  });
}
