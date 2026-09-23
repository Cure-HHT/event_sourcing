// Verifies: EVS-PRD-destinations/K
//
// The library's mutating surface is internal: the scan resolves `lib/`
// with the analyzer and checks annotations, overrides, exports and return
// types as resolved elements, not as text. Each rule also runs against
// synthetic fixtures laid over `lib/` in memory, which must fail it.
import 'dart:io';

import 'package:analyzer/dart/ast/ast.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

import '../test_support/surface_scan.dart';

/// `StorageBackend` members that stay public because they change nothing.
const _reads = <String, String>{
  'findEventsForAggregate': "reads one aggregate's events",
  'findEventsForAggregateInTxn': "reads one aggregate's events in a txn",
  'findAllEvents': 'reads the event log',
  'findAllEventsInTxn': 'reads the event log in a txn',
  'readLatestEventHash': 'reads the hash-chain tip',
  'readSequenceCounter': 'reads the sequence counter',
  'readViewRowInTxn': 'reads one view row in a txn',
  'findViewRows': 'reads a view',
  'readViewRowsByKeys': 'reads view rows by key',
  'findViewRowsInTxn': 'reads a view in a txn',
  'readViewTargetVersionInTxn': 'reads one view target version',
  'readAllViewTargetVersionsInTxn': "reads a view's target versions",
  'readFifoHead': 'reads a queue head',
  'listFifoEntries': 'reads a queue',
  'readFifoRow': 'reads one queue row',
  'hasFifoWedged': 'reads whether any queue head is wedged',
  'wedgedFifos': 'reads the wedged queue heads',
  'readSchemaVersion': 'reads the storage schema version',
  'readFillCursor': 'reads a fill position',
  'findEventById': 'reads one event',
  'findEventByIdInTxn': 'reads one event in a txn',
  'readSchedule': 'reads a destination schedule',
  'readEventsReverse': 'reads the event log newest first',
  'queryAudit': 'reads the security-context audit',
};

/// `StorageBackend` members that change state yet stay public.
const _nonReads = <String, String>{
  'transaction':
      "a consumer runs its own reads in one transaction (reaction's server "
      'handlers do); an event-store append refuses any transaction but the '
      "event store's own runTransaction, and every mutator reachable "
      'through the handle is itself internal',
  'close': "the consumer owns the backend's lifetime",
};

/// Function-typed parameters, fields and setters the exported surface
/// legitimately carries. Each reason says what the callback decides and
/// which trust entry covers it; a callback that takes part in a decision
/// and is covered by no entry is marked a known unenumerated input.
const _functionTyped = <String, String>{
  'AggregateMode.mapper (field)':
      "maps a view row to the subscriber's type after the read; decides "
      'nothing the library derives',
  'AggregateMode.new(mapper)':
      "maps a view row to the subscriber's type after the read; decides "
      'nothing the library derives',
  'ContainmentResolver.findRowsInTxn (field)':
      'known unenumerated input: reads the containment rows an '
      'authorization decision uses',
  'ContainmentResolver.new(findRowsInTxn)':
      'known unenumerated input: reads the containment rows an '
      'authorization decision uses',
  'EventStore.open(clock)':
      'stamps the client timestamp the log records as event data; decides '
      'nothing the library derives',
  'EventStore.open(syncCycleTrigger)':
      'wakes the delivery cycle after a commit; decides nothing',
  'EventStore.openForTest(clock)':
      'stamps the client timestamp the log records as event data',
  'EventStore.openForTest(syncCycleTrigger)':
      'wakes the delivery cycle after a commit; decides nothing',
  'EventStore.runTransaction(body)':
      "the consumer's transaction body; every write it can reach is internal",
  'EventStore.syncCycleTrigger (field)':
      'read-only view of the trigger the store fires after a commit',
  'PostgresBackend.transaction(body)':
      "the consumer's transaction body; every write it can reach is internal",
  'ScopeClassRegistry.new(projectionLookup)':
      'known unenumerated input: resolves a scope class to the projection '
      'an authorization decision reads',
  'ScopeDescendantExpander.findRowsInTxn (field)':
      'known unenumerated input: reads the containment rows that narrow a '
      'scoped read',
  'ScopeDescendantExpander.new(findRowsInTxn)':
      'known unenumerated input: reads the containment rows that narrow a '
      'scoped read',
  'SembastBackend.transaction(body)':
      "the consumer's transaction body; every write it can reach is internal",
  'StorageBackend.transaction(body)':
      "the consumer's transaction body; every write it can reach is internal",
  'SubscriptionFilter.new(predicate)':
      "a destination's filter is delivery configuration under the "
      "Destination trust entry; a subscription's filter decides only what "
      'its subscriber sees',
  'SubscriptionFilter.predicate (field)':
      "a destination's filter is delivery configuration under the "
      "Destination trust entry; a subscription's filter decides only what "
      'its subscriber sees',
  'SyncCycle.new(clock)':
      'known unenumerated input: delivery configuration; fill computes its '
      'window from it',
  'SyncCycle.new(policyResolver)':
      'known unenumerated input: delivery configuration; decides the retry '
      'policy per cycle',
  'TableBackedAuthorizationPolicy.new(transactionProvider)':
      'known unenumerated input: opens the transaction the authorization '
      'policy reads in',
  'TableBackedAuthorizationPolicy.transactionProvider (field)':
      'known unenumerated input: opens the transaction the authorization '
      'policy reads in',
  'bootstrapEventStore(syncCycleTrigger)':
      'wakes the delivery cycle after a commit; decides nothing',
};

/// Members on types other than `StorageBackend`, and top-level functions,
/// that must be internal.
const _mustBeInternal = <String, String>{
  'drain': 'drains a queue outside the delivery cycle',
  'fillBatch': 'fills a queue outside the delivery cycle',
  'runHistoricalReplay': 'enqueues and rewinds a fill position',
  'runGapReplay': 'enqueues and rewinds a fill position',
  'seedViewTargetVersions': 'writes view target versions',
  'promoteViewSnapshots': 'rewrites view rows and target versions',
  'EventStoreBundle.setViewTargetVersion':
      'writes view_target_versions without the boot seeding',
  'AggregateFold.applyEvent': 'writes view rows',
  'TableFold.applyEvent': 'writes view rows',
  'ProjectionInterpreter.applyEvent': 'writes view rows',
  'MutableSecurityContextStore.writeInTxn':
      "writes an event's security "
      'context',
  'MutableSecurityContextStore.upsertInTxn':
      "rewrites an event's security "
      'context',
  'MutableSecurityContextStore.deleteInTxn':
      "deletes an event's security "
      'context',
  'SembastSecurityContextStore.writeInTxn':
      "writes an event's security "
      'context',
  'SembastSecurityContextStore.upsertInTxn':
      "rewrites an event's security "
      'context',
  'SembastSecurityContextStore.deleteInTxn':
      "deletes an event's security "
      'context',
  'PostgresSecurityContextStore.writeInTxn':
      "writes an event's security "
      'context',
  'PostgresSecurityContextStore.upsertInTxn':
      "rewrites an event's security "
      'context',
  'PostgresSecurityContextStore.deleteInTxn':
      "deletes an event's security "
      'context',
  'PostgresTxn.invalidate': 'ends a transaction handle the backend owns',
  'PostgresTxn.session': 'raw engine transaction',
  'SembastBackend.unwrapSembastTxn': 'raw engine transaction',
  'PublishCollector.add': 'publishes an event to live subscribers',
  'PublishCollector.addRowChanges':
      'publishes view changes to live subscribers',
  'PostgresBackend.pool': 'raw connection pool',
  'SembastBackendTestSupport.databaseForTesting': 'raw database handle',
};

/// Raw-handle members; each must be internal.
const _sanctionedRawHandles = <String>{
  'PostgresBackend.pool',
  'PostgresTxn.session',
  'SembastBackend.unwrapSembastTxn',
  'SembastBackendTestSupport.databaseForTesting',
};

/// Public members a concrete backend declares beyond the contract that are
/// neither read-named nor internal.
const _concreteOperations = <String, String>{
  'PostgresBackend.open': 'static opener; the consumer owns the backend',
  'PostgresBackend.endpointFromUrl': 'pure URL parse; changes nothing',
};

/// Public members of unexported types, and unexported top-level functions,
/// that a `src/` import reaches and that change no persisted state.
const _unexportedOperations = <String, String>{
  'verifyNoEntryTypeDowngrade': 'reads view target versions; changes nothing',
  'PublishCollector.events': 'reads what the run collected',
  'PublishCollector.rowChanges': 'reads what the run collected',
  'LibVersion.version': 'constant',
  'LibVersion.compare': 'pure function',
  'LibVersionEvents.initialized': 'constant',
  'LibVersionEvents.changed': 'constant',
  'VersionCheckResult.recordedVersion': 'value field',
  'VersionCheckResult.sequenceNumber': 'value field',
  'VersionCheckResult.eventType': 'value field',
  'VersionCheck.findMostRecent': 'reads the log',
  'AggregateFoldChange.viewName': 'value field',
  'AggregateFoldChange.aggregateId': 'value field',
  'AggregateFoldChange.newValue': 'value field',
  'AggregateFoldChange.sequence': 'value field',
  'AggregateFoldChange.cause': 'value field',
  'AggregateFoldChange.isTombstone': 'value field',
  'ProjectionInterpreter.projections': 'value field',
  'ProjectionInterpreter.promoters': 'value field',
  'ProjectionInterpreter.entryTypes': 'value field',
  'Merge.applyDelta': 'pure function',
  'Merge.applyDeepDelta': 'pure function',
  'PromoterExecutor.promote': 'pure function',
  'MutableSecurityContextStore.readInTxn': 'reads a security context',
  'MutableSecurityContextStore.findUnredactedOlderThanInTxn':
      'reads security contexts',
  'MutableSecurityContextStore.findOlderThanInTxn': 'reads security contexts',
  'SubscriptionEngine.events': 'reads the event bus of an engine',
  'SubscriptionEngine.rowChanges': 'reads the row-change bus of an engine',
  'SubscriptionEngine.close':
      "closes an engine its caller constructed; the event store's engine is "
      'private to it',
};

/// Exported top-level functions that are library operations.
const _topLevelOperations = <String, String>{
  'bootstrapActionPermissions':
      'appends the role-permission seed through the event store',
  'bootstrapAuditedActions': "registers the library's audited actions",
  'bootstrapEventStore': 'opens the event store and its registries',
  'bootstrapRoleAssignments':
      'appends the role-assignment seed through the event store',
  'classifyStorageException': 'pure function; changes nothing',
  'computeRoleAssignmentAggregateId': 'pure function; changes nothing',
  'denialAuthorizationDenied': 'builds an event draft; changes nothing',
  'denialExecutionFailed': 'builds an event draft; changes nothing',
  'denialIdempotencyMismatch': 'builds an event draft; changes nothing',
  'denialParseDenied': 'builds an event draft; changes nothing',
  'denialUnknownAction': 'builds an event draft; changes nothing',
  'denialValidationDenied': 'builds an event draft; changes nothing',
  'matchScopeClass': 'pure function; changes nothing',
  'rebuildView':
      'replays a view from the log through the projection interpreter at '
      'the registered entry-type versions, refusing any other target; it '
      'does not notify live subscribers',
  'sanitizeErrorMessage': 'pure function; changes nothing',
};

/// Public members of `EventStoreBundle` that are library operations.
const _bundleOperations = <String, String>{
  'eventStore': 'the event store',
  'entryTypes': 'the entry-type registry',
  'destinations': 'the destination registry',
  'securityContexts':
      'the read-only security-context store; its mutators are internal',
};

const _fixtureBackends = '''
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart';
import 'package:postgres/postgres.dart' show Pool, Session;
import 'package:sembast/sembast.dart' as s;
import 'package:sembast/sembast.dart' show Database;

typedef RawDb = Database;

class ScanFixtureOverrideBackend extends SembastBackend {
  ScanFixtureOverrideBackend({required super.database});

  @override
  Future<void> markFinal(
    String destinationId,
    String entryId,
    FinalStatus status,
  ) => super.markFinal(destinationId, entryId, status);
}

abstract class ScanFixtureContract extends StorageBackend {
  Future<void> purgeEverything(Transaction txn);

  @internal
  Future<int> readThing();
}

class ScanFixtureRawBackend extends SembastBackend {
  ScanFixtureRawBackend({required super.database});

  s.Transaction rawOf(Transaction txn) => throw UnimplementedError();
  RawDb aliasedDatabase() => throw UnimplementedError();
  s.Database prefixedDatabase() => throw UnimplementedError();
  Stream<Database> databases() => throw UnimplementedError();
  (Database, int) paired() => throw UnimplementedError();
  set rawDatabase(Database db) {}
  void Function(String)? traceLog;
  set onTrace(void Function(String) f) {}
}

class ScanFixtureSembastConcrete extends SembastBackend {
  ScanFixtureSembastConcrete({required super.database});

  Future<void> resetFifo(String destinationId) async {}
}

abstract class ScanFixturePostgresConcrete extends PostgresBackend {
  Future<void> purgeQueue(String destinationId);
}

class ScanFixtureTxn extends Transaction {
  Session get raw => throw UnimplementedError();
}

class ScanFixtureIdempotencyStore {
  Pool<void> get pool => throw UnimplementedError();
}

class ScanFixtureSanctioned {
  Database get db => throw UnimplementedError();
}
''';

const _fixtureExports = '''
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';

void configureDelivery({void Function()? onHook}) {}

void configureSwitch({bool skipFenceForTest = false}) {}

void configureOptions({DeliveryTestHooks? options}) {}

class ScanFixtureSink {
  void Function(String)? traceSink;
}

class ScanFixtureCallbacks {
  void Function()? onDone;
}

void scanFixtureOperation() {}
''';

const _fixtureTestingSeam = '''
class ScanFixtureSeam {}
''';

const _fixtureBarrel = '''
export 'package:event_sourcing/event_sourcing.dart';
export 'src/scan_fixture_exports.dart';
export 'src/sync/drain.dart' show drain;
export 'src/sync/fill_batch.dart' show fillBatch;
export 'src/testing/scan_fixture_seam.dart';
''';

const _fixtureInternalCopies = '''
class EventStoreBundle {
  Future<void> setViewTargetVersion(String v, String e, int n) async {}
}

class PostgresTxn {
  Object get session => Object();
  void invalidate() {}
}

class SembastBackend {
  Object unwrapSembastTxn(Object txn) => txn;
}

class PublishCollector {
  void add(Object event) {}
  void addRowChanges(Iterable<Object> changes) {}
}

class PostgresBackend {
  Object get pool => Object();
}

extension SembastBackendTestSupport on SembastBackend {
  Object get databaseForTesting => Object();
}

class AggregateFold {
  static Future<void> applyEvent() async {}
}

class TableFold {
  static Future<void> applyEvent() async {}
}

class ProjectionInterpreter {
  Future<void> applyEvent() async {}
}

abstract class MutableSecurityContextStore {
  Future<void> writeInTxn();
  Future<void> upsertInTxn();
  Future<void> deleteInTxn();
}

class SembastSecurityContextStore {
  Future<void> writeInTxn() async {}
  Future<void> upsertInTxn() async {}
  Future<void> deleteInTxn() async {}
}

class PostgresSecurityContextStore {
  Future<void> writeInTxn() async {}
  Future<void> upsertInTxn() async {}
  Future<void> deleteInTxn() async {}
}

Future<void> drain() async {}
Future<void> fillBatch() async {}
Future<void> runHistoricalReplay() async {}
Future<void> runGapReplay() async {}
Future<void> seedViewTargetVersions() async {}
Future<void> promoteViewSnapshots() async {}
''';

const _fixtureUnexportedWriter = '''
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/transaction.dart';

Future<void> rewindQueue(StorageBackend backend, Transaction txn) =>
    backend.writeFillCursorTxn(txn, 'dest', 0);

class ScanFixtureWriter {
  static Future<void> purge(StorageBackend backend) async {}
}
''';

const _fixtureZoneAndLog = '''
import 'dart:async';
import 'dart:developer' as developer;

Object? aliasedRead() {
  final z = Zone.current;
  return z[#key];
}

Object? spacedRead() => Zone.current [#key];

void printed() {
  print('direct');
  developer.log('direct');
}
''';

/// The resolved syntax trees of every library under `lib/` on disk.
Future<Map<String, List<CompilationUnit>>> _unitsUnderLib(
  SurfaceScanner scanner,
) async {
  final files =
      Directory(p.join(scanner.root, 'lib'))
          .listSync(recursive: true)
          .whereType<File>()
          .where((f) => f.path.endsWith('.dart'))
          .map((f) => p.relative(f.path, from: scanner.root))
          .toList()
        ..sort();
  return <String, List<CompilationUnit>>{
    for (final file in files) file: await scanner.units(file),
  };
}

void main() {
  late SurfaceScanner scanner;
  late List<LibraryElement> libraries;
  late LibraryElement barrel;

  setUpAll(() async {
    scanner = SurfaceScanner();
    libraries = await scanner.librariesUnder('lib');
    barrel = (await scanner.library('lib/event_sourcing.dart'))!;
  });

  InterfaceElement contractOf(Iterable<LibraryElement> libs) => classNamed(
    libs.where((l) => l.uri.toString().contains('/src/storage/')),
    'StorageBackend',
  );

  group('library surface', () {
    test('(a) StorageBackend mutators and their overrides are internal', () {
      final contract = contractOf(libraries);
      final impls = classesOf(libraries).where(
        (c) =>
            c != contract && c.allSupertypes.any((s) => s.element == contract),
      );
      expect(
        impls.map((c) => c.name),
        containsAll(<String>['SembastBackend', 'PostgresBackend']),
      );
      expect(
        storageBackendRule(
          contract: contract,
          implementations: impls,
          reads: _reads,
          nonReads: _nonReads,
        ),
        isEmpty,
      );
    });

    test('(a) concrete-only backend members are reads, named operations or '
        'internal', () {
      final owners = backendOwners(libraries, const <String>{
        'SembastBackend',
        'PostgresBackend',
      });
      expect(
        concreteBackendRule(
          contract: contractOf(libraries),
          owners: owners,
          allowlist: _concreteOperations,
        ),
        isEmpty,
      );
    });

    test('(b) the barrel exports neither drain nor fillBatch', () {
      expect(
        forbiddenExportsRule(barrel, const <String>{'drain', 'fillBatch'}),
        isEmpty,
      );
    });

    test('(c) no unannotated raw handle on any type, and no function-typed '
        'state on the backends', () {
      final backends = backendOwners(libraries, const <String>{
        'SembastBackend',
        'PostgresBackend',
        'Transaction',
      });
      expect(
        backends.map((o) => o.name),
        containsAll(<String>[
          'SembastBackend',
          'PostgresBackend',
          'SembastBackendTestSupport',
          'PostgresTxn',
        ]),
      );
      expect(
        rawHandleRule(
          owners: instanceOwners(libraries),
          sanctioned: _sanctionedRawHandles,
          functionTypedOwners: backends,
        ),
        isEmpty,
      );
    });

    test('(d) no test seam on the exported surface', () async {
      expect(
        seamSurfaceRule(barrel: barrel, allowlist: _functionTyped),
        isEmpty,
      );
      final units = await _unitsUnderLib(scanner);
      expect(units.keys, contains('lib/src/testing/delivery_test_hooks.dart'));
      expect(zoneReadRule(units), isEmpty);
    });

    test('(e) the library logs only through its internal logger', () async {
      expect(directLoggingRule(await _unitsUnderLib(scanner)), isEmpty);
    });

    test('(f) the must-be-internal set is internal', () {
      final owners = <InstanceElement>[
        ...libraries.expand((l) => l.classes),
        ...libraries.expand((l) => l.extensions),
      ];
      expect(
        mustBeInternalRule(
          owners: owners,
          required: _mustBeInternal,
          functions: libraries.expand((l) => l.topLevelFunctions),
        ),
        isEmpty,
      );
    });

    test('(f) exported functions and bundle members are library operations '
        'or internal', () {
      expect(
        libraryOperationsRule(
          barrel: barrel,
          topLevelOperations: _topLevelOperations,
          bundle: classNamed(libraries, 'EventStoreBundle'),
          bundleOperations: _bundleOperations,
        ),
        isEmpty,
      );
    });

    test('(g) the unexported surface a src import reaches is internal or a '
        'named operation', () {
      expect(
        unexportedSurfaceRule(
          libraries: libraries,
          barrel: barrel,
          operations: _unexportedOperations,
        ),
        isEmpty,
      );
    });
  });

  group('synthetic fixtures fail the rules', () {
    late SurfaceScanner fixtures;
    late List<LibraryElement> real;
    late LibraryElement backendsLib;
    late LibraryElement fixtureBarrel;
    late LibraryElement copiesLib;
    late LibraryElement writerLib;
    late Map<String, List<CompilationUnit>> zoneUnits;

    setUpAll(() async {
      fixtures = SurfaceScanner(
        overlays: const <String, String>{
          'lib/src/storage/scan_fixture_backends.dart': _fixtureBackends,
          'lib/src/scan_fixture_exports.dart': _fixtureExports,
          'lib/src/testing/scan_fixture_seam.dart': _fixtureTestingSeam,
          'lib/scan_fixture_barrel.dart': _fixtureBarrel,
          'lib/src/scan_fixture_internal_copies.dart': _fixtureInternalCopies,
          'lib/src/sync/scan_fixture_writer.dart': _fixtureUnexportedWriter,
          'lib/src/sync/scan_fixture_zone.dart': _fixtureZoneAndLog,
        },
      );
      real = <LibraryElement>[
        (await fixtures.library('lib/src/storage/storage_backend.dart'))!,
      ];
      backendsLib = (await fixtures.library(
        'lib/src/storage/scan_fixture_backends.dart',
      ))!;
      fixtureBarrel = (await fixtures.library('lib/scan_fixture_barrel.dart'))!;
      copiesLib = (await fixtures.library(
        'lib/src/scan_fixture_internal_copies.dart',
      ))!;
      writerLib = (await fixtures.library(
        'lib/src/sync/scan_fixture_writer.dart',
      ))!;
      zoneUnits = <String, List<CompilationUnit>>{
        'lib/src/sync/scan_fixture_zone.dart': await fixtures.units(
          'lib/src/sync/scan_fixture_zone.dart',
        ),
      };
    });

    InterfaceElement fixtureClass(String name) =>
        classNamed(<LibraryElement>[backendsLib], name);

    test('(a) an unannotated override fails', () {
      final violations = storageBackendRule(
        contract: classNamed(real, 'StorageBackend'),
        implementations: <InterfaceElement>[
          fixtureClass('ScanFixtureOverrideBackend'),
        ],
        reads: _reads,
        nonReads: _nonReads,
      );
      expect(violations, <Matcher>[
        contains('ScanFixtureOverrideBackend.markFinal'),
      ]);
    });

    test('(a) an unannotated new mutator fails', () {
      final violations = storageBackendRule(
        contract: fixtureClass('ScanFixtureContract'),
        implementations: const <InterfaceElement>[],
        reads: const <String, String>{'readThing': 'a read'},
        nonReads: const <String, String>{},
        checkStale: false,
      );
      expect(violations, contains(contains('purgeEverything')));
    });

    test('(a) a non-read added to the reads category fails', () {
      final violations = storageBackendRule(
        contract: fixtureClass('ScanFixtureContract'),
        implementations: const <InterfaceElement>[],
        reads: const <String, String>{
          'purgeEverything': 'wrongly listed',
          'readThing': 'a read',
        },
        nonReads: const <String, String>{},
        checkStale: false,
      );
      expect(
        violations,
        contains(contains('reads category holds "purgeEverything"')),
      );
    });

    test('(a) an allowlisted read that carries @internal fails', () {
      final violations = storageBackendRule(
        contract: fixtureClass('ScanFixtureContract'),
        implementations: const <InterfaceElement>[],
        reads: const <String, String>{'readThing': 'a read'},
        nonReads: const <String, String>{'purgeEverything': 'listed'},
        checkStale: false,
      );
      expect(violations, <String>[
        'ScanFixtureContract.readThing is on an allowlist yet carries @internal',
      ]);
    });

    test('(a) an unannotated concrete-only mutator on either backend '
        'fails', () {
      final violations = concreteBackendRule(
        contract: classNamed(real, 'StorageBackend'),
        owners: <InstanceElement>[
          fixtureClass('ScanFixtureSembastConcrete'),
          fixtureClass('ScanFixturePostgresConcrete'),
        ],
        allowlist: _concreteOperations,
        checkStale: false,
      );
      expect(violations, <Matcher>[
        contains('ScanFixtureSembastConcrete.resetFifo is a concrete-only'),
        contains('ScanFixturePostgresConcrete.purgeQueue is a concrete-only'),
      ]);
    });

    test('(b) an export line for drain and fillBatch fails', () {
      expect(
        forbiddenExportsRule(fixtureBarrel, const <String>{
          'drain',
          'fillBatch',
        }),
        <String>['the barrel exports drain', 'the barrel exports fillBatch'],
      );
    });

    test('(c) raw-handle accessors under any name, alias, prefix, wrapper '
        'or owner fail, as does function-typed state', () {
      final raw = fixtureClass('ScanFixtureRawBackend');
      final violations = rawHandleRule(
        owners: <InstanceElement>[
          raw,
          fixtureClass('ScanFixtureTxn'),
          fixtureClass('ScanFixtureIdempotencyStore'),
          fixtureClass('ScanFixtureSanctioned'),
        ],
        sanctioned: const <String>{'ScanFixtureSanctioned.db'},
        functionTypedOwners: <InstanceElement>[raw],
      );
      expect(
        violations,
        unorderedEquals(<Matcher>[
          contains('ScanFixtureRawBackend.rawOf returns a raw'),
          contains('ScanFixtureRawBackend.aliasedDatabase returns a raw'),
          contains('ScanFixtureRawBackend.prefixedDatabase returns a raw'),
          contains('ScanFixtureRawBackend.databases returns a raw'),
          contains('ScanFixtureRawBackend.paired returns a raw'),
          contains('ScanFixtureRawBackend.rawDatabase= returns a raw'),
          contains(
            'ScanFixtureRawBackend.traceLog is a public field of '
            'function type',
          ),
          contains(
            'ScanFixtureRawBackend.onTrace= is a public setter of '
            'function type',
          ),
          contains('ScanFixtureTxn.raw returns a raw'),
          contains('ScanFixtureIdempotencyStore.pool returns a raw'),
          contains(
            'ScanFixtureSanctioned.db returns a raw handle and lacks '
            '@internal',
          ),
        ]),
      );
    });

    test('(d) seam-named parameters and fields of any type, a testing type '
        'in a signature, an unlisted function-typed field and a testing '
        'export fail', () {
      final violations = seamSurfaceRule(
        barrel: fixtureBarrel,
        allowlist: _functionTyped,
      );
      expect(
        violations,
        containsAll(<Matcher>[
          contains('configureDelivery(onHook) carries a seam name'),
          contains('configureSwitch(skipFenceForTest) carries a seam name'),
          contains(
            'configureOptions(options) mentions a type declared under '
            'lib/src/testing/',
          ),
          contains('ScanFixtureSink.traceSink (field) carries a seam name'),
          contains(
            'ScanFixtureCallbacks.onDone (field) is a function-typed '
            'surface not on the allowlist',
          ),
          contains('the barrel exports ScanFixtureSeam'),
        ]),
      );
    });

    test('(d) a zone read outside the seam file fails however the zone is '
        'reached', () {
      expect(zoneReadRule(zoneUnits), <Matcher>[
        contains('z[#key]'),
        contains('Zone.current[#key]'),
      ]);
    });

    test('(e) a direct print or developer log fails', () {
      expect(directLoggingRule(zoneUnits), <Matcher>[
        contains("print('direct')"),
        contains("developer.log('direct')"),
      ]);
    });

    test('(f) an unannotated copy of each must-be-internal member fails', () {
      final owners = <InstanceElement>[
        ...copiesLib.classes,
        ...copiesLib.extensions,
      ];
      final violations = mustBeInternalRule(
        owners: owners,
        required: _mustBeInternal,
        functions: copiesLib.topLevelFunctions,
      );
      expect(violations, <String>[
        for (final key in _mustBeInternal.keys) '$key lacks @internal',
      ]);
    });

    test('(f) an exported top-level function on neither list, and a bundle '
        'member on neither list, fail', () {
      final violations = libraryOperationsRule(
        barrel: fixtureBarrel,
        topLevelOperations: _topLevelOperations,
        bundle: classNamed(<LibraryElement>[copiesLib], 'EventStoreBundle'),
        bundleOperations: _bundleOperations,
      );
      expect(
        violations,
        containsAll(<Matcher>[
          contains('exported top-level function scanFixtureOperation'),
          contains('EventStoreBundle.setViewTargetVersion is neither'),
        ]),
      );
    });

    test('(g) an unexported, unannotated queue writer fails', () {
      final violations = unexportedSurfaceRule(
        libraries: <LibraryElement>[writerLib],
        barrel: fixtureBarrel,
        operations: _unexportedOperations,
        checkStale: false,
      );
      expect(violations, <Matcher>[
        contains('unexported top-level function rewindQueue'),
        contains('ScanFixtureWriter.purge (unexported)'),
      ]);
    });
  });
}
