// Runs the boot-progress scenarios on Sembast, plus the reports of a boot
// whose transaction body runs twice, and the observer threaded through
// bootstrapEventStore and EventStore.openForTest. The scenarios'
// assertions are cited on their own tests in
// test_support/boot_progress_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/boot_progress_conformance.dart';
import '../test_support/rerunning_sembast_backend.dart';
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;

class _SembastProgressDatabase implements VersionTestDatabase {
  _SembastProgressDatabase(this._db, {this.backend});

  final Database _db;

  /// The one backend every open uses, when set.
  final SembastBackend? backend;

  @override
  Future<StorageBackend> openBackend() async =>
      backend ?? SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _dbCounter = 0;

Future<Database> _memoryDatabase() {
  _dbCounter += 1;
  return newDatabaseFactoryMemory().openDatabase('progress-$_dbCounter.db');
}

void main() {
  runBootProgressScenarios(
    () async => _SembastProgressDatabase(await _memoryDatabase()),
    backendLabel: 'sembast (memory)',
  );

  group('boot progress on a transaction body that runs twice', () {
    // Verifies: EVS-DEV-event-store-open/I+J
    test('each run reports its phases again from the checks, and the '
        'completion is reported once, after the committed run', () async {
      final database = await _memoryDatabase();
      final backend = RerunningSembastBackend(database: database)
        ..rerunEnabled = false;
      final db = _SembastProgressDatabase(database, backend: backend);
      final older = await openProgressStore(
        db,
        backend,
        registered: const EntryTypeVersion(1, 0),
        projections: const <ProjectionSpec>[
          AggregateProjectionSpec(
            viewName: 'progress_notes',
            interest: SubscriptionFilter(entryTypes: <String>{'progress_note'}),
            tombstoneEventTypes: <String>{},
          ),
        ],
      );
      await seedProgressNotes(older, kProgressScenarioAggregates);

      backend.rerunEnabled = true;
      final reports = <BootProgress>[];
      final newer = await openProgressStore(
        db,
        backend,
        registered: const EntryTypeVersion(1, 1),
        projections: const <ProjectionSpec>[
          AggregateProjectionSpec(
            viewName: 'progress_notes',
            interest: SubscriptionFilter(entryTypes: <String>{'progress_note'}),
            tombstoneEventTypes: <String>{},
          ),
        ],
        onBootProgress: reports.add,
      );
      backend.rerunEnabled = false;

      final phases = phasesOf(reports);
      expect(phases.where((p) => p == BootPhase.complete), hasLength(1));
      expect(phases.last, BootPhase.complete);
      // The open's checks, then the second run's.
      expect(phases.where((p) => p == BootPhase.checks), hasLength(2));
      final secondChecks = phases.lastIndexOf(BootPhase.checks);
      final firstRun = reports.sublist(0, secondChecks);
      final secondRun = reports.sublist(secondChecks);
      expectWellFormedReports(firstRun);
      expectWellFormedReports(secondRun);
      for (final run in <List<BootProgress>>[firstRun, secondRun]) {
        final promotion = reportsOf(run, BootPhase.promotion);
        expect(promotion, isNotEmpty);
        expect(promotion.first.total, kProgressScenarioAggregates);
        expect(promotion.last.done, kProgressScenarioAggregates);
      }
      final rows = await newer.backend.findViewRows('progress_notes');
      expect(rows.every((row) => row['b'] == 0), isTrue);
      await database.close();
    });
  });

  group('the observer through the other entry points', () {
    // Verifies: EVS-DEV-event-store-open/G
    test('bootstrapEventStore reports the boot of the open it runs', () async {
      final db = await _memoryDatabase();
      final reports = <BootProgress>[];
      await bootstrapEventStore(
        backend: SembastBackend(database: db),
        source: const Source(
          hopId: 'progress-hop',
          identifier: 'progress-install',
          softwareVersion: 'progress-test',
        ),
        entryTypes: const <EntryTypeDefinition>[],
        destinations: const <Destination>[],
        onBootProgress: reports.add,
      );
      expect(phasesOf(reports), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
      await db.close();
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('EventStore.openForTest reports its boot', () async {
      final db = await _memoryDatabase();
      final backend = SembastBackend(database: db);
      final reports = <BootProgress>[];
      await EventStore.openForTest(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: const Source(
          hopId: 'progress-hop',
          identifier: 'progress-install',
          softwareVersion: 'progress-test',
        ),
        securityContexts: SembastSecurityContextStore(backend: backend),
        onBootProgress: reports.add,
      );
      expect(phasesOf(reports), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
      await db.close();
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('BootProgress is a value: equal fields are equal, and its string '
        'names its fields', () {
      const a = BootProgress(
        phase: BootPhase.promotion,
        done: 3,
        total: 7,
        elapsed: Duration(milliseconds: 12),
      );
      const b = BootProgress(
        phase: BootPhase.promotion,
        done: 3,
        total: 7,
        elapsed: Duration(milliseconds: 12),
      );
      expect(a, b);
      expect(a.hashCode, b.hashCode);
      expect(
        a,
        isNot(
          const BootProgress(
            phase: BootPhase.catchUp,
            done: 3,
            total: 7,
            elapsed: Duration(milliseconds: 12),
          ),
        ),
      );
      expect('$a', contains('promotion'));
      expect('$a', contains('3/7'));
    });
  });
}
