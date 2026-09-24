// Runs the boot-progress scenarios on Postgres; gated on PG_TEST_URL. The
// scenarios' assertions are cited on their own tests in
// test_support/boot_progress_conformance.dart; the Postgres-only tests
// below cite theirs.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/boot_progress_conformance.dart';
import '../../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'test_postgres_url.dart';

class _PostgresProgressDatabase implements VersionTestDatabase {
  _PostgresProgressDatabase(this._url);

  final String _url;
  final List<PostgresBackend> _backends = <PostgresBackend>[];

  @override
  Future<StorageBackend> openBackend() async {
    final backend = await PostgresBackend.open(
      url: _url,
      sslMode: SslMode.disable,
      provisionSchema: true,
    );
    _backends.add(backend);
    return backend;
  }

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      PostgresSecurityContextStore(backend: backend as PostgresBackend);

  @override
  Future<void> stop(EventStore store) => store.close();

  @override
  Future<void> close() async {
    for (final backend in _backends) {
      await backend.close();
    }
    _backends.clear();
  }
}

Future<void> _resetSchema(String url) async {
  final tmp = await Connection.open(
    PostgresBackend.endpointFromUrl(url),
    settings: const ConnectionSettings(sslMode: SslMode.disable),
  );
  await tmp.execute('DROP SCHEMA public CASCADE');
  await tmp.execute('CREATE SCHEMA public');
  await tmp.close();
}

/// Commits [contender]'s open transaction once another session of this
/// database waits for a lock, or once [waiter] has completed.
Future<void> _commitOnceBlocked(
  Connection contender,
  Future<Object?> waiter,
) async {
  var done = false;
  unawaited(waiter.whenComplete(() => done = true));
  final deadline = DateTime.now().add(const Duration(seconds: 30));
  while (!done) {
    final waiting = await contender.execute(
      "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' "
      'AND datname = current_database()',
    );
    if ((waiting.first[0]! as int) > 0) break;
    if (DateTime.now().isAfter(deadline)) {
      fail('no session waited for the contender');
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  await contender.execute('COMMIT');
}

Future<void> _until(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 30));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition not reached');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  final url = testPostgresUrl();
  runBootProgressScenarios(() async {
    if (url == null) return null;
    await _resetSchema(url);
    return _PostgresProgressDatabase(url);
  }, backendLabel: 'postgres');

  group('boot progress on Postgres', () {
    _PostgresProgressDatabase? db;

    setUp(() async {
      if (url == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      await _resetSchema(url);
      db = _PostgresProgressDatabase(url);
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    const projections = <ProjectionSpec>[
      kProgressAggregateSpec,
      kProgressNewAggregateSpec,
    ];

    /// Seeds [kProgressScenarioAggregates] notes under the older build
    /// (`1.0`, promoted view only) and stops it.
    Future<void> seedOlder() async {
      final older = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 0),
        projections: const <ProjectionSpec>[kProgressAggregateSpec],
      );
      await seedProgressNotes(older, kProgressScenarioAggregates);
      await older.close();
    }

    // Verifies: EVS-DEV-event-store-open/I+J
    test('a boot a serialization failure aborted after its promotion reports '
        'its phases again from the checks, and its completion once, last; '
        'the database holds what one run writes', () async {
      if (db == null) return;
      await seedOlder();
      final baselineStore = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 1),
        projections: projections,
      );
      final baseline = await bootOutcome(baselineStore);
      await baselineStore.close();
      await db!.close();
      await _resetSchema(url!);
      db = _PostgresProgressDatabase(url);
      await seedOlder();

      // The contender writes, without committing, the first row the boot's
      // re-derivation of the new view writes, so the first run reports its
      // promotion, starts the re-derivation, waits for the contender, and
      // fails to serialize once the contender commits.
      final contender = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await contender.execute('BEGIN');
      await contender.execute(
        'INSERT INTO view_rows (view_name, row_key, row_data, updated_at) '
        "VALUES ('$kProgressNewAggregateView', 'agg-0000', '{}', NOW())",
      );
      var bootRuns = 0;
      final reports = <BootProgress>[];
      final backend = await db!.openBackend();
      final open = runWithDeliveryTestHooks(
        DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
        () => openProgressStore(
          db!,
          backend,
          registered: const EntryTypeVersion(1, 1),
          projections: projections,
          onBootProgress: reports.add,
        ),
      );
      await _commitOnceBlocked(contender, open);
      final store = await open;
      await contender.close();

      expect(bootRuns, 2);
      final phases = phasesOf(reports);
      expect(phases.where((p) => p == BootPhase.checks), hasLength(2));
      expect(phases.where((p) => p == BootPhase.complete), hasLength(1));
      expect(phases.last, BootPhase.complete);
      final secondChecks = phases.lastIndexOf(BootPhase.checks);
      final firstRun = reports.sublist(0, secondChecks);
      final secondRun = reports.sublist(secondChecks);
      // The first run was discarded while it re-derived the new view: its
      // phases up to then are complete, and its re-derivation reported its
      // start only.
      final firstCatchUp = phasesOf(firstRun).indexOf(BootPhase.catchUp);
      expect(firstCatchUp, greaterThan(0));
      expectWellFormedReports(firstRun.sublist(0, firstCatchUp));
      expect(
        phasesOf(firstRun.sublist(firstCatchUp)),
        everyElement(BootPhase.catchUp),
      );
      expect(firstRun[firstCatchUp].done, 0);
      expect(phasesOf(firstRun), contains(BootPhase.promotion));
      expectWellFormedReports(secondRun);
      for (final run in <List<BootProgress>>[firstRun, secondRun]) {
        expect(
          reportsOf(run, BootPhase.promotion).last.done,
          kProgressScenarioAggregates,
        );
        expect(
          reportsOf(run, BootPhase.catchUp).first.total,
          kProgressScenarioAggregates,
        );
      }
      expect(await bootOutcome(store), baseline);
    });

    // Verifies: EVS-DEV-event-store-open/I
    test(
      'an open the generation guard refuses reports its checks only',
      () async {
        if (db == null) return;
        final serving = await openProgressStore(
          db!,
          await db!.openBackend(),
          registered: const EntryTypeVersion(1, 0),
          projections: const <ProjectionSpec>[kProgressAggregateSpec],
        );
        final reports = <BootProgress>[];
        await expectLater(
          openProgressStore(
            db!,
            await db!.openBackend(),
            registered: const EntryTypeVersion(2, 0),
            projections: const <ProjectionSpec>[kProgressAggregateSpec],
            onBootProgress: reports.add,
          ),
          throwsA(isA<IncompatibleGenerationException>()),
        );
        expect(phasesOf(reports), <BootPhase>[BootPhase.checks]);
        await serving.close();
      },
    );

    // Verifies: EVS-DEV-event-store-open/G
    test('an open waiting for the boot lock has reported its checks, and '
        'reports its completion once it has booted', () async {
      if (db == null) return;
      final release = Completer<void>();
      final held = Completer<void>();
      final backendA = await db!.openBackend();
      final backendB = await db!.openBackend();
      final openA = runWithDeliveryTestHooks(
        DeliveryTestHooks(
          insideBootLock: () {
            held.complete();
            return release.future;
          },
        ),
        () => openProgressStore(
          db!,
          backendA,
          registered: const EntryTypeVersion(1, 0),
          projections: const <ProjectionSpec>[kProgressAggregateSpec],
        ),
      );
      await held.future;
      var waiting = false;
      var openedB = false;
      final reports = <BootProgress>[];
      final openB =
          runWithDeliveryTestHooks(
            DeliveryTestHooks(
              onLog: (record) {
                if (record.message.contains('waiting for the boot lock')) {
                  waiting = true;
                }
              },
            ),
            () => openProgressStore(
              db!,
              backendB,
              registered: const EntryTypeVersion(1, 0),
              projections: const <ProjectionSpec>[kProgressAggregateSpec],
              onBootProgress: reports.add,
            ),
          ).then((store) {
            openedB = true;
            return store;
          });
      await _until(() => waiting);
      expect(openedB, isFalse);
      expect(phasesOf(reports), <BootPhase>[BootPhase.checks]);
      release.complete();
      await openA;
      await openB;
      expect(phasesOf(reports), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
    });
  });
}
