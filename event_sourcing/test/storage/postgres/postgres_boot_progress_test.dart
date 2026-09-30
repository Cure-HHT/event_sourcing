// Runs the boot-progress scenarios on Postgres; gated on PG_TEST_URL. The
// scenarios' assertions are cited on their own tests in
// test_support/boot_progress_conformance.dart; the Postgres-only tests
// below cite theirs.
//
// Each observer scenario seeds and boots a promotion twice; a CI runner
// takes several times longer than a workstation, so the per-test limit is
// raised from the default.

@TestOn('vm')
@Timeout(Duration(minutes: 3))
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:test/test.dart';

import '../../test_support/boot_progress_conformance.dart';
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

Future<void> _until(bool Function() condition) async {
  final deadline = DateTime.now().add(const Duration(seconds: 30));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('condition not reached');
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
}

void main() {
  final pg = PostgresTestDatabase.fromEnvironment();
  if (pg != null) tearDownAll(pg.drop);
  runBootProgressScenarios(
    () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
  );

  group('boot progress on Postgres', () {
    PostgresScenarioDatabase? db;

    setUp(() async {
      if (pg == null) {
        markTestSkipped('PG_TEST_URL unset');
        return;
      }
      db = await PostgresScenarioDatabase.fresh(pg);
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    // Verifies: EVS-DEV-event-store-open/I
    test(
      'an open the generation guard refuses reports its checks only',
      () async {
        if (db == null) return;
        final serving = await openProgressStore(db!, await db!.openBackend());
        final reports = <BootProgress>[];
        await expectLater(
          openProgressStore(
            db!,
            await db!.openBackend(),
            registered: const EntryTypeVersion(2, 0),
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
        () => openProgressStore(db!, backendA),
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
            () => openProgressStore(db!, backendB, onBootProgress: reports.add),
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
