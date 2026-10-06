// Backend-agnostic scenarios for the progress an open reports to its
// optional observer: its phases (checks, then completion), the elapsed
// time, and the observer's inability to change or observe into the boot.
// Run on Sembast by test/event_store/boot_progress_test.dart and on
// Postgres by test/storage/postgres/postgres_boot_progress_test.dart.
//
// Traceability lives on the individual tests below.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/postgres.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'deliveries.dart';
import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const _kType = 'progress_note';
const _kAggregateView = 'progress_notes';
const _kTableView = 'progress_note_rows';

const _kAggregateSpec = AggregateProjectionSpec(
  viewName: _kAggregateView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kTableSpec = TableProjectionSpec(
  viewName: _kTableView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

const _kSource = Source(
  hopId: 'progress-hop',
  identifier: 'progress-install',
  softwareVersion: 'progress-test',
);

/// Opens an event store over [backend] with [_kType] at [registered] and
/// [_kAggregateSpec] and [_kTableSpec] registered, reporting to
/// [onBootProgress].
Future<EventStore> openProgressStore(
  VersionTestDatabase db,
  StorageBackend backend, {
  EntryTypeVersion registered = const EntryTypeVersion(1, 0),
  void Function(BootProgress progress)? onBootProgress,
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      EntryTypeDefinition(
        id: _kType,
        registeredVersion: registered,
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry()
    ..register(_kAggregateSpec)
    ..register(_kTableSpec);
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: entryTypes,
    source: _kSource,
    projections: registry,
    onBootProgress: onBootProgress,
  );
}

/// Appends one `finalized` note to each of [count] aggregates.
Future<void> seedProgressNotes(EventStore store, int count) async {
  for (var i = 0; i < count; i++) {
    await store.append(
      entryType: _kType,
      aggregateId: 'agg-${i.toString().padLeft(4, '0')}',
      aggregateType: 'note',
      eventType: 'finalized',
      data: <String, Object?>{'a': i},
      initiator: const UserInitiator('progress-user'),
    );
  }
}

/// The reports of one open, in the order the observer received them.
typedef Reports = List<BootProgress>;

List<BootPhase> phasesOf(Reports reports) => <BootPhase>[
  for (final report in reports) report.phase,
];

/// Asserts every report of [reports] carries no units (`done == total ==
/// 0`, the boot's two phases both count none) and `elapsed` never falls.
void expectWellFormedReports(Reports reports) {
  for (var i = 1; i < reports.length; i++) {
    expect(
      reports[i].elapsed >= reports[i - 1].elapsed,
      isTrue,
      reason: 'elapsed fell at report $i: ${reports[i - 1]} -> ${reports[i]}',
    );
  }
  for (final report in reports) {
    expect(report.done, 0, reason: '$report');
    expect(report.total, 0, reason: '$report');
  }
}

/// The class name a refusal on [backend]'s own transaction names.
String _backendName(StorageBackend backend) =>
    backend is PostgresBackend ? 'PostgresBackend' : 'SembastBackend';

/// Registers the boot-progress scenarios against databases produced by
/// [openDatabase]. A null database skips the test.
void runBootProgressScenarios(
  Future<VersionTestDatabase?> Function() openDatabase, {
  required String backendLabel,
}) {
  group('boot progress ($backendLabel)', () {
    VersionTestDatabase? db;

    setUp(() async {
      db = await openDatabase();
      if (db == null) markTestSkipped('no database for $backendLabel');
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('an open reports its checks and its completion only, the first '
        'open and a later one', () async {
      if (db == null) return;
      final first = <BootProgress>[];
      final store = await openProgressStore(
        db!,
        await db!.openBackend(),
        onBootProgress: first.add,
      );
      await seedProgressNotes(store, 3);
      await db!.stop(store);
      expect(phasesOf(first), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
      expectWellFormedReports(first);

      final later = <BootProgress>[];
      await openProgressStore(
        db!,
        await db!.openBackend(),
        onBootProgress: later.add,
      );
      expect(phasesOf(later), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
      expectWellFormedReports(later);
    });

    // Verifies: EVS-DEV-event-store-open/L
    test('an observer that throws is logged, keeps receiving reports, and '
        'leaves the boot writing what it writes without one', () async {
      if (db == null) return;
      final baselineReports = <BootProgress>[];
      await openProgressStore(
        db!,
        await db!.openBackend(),
        onBootProgress: baselineReports.add,
      );

      final received = <BootProgress>[];
      final logged = <LibraryLogRecord>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(onLog: logged.add),
        () async => openProgressStore(
          db!,
          await db!.openBackend(),
          onBootProgress: (progress) {
            received.add(progress);
            throw StateError('observer failure ${received.length}');
          },
        ),
      );

      expect(
        <Object>[
          for (final r in received) <Object>[r.phase, r.done, r.total],
        ],
        <Object>[
          for (final r in baselineReports) <Object>[r.phase, r.done, r.total],
        ],
      );
      final failures = <LibraryLogRecord>[
        for (final record in logged)
          if (record.error is StateError &&
              '${record.error}'.contains('observer failure'))
            record,
      ];
      expect(failures, hasLength(received.length));
      expect(failures.every((r) => r.level == LibraryLogLevel.severe), isTrue);
    });

    // Verifies: EVS-DEV-event-store-open/L
    test('an async observer whose future fails is logged once per report, and '
        'the boot writes what it writes without one', () async {
      if (db == null) return;
      var received = 0;
      final logged = <LibraryLogRecord>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(onLog: logged.add),
        () async => openProgressStore(
          db!,
          await db!.openBackend(),
          onBootProgress: (progress) async {
            received += 1;
            await Future<void>.value();
            throw StateError('async observer failure $received');
          },
        ),
      );
      await pumpEventQueue();

      final failures = <LibraryLogRecord>[
        for (final record in logged)
          if ('${record.error}'.contains('async observer failure')) record,
      ];
      expect(received, 2);
      expect(failures, hasLength(received));
      expect(failures.every((r) => r.level == LibraryLogLevel.severe), isTrue);
    });

    // Verifies: EVS-DEV-event-store-open/L
    test('a timer the observer started that throws is logged, not raised in '
        "the caller's zone", () async {
      if (db == null) return;
      var timers = 0;
      final logged = <LibraryLogRecord>[];
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(onLog: logged.add),
        () async => openProgressStore(
          db!,
          await db!.openBackend(),
          onBootProgress: (progress) {
            timers += 1;
            final n = timers;
            Timer.run(() => throw StateError('timer failure $n'));
          },
        ),
      );
      await pumpEventQueue();

      final failures = <LibraryLogRecord>[
        for (final record in logged)
          if ('${record.error}'.contains('timer failure')) record,
      ];
      expect(timers, 2);
      expect(failures, hasLength(timers));
      expect(failures.every((r) => r.level == LibraryLogLevel.severe), isTrue);
    });

    // Verifies: EVS-DEV-event-store-open/M
    test(
      'an observer calling back into an event store or a storage backend '
      'during the boot, directly or from work it started, is refused with '
      'StateError and changes nothing; its calls after the boot run',
      () async {
        if (db == null) return;
        // A store over the same database that stays open while the newer
        // instance boots.
        final other = await openProgressStore(db!, await db!.openBackend());
        await seedProgressNotes(other, 2);
        final stored = (await other.reader.findAllEvents(
          entryType: _kType,
        )).first;
        final registry = DestinationRegistry(eventStore: other);
        final backend = await db!.openBackend();
        final logged = <LibraryLogRecord>[];
        final bootDone = Completer<void>();
        final afterBoot = Completer<void>();
        var calls = 0;
        var bootReturned = false;
        final ranDuringBoot = <String>[];

        Future<StoredEvent?> appendNote(String aggregateId, int a) =>
            other.append(
              entryType: _kType,
              aggregateId: aggregateId,
              aggregateType: 'note',
              eventType: 'finalized',
              data: <String, Object?>{'a': a},
              initiator: const UserInitiator('progress-user'),
            );

        final store = await runWithDeliveryTestHooks(
          DeliveryTestHooks(onLog: logged.add),
          () async => openProgressStore(
            db!,
            backend,
            onBootProgress: (progress) {
              calls += 1;
              if (progress.phase == BootPhase.complete) return;
              // Direct calls, each refused.
              unawaited(appendNote('reentrant-append', -1));
              unawaited(openProgressStore(db!, backend));
              unawaited(
                other.runTransaction<void>((txn, collector) async {
                  ranDuringBoot.add('runTransaction');
                }),
              );
              unawaited(ingestEventForTest(other, stored));
              unawaited(
                rebuildView(
                  store: other,
                  viewName: _kAggregateView,
                  deadline: DateTime.now().toUtc().add(
                    const Duration(seconds: 20),
                  ),
                ),
              );
              unawaited(registry.readDeliveryStatus());
              // The reader refuses synchronously; Future.sync delivers the
              // refusal to the observer's zone as the other calls' do.
              unawaited(
                Future<void>.sync(
                  () => other.reader.transaction<void>((txn) async {
                    ranDuringBoot.add('reader.transaction');
                  }),
                ),
              );
              // Work the observer started that runs while the boot still
              // runs, each refused.
              scheduleMicrotask(
                () => unawaited(appendNote('reentrant-microtask', -3)),
              );
              unawaited(
                Future<void>.microtask(
                  () => other.runTransaction<void>((txn, collector) async {
                    ranDuringBoot.add('microtask runTransaction');
                  }),
                ),
              );
              // A call the observer starts that runs once the boot has
              // finished is not refused.
              unawaited(
                bootDone.future
                    .then((_) => appendNote('after-boot', -2))
                    .whenComplete(afterBoot.complete),
              );
            },
          ),
        ).whenComplete(() => bootReturned = true);
        expect(calls, 2);
        expect(bootReturned, isTrue);
        await pumpEventQueue();
        expect(ranDuringBoot, isEmpty);
        final refusals = <LibraryLogRecord>[
          for (final record in logged)
            if (record.error is StateError &&
                '${record.error}'.contains('boot progress'))
              record,
        ];
        expect(
          <String>[
            for (final r in refusals) '${r.error}'.split(' was called').first,
          ]..sort(),
          <String>[
            'Bad state: An EventStore transaction',
            'Bad state: An EventStore transaction',
            'Bad state: An EventStore transaction',
            'Bad state: An EventStore transaction',
            'Bad state: An EventStore transaction',
            'Bad state: EventStore.open',
            'Bad state: ${_backendName(backend)}.transaction',
            'Bad state: StorageReader.transaction',
            'Bad state: rebuildView',
          ]..sort(),
          reason: '$logged',
        );
        expect(
          refusals.every((r) => r.level == LibraryLogLevel.severe),
          isTrue,
        );
        expect(
          (await store.reader.findAllEvents()).map((e) => e.aggregateId),
          isNot(contains('reentrant-append')),
          reason: 'the refused calls wrote nothing',
        );

        bootDone.complete();
        await afterBoot.future.timeout(const Duration(seconds: 10));
        final notes = await other.reader.findAllEvents(entryType: _kType);
        expect(notes.map((e) => e.aggregateId), contains('after-boot'));
        expect(
          notes.map((e) => e.aggregateId).where((id) => id.startsWith('re')),
          isEmpty,
        );
        await db!.stop(other);
      },
    );

    // Verifies: EVS-DEV-event-store-open/K
    test('the boot does not await an observer that returns a future that '
        'never completes', () async {
      if (db == null) return;
      final never = Completer<void>();
      var calls = 0;
      await openProgressStore(
        db!,
        await db!.openBackend(),
        onBootProgress: (progress) async {
          calls += 1;
          await never.future;
        },
      ).timeout(const Duration(seconds: 30));
      expect(calls, 2);
    });

    // Verifies: EVS-DEV-event-store-open/I
    test(
      'a refused open reports its checks and never its completion',
      () async {
        if (db == null) return;
        final first = await openProgressStore(
          db!,
          await db!.openBackend(),
          registered: const EntryTypeVersion(2, 0),
        );
        await seedProgressNotes(first, 2);
        await db!.stop(first);
        final reports = <BootProgress>[];
        await expectLater(
          openProgressStore(
            db!,
            await db!.openBackend(),
            onBootProgress: reports.add,
          ),
          throwsA(isA<EntryTypeVersionDowngradeError>()),
        );
        expect(phasesOf(reports), <BootPhase>[BootPhase.checks]);
      },
    );
  });
}
