// Backend-agnostic scenarios for the progress an open reports to its
// optional observer: the phases, the counted units of each phase, and the
// observer's inability to change the boot. Run on Sembast by
// test/event_store/boot_progress_test.dart and on Postgres by
// test/storage/postgres/postgres_boot_progress_test.dart.
//
// Traceability lives on the individual tests below.
import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'version_compatibility_conformance.dart' show VersionTestDatabase;

const _kType = 'progress_note';
const _kAggregateView = 'progress_notes';
const _kTableView = 'progress_note_rows';
const _kNewAggregateView = 'progress_notes_new';
const _kNewTableView = 'progress_note_rows_new';

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

const _kNewAggregateSpec = AggregateProjectionSpec(
  viewName: _kNewAggregateView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kNewTableSpec = TableProjectionSpec(
  viewName: _kNewTableView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: AggregateIdKey(),
  rowData: WholePayload(),
);

/// The aggregate view the promotion scenarios promote.
const ProjectionSpec kProgressAggregateSpec = _kAggregateSpec;

/// A new aggregate view the catch-up scenarios re-derive, and its name.
const ProjectionSpec kProgressNewAggregateSpec = _kNewAggregateSpec;
const String kProgressNewAggregateView = _kNewAggregateView;

const _kSource = Source(
  hopId: 'progress-hop',
  identifier: 'progress-install',
  softwareVersion: 'progress-test',
);

/// More aggregates than one progress chunk holds, so a phase over them
/// reports between its start and its end.
const int kProgressScenarioAggregates = 600;

/// The `1.0 -> 1.1` step adding `b` with the default `0` to [viewName].
PromoterSpec _defaultB(String viewName) => PromoterSpec(
  viewName: viewName,
  entryType: _kType,
  fromVersion: const EntryTypeVersion(1, 0),
  toVersion: const EntryTypeVersion(1, 1),
  transforms: const <TransformPrimitive>[
    DefaultField(fieldName: 'b', defaultValue: 0),
  ],
);

/// Opens an event store over [backend] with [_kType] at [registered], the
/// [projections] (with the `1.1` minor step's promoter for each when
/// [registered] is `1.1`), reporting to [onBootProgress].
Future<EventStore> openProgressStore(
  VersionTestDatabase db,
  StorageBackend backend, {
  required EntryTypeVersion registered,
  required List<ProjectionSpec> projections,
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
  final promoters = PromoterRegistry();
  if (registered == const EntryTypeVersion(1, 1)) {
    for (final spec in projections) {
      promoters.register(_defaultB(spec.viewName));
    }
  }
  final registry = ProjectionRegistry();
  for (final spec in projections) {
    registry.register(spec);
  }
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, db.securityFor(backend)),
    entryTypes: entryTypes,
    source: _kSource,
    projections: registry,
    promoters: promoters,
    onBootProgress: onBootProgress,
  );
}

/// Appends one `finalized` note to each of [count] aggregates, in
/// transactions of 100.
Future<void> seedProgressNotes(EventStore store, int count) async {
  for (var start = 0; start < count; start += 100) {
    final end = start + 100 < count ? start + 100 : count;
    await store.runTransaction<void>((txn, collector) async {
      for (var i = start; i < end; i++) {
        await store.appendInTxn(
          txn,
          entryType: _kType,
          aggregateId: 'agg-${i.toString().padLeft(4, '0')}',
          aggregateType: 'note',
          eventType: 'finalized',
          data: <String, Object?>{'a': i},
          initiator: const UserInitiator('progress-user'),
          flowToken: null,
          metadata: null,
          security: null,
          checkpointReason: null,
          changeReason: null,
          dedupeByContent: false,
          collector: collector,
        );
      }
    });
  }
}

/// The reports of one open, in the order the observer received them.
typedef Reports = List<BootProgress>;

List<BootPhase> phasesOf(Reports reports) => <BootPhase>[
  for (final report in reports) report.phase,
];

/// Asserts the reports of one run of the boot body: every phase's first
/// report has `done == 0`, its `total` never changes, its `done` never
/// falls, and its last report has `done == total`; and `elapsed` never
/// falls across all of [reports].
void expectWellFormedReports(Reports reports) {
  for (var i = 1; i < reports.length; i++) {
    expect(
      reports[i].elapsed >= reports[i - 1].elapsed,
      isTrue,
      reason: 'elapsed fell at report $i: ${reports[i - 1]} -> ${reports[i]}',
    );
  }
  var i = 0;
  while (i < reports.length) {
    final phase = reports[i].phase;
    final start = i;
    while (i < reports.length && reports[i].phase == phase) {
      i++;
    }
    final run = reports.sublist(start, i);
    expect(run.first.done, 0, reason: '$phase starts at done 0: $run');
    for (var j = 0; j < run.length; j++) {
      expect(run[j].total, run.first.total, reason: '$phase total: $run');
      expect(run[j].done <= run[j].total, isTrue, reason: '$phase: $run');
      if (j > 0) {
        expect(run[j].done >= run[j - 1].done, isTrue, reason: '$phase: $run');
      }
    }
    expect(run.last.done, run.last.total, reason: '$phase ends at its total');
  }
}

/// The reports of [phase] in [reports].
Reports reportsOf(Reports reports, BootPhase phase) => <BootProgress>[
  for (final report in reports)
    if (report.phase == phase) report,
];

/// The row fields that carry an event id or a time a fresh database mints.
const _kMintedRowFields = <String>{
  'latestEventId',
  'updatedAt',
  'firstEventTimestamp',
};

/// What a boot leaves in the database, independent of the identity, event
/// ids and timestamps a fresh database mints: every event's position, type
/// and aggregate (and its payload, other than a library-version event's),
/// every view's rows, and the stored view targets.
Future<Map<String, Object?>> bootOutcome(EventStore store) async {
  final events = await store.reader.findAllEvents();
  final views = <String>[
    _kAggregateView,
    _kTableView,
    _kNewAggregateView,
    _kNewTableView,
  ];
  final rows = <String, Object?>{};
  final targets = <String, Object?>{};
  for (final view in views) {
    final held = <Map<String, Object?>>[
      for (final row in await store.reader.findViewRows(view))
        <String, Object?>{
          for (final field in row.entries)
            if (!_kMintedRowFields.contains(field.key)) field.key: field.value,
        },
    ]..sort((x, y) => '$x'.compareTo('$y'));
    rows[view] = held;
    targets[view] = await store.reader.transaction(
      (txn) async => (await store.reader.readViewTargetVersionInTxn(
        txn,
        view,
        _kType,
      ))?.toString(),
    );
  }
  return <String, Object?>{
    'events': <Object?>[
      for (final e in events)
        <Object?>[
          e.sequenceNumber,
          e.entryType,
          e.eventType,
          e.aggregateId,
          if (!e.entryType.startsWith('lib_version')) e.data,
        ],
    ],
    'rows': rows,
    'targets': targets,
  };
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

    Future<VersionTestDatabase?> freshDatabase() async {
      await db?.close();
      return db = await openDatabase();
    }

    setUp(() async {
      db = await openDatabase();
      if (db == null) markTestSkipped('no database for $backendLabel');
    });

    tearDown(() async {
      await db?.close();
      db = null;
    });

    /// Opens the older build (`1.0`) with [projections], seeds
    /// [kProgressScenarioAggregates] notes, and stops it. Returns the
    /// number of events in the log afterwards.
    Future<int> seedOlder(List<ProjectionSpec> projections) async {
      final older = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 0),
        projections: projections,
      );
      await seedProgressNotes(older, kProgressScenarioAggregates);
      final count = (await older.reader.findAllEvents()).length;
      await db!.stop(older);
      return count;
    }

    // Verifies: EVS-DEV-event-store-open/G+H
    test('an open with nothing to promote or catch up reports its checks '
        'and its completion only, the first open and a later one', () async {
      if (db == null) return;
      final first = <BootProgress>[];
      final store = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 0),
        projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
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
        registered: const EntryTypeVersion(1, 0),
        projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
        onBootProgress: later.add,
      );
      expect(phasesOf(later), <BootPhase>[
        BootPhase.checks,
        BootPhase.complete,
      ]);
      expectWellFormedReports(later);
      expect(later.first.total, 0);
      expect(later.last.total, 0);
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('snapshot promotion reports its aggregates and the log events its '
        'table refold reads, counted before it starts, from 0 to the total, '
        'between chunks too', () async {
      if (db == null) return;
      final eventsBefore = await seedOlder(const <ProjectionSpec>[
        _kAggregateSpec,
        _kTableSpec,
      ]);
      final reports = <BootProgress>[];
      final newer = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 1),
        projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
        onBootProgress: reports.add,
      );
      expectWellFormedReports(reports);
      expect(phasesOf(reports).first, BootPhase.checks);
      expect(phasesOf(reports).last, BootPhase.complete);
      expect(phasesOf(reports).toSet(), <BootPhase>{
        BootPhase.checks,
        BootPhase.promotion,
        BootPhase.complete,
      });
      final promotion = reportsOf(reports, BootPhase.promotion);
      expect(promotion.first.total, kProgressScenarioAggregates + eventsBefore);
      expect(
        promotion.where((r) => r.done > 0 && r.done < r.total),
        isNotEmpty,
        reason: 'a phase over more than one chunk reports between chunks',
      );
      // The promotion happened: every row carries the minor's default.
      final rows = await newer.reader.findViewRows(_kAggregateView);
      expect(rows, hasLength(kProgressScenarioAggregates));
      expect(rows.every((row) => row['b'] == 0), isTrue);
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('the re-derivation of views behind the log reports its aggregates '
        'and the log events its table refold reads', () async {
      if (db == null) return;
      final eventsBefore = await seedOlder(const <ProjectionSpec>[]);
      final reports = <BootProgress>[];
      final newer = await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 0),
        projections: const <ProjectionSpec>[_kNewAggregateSpec, _kNewTableSpec],
        onBootProgress: reports.add,
      );
      expectWellFormedReports(reports);
      expect(phasesOf(reports).toSet(), <BootPhase>{
        BootPhase.checks,
        BootPhase.catchUp,
        BootPhase.complete,
      });
      expect(phasesOf(reports).last, BootPhase.complete);
      final catchUp = reportsOf(reports, BootPhase.catchUp);
      expect(catchUp.first.total, kProgressScenarioAggregates + eventsBefore);
      expect(catchUp.where((r) => r.done > 0 && r.done < r.total), isNotEmpty);
      expect(
        await newer.reader.findViewRows(_kNewAggregateView),
        hasLength(kProgressScenarioAggregates),
      );
      expect(
        await newer.reader.findViewRows(_kNewTableView),
        hasLength(kProgressScenarioAggregates),
      );
    });

    // Verifies: EVS-DEV-event-store-open/G
    test('an open that promotes and catches up reports both phases in boot '
        'order', () async {
      if (db == null) return;
      await seedOlder(const <ProjectionSpec>[_kAggregateSpec]);
      final reports = <BootProgress>[];
      await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 1),
        projections: const <ProjectionSpec>[
          _kAggregateSpec,
          _kNewAggregateSpec,
        ],
        onBootProgress: reports.add,
      );
      expectWellFormedReports(reports);
      final order = <BootPhase>[];
      for (final phase in phasesOf(reports)) {
        if (order.isEmpty || order.last != phase) order.add(phase);
      }
      expect(order, <BootPhase>[
        BootPhase.checks,
        BootPhase.promotion,
        BootPhase.catchUp,
        BootPhase.complete,
      ]);
      expect(
        reportsOf(reports, BootPhase.promotion).first.total,
        kProgressScenarioAggregates,
      );
      expect(
        reportsOf(reports, BootPhase.catchUp).first.total,
        kProgressScenarioAggregates,
      );
    });

    /// Seeds the promotion scenario on a fresh database, opens the newer
    /// build reporting to [onBootProgress] (under [hooks] when given), and
    /// returns what the boot left in the database.
    Future<Map<String, Object?>> promoteOnFreshDatabase(
      void Function(BootProgress progress)? onBootProgress, {
      DeliveryTestHooks? hooks,
    }) async {
      if (await freshDatabase() == null) return const <String, Object?>{};
      await seedOlder(const <ProjectionSpec>[_kAggregateSpec, _kTableSpec]);
      final backend = await db!.openBackend();
      Future<EventStore> open() => openProgressStore(
        db!,
        backend,
        registered: const EntryTypeVersion(1, 1),
        projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
        onBootProgress: onBootProgress,
      );
      final store = hooks == null
          ? await open()
          : await runWithDeliveryTestHooks(hooks, open);
      return bootOutcome(store);
    }

    // Verifies: EVS-DEV-event-store-open/L
    test('an observer that throws is logged, keeps receiving reports, and '
        'leaves the boot writing what it writes without one', () async {
      if (db == null) return;
      final baselineReports = <BootProgress>[];
      final baseline = await promoteOnFreshDatabase(baselineReports.add);

      final received = <BootProgress>[];
      final logged = <LibraryLogRecord>[];
      final outcome = await promoteOnFreshDatabase((progress) {
        received.add(progress);
        throw StateError('observer failure ${received.length}');
      }, hooks: DeliveryTestHooks(onLog: logged.add));

      expect(outcome, baseline);
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
      final baseline = await promoteOnFreshDatabase(null);

      var received = 0;
      final logged = <LibraryLogRecord>[];
      final outcome = await promoteOnFreshDatabase((progress) async {
        received += 1;
        await Future<void>.value();
        throw StateError('async observer failure $received');
      }, hooks: DeliveryTestHooks(onLog: logged.add));
      await pumpEventQueue();

      expect(outcome, baseline);
      final failures = <LibraryLogRecord>[
        for (final record in logged)
          if ('${record.error}'.contains('async observer failure')) record,
      ];
      expect(received, greaterThan(2));
      expect(failures, hasLength(received));
      expect(failures.every((r) => r.level == LibraryLogLevel.severe), isTrue);
    });

    // Verifies: EVS-DEV-event-store-open/L
    test('a timer the observer started that throws is logged, not raised in '
        "the caller's zone", () async {
      if (db == null) return;
      final baseline = await promoteOnFreshDatabase(null);

      var timers = 0;
      final logged = <LibraryLogRecord>[];
      final outcome = await promoteOnFreshDatabase((progress) {
        timers += 1;
        final n = timers;
        Timer.run(() => throw StateError('timer failure $n'));
      }, hooks: DeliveryTestHooks(onLog: logged.add));
      await pumpEventQueue();

      expect(outcome, baseline);
      final failures = <LibraryLogRecord>[
        for (final record in logged)
          if ('${record.error}'.contains('timer failure')) record,
      ];
      expect(timers, greaterThan(2));
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
        final baseline = await promoteOnFreshDatabase(null);

        if (await freshDatabase() == null) return;
        await seedOlder(const <ProjectionSpec>[_kAggregateSpec, _kTableSpec]);
        // A store over the same database that stays open while the newer
        // build boots.
        final other = await openProgressStore(
          db!,
          await db!.openBackend(),
          registered: const EntryTypeVersion(1, 0),
          projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
        );
        final stored = (await other.reader.findAllEvents(
          entryType: _kType,
        )).first;
        final registry = DestinationRegistry(eventStore: other);
        final backend = await db!.openBackend();
        final logged = <LibraryLogRecord>[];
        final release = Completer<void>();
        final bootDone = Completer<void>();
        final afterBoot = Completer<void>();
        var calls = 0;
        var released = false;
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
          () => openProgressStore(
            db!,
            backend,
            registered: const EntryTypeVersion(1, 1),
            projections: const <ProjectionSpec>[_kAggregateSpec, _kTableSpec],
            onBootProgress: (progress) {
              if (progress.phase != BootPhase.promotion) return;
              if (calls > 0) {
                // A later report releases work the first report chained.
                if (!released && progress.done > 0) {
                  released = true;
                  release.complete();
                }
                return;
              }
              calls += 1;
              // Direct calls, each refused.
              unawaited(appendNote('reentrant-append', -1));
              unawaited(
                openProgressStore(
                  db!,
                  backend,
                  registered: const EntryTypeVersion(1, 1),
                  projections: const <ProjectionSpec>[
                    _kAggregateSpec,
                    _kTableSpec,
                  ],
                ),
              );
              unawaited(
                other.runTransaction<void>((txn, collector) async {
                  ranDuringBoot.add('runTransaction');
                }),
              );
              unawaited(other.ingestEvent(stored));
              unawaited(
                rebuildView(
                  store: other,
                  viewName: _kAggregateView,
                  targetVersionByEntryType: const <String, EntryTypeVersion>{
                    _kType: EntryTypeVersion(1, 0),
                  },
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
              unawaited(
                release.future.then(
                  (_) => Future<void>.sync(
                    () => other.reader.transaction<void>((txn) async {
                      ranDuringBoot.add('released reader.transaction');
                    }),
                  ),
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
        expect(calls, 1);
        expect(released, isTrue, reason: 'a later promotion report released');
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
          await bootOutcome(store),
          baseline,
          reason: 'the refused calls wrote nothing and changed no decision',
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
      await seedOlder(const <ProjectionSpec>[_kAggregateSpec]);
      final never = Completer<void>();
      var calls = 0;
      await openProgressStore(
        db!,
        await db!.openBackend(),
        registered: const EntryTypeVersion(1, 1),
        projections: const <ProjectionSpec>[_kAggregateSpec],
        onBootProgress: (progress) async {
          calls += 1;
          await never.future;
        },
      ).timeout(const Duration(seconds: 30));
      expect(calls, greaterThan(2));
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
          projections: const <ProjectionSpec>[_kAggregateSpec],
        );
        await seedProgressNotes(first, 2);
        await db!.stop(first);
        final reports = <BootProgress>[];
        await expectLater(
          openProgressStore(
            db!,
            await db!.openBackend(),
            registered: const EntryTypeVersion(1, 0),
            projections: const <ProjectionSpec>[_kAggregateSpec],
            onBootProgress: reports.add,
          ),
          throwsA(isA<EntryTypeVersionDowngradeError>()),
        );
        expect(phasesOf(reports), <BootPhase>[BootPhase.checks]);
      },
    );
  });
}
