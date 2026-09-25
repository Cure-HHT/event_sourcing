// Decisions taken by a tab whose copy of the database is stale. Two tab
// models share one IndexedDB database, each through its own sembast_web
// factory; in one page neither receives the other's revision notices, so a
// tab sees the other's commits only when it commits a write. Every decision
// the library relies on across tabs is taken in a transaction that writes,
// so a stale first run is re-run on fresh data before it commits.
@TestOn('browser')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/web_locks.dart'
    show heldBrowserLocks;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;

import 'web_tab_support.dart';

const _kView = 'web_stale_notes';

/// A view over the notes, with a 1.0 -> 2.0 promoter (a major step) when
/// [major] is 2.
({ProjectionRegistry projections, PromoterRegistry promoters}) _views(
  int major,
) {
  final projections = ProjectionRegistry()
    ..register(
      const AggregateProjectionSpec(
        viewName: _kView,
        interest: SubscriptionFilter(entryTypes: <String>{kWebNote}),
        tombstoneEventTypes: <String>{},
      ),
    );
  final promoters = PromoterRegistry();
  if (major == 2) {
    promoters.register(
      const PromoterSpec(
        viewName: _kView,
        entryType: kWebNote,
        fromVersion: EntryTypeVersion(1, 0),
        toVersion: EntryTypeVersion(2, 0),
        transforms: <TransformPrimitive>[
          RenameField(sourceField: 'id', targetField: 'note_id'),
        ],
      ),
    );
  }
  return (projections: projections, promoters: promoters);
}

Future<WebTab> _openAt(String name, int major, {sembast.Database? database}) {
  final views = _views(major);
  return openWebTab(
    name,
    noteVersion: EntryTypeVersion(major, 0),
    database: database,
    projections: views.projections,
    promoters: views.promoters,
  );
}

/// Every event id [name] holds, and its entry types, through a fresh
/// read-only handle.
Future<List<StoredEvent>> _events(String name) =>
    readFresh(name, (b) => b.findAllEvents());

void main() {
  // Verifies: EVS-DEV-event-store-open/E
  // a boot whose first run decided on a tab's stale copy writes its boot
  //   record, so its commit finds the other tab's commits and the body
  //   re-runs on fresh data, where it refuses; nothing is written.
  // Verifies: EVS-DEV-version-compatibility/I
  // the durable generation record another tab raised refuses the older
  //   major although no tab of that major is live, and the refused tab
  //   holds no lock afterwards.
  test(
    'a stale tab refuses to boot below a major another tab recorded',
    () async {
      final name = freshWebName('stale-boot');
      final first = await _openAt(name, 1);
      await first.note('a');
      await first.note('b');
      await first.close();

      // F2 opens its sembast database now and reads once: its copy predates
      // what follows.
      final f2Database = await openTabDatabase(name);
      final before = await SembastBackend(
        database: f2Database,
      ).readSequenceCounter();

      final f1 = await _openAt(name, 2);
      final promoted = await f1.backend.findViewRows(_kView);
      expect(promoted.every((r) => r.containsKey('note_id')), isTrue);
      await f1.close();
      final eventsAfterF1 = (await _events(name)).length;

      var bootRuns = 0;
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
          () => _openAt(name, 1, database: f2Database),
        ),
        throwsA(isA<EntryTypeVersionDowngradeError>()),
      );
      expect(bootRuns, 2, reason: 'a stale run, then a run on fresh data');
      expect((await _events(name)).length, eventsAfterF1);
      expect(await heldBrowserLocks(name), isEmpty);
      expect(before, greaterThan(0));
    },
  );

  // Verifies: EVS-DEV-event-store-open/E
  // two tabs that open a fresh database at once: the boots serialize, and
  //   the database is initialized once.
  // Verifies: EVS-DEV-event-store-open/F
  // both tabs report one database identity, which the one library-version
  //   initialization records, and a later open through either factory
  //   adopts it.
  test(
    'two tabs opening a fresh database at once share one identity',
    () async {
      final name = freshWebName('first-open');
      final tabs = await Future.wait(<Future<WebTab>>[
        openWebTab(name),
        openWebTab(name),
      ]);
      expect(tabs[1].store.databaseId, tabs[0].store.databaseId);
      final id = tabs[0].store.databaseId;
      for (final t in tabs) {
        await t.close();
      }
      final initialized = <StoredEvent>[
        for (final e in await _events(name))
          if (e.entryType == kLibVersionInitializedEntryType) e,
      ];
      expect(initialized, hasLength(1));
      expect(initialized.single.data['database_id'], id);
      for (var i = 0; i < 2; i++) {
        final again = await openWebTab(name);
        expect(again.store.databaseId, id);
        await again.close();
      }
    },
  );

  group('registry operations on a stale tab', () {
    late WebTab f1;
    late WebTab f2;
    late String name;

    setUp(() async {
      name = freshWebName('stale-registry');
      f1 = await openWebTab(name);
      f2 = await openWebTab(name);
      await f1.register(WebReceiver(id: 'x'));
    });

    tearDown(() async {
      await f1.close();
      await f2.close();
    });

    Future<RegistryCheck?> check() =>
        readFresh(name, (b) => b.transaction(b.readRegistryCheckTxn));

    // Verifies: EVS-DEV-destination-drain/U
    // a refusal decided on a stale copy writes the registry check record, so
    //   it re-runs on fresh data before the refusal is returned.
    // Verifies: EVS-DEV-destination-drain/Q
    // a second halt request while one is open is refused, and only the
    //   registry check record is written.
    test('a second halt request is refused on fresh data', () async {
      final requestId = await f1.registry.requestHalt(
        'x',
        initiator: kWebOperator,
        purpose: HaltPurpose.pause,
      );
      final eventsBefore = (await _events(name)).length;
      final runs = <String>[];
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(onRegistryBodyRun: runs.add),
          () => f2.registry.requestHalt(
            'x',
            initiator: kWebOperator,
            purpose: HaltPurpose.pause,
          ),
        ),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('is already open'),
          ),
        ),
      );
      expect(runs, <String>['requestHalt', 'requestHalt']);
      expect((await _events(name)).length, eventsBefore);
      final open = await readFresh(
        name,
        (b) => b.transaction((txn) => b.readHaltRequestTxn(txn, 'x')),
      );
      expect(open?.requestEventId, requestId);
      expect((await check())?.outcome, 'refused_halt_open');
    });

    // Verifies: EVS-DEV-destination-drain/U
    // an outcome that a stale copy would decide as "no change" is decided on
    //   fresh data: there the same date is a forward move, refused.
    test(
      'a start date the stale copy holds is a forward move on fresh data',
      () async {
        // F2's copy holds 2026-01-01 (it registered nothing, but its first
        // write loads F1's commits).
        await expectLater(
          f2.registry.cancelHalt('x', initiator: kWebOperator),
          throwsA(isA<StateError>()),
        );
        await f1.registry.setStartDate(
          'x',
          DateTime.utc(2025, 6, 1),
          initiator: kWebInit,
        );
        final runs = <String>[];
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(onRegistryBodyRun: runs.add),
            () => f2.registry.setStartDate(
              'x',
              DateTime.utc(2026, 1, 1),
              initiator: kWebInit,
            ),
          ),
          throwsA(isA<StateError>()),
        );
        expect(runs, <String>['setStartDate', 'setStartDate']);
        expect((await check())?.outcome, 'refused_forward_move');
      },
    );

    // Verifies: EVS-DEV-destination-drain/U
    // a cancellation a stale copy would refuse ("no request open") re-runs
    //   on fresh data and succeeds.
    // Verifies: EVS-DEV-destination-drain/Q
    // the cancellation closes the request another tab opened.
    test(
      "a cancellation on a stale tab closes the other tab's request",
      () async {
        final requestId = await f1.registry.requestHalt(
          'x',
          initiator: kWebOperator,
          purpose: HaltPurpose.pause,
        );
        final runs = <String>[];
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onRegistryBodyRun: runs.add),
          () => f2.registry.cancelHalt('x', initiator: kWebOperator),
        );
        expect(runs, <String>['cancelHalt', 'cancelHalt']);
        final open = await readFresh(
          name,
          (b) => b.transaction((txn) => b.readHaltRequestTxn(txn, 'x')),
        );
        expect(open, isNull);
        final cancelled = <StoredEvent>[
          for (final e in await _events(name))
            if (e.entryType == kDestinationHaltCancelledEntryType) e,
        ];
        expect(cancelled.single.data['halt_request_event_id'], requestId);
      },
    );

    // Verifies: EVS-DEV-destination-drain/U
    // a refusal on fresh data commits only the registry check record, and
    //   is thrown after the commit.
    // Verifies: EVS-DEV-destination-drain/Q
    // a cancellation with no request open in either tab is refused.
    test(
      'a cancellation with no request open is refused after a commit',
      () async {
        final eventsBefore = (await _events(name)).length;
        final runs = <String>[];
        await expectLater(
          runWithDeliveryTestHooks(
            DeliveryTestHooks(onRegistryBodyRun: runs.add),
            () => f2.registry.cancelHalt('x', initiator: kWebOperator),
          ),
          throwsA(isA<StateError>()),
        );
        expect(runs, <String>['cancelHalt', 'cancelHalt']);
        expect((await _events(name)).length, eventsBefore);
        final recorded = await check();
        expect(recorded?.op, 'cancelHalt');
        expect(recorded?.outcome, 'refused_no_halt_open');
      },
    );

    // Verifies: EVS-DEV-destination-drain/U
    // a recovery a stale copy would refuse (it does not hold the wedged
    //   head) re-runs on fresh data and succeeds.
    test('a recovery on a stale tab right after the drainer wedged', () async {
      final d1 = WebReceiver(id: 'y')
        ..outcome = (_) => const SendPermanent(error: 'refused');
      await f1.register(d1);
      await f1.note('n1');
      final cycle = await SyncCycle.start(
        registry: f1.registry,
        cadence: const Duration(hours: 1),
        policy: kWebPolicy,
      );
      try {
        await cycle();
        final head = await f1.backend.readFifoHead('y');
        expect(head?.finalStatus, FinalStatus.wedged);
        final runs = <String>[];
        final result = await runWithDeliveryTestHooks(
          DeliveryTestHooks(onRegistryBodyRun: runs.add),
          () => f2.registry.tombstoneAndRefill(
            'y',
            head!.entryId,
            initiator: kWebOperator,
          ),
        );
        expect(runs, <String>['tombstoneAndRefill', 'tombstoneAndRefill']);
        expect(result, isNotNull);
        final row = await readFresh(
          name,
          (b) => b.readFifoRow('y', head!.entryId),
        );
        expect(row?.finalStatus, FinalStatus.tombstoned);
      } finally {
        await cycle.close();
      }
    });
  });

  // Verifies: EVS-DEV-destination-drain/G
  // a first-activation replay over a long log while another tab appends
  //   between the replay's transforms: the replay's compare-and-set commits
  //   (the other tab's appends lie past the position it read), with at most
  //   one re-run on the data the other tab committed; the replay's commit
  //   enqueues the whole log; no event lies in two queue items, and the
  //   other tab's events are enqueued by the next fill.
  test('a replay while another tab appends', () async {
    final name = freshWebName('replay');
    final f1 = await openWebTab(name);
    final f2 = await openWebTab(name);
    final d2 = WebReceiver(id: 'x', batchCapacity: 50);
    await f2.register(d2, activate: false);
    final logged = <String>[];
    for (var i = 0; i < 500; i++) {
      logged.add(await f2.note('r$i'));
    }
    final appended = <String>[];
    // Runs of the replay's compare-and-set: the queue-changing
    // transactions that find the queue empty.
    var replayRuns = 0;
    final hooks = DeliveryTestHooks(
      insideTransform: (id) async {
        if (appended.length >= 2) return;
        appended.add(await f1.note('late${appended.length}'));
      },
      beforeQueueWrites: (id) async {
        // Before the replay commits the queue is empty. The fresh read
        // takes the write lock shared, as the replay's run does; a run
        // holding it exclusively would wait on it (the assertion below
        // bounds the runs to two, far below that point).
        final rows = await readFresh(name, (b) => b.listFifoEntries('x'));
        if (rows.isEmpty) replayRuns++;
      },
    );
    final cycle = await runWithDeliveryTestHooks(
      hooks,
      () => SyncCycle.start(
        registry: f2.registry,
        cadence: const Duration(hours: 1),
        policy: kWebPolicy,
      ),
    );
    try {
      await runWithDeliveryTestHooks(hooks, () async {
        await f2.registry.setStartDate(
          'x',
          DateTime.utc(2026, 1, 1),
          initiator: kWebInit,
        );
        await cycle();
      });
      expect(appended, hasLength(2));
      expect(
        replayRuns,
        inInclusiveRange(1, 2),
        reason: 'the replay committed',
      );
      final replayed = await readFresh(name, (b) => b.listFifoEntries('x'));
      expect(
        <String>[for (final r in replayed) ...r.eventIds].take(logged.length),
        logged,
        reason: "the replay's commit enqueued the whole log",
      );
      await cycle();
      final all = await readFresh(name, (b) => b.listFifoEntries('x'));
      final ids = <String>[for (final r in all) ...r.eventIds];
      expect(ids.toSet(), hasLength(ids.length), reason: 'no event twice');
      expect(ids, <String>[...logged, ...appended]);
    } finally {
      await cycle.close();
      await f1.close();
      await f2.close();
    }
  });
}
