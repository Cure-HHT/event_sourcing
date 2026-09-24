// The drain lock in the browser: a Web Lock named for the IndexedDB
// database and its identity, held only by a visible tab. Two tab models
// share one IndexedDB database, each opening it through its own
// independently built sembast_web factory, as two browser tabs do; each
// holds its own sembast `Database`, so the in-isolate registration of a
// delivery cycle accepts a cycle in each and only the Web Lock excludes
// them. The page's visibility is driven through the `pageVisibility` seam,
// because a test cannot set `document.visibilityState` and the two tab
// models share one document.
@TestOn('browser')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/web_locks.dart'
    show
        browserDrainLockName,
        browserLockCounts,
        requestBrowserLock,
        tryBrowserLock;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show databaseFactoryMemory;
import 'package:web/web.dart' as web;

import '../test_support/manual_timers.dart';
import 'web_tab_support.dart';

/// Starts a delivery cycle over [tab] in a zone whose seams are [hooks], so
/// every timer, request and visibility signal of the cycle reads them.
Future<SyncCycle> startIn(
  WebTab tab,
  DeliveryTestHooks hooks, {
  Duration cadence = const Duration(hours: 1),
}) => runWithDeliveryTestHooks(
  hooks,
  () => SyncCycle.start(
    registry: tab.registry,
    cadence: cadence,
    policy: kWebPolicy,
  ),
);

/// The held and pending requests for the drain lock of [tab]'s database.
Future<({int held, int pending})> drainLockCounts(WebTab tab) =>
    browserLockCounts(browserDrainLockName(tab.name, tab.store.databaseId));

/// The causes of the wedge events [name] holds, read through a fresh
/// handle.
Future<List<Object?>> wedgeCauses(String name) => readFresh(name, (b) async {
  final events = await b.findAllEvents();
  return <Object?>[
    for (final e in events)
      if (e.entryType == kDestinationWedgedEntryType) e.data['cause'],
  ];
});

void main() {
  final cycles = <SyncCycle>[];
  final tabs = <WebTab>[];

  Future<WebTab> tab(String name) async {
    final t = await openWebTab(name);
    tabs.add(t);
    return t;
  }

  tearDown(() async {
    for (final c in cycles) {
      await c.close(timeout: const Duration(seconds: 5));
    }
    cycles.clear();
    for (final t in tabs) {
      await t.close();
    }
    tabs.clear();
  });

  group('raw lock wrapper', () {
    // Verifies: EVS-DEV-destination-drain-lock/A
    // the browser's lock manager grants a name to one holder in the page: a
    //   second try is refused while the first holds it, the held name shows
    //   in the lock manager's listing, and a release lets it be taken again;
    //   a request cancelled before any grant is never granted and leaves no
    //   pending request; a cancellation that races a grant releases the
    //   lock before the request reports no grant.
    test(
      'try, release, cancel before a grant, cancel racing a grant',
      () async {
        final name = 'event_sourcing.drainer:${freshWebName('raw')}:id';
        final first = await tryBrowserLock(name);
        expect(first, isNotNull);
        expect(await tryBrowserLock(name), isNull);
        expect(await browserLockCounts(name), (held: 1, pending: 0));
        await first!.release();
        expect(await browserLockCounts(name), (held: 0, pending: 0));

        final again = await tryBrowserLock(name);
        expect(again, isNotNull);
        final waiting = requestBrowserLock(name);
        await until(
          () async => (await browserLockCounts(name)).pending == 1,
          reason: 'the request pending',
        );
        await waiting.cancel();
        expect(await waiting.granted, isNull);
        expect(await browserLockCounts(name), (held: 1, pending: 0));
        await again!.release();
        expect(
          await browserLockCounts(name),
          (held: 0, pending: 0),
          reason: 'the cancelled request was never granted',
        );

        final entered = Completer<void>();
        final proceed = Completer<void>();
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(
            beforeGrantDelivered: () async {
              if (!entered.isCompleted) entered.complete();
              await proceed.future;
            },
          ),
          () async {
            final racing = requestBrowserLock(name);
            await entered.future;
            expect(await browserLockCounts(name), (held: 1, pending: 0));
            final cancelling = racing.cancel();
            proceed.complete();
            await cancelling;
            expect(await racing.granted, isNull);
          },
        );
        expect(
          await browserLockCounts(name),
          (held: 0, pending: 0),
          reason: 'the grant that raced the cancellation was released',
        );
      },
    );
  });

  group('exclusion', () {
    // Verifies: EVS-PRD-destinations/V
    // two tabs of an origin over one database: while one holds the drain
    //   lock the other is refused it, although the two hold distinct
    //   sembast databases, which only the browser's lock manager can
    //   exclude.
    // Verifies: EVS-DEV-destination-drain-lock/A
    // in the browser the drain lock is a Web Lock named for the IndexedDB
    //   database and its identity; every acquisition raises the drain epoch.
    test('one tab holds the drain lock, the other is refused', () async {
      final name = freshWebName('exclusion');
      final f1 = await tab(name);
      final f2 = await tab(name);
      expect(f2.store.databaseId, f1.store.databaseId);
      final lock = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      expect(await drainLockCounts(f1), (held: 1, pending: 0));
      await expectLater(
        f2.backend.tryAcquireDrainLock(databaseId: f2.store.databaseId),
        throwsA(isA<DrainLockUnavailableException>()),
      );
      expect(await freshEpoch(name), lock.epoch);
      await lock.release();
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      final next = await f2.backend.tryAcquireDrainLock(
        databaseId: f2.store.databaseId,
      );
      expect(next.epoch, lock.epoch + 1);
      await next.release();
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // the lock's scope is one database: tabs over two IndexedDB databases
    //   (two identities) both acquire.
    test('tabs over two databases both acquire', () async {
      final f1 = await tab(freshWebName('scope-a'));
      final f2 = await tab(freshWebName('scope-b'));
      final a = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      final b = await f2.backend.tryAcquireDrainLock(
        databaseId: f2.store.databaseId,
      );
      await a.release();
      await b.release();
    });
  });

  group('missing lock manager', () {
    // Verifies: EVS-DEV-destination-drain-lock/A
    // a page without a lock manager refuses the drain lock as a
    //   misconfiguration, naming the secure-context requirement.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a start refused as a misconfiguration throws and leaves nothing
    //   started: the trigger slot is empty, the in-isolate registration is
    //   gone (a later start succeeds), and an append raises nothing and
    //   runs no pass.
    test('a start throws, naming the secure context, and leaves nothing '
        'started', () async {
      final name = freshWebName('no-locks');
      final f1 = await tab(name);
      final d = WebReceiver(id: 'x');
      await f1.register(d);
      await expectLater(
        startIn(f1, const DeliveryTestHooks(webLocksUnavailable: true)),
        throwsA(
          isA<DrainLockConfigurationException>().having(
            (e) => e.message,
            'message',
            contains('secure context'),
          ),
        ),
      );
      expect(f1.store.deliveryTrigger, isNull);
      await f1.note('n1');
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(d.started, isEmpty, reason: 'no pass ran');
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      final cycle = await startIn(f1, const DeliveryTestHooks());
      cycles.add(cycle);
      expect(cycle.state, SyncCycleState.running);
    });
  });

  group('visibility', () {
    // Verifies: EVS-PRD-destinations/V
    // the drain lock follows the visible tab: a tab whose page becomes
    //   hidden finishes the send it has in flight, starts no other,
    //   releases, and the other tab takes over and delivers the next event.
    // Verifies: EVS-DEV-destination-drain-lock/A
    // a hidden tab's outcome of the send in flight commits (the lock is
    //   still held), it then releases the lock and does not request it
    //   while hidden; visible again, it requests it and stands by.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // the hand-over is a release the cycle initiates, not a loss: the
    //   cycle stands by with no request pending while hidden.
    test('a tab whose page becomes hidden hands the drain lock over', () async {
      final name = freshWebName('handover');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final v1 = TestPageVisibility();
      final v2 = TestPageVisibility();
      final d1 = WebReceiver(id: 'x');
      final d2 = WebReceiver(id: 'x');
      await f1.register(d1);
      await f2.register(d2, activate: false);
      // Two queued events, each its own queue item, before the cycle runs.
      final n1 = await f1.note('n1');
      final n2 = await f1.note('n2');
      final logged = <LibraryLogRecord>[];
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          pageVisibility: v1,
          timerFactory: ManualTimers().create,
          onLog: logged.add,
        ),
      );
      cycles.add(c1);
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(
          pageVisibility: v2,
          timerFactory: ManualTimers().create,
        ),
      );
      cycles.add(c2);
      expect(c1.state, SyncCycleState.running);
      expect(c2.state, SyncCycleState.standby);
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );

      final gate = Completer<void>();
      d1.gate = () => gate.future;
      unawaited(c1());
      await until(() => d1.started.length == 1, reason: 'the first send');
      final queued = await readFresh(name, (b) => b.listFifoEntries('x'));
      expect(queued, hasLength(2), reason: 'both events are queued');
      v1.visible = false;
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(c1.state, SyncCycleState.running, reason: 'a send is in flight');
      expect(d1.started, hasLength(1));
      gate.complete();
      await until(
        () => c1.state == SyncCycleState.standby,
        reason: 'the hand-over',
      );
      expect(d1.sentIds, <String>[n1]);
      expect(d1.started, hasLength(1), reason: 'F1 started no further send');
      final rows = await readFresh(name, (b) => b.listFifoEntries('x'));
      final first = rows.firstWhere((r) => r.eventIds.contains(n1));
      expect(first.finalStatus, FinalStatus.sent);
      expect(first.attempts, hasLength(1));

      await until(
        () => c2.state == SyncCycleState.running,
        reason: "F2's takeover",
      );
      await until(() => d2.sentIds.contains(n2), reason: "F2's delivery");
      expect(d1.sentIds, isNot(contains(n2)));
      expect(await drainLockCounts(f1), (
        held: 1,
        pending: 0,
      ), reason: 'F1 is hidden and requests nothing');
      expect(
        logged.where((r) => r.message.contains('lost the drain lock')),
        isEmpty,
        reason: 'the hand-over is a release, not a loss',
      );
      expect(
        logged.where((r) => r.message.contains('hands the drain lock over')),
        hasLength(1),
      );

      v1.visible = true;
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F1's request once visible",
      );
      expect(c1.state, SyncCycleState.standby);
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a hidden tab does not request the drain lock: with the lock free it
    //   stays in standby over three cadence ticks, and takes it once
    //   visible.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a standing-by cycle whose page becomes hidden withdraws its request.
    test('a hidden tab does not take a free drain lock', () async {
      final name = freshWebName('hidden');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final v2 = TestPageVisibility();
      final t2 = ManualTimers();
      final d2 = WebReceiver(id: 'x');
      await f2.register(d2);
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(pageVisibility: v2, timerFactory: t2.create),
        cadence: const Duration(milliseconds: 20),
      );
      cycles.add(c2);
      expect(c2.state, SyncCycleState.standby);
      await until(
        () async => (await drainLockCounts(f2)).pending == 1,
        reason: "F2's request pending",
      );
      v2.visible = false;
      await until(
        () async => (await drainLockCounts(f2)).pending == 0,
        reason: "F2's request withdrawn",
      );
      await holder.release();
      for (var i = 0; i < 3; i++) {
        await t2.fire();
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
      expect(c2.state, SyncCycleState.standby);
      expect(await drainLockCounts(f2), (held: 0, pending: 0));
      final n1 = await f2.note('n1');
      v2.visible = true;
      await until(
        () => c2.state == SyncCycleState.running,
        reason: 'the takeover once visible',
      );
      await until(() => d2.sentIds.contains(n1), reason: 'delivery');
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a start on a hidden page stands by without requesting the drain
    //   lock, and takes it once the page is visible.
    test('a start on a hidden page stands by until visible', () async {
      final name = freshWebName('start-hidden');
      final f1 = await tab(name);
      final v1 = TestPageVisibility(visible: false);
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          pageVisibility: v1,
          timerFactory: ManualTimers().create,
        ),
      );
      cycles.add(c1);
      expect(c1.state, SyncCycleState.standby);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      expect(await freshEpoch(name), isNull, reason: 'no acquisition');
      v1.visible = true;
      await until(
        () => c1.state == SyncCycleState.running,
        reason: 'the acquisition once visible',
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // closing a cycle during a hand-over waits for it: close returns once
    //   the send in flight returned and its outcome committed, and the lock
    //   is free afterwards.
    test('close during a hand-over awaits it', () async {
      final name = freshWebName('close-handover');
      final f1 = await tab(name);
      final v1 = TestPageVisibility();
      final logged = <LibraryLogRecord>[];
      final d1 = WebReceiver(id: 'x');
      await f1.register(d1);
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          pageVisibility: v1,
          timerFactory: ManualTimers().create,
          onLog: logged.add,
        ),
      );
      final gate = Completer<void>();
      // Released on failure too, so the send in flight cannot outlive the
      // test and keep the tab's lock.
      addTearDown(() {
        if (!gate.isCompleted) gate.complete();
      });
      d1.gate = () => gate.future;
      final n1 = await f1.note('n1');
      await until(() => d1.started.length == 1, reason: 'the send');
      v1.visible = false;
      // The hand-over has begun before close is called, so close arrives
      // during it rather than stopping the cycle ahead of it.
      await until(
        () => logged.any((r) => r.message.contains('hands the drain lock')),
        reason: 'the hand-over',
      );
      var closed = false;
      final closing = c1.close().then((_) => closed = true);
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(closed, isFalse, reason: 'close waits for the send in flight');
      gate.complete();
      await closing;
      expect(c1.state, SyncCycleState.stopped);
      expect(d1.sentIds, <String>[n1]);
      final head = await readFresh(name, (b) => b.listFifoEntries('x'));
      expect(head.single.finalStatus, FinalStatus.sent);
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      expect(
        logged.where((r) => r.message.contains('lost the drain lock')),
        isEmpty,
        reason: 'the hand-over is not reported as a loss',
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a loss detected during a hand-over is not reported as a loss and
    //   releases nothing twice: the hand-over releases once, the outcome of
    //   the send in flight commits nothing (the lock is lost), and the
    //   cycle, visible again, requests the lock once and sends the head
    //   again.
    test('a heartbeat failure during a hand-over', () async {
      final name = freshWebName('handover-loss');
      final f1 = await tab(name);
      final v1 = TestPageVisibility();
      final timers = ManualTimers();
      final logged = <LibraryLogRecord>[];
      var failBeat = false;
      final d1 = WebReceiver(id: 'x');
      await f1.register(d1);
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          pageVisibility: v1,
          timerFactory: timers.create,
          onLog: logged.add,
          failNextHeartbeat: () => failBeat,
        ),
      );
      cycles.add(c1);
      final gate = Completer<void>();
      d1.gate = () => gate.future;
      final n1 = await f1.note('n1');
      await until(() => d1.started.length == 1, reason: 'the send');
      v1.visible = false;
      await until(
        () => logged.any((r) => r.message.contains('hands the drain lock')),
        reason: 'the hand-over',
      );
      failBeat = true;
      await timers.fire();
      failBeat = false;
      gate.complete();
      await until(
        () => c1.state == SyncCycleState.standby,
        reason: 'the hand-over',
      );
      expect(
        logged.where((r) => r.message.contains('lost the drain lock')),
        isEmpty,
      );
      final rows = await readFresh(name, (b) => b.listFifoEntries('x'));
      expect(
        rows.single.finalStatus,
        isNot(FinalStatus.sent),
        reason: 'the outcome under a lost lock commits nothing',
      );
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      v1.visible = true;
      await until(
        () => c1.state == SyncCycleState.running,
        reason: 'the request once visible',
      );
      expect(
        logged.where((r) => r.message.contains('took the drain lock')),
        hasLength(1),
      );
      await until(() async {
        await c1();
        return d1.sentIds.where((id) => id == n1).length == 2;
      }, reason: 'the head sent again');
    });

    // Verifies: EVS-PRD-destinations/V
    // a hidden tab whose send does not return keeps the drain lock for at
    //   most one cadence: it then releases the lock and the visible tab
    //   takes over and delivers; the late outcome commits nothing.
    // Verifies: EVS-DEV-destination-drain-lock/A
    // a hidden tab waits at most one cadence for its sends in flight
    //   before it releases the lock.
    test('a send that does not return bounds the hand-over', () async {
      final name = freshWebName('handover-hung');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final v1 = TestPageVisibility();
      final logged = <LibraryLogRecord>[];
      final d1 = WebReceiver(id: 'x');
      final d2 = WebReceiver(id: 'x');
      await f1.register(d1);
      await f2.register(d2, activate: false);
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          pageVisibility: v1,
          timerFactory: ManualTimers().create,
          onLog: logged.add,
        ),
        cadence: const Duration(milliseconds: 300),
      );
      cycles.add(c1);
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(timerFactory: ManualTimers().create),
      );
      cycles.add(c2);
      final hung = Completer<void>();
      d1.gate = () => hung.future;
      final n1 = await f1.note('n1');
      await until(() => d1.started.length == 1, reason: 'the send');
      v1.visible = false;
      await until(
        () => c2.state == SyncCycleState.running,
        reason: "F2's takeover once F1's wait ran out",
      );
      expect(
        logged.where((r) => r.message.contains('did not return within')),
        hasLength(1),
      );
      await until(() async {
        await c2();
        return d2.sentIds.contains(n1);
      }, reason: "F2's delivery");
      hung.complete();
      await until(() => d1.received.length == 1, reason: 'the late return');
      await Future<void>.delayed(const Duration(milliseconds: 100));
      final rows = await readFresh(name, (b) => b.listFifoEntries('x'));
      expect(rows.single.finalStatus, FinalStatus.sent);
      expect(
        rows.single.attempts,
        hasLength(1),
        reason: "F1's late outcome committed nothing",
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a grant that arrives as the page becomes hidden is released: the
    //   cycle stays in standby with no lock held and no request pending,
    //   and takes the lock once the page is visible.
    test('a grant racing the page becoming hidden is released', () async {
      final name = freshWebName('grant-hide');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final v2 = TestPageVisibility();
      final logged = <LibraryLogRecord>[];
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      var hid = false;
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(
          pageVisibility: v2,
          timerFactory: ManualTimers().create,
          onLog: logged.add,
          beforeGrantDelivered: () async {
            if (hid) return;
            hid = true;
            v2.visible = false;
          },
        ),
      );
      cycles.add(c2);
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );
      await holder.release();
      await until(() => hid, reason: 'the grant');
      await until(
        () async => await drainLockCounts(f1) == (held: 0, pending: 0),
        reason: 'the raced grant released',
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(c2.state, SyncCycleState.standby);
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      expect(await freshEpoch(name), holder.epoch, reason: 'no acquisition');
      expect(
        logged.where((r) => r.message.contains('took the drain lock')),
        isEmpty,
      );
      v2.visible = true;
      await until(
        () => c2.state == SyncCycleState.running,
        reason: 'the acquisition once visible',
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // a page hidden while the acquisition raises the epoch releases the lock
    //   it just took and stands by: the cycle never runs while hidden, and
    //   takes the lock once the page is visible.
    test('a page hidden during the epoch raise releases the lock', () async {
      final name = freshWebName('raise-hide');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final v2 = TestPageVisibility();
      final logged = <LibraryLogRecord>[];
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      var hid = false;
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(
          pageVisibility: v2,
          timerFactory: ManualTimers().create,
          onLog: logged.add,
          failAfterExclusionObtained: () {
            if (!hid) {
              hid = true;
              v2.visible = false;
            }
            return false;
          },
        ),
      );
      cycles.add(c2);
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );
      await holder.release();
      await until(() => hid, reason: 'the epoch raise');
      await until(
        () async => await drainLockCounts(f1) == (held: 0, pending: 0),
        reason: 'the lock released',
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(c2.state, SyncCycleState.standby);
      expect(await freshEpoch(name), holder.epoch + 1);
      expect(
        logged.where((r) => r.message.contains('took the drain lock')),
        isEmpty,
        reason: 'the cycle never ran while hidden',
      );
      v2.visible = true;
      await until(
        () => c2.state == SyncCycleState.running,
        reason: 'the acquisition once visible',
      );
    });
  });

  group('document visibility', () {
    void dispatch(String type) {
      final event = web.Event(type);
      if (type == 'freeze' || type == 'resume') {
        web.document.dispatchEvent(event);
      } else {
        web.window.dispatchEvent(event);
      }
    }

    tearDown(() {
      dispatch('pageshow');
      dispatch('resume');
    });

    for (final (hide, show) in const <(String, String)>[
      ('pagehide', 'pageshow'),
      ('freeze', 'resume'),
    ]) {
      // Verifies: EVS-DEV-destination-drain-lock/A
      // with no seam, the page's own lifecycle drives the drain lock: a
      //   hiding event hands it over (the send in flight returns and its
      //   outcome commits, the lock is released and nothing is requested),
      //   and the showing event requests it again.
      test(
        '$hide hands the drain lock over and $show requests it again',
        () async {
          final name = freshWebName('document-$hide');
          final f1 = await tab(name);
          final d1 = WebReceiver(id: 'x');
          await f1.register(d1);
          final c1 = await startIn(
            f1,
            DeliveryTestHooks(timerFactory: ManualTimers().create),
          );
          cycles.add(c1);
          expect(c1.state, SyncCycleState.running);
          final gate = Completer<void>();
          d1.gate = () => gate.future;
          final n1 = await f1.note('n1');
          await until(() => d1.started.length == 1, reason: 'the send');
          dispatch(hide);
          await Future<void>.delayed(const Duration(milliseconds: 50));
          expect(c1.state, SyncCycleState.running, reason: 'a send in flight');
          gate.complete();
          await until(
            () => c1.state == SyncCycleState.standby,
            reason: 'the hand-over',
          );
          expect(d1.sentIds, <String>[n1]);
          final rows = await readFresh(name, (b) => b.listFifoEntries('x'));
          expect(rows.single.finalStatus, FinalStatus.sent);
          await Future<void>.delayed(const Duration(milliseconds: 50));
          expect(await drainLockCounts(f1), (held: 0, pending: 0));
          dispatch(show);
          await until(
            () => c1.state == SyncCycleState.running,
            reason: 'the request once shown',
          );
        },
      );
    }

    // Verifies: EVS-DEV-destination-drain-lock/F
    // the page-visibility seam narrows the page's own visibility and never
    //   widens it: with the seam reporting visible on a page that is
    //   hidden, the drain lock is refused.
    test('the visibility seam cannot make a hidden page visible', () async {
      final name = freshWebName('seam-narrows');
      final f1 = await tab(name);
      // The page's own visibility listens from the first drain lock on.
      final first = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      await first.release();
      dispatch('pagehide');
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(pageVisibility: TestPageVisibility()),
          () => f1.backend.tryAcquireDrainLock(databaseId: f1.store.databaseId),
        ),
        throwsA(isA<DrainLockUnavailableException>()),
      );
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
    });
  });

  group('a backend that can no longer take the lock', () {
    // Verifies: EVS-PRD-destinations/V
    // a draining tab whose database handle cannot commit gives the drain
    //   lock up, and the other visible tab takes over and delivers.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // a cycle whose handle cannot commit stops for good with one error
    //   line, releases the lock, and reports the cause; an append on that
    //   tab fails at once.
    test('a drainer whose handle another opener compacted past stops, and '
        'another tab takes over', () async {
      final name = freshWebName('stuck-drainer');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d1 = WebReceiver(id: 'x');
      final d2 = WebReceiver(id: 'x');
      await f1.register(d1);
      await f2.register(d2, activate: false);
      final logged = <LibraryLogRecord>[];
      final hooks1 = DeliveryTestHooks(
        timerFactory: ManualTimers().create,
        onLog: logged.add,
      );
      final c1 = await startIn(f1, hooks1);
      cycles.add(c1);
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(timerFactory: ManualTimers().create),
      );
      cycles.add(c2);
      expect(c1.state, SyncCycleState.running);
      expect(c2.state, SyncCycleState.standby);
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );
      // F2 commits a deletion F1 has not seen, and another opener that may
      // write compacts the database past F1's handle.
      await f2.register(WebReceiver(id: 'y'), activate: false);
      await f2.registry.deleteDestination('y', initiator: kWebInit);
      await (await openTabDatabase(name)).close();

      await runWithDeliveryTestHooks(hooks1, c1.call);
      await c1.stopped.timeout(const Duration(seconds: 10));
      expect(c1.state, SyncCycleState.stopped);
      expect(c1.stopCause, isA<TransactionRerunLimitException>());
      expect(
        logged.where(
          (r) => r.message.startsWith('the delivery cycle stops and releases'),
        ),
        hasLength(1),
      );
      await until(
        () => c2.state == SyncCycleState.running,
        reason: "F2's takeover",
      );
      final n1 = await f2.note('n1');
      await until(() => d2.sentIds.contains(n1), reason: "F2's delivery");
      expect(await drainLockCounts(f2), (held: 1, pending: 0));
      await expectLater(
        f1.note('n2'),
        throwsA(isA<TransactionRerunLimitException>()),
      );
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a standing-by cycle whose backend is closed stops at once, reporting
    //   the closed backend, and leaves no request pending; a cycle over a
    //   reopened handle starts.
    test('closing the backend of a standing-by cycle stops it', () async {
      final name = freshWebName('close-backend');
      final f1 = await tab(name);
      final f2 = await openWebTab(name);
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      final c2 = await startIn(
        f2,
        DeliveryTestHooks(timerFactory: ManualTimers().create),
      );
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );
      await f2.close();
      await c2.stopped.timeout(const Duration(seconds: 5));
      expect(c2.state, SyncCycleState.stopped);
      expect(c2.stopCause, isA<DrainLockBackendClosedException>());
      expect(await drainLockCounts(f1), (held: 1, pending: 0));
      await holder.release();
      final f3 = await tab(name);
      final c3 = await startIn(
        f3,
        DeliveryTestHooks(timerFactory: ManualTimers().create),
      );
      cycles.add(c3);
      expect(c3.state, SyncCycleState.running);
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // a close that runs while an acquisition raises the epoch leaves no
    //   lock held: the acquisition fails and releases the Web Lock.
    test('a close during an acquisition leaves the Web Lock free', () async {
      final name = freshWebName('close-acquire');
      final f1 = await openWebTab(name);
      final id = f1.store.databaseId;
      Future<void>? closing;
      await expectLater(
        runWithDeliveryTestHooks(
          DeliveryTestHooks(
            failAfterExclusionObtained: () {
              closing ??= f1.backend.close();
              return false;
            },
          ),
          () => f1.backend.tryAcquireDrainLock(databaseId: id),
        ),
        throwsA(anything),
      );
      await closing;
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      await f1.close();
    });

    // Verifies: EVS-DEV-destination-drain-lock/A
    // on the web the drain lock of a Sembast database in memory is a Web
    //   Lock too, named for the database's path.
    test('an in-memory database takes the Web Lock', () async {
      final path = freshWebName('memory');
      final backend = SembastBackend(
        database: await databaseFactoryMemory.openDatabase(path),
      );
      final name = browserDrainLockName(path, 'memory-id');
      final lock = await backend.tryAcquireDrainLock(databaseId: 'memory-id');
      expect(await browserLockCounts(name), (held: 1, pending: 0));
      await lock.release();
      expect(await browserLockCounts(name), (held: 0, pending: 0));
      await backend.close();
    });
  });

  group('acquisition', () {
    // Verifies: EVS-DEV-destination-drain-lock/C
    // an acquisition that fails after it obtained the Web Lock gives the
    //   Web Lock up before the failure surfaces: the lock is free while the
    //   cycle stands by, another tab takes and releases it, and the next
    //   retry acquires.
    test('an acquisition that fails part-way gives the Web Lock up', () async {
      final name = freshWebName('give-up');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d1 = WebReceiver(id: 'x');
      await f1.register(d1);
      final n1 = await f1.note('n1');
      final timers = ManualTimers();
      final log = <LibraryLogRecord>[];
      var failing = true;
      final c1 = await startIn(
        f1,
        DeliveryTestHooks(
          failAfterExclusionObtained: () => failing,
          timerFactory: timers.create,
          onLog: log.add,
        ),
        cadence: const Duration(milliseconds: 20),
      );
      cycles.add(c1);
      expect(c1.state, SyncCycleState.standby);
      await until(
        () => log.any(
          (r) => r.message.startsWith('acquiring the drain lock failed'),
        ),
        reason: "the request's first attempt",
      );
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      expect(await freshEpoch(name), isNull, reason: 'no epoch was raised');
      failing = false;
      final other = await f2.backend.tryAcquireDrainLock(
        databaseId: f2.store.databaseId,
      );
      await other.release();
      await until(() async {
        await timers.fire();
        return c1.state == SyncCycleState.running;
      }, reason: 'the retry');
      await c1();
      expect(d1.sentIds, contains(n1));
    });

    // Verifies: EVS-PRD-destinations/V
    // a cycle started while another tab holds the drain lock stands by, and
    //   takes over and delivers once it is released, without a restart.
    // Verifies: EVS-DEV-destination-drain-lock/C
    // standby, then running on the grant; a trigger in standby does no
    //   work.
    test('standby, then takeover when the other tab releases', () async {
      final name = freshWebName('standby');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d2 = WebReceiver(id: 'x');
      await f2.register(d2);
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      final c2 = await startIn(f2, const DeliveryTestHooks());
      cycles.add(c2);
      expect(c2.state, SyncCycleState.standby);
      final n1 = await f2.note('n1');
      await c2();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(d2.started, isEmpty, reason: 'no work while standing by');
      await holder.release();
      await until(
        () => c2.state == SyncCycleState.running,
        reason: 'the takeover',
      );
      await until(() => d2.sentIds.contains(n1), reason: 'delivery');
      expect(await freshEpoch(name), greaterThan(holder.epoch));
    });

    // Verifies: EVS-DEV-destination-drain-lock/C
    // closing a cycle that stands by withdraws its request: after the
    //   holder releases, the closed cycle never acquires, no request is
    //   pending, and the other tab takes the lock again.
    test('close during standby acquires nothing', () async {
      final name = freshWebName('close-standby');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final holder = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      final c2 = await startIn(f2, const DeliveryTestHooks());
      expect(c2.state, SyncCycleState.standby);
      await until(
        () async => (await drainLockCounts(f1)).pending == 1,
        reason: "F2's request pending",
      );
      await c2.close();
      expect(await drainLockCounts(f1), (held: 1, pending: 0));
      await holder.release();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(await drainLockCounts(f1), (held: 0, pending: 0));
      expect(await freshEpoch(name), holder.epoch, reason: 'no acquisition');
      final later = await f1.backend.tryAcquireDrainLock(
        databaseId: f1.store.databaseId,
      );
      await later.release();
    });
  });

  group('across tabs', () {
    // Verifies: EVS-PRD-destinations/U
    // a halt another tab commits while this tab's drainer is about to send
    //   is honoured: no delivery attempt completes after the request.
    // Verifies: EVS-DEV-destination-drain/N
    // the pre-send fence writes, so its commit finds the other tab's
    //   commit, re-runs on fresh data, sees the request and starts no send;
    //   the head is wedged with cause operator halt.
    test('a halt committed in another tab stops the send', () async {
      final name = freshWebName('fence');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d2 = WebReceiver(id: 'x');
      await f2.register(d2);
      final fenceRuns = <String>[];
      var fired = false;
      final hooks = DeliveryTestHooks(
        beforeSendFence: (id) async {
          if (fired) return;
          fired = true;
          await f1.registry.requestHalt(
            'x',
            initiator: kWebOperator,
            purpose: HaltPurpose.pause,
          );
        },
        onFenceBodyRun: fenceRuns.add,
      );
      final c2 = await startIn(f2, hooks);
      cycles.add(c2);
      // The append wakes the cycle in the append's zone: append in the
      // seams' zone, so the pass it starts reads them.
      await runWithDeliveryTestHooks(hooks, () async {
        await f2.note('n1');
        await c2();
      });
      expect(fired, isTrue);
      expect(fenceRuns, <String>['x', 'x']);
      expect(d2.started, isEmpty);
      expect(await wedgeCauses(name), <Object?>['operator_halt']);
    });

    // Verifies: EVS-DEV-destination-drain-lock/E
    // events another tab appends are enqueued and delivered by the draining
    //   tab's next pass: the pass-start write loads the other tab's commits.
    test("the next pass delivers another tab's appends", () async {
      final name = freshWebName('append');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d2 = WebReceiver(id: 'x');
      await f2.register(d2);
      final c2 = await startIn(f2, const DeliveryTestHooks());
      cycles.add(c2);
      final n1 = await f1.note('n1');
      await c2();
      expect(d2.sentIds, <String>[n1]);
    });

    // Verifies: EVS-DEV-destination-drain/G
    // an end date another tab commits while this tab's fill runs the
    //   transform: the fill's compare-and-set re-runs on fresh data and
    //   enqueues no event past the new end date.
    test("the fill's compare-and-set sees another tab's end date", () async {
      final name = freshWebName('cas');
      final f1 = await tab(name);
      final f2 = await tab(name);
      final d2 = WebReceiver(id: 'x');
      await f2.register(d2);
      var fired = false;
      final hooks = DeliveryTestHooks(
        insideTransform: (id) async {
          if (fired) return;
          fired = true;
          await f1.registry.setEndDate(
            'x',
            DateTime.utc(2026, 2, 1),
            initiator: kWebInit,
          );
        },
      );
      final c2 = await startIn(f2, hooks);
      cycles.add(c2);
      await runWithDeliveryTestHooks(hooks, () async {
        await f2.note('n1');
        await c2();
      });
      expect(fired, isTrue);
      expect(await readFresh(name, (b) => b.listFifoEntries('x')), isEmpty);
      expect(d2.started, isEmpty);
    });
  });
}
