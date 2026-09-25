// The bound on sembast_web's re-runs of a transaction body. sembast_web
// re-runs a body whose commit another tab preceded. A transaction that
// keeps losing runs that way runs again holding the database's write lock
// exclusively, where no other tab's write comes between, so contention
// delays it but never fails it. A handle that fails even then is one that
// another opener compacted past: sembast_web compacts the database when a
// handle that may write opens it and a deleted record is on file, and a
// handle that has not seen the commits up to the compaction then fails
// every commit (its full reload keeps its stale revision). The library
// reports that handle as unable to commit, and every later transaction on
// it fails at once; a reopened handle commits.
@TestOn('browser')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/logging.dart';
import 'package:event_sourcing/src/storage/web_locks.dart'
    show heldBrowserWriteLockModes;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import 'web_tab_support.dart';

void main() {
  // Verifies: EVS-PRD-event-log/E
  // a write on a handle that cannot commit even with every other tab's
  //   writes held back fails within a bound, naming the reopen that
  //   recovers; the failed write committed nothing; every later write on
  //   the handle fails at once without running its body; a reopened handle
  //   commits.
  test('a tab behind a compaction fails its write within a bound, and a '
      'reopened handle commits', () async {
    final name = freshWebName('compaction');
    // Every tab still open when the test ends, failed or not, is closed.
    final open = <WebTab>[];
    addTearDown(() async {
      for (final tab in open) {
        await tab.close();
      }
    });
    Future<WebTab> tab() async {
      final t = await openWebTab(name);
      open.add(t);
      return t;
    }

    Future<void> closeTab(WebTab t) async {
      open.remove(t);
      await t.close();
    }

    final f1 = await tab();
    final f2 = await tab();
    // The destination the failing write targets: its schedule is on file,
    // so a halt request on it is accepted and would write.
    await f1.register(WebReceiver(id: 'y'), activate: false);
    // A deleted record on file: a destination registered, then deleted.
    await f1.register(WebReceiver(id: 'x'), activate: false);
    await f1.registry.deleteDestination('x', initiator: kWebInit);
    // Another opener that may write compacts the database.
    await (await openTabDatabase(name)).close();

    var runs = 0;
    Future<Object?> attempt(String id) => runWithDeliveryTestHooks(
      DeliveryTestHooks(onRegistryBodyRun: (_) => runs++),
      () => f2.registry
          .requestHalt(id, initiator: kWebOperator, purpose: HaltPurpose.pause)
          .then<Object?>((v) => v, onError: (Object e) => e)
          .timeout(
            const Duration(seconds: 10),
            onTimeout: () => 'no outcome within 10 s',
          ),
    );
    Future<({Object? request, int events})> haltState() => readFresh(
      name,
      (b) async => (
        request: await b.transaction((txn) => b.readHaltRequestTxn(txn, 'y')),
        events: (await b.findAllEvents(
          entryType: kDestinationHaltRequestedEntryType,
        )).length,
      ),
    );

    final outcome = await attempt('y');
    expect(
      outcome,
      isA<TransactionRerunLimitException>().having(
        (e) => e.toString(),
        'message',
        contains('open it again'),
      ),
    );
    // Four runs holding the write lock shared, four holding it exclusively.
    expect(runs, 4 + TransactionRerunLimitException.maxRuns);
    expect(await heldBrowserWriteLockModes(name), isEmpty);
    expect(await haltState(), (
      request: null,
      events: 0,
    ), reason: 'the failed write committed nothing');

    runs = 0;
    expect(await attempt('y'), isA<TransactionRerunLimitException>());
    expect(runs, 0, reason: 'a later write on the handle fails at once');
    expect(await haltState(), (request: null, events: 0));

    await closeTab(f2);
    final reopened = await tab();
    final id = await reopened.note('after-reopen');
    final ids = await readFresh(
      name,
      (b) async => <String>[for (final e in await b.findAllEvents()) e.eventId],
    );
    expect(ids, contains(id));
    // The same halt request commits on the reopened handle, so the failed
    // attempts had a write to make.
    await reopened.registry.requestHalt(
      'y',
      initiator: kWebOperator,
      purpose: HaltPurpose.pause,
    );
    final after = await haltState();
    expect(after.request, isNotNull);
    expect(after.events, 1);
  });

  // Verifies: EVS-PRD-event-log/E
  // a transaction whose commit another tab precedes on every run it makes
  //   holding the write lock shared runs again holding it exclusively, and
  //   commits there: contention between tabs never fails a transaction.
  //   Here the drainer's fill loses its compare-and-set four times to
  //   another tab's appends and commits on its first exclusive run.
  test("a fill that keeps losing to another tab's appends commits holding "
      'the write lock exclusively', () async {
    final name = freshWebName('contention');
    final f1 = await openWebTab(name);
    final f2 = await openWebTab(name);
    final d2 = WebReceiver(id: 'x');
    await f2.register(d2);
    final n1 = await f2.note('n1');
    var shared = 0;
    var exclusive = 0;
    var done = false;
    final logged = <LibraryLogRecord>[];
    final interleaved = <String>[];
    final hooks = DeliveryTestHooks(
      onLog: logged.add,
      beforeQueueWrites: (id) async {
        if (done) return;
        final modes = await heldBrowserWriteLockModes(name);
        if (modes.contains('exclusive')) {
          exclusive++;
          done = true;
          return;
        }
        shared++;
        // Another tab commits while this run's body is open.
        interleaved.add(await f1.note('c$shared'));
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
      await runWithDeliveryTestHooks(hooks, cycle.call);
      expect(shared, 4, reason: 'four runs lost holding the lock shared');
      expect(exclusive, 1, reason: 'one run holding it exclusively');
      expect(
        logged.where((r) => r.message.startsWith('fillBatch failed')),
        isEmpty,
      );
      expect(d2.sentIds.first, n1, reason: 'the fill committed');
      await until(() async {
        await cycle();
        return d2.sentIds.length == 1 + interleaved.length;
      }, reason: "the other tab's appends delivered");
      expect(d2.sentIds, <String>[n1, ...interleaved]);
    } finally {
      await cycle.close();
      await f1.close();
      await f2.close();
    }
  });
}
