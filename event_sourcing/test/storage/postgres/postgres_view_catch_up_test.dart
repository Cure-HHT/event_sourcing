// The Postgres-specific mechanics of a catch-up transaction's ordering and
// locking: the SHARE lock on backend_state ordered against appends, the
// per-copy advisory lock that admits one catch-up transaction per copy at a
// time across instances, and the generation guard's live view-fingerprint
// registrations that spare a serving instance's copy from a canary's boot.
// Gated on PG_TEST_URL.
//
// Verifies: EVS-DEV-view-convergence/C
// a view's fingerprint is registered with the generation guard's live
//   components from before the boot transaction until the store closes.
// Verifies: EVS-DEV-view-convergence/D (live registrations)
// the boot marks for deletion only a stored copy whose fingerprint neither
//   the opening build nor a live registration of another instance names.
// Verifies: EVS-DEV-view-convergence/L
// a catch-up transaction's first statement locks backend_state in SHARE
//   mode, before any read of its copy, and an append waits for it.
// Verifies: EVS-DEV-view-convergence/M (Postgres advisory)
// a catch-up transaction takes a transaction-scoped advisory lock on its
//   copy without waiting, and a second instance's catch-up transaction on
//   the same copy ends without writing while the first holds it.

@TestOn('vm')
library;

import 'dart:async';
import 'dart:math' show Random;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const _kType = 'view_catch_up_note';
const _kView = 'view_catch_up_notes';
const _kOtherView = 'view_catch_up_notes_other';

const _kSpec = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kOtherSpec = AggregateProjectionSpec(
  viewName: _kOtherView,
  interest: SubscriptionFilter(entryTypes: <String>{_kType}),
  tombstoneEventTypes: <String>{},
);

const _kSource = Source(
  hopId: 'view-catch-up-hop',
  identifier: 'view-catch-up-install',
  softwareVersion: 'view-catch-up-test',
);

/// True while a session holds a `ShareLock` on `backend_state`, seen
/// through `pg_locks` from [db]'s admin connection.
Future<bool> _backendStateShareLockHeld(PostgresTestDatabase db) async {
  final c = await db.connectAdmin();
  try {
    final r = await c.execute('''
      SELECT count(*) FROM pg_locks
      WHERE relation = 'backend_state'::regclass
        AND mode = 'ShareLock' AND granted
    ''');
    return (r.first[0]! as int) > 0;
  } finally {
    await c.close();
  }
}

Future<EventStore> _openStore(
  PostgresBackend backend, {
  List<ProjectionSpec> projections = const <ProjectionSpec>[_kSpec],
}) {
  final entryTypes = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: _kType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kType,
      ),
    );
  final registry = ProjectionRegistry();
  for (final spec in projections) {
    registry.register(spec);
  }
  final security = PostgresSecurityContextStore(backend: backend);
  return EventStore.open(
    storage: ApplicationSuppliedStorage(backend, security),
    entryTypes: entryTypes,
    source: _kSource,
    projections: registry,
  );
}

Future<void> _append(EventStore store, [String aggregateId = 'agg-1']) =>
    store.runTransaction(
      (txn, collector) => store.appendInTxn(
        txn,
        entryType: _kType,
        aggregateId: aggregateId,
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'title': 't'},
        initiator: const UserInitiator('view-catch-up-user'),
        flowToken: null,
        metadata: null,
        security: null,
        checkpointReason: null,
        changeReason: null,
        dedupeByContent: false,
        collector: collector,
      ),
    );

Future<ViewCopy> _copyOf(PostgresBackend backend, String viewName) async {
  final all = await backend.transaction(backend.readViewCopiesInTxn);
  return all.singleWhere((c) => c.viewName == viewName && !c.markedForDeletion);
}

/// The most recently created copy of [viewName], marked or not: used once
/// a copy is expected to be marked for deletion, when [_copyOf] would find
/// none.
Future<ViewCopy> _anyCopyOf(PostgresBackend backend, String viewName) async {
  final all = await backend.transaction(backend.readViewCopiesInTxn);
  return all.lastWhere((c) => c.viewName == viewName);
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'vcu');
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  Future<PostgresBackend> open() async {
    final backend = await PostgresBackend.open(
      url: db!.runtimeUrl,
      schema: db.schema,
      sslMode: SslMode.disable,
    );
    backends.add(backend);
    return backend;
  }

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset(provision: true);
  });

  tearDown(() async {
    for (final backend in backends.reversed) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-DEV-view-convergence/C
  // Verifies: EVS-DEV-view-convergence/D (live registrations)
  test("a canary's boot does not mark the serving instance's copy, and marks "
      'it once the serving instance closes', () async {
    if (db == null) return;
    final serving = await _openStore(await open());
    final copyBefore = await _copyOf(backends.first, _kView);
    expect(copyBefore.markedForDeletion, isFalse);

    // A canary that registers an added view, not the serving view: its
    // boot must not mark the serving instance's copy while it is live.
    await _openStore(await open(), projections: const [_kOtherSpec]);
    final copyWhileServing = await _copyOf(backends.first, _kView);
    expect(
      copyWhileServing.markedForDeletion,
      isFalse,
      reason: "a canary's boot spares a copy a live registration names",
    );

    await serving.close();

    // A fresh boot, once the serving instance's registration is gone,
    // marks the copy no live registration and no registered build names.
    await _openStore(await open(), projections: const [_kOtherSpec]);
    final copyAfter = await _anyCopyOf(backends.first, _kView);
    expect(copyAfter.markedForDeletion, isTrue);
  });

  // Verifies: EVS-DEV-view-convergence/M (Postgres advisory)
  test("two instances registering one copy: while one holds the copy's "
      "advisory lock, the other's catch-up commits nothing", () async {
    if (db == null) return;
    final seeder = await _openStore(await open(), projections: const []);
    await _append(seeder);
    await seeder.close();

    final blocker = Completer<void>();
    var stepEntered = false;
    late String copyId;
    await runWithDeliveryTestHooks(
      DeliveryTestHooks(
        onCatchUpStep: (id, eventId) async {
          if (id != copyId) return;
          if (stepEntered) return;
          stepEntered = true;
          await blocker.future;
        },
      ),
      () async {
        final a = await _openStore(await open());
        copyId = a.copyIdOf(_kView);
        for (var i = 0; i < 500 && !stepEntered; i++) {
          await pumpEventQueue();
        }
        expect(stepEntered, isTrue);

        // A second instance's catch-up transaction on the same copy, tried
        // directly at the backend, ends granted-nothing while the first
        // holds the copy's advisory lock.
        final backendB = await open();
        var bodyRan = false;
        final result = await backendB.catchUpTransaction(copyId, (txn) async {
          bodyRan = true;
          return null;
        });
        expect(result, isNull);
        expect(bodyRan, isFalse);

        blocker.complete();
      },
    );
  });

  // Verifies: EVS-DEV-view-convergence/L
  test(
    "a catch-up transaction's first lock is SHARE on backend_state, seen "
    'through pg_locks, and an append waits for it and then commits',
    () async {
      if (db == null) return;
      final seeder = await _openStore(await open(), projections: const []);
      await _append(seeder);
      await seeder.close();

      final blocker = Completer<void>();
      var stepEntered = false;
      late EventStore store;
      late String copyId;
      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          onCatchUpStep: (id, eventId) async {
            if (id != copyId) return;
            if (stepEntered) return;
            stepEntered = true;
            await blocker.future;
          },
        ),
        () async {
          store = await _openStore(await open());
          copyId = store.copyIdOf(_kView);
          for (var i = 0; i < 500 && !stepEntered; i++) {
            await pumpEventQueue();
          }
          expect(stepEntered, isTrue);

          expect(await _backendStateShareLockHeld(db), isTrue);

          var appended = false;
          final appendFuture = _append(
            store,
            'agg-append',
          ).then((_) => appended = true);
          await Future<void>.delayed(const Duration(milliseconds: 200));
          expect(
            appended,
            isFalse,
            reason:
                "the append's write to backend_state waits for the "
                'catch-up transaction holding the SHARE lock',
          );

          blocker.complete();
          await appendFuture;
          expect(appended, isTrue);
        },
      );
    },
  );

  // Verifies: EVS-DEV-view-convergence/T
  // Verifies: EVS-DEV-converging-view-reads/A
  // Verifies: EVS-DEV-converging-view-reads/H
  test('a read of a view with no unmarked copy reports it converging, with no '
      'rows, instead of throwing -- the storage reader runs a read-only '
      'transaction on Postgres, so it never itself creates the replacement '
      "copy the read's currency scan reports against", () async {
    if (db == null) return;
    // Keeps the catch-up driver's own replacement-copy attempt failing
    // (its create rolls back with the transaction), so this read's own
    // placeholder path is what actually runs, deterministically, rather
    // than a race the driver might win first.
    await runWithDeliveryTestHooks(
      const DeliveryTestHooks(onCatchUpStep: _throwInjected),
      () async {
        final backend = await open();
        final store = await _openStore(backend);
        await _append(store);
        final copyId = store.copyIdOf(_kView);

        await backend.transaction((txn) async {
          await backend.markViewCopyForDeletionInTxn(txn, copyId);
          await backend.deleteViewCopyRowsInTxn(txn, copyId, limit: 10000);
          await backend.deleteViewCopyRecordInTxn(txn, copyId);
        });

        final read = await store.reader.findViewRows(_kView);
        expect(read.state, ViewConvergenceState.converging);
        expect(read.rows, isEmpty);

        await store.reader.transaction((txn) async {
          await expectLater(
            () => currentViewRows(store.reader)(txn, _kView),
            throwsA(isA<ViewConvergingRefusal>()),
          );
        });

        await store.close();
      },
    );
  });

  // Verifies: EVS-DEV-view-convergence/Z
  // Verifies: EVS-DEV-security-findings/F
  // Verifies: EVS-DEV-security-findings/S
  // Verifies: EVS-PRD-materializer/I
  test('a converging copy that cannot key one event records one fold_failed '
      'finding, passes over it, and becomes current', () async {
    if (db == null) return;
    const keyedView = 'view_catch_up_keyed_notes';
    const keyedSpec = TableProjectionSpec(
      viewName: keyedView,
      interest: SubscriptionFilter(entryTypes: <String>{_kType}),
      insertEventTypes: <String>{'finalized'},
      removeEventTypes: <String>{'removed'},
      rowKey: CompositeKey(<String>['data.k']),
      rowData: WholePayload(),
    );

    Future<StoredEvent?> appendKeyed(
      EventStore store,
      String aggregateId, {
      required bool keyed,
    }) => store.runTransaction(
      (txn, collector) => store.appendInTxn(
        txn,
        entryType: _kType,
        aggregateId: aggregateId,
        aggregateType: 'note',
        eventType: 'finalized',
        data: keyed
            ? <String, Object?>{'k': aggregateId}
            : <String, Object?>{'title': aggregateId},
        initiator: const UserInitiator('view-catch-up-user'),
        flowToken: null,
        metadata: null,
        security: null,
        checkpointReason: null,
        changeReason: null,
        dedupeByContent: false,
        collector: collector,
      ),
    );

    final seeder = await _openStore(await open(), projections: const []);
    StoredEvent? unkeyable;
    StoredEvent? last;
    for (var i = 0; i < 4; i++) {
      last = await appendKeyed(seeder, 'agg-$i', keyed: i != 2);
      if (i == 2) unkeyable = last;
    }
    await seeder.close();

    final backend = await open();
    final store = await _openStore(backend, projections: const [keyedSpec]);
    final copyId = store.copyIdOf(keyedView);

    for (var i = 0; i < 500; i++) {
      final copy = await _copyOf(backend, keyedView);
      if (copy.watermark >= last!.sequenceNumber) break;
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    final copy = await _copyOf(backend, keyedView);
    expect(copy.watermark, greaterThanOrEqualTo(last!.sequenceNumber));

    final findings = await store.reader.findAllEvents(
      entryType: kSecurityFindingEntryType,
    );
    expect(findings, hasLength(1));
    final data = findings.single.data;
    expect(data['kind'], 'fold_failed');
    final evidence = data['evidence']! as Map<String, Object?>;
    expect(evidence['view'], keyedView);
    expect(evidence['event_id'], unkeyable!.eventId);
    expect(evidence['reason'], 'row_key_failed');

    final rows = (await store.reader.findViewRows(keyedView)).rows;
    final keys = rows.map((r) => r['aggregateId']).toSet();
    expect(keys, containsAll(<String>['agg-0', 'agg-1', 'agg-3']));
    expect(keys, isNot(contains('agg-2')));

    final progress = await store.reader.viewProgress();
    expect(
      progress.singleWhere((p) => p.viewName == keyedView).state,
      ViewConvergenceState.current,
    );
    expect(store.catchUpProgressOf(copyId)?.lastFailure, isNull);

    await store.close();
  });

  // Verifies: EVS-DEV-view-convergence/Z (RowWriteRejected classified as
  //   a fold failure in catch-up, not a storage failure)
  // Verifies: EVS-DEV-view-convergence/E
  // Verifies: EVS-DEV-view-convergence/Q (no back-off recorded for it)
  // Verifies: EVS-DEV-view-convergence/Z
  // Verifies: EVS-DEV-security-findings/S
  // Verifies: EVS-PRD-materializer/I
  test('a converging copy whose row write the server rejects (SQLSTATE '
      '54000, index row too large) inside catch-up records one fold_failed '
      'finding of reason row_write_failed, passes over the event and '
      'becomes current', () async {
    if (db == null) return;
    const bigKeyView = 'view_catch_up_big_key_notes';
    const bigKeySpec = TableProjectionSpec(
      viewName: bigKeyView,
      interest: SubscriptionFilter(entryTypes: <String>{_kType}),
      insertEventTypes: <String>{'finalized'},
      removeEventTypes: <String>{'removed'},
      rowKey: CompositeKey(<String>['data.k']),
      rowData: WholePayload(),
    );

    Future<StoredEvent?> appendKeyed(
      EventStore store,
      String aggregateId, {
      required String key,
    }) => store.runTransaction(
      (txn, collector) => store.appendInTxn(
        txn,
        entryType: _kType,
        aggregateId: aggregateId,
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'k': key},
        initiator: const UserInitiator('view-catch-up-user'),
        flowToken: null,
        metadata: null,
        security: null,
        checkpointReason: null,
        changeReason: null,
        dedupeByContent: false,
        collector: collector,
      ),
    );

    final seeder = await _openStore(await open(), projections: const []);
    StoredEvent? huge;
    StoredEvent? last;
    for (var i = 0; i < 4; i++) {
      last = await appendKeyed(
        seeder,
        'agg-$i',
        key: i == 2 ? _incompressibleKey(4000) : 'k-$i',
      );
      if (i == 2) huge = last;
    }
    await seeder.close();

    final backend = await open();
    final store = await _openStore(backend, projections: const [bigKeySpec]);
    final copyId = store.copyIdOf(bigKeyView);

    for (var i = 0; i < 500; i++) {
      final copy = await _copyOf(backend, bigKeyView);
      if (copy.watermark >= last!.sequenceNumber) break;
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
    final copy = await _copyOf(backend, bigKeyView);
    expect(copy.watermark, greaterThanOrEqualTo(last!.sequenceNumber));

    final findings = await store.reader.findAllEvents(
      entryType: kSecurityFindingEntryType,
    );
    expect(findings, hasLength(1));
    final data = findings.single.data;
    expect(data['kind'], 'fold_failed');
    final evidence = data['evidence']! as Map<String, Object?>;
    expect(evidence['view'], bigKeyView);
    expect(evidence['event_id'], huge!.eventId);
    expect(evidence['reason'], 'row_write_failed');

    final rows = (await store.reader.findViewRows(bigKeyView)).rows;
    final keys = rows.map((r) => r['k']).toSet();
    expect(keys, containsAll(<String>['k-0', 'k-1', 'k-3']));

    final progress = await store.reader.viewProgress();
    expect(
      progress.singleWhere((p) => p.viewName == bigKeyView).state,
      ViewConvergenceState.current,
      reason: 'a copy that passes over a failed fold stays current',
    );
    // Verifies: EVS-DEV-view-convergence/Q
    // a fold failure is excluded from the logged back-off: the copy's last
    //   recorded failure is left null throughout.
    expect(
      store.catchUpProgressOf(copyId)?.lastFailure,
      isNull,
      reason: "a fold failure is not logged as the copy's last failure",
    );

    await store.close();
  });
}

void _throwInjected(String copyId, String eventId) =>
    throw const InjectedFailure('paused for this test');

/// A deterministic pseudo-random string of [length] characters, spread over
/// a 62-symbol alphabet: too irregular for the server's storage compression
/// to shrink it back under the btree index's row-size limit, unlike a
/// repeated character (which compresses to almost nothing).
String _incompressibleKey(int length) {
  final random = Random(1234567);
  const alphabet =
      'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  return String.fromCharCodes(
    List<int>.generate(
      length,
      (_) => alphabet.codeUnitAt(random.nextInt(alphabet.length)),
    ),
  );
}
