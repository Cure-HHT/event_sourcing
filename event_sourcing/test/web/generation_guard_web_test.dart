// The incompatible-generation guard on the web: tabs of one origin that
// share an IndexedDB database register their data generations with the
// browser's lock manager. Two tab models share one IndexedDB database, each
// opening it through its own independently built sembast_web factory, so
// each holds its own sembast Database, as two browser tabs do.

@TestOn('browser')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;
import 'package:event_sourcing/src/storage/web_locks.dart'
    show browserLockCounts, heldBrowserLocks;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart' show newDatabaseFactoryMemory;
import 'package:sembast/utils/database_utils.dart' show getNonEmptyStoreNames;
// ignore: implementation_imports, builds a second independent factory to model a second tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

const _kX = 'web_gen_x';
const _kView = 'web_gen_notes';

/// Opens [dbName] through a factory of its own, as one tab would.
Future<sembast.Database> _openDatabase(String dbName) =>
    DatabaseFactoryWeb(JdbFactoryWeb(idbFactoryWeb)).openDatabase(dbName);

/// Every store's record count in the IndexedDB database [dbName], read
/// through a tab of its own, and the sequence counter.
Future<Map<String, int>> _contents(String dbName) async {
  final database = await _openDatabase(dbName);
  try {
    final counts = <String, int>{};
    for (final store in getNonEmptyStoreNames(database)) {
      counts[store] = await sembast.StoreRef<Object?, Object?>(
        store,
      ).count(database);
    }
    counts['(sequence counter)'] = await SembastBackend(
      database: database,
    ).readSequenceCounter();
    return counts;
  } finally {
    await database.close();
  }
}

/// Opens an event store over [dbName] as one tab would, registering [_kX]
/// at [x]; over [database] when given.
Future<EventStore> _openTab(
  String dbName, {
  EntryTypeVersion x = const EntryTypeVersion(1, 0),
  sembast.Database? database,
  bool withView = false,
}) async {
  final backend = SembastBackend(
    database: database ?? await _openDatabase(dbName),
  );
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    EntryTypeDefinition(id: _kX, registeredVersion: x, name: _kX),
  );
  final projections = ProjectionRegistry();
  final promoters = PromoterRegistry();
  if (withView) {
    projections.register(
      const AggregateProjectionSpec(
        viewName: _kView,
        interest: SubscriptionFilter(entryTypes: <String>{_kX}),
        tombstoneEventTypes: <String>{},
      ),
    );
    if (x == const EntryTypeVersion(1, 1)) {
      promoters.register(
        const PromoterSpec(
          viewName: _kView,
          entryType: _kX,
          fromVersion: EntryTypeVersion(1, 0),
          toVersion: EntryTypeVersion(1, 1),
          transforms: <TransformPrimitive>[
            DefaultField(fieldName: 'b', defaultValue: 0),
          ],
        ),
      );
    }
  }
  try {
    return await EventStore.open(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      entryTypes: registry,
      source: const Source(
        hopId: 'web-hop',
        identifier: 'web-install',
        softwareVersion: 'web-test',
      ),
      projections: projections,
      promoters: promoters,
    );
  } catch (_) {
    await backend.close();
    rethrow;
  }
}

Future<void> _appendNote(EventStore store, String aggregateId) =>
    store.runTransaction(
      (Transaction txn, PublishCollector collector) => store.appendInTxn(
        txn,
        entryType: _kX,
        aggregateId: aggregateId,
        aggregateType: 'note',
        eventType: 'finalized',
        data: <String, Object?>{'title': aggregateId},
        initiator: const UserInitiator('web-user'),
        flowToken: null,
        metadata: null,
        security: null,
        checkpointReason: null,
        changeReason: null,
        dedupeByContent: false,
        collector: collector,
      ),
    );

String _freshName() => 'guard-${DateTime.now().microsecondsSinceEpoch}.db';

void main() {
  // Verifies: EVS-DEV-version-compatibility/F
  // Verifies: EVS-DEV-version-compatibility/H
  test('a compatible tab opens beside another, an incompatible one is '
      'refused with the database unchanged, and it opens once the other '
      'tabs closed', () async {
    final name = _freshName();
    final first = await _openTab(name);
    final second = await _openTab(name, x: const EntryTypeVersion(1, 1));
    final held = await heldBrowserLocks(name);
    expect(
      held.where((l) => l.name.endsWith('entry_type:$_kX:1')),
      hasLength(2),
    );
    expect(held.every((l) => l.mode == 'shared'), isTrue);
    final before = await _contents(name);
    await expectLater(
      _openTab(name, x: const EntryTypeVersion(2, 0)),
      throwsA(
        isA<IncompatibleGenerationException>().having(
          (e) => e.conflictingComponents,
          'components',
          ['entry_type:$_kX:1'],
        ),
      ),
    );
    expect(await _contents(name), before);
    expect(
      (await heldBrowserLocks(name)).map((l) => l.name).toList()..sort(),
      held.map((l) => l.name).toList()..sort(),
      reason: 'the refused tab holds no lock, the boot lock included',
    );
    await first.close();
    await second.close();
    expect(await heldBrowserLocks(name), isEmpty);
    final upgraded = await _openTab(name, x: const EntryTypeVersion(2, 0));
    await upgraded.close();
  });

  // Verifies: EVS-DEV-version-compatibility/G
  test('a conflicting tab that opens while another is between inspection '
      'and registration waits for it and is then refused', () async {
    final name = _freshName();
    final release = Completer<void>();
    // Released on failure too, so a held boot cannot keep the lock past
    // this test.
    addTearDown(() {
      if (!release.isCompleted) release.complete();
    });
    final heldA = Completer<void>();
    final openA = runWithDeliveryTestHooks(
      DeliveryTestHooks(
        insideBootLock: () {
          heldA.complete();
          return release.future;
        },
      ),
      () => _openTab(name),
    );
    await heldA.future;
    // Tab A has inspected and holds only its boot lock: it has registered
    // no component yet, so a tab that inspected now would see nothing.
    final heldWhileInside = await heldBrowserLocks(name);
    expect(
      heldWhileInside,
      hasLength(1),
      reason: 'A holds its boot lock and no component lock yet',
    );
    final bootLock = heldWhileInside.single.name;

    Object? outcomeB;
    var settledB = false;
    final openB = _openTab(name, x: const EntryTypeVersion(2, 0)).then<void>(
      (store) {
        outcomeB = store;
        settledB = true;
      },
      onError: (Object e) {
        outcomeB = e;
        settledB = true;
      },
    );
    final deadline = DateTime.now().add(const Duration(seconds: 10));
    while ((await browserLockCounts(bootLock)).pending != 1) {
      if (settledB) fail('B settled without waiting: $outcomeB');
      if (DateTime.now().isAfter(deadline)) {
        fail('B never requested the boot lock');
      }
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    await Future<void>.delayed(const Duration(milliseconds: 300));
    expect(settledB, isFalse, reason: 'B waits while A holds the boot lock');

    release.complete();
    final storeA = await openA;
    addTearDown(storeA.close);
    await openB;
    if (outcomeB is EventStore) {
      await (outcomeB! as EventStore).close();
    }
    expect(
      outcomeB,
      isA<IncompatibleGenerationException>().having(
        (e) => e.conflictingComponents,
        'components',
        ['entry_type:$_kX:1'],
      ),
      reason: 'B inspects after A registered, and A conflicts with it',
    );
  });

  // Verifies: EVS-DEV-version-compatibility/G
  test('of two conflicting tabs opening at once exactly one opens, in each '
      'of 10 rounds', () async {
    for (var round = 0; round < 10; round++) {
      final name = _freshName();
      final outcomes = await Future.wait(<Future<Object?>>[
        _openTab(name).then<Object?>((s) => s, onError: (Object e) => e),
        _openTab(
          name,
          x: const EntryTypeVersion(2, 0),
        ).then<Object?>((s) => s, onError: (Object e) => e),
      ]);
      for (final store in outcomes.whereType<EventStore>()) {
        await store.close();
      }
      expect(
        outcomes.whereType<EventStore>(),
        hasLength(1),
        reason: 'round $round: $outcomes',
      );
      expect(
        outcomes.whereType<IncompatibleGenerationException>(),
        hasLength(1),
        reason: 'round $round: $outcomes',
      );
    }
  });

  // Verifies: EVS-DEV-version-compatibility/H
  test('a page without a lock manager is refused, naming the secure-context '
      'requirement', () async {
    await expectLater(
      runWithDeliveryTestHooks(
        const DeliveryTestHooks(webLocksUnavailable: true),
        () => _openTab(_freshName()),
      ),
      throwsA(
        isA<GenerationGuardConfigurationException>().having(
          (e) => e.message,
          'message',
          contains('secure context'),
        ),
      ),
    );
  });

  // Verifies: EVS-DEV-version-compatibility/F
  // Verifies: EVS-DEV-version-compatibility/I
  test('a refused or failed open releases its locks, and the durable record '
      'refuses the older build after a major bump', () async {
    final name = _freshName();
    await expectLater(
      runWithDeliveryTestHooks(
        DeliveryTestHooks(afterBootVersionEvent: () => true),
        () => _openTab(name),
      ),
      throwsA(isA<InjectedFailure>()),
    );
    expect(await heldBrowserLocks(name), isEmpty);
    await (await _openTab(name)).close();
    await (await _openTab(name, x: const EntryTypeVersion(2, 0))).close();
    await expectLater(
      _openTab(name, x: const EntryTypeVersion(1, 3)),
      throwsA(isA<EntryTypeVersionDowngradeError>()),
    );
    expect(await heldBrowserLocks(name), isEmpty);
  });

  // Verifies: EVS-DEV-version-compatibility/I
  test('after a tab of another data-format major opened and closed, the '
      'compiled build is refused with the database unchanged', () async {
    final name = _freshName();
    await runWithDeliveryTestHooks(
      const DeliveryTestHooks(
        buildDeclaration: (
          version: '9.0.0',
          dataFormat: DataFormatVersion(3, 0),
        ),
      ),
      () async => (await _openTab(name)).close(),
    );
    final before = await _contents(name);
    await expectLater(
      _openTab(name),
      throwsA(isA<DataFormatIncompatibleError>()),
    );
    expect(await _contents(name), before);
    expect(await heldBrowserLocks(name), isEmpty);
  });

  // Verifies: EVS-DEV-version-compatibility/H
  test(
    'an in-memory database on the web registers under its name too',
    () async {
      final name = _freshName();
      final memory = await newDatabaseFactoryMemory().openDatabase(name);
      final store = await _openTab(name, database: memory);
      final held = await heldBrowserLocks(name);
      expect(
        held.where((l) => l.name.endsWith('entry_type:$_kX:1')),
        hasLength(1),
      );
      expect(held.every((l) => l.mode == 'shared'), isTrue);
      await store.close();
      expect(await heldBrowserLocks(name), isEmpty);
    },
  );

  // Verifies: EVS-DEV-event-store-open/E
  // Verifies: EVS-DEV-version-compatibility/E
  test('a newer-minor tab promotes the view while another tab appends, and '
      'every row is promoted or the target is left lowered', () async {
    final name = _freshName();
    final serving = await _openTab(name, withView: true);
    for (var i = 0; i < 100; i++) {
      await _appendNote(serving, 'agg-$i');
    }
    var stop = false;
    var appended = 0;
    Object? loopError;
    final loop = () async {
      try {
        while (!stop) {
          await _appendNote(serving, 'late-$appended');
          appended++;
        }
      } on Object catch (e) {
        loopError = e;
      }
    }();
    while (appended < 3) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    var bootRuns = 0;
    final newer = await runWithDeliveryTestHooks(
      DeliveryTestHooks(onBootBodyRun: () => bootRuns++),
      () => _openTab(name, x: const EntryTypeVersion(1, 1), withView: true),
    );
    // The other tab's writes wait for the boot. The first run may start
    // from this tab's copy of the database from before those writes, and
    // then re-runs once on fresh data.
    expect(bootRuns, lessThanOrEqualTo(2));
    final atBoot = appended;
    while (appended < atBoot + 3) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    stop = true;
    await loop;
    expect(
      loopError,
      isNull,
      reason: 'every append of the serving tab commits',
    );
    await serving.close();
    await newer.close();

    final observer = SembastBackend(database: await _openDatabase(name));
    final rows = await observer.findViewRows(_kView);
    final target = await observer.transaction(
      (txn) => observer.readViewTargetVersionInTxn(txn, _kView, _kX),
    );
    await observer.close();
    expect(
      rows.where((r) => (r['aggregateId']! as String).startsWith('agg-')),
      hasLength(100),
    );
    expect(
      rows
          .where((r) => (r['aggregateId']! as String).startsWith('agg-'))
          .every((r) => r['b'] == 0),
      isTrue,
      reason: 'the boot promoted every row it found',
    );
    expect(
      target == const EntryTypeVersion(1, 0) || rows.every((r) => r['b'] == 0),
      isTrue,
      reason:
          'a row folded under 1.0 after the boot left the target lowered '
          '(it is $target)',
    );
  });
}
