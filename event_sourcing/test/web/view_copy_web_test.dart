// The web half of view convergence: the incompatible-generation guard's
// snapshot of live view fingerprints spares a serving tab's copy at a
// second tab's boot, and the view copy lock's `ifAvailable` request lets a
// second holder skip instead of waiting. Two tab models share one
// IndexedDB database, each opening it through its own independently built
// sembast_web factory, so each holds its own sembast Database, as two
// browser tabs do.

@TestOn('browser')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/view_fingerprint.dart'
    show viewFingerprint;
import 'package:event_sourcing/src/storage/web_locks.dart'
    show heldBrowserLocks, runHoldingBrowserViewCopyLock;
import 'package:flutter_test/flutter_test.dart';
import 'package:idb_shim/idb_client_native.dart' show idbFactoryWeb;
import 'package:sembast/sembast.dart' as sembast;
// ignore: implementation_imports, builds a second independent factory to model a second tab
import 'package:sembast_web/src/web_interop.dart'
    show DatabaseFactoryWeb, JdbFactoryWeb;

const _kX = 'view_copy_web_x';
const _kY = 'view_copy_web_y';
const _kView = 'view_copy_web_notes';

const _kSpecX = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kX}),
  tombstoneEventTypes: <String>{},
);

const _kSpecY = AggregateProjectionSpec(
  viewName: _kView,
  interest: SubscriptionFilter(entryTypes: <String>{_kY}),
  tombstoneEventTypes: <String>{},
);

/// The entry-type registry every [_openTab] call registers: the system
/// types plus [_kX] and [_kY], both at version 1.0.
EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry
    ..register(
      const EntryTypeDefinition(
        id: _kX,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kX,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kY,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kY,
      ),
    );
}

/// [spec]'s fingerprint under the entry types every [_openTab] call
/// registers and no promoters, matching what a real boot computes.
String _fingerprintOf(ProjectionSpec spec) =>
    viewFingerprint(spec, _entryTypes(), PromoterRegistry());

/// Opens [dbName] through a factory of its own, as one tab would.
Future<sembast.Database> _openDatabase(String dbName) =>
    DatabaseFactoryWeb(JdbFactoryWeb(idbFactoryWeb)).openDatabase(dbName);

/// Opens an event store over [dbName] as one tab would, registering both
/// [_kX] and [_kY] and the single view [spec].
Future<EventStore> _openTab(String dbName, ProjectionSpec spec) async {
  final backend = SembastBackend(database: await _openDatabase(dbName));
  final registry = _entryTypes();
  final projections = ProjectionRegistry()..register(spec);
  try {
    return await EventStore.open(
      storage: ApplicationSuppliedStorage(
        backend,
        SembastSecurityContextStore(backend: backend),
      ),
      entryTypes: registry,
      source: const Source(
        hopId: 'view-copy-web-hop',
        identifier: 'view-copy-web-install',
        softwareVersion: 'view-copy-web-test',
      ),
      projections: projections,
      promoters: PromoterRegistry(),
    );
  } catch (_) {
    await backend.close();
    rethrow;
  }
}

/// The stored view copies of a fresh handle onto [dbName], excluding the
/// library's own default destination-wedges view.
Future<List<ViewCopy>> _copies(String dbName) async {
  final backend = SembastBackend(database: await _openDatabase(dbName));
  try {
    final all = await backend.transaction(backend.readViewCopiesInTxn);
    return [
      for (final c in all)
        if (c.viewName == _kView) c,
    ];
  } finally {
    await backend.close();
  }
}

String _freshName() => 'view-copy-${DateTime.now().microsecondsSinceEpoch}.db';

void main() {
  // Verifies: EVS-DEV-view-convergence/C
  // Verifies: EVS-DEV-view-convergence/D
  test("a second tab's boot spares a still-open tab's copy, and a further "
      'boot after that tab closes marks it for deletion', () async {
    final name = _freshName();
    final tabA = await _openTab(name, _kSpecX);
    final copyIdA = tabA.copyIdOf(_kView);

    final tabB = await _openTab(name, _kSpecY);
    final copyIdB = tabB.copyIdOf(_kView);
    expect(copyIdB, isNot(copyIdA));

    // A's own registration holds a shared view_fingerprint lock for its
    // definition, observed directly rather than only through B's boot
    // sparing A's copy.
    final fingerprintA = _fingerprintOf(_kSpecX);
    final heldWhileALive = await heldBrowserLocks(name);
    final lockA = heldWhileALive.where(
      (l) => l.name.endsWith('view_fingerprint:$fingerprintA'),
    );
    expect(lockA, hasLength(1));
    expect(lockA.single.mode, 'shared');

    final whileAIsLive = await _copies(name);
    expect(whileAIsLive, hasLength(2));
    expect(
      whileAIsLive.singleWhere((c) => c.copyId == copyIdA).markedForDeletion,
      isFalse,
      reason: "B's boot must spare A's copy: A's fingerprint is live",
    );
    expect(
      whileAIsLive.singleWhere((c) => c.copyId == copyIdB).markedForDeletion,
      isFalse,
    );
    await tabB.close();

    await tabA.close();
    expect(
      (await heldBrowserLocks(
        name,
      )).where((l) => l.name.endsWith('view_fingerprint:$fingerprintA')),
      isEmpty,
      reason: "A's fingerprint is no longer live once A closed",
    );

    final tabC = await _openTab(name, _kSpecY);
    final afterAClosed = await _copies(name);
    expect(
      afterAClosed.singleWhere((c) => c.copyId == copyIdA).markedForDeletion,
      isTrue,
      reason:
          "no build registers A's fingerprint and no live tab holds it "
          "any more, so the further boot marks A's copy",
    );
    expect(
      afterAClosed.singleWhere((c) => c.copyId == copyIdB).markedForDeletion,
      isFalse,
      reason:
          "tabC names the same fingerprint as B, so tabC's boot reuses "
          "B's copy regardless of whether B is still live",
    );
    await tabC.close();
  });

  // Verifies: EVS-DEV-view-convergence/M
  test('an ifAvailable request for a held view copy lock skips without '
      'running, and runs once the holder releases', () async {
    final path = 'view-copy-lock-${DateTime.now().microsecondsSinceEpoch}';
    const copyKey = 'copy-under-test';
    final release = Completer<void>();
    final holderRunning = Completer<void>();
    final holderDone = runHoldingBrowserViewCopyLock<int>(
      path: path,
      copyKey: copyKey,
      body: () async {
        holderRunning.complete();
        await release.future;
        return 1;
      },
    );
    await holderRunning.future;

    var secondRan = false;
    final second =
        await runHoldingBrowserViewCopyLock<int>(
          path: path,
          copyKey: copyKey,
          body: () async {
            secondRan = true;
            return 2;
          },
        ).timeout(
          const Duration(seconds: 2),
          onTimeout: () =>
              fail('the ifAvailable request waited instead of returning null'),
        );
    expect(second, isNull, reason: 'the holder still has the lock');
    expect(secondRan, isFalse);

    release.complete();
    expect(await holderDone, 1);

    var thirdRan = false;
    final third = await runHoldingBrowserViewCopyLock<int>(
      path: path,
      copyKey: copyKey,
      body: () async {
        thirdRan = true;
        return 3;
      },
    );
    expect(third, 3);
    expect(thirdRan, isTrue);
  });
}
