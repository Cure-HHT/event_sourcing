// Verifies: EVS-DEV-view-target-versions-seeding — EventStore.open seeds
// view_target_versions rows for every (projection viewName, entry type
// in the projection's interest) pair where no row exists, at the entry
// type's current registeredVersion.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/snapshot_promotion.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kNote = EntryTypeDefinition(
  id: 'note',
  registeredVersion: 3,
  name: 'Note',
);

const _kLights = EntryTypeDefinition(
  id: 'lights',
  registeredVersion: 1,
  name: 'Lights',
);

const _kNotesSpec = AggregateProjectionSpec(
  viewName: 'notes',
  interest: SubscriptionFilter(entryTypes: <String>['note']),
  tombstoneEventTypes: <String>{},
);

const _kLightsSpec = AggregateProjectionSpec(
  viewName: 'lights',
  interest: SubscriptionFilter(entryTypes: <String>['lights']),
  tombstoneEventTypes: <String>{},
);

Future<SembastBackend> _openBackend() async {
  final db = await databaseFactoryMemory.openDatabase(
    'test_${_dbCounter++}.db',
  );
  return SembastBackend(database: db);
}

EntryTypeRegistry _registry() {
  final r = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    r.register(defn);
  }
  r
    ..register(_kNote)
    ..register(_kLights);
  return r;
}

void main() {
  group('seedViewTargetVersions', () {
    test('greenfield install seeds rows at current registeredVersion '
        'for every interest-matched entry type', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()
        ..register(_kNotesSpec)
        ..register(_kLightsSpec);

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
          3,
        );
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'lights', 'lights'),
          1,
        );
      });
    });

    test('does not overwrite existing rows', () async {
      final backend = await _openBackend();
      final entryTypes = _registry();
      final projections = ProjectionRegistry()..register(_kNotesSpec);

      // Pre-seed at a lagging version (simulating a prior boot under an
      // older registry where note was at registeredVersion=1).
      await backend.transaction((txn) async {
        await backend.writeViewTargetVersionInTxn(txn, 'notes', 'note', 1);
      });

      await backend.transaction((txn) async {
        await seedViewTargetVersions(
          txn: txn,
          backend: backend,
          projections: projections,
          entryTypes: entryTypes,
        );
      });

      await backend.transaction((txn) async {
        expect(
          await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
          1,
          reason:
              'seedViewTargetVersions must not overwrite an existing '
              'row; that lagging value drives the snapshot-promotion pass.',
        );
      });
    });

    test(
      'ignores entry types not matched by any projection interest',
      () async {
        final backend = await _openBackend();
        final entryTypes = _registry();
        // No projection cares about lights.
        final projections = ProjectionRegistry()..register(_kNotesSpec);

        await backend.transaction((txn) async {
          await seedViewTargetVersions(
            txn: txn,
            backend: backend,
            projections: projections,
            entryTypes: entryTypes,
          );
        });

        await backend.transaction((txn) async {
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'notes', 'note'),
            3,
          );
          // lights has no projection; no row was seeded.
          expect(
            await backend.readViewTargetVersionInTxn(txn, 'lights', 'lights'),
            isNull,
          );
        });
      },
    );
  });
}
