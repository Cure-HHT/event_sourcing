// Verifies: EVS-DEV-view-target-versions-seeding
// Tests for EventStoreBundle.setViewTargetVersion.
// View target versions are written directly via setViewTargetVersion or
// by rebuildView.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'bvt-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'd',
  softwareVersion: 'v',
);

const EntryTypeDefinition _demoNote = EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'demo_note',
);

void main() {
  group('EventStoreBundle.setViewTargetVersion', () {
    test('writes a new entry-type version after bootstrap', () async {
      //   entry type into a view's view_target_versions.
      final backend = await _openBackend();
      final ds = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
      );
      await ds.setViewTargetVersion(
        'toy_view',
        'late_arrival',
        const EntryTypeVersion(3, 0),
      );
      final stored = await backend.transaction<EntryTypeVersion?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'late_arrival'),
      );
      expect(stored, const EntryTypeVersion(3, 0));
    });

    test('overwrites an existing entry-type version', () async {
      final backend = await _openBackend();
      final ds = await bootstrapEventStore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
      );
      await ds.setViewTargetVersion(
        'toy_view',
        'demo_note',
        const EntryTypeVersion(1, 0),
      );
      await ds.setViewTargetVersion(
        'toy_view',
        'demo_note',
        const EntryTypeVersion(5, 2),
      );
      final stored = await backend.transaction<EntryTypeVersion?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'demo_note'),
      );
      expect(stored, const EntryTypeVersion(5, 2));
    });
  });
}
