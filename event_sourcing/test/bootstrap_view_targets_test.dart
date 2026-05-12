// Tests for AppendOnlyDatastore.setViewTargetVersion.
//
// The legacy initialViewTargetVersions / materializers bootstrap parameters
// are removed in Task 22 (CUR-1317). View target versions are now written
// directly via setViewTargetVersion or by rebuildView (Task 23). This file
// retains only the setViewTargetVersion tests.

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
  registeredVersion: 1,
  name: 'demo_note',
);

void main() {
  group('AppendOnlyDatastore.setViewTargetVersion', () {
    test('writes a new entry-type version after bootstrap', () async {
      //   entry type into a view's view_target_versions.
      final backend = await _openBackend();
      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
      );
      await ds.setViewTargetVersion('toy_view', 'late_arrival', 3);
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'late_arrival'),
      );
      expect(stored, 3);
    });

    test('overwrites an existing entry-type version', () async {
      final backend = await _openBackend();
      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
      );
      await ds.setViewTargetVersion('toy_view', 'demo_note', 1);
      await ds.setViewTargetVersion('toy_view', 'demo_note', 5);
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'demo_note'),
      );
      expect(stored, 5);
    });
  });
}
