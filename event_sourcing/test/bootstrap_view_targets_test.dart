import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

// Toy materializer used in place of DiaryEntriesMaterializer to verify the
// generic bootstrap initial-view-target-version contract without any
// diary-domain coupling. Maintains a 'toy_view' keyed on aggregate_id.
class _ToyMaterializer extends Materializer {
  const _ToyMaterializer();

  @override
  String get viewName => 'toy_view';

  @override
  bool appliesTo(StoredEvent event) => true;

  @override
  EntryPromoter get promoter => identityPromoter;

  @override
  Future<void> applyInTxn(
    Txn txn,
    StorageBackend backend, {
    required StoredEvent event,
    required Map<String, Object?> promotedData,
    required EntryTypeDefinition def,
    required List<StoredEvent> aggregateHistory,
  }) async {
    await backend.upsertViewRowInTxn(
      txn,
      viewName,
      event.aggregateId,
      <String, Object?>{'latest_event_id': event.eventId},
    );
  }
}

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
  widgetId: 'w',
  widgetConfig: <String, Object?>{},
);

void main() {
  group('REQ-d00140-J: initialViewTargetVersions', () {
    test('writes target versions atomically before returning', () async {
      // Verifies: REQ-d00140-J — bootstrap persists the supplied target
      //   versions; they are readable via the backend after bootstrap.
      final backend = await _openBackend();
      await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'demo_note'),
      );
      expect(stored, 1);
    });

    test('missing materializer entry throws ArgumentError', () async {
      // Verifies: REQ-d00140-J — bootstrap fails when a registered
      //   materializer has no entry in initialViewTargetVersions.
      final backend = await _openBackend();
      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[_demoNote],
          destinations: const <Destination>[],
          materializers: <Materializer>[const _ToyMaterializer()],
          initialViewTargetVersions: const <String, Map<String, int>>{},
        ),
        throwsArgumentError,
      );
    });

    test('conflict with existing storage throws StateError', () async {
      // Verifies: REQ-d00140-J — bootstrap conflict on existing storage
      //   surfaces as StateError; partial overwrites are not silent.
      final backend = await _openBackend();
      await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      // Re-bootstrap with a different version for the same entry type.
      await expectLater(
        bootstrapAppendOnlyDatastore(
          backend: backend,
          source: _source,
          entryTypes: const <EntryTypeDefinition>[_demoNote],
          destinations: const <Destination>[],
          materializers: <Materializer>[const _ToyMaterializer()],
          initialViewTargetVersions: const <String, Map<String, int>>{
            'toy_view': <String, int>{'demo_note': 2},
          },
        ),
        throwsStateError,
      );
    });

    test('idempotent re-bootstrap with matching values is a no-op', () async {
      // Verifies: REQ-d00140-J — re-bootstrap with identical values
      //   succeeds; the write is idempotent.
      final backend = await _openBackend();
      await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      // Second bootstrap with the same value.
      await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'demo_note'),
      );
      expect(stored, 1);
    });
  });

  group('REQ-d00140-K: AppendOnlyDatastore.setViewTargetVersion', () {
    test('writes a new entry-type version after bootstrap', () async {
      // Verifies: REQ-d00140-K — setViewTargetVersion registers a new
      //   entry type into a materializer's view_target_versions.
      final backend = await _openBackend();
      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      await ds.setViewTargetVersion('toy_view', 'late_arrival', 3);
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'late_arrival'),
      );
      expect(stored, 3);
    });

    test('overwrites an existing entry-type version', () async {
      // Verifies: REQ-d00140-K — repeated writes to the same key overwrite.
      final backend = await _openBackend();
      final ds = await bootstrapAppendOnlyDatastore(
        backend: backend,
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_demoNote],
        destinations: const <Destination>[],
        materializers: <Materializer>[const _ToyMaterializer()],
        initialViewTargetVersions: const <String, Map<String, int>>{
          'toy_view': <String, int>{'demo_note': 1},
        },
      );
      await ds.setViewTargetVersion('toy_view', 'demo_note', 5);
      final stored = await backend.transaction<int?>(
        (txn) async =>
            backend.readViewTargetVersionInTxn(txn, 'toy_view', 'demo_note'),
      );
      expect(stored, 5);
    });
  });
}
