// Verifies: EVS-DEV-append-stamps-registered-version/A — substrate stamps
//   entry_type_version from the registry's registeredVersion on every local
//   append; also verifies that lib_format_version is stamped from
//   StoredEvent.currentLibFormatVersion (EVS-PRD-event-log/A: immutable
//   log record carries all version metadata at write-time).
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<EventStore> _bootstrap() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'append-versioning-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final ds = await bootstrapAppendOnlyDatastore(
    backend: backend,
    source: const Source(
      hopId: 'mobile-device',
      identifier: 'demo-device',
      softwareVersion: 'test',
    ),
    entryTypes: <EntryTypeDefinition>[
      const EntryTypeDefinition(
        id: 'demo_note',
        registeredVersion: 5,
        name: 'demo_note',
      ),
    ],
    destinations: const <Destination>[],
  );
  return ds.eventStore;
}

void main() {
  group('EventStore.append stamps version fields', () {
    // Verifies: EVS-DEV-append-stamps-registered-version/A — substrate stamps
    // entry_type_version from the registry's registeredVersion.
    test('entry_type_version is stamped from the registry', () async {
      final es = await _bootstrap();
      final stored = await es.append(
        entryType: 'demo_note',
        aggregateId: 'a-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'answers': <String, Object?>{}},
        initiator: const UserInitiator('u-1'),
      );
      expect(stored, isNotNull);
      // Registry says registeredVersion=5; substrate stamps that, not 1.
      expect(stored!.entryTypeVersion, 5);
    });

    test('lib_format_version stamped from currentLibFormatVersion', () async {
      final es = await _bootstrap();
      final stored = await es.append(
        entryType: 'demo_note',
        aggregateId: 'a-1',
        aggregateType: 'note',
        eventType: 'finalized',
        data: const <String, Object?>{'answers': <String, Object?>{}},
        initiator: const UserInitiator('u-1'),
      );
      expect(stored, isNotNull);
      expect(stored!.libFormatVersion, StoredEvent.currentLibFormatVersion);
    });

    // Note: the substrate stamps entry_type_version from the registry's
    // registeredVersion, so there is no caller-supplied value to validate.
    // Ingest-time entry_type_version-vs-registry validation is verified by
    // test/event_store/ingest_version_validation_test.dart.
  });
}
