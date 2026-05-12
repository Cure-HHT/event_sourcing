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
  group('REQ-d00141-B,E: EventStore.append stamps version fields', () {
    // Verifies: REQ-d00141-B
    // Verifies: EVS-DEV-append-stamps-registered-version — substrate stamps
    // entry_type_version from the registry's registeredVersion.
    test(
      'REQ-d00141-B: entry_type_version is stamped from the registry',
      () async {
        final es = await _bootstrap();
        final stored = await es.append(
          entryType: 'demo_note',
          aggregateId: 'a-1',
          aggregateType: 'DiaryEntry',
          eventType: 'finalized',
          data: const <String, Object?>{'answers': <String, Object?>{}},
          initiator: const UserInitiator('u-1'),
        );
        expect(stored, isNotNull);
        // Registry says registeredVersion=5; substrate stamps that, not 1.
        expect(stored!.entryTypeVersion, 5);
      },
    );

    // Verifies: REQ-d00141-E
    test(
      'REQ-d00141-E: lib_format_version stamped from currentLibFormatVersion',
      () async {
        final es = await _bootstrap();
        final stored = await es.append(
          entryType: 'demo_note',
          aggregateId: 'a-1',
          aggregateType: 'DiaryEntry',
          eventType: 'finalized',
          data: const <String, Object?>{'answers': <String, Object?>{}},
          initiator: const UserInitiator('u-1'),
        );
        expect(stored, isNotNull);
        expect(stored!.libFormatVersion, StoredEvent.currentLibFormatVersion);
      },
    );

    // Note: the former REQ-d00141-F test ("append does NOT validate
    // entryTypeVersion against registry") has been deleted. Under the new
    // contract the substrate stamps entry_type_version from the registry's
    // registeredVersion, so there is no caller-supplied value to validate;
    // ingest-time entry_type_version-vs-registry validation is still
    // verified by test/event_store/ingest_version_validation_test.dart.
  });
}
