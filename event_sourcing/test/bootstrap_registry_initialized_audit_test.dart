// The bootstrap-time `system.entry_type_registry_initialized` audit event:
//
// - Fresh bootstrap emits exactly one event whose data.registry maps every
//   registered entry-type id to its registered version, written `M.m`.
// - Same-version reboot (same backend, same caller-supplied entry types)
//   no-ops via dedupeByContent — the second bootstrap finds prior content
//   identical and writes nothing.
// - Schema bumps emit a new audit event:
//     - adding a new caller entry type changes the registry map shape, so
//       dedupe is broken and a new event lands.
//     - raising the minor (or the major) of an existing caller entry type
//       changes the map's value for that key, so a new event lands.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _source = Source(
  hopId: 'mobile-device',
  identifier: 'init-audit-test',
  softwareVersion: 'init-audit-test@1.0.0',
);

EntryTypeDefinition _typeA({
  EntryTypeVersion version = const EntryTypeVersion(1, 0),
}) => EntryTypeDefinition(
  id: 'demo_note',
  registeredVersion: version,
  name: 'Demo Note',
);

EntryTypeDefinition _typeB() => const EntryTypeDefinition(
  id: 'red_button',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Red Button',
);

/// Open a `SembastBackend` against a path-keyed in-memory database via
/// [factory]. Reusing the same factory and path simulates a process
/// reboot against the same persisted state — sembast's in-memory
/// factory caches by path within a single factory instance, so a second
/// `openDatabase(path)` call on the same factory returns the database
/// already populated by the first call.
Future<SembastBackend> _openMemoryBackend(
  DatabaseFactory factory,
  String path,
) async {
  final db = await factory.openDatabase(path);
  return SembastBackend(database: db);
}

/// Find every event whose entry_type matches [entryType], in the order
/// returned by `findAllEvents` (ascending sequence_number).
Future<List<StoredEvent>> _eventsOfType(
  SembastBackend backend,
  String entryType,
) async {
  final all = await backend.findAllEvents();
  return all.where((e) => e.entryType == entryType).toList();
}

void main() {
  group('bootstrap registry-initialized audit', () {
    // Verifies: EVS-DEV-version-compatibility/K
    test(
      'fresh bootstrap emits '
      'system.entry_type_registry_initialized with full registry map',
      () async {
        final factory = newDatabaseFactoryMemory();
        final backend = await _openMemoryBackend(factory, 'fresh.db');
        final ds = await bootstrapEventStore(
          storage: ApplicationSuppliedStorage(
            backend,
            SembastSecurityContextStore(backend: backend),
          ),
          source: _source,
          entryTypes: <EntryTypeDefinition>[_typeA(), _typeB()],
          destinations: const <Destination>[],
        );

        final audits = await _eventsOfType(
          backend,
          kEntryTypeRegistryInitializedEntryType,
        );
        expect(audits, hasLength(1));
        final audit = audits.single;
        expect(audit.aggregateId, _source.identifier);
        expect(audit.aggregateType, 'system_registry');
        expect(audit.eventType, 'finalized');

        final registryData = audit.data['registry'];
        expect(registryData, isA<Map<String, Object?>>());
        final registryMap = registryData as Map<String, Object?>;
        // Every system + caller entry type appears with its registered
        // version. The audit event's own entry type is included too —
        // the registry was complete before the append fired.
        for (final definition in ds.entryTypes.all()) {
          expect(
            registryMap[definition.id],
            definition.registeredVersion.toString(),
            reason:
                'registry map missing or wrong version for ${definition.id}',
          );
        }
        expect(registryMap.length, ds.entryTypes.all().length);

        // The audit's own entry type is registered at 1.0 in
        // kSystemEntryTypes.
        expect(audit.entryTypeVersion, const EntryTypeVersion(1, 0));
        expect(registryMap['demo_note'], '1.0');
        expect(
          audit.initiator,
          const AutomationInitiator(service: 'lib-bootstrap'),
        );
      },
    );

    // Verifies: EVS-DEV-version-compatibility/K
    test('same-version reboot no-ops via dedupeByContent — '
        'still exactly one audit event', () async {
      // First bootstrap.
      final factory = newDatabaseFactoryMemory();
      const path = 'reboot-same.db';
      final backendA = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendA,
          SembastSecurityContextStore(backend: backendA),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA(), _typeB()],
        destinations: const <Destination>[],
      );
      final firstAudits = await _eventsOfType(
        backendA,
        kEntryTypeRegistryInitializedEntryType,
      );
      expect(firstAudits, hasLength(1));

      // Reboot — re-open the SAME database (same path on the in-memory
      // factory) and bootstrap with the SAME entry-type list.
      // dedupeByContent on the registry-init append sees identical
      // content and returns null without writing.
      final backendB = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendB,
          SembastSecurityContextStore(backend: backendB),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA(), _typeB()],
        destinations: const <Destination>[],
      );
      final secondAudits = await _eventsOfType(
        backendB,
        kEntryTypeRegistryInitializedEntryType,
      );
      expect(secondAudits, hasLength(1));
      // The single audit is unchanged — same eventId.
      expect(secondAudits.single.eventId, firstAudits.single.eventId);
    });

    // Verifies: EVS-DEV-version-compatibility/K
    test('schema bump (new entry type added) emits a new '
        'audit event with the updated registry map', () async {
      final factory = newDatabaseFactoryMemory();
      const path = 'add-type.db';
      final backendA = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendA,
          SembastSecurityContextStore(backend: backendA),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA()],
        destinations: const <Destination>[],
      );

      // Reboot with a NEW entry type added — the registry map shape
      // changes, dedupe breaks, a new audit lands.
      final backendB = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendB,
          SembastSecurityContextStore(backend: backendB),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA(), _typeB()],
        destinations: const <Destination>[],
      );

      final audits = await _eventsOfType(
        backendB,
        kEntryTypeRegistryInitializedEntryType,
      );
      expect(audits, hasLength(2));
      final later = audits[1];
      final laterRegistry = later.data['registry'] as Map<String, Object?>;
      expect(laterRegistry['demo_note'], '1.0');
      expect(laterRegistry['red_button'], '1.0');
    });

    // Verifies: EVS-DEV-version-compatibility/K
    test('a minor raise on an existing caller type emits a new audit event '
        'recording M.m for every type', () async {
      final factory = newDatabaseFactoryMemory();
      const path = 'bump-minor.db';
      final backendA = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendA,
          SembastSecurityContextStore(backend: backendA),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA(), _typeB()],
        destinations: const <Destination>[],
      );

      // Reboot with the same id at the next minor: the map value for
      // demo_note changes from 1.0 to 1.1, so a new audit lands.
      final backendB = await _openMemoryBackend(factory, path);
      final ds = await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendB,
          SembastSecurityContextStore(backend: backendB),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[
          _typeA(version: const EntryTypeVersion(1, 1)),
          _typeB(),
        ],
        destinations: const <Destination>[],
      );

      final audits = await _eventsOfType(
        backendB,
        kEntryTypeRegistryInitializedEntryType,
      );
      expect(audits, hasLength(2));
      final earlierMap = audits[0].data['registry'] as Map<String, Object?>;
      final laterMap = audits[1].data['registry'] as Map<String, Object?>;
      expect(earlierMap['demo_note'], '1.0');
      expect(laterMap['demo_note'], '1.1');
      expect(laterMap['red_button'], '1.0');
      expect(laterMap.length, ds.entryTypes.all().length);
      for (final definition in ds.entryTypes.all()) {
        expect(
          laterMap[definition.id],
          '${definition.registeredVersion.major}.'
          '${definition.registeredVersion.minor}',
        );
      }
    });

    // Verifies: EVS-DEV-version-compatibility/K
    test('a major raise on an existing caller type emits a new audit '
        'event', () async {
      final factory = newDatabaseFactoryMemory();
      const path = 'bump-major.db';
      final backendA = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendA,
          SembastSecurityContextStore(backend: backendA),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[_typeA()],
        destinations: const <Destination>[],
      );

      final backendB = await _openMemoryBackend(factory, path);
      await bootstrapEventStore(
        storage: ApplicationSuppliedStorage(
          backendB,
          SembastSecurityContextStore(backend: backendB),
        ),
        source: _source,
        entryTypes: <EntryTypeDefinition>[
          _typeA(version: const EntryTypeVersion(2, 0)),
        ],
        destinations: const <Destination>[],
      );

      final audits = await _eventsOfType(
        backendB,
        kEntryTypeRegistryInitializedEntryType,
      );
      expect(audits, hasLength(2));
      final earlierMap = audits[0].data['registry'] as Map<String, Object?>;
      final laterMap = audits[1].data['registry'] as Map<String, Object?>;
      expect(earlierMap['demo_note'], '1.0');
      expect(laterMap['demo_note'], '2.0');
    });
  });
}
