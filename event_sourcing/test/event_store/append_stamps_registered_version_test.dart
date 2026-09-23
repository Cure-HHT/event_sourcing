// EventStore.append and appendInTxn stamp the registry's registered major and
// minor, and the library's data-format version, on every appended event; the
// entryTypeVersion parameter does not appear on the public signatures, so
// callers cannot override the registry-derived value.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show
        kEntryTypeRegistryInitializedEntryType,
        kLibVersionInitializedEntryType,
        kViewSnapshotPromotedEntryType;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kCustom = EntryTypeDefinition(
  id: 'custom_type',
  registeredVersion: EntryTypeVersion(7, 3),
  name: 'Custom Type',
);

Future<EventStore> _openStore() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'append-stamps-${_dbCounter++}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(_kCustom);
  return EventStore.openForTest(
    storage: backend,
    entryTypes: registry,
    source: const Source(
      hopId: 'test',
      identifier: 'test-install',
      softwareVersion: '0.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

void main() {
  group('EventStore.append stamps registeredVersion', () {
    // Verifies: EVS-DEV-append-stamps-registered-version/A+C
    // Verifies: EVS-DEV-version-compatibility/C
    test('append stamps registry version 7.3 and the data format', () async {
      final store = await _openStore();
      final stored = await store.append(
        entryType: 'custom_type',
        aggregateId: 'agg-1',
        aggregateType: 'custom_type',
        eventType: 'finalized',
        data: const <String, Object?>{},
        initiator: const AutomationInitiator(service: 'test'),
      );
      expect(stored, isNotNull);
      expect(
        stored!.entryTypeVersion,
        const EntryTypeVersion(7, 3),
        reason:
            'append must stamp the registered 7.3 from the registry, '
            'not a caller-supplied value (the parameter does not exist) '
            'and not a default.',
      );
      expect(stored.libFormatVersion, LibVersion.dataFormat);
      final readBack = await store.backend.findEventById(stored.eventId);
      expect(readBack!.entryTypeVersion, const EntryTypeVersion(7, 3));
      expect(readBack.libFormatVersion, LibVersion.dataFormat);
    });

    // Verifies: EVS-DEV-append-stamps-registered-version/B+C
    // Verifies: EVS-DEV-version-compatibility/C
    test(
      'appendInTxn stamps registry version 7.3 and the data format',
      () async {
        final store = await _openStore();
        final stored = await store.runTransaction<StoredEvent?>(
          (txn, collector) => store.appendInTxn(
            txn,
            collector: collector,
            flowToken: null,
            metadata: null,
            security: null,
            checkpointReason: null,
            changeReason: null,
            dedupeByContent: false,
            entryType: 'custom_type',
            aggregateId: 'agg-2',
            aggregateType: 'custom_type',
            eventType: 'finalized',
            data: const <String, Object?>{},
            initiator: const AutomationInitiator(service: 'test'),
          ),
        );
        expect(stored!.entryTypeVersion, const EntryTypeVersion(7, 3));
        expect(stored.libFormatVersion, LibVersion.dataFormat);
        final readBack = await store.backend.findEventById(stored.eventId);
        expect(readBack!.entryTypeVersion, const EntryTypeVersion(7, 3));
        expect(readBack.libFormatVersion, LibVersion.dataFormat);
      },
    );

    test(
      'throws on unregistered entry type (existing behavior preserved)',
      () async {
        final store = await _openStore();
        await expectLater(
          store.append(
            entryType: 'not_registered',
            aggregateId: 'agg-1',
            aggregateType: 'whatever',
            eventType: 'finalized',
            data: const <String, Object?>{},
            initiator: const AutomationInitiator(service: 'test'),
          ),
          throwsArgumentError,
        );
      },
    );
  });

  group('the events the library appends itself', () {
    Future<void> bootstrapAt(
      SembastBackend backend,
      EntryTypeVersion registered,
    ) => bootstrapEventStore(
      backend: backend,
      source: const Source(
        hopId: 'test',
        identifier: 'test-install',
        softwareVersion: '0.0.0',
      ),
      entryTypes: <EntryTypeDefinition>[
        EntryTypeDefinition(
          id: 'custom_type',
          registeredVersion: registered,
          name: 'Custom Type',
        ),
      ],
      destinations: const <Destination>[],
      projections: ProjectionRegistry()
        ..register(
          const AggregateProjectionSpec(
            viewName: 'customs',
            interest: SubscriptionFilter(entryTypes: <String>{'custom_type'}),
            tombstoneEventTypes: <String>{},
          ),
        ),
    );

    // Verifies: EVS-DEV-version-compatibility/C
    test('the library-version, registry and snapshot-promotion audits carry '
        'the data format and their registered versions', () async {
      final db = await newDatabaseFactoryMemory().openDatabase(
        'append-stamps-internal-${_dbCounter++}.db',
      );
      final backend = SembastBackend(database: db);
      await bootstrapAt(backend, const EntryTypeVersion(1, 0));
      await bootstrapAt(backend, const EntryTypeVersion(1, 1));

      final registered = <String, EntryTypeVersion>{
        for (final definition in kSystemEntryTypes)
          definition.id: definition.registeredVersion,
      };
      for (final entryType in <String>[
        kLibVersionInitializedEntryType,
        kEntryTypeRegistryInitializedEntryType,
        kViewSnapshotPromotedEntryType,
      ]) {
        final events = await backend.findAllEvents(entryType: entryType);
        expect(events, isNotEmpty, reason: entryType);
        for (final event in events) {
          expect(
            event.libFormatVersion,
            LibVersion.dataFormat,
            reason: entryType,
          );
          expect(
            event.entryTypeVersion,
            registered[entryType],
            reason: entryType,
          );
        }
      }
    });
  });
}
