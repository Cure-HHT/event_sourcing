// Verifies: EVS-DEV-append-stamps-registered-version — EventStore.append
// stamps the registry's registeredVersion on every appended event. Callers
// no longer pass entryTypeVersion.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

var _dbCounter = 0;

const _kCustom = EntryTypeDefinition(
  id: 'custom_type',
  registeredVersion: 7,
  name: 'Custom Type',
);

Future<EventStore> _openStore() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'append-stamps-${_dbCounter++}.db',
  );
  final backend = SembastBackend(database: db);
  final registry = EntryTypeRegistry();
  for (final defn in kSystemEntryTypes) {
    registry.register(defn);
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
    test('stamps registry version 7 for custom_type', () async {
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
        7,
        reason:
            'append must stamp registeredVersion = 7 from the registry, '
            'not a caller-supplied value (since the parameter is removed) '
            'and not a default of 1.',
      );
    });

    test(
      'throws on unregistered entry type (existing behavior preserved)',
      () async {
        final store = await _openStore();
        expect(
          () async => store.append(
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
}
