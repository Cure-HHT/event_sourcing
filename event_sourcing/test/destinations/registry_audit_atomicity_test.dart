// Verifies: EVS-PRD-destinations/D
// verifies atomicity of DestinationRegistry
// mutations: a failure injected after an operation's last write (through the
// `failRegistryAuditAppend` test seam) rolls the whole transaction back, so
// the schedule write or queue retirement never persists without its audit
// (D — durable queues commit atomically with their audit). Each test asserts
// the exact end state: every record the operation writes, and the log, as
// they were before the call.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';
import '../test_support/queue_test_support.dart';
import '../test_support/registry_with_audit.dart';

const _testInit = AutomationInitiator(service: 'test-bootstrap');

Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

/// Everything the registry operations under test may change, and the log.
Future<Map<String, Object?>> _state(
  SembastBackend backend,
  String id,
) async => <String, Object?>{
  'schedule': (await backend.readSchedule(id))?.toJson(),
  'rows': <Object?>[
    for (final r in await backend.listFifoEntries(id)) r.toJson(),
  ],
  'cursor': await backend.readFillCursor(id),
  'request': (await backend.transaction(
    (txn) => backend.readReplayRequestTxn(txn, id),
  ))?.toJson(),
  'events': <String>[for (final e in await backend.findAllEvents()) e.eventId],
};

/// Runs [op] with a failure injected after the last write of the operation
/// whose audit is [entryType].
Future<void> _failing(String entryType, Future<void> Function() op) =>
    runWithDeliveryTestHooks(
      DeliveryTestHooks(failRegistryAuditAppend: (t) => t == entryType),
      op,
    );

void main() {
  group('DestinationRegistry mutation atomicity', () {
    late SembastBackend backend;
    late DestinationRegistry registry;
    var counter = 0;

    setUp(() async {
      counter += 1;
      backend = await _openBackend('atomicity-$counter.db');
      final deps = await buildAuditedRegistryDeps(backend);
      registry = DestinationRegistry(eventStore: deps.eventStore);
    });

    tearDown(() async {
      await backend.close();
    });

    // The schedule write and the in-memory registration roll back when the
    // transaction fails after the audit append.
    test(
      'addDestination: an injected failure rolls back the schedule write',
      () async {
        final dest = FakeDestination(id: 'atomic');
        final before = await _state(backend, 'atomic');

        await expectLater(
          _failing(
            kDestinationRegisteredEntryType,
            () => registry.addDestination(dest, initiator: _testInit),
          ),
          throwsA(isA<InjectedFailure>()),
        );

        expect(await _state(backend, 'atomic'), before);
        expect(await backend.readSchedule('atomic'), isNull);
        expect(registry.byId('atomic'), isNull);
      },
    );

    // The schedule write and the replay request roll back with the audit.
    test(
      'setStartDate: an injected failure rolls back the schedule write',
      () async {
        await registry.addDestination(
          FakeDestination(id: 'atomic'),
          initiator: _testInit,
        );
        final before = await _state(backend, 'atomic');

        await expectLater(
          _failing(
            kDestinationStartDateSetEntryType,
            () => registry.setStartDate(
              'atomic',
              DateTime.utc(2026, 1, 1),
              initiator: _testInit,
            ),
          ),
          throwsA(isA<InjectedFailure>()),
        );

        expect(await _state(backend, 'atomic'), before);
        expect((await backend.readSchedule('atomic'))!.startDate, isNull);
      },
    );

    // The queue retirement and the schedule drop roll back with the audit.
    test('deleteDestination: an injected failure rolls back the queue '
        'retirement and the schedule drop', () async {
      await registry.addDestination(
        FakeDestination(id: 'purgeable', allowHardDelete: true),
        initiator: _testInit,
      );
      await registry.setStartDate(
        'purgeable',
        DateTime.utc(2020, 1, 1),
        initiator: _testInit,
      );
      await enqueueSingle(
        backend,
        'purgeable',
        eventId: 'evt-1',
        sequenceNumber: 1,
      );
      await enqueueSingle(
        backend,
        'purgeable',
        eventId: 'evt-2',
        sequenceNumber: 2,
      );
      await wedgeHeadForTest(registry, 'purgeable');
      final before = await _state(backend, 'purgeable');

      await expectLater(
        _failing(
          kDestinationDeletedEntryType,
          () => registry.deleteDestination('purgeable', initiator: _testInit),
        ),
        throwsA(isA<InjectedFailure>()),
      );

      expect(await _state(backend, 'purgeable'), before);
      expect(registry.byId('purgeable'), isNotNull);
      expect(await backend.readFifoHead('purgeable'), isNotNull);
    });
  });
}
