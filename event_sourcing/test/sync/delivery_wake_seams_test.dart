// Verifies: EVS-DEV-destination-drain-lock/D
// an event store's trigger slot is held by its one started, not yet closed
//   delivery cycle: the onDeliveryWake seam reports, for each wake, whether
//   a started cycle held the slot, and an observer that throws does not
//   reach the operation that woke; a cycle started under the
//   handDrivenCycle seam holds the slot while a wake runs no pass of it,
//   and its own calls still run passes.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';

const Initiator _init = AutomationInitiator(service: 'wake-seams-test');
const String _noteType = 'wake_note';

class _Fixture {
  _Fixture(this.backend, this.store, this.registry);

  final SembastBackend backend;
  final EventStore store;
  final DestinationRegistry registry;

  Future<void> note(String id) => store.append(
    entryType: _noteType,
    aggregateId: id,
    aggregateType: 'note',
    eventType: 'finalized',
    data: <String, Object?>{'id': id},
    initiator: _init,
  );
}

var _counter = 0;

Future<_Fixture> _open() async {
  _counter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'wake-seams-$_counter.db',
  );
  final backend = SembastBackend(database: db);
  final store = await EventStore.openForTest(
    storage: backend,
    entryTypes: EntryTypeRegistry()
      ..register(
        const EntryTypeDefinition(
          id: _noteType,
          registeredVersion: EntryTypeVersion(1, 0),
          name: _noteType,
        ),
      ),
    source: const Source(
      hopId: 'test',
      identifier: 'wake-seams-install',
      softwareVersion: 'test@1.0.0',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
  addTearDown(backend.close);
  return _Fixture(backend, store, DestinationRegistry(eventStore: store));
}

/// Waits until [condition] holds, or fails after two seconds.
Future<void> _until(bool Function() condition, String reason) async {
  final deadline = DateTime.now().add(const Duration(seconds: 2));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('timed out: $reason');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

void main() {
  group('onDeliveryWake', () {
    test(
      'reports each wake and whether a started cycle held the slot',
      () async {
        final f = await _open();
        final wakes = <bool>[];
        final hooks = DeliveryTestHooks(onDeliveryWake: wakes.add);
        await runWithDeliveryTestHooks(hooks, () => f.note('before'));
        expect(wakes, <bool>[false], reason: 'no cycle is started');

        final cycle = await SyncCycle.start(
          registry: f.registry,
          cadence: const Duration(hours: 1),
        );
        await runWithDeliveryTestHooks(hooks, () => f.note('during'));
        expect(wakes, <bool>[false, true], reason: 'the cycle holds the slot');

        await cycle.close();
        await runWithDeliveryTestHooks(hooks, () => f.note('after'));
        expect(wakes, <bool>[
          false,
          true,
          false,
        ], reason: 'a closed cycle gives the slot up');
      },
    );

    test('an observer that throws does not reach the append', () async {
      final f = await _open();
      final logged = <String>[];
      final hooks = DeliveryTestHooks(
        onDeliveryWake: (_) => throw StateError('observer failed'),
        onLog: (r) => logged.add(r.message),
      );
      await runWithDeliveryTestHooks(hooks, () => f.note('n'));
      expect(await f.backend.findAllEvents(entryType: _noteType), hasLength(1));
      expect(logged, contains('the onDeliveryWake test seam threw'));
    });
  });

  group('handDrivenCycle', () {
    Future<(_Fixture, FakeDestination)> withDestination() async {
      final f = await _open();
      final d = FakeDestination(
        id: 'x',
        script: <SendResult>[for (var i = 0; i < 4; i++) const SendOk()],
      );
      await f.registry.addDestination(d, initiator: _init);
      await f.registry.setStartDate('x', DateTime.utc(2000), initiator: _init);
      return (f, d);
    }

    test('without the seam an append wakes the cycle into a pass', () async {
      final (f, d) = await withDestination();
      final cycle = await SyncCycle.start(
        registry: f.registry,
        cadence: const Duration(hours: 1),
      );
      addTearDown(cycle.close);
      await cycle();
      final before = d.sent.length;
      await f.note('n1');
      await _until(() => d.sent.length > before, 'the woken pass sends');
    });

    test(
      'a wake runs no pass of a hand-driven cycle; calling it does',
      () async {
        final (f, d) = await withDestination();
        final wakes = <bool>[];
        final cycle = await runWithDeliveryTestHooks(
          const DeliveryTestHooks(handDrivenCycle: true),
          () => SyncCycle.start(
            registry: f.registry,
            cadence: const Duration(hours: 1),
          ),
        );
        addTearDown(cycle.close);
        await cycle();
        final before = d.sent.length;
        await runWithDeliveryTestHooks(
          DeliveryTestHooks(onDeliveryWake: wakes.add),
          () => f.note('n1'),
        );
        expect(wakes, <bool>[true], reason: 'the cycle still holds the slot');
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(d.sent.length, before, reason: 'the wake ran no pass');
        await cycle();
        expect(d.sent.length, greaterThan(before), reason: 'the call ran one');
      },
    );
  });
}
